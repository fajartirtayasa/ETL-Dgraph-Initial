package incremental

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"strings"

	"hara/traceability-dgraph-etl/internal/etl/common"
)

const eventIDsQuery = `WITH UnsyncedLinks AS (
    SELECT event1_id, event2_id
    FROM links_bc
    WHERE sync_time IS NULL
),

ValidUnsyncedLinks AS (
    SELECT ul.event1_id AS unique_id
    FROM UnsyncedLinks ul
    JOIN events_bc e ON ul.event1_id = e.unique_id
    WHERE e.status = 'valid'
    UNION
    SELECT ul.event2_id AS unique_id
    FROM UnsyncedLinks ul
    JOIN events_bc e ON ul.event2_id = e.unique_id
    WHERE e.status = 'valid'
),

UnsyncedEvents AS (
  SELECT e.unique_id
  FROM events_bc e
  WHERE e.sync_time IS NULL
    AND e.status = 'valid'
    AND EXISTS (
        SELECT 1 FROM links_bc l
        WHERE l.status = 'valid'
              AND (l.event1_id = e.unique_id or l.event2_id = e.unique_id)
    )
),

UnsyncedUniqueIDs AS (
    SELECT unique_id FROM ValidUnsyncedLinks
    UNION
    SELECT unique_id FROM UnsyncedEvents
)

SELECT unique_id FROM UnsyncedUniqueIDs;`

const eventDetailsQuery = `
WITH MasterMill AS (
    SELECT uml_id, mill_id
    FROM master_mill
    GROUP BY uml_id, mill_id
)
SELECT
    e.unique_id,
    e.client_id,
    e.bl_number,
    e.sales_order,
    e.product_id,
    e.product_name,
    e.plant,
    e.production_label,
    e.process_date,
    e.material_type,
    e.material_code,
    e.amount,
    e.unit,
    e.src_path,
    e.sub_data_source,
    e.version,
    e.flag,
    e.material_document,
    e.purchase_order,
    e.uml_id,
    mm.mill_id,
    e.location_id,
    e.vendor,
    e.source,
    loc.subject_type AS location_subject_type,
    loc.master_id AS location_master_id,
    loc.master_type AS location_master_type,
    loc_fac.subject_id AS location_facility_subject_id,
    loc_fac.subject_type AS location_facility_subject_type,
    loc_fac.master_id AS location_facility_master_id,
    loc_fac.master_type AS location_facility_master_type,
    src.subject_type AS source_subject_type,
    src.master_id AS source_master_id,
    src.master_type AS source_master_type,
    src_fac.subject_id AS source_facility_subject_id,
    src_fac.subject_type AS source_facility_subject_type,
    src_fac.master_id AS source_facility_master_id,
    src_fac.master_type AS source_facility_master_type,
    e.movement_type,
    e.movement_type_text,
    e.storage_location,
    e.partner_pca,
    e.mill_name
FROM
    events_bc e
LEFT JOIN subjects loc ON loc.subject_id = e.location_id
LEFT JOIN mv_subject_hierarchy loc_fac ON loc_fac.start_subject_id = e.location_id
LEFT JOIN subjects src ON src.subject_id = e.source
LEFT JOIN mv_subject_hierarchy src_fac ON src_fac.start_subject_id = e.source
LEFT JOIN MasterMill mm ON mm.uml_id = e.uml_id
WHERE e.unique_id = ANY($1)
  AND e.status = 'valid';`

type eventRecord struct {
	UniqueID                    string
	ClientID                    string
	BLNumber                    string
	SalesOrder                  string
	ProductID                   string
	ProductName                 string
	Plant                       string
	ProductionLabel             string
	ProcessDate                 *int64
	MaterialType                string
	MaterialCode                string
	Amount                      *float64
	Unit                        string
	SrcPath                     string
	SubDataSource               string
	Version                     string
	Flag                        string
	MaterialDocument            string
	PurchaseOrder               string
	UMLID                       string
	MillID                      string
	LocationID                  string
	Vendor                      string
	Source                      string
	LocationSubjectType         string
	LocationMasterID            string
	LocationMasterType          string
	LocationFacilitySubjectID   string
	LocationFacilitySubjectType string
	LocationFacilityMasterID    string
	LocationFacilityMasterType  string
	SourceSubjectType           string
	SourceMasterID              string
	SourceMasterType            string
	SourceFacilitySubjectID     string
	SourceFacilitySubjectType   string
	SourceFacilityMasterID      string
	SourceFacilityMasterType    string
	MovementType                string
	MovementTypeText            string
	StorageLocation             string
	PartnerPCA                  string
	MillName                    string
}

func (m *Migrator) syncEvents(ctx context.Context) (*eventSyncResult, error) {
	rows, err := m.db.QueryRows(ctx, eventIDsQuery)
	if err != nil {
		return nil, fmt.Errorf("query event ids: %w", err)
	}
	defer rows.Close()

	uniqueIDs := stringSet{}
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, fmt.Errorf("scan event id: %w", err)
		}
		uniqueIDs.Add(id)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate event ids: %w", err)
	}

	allUniqueIDs := uniqueIDs.Values()
	uidMap := newUIDMap(nil)

	if len(allUniqueIDs) == 0 {
		return &eventSyncResult{
			UniqueIDs: allUniqueIDs,
			UIDs:      uidMap,
		}, nil
	}

	m.log.Info("syncing events in chunks", slog.Int("total_events", len(allUniqueIDs)), slog.Int("chunk_size", m.cfg.FetchChunkSize))

	for i, idx := range chunkIndices(len(allUniqueIDs), m.cfg.FetchChunkSize) {
		chunk := allUniqueIDs[idx[0]:idx[1]]
		m.log.Debug("processing event chunk", slog.Int("chunk_index", i), slog.Int("size", len(chunk)))

		// Lookup existing UIDs for this chunk to determine Blank Nodes vs existing UIDs
		uidLookup, err := m.lookupEventUIDs(ctx, chunk, m.cfg.Events.LookupBatchSize, m.cfg.Events.LookupConcurrency)
		if err != nil {
			return nil, err
		}
		uidMap.Merge(uidLookup)

		// Fetch full details for this chunk
		records, err := m.fetchEventDetails(ctx, chunk)
		if err != nil {
			return nil, err
		}

		// Perform upsert for this chunk
		if err := m.upsertEvents(ctx, records, uidMap); err != nil {
			return nil, err
		}
	}

	return &eventSyncResult{
		UniqueIDs: allUniqueIDs,
		UIDs:      uidMap,
	}, nil
}

func (m *Migrator) fetchEventDetails(ctx context.Context, ids []string) ([]eventRecord, error) {
	rows, err := m.db.QueryRows(ctx, eventDetailsQuery, ids)
	if err != nil {
		return nil, fmt.Errorf("query event details: %w", err)
	}
	defer rows.Close()

	records := make([]eventRecord, 0, len(ids))
	for rows.Next() {
		var raw struct {
			UniqueID                    sql.NullString
			ClientID                    sql.NullString
			BLNumber                    sql.NullString
			SalesOrder                  sql.NullString
			ProductID                   sql.NullString
			ProductName                 sql.NullString
			Plant                       sql.NullString
			ProductionLabel             sql.NullString
			ProcessDate                 sql.NullInt64
			MaterialType                sql.NullString
			MaterialCode                sql.NullString
			Amount                      sql.NullFloat64
			Unit                        sql.NullString
			SrcPath                     sql.NullString
			SubDataSource               sql.NullString
			Version                     sql.NullString
			Flag                        sql.NullString
			MaterialDocument            sql.NullString
			PurchaseOrder               sql.NullString
			UMLID                       sql.NullString
			MillID                      sql.NullString
			LocationID                  sql.NullString
			Vendor                      sql.NullString
			Source                      sql.NullString
			LocationSubjectType         sql.NullString
			LocationMasterID            sql.NullString
			LocationMasterType          sql.NullString
			LocationFacilitySubjectID   sql.NullString
			LocationFacilitySubjectType sql.NullString
			LocationFacilityMasterID    sql.NullString
			LocationFacilityMasterType  sql.NullString
			SourceSubjectType           sql.NullString
			SourceMasterID              sql.NullString
			SourceMasterType            sql.NullString
			SourceFacilitySubjectID     sql.NullString
			SourceFacilitySubjectType   sql.NullString
			SourceFacilityMasterID      sql.NullString
			SourceFacilityMasterType    sql.NullString
			MovementType                sql.NullString
			MovementTypeText            sql.NullString
			StorageLocation             sql.NullString
			PartnerPCA                  sql.NullString
			MillName                    sql.NullString
		}

		if scanErr := rows.Scan(
			&raw.UniqueID,
			&raw.ClientID,
			&raw.BLNumber,
			&raw.SalesOrder,
			&raw.ProductID,
			&raw.ProductName,
			&raw.Plant,
			&raw.ProductionLabel,
			&raw.ProcessDate,
			&raw.MaterialType,
			&raw.MaterialCode,
			&raw.Amount,
			&raw.Unit,
			&raw.SrcPath,
			&raw.SubDataSource,
			&raw.Version,
			&raw.Flag,
			&raw.MaterialDocument,
			&raw.PurchaseOrder,
			&raw.UMLID,
			&raw.MillID,
			&raw.LocationID,
			&raw.Vendor,
			&raw.Source,
			&raw.LocationSubjectType,
			&raw.LocationMasterID,
			&raw.LocationMasterType,
			&raw.LocationFacilitySubjectID,
			&raw.LocationFacilitySubjectType,
			&raw.LocationFacilityMasterID,
			&raw.LocationFacilityMasterType,
			&raw.SourceSubjectType,
			&raw.SourceMasterID,
			&raw.SourceMasterType,
			&raw.SourceFacilitySubjectID,
			&raw.SourceFacilitySubjectType,
			&raw.SourceFacilityMasterID,
			&raw.SourceFacilityMasterType,
			&raw.MovementType,
			&raw.MovementTypeText,
			&raw.StorageLocation,
			&raw.PartnerPCA,
			&raw.MillName,
		); scanErr != nil {
			return nil, fmt.Errorf("scan event detail row: %w", scanErr)
		}

		uniqueID := m.norm.String(raw.UniqueID)
		if uniqueID == "" {
			continue
		}

		var processDatePtr *int64
		if v, ok := m.norm.Int64(raw.ProcessDate); ok {
			processDate := v
			processDatePtr = &processDate
		}

		var amountPtr *float64
		if v, ok := m.norm.Float64(raw.Amount); ok {
			amount := v
			amountPtr = &amount
		}

		records = append(records, eventRecord{
			UniqueID:                    uniqueID,
			ClientID:                    m.norm.String(raw.ClientID),
			BLNumber:                    m.norm.String(raw.BLNumber),
			SalesOrder:                  m.norm.String(raw.SalesOrder),
			ProductID:                   m.norm.String(raw.ProductID),
			ProductName:                 m.norm.String(raw.ProductName),
			Plant:                       m.norm.String(raw.Plant),
			ProductionLabel:             m.norm.String(raw.ProductionLabel),
			ProcessDate:                 processDatePtr,
			MaterialType:                m.norm.String(raw.MaterialType),
			MaterialCode:                m.norm.String(raw.MaterialCode),
			Amount:                      amountPtr,
			Unit:                        m.norm.String(raw.Unit),
			SrcPath:                     m.norm.String(raw.SrcPath),
			SubDataSource:               m.norm.String(raw.SubDataSource),
			Version:                     m.norm.String(raw.Version),
			Flag:                        m.norm.String(raw.Flag),
			MaterialDocument:            m.norm.String(raw.MaterialDocument),
			PurchaseOrder:               m.norm.String(raw.PurchaseOrder),
			UMLID:                       m.norm.String(raw.UMLID),
			MillID:                      m.norm.String(raw.MillID),
			LocationID:                  m.norm.String(raw.LocationID),
			Vendor:                      m.norm.String(raw.Vendor),
			Source:                      m.norm.String(raw.Source),
			LocationSubjectType:         m.norm.String(raw.LocationSubjectType),
			LocationMasterID:            m.norm.String(raw.LocationMasterID),
			LocationMasterType:          m.norm.String(raw.LocationMasterType),
			LocationFacilitySubjectID:   m.norm.String(raw.LocationFacilitySubjectID),
			LocationFacilitySubjectType: m.norm.String(raw.LocationFacilitySubjectType),
			LocationFacilityMasterID:    m.norm.String(raw.LocationFacilityMasterID),
			LocationFacilityMasterType:  m.norm.String(raw.LocationFacilityMasterType),
			SourceSubjectType:           m.norm.String(raw.SourceSubjectType),
			SourceMasterID:              m.norm.String(raw.SourceMasterID),
			SourceMasterType:            m.norm.String(raw.SourceMasterType),
			SourceFacilitySubjectID:     m.norm.String(raw.SourceFacilitySubjectID),
			SourceFacilitySubjectType:   m.norm.String(raw.SourceFacilitySubjectType),
			SourceFacilityMasterID:      m.norm.String(raw.SourceFacilityMasterID),
			SourceFacilityMasterType:    m.norm.String(raw.SourceFacilityMasterType),
			MovementType:                m.norm.String(raw.MovementType),
			MovementTypeText:            m.norm.String(raw.MovementTypeText),
			StorageLocation:             m.norm.String(raw.StorageLocation),
			PartnerPCA:                  m.norm.String(raw.PartnerPCA),
			MillName:                    m.norm.String(raw.MillName),
		})
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate event detail rows: %w", err)
	}

	return records, nil
}

func (m *Migrator) upsertEvents(ctx context.Context, records []eventRecord, uids *uidMap) error {
	if len(records) == 0 {
		return nil
	}

	batches := make([][]eventRecord, 0)
	for _, idx := range chunkIndices(len(records), m.cfg.Events.UpsertBatchSize) {
		segment := records[idx[0]:idx[1]]
		if len(segment) > 0 {
			copy := append([]eventRecord(nil), segment...)
			batches = append(batches, copy)
		}
	}

	return fanOutBatches(ctx, batches, m.cfg.Events.UpsertConcurrency, func(ctx context.Context, batch []eventRecord) error {
		var setBuilder strings.Builder
		var delBuilder strings.Builder
		blankToID := make(map[string]string)

		for _, record := range batch {
			if record.UniqueID == "" {
				continue
			}

			subject := ""
			if uid, ok := uids.Get(record.UniqueID); ok && uid != "" {
				subject = fmt.Sprintf("<%s>", uid)
			} else {
				blank := common.BlankNode("event", record.UniqueID)
				subject = blank
				blankToID[blankName(blank)] = record.UniqueID
			}

			common.AppendTriple(&setBuilder, subject, "<dgraph.type>", common.StringLiteral("Event"))
			common.AppendTriple(&setBuilder, subject, "<unique_id>", common.StringLiteral(record.UniqueID))

			if v := record.ClientID; v != "" {
				common.AppendTriple(&setBuilder, subject, "<client_id>", common.StringLiteral(v))
			} else {
				common.AppendTriple(&delBuilder, subject, "<client_id>", "*")
			}
			if v := record.BLNumber; v != "" {
				common.AppendTriple(&setBuilder, subject, "<bl_number>", common.StringLiteral(v))
			} else {
				common.AppendTriple(&delBuilder, subject, "<bl_number>", "*")
			}
			if v := record.SalesOrder; v != "" {
				common.AppendTriple(&setBuilder, subject, "<sales_order>", common.StringLiteral(v))
			} else {
				common.AppendTriple(&delBuilder, subject, "<sales_order>", "*")
			}
			if v := record.ProductID; v != "" {
				common.AppendTriple(&setBuilder, subject, "<product_id>", common.StringLiteral(v))
			} else {
				common.AppendTriple(&delBuilder, subject, "<product_id>", "*")
			}
			if v := record.ProductName; v != "" {
				common.AppendTriple(&setBuilder, subject, "<product_name>", common.StringLiteral(v))
			} else {
				common.AppendTriple(&delBuilder, subject, "<product_name>", "*")
			}
			if v := record.Plant; v != "" {
				common.AppendTriple(&setBuilder, subject, "<plant>", common.StringLiteral(v))
			} else {
				common.AppendTriple(&delBuilder, subject, "<plant>", "*")
			}
			if v := record.ProductionLabel; v != "" {
				common.AppendTriple(&setBuilder, subject, "<production_label>", common.StringLiteral(v))
			} else {
				common.AppendTriple(&delBuilder, subject, "<production_label>", "*")
			}
			if v := record.MaterialType; v != "" {
				common.AppendTriple(&setBuilder, subject, "<material_type>", common.StringLiteral(v))
			} else {
				common.AppendTriple(&delBuilder, subject, "<material_type>", "*")
			}
			if v := record.MaterialCode; v != "" {
				common.AppendTriple(&setBuilder, subject, "<material_code>", common.StringLiteral(v))
			} else {
				common.AppendTriple(&delBuilder, subject, "<material_code>", "*")
			}
			if v := record.Unit; v != "" {
				common.AppendTriple(&setBuilder, subject, "<unit>", common.StringLiteral(v))
			} else {
				common.AppendTriple(&delBuilder, subject, "<unit>", "*")
			}
			if v := record.SrcPath; v != "" && v != "{}" {
				common.AppendTriple(&setBuilder, subject, "<src_path>", common.StringLiteral(v))
			} else {
				common.AppendTriple(&delBuilder, subject, "<src_path>", "*")
			}
			if v := record.SubDataSource; v != "" {
				common.AppendTriple(&setBuilder, subject, "<sub_data_source>", common.StringLiteral(v))
			} else {
				common.AppendTriple(&delBuilder, subject, "<sub_data_source>", "*")
			}
			if v := record.Version; v != "" {
				common.AppendTriple(&setBuilder, subject, "<version>", common.StringLiteral(v))
			} else {
				common.AppendTriple(&delBuilder, subject, "<version>", "*")
			}
			if v := record.Flag; v != "" {
				common.AppendTriple(&setBuilder, subject, "<flag>", common.StringLiteral(v))
			} else {
				common.AppendTriple(&delBuilder, subject, "<flag>", "*")
			}
			if v := record.MaterialDocument; v != "" {
				common.AppendTriple(&setBuilder, subject, "<material_document>", common.StringLiteral(v))
			} else {
				common.AppendTriple(&delBuilder, subject, "<material_document>", "*")
			}
			if v := record.PurchaseOrder; v != "" {
				common.AppendTriple(&setBuilder, subject, "<purchase_order>", common.StringLiteral(v))
			} else {
				common.AppendTriple(&delBuilder, subject, "<purchase_order>", "*")
			}
			if v := record.UMLID; v != "" {
				common.AppendTriple(&setBuilder, subject, "<uml_id>", common.StringLiteral(v))
			} else {
				common.AppendTriple(&delBuilder, subject, "<uml_id>", "*")
			}
			if v := record.MillID; v != "" {
				common.AppendTriple(&setBuilder, subject, "<mill_id>", common.StringLiteral(v))
			} else {
				common.AppendTriple(&delBuilder, subject, "<mill_id>", "*")
			}
			if v := record.LocationID; v != "" {
				common.AppendTriple(&setBuilder, subject, "<location_id>", common.StringLiteral(v))
			} else {
				common.AppendTriple(&delBuilder, subject, "<location_id>", "*")
			}
			if v := record.Vendor; v != "" {
				common.AppendTriple(&setBuilder, subject, "<vendor>", common.StringLiteral(v))
			} else {
				common.AppendTriple(&delBuilder, subject, "<vendor>", "*")
			}
			if v := record.Source; v != "" {
				common.AppendTriple(&setBuilder, subject, "<source>", common.StringLiteral(v))
			} else {
				common.AppendTriple(&delBuilder, subject, "<source>", "*")
			}
			if v := record.LocationSubjectType; v != "" {
				common.AppendTriple(&setBuilder, subject, "<location_subject_type>", common.StringLiteral(v))
			} else {
				common.AppendTriple(&delBuilder, subject, "<location_subject_type>", "*")
			}
			if v := record.LocationMasterID; v != "" {
				common.AppendTriple(&setBuilder, subject, "<location_master_id>", common.StringLiteral(v))
			} else {
				common.AppendTriple(&delBuilder, subject, "<location_master_id>", "*")
			}
			if v := record.LocationMasterType; v != "" {
				common.AppendTriple(&setBuilder, subject, "<location_master_type>", common.StringLiteral(v))
			} else {
				common.AppendTriple(&delBuilder, subject, "<location_master_type>", "*")
			}
			if v := record.LocationFacilitySubjectID; v != "" {
				common.AppendTriple(&setBuilder, subject, "<location_facility_subject_id>", common.StringLiteral(v))
			} else {
				common.AppendTriple(&delBuilder, subject, "<location_facility_subject_id>", "*")
			}
			if v := record.LocationFacilitySubjectType; v != "" {
				common.AppendTriple(&setBuilder, subject, "<location_facility_subject_type>", common.StringLiteral(v))
			} else {
				common.AppendTriple(&delBuilder, subject, "<location_facility_subject_type>", "*")
			}
			if v := record.LocationFacilityMasterID; v != "" {
				common.AppendTriple(&setBuilder, subject, "<location_facility_master_id>", common.StringLiteral(v))
			} else {
				common.AppendTriple(&delBuilder, subject, "<location_facility_master_id>", "*")
			}
			if v := record.LocationFacilityMasterType; v != "" {
				common.AppendTriple(&setBuilder, subject, "<location_facility_master_type>", common.StringLiteral(v))
			} else {
				common.AppendTriple(&delBuilder, subject, "<location_facility_master_type>", "*")
			}
			if v := record.SourceSubjectType; v != "" {
				common.AppendTriple(&setBuilder, subject, "<source_subject_type>", common.StringLiteral(v))
			} else {
				common.AppendTriple(&delBuilder, subject, "<source_subject_type>", "*")
			}
			if v := record.SourceMasterID; v != "" {
				common.AppendTriple(&setBuilder, subject, "<source_master_id>", common.StringLiteral(v))
			} else {
				common.AppendTriple(&delBuilder, subject, "<source_master_id>", "*")
			}
			if v := record.SourceMasterType; v != "" {
				common.AppendTriple(&setBuilder, subject, "<source_master_type>", common.StringLiteral(v))
			} else {
				common.AppendTriple(&delBuilder, subject, "<source_master_type>", "*")
			}
			if v := record.SourceFacilitySubjectID; v != "" {
				common.AppendTriple(&setBuilder, subject, "<source_facility_subject_id>", common.StringLiteral(v))
			} else {
				common.AppendTriple(&delBuilder, subject, "<source_facility_subject_id>", "*")
			}
			if v := record.SourceFacilitySubjectType; v != "" {
				common.AppendTriple(&setBuilder, subject, "<source_facility_subject_type>", common.StringLiteral(v))
			} else {
				common.AppendTriple(&delBuilder, subject, "<source_facility_subject_type>", "*")
			}
			if v := record.SourceFacilityMasterID; v != "" {
				common.AppendTriple(&setBuilder, subject, "<source_facility_master_id>", common.StringLiteral(v))
			} else {
				common.AppendTriple(&delBuilder, subject, "<source_facility_master_id>", "*")
			}
			if v := record.SourceFacilityMasterType; v != "" {
				common.AppendTriple(&setBuilder, subject, "<source_facility_master_type>", common.StringLiteral(v))
			} else {
				common.AppendTriple(&delBuilder, subject, "<source_facility_master_type>", "*")
			}
			if v := record.MovementType; v != "" {
				common.AppendTriple(&setBuilder, subject, "<movement_type>", common.StringLiteral(v))
			} else {
				common.AppendTriple(&delBuilder, subject, "<movement_type>", "*")
			}
			if v := record.MovementTypeText; v != "" {
				common.AppendTriple(&setBuilder, subject, "<movement_type_text>", common.StringLiteral(v))
			} else {
				common.AppendTriple(&delBuilder, subject, "<movement_type_text>", "*")
			}
			if v := record.StorageLocation; v != "" {
				common.AppendTriple(&setBuilder, subject, "<storage_location>", common.StringLiteral(v))
			} else {
				common.AppendTriple(&delBuilder, subject, "<storage_location>", "*")
			}
			if v := record.PartnerPCA; v != "" {
				common.AppendTriple(&setBuilder, subject, "<partner_pca>", common.StringLiteral(v))
			} else {
				common.AppendTriple(&delBuilder, subject, "<partner_pca>", "*")
			}
			if v := record.MillName; v != "" {
				common.AppendTriple(&setBuilder, subject, "<mill_name>", common.StringLiteral(v))
			} else {
				common.AppendTriple(&delBuilder, subject, "<mill_name>", "*")
			}

			if record.ProcessDate != nil {
				common.AppendTriple(&setBuilder, subject, "<process_date>", common.IntLiteral(*record.ProcessDate))
			} else {
				common.AppendTriple(&delBuilder, subject, "<process_date>", "*")
			}
			if record.Amount != nil {
				common.AppendTriple(&setBuilder, subject, "<amount>", common.FloatLiteral(*record.Amount))
			} else {
				common.AppendTriple(&delBuilder, subject, "<amount>", "*")
			}
		}

		if setBuilder.Len() == 0 && delBuilder.Len() == 0 {
			return nil
		}

		uidsMap, err := m.mutateSetAndDel(ctx, setBuilder.String(), delBuilder.String())
		if err != nil {
			return err
		}

		for key, uid := range uidsMap {
			if id, ok := blankToID[key]; ok {
				uids.Set(id, uid)
			}
		}
		return nil
	})
}
