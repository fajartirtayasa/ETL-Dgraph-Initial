package initial

import (
	"context"
	"database/sql"
	"fmt"

	"hara/traceability-dgraph-etl/pkg/database"
)

const eventsQuery = `
WITH UnsyncedLinks AS (
		SELECT l.event1_id, l.event2_id
		FROM links_bc l
		JOIN events_bc e1 ON l.event1_id = e1.unique_id AND e1.status = 'valid'
		JOIN events_bc e2 ON l.event2_id = e2.unique_id AND e2.status = 'valid'
		WHERE l.status = 'valid'
),
UnsyncedUniqueIDs AS (
    SELECT event1_id AS unique_id FROM UnsyncedLinks
    UNION
    SELECT event2_id AS unique_id FROM UnsyncedLinks
),
MasterMill AS (
    SELECT uml_id, mill_id FROM master_mill GROUP BY uml_id, mill_id
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
JOIN UnsyncedUniqueIDs u ON u.unique_id = e.unique_id
LEFT JOIN subjects loc ON loc.subject_id = e.location_id
LEFT JOIN mv_subject_hierarchy loc_fac ON loc_fac.start_subject_id = e.location_id
LEFT JOIN subjects src ON src.subject_id = e.source
LEFT JOIN mv_subject_hierarchy src_fac ON src_fac.start_subject_id = e.source
LEFT JOIN MasterMill mm ON mm.uml_id = e.uml_id;
`

// EventRow represents a single row returned from the events streaming query.
type EventRow struct {
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

type EventFetcher struct {
	db database.Adapter
}

func NewEventFetcher(db database.Adapter) *EventFetcher {
	return &EventFetcher{db: db}
}

func (f *EventFetcher) Stream(ctx context.Context, out chan<- *EventRow) error {
	rows, err := f.db.QueryRows(ctx, eventsQuery)
	if err != nil {
		return fmt.Errorf("query events: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		record := &EventRow{}
		if err := rows.Scan(
			&record.UniqueID,
			&record.ClientID,
			&record.BLNumber,
			&record.SalesOrder,
			&record.ProductID,
			&record.ProductName,
			&record.Plant,
			&record.ProductionLabel,
			&record.ProcessDate,
			&record.MaterialType,
			&record.MaterialCode,
			&record.Amount,
			&record.Unit,
			&record.SrcPath,
			&record.SubDataSource,
			&record.Version,
			&record.Flag,
			&record.MaterialDocument,
			&record.PurchaseOrder,
			&record.UMLID,
			&record.MillID,
			&record.LocationID,
			&record.Vendor,
			&record.Source,
			&record.LocationSubjectType,
			&record.LocationMasterID,
			&record.LocationMasterType,
			&record.LocationFacilitySubjectID,
			&record.LocationFacilitySubjectType,
			&record.LocationFacilityMasterID,
			&record.LocationFacilityMasterType,
			&record.SourceSubjectType,
			&record.SourceMasterID,
			&record.SourceMasterType,
			&record.SourceFacilitySubjectID,
			&record.SourceFacilitySubjectType,
			&record.SourceFacilityMasterID,
			&record.SourceFacilityMasterType,
			&record.MovementType,
			&record.MovementTypeText,
			&record.StorageLocation,
			&record.PartnerPCA,
			&record.MillName,
		); err != nil {
			return fmt.Errorf("scan event row: %w", err)
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case out <- record:
		}
	}

	if err := rows.Err(); err != nil {
		return fmt.Errorf("iterate event rows: %w", err)
	}

	return nil
}
