package incremental

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"strconv"
	"strings"

	"hara/traceability-dgraph-etl/internal/etl/common"
)

const linkEvent1IDsQuery = `WITH UnsyncedLinks AS (
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
),

UnsyncedUniqueIDs AS (
    SELECT unique_id FROM ValidUnsyncedLinks
    UNION
    SELECT unique_id FROM UnsyncedEvents
    UNION
    SELECT l.event1_id AS unique_id 
    FROM links_bc l 
    WHERE l.event2_id IN (SELECT unique_id FROM UnsyncedEvents)
)

SELECT DISTINCT l.event1_id FROM links_bc l
WHERE l.event1_id IN (SELECT unique_id FROM UnsyncedUniqueIDs);`

const linkDetailsQuery = `
SELECT
    l.id,
    l.event1_id,
    l.event2_id,
    l.event1_version,
    l.event2_version,
    l.role,
    l.qty,
    l.unit,
    l.source,
    l.status,
    e2.status AS event2_status
FROM
    links_bc l
LEFT JOIN events_bc e2 ON l.event2_id = e2.unique_id
WHERE l.event1_id = ANY($1);`

type linkRecord struct {
	ID            string
	Event1ID      string
	Event2ID      string
	Event1Version string
	Event2Version string
	Role          string
	Qty           *float64
	Unit          string
	Source        string
}

// linkChunk holds the links fetched for one chunk of event1 IDs.
type linkChunk struct {
	// records contains only valid links whose event2 is valid.
	records []linkRecord
	// linkIDs contains every fetched link ID, valid or not, for sync_time updates.
	linkIDs []int64
	// event1IDs contains every event1 seen in the chunk, valid link or not.
	event1IDs stringSet
	// missingUIDs contains event IDs that are not yet in the UID map.
	missingUIDs stringSet
}

func (m *Migrator) syncLinks(ctx context.Context, events *eventSyncResult) ([]int64, error) {
	rows, err := m.db.QueryRows(ctx, linkEvent1IDsQuery)
	if err != nil {
		return nil, fmt.Errorf("query link event1 ids: %w", err)
	}
	defer rows.Close()

	event1IDSet := stringSet{}
	for rows.Next() {
		var id sql.NullString
		if err := rows.Scan(&id); err != nil {
			return nil, fmt.Errorf("scan link event1 id: %w", err)
		}
		event1IDSet.Add(m.norm.String(id))
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate link event1 ids: %w", err)
	}

	allEvent1IDs := event1IDSet.Values()
	if len(allEvent1IDs) == 0 {
		return []int64{}, nil
	}

	m.log.Info("syncing links in chunks", slog.Int("total_event1", len(allEvent1IDs)), slog.Int("chunk_size", m.cfg.FetchChunkSize))

	// Chunks are split by event1_id so every link of an event1 is handled in a
	// single chunk; otherwise a later chunk would wipe links written earlier.
	allLinkIDs := make([]int64, 0)
	handledEvent1IDs := stringSet{}

	for i, idx := range chunkIndices(len(allEvent1IDs), m.cfg.FetchChunkSize) {
		chunk := allEvent1IDs[idx[0]:idx[1]]
		m.log.Debug("processing link chunk", slog.Int("chunk_index", i), slog.Int("size", len(chunk)))

		fetched, err := m.fetchLinkDetails(ctx, chunk, events.UIDs)
		if err != nil {
			return nil, err
		}

		allLinkIDs = append(allLinkIDs, fetched.linkIDs...)
		for e := range fetched.event1IDs {
			handledEvent1IDs.Add(e)
		}

		// Lookup missing event UIDs (e.g. event2_id that wasn't part of the unsynced events)
		if missing := fetched.missingUIDs.Values(); len(missing) > 0 {
			additional, err := m.lookupEventUIDs(ctx, missing, m.cfg.Links.LookupBatchSize, m.cfg.Links.LookupConcurrency)
			if err != nil {
				return nil, err
			}
			events.UIDs.Merge(additional)
		}

		// Delete existing links for every event1 in this chunk, including those
		// whose links all became invalid, then write back the valid ones.
		if err := m.deleteExistingLinks(ctx, events.UIDs, fetched.event1IDs); err != nil {
			return nil, err
		}

		if len(fetched.records) > 0 {
			if err := m.upsertLinks(ctx, fetched.records, events.UIDs); err != nil {
				return nil, err
			}
		}
	}

	// Final cleanup for synced events that are not event1 of any link
	if err := m.cleanupMissingLinks(ctx, events, handledEvent1IDs); err != nil {
		return nil, err
	}

	return allLinkIDs, nil
}

func (m *Migrator) fetchLinkDetails(ctx context.Context, event1IDs []string, uids *uidMap) (*linkChunk, error) {
	rows, err := m.db.QueryRows(ctx, linkDetailsQuery, event1IDs)
	if err != nil {
		return nil, fmt.Errorf("query link details: %w", err)
	}
	defer rows.Close()

	out := &linkChunk{
		records:     make([]linkRecord, 0, len(event1IDs)),
		linkIDs:     make([]int64, 0, len(event1IDs)),
		event1IDs:   stringSet{},
		missingUIDs: stringSet{},
	}

	for rows.Next() {
		var raw struct {
			ID            sql.NullInt64
			Event1ID      sql.NullString
			Event2ID      sql.NullString
			Event1Version sql.NullString
			Event2Version sql.NullString
			Role          sql.NullString
			Qty           sql.NullFloat64
			Unit          sql.NullString
			Source        sql.NullString
			Status        sql.NullString
			Event2Status  sql.NullString
		}

		if scanErr := rows.Scan(
			&raw.ID,
			&raw.Event1ID,
			&raw.Event2ID,
			&raw.Event1Version,
			&raw.Event2Version,
			&raw.Role,
			&raw.Qty,
			&raw.Unit,
			&raw.Source,
			&raw.Status,
			&raw.Event2Status,
		); scanErr != nil {
			return nil, fmt.Errorf("scan link detail row: %w", scanErr)
		}

		if !raw.ID.Valid {
			continue
		}
		out.linkIDs = append(out.linkIDs, raw.ID.Int64)

		event1 := m.norm.String(raw.Event1ID)
		if event1 == "" {
			continue
		}

		out.event1IDs.Add(event1)
		if _, ok := uids.Get(event1); !ok {
			out.missingUIDs.Add(event1)
		}

		event2 := m.norm.String(raw.Event2ID)
		if event2 == "" {
			continue
		}

		status := m.norm.String(raw.Status)
		event2Status := m.norm.String(raw.Event2Status)
		if status != "valid" || event2Status != "valid" {
			continue
		}

		if _, ok := uids.Get(event2); !ok {
			out.missingUIDs.Add(event2)
		}

		var qtyPtr *float64
		if v, ok := m.norm.Float64(raw.Qty); ok {
			qty := v
			qtyPtr = &qty
		}

		out.records = append(out.records, linkRecord{
			ID:            fmt.Sprintf("%d", raw.ID.Int64),
			Event1ID:      event1,
			Event2ID:      event2,
			Event1Version: m.norm.String(raw.Event1Version),
			Event2Version: m.norm.String(raw.Event2Version),
			Role:          m.norm.String(raw.Role),
			Qty:           qtyPtr,
			Unit:          m.norm.String(raw.Unit),
			Source:        m.norm.String(raw.Source),
		})
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate link detail rows: %w", err)
	}

	return out, nil
}

func (m *Migrator) deleteExistingLinks(ctx context.Context, uids *uidMap, deleteEvent1IDs stringSet) error {
	// Collect unique event1 UIDs that already exist in Dgraph (exclude new nodes)
	// Only delete links from event1_id since links are directional from event1 to event2
	event1UIDSet := make(map[string]struct{})
	for event1ID := range deleteEvent1IDs {
		if uid, ok := uids.Get(event1ID); ok && uid != "" {
			event1UIDSet[uid] = struct{}{}
		}
	}

	if len(event1UIDSet) == 0 {
		return nil
	}

	event1UIDs := make([]string, 0, len(event1UIDSet))
	for uid := range event1UIDSet {
		event1UIDs = append(event1UIDs, uid)
	}

	batches := make([][]string, 0)
	for _, idx := range chunkIndices(len(event1UIDs), m.cfg.Links.UpsertBatchSize) {
		segment := event1UIDs[idx[0]:idx[1]]
		if len(segment) > 0 {
			copy := append([]string(nil), segment...)
			batches = append(batches, copy)
		}
	}

	return fanOutBatches(ctx, batches, m.cfg.Links.UpsertConcurrency, func(ctx context.Context, batch []string) error {
		rdf := buildCleanupRDF(batch)
		if rdf == "" {
			return nil
		}
		err := m.graph.DeleteRDF(ctx, rdf)
		if err != nil {
			return fmt.Errorf("delete existing links: %w", err)
		}
		return nil
	})
}

func (m *Migrator) upsertLinks(ctx context.Context, records []linkRecord, uids *uidMap) error {
	batches := make([][]linkRecord, 0)
	for _, idx := range chunkIndices(len(records), m.cfg.Links.UpsertBatchSize) {
		segment := records[idx[0]:idx[1]]
		if len(segment) > 0 {
			copy := append([]linkRecord(nil), segment...)
			batches = append(batches, copy)
		}
	}

	return fanOutBatches(ctx, batches, m.cfg.Links.UpsertConcurrency, func(ctx context.Context, batch []linkRecord) error {
		var builder strings.Builder

		for _, record := range batch {
			event1UID, ok1 := uids.Get(record.Event1ID)
			event2UID, ok2 := uids.Get(record.Event2ID)
			if !ok1 || !ok2 || event1UID == "" || event2UID == "" {
				continue
			}

			subject := fmt.Sprintf("<%s>", event1UID)
			object := fmt.Sprintf("<%s>", event2UID)
			facets := buildLinkFacets(record)

			builder.WriteString(subject)
			builder.WriteString(" <links> ")
			builder.WriteString(object)
			if facets != "" {
				builder.WriteByte(' ')
				builder.WriteByte('(')
				builder.WriteString(facets)
				builder.WriteByte(')')
			}
			builder.WriteString(" .\n")
		}

		if builder.Len() == 0 {
			return nil
		}

		_, err := m.mutateSet(ctx, builder.String())
		return err
	})
}

func (m *Migrator) cleanupMissingLinks(ctx context.Context, events *eventSyncResult, handled stringSet) error {
	eventUIDs := make([]string, 0)
	for _, id := range events.UniqueIDs {
		if _, ok := handled[id]; ok {
			continue
		}
		if uid, ok := events.UIDs.Get(id); ok && uid != "" {
			eventUIDs = append(eventUIDs, uid)
		}
	}

	if len(eventUIDs) == 0 {
		return nil
	}

	batches := make([][]string, 0)
	for _, idx := range chunkIndices(len(eventUIDs), m.cfg.Links.CleanupBatchSize) {
		segment := eventUIDs[idx[0]:idx[1]]
		if len(segment) > 0 {
			copy := append([]string(nil), segment...)
			batches = append(batches, copy)
		}
	}

	return fanOutBatches(ctx, batches, m.cfg.Links.CleanupConcurrency, func(ctx context.Context, batch []string) error {
		rdf := buildCleanupRDF(batch)
		if rdf == "" {
			return nil
		}
		err := m.graph.DeleteRDF(ctx, rdf)
		if err != nil {
			return fmt.Errorf("cleanup links: %w", err)
		}
		return nil
	})
}

func buildLinkFacets(record linkRecord) string {
	facets := make([]string, 0, 6)

	if record.Event1Version != "" {
		facets = append(facets, fmt.Sprintf("event1_version=%s", common.StringLiteral(record.Event1Version)))
	}
	if record.Event2Version != "" {
		facets = append(facets, fmt.Sprintf("event2_version=%s", common.StringLiteral(record.Event2Version)))
	}
	if record.Role != "" {
		facets = append(facets, fmt.Sprintf("role=%s", common.StringLiteral(record.Role)))
	}
	if record.Qty != nil {
		facets = append(facets, fmt.Sprintf("qty=%s", strconv.FormatFloat(*record.Qty, 'f', -1, 64)))
	}
	if record.Unit != "" {
		facets = append(facets, fmt.Sprintf("unit=%s", common.StringLiteral(record.Unit)))
	}
	if record.Source != "" {
		facets = append(facets, fmt.Sprintf("source=%s", common.StringLiteral(record.Source)))
	}

	return strings.Join(facets, ", ")
}
