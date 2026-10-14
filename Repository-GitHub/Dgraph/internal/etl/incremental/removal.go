package incremental

import (
	"context"
	"database/sql"
	"fmt"
)

const removalQuery = `WITH InvalidLinks AS (
    SELECT id FROM links_bc WHERE sync_time IS NULL AND status = 'invalid'
),
InvalidEvents AS (
    SELECT unique_id FROM events_bc WHERE sync_time IS NULL AND status = 'invalid'
)
SELECT id AS link_id, NULL::text AS unique_id FROM InvalidLinks
UNION ALL
SELECT NULL::bigint AS link_id, unique_id FROM InvalidEvents;`

type removalResult struct {
	LinkIDs   []int64
	UniqueIDs []string
}

func (m *Migrator) removeInvalidEvents(ctx context.Context) (*removalResult, error) {
	rows, err := m.db.QueryRows(ctx, removalQuery)
	if err != nil {
		return nil, fmt.Errorf("query invalid events: %w", err)
	}
	defer rows.Close()

	linkIDSet := make(map[int64]struct{})
	ids := stringSet{}

	for rows.Next() {
		var linkID sql.NullInt64
		var uniqueID sql.NullString
		if scanErr := rows.Scan(&linkID, &uniqueID); scanErr != nil {
			return nil, fmt.Errorf("scan invalid event: %w", scanErr)
		}

		if linkID.Valid {
			linkIDSet[linkID.Int64] = struct{}{}
		}

		if normalized := m.norm.String(uniqueID); normalized != "" {
			ids.Add(normalized)
		}
	}

	linkIDs := make([]int64, 0, len(linkIDSet))
	for id := range linkIDSet {
		linkIDs = append(linkIDs, id)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate invalid events: %w", err)
	}

	uniqueIDs := ids.Values()
	if len(uniqueIDs) == 0 {
		return &removalResult{
			LinkIDs:   linkIDs,
			UniqueIDs: []string{},
		}, nil
	}

	uidMap, err := m.lookupEventUIDs(ctx, uniqueIDs, m.cfg.Removal.LookupBatchSize, m.cfg.Removal.LookupConcurrency)
	if err != nil {
		return nil, err
	}

	uids := make([]string, 0, len(uidMap))
	for _, uid := range uidMap {
		uids = append(uids, uid)
	}

	if err := m.deleteNodes(ctx, uids, m.cfg.Removal.DeleteBatchSize, m.cfg.Removal.DeleteConcurrency); err != nil {
		return nil, err
	}

	return &removalResult{
		LinkIDs:   linkIDs,
		UniqueIDs: uniqueIDs,
	}, nil
}
