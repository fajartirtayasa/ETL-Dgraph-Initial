package incremental

import (
	"context"
	"fmt"
	"strings"

	"hara/traceability-dgraph-etl/pkg/database"
)

const syncBatchSize = 200

func updateEventsSyncTime(ctx context.Context, db database.Adapter, ids []string) error {
	return updateSyncTime(ctx, db, "events_bc", "unique_id", ids)
}

func updateLinksSyncTime(ctx context.Context, db database.Adapter, ids []int64) error {
	return updateLinksSyncTimeInt64(ctx, db, ids)
}

func updateLinksSyncTimeInt64(ctx context.Context, db database.Adapter, ids []int64) error {
	if db == nil || len(ids) == 0 {
		return nil
	}

	for _, idx := range chunkIndices(len(ids), syncBatchSize) {
		chunk := ids[idx[0]:idx[1]]
		if len(chunk) == 0 {
			continue
		}

		placeholders := make([]string, len(chunk))
		args := make([]any, len(chunk))
		for i, id := range chunk {
			placeholders[i] = fmt.Sprintf("$%d", i+1)
			args[i] = id
		}

		query := fmt.Sprintf("UPDATE links_bc SET sync_time = NOW() WHERE id IN (%s) AND sync_time IS NULL", strings.Join(placeholders, ","))
		if _, err := db.Exec(ctx, query, args...); err != nil {
			return fmt.Errorf("update links_bc sync_time: %w", err)
		}
	}

	return nil
}

func updateSyncTime(ctx context.Context, db database.Adapter, table, column string, ids []string) error {
	if db == nil || len(ids) == 0 {
		return nil
	}

	for _, idx := range chunkIndices(len(ids), syncBatchSize) {
		chunk := ids[idx[0]:idx[1]]
		if len(chunk) == 0 {
			continue
		}

		placeholders := make([]string, len(chunk))
		args := make([]any, len(chunk))
		for i, id := range chunk {
			placeholders[i] = fmt.Sprintf("$%d", i+1)
			args[i] = id
		}

		query := fmt.Sprintf("UPDATE %s SET sync_time = NOW() WHERE %s IN (%s) AND sync_time IS NULL", table, column, strings.Join(placeholders, ","))
		if _, err := db.Exec(ctx, query, args...); err != nil {
			return fmt.Errorf("update %s sync_time: %w", table, err)
		}
	}

	return nil
}
