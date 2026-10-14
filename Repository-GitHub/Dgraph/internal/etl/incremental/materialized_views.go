package incremental

import (
	"context"
	"fmt"
	"log/slog"

	"hara/traceability-dgraph-etl/pkg/database"
)

const (
	// Materialized views that need to be refreshed after ETL
	mvEvents                   = "mv_events"
	mvMonitoringCacheShipments = "monitoring_cache_shipments"
	mvShipmentList             = "mv_shipment_list"
)

type materializedView struct {
	name         string
	isConcurrent bool
}

func refreshMaterializedViews(ctx context.Context, db database.Adapter, logger *slog.Logger) error {
	views := []materializedView{
		{name: mvEvents, isConcurrent: true},
		{name: mvMonitoringCacheShipments, isConcurrent: false},
		{name: mvShipmentList, isConcurrent: false},
	}

	for _, view := range views {
		if err := refreshMaterializedView(ctx, db, view, logger); err != nil {
			return fmt.Errorf("refresh materialized view %s: %w", view.name, err)
		}
	}

	logger.Info("all materialized views refreshed successfully")
	return nil
}

// refreshMaterializedView refreshes a single materialized view.
func refreshMaterializedView(ctx context.Context, db database.Adapter, view materializedView, logger *slog.Logger) error {
	query := fmt.Sprintf("REFRESH MATERIALIZED VIEW %s", view.name)
	if view.isConcurrent {
		query = fmt.Sprintf("REFRESH MATERIALIZED VIEW CONCURRENTLY %s", view.name)
	}

	logger.Info("refreshing materialized view", slog.String("view", view.name), slog.Bool("concurrent", view.isConcurrent))

	if _, err := db.Exec(ctx, query); err != nil {
		return fmt.Errorf("execute refresh for %s: %w", view.name, err)
	}

	logger.Info("materialized view refreshed", slog.String("view", view.name))
	return nil
}
