package incremental

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"hara/traceability-dgraph-etl/internal/etl/common"
	"hara/traceability-dgraph-etl/pkg/database"
	"hara/traceability-dgraph-etl/pkg/dgraph"
)

type Migrator struct {
	db    database.Adapter
	graph dgraph.Adapter
	cfg   Config
	log   *slog.Logger
	norm  *normalizer
}

func NewMigrator(db database.Adapter, graph dgraph.Adapter, cfg Config, logger *slog.Logger) (*Migrator, error) {
	if db == nil {
		return nil, fmt.Errorf("database adapter is required")
	}
	if graph == nil {
		return nil, fmt.Errorf("dgraph adapter is required")
	}

	cfg.ApplyDefaults()

	if logger == nil {
		logger = slog.Default()
	}

	return &Migrator{
		db:    db,
		graph: graph,
		cfg:   cfg,
		log:   logger,
		norm:  newNormalizer(common.NewStringInterner()),
	}, nil
}

func (m *Migrator) Run(ctx context.Context) error {
	m.log.Info("starting incremental migration")

	// Check traceability ETL status before proceeding
	// Use date 2 days ago in Jakarta timezone to check if traceability ETL is complete
	jakartaTZ, err := time.LoadLocation("Asia/Jakarta")
	if err != nil {
		return fmt.Errorf("load Jakarta timezone: %w", err)
	}
	targetDate := time.Now().In(jakartaTZ).AddDate(0, 0, -2)
	if err := waitForTraceabilityCompletion(ctx, m.db, targetDate, m.log); err != nil {
		return fmt.Errorf("wait for traceability completion: %w", err)
	}

	removalResult, err := m.removeInvalidEvents(ctx)
	if err != nil {
		return fmt.Errorf("remove invalid events: %w", err)
	}
	m.log.Info("removed invalid events", slog.Int("unique_ids", len(removalResult.UniqueIDs)), slog.Int("link_ids", len(removalResult.LinkIDs)))

	eventResult, err := m.syncEvents(ctx)
	if err != nil {
		return fmt.Errorf("sync events: %w", err)
	}
	m.log.Info("synced events", slog.Int("count", len(eventResult.UniqueIDs)))

	linkIDs, err := m.syncLinks(ctx, eventResult)
	if err != nil {
		return fmt.Errorf("sync links: %w", err)
	}
	m.log.Info("synced links", slog.Int("count", len(linkIDs)))

	if err := m.updateSyncTimes(ctx, removalResult, eventResult.UniqueIDs, linkIDs); err != nil {
		return fmt.Errorf("update sync times: %w", err)
	}

	m.log.Info("incremental migration completed")
	return nil
}

func (m *Migrator) updateSyncTimes(ctx context.Context, removalResult *removalResult, eventIDs []string, linkIDs []int64) error {
	allEvents := stringSet{}
	for _, id := range removalResult.UniqueIDs {
		allEvents.Add(id)
	}
	for _, id := range eventIDs {
		allEvents.Add(id)
	}

	allLinkIDs := make([]int64, 0, len(removalResult.LinkIDs)+len(linkIDs))
	allLinkIDs = append(allLinkIDs, removalResult.LinkIDs...)
	allLinkIDs = append(allLinkIDs, linkIDs...)

	var errs error
	if err := updateEventsSyncTime(ctx, m.db, allEvents.Values()); err != nil {
		errs = errors.Join(errs, err)
	}
	if err := updateLinksSyncTime(ctx, m.db, allLinkIDs); err != nil {
		errs = errors.Join(errs, err)
	}
	return errs
}

type eventSyncResult struct {
	UniqueIDs []string
	UIDs      *uidMap
}
