package initial

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"runtime"

	"golang.org/x/sync/errgroup"

	"hara/traceability-dgraph-etl/internal/etl/common"
	"hara/traceability-dgraph-etl/pkg/database"
)

const defaultOutputDir = "initial_imports"

type Migrator struct {
	db            database.Adapter
	interner      *common.StringInterner
	outputDir     string
	workerCount   int
	chunkSize     int
	stampSyncTime bool
	log           *slog.Logger
}

type Option func(*Migrator)

func WithWorkerCount(count int) Option {
	return func(m *Migrator) {
		if count > 0 {
			m.workerCount = count
		}
	}
}

func WithChunkSize(size int) Option {
	return func(m *Migrator) {
		if size > 0 {
			m.chunkSize = size
		}
	}
}

// WithSyncTimeStamping makes Run stamp sync_time on the exported rows once the RDF is complete (sync_time.go). It is
// off unless set: the export alone never writes to PostgreSQL.
func WithSyncTimeStamping(enabled bool) Option {
	return func(m *Migrator) {
		m.stampSyncTime = enabled
	}
}

func WithLogger(logger *slog.Logger) Option {
	return func(m *Migrator) {
		if logger != nil {
			m.log = logger
		}
	}
}

func NewMigrator(db database.Adapter, outputDir string, opts ...Option) (*Migrator, error) {
	if db == nil {
		return nil, fmt.Errorf("database adapter is required")
	}

	if outputDir == "" {
		outputDir = defaultOutputDir
	}

	m := &Migrator{
		db:          db,
		interner:    common.NewStringInterner(),
		outputDir:   outputDir,
		workerCount: runtime.NumCPU(),
		chunkSize:   common.DefaultChunkSize(),
		log:         slog.Default(),
	}

	for _, opt := range opts {
		opt(m)
	}

	if m.workerCount <= 0 {
		m.workerCount = 1
	}

	if m.chunkSize <= 0 {
		m.chunkSize = common.DefaultChunkSize()
	}

	return m, nil
}

func (m *Migrator) Run(ctx context.Context) error {
	// Remove existing output directory if it exists
	if _, err := os.Stat(m.outputDir); err == nil {
		m.log.Info("removing existing output directory", slog.String("output", m.outputDir))
		if err := os.RemoveAll(m.outputDir); err != nil {
			return fmt.Errorf("remove existing output directory: %w", err)
		}
	}

	if err := os.MkdirAll(m.outputDir, 0o755); err != nil {
		return fmt.Errorf("create output directory: %w", err)
	}

	m.log.Info("starting initial migration", slog.String("output", m.outputDir), slog.Int("workers", m.workerCount), slog.Int("chunk_size", m.chunkSize), slog.Bool("stamp_sync_time", m.stampSyncTime))

	agg, err := newAggregator(m.outputDir, m.interner, m.chunkSize)
	if err != nil {
		return fmt.Errorf("init aggregator: %w", err)
	}

	if err := m.processEvents(ctx, agg); err != nil {
		closeErr := agg.Close()
		return errors.Join(err, closeErr)
	}

	if err := agg.Close(); err != nil {
		return fmt.Errorf("finalise event writers: %w", err)
	}

	if err := streamLinks(ctx, m.db, m.interner, m.outputDir, m.chunkSize); err != nil {
		return fmt.Errorf("stream links: %w", err)
	}

	// Both writers are closed, so every RDF file is complete on disk. Only now may the source rows be stamped.
	output := filepath.Clean(m.outputDir)
	m.log.Info("rdf export completed", slog.String("output", output))

	if !m.stampSyncTime {
		m.log.Info("sync_time stamping disabled: no source row was changed")
	} else if err := stampSyncTime(ctx, m.db, m.log); err != nil {
		return fmt.Errorf("the RDF export in %s is complete and kept, but stamping sync_time failed; "+
			"re-run the stamping statements (Dgraph REFACTOR_PLAN.md §9.0.1) before events_bc or links_bc change: %w",
			output, err)
	}

	m.log.Info("initial migration completed", slog.String("output", output))
	return nil
}

func (m *Migrator) processEvents(ctx context.Context, agg *aggregator) error {
	g, gctx := errgroup.WithContext(ctx)

	rowsCh := make(chan *EventRow, m.workerCount*4)

	fetcher := NewEventFetcher(m.db)

	g.Go(func() error {
		defer close(rowsCh)
		return fetcher.Stream(gctx, rowsCh)
	})

	for i := 0; i < m.workerCount; i++ {
		g.Go(func() error {
			return agg.Process(gctx, rowsCh)
		})
	}

	return g.Wait()
}
