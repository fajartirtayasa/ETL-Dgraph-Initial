package etl

import (
	"context"
	"fmt"
	"log/slog"

	"hara/traceability-dgraph-etl/internal/etl/incremental"
	"hara/traceability-dgraph-etl/internal/etl/initial"
	"hara/traceability-dgraph-etl/pkg/database"
	"hara/traceability-dgraph-etl/pkg/dgraph"
)

type RunnerConfig struct {
	Mode        Mode
	OutputDir   string
	WorkerCount int
	ChunkSize   int
	Incremental *incremental.Config
}

type Dependencies struct {
	Primary database.Adapter
	Graph   dgraph.Adapter
	Logger  *slog.Logger
}

func Run(ctx context.Context, cfg RunnerConfig, deps Dependencies) error {
	if deps.Primary == nil {
		return fmt.Errorf("primary database adapter is required")
	}

	logger := deps.Logger
	if logger == nil {
		logger = slog.Default()
	}

	switch cfg.Mode {
	case ModeInitial:
		options := []initial.Option{initial.WithLogger(logger)}
		if cfg.WorkerCount > 0 {
			options = append(options, initial.WithWorkerCount(cfg.WorkerCount))
		}
		if cfg.ChunkSize > 0 {
			options = append(options, initial.WithChunkSize(cfg.ChunkSize))
		}

		migrator, err := initial.NewMigrator(deps.Primary, cfg.OutputDir, options...)
		if err != nil {
			return fmt.Errorf("create initial migrator: %w", err)
		}

		return migrator.Run(ctx)

	case ModeIncremental:
		if deps.Graph == nil {
			return fmt.Errorf("dgraph adapter is required for incremental migration")
		}
		if cfg.Incremental == nil {
			cfg.Incremental = &incremental.Config{}
		}
		cfg.Incremental.ApplyDefaults()

		migrator, err := incremental.NewMigrator(deps.Primary, deps.Graph, *cfg.Incremental, logger)
		if err != nil {
			return fmt.Errorf("create incremental migrator: %w", err)
		}

		return migrator.Run(ctx)

	default:
		return fmt.Errorf("unknown ETL mode: %s", cfg.Mode)
	}
}
