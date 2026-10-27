package etl

import (
	"context"
	"fmt"
	"log/slog"

	"hara/traceability-dgraph-etl/internal/etl/initial"
	"hara/traceability-dgraph-etl/pkg/database"
)

type RunnerConfig struct {
	Mode          Mode
	OutputDir     string
	WorkerCount   int
	ChunkSize     int
	StampSyncTime bool
}

type Dependencies struct {
	Primary database.Adapter
	Logger  *slog.Logger
}

func Run(ctx context.Context, cfg RunnerConfig, deps Dependencies) error {
	if cfg.Mode != ModeInitial {
		return fmt.Errorf("unsupported ETL mode %q: only %q is supported", cfg.Mode, ModeInitial)
	}

	if deps.Primary == nil {
		return fmt.Errorf("primary database adapter is required")
	}

	logger := deps.Logger
	if logger == nil {
		logger = slog.Default()
	}

	options := []initial.Option{initial.WithLogger(logger), initial.WithSyncTimeStamping(cfg.StampSyncTime)}
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
}
