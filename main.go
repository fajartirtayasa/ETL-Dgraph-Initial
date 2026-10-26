package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"

	"hara/traceability-dgraph-etl/internal/app"
	"hara/traceability-dgraph-etl/internal/etl"
	"hara/traceability-dgraph-etl/pkg/database"
	applogger "hara/traceability-dgraph-etl/pkg/logger"

	"github.com/joho/godotenv"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintf(os.Stderr, "etl failed: %v\n", err)
		os.Exit(1)
	}
}

func run() error {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	if err := godotenv.Load(); err != nil {
		fmt.Println("No .env file found")
	}

	cfg, err := app.NewConfig()
	if err != nil {
		return fmt.Errorf("load config: %w", err)
	}

	logger := setupLogger(cfg.Logger)

	primaryConfig := toDatabaseConfig(cfg.DBClean)
	if primaryConfig == nil {
		return fmt.Errorf("primary database config is missing")
	}

	primaryDB, err := database.NewDatabaseConnection(database.Postgres, primaryConfig)
	if err != nil {
		return fmt.Errorf("connect primary database: %w", err)
	}
	defer func() {
		if closeErr := primaryDB.Close(); closeErr != nil {
			logger.Error("failed to close database", slog.Any("error", closeErr))
		}
	}()

	appMode := ""
	if cfg.App != nil {
		appMode = cfg.App.Mode
	}
	mode := etl.ParseMode(appMode)

	var workerCount, chunkSize int
	if cfg.Initial != nil {
		workerCount = cfg.Initial.WorkerCount
		chunkSize = cfg.Initial.ChunkSize
	}

	runnerCfg := etl.RunnerConfig{
		Mode:        mode,
		OutputDir:   filepath.Join(".", "initial_imports"),
		WorkerCount: workerCount,
		ChunkSize:   chunkSize,
	}

	deps := etl.Dependencies{
		Primary: primaryDB,
		Logger:  logger,
	}

	if err := etl.Run(ctx, runnerCfg, deps); err != nil {
		logger.Error("etl execution failed", slog.Any("error", err))
		return err
	}

	logger.Info("etl execution completed successfully")
	return nil
}

func setupLogger(cfg *app.LoggerConfig) *slog.Logger {
	level := slog.LevelInfo
	if cfg != nil {
		switch strings.ToLower(strings.TrimSpace(cfg.Level)) {
		case "debug":
			level = slog.LevelDebug
		case "warn":
			level = slog.LevelWarn
		case "error":
			level = slog.LevelError
		}
	}

	levelVar := new(slog.LevelVar)
	levelVar.Set(level)

	handler := slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		Level: levelVar,
		ReplaceAttr: func(groups []string, attr slog.Attr) slog.Attr {
			if attr.Key == slog.MessageKey {
				attr.Key = applogger.MessageKey
			}
			return attr
		},
	})

	logger := slog.New(handler)
	slog.SetDefault(logger)
	return logger
}

func toDatabaseConfig(cfg *app.PostgresConfig) *database.Config {
	if cfg == nil {
		return nil
	}

	return &database.Config{
		Host:              cfg.Host,
		Port:              cfg.Port,
		Name:              cfg.Name,
		User:              cfg.User,
		Password:          cfg.Password,
		Schema:            cfg.Schema,
		MaxConnectionOpen: cfg.MaxConnectionOpen,
		MaxConnectionIdle: cfg.MaxConnectionIdle,
		MaxLifeTimeMS:     cfg.MaxLifeTimeMS,
		TimeoutSecond:     cfg.TimeoutSecond,
		Charset:           cfg.Charset,
	}
}
