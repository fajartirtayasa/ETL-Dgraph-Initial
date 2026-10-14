package incremental

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"time"

	"hara/traceability-dgraph-etl/pkg/database"
)

const (
	// traceabilityStatusCheckInterval is the wait time between status checks
	traceabilityStatusCheckInterval = 15 * time.Minute
)

// TraceabilityStatus represents the status of traceability ETL
type TraceabilityStatus string

const (
	TraceabilityStatusRunning TraceabilityStatus = "Running"
	TraceabilityStatusDone    TraceabilityStatus = "Done"
)

// checkTraceabilityStatus checks if traceability ETL has completed for the given date.
// It queries the traceability_log_daily table to see if there's a running status
// for entry_date + 1 day matching the target date.
func checkTraceabilityStatus(ctx context.Context, db database.Adapter, targetDate time.Time, logger *slog.Logger) (TraceabilityStatus, error) {
	query := `
		SELECT
			CASE WHEN EXISTS (
				SELECT 0 FROM traceability_log_daily
				WHERE entry_date + interval '1 days' = $1::date
					AND status = 'Running'
			) THEN 'Running'
			ELSE 'Done'
			END AS status
	`

	var status string
	row := db.QueryRow(ctx, query, targetDate.Format("2006-01-02"))
	if err := row.Scan(&status); err != nil {
		if err == sql.ErrNoRows {
			logger.Warn("no traceability status found, assuming done", slog.String("date", targetDate.Format("2006-01-02")))
			return TraceabilityStatusDone, nil
		}
		return "", fmt.Errorf("check traceability status: %w", err)
	}

	return TraceabilityStatus(status), nil
}

// waitForTraceabilityCompletion waits until traceability ETL is done for the given date.
// It checks the status every 15 minutes until it's no longer "Running".
func waitForTraceabilityCompletion(ctx context.Context, db database.Adapter, targetDate time.Time, logger *slog.Logger) error {
	logger.Info("checking traceability ETL status", slog.String("date", targetDate.Format("2006-01-02")))

	for {
		status, err := checkTraceabilityStatus(ctx, db, targetDate, logger)
		if err != nil {
			return fmt.Errorf("check traceability status: %w", err)
		}

		if status != TraceabilityStatusRunning {
			logger.Info("traceability ETL is done, proceeding with incremental ETL", slog.String("date", targetDate.Format("2006-01-02")))
			return nil
		}

		logger.Info("traceability ETL is still running, waiting before checking again",
			slog.String("date", targetDate.Format("2006-01-02")),
			slog.Duration("wait_interval", traceabilityStatusCheckInterval))

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(traceabilityStatusCheckInterval):
			// Continue loop to check again
		}
	}
}

