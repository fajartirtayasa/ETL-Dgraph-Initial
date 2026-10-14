package database

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/jmoiron/sqlx"
)

type databaseReplica struct {
	db *sqlx.DB
}

// NewPostgres initialize single Postgres db
func NewDatabaseConnectionReplica(driver DatabaseDriver, config *Config) (AdapterReplica, error) {
	switch driver {
	case Postgres:
		db, err := NewConnectionPostgres(config)
		if err != nil {
			return nil, err
		}

		return &databaseReplica{
			db: db,
		}, nil

	default:
		return nil, fmt.Errorf("unsupported database driver: %s", driver)
	}
}

// QueryRow selects a single row. It's a direct pass-through to sqlx.
func (r *databaseReplica) QueryRow(ctx context.Context, query string, args ...any) *sql.Row {
	return r.db.QueryRowContext(ctx, query, args...)
}

// QueryRows selects multiple rows. It's a direct pass-through to sqlx.
func (r *databaseReplica) QueryRows(ctx context.Context, query string, args ...any) (*sql.Rows, error) {
	return r.db.QueryContext(ctx, query, args...)
}

// Fetch selects multiple rows and scans them into a slice of structs.
func (r *databaseReplica) Fetch(ctx context.Context, target any, query string, args ...any) error {
	return r.db.SelectContext(ctx, target, query, args...)
}

// FetchRow selects a single row and scans it into a struct.
func (r *databaseReplica) FetchRow(ctx context.Context, target any, query string, args ...any) error {
	return r.db.GetContext(ctx, target, query, args...)
}

// Ping checks database connectivity.
func (r *databaseReplica) Ping(ctx context.Context) error {
	return r.db.PingContext(ctx)
}

// HealthCheck provides a simple, context-free way to check database health.
func (r *databaseReplica) HealthCheck() error {
	return r.Ping(context.Background())
}

// Close releases the underlying replica database connection pool.
func (r *databaseReplica) Close() error {
	if r.db == nil {
		return nil
	}
	return r.db.Close()
}
