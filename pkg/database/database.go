package database

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/jmoiron/sqlx"
)

type database struct {
	db *sqlx.DB
}

// NewPostgres initialize single Postgres db
func NewDatabaseConnection(driver DatabaseDriver, config *Config) (Adapter, error) {
	switch driver {
	case Postgres:
		db, err := NewConnectionPostgres(config)
		if err != nil {
			return nil, err
		}

		return &database{
			db: db,
		}, nil

	default:
		return nil, fmt.Errorf("unsupported database driver: %s", driver)
	}
}

// QueryRow selects a single row. It's a direct pass-through to sqlx.
func (r *database) QueryRow(ctx context.Context, query string, args ...any) *sql.Row {
	return r.db.QueryRowContext(ctx, query, args...)
}

// QueryRows selects multiple rows. It's a direct pass-through to sqlx.
func (r *database) QueryRows(ctx context.Context, query string, args ...any) (*sql.Rows, error) {
	return r.db.QueryContext(ctx, query, args...)
}

// Fetch selects multiple rows and scans them into a slice of structs.
func (r *database) Fetch(ctx context.Context, target any, query string, args ...any) error {
	return r.db.SelectContext(ctx, target, query, args...)
}

// FetchRow selects a single row and scans it into a struct.
func (r *database) FetchRow(ctx context.Context, target any, query string, args ...any) error {
	return r.db.GetContext(ctx, target, query, args...)
}

// Exec executes a command query (INSERT, UPDATE, DELETE).
func (r *database) Exec(ctx context.Context, query string, args ...any) (sql.Result, error) {
	return r.db.ExecContext(ctx, query, args...)
}

// BeginTx starts a new transaction.
func (r *database) BeginTx(ctx context.Context, opts *sql.TxOptions) (*sql.Tx, error) {
	return r.db.BeginTx(ctx, opts)
}

// Ping checks database connectivity.
func (r *database) Ping(ctx context.Context) error {
	return r.db.PingContext(ctx)
}

// HealthCheck provides a simple, context-free way to check database health.
func (r *database) HealthCheck() error {
	return r.Ping(context.Background())
}

// Close releases the underlying database connection pool.
func (r *database) Close() error {
	if r.db == nil {
		return nil
	}
	return r.db.Close()
}
