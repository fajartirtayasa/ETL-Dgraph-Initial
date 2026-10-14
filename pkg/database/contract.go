package database

import (
	"context"
	"database/sql"
)

type Config struct {
	Host              string
	Port              int
	Name              string
	User              string
	Password          string
	Schema            string
	MaxConnectionOpen int
	MaxConnectionIdle int
	MaxLifeTimeMS     int
	TimeoutSecond     int
	Charset           string
}

type DatabaseDriver string

const (
	Postgres DatabaseDriver = "postgres"
)

type Adapter interface {
	QueryRow(ctx context.Context, query string, args ...any) *sql.Row
	QueryRows(ctx context.Context, query string, args ...any) (rows *sql.Rows, err error)
	Fetch(ctx context.Context, target any, query string, args ...any) error
	FetchRow(ctx context.Context, target any, query string, args ...any) error
	BeginTx(ctx context.Context, opts *sql.TxOptions) (*sql.Tx, error)
	Exec(ctx context.Context, query string, args ...any) (sql.Result, error)
	Ping(ctx context.Context) error
	HealthCheck() error
	Close() error
}

type AdapterReplica interface {
	QueryRow(ctx context.Context, query string, args ...any) *sql.Row
	QueryRows(ctx context.Context, query string, args ...any) (rows *sql.Rows, err error)
	Fetch(ctx context.Context, target any, query string, args ...any) error
	FetchRow(ctx context.Context, target any, query string, args ...any) error
	Ping(ctx context.Context) error
	HealthCheck() error
	Close() error
}

type Transaction interface {
	ExecContext(context.Context, string, ...any) (sql.Result, error)
	PrepareContext(context.Context, string) (*sql.Stmt, error)
	QueryContext(context.Context, string, ...any) (*sql.Rows, error)
	QueryRowContext(context.Context, string, ...any) *sql.Row
}
