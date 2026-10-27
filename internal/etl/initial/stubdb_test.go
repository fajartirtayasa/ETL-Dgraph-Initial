package initial

// stubDB stands in for database.Adapter in tests that need *sql.Rows without a server.
// database.Adapter.QueryRows returns a concrete *sql.Rows, so the stub is a minimal database/sql driver
// (connector, conn, rows) whose rows come from a Go function. Exec is answered by an optional second function
// (withExec) and fails without one; transactions and prepared statements are not supported.
//
// Ported from internal/etl/incremental/parity_stubdb_test.go (blob 396b66d, last changed in 79a38f0,
// removed with the incremental mode), without the incremental-only helpers stringsArg and nullable.

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"io"

	"hara/traceability-dgraph-etl/pkg/database"
)

type stubResult struct {
	Columns []string
	Rows    [][]driver.Value
}

type stubQueryFunc func(query string, args []driver.NamedValue) (stubResult, error)

// stubExecFunc answers Exec with the number of rows affected.
type stubExecFunc func(query string, args []any) (int64, error)

type stubDB struct {
	db   *sql.DB
	exec stubExecFunc
}

var _ database.Adapter = (*stubDB)(nil)

func newStubDB(fn stubQueryFunc) *stubDB {
	return &stubDB{db: sql.OpenDB(&stubConnector{fn: fn})}
}

func (s *stubDB) withExec(fn stubExecFunc) *stubDB {
	s.exec = fn
	return s
}

func (s *stubDB) QueryRows(ctx context.Context, query string, args ...any) (*sql.Rows, error) {
	return s.db.QueryContext(ctx, query, args...)
}
func (s *stubDB) QueryRow(ctx context.Context, query string, args ...any) *sql.Row {
	return s.db.QueryRowContext(ctx, query, args...)
}
func (s *stubDB) Fetch(context.Context, any, string, ...any) error    { return errStub("Fetch") }
func (s *stubDB) FetchRow(context.Context, any, string, ...any) error { return errStub("FetchRow") }
func (s *stubDB) BeginTx(context.Context, *sql.TxOptions) (*sql.Tx, error) {
	return nil, errStub("BeginTx")
}
func (s *stubDB) Exec(_ context.Context, query string, args ...any) (sql.Result, error) {
	if s.exec == nil {
		return nil, errStub("Exec")
	}
	n, err := s.exec(query, args)
	if err != nil {
		return nil, err
	}
	return driver.RowsAffected(n), nil
}
func (s *stubDB) Ping(context.Context) error { return nil }
func (s *stubDB) HealthCheck() error         { return nil }
func (s *stubDB) Close() error               { return s.db.Close() }

func errStub(op string) error { return fmt.Errorf("stubDB: %s not supported", op) }

type stubConnector struct{ fn stubQueryFunc }

func (c *stubConnector) Connect(context.Context) (driver.Conn, error) {
	return &stubConn{fn: c.fn}, nil
}
func (c *stubConnector) Driver() driver.Driver { return stubDriver{} }

type stubDriver struct{}

func (stubDriver) Open(string) (driver.Conn, error) {
	return nil, errors.New("stubDriver: use the connector")
}

type stubConn struct{ fn stubQueryFunc }

func (c *stubConn) Prepare(string) (driver.Stmt, error) { return nil, errStub("Prepare") }
func (c *stubConn) Close() error                        { return nil }
func (c *stubConn) Begin() (driver.Tx, error)           { return nil, errStub("Begin") }

// CheckNamedValue passes every argument through unchanged, so the stub function sees exactly what was bound.
func (c *stubConn) CheckNamedValue(*driver.NamedValue) error { return nil }

func (c *stubConn) QueryContext(_ context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	res, err := c.fn(query, args)
	if err != nil {
		return nil, err
	}
	return &stubRows{res: res}, nil
}

type stubRows struct {
	res stubResult
	i   int
}

func (r *stubRows) Columns() []string { return r.res.Columns }
func (r *stubRows) Close() error      { return nil }
func (r *stubRows) Next(dest []driver.Value) error {
	if r.i >= len(r.res.Rows) {
		return io.EOF
	}
	copy(dest, r.res.Rows[r.i])
	r.i++
	return nil
}
