package incremental

// stubDB is an L2 stand-in for database.Adapter. database.Adapter.QueryRows
// returns a concrete *sql.Rows, so the stub is a minimal database/sql driver
// whose rows come from a Go function instead of a server. It answers only
// what an L2 test scripts; the real SQL is exercised at L3.

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

type stubDB struct {
	db *sql.DB
}

var _ database.Adapter = (*stubDB)(nil)

func newStubDB(fn stubQueryFunc) *stubDB {
	return &stubDB{db: sql.OpenDB(&stubConnector{fn: fn})}
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
func (s *stubDB) Exec(context.Context, string, ...any) (sql.Result, error) {
	return nil, errStub("Exec")
}
func (s *stubDB) Ping(context.Context) error { return nil }
func (s *stubDB) HealthCheck() error         { return nil }
func (s *stubDB) Close() error               { return s.db.Close() }

func errStub(op string) error { return fmt.Errorf("stubDB: %s not supported at L2", op) }

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

// CheckNamedValue passes every argument through unchanged, so a []string
// bound to `= ANY($1)` reaches the stub function as-is (as pgx accepts it).
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

// stringsArg returns the []string bound to the first placeholder.
func stringsArg(args []driver.NamedValue) ([]string, error) {
	if len(args) != 1 {
		return nil, fmt.Errorf("want 1 argument, got %d", len(args))
	}
	ids, ok := args[0].Value.([]string)
	if !ok {
		return nil, fmt.Errorf("argument is %T, want []string", args[0].Value)
	}
	return ids, nil
}

// nullable converts a Go value into a driver.Value, mapping nil pointers to
// SQL NULL.
func nullable[T any](v *T) driver.Value {
	if v == nil {
		return nil
	}
	return *v
}
