package database

import (
	"strings"
	"testing"

	"github.com/jackc/pgx/v5"
)

func TestPostgresConnectionStringApplicationName(t *testing.T) {
	base := Config{Host: "localhost", Port: 5432, Name: "db", User: "u", Password: "p", Schema: "public", TimeoutSecond: 5, Charset: "utf8"}

	withName := base
	withName.ApplicationName = "etl_dgraph_initial"
	parsed, err := pgx.ParseConfig(postgresConnectionString(&withName))
	if err != nil {
		t.Fatalf("parse connection string: %v", err)
	}
	if got := parsed.RuntimeParams["application_name"]; got != "etl_dgraph_initial" {
		t.Fatalf("application_name = %q, want %q", got, "etl_dgraph_initial")
	}

	if dsn := postgresConnectionString(&base); strings.Contains(dsn, "application_name") {
		t.Fatalf("empty ApplicationName must not set application_name: %s", dsn)
	}
}
