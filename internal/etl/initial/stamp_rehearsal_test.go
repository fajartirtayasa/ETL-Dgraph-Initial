package initial

// The stamping rehearsal runs the export and its sync_time stamping for real, against a disposable clone of the
// synthetic fixture (parity/fixture/rebuild.sh). It reads PARITY_PG_URL, which parity/go.sh sets to that fixture, and
// skips when the database cannot be reached unless PARITY_REQUIRE_L3=1, so the offline suite still passes. The fixture
// database itself is only the template of CREATE DATABASE ... TEMPLATE: every write goes to a clone, which is dropped
// afterwards.

import (
	"context"
	"database/sql"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"hara/traceability-dgraph-etl/internal/etl/common"
	"hara/traceability-dgraph-etl/pkg/database"
)

var databaseName = regexp.MustCompile(`^[a-z_][a-z0-9_]*$`)

// rehearsalUnavailable skips, or fails under PARITY_REQUIRE_L3=1.
func rehearsalUnavailable(t *testing.T, format string, args ...any) {
	t.Helper()
	if os.Getenv("PARITY_REQUIRE_L3") == "1" {
		t.Fatalf(format, args...)
	}
	t.Skipf(format+" (set PARITY_REQUIRE_L3=1 to make this a failure)", args...)
}

func connectFixtureServer(u *url.URL, name string) (database.Adapter, error) {
	port := 5432
	if p := u.Port(); p != "" {
		var err error
		if port, err = strconv.Atoi(p); err != nil {
			return nil, err
		}
	}
	password, _ := u.User.Password()
	return database.NewDatabaseConnection(database.Postgres, &database.Config{
		Host: u.Hostname(), Port: port, Name: name, User: u.User.Username(), Password: password, Schema: "public",
		MaxConnectionOpen: 4, MaxConnectionIdle: 2, MaxLifeTimeMS: 60_000, TimeoutSecond: 5, Charset: "utf8",
	})
}

// cloneFixture creates a fresh database from the fixture template and drops it when the test ends.
func cloneFixture(t *testing.T) database.Adapter {
	t.Helper()
	raw := os.Getenv("PARITY_PG_URL")
	if raw == "" {
		rehearsalUnavailable(t, "PARITY_PG_URL is not set: start the fixture with parity/fixture/rebuild.sh")
	}
	u, err := url.Parse(raw)
	if err != nil {
		t.Fatalf("PARITY_PG_URL: %v", err)
	}
	template := strings.TrimPrefix(u.Path, "/")
	if !databaseName.MatchString(template) {
		t.Fatalf("PARITY_PG_URL names database %q, which is not a plain identifier", template)
	}

	admin, err := connectFixtureServer(u, "postgres")
	if err != nil {
		rehearsalUnavailable(t, "fixture server unreachable: %v", err)
	}
	t.Cleanup(func() { admin.Close() })

	clone := "initial_stamp_" + strconv.FormatInt(time.Now().UnixNano(), 36)
	if clone == template {
		t.Fatal("the clone would be the template itself")
	}
	ctx := context.Background()
	if _, err := admin.Exec(ctx, fmt.Sprintf("CREATE DATABASE %s TEMPLATE %s", clone, template)); err != nil {
		t.Fatalf("clone %s from %s (no other session may be connected to the template): %v", clone, template, err)
	}
	t.Cleanup(func() {
		if _, err := admin.Exec(ctx, "DROP DATABASE IF EXISTS "+clone+" WITH (FORCE)"); err != nil {
			t.Errorf("drop the clone %s: %v", clone, err)
		}
	})

	db, err := connectFixtureServer(u, clone)
	if err != nil {
		t.Fatalf("connect to the clone %s: %v", clone, err)
	}
	t.Cleanup(func() { db.Close() })

	var current string
	if err := db.QueryRow(ctx, "SELECT current_database()").Scan(&current); err != nil || current != clone {
		t.Fatalf("connected to %q (%v), want the clone %q", current, err, clone)
	}
	return db
}

// sourceRow is one events_bc or links_bc row: the columns that decide stamping, and its sync_time.
type sourceRow struct {
	key, event1, event2, status string // key: unique_id, or links_bc.id
	syncTime                    sql.NullTime
}

func readSourceRows(t *testing.T, db database.Adapter, query string, links bool) []sourceRow {
	t.Helper()
	rows, err := db.QueryRows(context.Background(), query)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var out []sourceRow
	for rows.Next() {
		var r sourceRow
		var status sql.NullString
		dest := []any{&r.key, &status, &r.syncTime}
		if links {
			dest = []any{&r.key, &r.event1, &r.event2, &status, &r.syncTime}
		}
		if err := rows.Scan(dest...); err != nil {
			t.Fatal(err)
		}
		r.status = status.String
		out = append(out, r)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	return out
}

const (
	rehearsalEventsQuery = "SELECT unique_id, status, sync_time FROM events_bc"
	rehearsalLinksQuery  = "SELECT id::text, event1_id, event2_id, status, sync_time FROM links_bc"
)

// syncState renders a row's sync_time after the run: "stamped" when the run set it, else its value before.
func syncState(v sql.NullTime, runStart time.Time) string {
	switch {
	case !v.Valid:
		return "NULL"
	case !v.Time.Before(runStart):
		return "stamped"
	default:
		return v.Time.UTC().Format(time.RFC3339Nano)
	}
}

// expectedStates applies the rule to the rows as they were before the run: a row is stamped exactly when it was
// exported (exported reports that, from the RDF on disk), is valid, and had no sync_time; every other row keeps its
// value. The result is sorted "key|status|state" lines, comparable with actualStates.
func expectedStates(before []sourceRow, exported func(sourceRow) bool) []string {
	out := make([]string, 0, len(before))
	for _, r := range before {
		state := syncState(r.syncTime, time.Now().Add(time.Hour)) // nothing before the run counts as "stamped"
		if r.status == "valid" && !r.syncTime.Valid && exported(r) {
			state = "stamped"
		}
		out = append(out, r.key+"|"+r.status+"|"+state)
	}
	sort.Strings(out)
	return out
}

func actualStates(after []sourceRow, runStart time.Time) []string {
	out := make([]string, 0, len(after))
	for _, r := range after {
		out = append(out, r.key+"|"+r.status+"|"+syncState(r.syncTime, runStart))
	}
	sort.Strings(out)
	return out
}

func compareStates(t *testing.T, table string, want, got []string) (stamped, keptNull int) {
	t.Helper()
	if strings.Join(want, "\n") != strings.Join(got, "\n") {
		t.Errorf("%s after the run differs from the rule\nwant:\n  %s\ngot:\n  %s", table,
			strings.Join(want, "\n  "), strings.Join(got, "\n  "))
	}
	for _, line := range got {
		switch {
		case strings.HasSuffix(line, "|stamped"):
			stamped++
		case strings.HasSuffix(line, "|NULL"):
			keptNull++
		}
	}
	return stamped, keptNull
}

func TestStampRehearsalOnAFixtureClone(t *testing.T) {
	for _, stamp := range []bool{true, false} {
		t.Run(fmt.Sprintf("stamp_sync_time=%t", stamp), func(t *testing.T) {
			db := cloneFixture(t)
			ctx := context.Background()
			eventsBefore := readSourceRows(t, db, rehearsalEventsQuery, false)
			linksBefore := readSourceRows(t, db, rehearsalLinksQuery, true)

			var runStart time.Time
			if err := db.QueryRow(ctx, "SELECT clock_timestamp()").Scan(&runStart); err != nil {
				t.Fatal(err)
			}
			dir := filepath.Join(t.TempDir(), "initial_imports")
			m, err := NewMigrator(db, dir, WithSyncTimeStamping(stamp), WithLogger(discardLogger()))
			if err != nil {
				t.Fatal(err)
			}
			if err := m.Run(ctx); err != nil {
				t.Fatalf("Run: %v", err)
			}

			// What was exported, read back from the RDF on disk.
			exportedEvents, exportedLinks := map[string]bool{}, map[string]bool{}
			for _, tr := range readTriples(t, filepath.Join(dir, "events*.rdf.gz")) {
				if tr.predicate == "unique_id" {
					exportedEvents[tr.object] = true
				}
			}
			for _, tr := range readTriples(t, filepath.Join(dir, "links*.rdf.gz")) {
				exportedLinks[tr.subject+" "+tr.object] = true
			}
			eventExported := func(r sourceRow) bool { return stamp && exportedEvents[strings.TrimSpace(r.key)] }
			linkExported := func(r sourceRow) bool {
				return stamp && exportedLinks[common.BlankNode("event", strings.TrimSpace(r.event1))+" "+
					common.BlankNode("event", strings.TrimSpace(r.event2))]
			}

			eventsStamped, eventsNull := compareStates(t, "events_bc", expectedStates(eventsBefore, eventExported),
				actualStates(readSourceRows(t, db, rehearsalEventsQuery, false), runStart))
			linksStamped, linksNull := compareStates(t, "links_bc", expectedStates(linksBefore, linkExported),
				actualStates(readSourceRows(t, db, rehearsalLinksQuery, true), runStart))
			t.Logf("exported %d events and %d links; stamped %d events_bc and %d links_bc rows; %d and %d rows kept NULL",
				len(exportedEvents), len(exportedLinks), eventsStamped, linksStamped, eventsNull, linksNull)

			if len(exportedEvents) == 0 || len(exportedLinks) == 0 {
				t.Fatal("the export wrote no event or no link: the fixture did not load")
			}
			if stamp && (eventsStamped == 0 || linksStamped == 0) {
				t.Error("stamping enabled, yet a table had no row stamped")
			}
			if stamp && (eventsNull == 0 || linksNull == 0) {
				t.Error("every row of a table ended stamped: the scope rule (exported, valid, NULL) was not exercised")
			}
		})
	}
}
