package initial

import (
	"context"
	"database/sql/driver"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"unicode"
)

func discardLogger() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }

// exportStub serves eventsQuery with no row and linksQuery with one link, and answers Exec with exec (nil: Exec fails).
func exportStub(t *testing.T, exec stubExecFunc) *stubDB {
	t.Helper()
	db := newStubDB(func(query string, args []driver.NamedValue) (stubResult, error) {
		switch query {
		case eventsQuery:
			return stubResult{}, nil
		case linksQuery:
			return stubResult{
				Columns: []string{"id", "event1_id", "event2_id", "event1_version", "event2_version", "role", "qty", "unit", "source"},
				Rows:    [][]driver.Value{{int64(970001), "SYN-E1", "SYN-E2", nil, nil, "SYN-ROLE", 2.5, "SYN-UNIT", "SYN-SOURCE"}},
			}, nil
		}
		return stubResult{}, fmt.Errorf("unexpected query: %.80q", query)
	}).withExec(exec)
	t.Cleanup(func() { db.Close() })
	return db
}

// runExport runs the whole Migrator into a fresh output directory and returns that directory with Run's error.
func runExport(t *testing.T, db *stubDB, stamp bool) (string, error) {
	t.Helper()
	dir := filepath.Join(t.TempDir(), "initial_imports")
	m, err := NewMigrator(db, dir, WithWorkerCount(2), WithSyncTimeStamping(stamp), WithLogger(discardLogger()))
	if err != nil {
		t.Fatal(err)
	}
	return dir, m.Run(context.Background())
}

func TestTrimSpaceCharsAreExactlyWhatTrimSpaceRemoves(t *testing.T) {
	if strings.TrimSpace(trimSpaceChars) != "" {
		t.Fatalf("strings.TrimSpace keeps part of trimSpaceChars: %q", strings.TrimSpace(trimSpaceChars))
	}
	for r := rune(0); r <= unicode.MaxRune; r++ {
		if listed := strings.ContainsRune(trimSpaceChars, r); listed != unicode.IsSpace(r) {
			t.Errorf("U+%04X: unicode.IsSpace is %t but trimSpaceChars lists it: %t", r, unicode.IsSpace(r), listed)
		}
	}
}

// The export queries and the stamping statements must select the same rows, so they share the filter text.
func TestStampStatementsReuseTheExportFilters(t *testing.T) {
	shares := []struct{ name, text, shared string }{
		{"eventsQuery", eventsQuery, exportedEventIDsCTE},
		{"stampEventsStatement", stampEventsStatement, exportedEventIDsCTE},
		{"linksQuery", linksQuery, exportedLinksFilter},
		{"stampLinksStatement", stampLinksStatement, exportedLinksFilter},
	}
	for _, s := range shares {
		if !strings.Contains(s.text, s.shared) {
			t.Errorf("%s does not contain the shared export filter", s.name)
		}
	}

	if !strings.Contains(eventsQuery, "JOIN UnsyncedUniqueIDs u ON u.unique_id = e.unique_id") ||
		!strings.Contains(stampEventsStatement, "WHERE e.unique_id IN (SELECT unique_id FROM UnsyncedUniqueIDs)") {
		t.Error("events: the export and the stamp no longer both select by UnsyncedUniqueIDs")
	}
	if !strings.Contains(stampLinksStatement, "WHERE id IN (\n    SELECT l.id"+exportedLinksFilter) {
		t.Error("links: the stamp no longer selects links_bc.id through exportedLinksFilter")
	}

	for name, statement := range map[string]string{"events": stampEventsStatement, "links": stampLinksStatement} {
		for _, want := range []string{"SET sync_time = NOW()", "status = 'valid'", "sync_time IS NULL", "btrim("} {
			if !strings.Contains(statement, want) {
				t.Errorf("%s stamp statement lacks %q", name, want)
			}
		}
	}
}

func TestStampSyncTimeRunsOneStatementPerTableEventsFirst(t *testing.T) {
	var got []string
	db := exportStub(t, func(query string, args []any) (int64, error) {
		if len(args) != 1 || args[0] != trimSpaceChars {
			t.Fatalf("Exec args = %q, want exactly trimSpaceChars", args)
		}
		got = append(got, query)
		return 3, nil
	})

	if err := stampSyncTime(context.Background(), db, discardLogger()); err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 || got[0] != stampEventsStatement || got[1] != stampLinksStatement {
		t.Fatalf("executed %d statements, want stampEventsStatement then stampLinksStatement", len(got))
	}
}

func TestStampSyncTimeAttemptsLinksWhenEventsFail(t *testing.T) {
	for _, tt := range []struct {
		name       string
		failLinks  bool
		want, deny []string
	}{
		{"events fail", false, []string{"stamp events_bc sync_time: events boom"}, []string{"links_bc"}},
		{"both fail", true, []string{"stamp events_bc sync_time: events boom", "stamp links_bc sync_time: links boom"}, nil},
	} {
		t.Run(tt.name, func(t *testing.T) {
			calls := 0
			db := exportStub(t, func(query string, _ []any) (int64, error) {
				calls++
				if query == stampEventsStatement {
					return 0, errors.New("events boom")
				}
				if tt.failLinks {
					return 0, errors.New("links boom")
				}
				return 1, nil
			})

			err := stampSyncTime(context.Background(), db, discardLogger())
			if err == nil {
				t.Fatal("no error")
			}
			if calls != 2 {
				t.Fatalf("%d statements executed, want 2: links_bc must be attempted after events_bc fails", calls)
			}
			for _, want := range tt.want {
				if !strings.Contains(err.Error(), want) {
					t.Errorf("error %q lacks %q", err, want)
				}
			}
			for _, deny := range tt.deny {
				if strings.Contains(err.Error(), deny) {
					t.Errorf("error %q mentions %q", err, deny)
				}
			}
		})
	}
}

// Stamping starts only once both writers are closed: the links file is already a complete gzip stream when Exec runs.
func TestRunStampsOnlyAfterTheRDFIsComplete(t *testing.T) {
	var dir string
	var stamped []string
	db := exportStub(t, func(query string, _ []any) (int64, error) {
		if links := readTriples(t, filepath.Join(dir, "links*.rdf.gz")); len(links) != 1 {
			t.Fatalf("stamping started with %d link triples on disk, want the complete 1", len(links))
		}
		stamped = append(stamped, query)
		return 1, nil
	})

	dir = filepath.Join(t.TempDir(), "initial_imports")
	m, err := NewMigrator(db, dir, WithSyncTimeStamping(true), WithLogger(discardLogger()))
	if err != nil {
		t.Fatal(err)
	}
	if err := m.Run(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(stamped) != 2 {
		t.Fatalf("%d stamping statements, want 2", len(stamped))
	}
}

func TestRunWithStampingDisabledNeverWrites(t *testing.T) {
	db := exportStub(t, func(query string, _ []any) (int64, error) {
		t.Fatalf("Exec called with stamping disabled: %.60q", query)
		return 0, nil
	})
	if _, err := runExport(t, db, false); err != nil {
		t.Fatal(err)
	}
}

func TestRunKeepsTheRDFWhenStampingFails(t *testing.T) {
	db := exportStub(t, func(query string, _ []any) (int64, error) {
		return 0, errors.New("permission denied for table")
	})

	dir, err := runExport(t, db, true)
	if err == nil {
		t.Fatal("Run returned no error although stamping failed")
	}
	for _, want := range []string{"is complete and kept", "stamping sync_time failed", "REFACTOR_PLAN.md §9.0.1",
		"stamp events_bc sync_time: permission denied", "stamp links_bc sync_time: permission denied"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error %q lacks %q", err, want)
		}
	}

	if _, statErr := os.Stat(filepath.Join(dir, "events.rdf.gz")); statErr != nil {
		t.Errorf("events.rdf.gz was not kept: %v", statErr)
	}
	if links := readTriples(t, filepath.Join(dir, "links*.rdf.gz")); len(links) != 1 {
		t.Errorf("links.rdf.gz holds %d triples after the failure, want the exported 1", len(links))
	}
}
