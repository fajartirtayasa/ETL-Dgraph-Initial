package incremental

// L3 — the SQL contract and the full pipeline against the synthetic Postgres
// fixture (REFACTOR_PLAN.md P1-6..P1-8). Each test runs on a throwaway clone
// (CREATE DATABASE ... TEMPLATE) of the fixture, so the fixture is never
// modified. Needs PARITY_PG_URL (see parity/README.md); skipped without it
// unless PARITY_REQUIRE_L3 is set.

import (
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"

	"hara/traceability-dgraph-etl/pkg/database"
)

const (
	envPGURL     = "PARITY_PG_URL"
	envRequireL3 = "PARITY_REQUIRE_L3"
)

func l3URL(t *testing.T) *url.URL {
	t.Helper()
	raw := os.Getenv(envPGURL)
	if raw == "" {
		if os.Getenv(envRequireL3) != "" {
			t.Fatalf("%s is set but %s is empty; start the fixture with parity/fixture/rebuild.sh", envRequireL3, envPGURL)
		}
		t.Skipf("L3 skipped: set %s (parity/fixture/rebuild.sh starts the fixture)", envPGURL)
	}
	u, err := url.Parse(raw)
	if err != nil {
		t.Fatalf("parse %s: %v", envPGURL, err)
	}
	return u
}

type fixtureDB struct {
	adapter database.Adapter // the ETL's own adapter, as production uses it
	raw     *sql.DB          // same database, for inspecting state
}

var cloneSeq atomic.Int64

// openFixtureClone clones the fixture into a fresh database. A non-empty
// prefix keeps only that scenario's events_bc / links_bc rows.
func openFixtureClone(t *testing.T, prefix string) fixtureDB {
	t.Helper()
	u := l3URL(t)
	template := strings.TrimPrefix(u.Path, "/")

	admin := *u
	admin.Path = "/postgres"
	adminDB, err := sql.Open("pgx", admin.String())
	if err != nil {
		t.Fatal(err)
	}
	name := fmt.Sprintf("parity_t_%d_%d", os.Getpid(), cloneSeq.Add(1))
	if _, err := adminDB.Exec(fmt.Sprintf(`CREATE DATABASE %s TEMPLATE "%s"`, name, template)); err != nil {
		adminDB.Close()
		t.Fatalf("clone fixture %q: %v", template, err)
	}
	t.Cleanup(func() {
		_, _ = adminDB.Exec(`DROP DATABASE IF EXISTS ` + name + ` WITH (FORCE)`)
		adminDB.Close()
	})

	clone := *u
	clone.Path = "/" + name
	raw, err := sql.Open("pgx", clone.String())
	if err != nil {
		t.Fatal(err)
	}
	if prefix != "" {
		for _, q := range []string{
			`DELETE FROM events_bc WHERE unique_id NOT LIKE $1`,
			`DELETE FROM links_bc WHERE event1_id NOT LIKE $1`,
		} {
			if _, err := raw.Exec(q, prefix+"%"); err != nil {
				t.Fatal(err)
			}
		}
	}

	port, _ := strconv.Atoi(u.Port())
	password, _ := u.User.Password()
	adapter, err := database.NewDatabaseConnection(database.Postgres, &database.Config{
		Host: u.Hostname(), Port: port, Name: name, User: u.User.Username(), Password: password,
		Schema: "public", MaxConnectionOpen: 8, MaxConnectionIdle: 2, MaxLifeTimeMS: 60000,
		TimeoutSecond: 10, Charset: "UTF8",
	})
	if err != nil {
		raw.Close()
		t.Fatal(err)
	}
	t.Cleanup(func() {
		adapter.Close()
		raw.Close()
	})
	return fixtureDB{adapter: adapter, raw: raw}
}

// --- fixture manifest and graph seed ----------------------------------------

type manifestExpect struct {
	EventIDs         []string `json:"event_ids_query"`
	LinkEvent1IDs    []string `json:"link_event1_ids_query"`
	RemovalLinkIDs   []int64  `json:"removal_query_link_ids"`
	RemovalUniqueIDs []string `json:"removal_query_unique_ids"`
	StampedEvents    []string `json:"sync_time_stamped_events"`
	StampedLinks     []int64  `json:"sync_time_stamped_links"`
	CreatedNodes     []string `json:"nodes_created"`
	DeletedNodes     []string `json:"nodes_deleted"`
	EdgesAfter       []string `json:"edges_after"`
}

type manifestScenario struct {
	ID        string         `json:"id"`
	Key       string         `json:"key"`
	Title     string         `json:"title"`
	RowPrefix string         `json:"row_prefix"`
	Expect    manifestExpect `json:"expect"`
}

func loadFixtureJSON(t *testing.T, name string, v any) {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join(repoRoot(t), "parity", "fixture", name))
	if err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(raw, v); err != nil {
		t.Fatalf("%s: %v", name, err)
	}
}

func loadScenarios(t *testing.T) []manifestScenario {
	var m struct {
		Scenarios []manifestScenario `json:"scenarios"`
	}
	loadFixtureJSON(t, "scenarios.json", &m)
	if len(m.Scenarios) != 7 {
		t.Fatalf("fixture must define the seven P1-7 scenarios, found %d", len(m.Scenarios))
	}
	return m.Scenarios
}

// unionExpect merges every scenario's expectations: the "ALL" run.
func unionExpect(scs []manifestScenario) manifestExpect {
	var u manifestExpect
	for _, sc := range scs {
		e := sc.Expect
		u.EventIDs = append(u.EventIDs, e.EventIDs...)
		u.LinkEvent1IDs = append(u.LinkEvent1IDs, e.LinkEvent1IDs...)
		u.RemovalLinkIDs = append(u.RemovalLinkIDs, e.RemovalLinkIDs...)
		u.RemovalUniqueIDs = append(u.RemovalUniqueIDs, e.RemovalUniqueIDs...)
		u.StampedEvents = append(u.StampedEvents, e.StampedEvents...)
		u.StampedLinks = append(u.StampedLinks, e.StampedLinks...)
		u.CreatedNodes = append(u.CreatedNodes, e.CreatedNodes...)
		u.DeletedNodes = append(u.DeletedNodes, e.DeletedNodes...)
		u.EdgesAfter = append(u.EdgesAfter, e.EdgesAfter...)
	}
	return u
}

// seedFakeFromFixture loads graph_seed.json (all scenarios when id is "").
func seedFakeFromFixture(t *testing.T, g *fakeGraph, scenarioID string) {
	t.Helper()
	var seed struct {
		Nodes []struct {
			Scenario   string              `json:"scenario"`
			UID        string              `json:"uid"`
			UniqueID   string              `json:"unique_id"`
			Predicates map[string][]string `json:"predicates"`
		} `json:"nodes"`
		Edges []struct {
			Scenario string            `json:"scenario"`
			From     string            `json:"from"`
			To       string            `json:"to"`
			Facets   map[string]string `json:"facets"`
		} `json:"edges"`
	}
	loadFixtureJSON(t, "graph_seed.json", &seed)
	uids := map[string]string{}
	for _, n := range seed.Nodes {
		if scenarioID == "" || n.Scenario == scenarioID {
			g.seedNode(n.UID, n.UniqueID, n.Predicates)
			uids[n.UniqueID] = n.UID
		}
	}
	for _, e := range seed.Edges {
		if scenarioID != "" && e.Scenario != scenarioID {
			continue
		}
		parts := make([]string, 0, len(e.Facets))
		for _, k := range sortedKeys(e.Facets) {
			parts = append(parts, k+"="+e.Facets[k])
		}
		g.seedEdge(uids[e.From], uids[e.To], strings.Join(parts, ", "))
	}
}

// --- P1-8: expected rowsets --------------------------------------------------

// scanKind is how the Go ETL scans a column, which fixes how its value is
// rendered in the golden.
type scanKind string

const (
	scanString      scanKind = "string"      // non-null Go string (events.go:166)
	scanNullString  scanKind = "nullstring"  // sql.NullString -> string | null
	scanNullInt64   scanKind = "nullint64"   // sql.NullInt64 -> number | null
	scanNullFloat64 scanKind = "nullfloat64" // sql.NullFloat64 -> shortest decimal string | null
)

func queryRowset(t *testing.T, db database.Adapter, query string, kinds map[string]scanKind, args ...any) ([]string, [][]any) {
	t.Helper()
	rows, err := db.QueryRows(testContext(t), query, args...)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	cols, err := rows.Columns()
	if err != nil {
		t.Fatal(err)
	}
	var out [][]any
	for rows.Next() {
		dest := make([]any, len(cols))
		for i, c := range cols {
			switch kinds[c] {
			case scanString:
				dest[i] = new(string)
			case scanNullInt64:
				dest[i] = new(sql.NullInt64)
			case scanNullFloat64:
				dest[i] = new(sql.NullFloat64)
			default:
				dest[i] = new(sql.NullString)
			}
		}
		if err := rows.Scan(dest...); err != nil {
			t.Fatal(err)
		}
		row := make([]any, len(cols))
		for i, d := range dest {
			switch v := d.(type) {
			case *string:
				row[i] = *v
			case *sql.NullString:
				if v.Valid {
					row[i] = v.String
				}
			case *sql.NullInt64:
				if v.Valid {
					row[i] = v.Int64
				}
			case *sql.NullFloat64:
				if v.Valid {
					row[i] = strconv.FormatFloat(v.Float64, 'f', -1, 64)
				}
			}
		}
		out = append(out, row)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	sort.Slice(out, func(i, j int) bool {
		a, _ := json.Marshal(out[i])
		b, _ := json.Marshal(out[j])
		return string(a) < string(b)
	})
	if out == nil {
		out = [][]any{}
	}
	return cols, out
}

func columnStrings(rows [][]any, col int) []string {
	out := []string{}
	for _, r := range rows {
		if s, ok := r[col].(string); ok {
			out = append(out, s)
		}
	}
	sort.Strings(out)
	return out
}

func sortedUnique(in []string) []string {
	set := map[string]struct{}{}
	for _, v := range in {
		set[v] = struct{}{}
	}
	return sortedKeys(set)
}

func sortedInt64s(in []int64) []int64 {
	out := append([]int64{}, in...)
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	return out
}

func assertSameStrings(t *testing.T, what string, got, want []string) {
	t.Helper()
	if strings.Join(sortedUnique(got), ",") != strings.Join(sortedUnique(want), ",") {
		t.Errorf("%s\n got: %v\nwant: %v (hand-derived, parity/fixture/scenarios.json)", what, sortedUnique(got), sortedUnique(want))
	}
}

func assertSameInt64s(t *testing.T, what string, got, want []int64) {
	t.Helper()
	if fmt.Sprint(sortedInt64s(got)) != fmt.Sprint(sortedInt64s(want)) {
		t.Errorf("%s\n got: %v\nwant: %v (hand-derived, parity/fixture/scenarios.json)", what, sortedInt64s(got), sortedInt64s(want))
	}
}

func rowsetGolden(t *testing.T, name, source, query string, params map[string]any, kinds map[string]scanKind, cols []string, rows [][]any) {
	t.Helper()
	sum := sha256.Sum256([]byte(query))
	encoding := map[string]scanKind{}
	for _, c := range cols {
		k := kinds[c]
		if k == "" {
			k = scanNullString
		}
		encoding[c] = k
	}
	assertGolden(t, "l3/sql/"+name+".json", "l3.rowset",
		fmt.Sprintf("%s (%s) against the full synthetic fixture.", name, source),
		compareRowset, map[string]any{
			"query":      name,
			"source":     source,
			"sql":        query,
			"sql_sha256": hex.EncodeToString(sum[:]),
			"params":     params,
			"columns":    cols,
			"value_encoding": map[string]any{
				"columns": encoding,
				"rules": map[string]string{
					"string":      "JSON string (column scanned as non-null text)",
					"nullstring":  "JSON string, or null for SQL NULL; untrimmed, exactly as returned",
					"nullint64":   "JSON number, or null",
					"nullfloat64": "JSON string holding the float64 value as Go strconv.FormatFloat(v,'f',-1,64), or null",
				},
			},
			"rows": rows,
		})
}

func TestParityL3SQLRowsets(t *testing.T) {
	db := openFixtureClone(t, "")
	want := unionExpect(loadScenarios(t))

	// eventIDsQuery — scanned into a plain string (events.go:166).
	kinds := map[string]scanKind{"unique_id": scanString}
	cols, rows := queryRowset(t, db.adapter, eventIDsQuery, kinds)
	eventIDs := columnStrings(rows, 0)
	assertSameStrings(t, "eventIDsQuery", eventIDs, want.EventIDs)
	rowsetGolden(t, "event_ids_query", "incremental/events.go:13-49", eventIDsQuery, map[string]any{}, kinds, cols, rows)

	// linkEvent1IDsQuery — sql.NullString (links.go:102).
	kinds = map[string]scanKind{"event1_id": scanNullString}
	cols, rows = queryRowset(t, db.adapter, linkEvent1IDsQuery, kinds)
	event1IDs := columnStrings(rows, 0)
	assertSameStrings(t, "linkEvent1IDsQuery", event1IDs, want.LinkEvent1IDs)
	rowsetGolden(t, "link_event1_ids_query", "incremental/links.go:14-49", linkEvent1IDsQuery, map[string]any{}, kinds, cols, rows)

	// removalQuery — NullInt64 + NullString (removal.go:35-36).
	kinds = map[string]scanKind{"link_id": scanNullInt64, "unique_id": scanNullString}
	cols, rows = queryRowset(t, db.adapter, removalQuery, kinds)
	var linkIDs []int64
	for _, r := range rows {
		if id, ok := r[0].(int64); ok {
			linkIDs = append(linkIDs, id)
		}
	}
	assertSameInt64s(t, "removalQuery link_id", linkIDs, want.RemovalLinkIDs)
	assertSameStrings(t, "removalQuery unique_id", columnStrings(rows, 1), want.RemovalUniqueIDs)
	rowsetGolden(t, "removal_query", "incremental/removal.go:9-17", removalQuery, map[string]any{}, kinds, cols, rows)

	// eventDetailsQuery — the ids eventIDsQuery returned, plus an invalid event,
	// a valid event eventIDsQuery did not select, and an unknown id.
	detailIDs := append(append([]string{}, eventIDs...), "SYN-S3-E01", "SYN-S7-E01", "SYN-NONEXISTENT")
	kinds = map[string]scanKind{"process_date": scanNullInt64, "amount": scanNullFloat64}
	cols, rows = queryRowset(t, db.adapter, eventDetailsQuery, kinds, detailIDs)
	gotDetail := columnStrings(rows, 0)
	assertSameStrings(t, "eventDetailsQuery unique_id (status='valid' filter only)", gotDetail, append(eventIDs, "SYN-S7-E01"))
	if len(gotDetail) != len(sortedUnique(gotDetail)) {
		t.Errorf("eventDetailsQuery fans an event out into several rows: %v", gotDetail)
	}
	rowsetGolden(t, "event_details_query", "incremental/events.go:51-109", eventDetailsQuery,
		map[string]any{"$1": detailIDs}, kinds, cols, rows)

	// linkDetailsQuery — the event1 ids, plus an event that is only ever event2
	// and an unknown id. Returns every status (filtering happens in Go).
	linkDetailIDs := append(append([]string{}, event1IDs...), "SYN-S2-E02", "SYN-NONEXISTENT")
	kinds = map[string]scanKind{"id": scanNullInt64, "qty": scanNullFloat64}
	cols, rows = queryRowset(t, db.adapter, linkDetailsQuery, kinds, linkDetailIDs)
	var wantLinks int
	if err := db.raw.QueryRow(`SELECT count(*) FROM links_bc WHERE event1_id = ANY($1)`, linkDetailIDs).Scan(&wantLinks); err != nil {
		t.Fatal(err)
	}
	if len(rows) != wantLinks {
		t.Errorf("linkDetailsQuery returned %d rows, want every link of the event1s (%d)", len(rows), wantLinks)
	}
	rowsetGolden(t, "link_details_query", "incremental/links.go:51-67", linkDetailsQuery,
		map[string]any{"$1": linkDetailIDs}, kinds, cols, rows)
}

// TestParityL3FixtureIsSynthetic proves the CHECK constraints reject an
// identifier without the SYN- marker, i.e. no extracted row can be loaded.
func TestParityL3FixtureIsSynthetic(t *testing.T) {
	db := openFixtureClone(t, "")
	for _, q := range []string{
		`INSERT INTO events_bc (unique_id, client_id, sales_order, product_id, product_name, process_date,
			material_type, amount, unit, flag, location_id, source) VALUES ('REAL-1','x','x','x','x',0,'x',0,'x','x','x','x')`,
		`INSERT INTO links_bc (id, event1_id, event2_id, role, qty, unit, source) VALUES (1,'REAL-1','SYN-X','x',0,'x','x')`,
	} {
		if _, err := db.raw.Exec(q); err == nil || !strings.Contains(err.Error(), "check constraint") {
			t.Errorf("non-synthetic row was not rejected by a CHECK constraint: %v", err)
		}
	}
	var unmarked int
	if err := db.raw.QueryRow(`SELECT
		(SELECT count(*) FROM events_bc WHERE unique_id NOT LIKE 'SYN-%') +
		(SELECT count(*) FROM links_bc WHERE event1_id NOT LIKE 'SYN-%' OR event2_id NOT LIKE 'SYN-%') +
		(SELECT count(*) FROM subjects WHERE subject_id NOT LIKE 'SYN-%') +
		(SELECT count(*) FROM mv_subject_hierarchy WHERE start_subject_id NOT LIKE 'SYN-%') +
		(SELECT count(*) FROM master_mill WHERE mill_id NOT LIKE 'SYN-%')`).Scan(&unmarked); err != nil {
		t.Fatal(err)
	}
	if unmarked != 0 {
		t.Errorf("%d fixture rows lack the SYN- marker", unmarked)
	}
}

// --- P1-7: the seven scenarios through the whole pipeline -------------------

type pipelineOutcome struct {
	Writes     []canonWrite
	Lookups    []canonLookup
	GraphAfter map[string]canonNode
	Stamped    map[string]any
	Created    []string
	Deleted    []string
}

func pendingIDs(t *testing.T, db *sql.DB) (map[string]bool, map[int64]bool) {
	t.Helper()
	events, links := map[string]bool{}, map[int64]bool{}
	rows, err := db.Query(`SELECT unique_id FROM events_bc WHERE sync_time IS NULL`)
	if err != nil {
		t.Fatal(err)
	}
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			t.Fatal(err)
		}
		events[id] = true
	}
	rows.Close()
	rows, err = db.Query(`SELECT id FROM links_bc WHERE sync_time IS NULL`)
	if err != nil {
		t.Fatal(err)
	}
	for rows.Next() {
		var id int64
		if err := rows.Scan(&id); err != nil {
			t.Fatal(err)
		}
		links[id] = true
	}
	rows.Close()
	return events, links
}

// runFixturePipeline runs Migrator.Run's step sequence after its traceability
// gate (migrator.go:60-80) against a clone of the fixture and the fake graph.
// The gate (traceability_check.go) is outside P1's scope and not exercised.
func runFixturePipeline(t *testing.T, scenarioID, prefix string, cfg Config) pipelineOutcome {
	t.Helper()
	db := openFixtureClone(t, prefix)
	g := newFakeGraph()
	seedFakeFromFixture(t, g, scenarioID)
	beforeEvents, beforeLinks := pendingIDs(t, db.raw)

	m := newParityMigrator(g, db.adapter, cfg)
	ctx := testContext(t)
	g.setStep("removal")
	removal, err := m.removeInvalidEvents(ctx)
	if err != nil {
		t.Fatal(err)
	}
	g.setStep("events")
	events, err := m.syncEvents(ctx)
	if err != nil {
		t.Fatal(err)
	}
	g.setStep("links")
	linkIDs, err := m.syncLinks(ctx, events)
	if err != nil {
		t.Fatal(err)
	}
	g.setStep("sync_time")
	if err := m.updateSyncTimes(ctx, removal, events.UniqueIDs, linkIDs); err != nil {
		t.Fatal(err)
	}

	afterEvents, afterLinks := pendingIDs(t, db.raw)
	stampedEvents, stampedLinks := []string{}, []int64{}
	for id := range beforeEvents {
		if !afterEvents[id] {
			stampedEvents = append(stampedEvents, id)
		}
	}
	for id := range beforeLinks {
		if !afterLinks[id] {
			stampedLinks = append(stampedLinks, id)
		}
	}
	sort.Strings(stampedEvents)
	stampedLinks = sortedInt64s(stampedLinks)

	writes, err := g.canonicalWrites()
	if err != nil {
		t.Fatal(err)
	}
	snap, err := g.snapshot()
	if err != nil {
		t.Fatal(err)
	}
	created, deleted := []string{}, []string{}
	for id := range snap {
		if !g.seeded[id] {
			created = append(created, id)
		}
	}
	for id := range g.seeded {
		if _, ok := snap[id]; !ok {
			deleted = append(deleted, id)
		}
	}
	sort.Strings(created)
	sort.Strings(deleted)
	return pipelineOutcome{
		Writes: writes, Lookups: g.canonicalLookups(), GraphAfter: snap,
		Stamped: map[string]any{"events_bc": stampedEvents, "links_bc": stampedLinks},
		Created: created, Deleted: deleted,
	}
}

func TestParityL3Pipeline(t *testing.T) {
	scs := loadScenarios(t)
	type run struct {
		manifestScenario
		description string
	}
	runs := make([]run, 0, len(scs)+1)
	for _, sc := range scs {
		runs = append(runs, run{sc, sc.Title})
	}
	runs = append(runs, run{manifestScenario{ID: "ALL", Key: "all_scenarios", Title: "All scenarios together",
		Expect: unionExpect(scs)}, "All seven scenarios in one run, to show they do not interfere"})

	for _, r := range runs {
		t.Run(r.ID+"_"+r.Key, func(t *testing.T) {
			seedID := r.ID
			if seedID == "ALL" {
				seedID = ""
			}
			cfg := singleBatchConfig()
			out := runFixturePipeline(t, seedID, r.RowPrefix, cfg)
			e := r.Expect

			assertSameStrings(t, "sync_time stamped events", out.Stamped["events_bc"].([]string), e.StampedEvents)
			assertSameInt64s(t, "sync_time stamped links", out.Stamped["links_bc"].([]int64), e.StampedLinks)
			assertSameStrings(t, "nodes created", out.Created, e.CreatedNodes)
			assertSameStrings(t, "nodes deleted", out.Deleted, e.DeletedNodes)
			assertSameStrings(t, "edges after", edgeList(out.GraphAfter), e.EdgesAfter)

			// Scenario 3 must produce an actual node delete, not only field deletes.
			if r.ID == "S3" || r.ID == "ALL" {
				found := false
				for _, w := range out.Writes {
					if w.Step == "removal" && w.Op == "DeleteRDF" && containsString(w.Del, "<uid:SYN-S3-E01> * * .") {
						found = true
					}
				}
				if !found {
					t.Error("S3: no `<uid:SYN-S3-E01> * * .` DeleteRDF captured in the removal step")
				}
			}

			assertGolden(t, fmt.Sprintf("l3/pipeline/%s_%s.json", r.ID, r.Key), "l3.pipeline",
				fmt.Sprintf("%s: removal -> events -> links -> sync_time against the synthetic fixture with the "+
					"pre-run graph from graph_seed.json. %s", r.Title, r.description),
				comparePipeline, map[string]any{
					"scenario": r.ID,
					"fixture": map[string]any{
						"schema":     "parity/fixture/sql/01_schema.sql",
						"seed":       "parity/fixture/sql/02_seed.sql",
						"graph_seed": "parity/fixture/graph_seed.json",
						"row_prefix": r.RowPrefix,
					},
					"steps":                 []string{"removal", "events", "links", "sync_time"},
					"config":                configJSON(cfg),
					"writes":                out.Writes,
					"lookups_informational": out.Lookups,
					"graph_after":           out.GraphAfter,
					"sync_time_stamped":     out.Stamped,
				})

			// End state must not depend on chunk/batch sizes or concurrency;
			// in particular link chunks must stay grouped by event1 (P1-4 b).
			tiny := runFixturePipeline(t, seedID, r.RowPrefix, tinyBatchConfig())
			if got, want := normalizeJSON(t, tiny.GraphAfter), normalizeJSON(t, out.GraphAfter); got != want {
				t.Errorf("graph after a tiny-batch concurrent run differs from the single-batch run:\n%s", lineDiff(want, got))
			}
			if got, want := normalizeJSON(t, tiny.Stamped), normalizeJSON(t, out.Stamped); got != want {
				t.Errorf("sync_time stamping differs under tiny batches:\n%s", lineDiff(want, got))
			}
		})
	}
}
