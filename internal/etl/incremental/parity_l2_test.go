package incremental

// L2 — RDF golden files over plain structs (REFACTOR_PLAN.md P1-3, P1-5).
// No infrastructure: the fake dgraph.Adapter captures payloads and the stub
// database answers the one query removeInvalidEvents issues.

import (
	"database/sql/driver"
	"fmt"
	"sort"
	"strings"
	"testing"
)

type l2Fixture struct {
	graph *fakeGraph
	uids  *uidMap
}

// newL2Fixture seeds the fake graph with one bare Event node per existing
// unique_id and returns a uid map holding exactly those.
func newL2Fixture(existing map[string]string) l2Fixture {
	g := newFakeGraph()
	for id, uid := range existing {
		g.seedNode(uid, id, nil)
	}
	return l2Fixture{graph: g, uids: newUIDMap(existing)}
}

func (f l2Fixture) writes(t *testing.T) []canonWrite {
	t.Helper()
	w, err := f.graph.canonicalWrites()
	if err != nil {
		t.Fatal(err)
	}
	return w
}

// uidResolution reports, after a call, which unique_ids the uid map resolves
// and which of them were created during the call.
func (f l2Fixture) uidResolution(before map[string]string) map[string][]string {
	resolved, created := []string{}, []string{}
	for id := range f.uids.Values() {
		resolved = append(resolved, id)
		if _, ok := before[id]; !ok {
			created = append(created, id)
		}
	}
	sort.Strings(resolved)
	sort.Strings(created)
	return map[string][]string{"resolved_after": resolved, "created": created}
}

// l2Events is shared by the upsertEvents goldens: existing and new subjects,
// fully populated, fully cleared, zero values, "{}" src_path, escaping, and
// an empty unique_id (skipped, events.go:412-414).
func l2Events() ([]eventRecord, map[string]string) {
	full := fullEventRecord("SYN-L2-E01")

	created := fullEventRecord("SYN-L2-E02")
	created.ProductName = "SYN \"quoted\" back\\slash\nline2 é 中 🙂"
	created.SrcPath = `{"a": ["SYN", "é"], "z": 1.10}`
	created.Amount = float64p(12345678901234567890.125)

	cleared := eventRecord{UniqueID: "SYN-L2-E03"}

	zeros := fullEventRecord("SYN-L2-E04")
	zeros.SrcPath = "{}"
	zeros.ProcessDate = int64p(0)
	zeros.Amount = float64p(0)

	partial := eventRecord{UniqueID: "SYN-L2-E05", ClientID: "SYN-CLIENT-01", ProcessDate: int64p(-1)}

	skipped := fullEventRecord("")

	existing := map[string]string{"SYN-L2-E01": "0x101", "SYN-L2-E03": "0x103", "SYN-L2-E04": "0x104"}
	return []eventRecord{full, created, cleared, zeros, partial, skipped}, existing
}

func recordsJSON(rs []eventRecord) []map[string]any {
	out := make([]map[string]any, 0, len(rs))
	for _, r := range rs {
		out = append(out, eventRecordJSON(r))
	}
	return out
}

func TestParityL2UpsertEvents(t *testing.T) {
	records, existing := l2Events()
	f := newL2Fixture(existing)
	cfg := singleBatchConfig()
	m := newParityMigrator(f.graph, nil, cfg)

	if err := m.upsertEvents(testContext(t), records, f.uids); err != nil {
		t.Fatal(err)
	}
	writes := f.writes(t)
	if len(writes) != 1 || writes[0].Op != "MutateWithDeletesRDF" {
		t.Fatalf("want one MutateWithDeletesRDF, got %+v", writes)
	}

	assertGolden(t, "l2/upsert_events.json", "l2.writes",
		"upsertEvents (incremental/events.go:392-657) over one batch: existing uid subjects, new blank-node subjects, "+
			"a fully cleared record, zero-valued numbers and a \"{}\" src_path, string escaping, and a record with an "+
			"empty unique_id (skipped). All sets and deletes travel in one MutateWithDeletesRDF.",
		compareWrites, map[string]any{
			"operation":           "upsertEvents",
			"config":              map[string]int{"upsert_batch_size": cfg.Events.UpsertBatchSize, "upsert_concurrency": cfg.Events.UpsertConcurrency},
			"existing_unique_ids": sortedKeys(existing),
			"input":               map[string]any{"records": recordsJSON(records)},
			"writes":              writes,
			"result":              f.uidResolution(existing),
		})
}

func TestParityL2UpsertEventsBatched(t *testing.T) {
	records, existing := l2Events()
	f := newL2Fixture(existing)
	cfg := singleBatchConfig()
	cfg.Events.UpsertBatchSize = 2
	m := newParityMigrator(f.graph, nil, cfg)

	if err := m.upsertEvents(testContext(t), records, f.uids); err != nil {
		t.Fatal(err)
	}
	writes := f.writes(t)
	if len(writes) != 3 {
		t.Fatalf("6 records in batches of 2 must give 3 mutations, got %d", len(writes))
	}

	assertGolden(t, "l2/upsert_events_batched.json", "l2.writes",
		"upsertEvents with upsert_batch_size=2 and one worker: records are cut into consecutive batches in input "+
			"order, one MutateWithDeletesRDF per batch; the skipped empty unique_id still occupies a batch slot.",
		compareWrites, map[string]any{
			"operation":           "upsertEvents",
			"config":              map[string]int{"upsert_batch_size": cfg.Events.UpsertBatchSize, "upsert_concurrency": cfg.Events.UpsertConcurrency},
			"existing_unique_ids": sortedKeys(existing),
			"input":               map[string]any{"records": recordsJSON(records)},
			"writes":              writes,
			"result":              f.uidResolution(existing),
		})
}

func TestParityL2UpsertLinks(t *testing.T) {
	existing := map[string]string{
		"SYN-L2-A": "0x201", "SYN-L2-B": "0x202", "SYN-L2-C": "0x203", "SYN-L2-D": "0x204",
	}
	records := []linkRecord{
		{ID: "1", Event1ID: "SYN-L2-A", Event2ID: "SYN-L2-B", Event1Version: "v1", Event2Version: "v2",
			Role: "SYN-input", Qty: float64p(2.5), Unit: "MT", Source: "SYN-LINKSRC"},
		{ID: "2", Event1ID: "SYN-L2-A", Event2ID: "SYN-L2-C"},
		{ID: "3", Event1ID: "SYN-L2-A", Event2ID: "SYN-L2-D", Qty: float64p(0), Role: `SYN "r" é`},
		{ID: "4", Event1ID: "SYN-L2-B", Event2ID: "SYN-L2-MISSING", Role: "skipped: event2 has no uid"},
		{ID: "5", Event1ID: "SYN-L2-MISSING", Event2ID: "SYN-L2-A", Role: "skipped: event1 has no uid"},
		{ID: "6", Event1ID: "SYN-L2-C", Event2ID: "SYN-L2-A", Qty: float64p(1e21), Unit: "KG"},
	}
	f := newL2Fixture(existing)
	cfg := singleBatchConfig()
	m := newParityMigrator(f.graph, nil, cfg)

	if err := m.upsertLinks(testContext(t), records, f.uids); err != nil {
		t.Fatal(err)
	}
	in := make([]map[string]any, 0, len(records))
	for _, r := range records {
		in = append(in, linkRecordJSON(r))
	}
	assertGolden(t, "l2/upsert_links.json", "l2.writes",
		"upsertLinks (incremental/links.go:311-354): one `<e1> <links> <e2> (facets) .` per record whose endpoints "+
			"both resolve to a uid; links never create nodes, so an unresolved endpoint drops the record silently. "+
			"No parentheses when no facet is present.",
		compareWrites, map[string]any{
			"operation":           "upsertLinks",
			"config":              map[string]int{"upsert_batch_size": cfg.Links.UpsertBatchSize, "upsert_concurrency": cfg.Links.UpsertConcurrency},
			"existing_unique_ids": sortedKeys(existing),
			"input":               map[string]any{"records": in},
			"writes":              f.writes(t),
		})
}

func TestParityL2DeleteExistingLinks(t *testing.T) {
	existing := map[string]string{"SYN-L2-A": "0x301", "SYN-L2-B": "0x302", "SYN-L2-C": "0x303"}
	event1IDs := stringSet{"SYN-L2-A": {}, "SYN-L2-B": {}, "SYN-L2-NEW": {}}
	f := newL2Fixture(existing)
	f.graph.seedEdge("0x301", "0x302", "")
	f.graph.seedEdge("0x302", "0x303", `role="SYN-input"`)
	f.graph.seedEdge("0x303", "0x301", "")
	cfg := singleBatchConfig()
	m := newParityMigrator(f.graph, nil, cfg)

	if err := m.deleteExistingLinks(testContext(t), f.uids, event1IDs); err != nil {
		t.Fatal(err)
	}
	snap, err := f.graph.snapshot()
	if err != nil {
		t.Fatal(err)
	}
	if got := edgeList(snap); strings.Join(got, ",") != "SYN-L2-C->SYN-L2-A" {
		t.Errorf("only C's edge should survive, got %v", got)
	}
	assertGolden(t, "l2/delete_existing_links.json", "l2.writes",
		"deleteExistingLinks (incremental/links.go:270-309): `<uid> <links> * .` for every event1 of the chunk that "+
			"already has a uid; event1s without one (new nodes) are skipped. Edges of other events are untouched.",
		compareWrites, map[string]any{
			"operation":           "deleteExistingLinks",
			"config":              map[string]int{"upsert_batch_size": cfg.Links.UpsertBatchSize, "upsert_concurrency": cfg.Links.UpsertConcurrency},
			"existing_unique_ids": sortedKeys(existing),
			"input":               map[string]any{"event1_ids": sortedKeys(event1IDs)},
			"writes":              f.writes(t),
			"edges_after":         edgeList(snap),
		})
}

func TestParityL2CleanupMissingLinks(t *testing.T) {
	existing := map[string]string{"SYN-L2-A": "0x401", "SYN-L2-B": "0x402", "SYN-L2-D": "0x404"}
	f := newL2Fixture(existing)
	events := &eventSyncResult{
		UniqueIDs: []string{"SYN-L2-A", "SYN-L2-B", "SYN-L2-C", "SYN-L2-D"},
		UIDs:      f.uids,
	}
	handled := stringSet{"SYN-L2-B": {}}
	cfg := singleBatchConfig()
	cfg.Links.CleanupBatchSize = 1
	m := newParityMigrator(f.graph, nil, cfg)

	if err := m.cleanupMissingLinks(testContext(t), events, handled); err != nil {
		t.Fatal(err)
	}
	assertGolden(t, "l2/cleanup_missing_links.json", "l2.writes",
		"cleanupMissingLinks (incremental/links.go:356-391): for each synced event, in order, that was not an event1 "+
			"of any fetched link and has a uid, `<uid> <links> * .`; cleanup_batch_size=1 gives one DeleteRDF each.",
		compareWrites, map[string]any{
			"operation":           "cleanupMissingLinks",
			"config":              map[string]int{"cleanup_batch_size": cfg.Links.CleanupBatchSize, "cleanup_concurrency": cfg.Links.CleanupConcurrency},
			"existing_unique_ids": sortedKeys(existing),
			"input":               map[string]any{"synced_unique_ids": events.UniqueIDs, "handled_event1_ids": sortedKeys(handled)},
			"writes":              f.writes(t),
		})
}

// --- P1-5: removal-path delete payloads ------------------------------------

func TestParityL2DeleteNodes(t *testing.T) {
	existing := map[string]string{"SYN-L2-X1": "0x501", "SYN-L2-X2": "0x502", "SYN-L2-X3": "0x503"}
	f := newL2Fixture(existing)
	cfg := singleBatchConfig()
	m := newParityMigrator(f.graph, nil, cfg)
	uids := []string{"0x501", "", "0x502", "0x503"}

	if err := m.deleteNodes(testContext(t), uids, 2, 1); err != nil {
		t.Fatal(err)
	}
	// Raw payloads are pinned too: deleteNodes joins with "\n" and adds no
	// trailing newline (graph_ops.go:72-77).
	var raw []string
	for _, c := range f.graph.calls {
		if c.Op != "DeleteRDF" || c.Set != "" {
			t.Fatalf("deleteNodes must only issue DeleteRDF, got %+v", c)
		}
		raw = append(raw, c.Del)
	}
	if want := []string{"<0x501> * * .", "<0x502> * * .\n<0x503> * * ."}; strings.Join(raw, "|") != strings.Join(want, "|") {
		t.Errorf("raw delete payloads = %q, want %q", raw, want)
	}
	snap, err := f.graph.snapshot()
	if err != nil {
		t.Fatal(err)
	}
	if len(snap) != 0 {
		t.Errorf("all three nodes should be gone, still have %v", sortedKeys(snap))
	}
	assertGolden(t, "l2/delete_nodes.json", "l2.writes",
		"deleteNodes (incremental/graph_ops.go:54-84), the removal path's delete: `<uid> * * .` per non-empty uid, "+
			"batch size 2, one worker. The empty uid still occupies a slot in its batch.",
		compareWrites, map[string]any{
			"operation":           "deleteNodes",
			"config":              map[string]int{"delete_batch_size": 2, "delete_concurrency": 1},
			"existing_unique_ids": sortedKeys(existing),
			"input":               map[string]any{"uids_by_unique_id": []string{"SYN-L2-X1", "", "SYN-L2-X2", "SYN-L2-X3"}},
			"writes":              f.writes(t),
			"raw_payloads":        raw,
		})
}

// TestParityL2RemoveInvalidEvents drives removeInvalidEvents end to end with
// scripted removalQuery rows: lookup, `<uid> * * .` for found nodes only, and
// the ids returned for sync_time stamping.
func TestParityL2RemoveInvalidEvents(t *testing.T) {
	existing := map[string]string{"SYN-L2-X1": "0x601", "SYN-L2-X2": "0x602", "SYN-L2-KEEP": "0x603"}
	f := newL2Fixture(existing)
	f.graph.seedEdge("0x603", "0x601", "")
	type row struct {
		LinkID   *int64  `json:"link_id"`
		UniqueID *string `json:"unique_id"`
	}
	rows := []row{
		{LinkID: int64p(7001)},
		{LinkID: int64p(7001)},
		{LinkID: int64p(7002)},
		{UniqueID: strp("SYN-L2-X1")},
		{UniqueID: strp("  SYN-L2-X2  ")},
		{UniqueID: strp("SYN-L2-NEVER-LOADED")},
		{UniqueID: strp("   ")},
	}
	db := newStubDB(func(query string, args []driver.NamedValue) (stubResult, error) {
		if query != removalQuery {
			return stubResult{}, fmt.Errorf("unexpected query %q", query)
		}
		res := stubResult{Columns: []string{"link_id", "unique_id"}}
		for _, r := range rows {
			res.Rows = append(res.Rows, []driver.Value{nullable(r.LinkID), nullable(r.UniqueID)})
		}
		return res, nil
	})
	cfg := singleBatchConfig()
	m := newParityMigrator(f.graph, db, cfg)
	f.graph.setStep("removal")

	result, err := m.removeInvalidEvents(testContext(t))
	if err != nil {
		t.Fatal(err)
	}
	sort.Slice(result.LinkIDs, func(i, j int) bool { return result.LinkIDs[i] < result.LinkIDs[j] })
	sort.Strings(result.UniqueIDs)

	writes := f.writes(t)
	if len(writes) != 1 || writes[0].Op != "DeleteRDF" || len(writes[0].Del) != 2 {
		t.Fatalf("want one DeleteRDF with two `* * .` lines, got %+v", writes)
	}
	snap, err := f.graph.snapshot()
	if err != nil {
		t.Fatal(err)
	}
	assertGolden(t, "l2/remove_invalid_events.json", "l2.writes",
		"removeInvalidEvents (incremental/removal.go:24-85): removalQuery rows -> trimmed, de-duplicated unique_ids -> "+
			"uid lookup -> one `<uid> * * .` per node found. Ids never loaded produce no delete but are still returned "+
			"for stamping; link ids are returned for stamping only (no graph write). Incoming edges to a deleted node "+
			"are not touched here (they are rewritten by the links path; see scenario S3).",
		compareWrites, map[string]any{
			"operation":             "removeInvalidEvents",
			"config":                configJSON(cfg)["removal"],
			"existing_unique_ids":   sortedKeys(existing),
			"input":                 map[string]any{"removal_query_rows": rows},
			"writes":                writes,
			"lookups_informational": f.graph.canonicalLookups(),
			"result":                map[string]any{"link_ids": result.LinkIDs, "unique_ids": result.UniqueIDs},
			"graph_after":           snap,
		})
}
