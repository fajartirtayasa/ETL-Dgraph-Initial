package incremental

// The four silent-corruption invariants (REFACTOR_PLAN.md P1-4). Each test
// cites the source it pins and is meant to fail when that source is broken;
// parity/README.md §Mutation evidence records the demonstration.

import (
	"database/sql/driver"
	"fmt"
	"sort"
	"strings"
	"testing"
)

// rawLines parses a raw payload without canonicalisation.
func rawLines(t *testing.T, payload string) []nquad {
	t.Helper()
	qs, err := parsePayload(payload)
	if err != nil {
		t.Fatal(err)
	}
	return qs
}

func predicatesOf(qs []nquad, subject string) []string {
	var out []string
	for _, q := range qs {
		if q.Subject == subject {
			out = append(out, strings.Trim(q.Predicate, "<>"))
		}
	}
	sort.Strings(out)
	return out
}

func upsertCalls(t *testing.T, records []eventRecord, existing map[string]string) (*fakeGraph, []graphCall) {
	t.Helper()
	f := newL2Fixture(existing)
	m := newParityMigrator(f.graph, nil, singleBatchConfig())
	if err := m.upsertEvents(testContext(t), records, f.uids); err != nil {
		t.Fatal(err)
	}
	return f.graph, f.graph.calls
}

// --- (a) set-or-delete per field, in the same mutation ----------------------
// events.go:428-638 emits, per field, a set triple when the value is present
// and `<s> <p> *` otherwise; events.go:645 sends both payloads through
// mutateSetAndDel -> MutateWithDeletesRDF (graph_ops.go:94-100), which puts
// SetNquads and DelNquads on one api.Mutation (pkg/dgraph/dgraph.go:346-350).

func TestInvariantA_UpsertCoversEverySchemaPredicate(t *testing.T) {
	var fromCode []string
	for _, fp := range eventFieldPredicates {
		fromCode = append(fromCode, fp.Predicate)
	}
	fromCode = append(fromCode, "unique_id", "links")
	sort.Strings(fromCode)
	if got, want := strings.Join(fromCode, " "), strings.Join(schemaEventPredicates(t), " "); got != want {
		t.Fatalf("upsertEvents predicates + links differ from schema.dql type Event:\ncode:   %s\nschema: %s", got, want)
	}
}

func TestInvariantA_EachClearedFieldIsDeletedInTheSameMutation(t *testing.T) {
	const uid = "0x1"
	for _, fp := range eventFieldPredicates {
		t.Run(fp.Predicate, func(t *testing.T) {
			r := fullEventRecord("SYN-INV-A")
			clearField(&r, fp.Field)
			_, calls := upsertCalls(t, []eventRecord{r}, map[string]string{"SYN-INV-A": uid})

			if len(calls) != 1 || calls[0].Op != "MutateWithDeletesRDF" {
				t.Fatalf("want exactly one MutateWithDeletesRDF, got %d calls %+v", len(calls), calls)
			}
			del := rawLines(t, calls[0].Del)
			if len(del) != 1 || del[0].String() != fmt.Sprintf("<%s> <%s> * .", uid, fp.Predicate) {
				t.Fatalf("del payload = %q, want only `<%s> <%s> * .`", calls[0].Del, uid, fp.Predicate)
			}
			set := predicatesOf(rawLines(t, calls[0].Set), "<"+uid+">")
			if containsString(set, fp.Predicate) {
				t.Errorf("cleared %s is also set", fp.Predicate)
			}
			if len(set) != len(eventFieldPredicates)+1 { // 41 others + dgraph.type + unique_id
				t.Errorf("set carries %d predicates, want %d: %v", len(set), len(eventFieldPredicates)+1, set)
			}
		})
	}
}

func TestInvariantA_AllClearedAndAllPresent(t *testing.T) {
	_, calls := upsertCalls(t, []eventRecord{{UniqueID: "SYN-INV-A"}}, map[string]string{"SYN-INV-A": "0x1"})
	if len(calls) != 1 || calls[0].Op != "MutateWithDeletesRDF" {
		t.Fatalf("want one MutateWithDeletesRDF, got %+v", calls)
	}
	if got := predicatesOf(rawLines(t, calls[0].Set), "<0x1>"); strings.Join(got, ",") != "dgraph.type,unique_id" {
		t.Errorf("an all-empty record must set only dgraph.type and unique_id, got %v", got)
	}
	del := rawLines(t, calls[0].Del)
	if len(del) != len(eventFieldPredicates) {
		t.Errorf("an all-empty record must delete all %d optional predicates, got %d", len(eventFieldPredicates), len(del))
	}
	for _, q := range del {
		if q.Object != "*" {
			t.Errorf("delete %q is not a `<s> <p> *` wildcard", q.String())
		}
	}

	_, calls = upsertCalls(t, []eventRecord{fullEventRecord("SYN-INV-A")}, map[string]string{"SYN-INV-A": "0x1"})
	if len(calls) != 1 || calls[0].Op != "MutateWithDeletesRDF" || calls[0].Del != "" {
		t.Errorf("a fully populated record must send an empty del payload, got %+v", calls)
	}
}

func TestInvariantA_MixedBatchStaysOneMutation(t *testing.T) {
	cleared := fullEventRecord("SYN-INV-A1")
	cleared.Plant = ""
	present := fullEventRecord("SYN-INV-A2")
	_, calls := upsertCalls(t, []eventRecord{cleared, present},
		map[string]string{"SYN-INV-A1": "0x1", "SYN-INV-A2": "0x2"})
	if len(calls) != 1 {
		t.Fatalf("one batch must be one mutation, got %d", len(calls))
	}
	if !strings.Contains(calls[0].Del, "<0x1> <plant> * .") {
		t.Error("record 1's cleared plant is missing from the mutation's del payload")
	}
	if !strings.Contains(calls[0].Set, "<0x2> <plant> ") {
		t.Error("record 2's plant is missing from the mutation's set payload")
	}
}

func TestInvariantA_ClearedValueIsRemovedFromGraph(t *testing.T) {
	f := newL2Fixture(nil)
	f.graph.seedNode("0x1", "SYN-INV-A", map[string][]string{
		"plant":  {`"SYN-PL-OLD"`},
		"amount": {`"1"^^<http://www.w3.org/2001/XMLSchema#float>`},
	})
	f.uids.Set("SYN-INV-A", "0x1")
	r := fullEventRecord("SYN-INV-A")
	r.Plant = ""
	r.Amount = nil
	m := newParityMigrator(f.graph, nil, singleBatchConfig())
	if err := m.upsertEvents(testContext(t), []eventRecord{r}, f.uids); err != nil {
		t.Fatal(err)
	}
	snap, err := f.graph.snapshot()
	if err != nil {
		t.Fatal(err)
	}
	for _, p := range []string{"plant", "amount"} {
		if v, ok := snap["SYN-INV-A"].Predicates[p]; ok {
			t.Errorf("%s still holds %v after being cleared in Postgres", p, v)
		}
	}
}

// --- (b) link replacement chunked by event1_id, never by link row ------------
// syncLinks chunks the distinct event1 ids (links.go:113-126), fetches every
// link of the chunk's event1s (linkDetailsQuery `event1_id = ANY($1)`,
// links.go:67), deletes all of their links (links.go:150) and only then
// re-adds the valid ones (links.go:154-158).

type stubLink struct {
	ID               int64
	Event1, Event2   string
	Status, E2Status string
	Qty              float64
}

func linkStub(event1IDs []string, links []stubLink) stubQueryFunc {
	return func(query string, args []driver.NamedValue) (stubResult, error) {
		switch query {
		case linkEvent1IDsQuery:
			res := stubResult{Columns: []string{"event1_id"}}
			for _, id := range event1IDs {
				res.Rows = append(res.Rows, []driver.Value{id})
			}
			return res, nil
		case linkDetailsQuery:
			ids, err := stringsArg(args)
			if err != nil {
				return stubResult{}, err
			}
			want := map[string]bool{}
			for _, id := range ids {
				want[id] = true
			}
			res := stubResult{Columns: []string{"id", "event1_id", "event2_id", "event1_version", "event2_version",
				"role", "qty", "unit", "source", "status", "event2_status"}}
			for _, l := range links {
				if want[l.Event1] {
					res.Rows = append(res.Rows, []driver.Value{l.ID, l.Event1, l.Event2, "v1", "v1",
						"SYN-input", l.Qty, "MT", "SYN-LINKSRC", l.Status, l.E2Status})
				}
			}
			return res, nil
		}
		return stubResult{}, fmt.Errorf("unexpected query %q", query)
	}
}

func TestInvariantB_LinkReplacementIsGroupedByEvent1(t *testing.T) {
	nodes := []string{"E1", "E2", "E3", "X", "Y", "Z", "W", "Q"}
	links := []stubLink{
		{1, "SYN-B-E1", "SYN-B-X", "valid", "valid", 1},
		{2, "SYN-B-E1", "SYN-B-Y", "valid", "valid", 2},
		{3, "SYN-B-E1", "SYN-B-Z", "valid", "valid", 3},
		{4, "SYN-B-E1", "SYN-B-W", "invalid", "valid", 4},
		{5, "SYN-B-E2", "SYN-B-X", "valid", "valid", 5},
		{6, "SYN-B-E2", "SYN-B-Q", "valid", "invalid", 6},
		{7, "SYN-B-E3", "SYN-B-Y", "invalid", "valid", 7},
	}
	want := "SYN-B-E1->SYN-B-X,SYN-B-E1->SYN-B-Y,SYN-B-E1->SYN-B-Z,SYN-B-E2->SYN-B-X"

	configs := map[string]func(*Config){
		"fetch1_batch1_conc1":   func(c *Config) { c.FetchChunkSize, c.Links.UpsertBatchSize = 1, 1 },
		"fetch1_batch1_conc4":   func(c *Config) { c.FetchChunkSize, c.Links.UpsertBatchSize, c.Links.UpsertConcurrency = 1, 1, 4 },
		"fetch2_batch2_conc2":   func(c *Config) { c.FetchChunkSize, c.Links.UpsertBatchSize, c.Links.UpsertConcurrency = 2, 2, 2 },
		"fetch1_batch100_conc1": func(c *Config) { c.FetchChunkSize, c.Links.UpsertBatchSize = 1, 100 },
		"single_batch":          func(*Config) {},
	}
	for name, tweak := range configs {
		t.Run(name, func(t *testing.T) {
			// stringSet iteration order is random, so repeat to vary chunk composition.
			for round := 0; round < 8; round++ {
				g := newFakeGraph()
				for i, n := range nodes {
					g.seedNode(fmt.Sprintf("0x%x", 0x700+i), "SYN-B-"+n, nil)
				}
				uid := func(n string) string { u, _ := g.uidOf("SYN-B-" + n); return u }
				g.seedEdge(uid("E1"), uid("W"), "")
				g.seedEdge(uid("E1"), uid("Q"), "")
				g.seedEdge(uid("E2"), uid("Y"), "")
				g.seedEdge(uid("E3"), uid("Z"), "")

				cfg := singleBatchConfig()
				tweak(&cfg)
				db := newStubDB(linkStub([]string{"SYN-B-E1", "SYN-B-E2", "SYN-B-E3"}, links))
				m := newParityMigrator(g, db, cfg)
				ids, err := m.syncLinks(testContext(t), &eventSyncResult{UIDs: newUIDMap(nil)})
				if err != nil {
					t.Fatal(err)
				}
				if len(ids) != len(links) {
					t.Errorf("every fetched link id must be returned for stamping: got %v", ids)
				}
				snap, err := g.snapshot()
				if err != nil {
					t.Fatal(err)
				}
				if got := strings.Join(edgeList(snap), ","); got != want {
					t.Fatalf("round %d: edges after sync\n got: %s\nwant: %s", round, got, want)
				}
				assertDeleteBeforeAdd(t, g.calls)
			}
		})
	}
}

// assertDeleteBeforeAdd checks no link is added for an event1 before that
// event1's `<links> *` delete, and that the delete is never repeated after
// an add (which would erase links written by an earlier chunk).
func assertDeleteBeforeAdd(t *testing.T, calls []graphCall) {
	t.Helper()
	deleted := map[string]bool{}
	added := map[string]bool{}
	for i, c := range calls {
		switch c.Op {
		case "DeleteRDF":
			for _, q := range rawLines(t, c.Del) {
				if q.Predicate == "<links>" {
					if added[q.Subject] {
						t.Errorf("call %d deletes links of %s after links were already added to it", i, q.Subject)
					}
					deleted[q.Subject] = true
				}
			}
		case "MutateRDF":
			for _, q := range rawLines(t, c.Set) {
				if !deleted[q.Subject] {
					t.Errorf("call %d adds %s before that event1's links were deleted", i, q.String())
				}
				added[q.Subject] = true
			}
		}
	}
}

// --- (c) facets only for non-empty values -----------------------------------
// buildLinkFacets (links.go:393-416); upsertLinks omits the parentheses when
// it returns "" (links.go:338-343).

var facetKeys = []string{"event1_version", "event2_version", "role", "qty", "unit", "source"}

func linkWithOnly(keys map[string]bool) linkRecord {
	r := linkRecord{}
	if keys["event1_version"] {
		r.Event1Version = "v1"
	}
	if keys["event2_version"] {
		r.Event2Version = "v2"
	}
	if keys["role"] {
		r.Role = "SYN-input"
	}
	if keys["qty"] {
		r.Qty = float64p(0)
	}
	if keys["unit"] {
		r.Unit = "MT"
	}
	if keys["source"] {
		r.Source = "SYN-LINKSRC"
	}
	return r
}

func TestInvariantC_FacetEmittedIffValuePresent(t *testing.T) {
	for _, key := range facetKeys {
		t.Run(key, func(t *testing.T) {
			only, err := parseFacets(buildLinkFacets(linkWithOnly(map[string]bool{key: true})))
			if err != nil {
				t.Fatal(err)
			}
			if got := sortedKeys(only); strings.Join(got, ",") != key {
				t.Errorf("only %s present: facets = %v", key, got)
			}

			others := map[string]bool{}
			for _, k := range facetKeys {
				others[k] = k != key
			}
			rest, err := parseFacets(buildLinkFacets(linkWithOnly(others)))
			if err != nil {
				t.Fatal(err)
			}
			if _, ok := rest[key]; ok {
				t.Errorf("%s absent but emitted: %v", key, rest)
			}
			if len(rest) != len(facetKeys)-1 {
				t.Errorf("all but %s present: got %v", key, sortedKeys(rest))
			}
		})
	}
	if got := buildLinkFacets(linkRecord{Qty: float64p(0)}); got != "qty=0" {
		t.Errorf("qty is non-empty when non-NULL, including zero: got %q", got)
	}
}

func TestInvariantC_NoParenthesesWithoutFacets(t *testing.T) {
	f := newL2Fixture(map[string]string{"SYN-C-A": "0x1", "SYN-C-B": "0x2"})
	m := newParityMigrator(f.graph, nil, singleBatchConfig())
	rec := linkRecord{ID: "1", Event1ID: "SYN-C-A", Event2ID: "SYN-C-B"}
	if err := m.upsertLinks(testContext(t), []linkRecord{rec}, f.uids); err != nil {
		t.Fatal(err)
	}
	if len(f.graph.calls) != 1 || f.graph.calls[0].Set != "<0x1> <links> <0x2> .\n" {
		t.Fatalf("got %+v", f.graph.calls)
	}
}

// --- (d) existing uid vs newly minted blank node ----------------------------
// events.go:416-423 picks `<uid>` when the uid map holds a non-empty uid and
// common.BlankNode otherwise, remembering blank -> unique_id; events.go:650-654
// merges the uids Dgraph returns for those blanks back into the map, which
// upsertLinks (links.go:325-332) then uses. Links never mint blank nodes.

func subjectsByUniqueID(t *testing.T, set string) map[string]string {
	t.Helper()
	out := map[string]string{}
	for _, q := range rawLines(t, set) {
		if q.Predicate == "<unique_id>" {
			id, err := literalValue(q.Object)
			if err != nil {
				t.Fatal(err)
			}
			if prev, ok := out[id]; ok && prev != q.Subject {
				t.Errorf("unique_id %s has two subjects %s and %s in one mutation", id, prev, q.Subject)
			}
			out[id] = q.Subject
		}
	}
	return out
}

func TestInvariantD_SubjectSelection(t *testing.T) {
	existing := map[string]string{"SYN-D-E1": "0x1", "SYN-D-E3": "0x3"}
	ids := []string{"SYN-D-E1", "SYN-D-E2", "SYN-D-E3", "SYN-D-E4",
		"SYN-D-A-1", "SYN-D-A_1", "SYN-D-A.1", "SYN-D-A 1", "SYN-D-A--1", "SYNDA1"}
	var records []eventRecord
	for _, id := range ids {
		records = append(records, fullEventRecord(id))
	}
	_, calls := upsertCalls(t, records, existing)
	if len(calls) != 1 {
		t.Fatalf("want one mutation, got %d", len(calls))
	}
	subjects := subjectsByUniqueID(t, calls[0].Set)

	labels := map[string]string{}
	for _, id := range ids {
		s := subjects[id]
		if uid, ok := existing[id]; ok {
			if s != "<"+uid+">" {
				t.Errorf("%s has uid %s but subject %s", id, uid, s)
			}
			continue
		}
		if !strings.HasPrefix(s, "_:") {
			t.Errorf("%s has no uid but subject %s is not a blank node", id, s)
			continue
		}
		if prev, ok := labels[s]; ok {
			t.Errorf("blank label %s shared by %s and %s", s, prev, id)
		}
		labels[s] = id
	}
	// Every triple of a record uses that record's one subject.
	all := append(rawLines(t, calls[0].Set), rawLines(t, calls[0].Del)...)
	perSubject := map[string]int{}
	for _, q := range all {
		perSubject[q.Subject]++
	}
	for _, id := range ids {
		if n := perSubject[subjects[id]]; n != len(eventFieldPredicates)+2 {
			t.Errorf("%s: subject %s carries %d triples, want %d", id, subjects[id], n, len(eventFieldPredicates)+2)
		}
	}
}

func TestInvariantD_BlankNodeUIDRoundTrip(t *testing.T) {
	existing := map[string]string{"SYN-D-E1": "0x1"}
	f := newL2Fixture(existing)
	m := newParityMigrator(f.graph, nil, singleBatchConfig())
	records := []eventRecord{fullEventRecord("SYN-D-E1"), fullEventRecord("SYN-D-E2"), fullEventRecord("SYN-D-A-1")}
	if err := m.upsertEvents(testContext(t), records, f.uids); err != nil {
		t.Fatal(err)
	}

	if uid, _ := f.uids.Get("SYN-D-E1"); uid != "0x1" {
		t.Errorf("existing uid replaced: SYN-D-E1 -> %q", uid)
	}
	minted := map[string]string{}
	for _, id := range []string{"SYN-D-E2", "SYN-D-A-1"} {
		got, ok := f.uids.Get(id)
		want, created := f.graph.uidOf(id)
		if !ok || !created || got != want {
			t.Errorf("%s: uid map holds %q (ok=%v), graph created %q", id, got, ok, want)
		}
		minted[id] = got
	}
	if minted["SYN-D-E2"] == minted["SYN-D-A-1"] {
		t.Error("two new events received the same uid")
	}

	// The returned uids are what later links use; links never mint blank nodes.
	f.graph.calls = nil
	links := []linkRecord{
		{ID: "1", Event1ID: "SYN-D-E2", Event2ID: "SYN-D-E1"},
		{ID: "2", Event1ID: "SYN-D-A-1", Event2ID: "SYN-D-E2"},
		{ID: "3", Event1ID: "SYN-D-E1", Event2ID: "SYN-D-NOT-SYNCED"},
	}
	if err := m.upsertLinks(testContext(t), links, f.uids); err != nil {
		t.Fatal(err)
	}
	want := fmt.Sprintf("<%s> <links> <0x1> .\n<%s> <links> <%s> .\n", minted["SYN-D-E2"], minted["SYN-D-A-1"], minted["SYN-D-E2"])
	if len(f.graph.calls) != 1 || f.graph.calls[0].Set != want {
		t.Errorf("links payload\n got: %q\nwant: %q", f.graph.calls, want)
	}
}
