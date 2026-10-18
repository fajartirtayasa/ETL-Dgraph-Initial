// Command gen generates the synthetic parity fixture (REFACTOR_PLAN.md P1-6,
// P1-7). Every row it emits is authored here; nothing is extracted from
// Staging or Production (PD-3).
//
// Run from the repository root:
//
//	go run ./parity/fixture/gen
//
// It rewrites, deterministically:
//
//	parity/fixture/sql/02_seed.sql   Postgres rows for all seven scenarios
//	parity/fixture/graph_seed.json   the Dgraph state those rows imply was
//	                                 already loaded before the incremental run
//	parity/fixture/scenarios.json    scenario manifest plus hand-derived
//	                                 expectations the L3 tests assert
package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"

	"hara/traceability-dgraph-etl/internal/etl/common"
)

const (
	syncedAt      = "2026-01-01 00:00:00+00"
	formatSeed    = "dgraph-etl-parity/graph-seed/v1"
	formatSummary = "dgraph-etl-parity/scenarios/v1"
)

// eventColumns is the INSERT column order for events_bc (sql/01_schema.sql).
var eventColumns = []string{
	"unique_id", "client_id", "bl_number", "sales_order", "product_id",
	"product_name", "plant", "production_label", "process_date",
	"material_type", "material_code", "amount", "unit", "src_path",
	"sub_data_source", "version", "flag", "material_document",
	"purchase_order", "uml_id", "location_id", "vendor", "source",
	"movement_type", "movement_type_text", "storage_location", "partner_pca",
	"mill_name", "status", "sync_time",
}

type row map[string]*string

func s(v string) *string { return &v }

type event struct {
	ID      string
	Now     row  // current Postgres row (without unique_id / sync_time)
	Before  row  // values already in Dgraph; nil means "same as Now"
	Synced  bool // sync_time IS NOT NULL
	InGraph bool // present in graph_seed.json before the run
}

type link struct {
	ID      int64
	Event1  string
	Event2  string
	Cols    row // role, qty, unit, source, status, event1_version, event2_version
	Synced  bool
	InGraph bool
}

type expect struct {
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

type scenario struct {
	ID          string `json:"id"`
	Key         string `json:"key"`
	Title       string `json:"title"`
	RowPrefix   string `json:"row_prefix"`
	Description string `json:"description"`
	Expect      expect `json:"expect"`
	events      []event
	links       []link
}

type subject struct{ ID, MasterType, SubjectType, MasterID string }
type hierarchy struct{ Start, SubjectID, SubjectType, MasterID, MasterType string }
type mill struct{ MillID, UmlID string }

var subjects = []subject{
	{"SYN-LOC-001", "syn_mill", "SYN_MILL", "SYN-MST-001"},
	{"SYN-LOC-002", "syn_refinery", "SYN_REFINERY", "SYN-MST-002"},
	{"SYN-SRC-001", "syn_supplier", "SYN_SUPPLIER", "SYN-MST-003"},
}

var hierarchies = []hierarchy{
	{"SYN-LOC-001", "SYN-FAC-001", "SYN_FACILITY", "SYN-MST-F01", "syn_facility"},
	{"SYN-LOC-002", "SYN-FAC-002", "SYN_FACILITY", "SYN-MST-F02", "syn_facility"},
}

// The duplicated SYN-MILL-002 row exercises the GROUP BY in the MasterMill
// CTE (events.go:52-56): it must not fan an event out into two rows.
var mills = []mill{
	{"SYN-MILL-001", "SYN-UML-001"},
	{"SYN-MILL-002", "SYN-UML-002"},
	{"SYN-MILL-002", "SYN-UML-002"},
}

func base() row {
	return row{
		"client_id":          s("SYN-CLIENT-01"),
		"bl_number":          s("SYN-BL-0001"),
		"sales_order":        s("SYN-SO-0001"),
		"product_id":         s("SYN-PRD-01"),
		"product_name":       s("SYN Product"),
		"plant":              s("SYN-PL01"),
		"production_label":   s("SYN-PLB-01"),
		"process_date":       s("1700000000"),
		"material_type":      s("SYN-MT"),
		"material_code":      s("SYN-MC-01"),
		"amount":             s("10.5"),
		"unit":               s("MT"),
		"src_path":           s(`{"step": 1}`),
		"sub_data_source":    s("SYN-SDS"),
		"version":            s("v1"),
		"flag":               s("SYN-FLAG"),
		"material_document":  s("SYN-MD-01"),
		"purchase_order":     s("SYN-PO-01"),
		"uml_id":             s("SYN-UML-001"),
		"location_id":        s("SYN-LOC-001"),
		"vendor":             s("SYN-VEN-01"),
		"source":             s("SYN-SRC-001"),
		"movement_type":      s("SYN-101"),
		"movement_type_text": s("SYN goods receipt"),
		"storage_location":   s("SYN-SL01"),
		"partner_pca":        s("SYN-PCA"),
		"mill_name":          s("SYN Mill One"),
		"status":             s("valid"),
	}
}

func with(r row, overrides row) row {
	out := row{}
	for k, v := range r {
		out[k] = v
	}
	for k, v := range overrides {
		out[k] = v
	}
	return out
}

func linkCols(overrides row) row {
	return with(row{
		"role":           s("SYN-input"),
		"qty":            s("5"),
		"unit":           s("MT"),
		"source":         s("SYN-LINKSRC"),
		"status":         s("valid"),
		"event1_version": s("v1"),
		"event2_version": s("v1"),
	}, overrides)
}

func scenarios() []scenario {
	invalid := row{"status": s("invalid")}
	return []scenario{
		{
			ID: "S1", Key: "new_event", Title: "New event",
			Description: "S1-E01 is valid, unsynced and absent from Dgraph; an unsynced valid link S1-E01 -> S1-E02 makes it eligible. " +
				"It must be created through a blank node whose returned uid is then used by the link. Its values exercise string " +
				"escaping (quotes, backslash, newline, non-ASCII), whitespace trimming, a NULL nullable column, a non-trivial jsonb " +
				"src_path and a numeric beyond float64 precision.",
			events: []event{
				{ID: "SYN-S1-E01", Now: with(base(), row{
					"product_name": s("SYN \"quoted\" back\\slash\nline2 é 中 🙂"),
					"plant":        s("  SYN-PL01  "),
					"src_path":     s(`{"z": 1.10, "a": ["SYN", "é"], "nested": {"k": null}}`),
					"amount":       s("12345678901234567890.125"),
					"version":      nil,
				})},
				{ID: "SYN-S1-E02", Now: base(), Synced: true, InGraph: true},
			},
			links: []link{
				{ID: 910001, Event1: "SYN-S1-E01", Event2: "SYN-S1-E02", Cols: linkCols(row{"qty": s("2.5")})},
			},
			Expect: expect{
				EventIDs: []string{"SYN-S1-E01", "SYN-S1-E02"}, LinkEvent1IDs: []string{"SYN-S1-E01"},
				StampedEvents: []string{"SYN-S1-E01"}, StampedLinks: []int64{910001},
				CreatedNodes: []string{"SYN-S1-E01"}, EdgesAfter: []string{"SYN-S1-E01->SYN-S1-E02"},
			},
		},
		{
			ID: "S2", Key: "updated_event", Title: "Updated event",
			Description: "S2-E01 is already in Dgraph and was updated in Postgres: one value changed, values cleared by NULL, " +
				"by an empty string, by whitespace only and by an empty jsonb object, a join-derived value changed (uml_id -> mill_id) " +
				"and a join-derived group cleared (location_id with no subject). Every cleared field must produce `<s> <p> *` in the " +
				"same mutation as the sets.",
			events: []event{
				{ID: "SYN-S2-E01", Now: with(base(), row{
					"bl_number":    s("SYN-BL-NEW"),
					"plant":        nil,
					"product_name": s("   "),
					"src_path":     s("{}"),
					"process_date": s("1650000000"),
					"amount":       s("3.75"),
					"uml_id":       s("SYN-UML-002"),
					"mill_name":    nil,
					"vendor":       s(""),
					"location_id":  s("SYN-LOC-404"),
				}), Before: base(), InGraph: true},
				{ID: "SYN-S2-E02", Now: base(), Synced: true, InGraph: true},
			},
			links: []link{
				{ID: 920001, Event1: "SYN-S2-E01", Event2: "SYN-S2-E02", Cols: linkCols(nil), Synced: true, InGraph: true},
			},
			Expect: expect{
				EventIDs: []string{"SYN-S2-E01"}, LinkEvent1IDs: []string{"SYN-S2-E01"},
				StampedEvents: []string{"SYN-S2-E01"},
				EdgesAfter:    []string{"SYN-S2-E01->SYN-S2-E02"},
			},
		},
		{
			ID: "S3", Key: "invalidated_event", Title: "Invalidated event",
			Description: "S3-E01 is in Dgraph and now status='invalid': the removal path must emit `<uid> * * .`. Its incoming " +
				"edge (S3-E02 -> S3-E01) is removed by the links path rewriting S3-E02, which keeps S3-E02 -> S3-E03; its outgoing " +
				"edge disappears with the node and is not resurrected. S3-E05 is invalid but was never loaded: no delete, still stamped.",
			events: []event{
				{ID: "SYN-S3-E01", Now: with(base(), invalid), Before: base(), InGraph: true},
				{ID: "SYN-S3-E02", Now: base(), Synced: true, InGraph: true},
				{ID: "SYN-S3-E03", Now: base(), Synced: true, InGraph: true},
				{ID: "SYN-S3-E04", Now: base(), Synced: true, InGraph: true},
				{ID: "SYN-S3-E05", Now: with(base(), invalid)},
			},
			links: []link{
				{ID: 930001, Event1: "SYN-S3-E02", Event2: "SYN-S3-E01", Cols: linkCols(nil), Synced: true, InGraph: true},
				{ID: 930002, Event1: "SYN-S3-E02", Event2: "SYN-S3-E03", Cols: linkCols(nil), Synced: true, InGraph: true},
				{ID: 930003, Event1: "SYN-S3-E01", Event2: "SYN-S3-E04", Cols: linkCols(nil), Synced: true, InGraph: true},
			},
			Expect: expect{
				LinkEvent1IDs:    []string{"SYN-S3-E01", "SYN-S3-E02"},
				RemovalUniqueIDs: []string{"SYN-S3-E01", "SYN-S3-E05"},
				StampedEvents:    []string{"SYN-S3-E01", "SYN-S3-E05"},
				DeletedNodes:     []string{"SYN-S3-E01"},
				EdgesAfter:       []string{"SYN-S3-E02->SYN-S3-E03"},
			},
		},
		{
			ID: "S4", Key: "link_added", Title: "Link added",
			Description: "A new unsynced link S4-E01 -> S4-E02 joins two synced events. S4-E01 already has two links, so its whole " +
				"link set is deleted and re-added (three links). The new link's facets exercise omission: NULL and empty versions " +
				"and a whitespace-only role are omitted, while qty=0 is emitted.",
			events: []event{
				{ID: "SYN-S4-E01", Now: base(), Synced: true, InGraph: true},
				{ID: "SYN-S4-E02", Now: base(), Synced: true, InGraph: true},
				{ID: "SYN-S4-E03", Now: base(), Synced: true, InGraph: true},
				{ID: "SYN-S4-E04", Now: base(), Synced: true, InGraph: true},
			},
			links: []link{
				{ID: 940001, Event1: "SYN-S4-E01", Event2: "SYN-S4-E03", Cols: linkCols(row{"qty": s("1.25")}), Synced: true, InGraph: true},
				{ID: 940002, Event1: "SYN-S4-E01", Event2: "SYN-S4-E04", Cols: linkCols(row{"qty": s("7")}), Synced: true, InGraph: true},
				{ID: 940003, Event1: "SYN-S4-E01", Event2: "SYN-S4-E02", Cols: linkCols(row{
					"event1_version": nil, "event2_version": s(""), "role": s("  "), "qty": s("0"), "unit": s("KG"),
				})},
			},
			Expect: expect{
				EventIDs: []string{"SYN-S4-E01", "SYN-S4-E02"}, LinkEvent1IDs: []string{"SYN-S4-E01"},
				StampedLinks: []int64{940003},
				EdgesAfter:   []string{"SYN-S4-E01->SYN-S4-E02", "SYN-S4-E01->SYN-S4-E03", "SYN-S4-E01->SYN-S4-E04"},
			},
		},
		{
			ID: "S5", Key: "link_invalidated", Title: "Link invalidated",
			Description: "Link S5-E01 -> S5-E02 is now status='invalid' and unsynced: removalQuery returns its id (stamp only) and " +
				"the links path drops the edge while keeping S5-E01 -> S5-E03. Link S5-E04 -> S5-E03 has status NULL and is " +
				"unsynced: removalQuery ignores it, but the links path drops its edge and stamps it.",
			events: []event{
				{ID: "SYN-S5-E01", Now: base(), Synced: true, InGraph: true},
				{ID: "SYN-S5-E02", Now: base(), Synced: true, InGraph: true},
				{ID: "SYN-S5-E03", Now: base(), Synced: true, InGraph: true},
				{ID: "SYN-S5-E04", Now: base(), Synced: true, InGraph: true},
			},
			links: []link{
				{ID: 950001, Event1: "SYN-S5-E01", Event2: "SYN-S5-E02", Cols: linkCols(row{"status": s("invalid")}), InGraph: true},
				{ID: 950002, Event1: "SYN-S5-E01", Event2: "SYN-S5-E03", Cols: linkCols(nil), Synced: true, InGraph: true},
				{ID: 950003, Event1: "SYN-S5-E04", Event2: "SYN-S5-E03", Cols: linkCols(row{"status": nil}), InGraph: true},
			},
			Expect: expect{
				EventIDs:       []string{"SYN-S5-E01", "SYN-S5-E02", "SYN-S5-E03", "SYN-S5-E04"},
				LinkEvent1IDs:  []string{"SYN-S5-E01", "SYN-S5-E04"},
				RemovalLinkIDs: []int64{950001},
				StampedLinks:   []int64{950001, 950003},
				EdgesAfter:     []string{"SYN-S5-E01->SYN-S5-E03"},
			},
		},
		{
			ID: "S6", Key: "event2_only_change", Title: "Event2-only change",
			Description: "S6-E02 changed in Postgres and appears only as event2 (of the synced link S6-E01 -> S6-E02). It is " +
				"upserted, and S6-E01 is pulled into the links path by linkEvent1IDsQuery's third UNION branch and rewritten.",
			events: []event{
				{ID: "SYN-S6-E01", Now: base(), Synced: true, InGraph: true},
				{ID: "SYN-S6-E02", Now: with(base(), row{"version": s("v2"), "product_name": s("SYN Product v2")}), Before: base(), InGraph: true},
			},
			links: []link{
				{ID: 960001, Event1: "SYN-S6-E01", Event2: "SYN-S6-E02", Cols: linkCols(nil), Synced: true, InGraph: true},
			},
			Expect: expect{
				EventIDs: []string{"SYN-S6-E02"}, LinkEvent1IDs: []string{"SYN-S6-E01"},
				StampedEvents: []string{"SYN-S6-E02"},
				EdgesAfter:    []string{"SYN-S6-E01->SYN-S6-E02"},
			},
		},
		{
			ID: "S7", Key: "event_without_links", Title: "Event with no links",
			Description: "S7-E01 is new and S7-E02 is an updated in-graph event; neither has any link. eventIDsQuery requires a " +
				"valid link, so neither is written to Dgraph and neither is stamped: both stay pending (sync_time NULL).",
			events: []event{
				{ID: "SYN-S7-E01", Now: base()},
				{ID: "SYN-S7-E02", Now: with(base(), row{"product_name": s("SYN Product changed")}), Before: base(), InGraph: true},
			},
		},
	}
}

func main() {
	out := flag.String("out", "parity/fixture", "fixture directory, relative to the working directory")
	flag.Parse()

	files, err := render()
	if err != nil {
		log.Fatal(err)
	}
	for name, content := range files {
		path := filepath.Join(*out, filepath.FromSlash(name))
		if err := os.WriteFile(path, content, 0o644); err != nil {
			log.Fatal(err)
		}
		fmt.Println("wrote", path)
	}
}

// render returns every generated file keyed by its path relative to the
// fixture directory.
func render() (map[string][]byte, error) {
	all := scenarios()
	for _, sc := range all {
		if err := validate(sc); err != nil {
			return nil, err
		}
	}

	seed, err := renderSeed(all)
	if err != nil {
		return nil, err
	}
	graph, err := renderGraphSeed(all)
	if err != nil {
		return nil, err
	}
	manifest, err := renderManifest(all)
	if err != nil {
		return nil, err
	}
	return map[string][]byte{
		"sql/02_seed.sql": seed,
		"graph_seed.json": graph,
		"scenarios.json":  manifest,
	}, nil
}

func validate(sc scenario) error {
	prefix := "SYN-" + sc.ID + "-"
	for _, e := range sc.events {
		if !strings.HasPrefix(e.ID, prefix) {
			return fmt.Errorf("%s: event %s lacks prefix %s", sc.ID, e.ID, prefix)
		}
	}
	for _, l := range sc.links {
		if !strings.HasPrefix(l.Event1, prefix) || !strings.HasPrefix(l.Event2, prefix) {
			return fmt.Errorf("%s: link %d crosses scenarios", sc.ID, l.ID)
		}
	}
	return nil
}

// sqlLiteral quotes a value for the seed. Values holding control characters
// use an E” escape string, so the seed file never carries a raw newline or
// carriage return inside a literal that a checkout's line-ending conversion
// could rewrite.
func sqlLiteral(v *string) string {
	if v == nil {
		return "NULL"
	}
	if !strings.ContainsFunc(*v, func(r rune) bool { return r < 0x20 }) {
		return "'" + strings.ReplaceAll(*v, "'", "''") + "'"
	}
	var b strings.Builder
	b.WriteString("E'")
	for _, r := range *v {
		switch {
		case r == '\'':
			b.WriteString("''")
		case r == '\\':
			b.WriteString(`\\`)
		case r < 0x20:
			fmt.Fprintf(&b, `\x%02x`, r)
		default:
			b.WriteRune(r)
		}
	}
	b.WriteString("'")
	return b.String()
}

func renderSeed(all []scenario) ([]byte, error) {
	var b bytes.Buffer
	b.WriteString("-- GENERATED by `go run ./parity/fixture/gen` - do not edit by hand.\n")
	b.WriteString("-- SYNTHETIC DATA ONLY (REFACTOR_PLAN.md PD-3): every row is authored in\n")
	b.WriteString("-- parity/fixture/gen/main.go; nothing is extracted from Staging or Production.\n\n")

	b.WriteString("INSERT INTO subjects (subject_id, master_type, subject_type, master_id) VALUES\n")
	for i, sub := range subjects {
		fmt.Fprintf(&b, "    (%s, %s, %s, %s)%s\n", sqlLiteral(&sub.ID), sqlLiteral(&sub.MasterType),
			sqlLiteral(&sub.SubjectType), sqlLiteral(&sub.MasterID), sep(i, len(subjects)))
	}
	b.WriteString("\nINSERT INTO mv_subject_hierarchy (start_subject_id, subject_id, subject_type, master_id, master_type) VALUES\n")
	for i, h := range hierarchies {
		fmt.Fprintf(&b, "    (%s, %s, %s, %s, %s)%s\n", sqlLiteral(&h.Start), sqlLiteral(&h.SubjectID),
			sqlLiteral(&h.SubjectType), sqlLiteral(&h.MasterID), sqlLiteral(&h.MasterType), sep(i, len(hierarchies)))
	}
	b.WriteString("\nINSERT INTO master_mill (mill_id, uml_id) VALUES\n")
	for i, m := range mills {
		fmt.Fprintf(&b, "    (%s, %s)%s\n", sqlLiteral(&m.MillID), sqlLiteral(&m.UmlID), sep(i, len(mills)))
	}

	for _, sc := range all {
		fmt.Fprintf(&b, "\n-- %s %s\n", sc.ID, sc.Title)
		if len(sc.events) > 0 {
			fmt.Fprintf(&b, "INSERT INTO events_bc (%s) VALUES\n", strings.Join(eventColumns, ", "))
			for i, e := range sc.events {
				vals := make([]string, 0, len(eventColumns))
				for _, col := range eventColumns {
					switch col {
					case "unique_id":
						vals = append(vals, sqlLiteral(&e.ID))
					case "sync_time":
						vals = append(vals, syncLiteral(e.Synced))
					default:
						v, ok := e.Now[col]
						if !ok {
							return nil, fmt.Errorf("%s: column %s not set", e.ID, col)
						}
						vals = append(vals, sqlLiteral(v))
					}
				}
				fmt.Fprintf(&b, "    (%s)%s\n", strings.Join(vals, ", "), sep(i, len(sc.events)))
			}
		}
		if len(sc.links) > 0 {
			b.WriteString("INSERT INTO links_bc (id, event1_id, event2_id, role, qty, unit, source, status, event1_version, event2_version, sync_time) VALUES\n")
			for i, l := range sc.links {
				fmt.Fprintf(&b, "    (%d, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)%s\n", l.ID,
					sqlLiteral(&l.Event1), sqlLiteral(&l.Event2),
					sqlLiteral(l.Cols["role"]), sqlLiteral(l.Cols["qty"]), sqlLiteral(l.Cols["unit"]),
					sqlLiteral(l.Cols["source"]), sqlLiteral(l.Cols["status"]),
					sqlLiteral(l.Cols["event1_version"]), sqlLiteral(l.Cols["event2_version"]),
					syncLiteral(l.Synced), sep(i, len(sc.links)))
			}
		}
	}
	return b.Bytes(), nil
}

func sep(i, n int) string {
	if i == n-1 {
		return ";"
	}
	return ","
}

func syncLiteral(synced bool) string {
	if synced {
		return "'" + syncedAt + "'"
	}
	return "NULL"
}

type seedNode struct {
	Scenario   string              `json:"scenario"`
	UID        string              `json:"uid"`
	UniqueID   string              `json:"unique_id"`
	Predicates map[string][]string `json:"predicates"`
}

type seedEdge struct {
	Scenario string            `json:"scenario"`
	From     string            `json:"from"`
	To       string            `json:"to"`
	Facets   map[string]string `json:"facets,omitempty"`
}

type graphSeed struct {
	Format      string     `json:"format"`
	Description string     `json:"description"`
	Nodes       []seedNode `json:"nodes"`
	Edges       []seedEdge `json:"edges"`
}

func renderGraphSeed(all []scenario) ([]byte, error) {
	seed := graphSeed{
		Format: formatSeed,
		Description: "Dgraph state assumed already loaded before the incremental run. Predicate values and facet values are " +
			"RDF object text, so `<uid> <predicate> <value> .` is a valid N-Quad. uids are fixture-local and only identify " +
			"nodes; goldens never depend on them (they are rewritten to <uid:UNIQUE_ID>).",
		Nodes: []seedNode{},
		Edges: []seedEdge{},
	}
	for n, sc := range all {
		for i, e := range sc.events {
			if !e.InGraph {
				continue
			}
			r := e.Before
			if r == nil {
				r = e.Now
			}
			seed.Nodes = append(seed.Nodes, seedNode{
				Scenario:   sc.ID,
				UID:        fmt.Sprintf("0x%d%03d", n+1, i+1),
				UniqueID:   e.ID,
				Predicates: eventPredicates(e.ID, r),
			})
		}
		for _, l := range sc.links {
			if !l.InGraph {
				continue
			}
			seed.Edges = append(seed.Edges, seedEdge{Scenario: sc.ID, From: l.Event1, To: l.Event2, Facets: linkFacets(l.Cols)})
		}
	}
	return marshal(seed)
}

func trimmed(v *string) string {
	if v == nil {
		return ""
	}
	return strings.TrimSpace(*v)
}

// eventPredicates mirrors what the ETL would have written for r: it resolves
// the eventDetailsQuery joins against the reference tables and keeps only
// non-empty values. It only shapes the pre-run graph; it is not an oracle.
func eventPredicates(id string, r row) map[string][]string {
	preds := map[string][]string{
		"dgraph.type": {common.StringLiteral("Event")},
		"unique_id":   {common.StringLiteral(id)},
	}
	put := func(pred, v string) {
		if v != "" {
			preds[pred] = []string{common.StringLiteral(v)}
		}
	}
	for _, col := range []string{
		"client_id", "bl_number", "sales_order", "product_id", "product_name", "plant", "production_label",
		"material_type", "material_code", "unit", "sub_data_source", "version", "flag", "material_document",
		"purchase_order", "uml_id", "location_id", "vendor", "source", "movement_type", "movement_type_text",
		"storage_location", "partner_pca", "mill_name",
	} {
		put(col, trimmed(r[col]))
	}
	if v := trimmed(r["src_path"]); v != "{}" {
		put("src_path", v)
	}
	if v := trimmed(r["process_date"]); v != "" {
		n, _ := strconv.ParseInt(v, 10, 64)
		preds["process_date"] = []string{common.IntLiteral(n)}
	}
	if v := trimmed(r["amount"]); v != "" {
		f, _ := strconv.ParseFloat(v, 64)
		preds["amount"] = []string{common.FloatLiteral(f)}
	}
	for _, m := range mills {
		if m.UmlID == trimmed(r["uml_id"]) {
			put("mill_id", m.MillID)
		}
	}
	for _, side := range []string{"location", "source"} {
		key := trimmed(r[map[string]string{"location": "location_id", "source": "source"}[side]])
		for _, sub := range subjects {
			if sub.ID == key {
				put(side+"_subject_type", sub.SubjectType)
				put(side+"_master_id", sub.MasterID)
				put(side+"_master_type", sub.MasterType)
			}
		}
		for _, h := range hierarchies {
			if h.Start == key {
				put(side+"_facility_subject_id", h.SubjectID)
				put(side+"_facility_subject_type", h.SubjectType)
				put(side+"_facility_master_id", h.MasterID)
				put(side+"_facility_master_type", h.MasterType)
			}
		}
	}
	return preds
}

func linkFacets(cols row) map[string]string {
	facets := map[string]string{}
	for _, key := range []string{"event1_version", "event2_version", "role", "unit", "source"} {
		if v := trimmed(cols[key]); v != "" {
			facets[key] = common.StringLiteral(v)
		}
	}
	if v := trimmed(cols["qty"]); v != "" {
		f, _ := strconv.ParseFloat(v, 64)
		facets["qty"] = strconv.FormatFloat(f, 'f', -1, 64)
	}
	return facets
}

type manifest struct {
	Format      string     `json:"format"`
	Description string     `json:"description"`
	Scenarios   []scenario `json:"scenarios"`
}

func renderManifest(all []scenario) ([]byte, error) {
	for i := range all {
		all[i].RowPrefix = "SYN-" + all[i].ID + "-"
		e := &all[i].Expect
		for _, list := range []*[]string{&e.EventIDs, &e.LinkEvent1IDs, &e.RemovalUniqueIDs, &e.StampedEvents,
			&e.CreatedNodes, &e.DeletedNodes, &e.EdgesAfter} {
			if *list == nil {
				*list = []string{}
			}
			sort.Strings(*list)
		}
		for _, list := range []*[]int64{&e.RemovalLinkIDs, &e.StampedLinks} {
			if *list == nil {
				*list = []int64{}
			}
			sort.Slice(*list, func(a, b int) bool { return (*list)[a] < (*list)[b] })
		}
	}
	return marshal(manifest{
		Format: formatSummary,
		Description: "The seven P1-7 scenarios. Rows of scenario Sn are exactly the events_bc rows whose unique_id and the " +
			"links_bc rows whose event1_id start with row_prefix; reference tables (subjects, mv_subject_hierarchy, " +
			"master_mill) are shared. `expect` is derived by hand from the SQL and Go source, independently of the goldens; " +
			"the L3 tests assert the Go implementation agrees with it.",
		Scenarios: all,
	})
}

func marshal(v any) ([]byte, error) {
	var b bytes.Buffer
	enc := json.NewEncoder(&b)
	enc.SetEscapeHTML(false)
	enc.SetIndent("", "  ")
	if err := enc.Encode(v); err != nil {
		return nil, err
	}
	return b.Bytes(), nil
}
