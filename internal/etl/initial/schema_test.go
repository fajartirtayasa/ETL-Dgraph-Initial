package initial

// The export must emit exactly the predicates, with exactly the types, that the canonical schema declares.
// The canonical schema.dql is owned by ETL-Dgraph-Incremental and read from the sibling checkout at a fixed
// path (no override): when it is absent these tests FAIL, they never skip. parity/go.sh mounts both
// repositories side by side under /work, so the path resolves inside the container too.

import (
	"bufio"
	"bytes"
	"compress/gzip"
	"context"
	"database/sql"
	"database/sql/driver"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"regexp"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"testing"

	"hara/traceability-dgraph-etl/internal/etl/common"
)

const (
	canonicalSchemaRepo = "ETL-Dgraph-Incremental"
	xsdInt              = "http://www.w3.org/2001/XMLSchema#int"
	xsdFloat            = "http://www.w3.org/2001/XMLSchema#float"
)

// repoRoot walks up from this file's directory to the directory holding go.mod.
func repoRoot(t *testing.T) string {
	t.Helper()
	_, file, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("cannot locate schema_test.go")
	}
	for dir := filepath.Dir(file); ; {
		if _, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil {
			return dir
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			t.Fatalf("no go.mod above %s", filepath.Dir(file))
		}
		dir = parent
	}
}

func canonicalSchemaPath(t *testing.T) string {
	t.Helper()
	path, err := filepath.Abs(filepath.Join(repoRoot(t), "..", canonicalSchemaRepo, "schema.dql"))
	if err != nil {
		t.Fatal(err)
	}
	return path
}

func readCanonicalSchema(t *testing.T) []byte {
	t.Helper()
	path := canonicalSchemaPath(t)
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("the canonical schema is required: %v\n  expected at %s\n  fix: place the %s repository beside "+
			"ETL-Dgraph-Initial (parity/go.sh mounts both under /work)", err, path, canonicalSchemaRepo)
	}
	return raw
}

// ---- the schema ---------------------------------------------------------------------------------------------

type dqlSchema struct {
	predicates map[string]string   // declared predicate -> type as written: string, int, float, [uid], ...
	types      map[string][]string // type name -> its fields
}

var (
	predicateDecl = regexp.MustCompile(`^<?([A-Za-z_][A-Za-z0-9_.]*)>?\s*:\s*(\[?[A-Za-z]+\]?)(\s|$)`)
	typeDecl      = regexp.MustCompile(`^type\s+([A-Za-z_][A-Za-z0-9_]*)\s*\{$`)
)

// parseSchema reads predicate declarations (`name: type ... .`) and type blocks (`type Name {` ... `}`). Any
// other non-blank, non-comment line fails the test, so a schema form this parser does not know is never ignored.
func parseSchema(t *testing.T, raw []byte) dqlSchema {
	t.Helper()
	s := dqlSchema{predicates: map[string]string{}, types: map[string][]string{}}
	current := ""
	for n, line := range strings.Split(string(raw), "\n") {
		line = strings.TrimSpace(line)
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		if current != "" {
			if line == "}" {
				current = ""
				continue
			}
			s.types[current] = append(s.types[current], strings.Trim(line, "<>"))
			continue
		}
		if m := typeDecl.FindStringSubmatch(line); m != nil {
			if _, dup := s.types[m[1]]; dup {
				t.Fatalf("schema.dql line %d declares type %s twice", n+1, m[1])
			}
			current = m[1]
			s.types[current] = []string{}
			continue
		}
		if m := predicateDecl.FindStringSubmatch(line); m != nil {
			if _, dup := s.predicates[m[1]]; dup {
				t.Fatalf("schema.dql line %d declares %s twice", n+1, m[1])
			}
			s.predicates[m[1]] = m[2]
			continue
		}
		t.Fatalf("schema.dql line %d is neither a predicate declaration nor part of a type block: %q", n+1, line)
	}
	if current != "" {
		t.Fatalf("schema.dql: type %s is never closed", current)
	}
	return s
}

// ---- the RDF ------------------------------------------------------------------------------------------------

type triple struct {
	subject, predicate, object string
	kind                       string // string, int, float or [uid]
}

// parseTriple reads one N-Quad line as the exporter writes it: `<blank node> <predicate> <object> .`. The object
// is a quoted literal (string), a typed literal (int or float; any other datatype is an error) or a blank node
// ([uid]), which facets in parentheses may follow.
func parseTriple(line string) (triple, error) {
	body, ok := strings.CutSuffix(line, " .")
	if !ok {
		return triple{}, fmt.Errorf("no terminating \" .\"")
	}
	parts := strings.SplitN(body, " ", 3)
	if len(parts) != 3 {
		return triple{}, fmt.Errorf("not subject, predicate and object")
	}
	tr := triple{subject: parts[0]}
	if !strings.HasPrefix(tr.subject, "_:") {
		return triple{}, fmt.Errorf("subject %q is not a blank node", tr.subject)
	}
	if p := parts[1]; len(p) > 2 && p[0] == '<' && p[len(p)-1] == '>' {
		tr.predicate = p[1 : len(p)-1]
	} else {
		return triple{}, fmt.Errorf("predicate %q is not an IRI", p)
	}
	rest := parts[2]
	switch {
	case strings.HasPrefix(rest, `"`):
		lit, err := strconv.QuotedPrefix(rest)
		if err != nil {
			return triple{}, fmt.Errorf("object literal: %v", err)
		}
		if tr.object, err = strconv.Unquote(lit); err != nil {
			return triple{}, fmt.Errorf("object literal: %v", err)
		}
		switch datatype := rest[len(lit):]; datatype {
		case "":
			tr.kind = "string"
		case "^^<" + xsdInt + ">":
			tr.kind = "int"
		case "^^<" + xsdFloat + ">":
			tr.kind = "float"
		default:
			return triple{}, fmt.Errorf("unknown datatype %q", datatype)
		}
	case strings.HasPrefix(rest, "_:"):
		node, facets, _ := strings.Cut(rest, " ")
		if facets != "" && !(strings.HasPrefix(facets, "(") && strings.HasSuffix(facets, ")")) {
			return triple{}, fmt.Errorf("unexpected text after the object node: %q", facets)
		}
		tr.object, tr.kind = node, "[uid]"
	default:
		return triple{}, fmt.Errorf("object %q is neither a literal nor a blank node", rest)
	}
	return tr, nil
}

// readTriples decompresses every file matching pattern (all chunks) and parses each line.
func readTriples(t *testing.T, pattern string) []triple {
	t.Helper()
	files, err := filepath.Glob(pattern)
	if err != nil || len(files) == 0 {
		t.Fatalf("no file matches %s (%v)", pattern, err)
	}
	var out []triple
	for _, file := range files {
		fh, err := os.Open(file)
		if err != nil {
			t.Fatal(err)
		}
		zr, err := gzip.NewReader(fh)
		if err != nil {
			fh.Close()
			t.Fatalf("%s: %v", file, err)
		}
		sc := bufio.NewScanner(zr)
		sc.Buffer(make([]byte, 0, 64*1024), 1<<20)
		for n := 1; sc.Scan(); n++ {
			if sc.Text() == "" {
				continue
			}
			tr, err := parseTriple(sc.Text())
			if err != nil {
				t.Fatalf("%s line %d: %v\n  %s", filepath.Base(file), n, err, sc.Text())
			}
			out = append(out, tr)
		}
		if err := sc.Err(); err != nil {
			t.Fatalf("%s: %v", file, err)
		}
		zr.Close()
		fh.Close()
	}
	return out
}

// ---- what the exporter emits --------------------------------------------------------------------------------

// fullyPopulatedEventRow sets every EventRow field, by reflection, to a valid non-empty value of its type. A field
// type this test does not know fails it, so a new column cannot slip past the check unpopulated.
func fullyPopulatedEventRow(t *testing.T) *EventRow {
	t.Helper()
	row := &EventRow{}
	v := reflect.ValueOf(row).Elem()
	for i := 0; i < v.NumField(); i++ {
		field := v.Type().Field(i)
		switch f := v.Field(i).Addr().Interface().(type) {
		case *sql.NullString:
			*f = sql.NullString{String: "SYN-" + field.Name, Valid: true}
		case *sql.NullInt64:
			*f = sql.NullInt64{Int64: 20240101, Valid: true}
		case *sql.NullFloat64:
			*f = sql.NullFloat64{Float64: 12.5, Valid: true}
		default:
			t.Fatalf("EventRow.%s has type %s, which this test cannot populate; extend fullyPopulatedEventRow",
				field.Name, field.Type)
		}
	}
	return row
}

// emitted maps each predicate to the object type it was written with; a predicate written twice, or with two
// types, fails the test.
func addEmitted(t *testing.T, emitted map[string]string, tr triple) {
	t.Helper()
	if prev, dup := emitted[tr.predicate]; dup {
		t.Fatalf("predicate <%s> emitted more than once (as %s and %s)", tr.predicate, prev, tr.kind)
	}
	emitted[tr.predicate] = tr.kind
}

// emittedEventTriples runs one fully populated row through the aggregator and returns every triple it wrote.
func emittedEventTriples(t *testing.T) []triple {
	t.Helper()
	dir := t.TempDir()
	agg, err := newAggregator(dir, common.NewStringInterner(), 0)
	if err != nil {
		t.Fatal(err)
	}
	row := fullyPopulatedEventRow(t)
	if err := agg.emitEvent(context.Background(), row, row.UniqueID.String); err != nil {
		t.Fatal(err)
	}
	if err := agg.Close(); err != nil {
		t.Fatal(err)
	}
	return readTriples(t, filepath.Join(dir, "events*.rdf.gz"))
}

// emittedLinkTriples runs streamLinks over one fully populated link row served by the stub driver.
func emittedLinkTriples(t *testing.T) []triple {
	t.Helper()
	db := newStubDB(func(query string, args []driver.NamedValue) (stubResult, error) {
		if query != linksQuery || len(args) != 0 {
			return stubResult{}, fmt.Errorf("unexpected query (%d args): %.80q", len(args), query)
		}
		return stubResult{
			Columns: []string{"id", "event1_id", "event2_id", "event1_version", "event2_version", "role", "qty", "unit", "source"},
			Rows: [][]driver.Value{
				{int64(970001), "SYN-E1", "SYN-E2", "SYN-V1", "SYN-V2", "SYN-ROLE", 2.5, "SYN-UNIT", "SYN-SOURCE"},
			},
		}, nil
	})
	t.Cleanup(func() { db.Close() })

	dir := t.TempDir()
	if err := streamLinks(context.Background(), db, common.NewStringInterner(), dir, 0); err != nil {
		t.Fatal(err)
	}
	triples := readTriples(t, filepath.Join(dir, "links*.rdf.gz"))
	if len(triples) != 1 {
		t.Fatalf("one link row produced %d triples: %v", len(triples), triples)
	}
	tr := triples[0]
	if want := common.BlankNode("event", "SYN-E1"); tr.subject != want {
		t.Fatalf("link subject %s, want event1's blank node %s", tr.subject, want)
	}
	if want := common.BlankNode("event", "SYN-E2"); tr.object != want {
		t.Fatalf("link object %s, want event2's blank node %s", tr.object, want)
	}
	return triples
}

// ---- the checks ---------------------------------------------------------------------------------------------

// schemaProblems compares what was emitted with the schema in both directions and names every offending
// predicate: emitted == declared == fields of the type named by dgraph.type, with matching types.
func schemaProblems(s dqlSchema, emitted map[string]string, typeName string) []string {
	var out []string
	fields, ok := s.types[typeName]
	if !ok {
		out = append(out, fmt.Sprintf("dgraph.type is %q, but schema.dql declares no such type", typeName))
	}
	inType := map[string]bool{}
	for _, f := range fields {
		inType[f] = true
	}
	for p, kind := range emitted {
		declared, ok := s.predicates[p]
		switch {
		case !ok:
			out = append(out, fmt.Sprintf("<%s> is emitted (as %s) but not declared in schema.dql", p, kind))
		case declared != kind:
			out = append(out, fmt.Sprintf("<%s> is emitted as %s but declared as %s", p, kind, declared))
		}
	}
	for p, declared := range s.predicates {
		if _, ok := emitted[p]; !ok {
			out = append(out, fmt.Sprintf("<%s> is declared (%s) but never emitted", p, declared))
		}
		if !inType[p] {
			out = append(out, fmt.Sprintf("<%s> is declared but is not a field of type %s", p, typeName))
		}
	}
	for f := range inType {
		if _, ok := s.predicates[f]; !ok {
			out = append(out, fmt.Sprintf("type %s lists <%s>, which schema.dql does not declare", typeName, f))
		}
	}
	sort.Strings(out)
	return out
}

func TestEmittedPredicatesMatchTheCanonicalSchema(t *testing.T) {
	s := parseSchema(t, readCanonicalSchema(t))

	emitted := map[string]string{}
	typeName := ""
	for _, tr := range emittedEventTriples(t) {
		if tr.predicate == "dgraph.type" {
			if tr.kind != "string" || typeName != "" {
				t.Fatalf("dgraph.type must be emitted once as a string, got %s %q", tr.kind, tr.object)
			}
			typeName = tr.object
			continue
		}
		addEmitted(t, emitted, tr)
	}
	if typeName != "Event" {
		t.Fatalf("dgraph.type is %q, want Event", typeName)
	}
	for _, tr := range emittedLinkTriples(t) {
		addEmitted(t, emitted, tr)
	}

	if problems := schemaProblems(s, emitted, typeName); len(problems) > 0 {
		t.Fatalf("the export and %s disagree:\n  %s", canonicalSchemaPath(t), strings.Join(problems, "\n  "))
	}
}

func TestSchemaMirrorIsByteIdenticalToTheCanonicalSchema(t *testing.T) {
	canonical := readCanonicalSchema(t)
	local, err := os.ReadFile(filepath.Join(repoRoot(t), "schema.dql"))
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Equal(local, canonical) {
		return
	}
	a, b := strings.Split(string(local), "\n"), strings.Split(string(canonical), "\n")
	n := 0
	for n < len(a) && n < len(b) && a[n] == b[n] {
		n++
	}
	line := func(lines []string) string {
		if n < len(lines) {
			return strconv.Quote(lines[n])
		}
		return "(end of file)"
	}
	t.Fatalf("schema.dql must be byte-identical to %s; first difference at line %d:\n  here:      %s\n  canonical: %s",
		canonicalSchemaPath(t), n+1, line(a), line(b))
}

// The check must name the offending predicate in each direction; this runs on a planted schema, not the real one.
func TestNegativeSchemaProblemsNameThePredicate(t *testing.T) {
	s := dqlSchema{
		predicates: map[string]string{"unique_id": "string", "amount": "float", "planted_extra": "string"},
		types:      map[string][]string{"Event": {"unique_id", "amount", "planted_extra"}},
	}
	emitted := map[string]string{"unique_id": "string", "amount": "int", "planted_undeclared": "string"}
	got := strings.Join(schemaProblems(s, emitted, "Event"), "\n")
	for _, want := range []string{
		"<planted_extra> is declared (string) but never emitted",
		"<amount> is emitted as int but declared as float",
		"<planted_undeclared> is emitted (as string) but not declared in schema.dql",
	} {
		if !strings.Contains(got, want) {
			t.Errorf("missing %q in:\n%s", want, got)
		}
	}
}
