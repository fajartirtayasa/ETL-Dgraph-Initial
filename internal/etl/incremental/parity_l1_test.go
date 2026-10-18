package incremental

// L1 — pure functions (REFACTOR_PLAN.md P1-1). Each test records Go's output
// for a case table as a golden the Python port must reproduce exactly.

import (
	"database/sql"
	"math"
	"strconv"
	"strings"
	"testing"

	"hara/traceability-dgraph-etl/internal/etl/common"
)

type l1Case struct {
	Input          any    `json:"input"`
	Expected       any    `json:"expected"`
	Note           string `json:"note,omitempty"`
	ParityRequired *bool  `json:"parity_required,omitempty"`
}

func boolp(v bool) *bool { return &v }

// stringLiteralInputs covers strconv.Quote's escaping rules, which
// common.StringLiteral (common/rdf.go:17-19) uses for every string object and
// facet value. Python's repr/json escaping differ from it on most of these.
var stringLiteralInputs = []struct{ in, note string }{
	{"", ""},
	{"SYN plain ascii", ""},
	{`SYN "double" quotes`, ""},
	{`SYN back\slash`, ""},
	{"SYN single ' quote", "not escaped"},
	{"line1\nline2", ""},
	{"tab\there", ""},
	{"carriage\rreturn", ""},
	{"bell\a vt\v ff\f bs\b", "C escapes"},
	{"nul\x00 soh\x01 us\x1f del\x7f", "other C0 controls and DEL use \\x"},
	{"é ñ 中文 日本語", "printable non-ASCII is kept raw"},
	{"emoji 🙂", "astral printable is kept raw"},
	{"nbsp\u00a0x", "U+00A0 is not printable per strconv.IsPrint: \\u00a0"},
	{"nel\u0085x", "C1 control: \\u0085"},
	{"zwsp\u200bx", "format char: \\u200b"},
	{"ls\u2028ps\u2029", "line/paragraph separators: \\u2028 \\u2029"},
	{"bom\ufeffx", "\\ufeff"},
	{"ideographic\u3000space", "U+3000 is not printable per strconv.IsPrint: \\u3000"},
	{"\U000e0001 tag", "astral non-printable: \\U000e0001"},
	{"<0x1> * * .", "RDF syntax inside a literal stays inert"},
	{"{\"a\": [1, 2]}", "jsonb text as src_path"},
}

func TestParityL1StringLiteral(t *testing.T) {
	cases := make([]l1Case, 0, len(stringLiteralInputs))
	for _, c := range stringLiteralInputs {
		got := common.StringLiteral(c.in)
		if back, err := strconv.Unquote(got); err != nil || back != c.in {
			t.Errorf("StringLiteral(%q) = %s does not round-trip: %v", c.in, got, err)
		}
		cases = append(cases, l1Case{Input: c.in, Expected: got, Note: c.note})
	}
	assertGolden(t, "l1/string_literal.json", "l1.cases",
		"common.StringLiteral (common/rdf.go:17-19) = Go strconv.Quote. Used for every string object and string facet value.",
		compareL1, map[string]any{"function": "common.StringLiteral", "cases": cases})
}

func TestParityL1IntLiteral(t *testing.T) {
	inputs := []int64{0, 1, -1, 1650000000, 1700000000, math.MaxInt32, math.MaxInt64, math.MinInt64}
	cases := make([]l1Case, 0, len(inputs))
	for _, in := range inputs {
		cases = append(cases, l1Case{Input: strconv.FormatInt(in, 10), Expected: common.IntLiteral(in)})
	}
	assertGolden(t, "l1/int_literal.json", "l1.cases",
		"common.IntLiteral (common/rdf.go:22-24), used for process_date. `input` is a decimal int64.",
		compareL1, map[string]any{"function": "common.IntLiteral", "cases": cases})
}

// floatInputs are decimal strings parsed to the nearest float64 (both Go's
// ParseFloat and Python's float() round correctly), then formatted.
var floatInputs = []struct{ in, note string }{
	{"0", ""}, {"-0", "negative zero keeps its sign"}, {"1", ""}, {"1.5", ""},
	{"1.10", "trailing zero dropped"}, {"0.1", ""}, {"0.30000000000000004", ""},
	{"3.75", ""}, {"10.5", ""}, {"100", "no exponent, no .0"}, {"-2.5", ""},
	{"1e21", "Python repr would give 1e+21; Go 'f' never uses an exponent"},
	{"1e-7", "Python repr would give 1e-07"},
	{"123456789.123456789", "shortest round-trip digits"},
	{"12345678901234567890.125", "beyond float64 precision; digits past 17 become zeros"},
	{"5e-324", "smallest subnormal"},
	{"1.7976931348623157e308", "largest finite"},
	{"NaN", "Postgres numeric can hold NaN"},
	{"+Inf", "Postgres numeric (>=14) can hold Infinity"},
	{"-Inf", ""},
}

func TestParityL1FloatLiteral(t *testing.T) {
	cases := make([]l1Case, 0, len(floatInputs))
	for _, c := range floatInputs {
		v, err := strconv.ParseFloat(c.in, 64)
		if err != nil {
			t.Fatalf("ParseFloat(%q): %v", c.in, err)
		}
		got := common.FloatLiteral(v)
		if strings.ContainsAny(strings.TrimSuffix(strings.Split(got, "^^")[0], `"`), "eE") && !math.IsInf(v, 0) {
			t.Errorf("FloatLiteral(%s) = %s uses an exponent", c.in, got)
		}
		cases = append(cases, l1Case{Input: c.in, Expected: got, Note: c.note})
	}
	assertGolden(t, "l1/float_literal.json", "l1.cases",
		"common.FloatLiteral (common/rdf.go:28-31), used for amount: strconv.FormatFloat(v, 'f', -1, 64) wrapped as an "+
			"xsd:float typed literal. `input` is a decimal string parsed to the nearest IEEE-754 double.",
		compareL1, map[string]any{"function": "common.FloatLiteral", "cases": cases})
}

// TestParityL1NormalizeString pins normalizer.String (normalize.go:18-31):
// NULL -> "", otherwise strings.TrimSpace. Go's TrimSpace and Python's
// str.strip disagree on U+001C..U+001F (Python strips them, Go does not).
func TestParityL1NormalizeString(t *testing.T) {
	n := newNormalizer(common.NewStringInterner())
	inputs := []struct {
		in   *string
		note string
	}{
		{nil, "SQL NULL"},
		{strp(""), ""},
		{strp("SYN"), ""},
		{strp("  SYN  "), ""},
		{strp("\t\n\v\f\r SYN \r\n"), "ASCII whitespace"},
		{strp("   "), "whitespace-only becomes empty, so the field is deleted"},
		{strp("\u00a0SYN\u00a0"), "U+00A0 is trimmed"},
		{strp("\u0085SYN\u2028"), "U+0085 and U+2028 are trimmed"},
		{strp("\u3000SYN\u2003"), "U+3000 and U+2003 are trimmed"},
		{strp("\x1cSYN\x1f"), "U+001C..U+001F are NOT trimmed by Go (Python str.strip would trim them)"},
		{strp("\u200bSYN\ufeff"), "U+200B and U+FEFF are not whitespace in either language"},
		{strp("S Y N"), "inner whitespace kept"},
		{strp("{}"), "kept here; src_path treats \"{}\" as empty separately (events.go:478)"},
	}
	cases := make([]l1Case, 0, len(inputs))
	for _, c := range inputs {
		ns := sql.NullString{}
		var in any
		if c.in != nil {
			ns = sql.NullString{String: *c.in, Valid: true}
			in = *c.in
		}
		cases = append(cases, l1Case{Input: in, Expected: n.String(ns), Note: c.note})
	}
	assertGolden(t, "l1/normalize_string.json", "l1.cases",
		"normalizer.String (incremental/normalize.go:18-31): NULL -> \"\", otherwise Go strings.TrimSpace "+
			"(Unicode White_Space). `input` null means SQL NULL. Applied to every scanned text column.",
		compareL1, map[string]any{"function": "normalizer.String", "cases": cases})

	// Int64/Float64 normalisers are pass-through on Valid (normalize.go:33-45).
	if v, ok := n.Int64(sql.NullInt64{Int64: 0, Valid: true}); !ok || v != 0 {
		t.Error("Int64: a valid zero must stay present")
	}
	if _, ok := n.Int64(sql.NullInt64{}); ok {
		t.Error("Int64: NULL must be absent")
	}
	if v, ok := n.Float64(sql.NullFloat64{Float64: 0, Valid: true}); !ok || v != 0 {
		t.Error("Float64: a valid zero must stay present")
	}
	if _, ok := n.Float64(sql.NullFloat64{}); ok {
		t.Error("Float64: NULL must be absent")
	}
}

func strp(v string) *string { return &v }

type facetInput struct {
	Event1Version string  `json:"event1_version"`
	Event2Version string  `json:"event2_version"`
	Role          string  `json:"role"`
	Qty           *string `json:"qty"`
	Unit          string  `json:"unit"`
	Source        string  `json:"source"`
}

func (f facetInput) record(t *testing.T) linkRecord {
	r := linkRecord{Event1Version: f.Event1Version, Event2Version: f.Event2Version, Role: f.Role, Unit: f.Unit, Source: f.Source}
	if f.Qty != nil {
		v, err := strconv.ParseFloat(*f.Qty, 64)
		if err != nil {
			t.Fatal(err)
		}
		r.Qty = &v
	}
	return r
}

func TestParityL1BuildLinkFacets(t *testing.T) {
	full := facetInput{"v1", "v2", "SYN-input", strp("2.5"), "MT", "SYN-LINKSRC"}
	inputs := []struct {
		in   facetInput
		note string
	}{
		{full, "all six present, fixed order"},
		{facetInput{}, "all absent: empty string, so upsertLinks writes no parentheses"},
		{facetInput{Qty: strp("0")}, "qty is omitted only when NULL; zero is emitted"},
		{facetInput{Qty: strp("-0")}, ""},
		{facetInput{Qty: strp("1e21")}, "no exponent"},
		{facetInput{Qty: strp("0.1")}, ""},
		{facetInput{Role: `SYN "q" \ é`}, "string facets use StringLiteral escaping"},
		{facetInput{Role: " "}, "buildLinkFacets does not trim; normalisation upstream does (links.go:256)"},
		{facetInput{Event2Version: "v9", Unit: "KG"}, "subset keeps relative order"},
		{facetInput{Source: "SYN-LINKSRC"}, ""},
	}
	cases := make([]l1Case, 0, len(inputs))
	for _, c := range inputs {
		cases = append(cases, l1Case{Input: c.in, Expected: buildLinkFacets(c.in.record(t)), Note: c.note})
	}
	assertGolden(t, "l1/build_link_facets.json", "l1.cases",
		"buildLinkFacets (incremental/links.go:393-416). String facets are emitted only when non-empty; qty only when "+
			"non-NULL; order is event1_version, event2_version, role, qty, unit, source. `qty` input is a decimal string or null.",
		compareL1, map[string]any{"function": "buildLinkFacets", "cases": cases})
}

func TestParityL1ChunkIndices(t *testing.T) {
	type in struct {
		Length int `json:"length"`
		Size   int `json:"size"`
	}
	inputs := []struct {
		in   in
		note string
	}{
		{in{0, 10}, ""}, {in{1, 10}, ""}, {in{10, 10}, ""}, {in{11, 10}, ""}, {in{25, 10}, ""},
		{in{3, 1}, ""}, {in{5, 0}, "size <= 0 means one chunk of everything"}, {in{5, -3}, ""},
	}
	cases := make([]l1Case, 0, len(inputs)+1)
	for _, c := range inputs {
		got := chunkIndices(c.in.Length, c.in.Size)
		covered := 0
		for _, ch := range got {
			if ch[0] != covered || ch[1] <= ch[0] {
				t.Errorf("chunkIndices(%d,%d) = %v is not a contiguous partition", c.in.Length, c.in.Size, got)
			}
			covered = ch[1]
		}
		if covered != c.in.Length {
			t.Errorf("chunkIndices(%d,%d) covers %d", c.in.Length, c.in.Size, covered)
		}
		cases = append(cases, l1Case{Input: c.in, Expected: got, Note: c.note})
	}

	// length 0 with size <= 0 divides by zero (helpers.go:37). No call site can
	// reach it: every caller passes a size ApplyDefaults made positive, or
	// returns early on empty input. Recorded, not required of the port.
	panicked := func() (p bool) {
		defer func() { p = recover() != nil }()
		chunkIndices(0, 0)
		return false
	}()
	if !panicked {
		t.Error("chunkIndices(0, 0) was expected to panic at b487294")
	}
	cases = append(cases, l1Case{Input: in{0, 0}, Expected: "panic: integer divide by zero",
		Note: "unreachable from any call site (sizes are ApplyDefaults-positive; empty inputs return early)", ParityRequired: boolp(false)})

	assertGolden(t, "l1/chunk_indices.json", "l1.cases",
		"chunkIndices (incremental/helpers.go:33-46): [start, end) pairs partitioning [0, length).",
		compareL1, map[string]any{"function": "chunkIndices", "cases": cases})
}

func TestParityL1BuildCleanupRDF(t *testing.T) {
	inputs := [][]string{{}, {"0x1"}, {"0x1", "", "0x2"}, {"0x2", "0x1"}}
	cases := make([]l1Case, 0, len(inputs))
	for _, in := range inputs {
		cases = append(cases, l1Case{Input: in, Expected: buildCleanupRDF(in)})
	}
	assertGolden(t, "l1/build_cleanup_rdf.json", "l1.cases",
		"buildCleanupRDF (incremental/graph_ops.go:106-115): one `<uid> <links> * .` per non-empty uid, joined by "+
			"newline with no trailing newline. Used by deleteExistingLinks and cleanupMissingLinks.",
		compareL1, map[string]any{"function": "buildCleanupRDF", "cases": cases})
}

func TestParityL1ParseLookupJSON(t *testing.T) {
	inputs := []struct{ in, note string }{
		{`{"nodes":[{"uid":"0x1","unique_id":"SYN-A"},{"uid":"0x2","unique_id":"SYN-B"}]}`, ""},
		{`{"nodes":[]}`, ""},
		{`{"nodes":[{"uid":"","unique_id":"SYN-A"},{"uid":"0x3","unique_id":""}]}`, "entries missing either side are dropped"},
		{`{"nodes":[{"uid":"0x1","unique_id":"SYN-A"},{"uid":"0x9","unique_id":"SYN-A"}]}`, "duplicate unique_id: last wins"},
		{`not json`, "malformed JSON is swallowed and reads as 'not found' (helpers.go:85-87)"},
	}
	cases := make([]l1Case, 0, len(inputs))
	for _, c := range inputs {
		cases = append(cases, l1Case{Input: c.in, Expected: parseLookupJSON(c.in), Note: c.note})
	}
	assertGolden(t, "l1/parse_lookup_json.json", "l1.cases",
		"parseLookupJSON (incremental/helpers.go:83-96): unique_id -> uid from a lookup response.",
		compareL1, map[string]any{"function": "parseLookupJSON", "cases": cases})
}
