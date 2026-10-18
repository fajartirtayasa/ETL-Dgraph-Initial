package incremental

// fakeGraph is the P1-2 fake dgraph.Adapter. It captures every RDF payload
// the ETL sends instead of sending it, and also applies the payload to a
// small in-memory graph so lookups see earlier writes and tests can assert
// end state. Only the operations the incremental ETL uses are supported;
// anything else returns an error so an unexpected call fails the test.

import (
	"context"
	"encoding/json"
	"fmt"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"

	"hara/traceability-dgraph-etl/pkg/dgraph"
)

type graphCall struct {
	Step string
	Op   string // MutateRDF | MutateWithDeletesRDF | DeleteRDF
	Set  string
	Del  string
}

type lookupCall struct {
	Step      string
	UniqueIDs []string
}

type fakeNode struct {
	preds map[string][]string // predicate (no brackets) -> RDF object text
	edges map[string]string   // target uid -> raw facet text
}

type fakeGraph struct {
	mu       sync.Mutex
	step     string
	calls    []graphCall
	lookups  []lookupCall
	nodes    map[string]*fakeNode
	registry map[string]string // uid -> unique_id, never forgotten
	seeded   map[string]bool   // unique_ids present before the run
	nextUID  uint64
}

var _ dgraph.Adapter = (*fakeGraph)(nil)

func newFakeGraph() *fakeGraph {
	return &fakeGraph{
		nodes:    map[string]*fakeNode{},
		registry: map[string]string{},
		seeded:   map[string]bool{},
		nextUID:  0xa00000,
	}
}

func (g *fakeGraph) setStep(step string) {
	g.mu.Lock()
	g.step = step
	g.mu.Unlock()
}

func (g *fakeGraph) node(uid string) *fakeNode {
	n, ok := g.nodes[uid]
	if !ok {
		n = &fakeNode{preds: map[string][]string{}, edges: map[string]string{}}
		g.nodes[uid] = n
	}
	return n
}

// seedNode adds a pre-existing Event node. preds may omit dgraph.type and
// unique_id; both are always set.
func (g *fakeGraph) seedNode(uid, uniqueID string, preds map[string][]string) {
	n := g.node(uid)
	for p, v := range preds {
		n.preds[p] = append([]string(nil), v...)
	}
	n.preds["dgraph.type"] = []string{strconv.Quote("Event")}
	n.preds["unique_id"] = []string{strconv.Quote(uniqueID)}
	g.registry[uid] = uniqueID
	g.seeded[uniqueID] = true
}

func (g *fakeGraph) seedEdge(fromUID, toUID, facets string) {
	g.node(fromUID).edges[toUID] = facets
}

func (g *fakeGraph) uidOf(uniqueID string) (string, bool) {
	g.mu.Lock()
	defer g.mu.Unlock()
	for uid, id := range g.registry {
		if id == uniqueID {
			return uid, true
		}
	}
	return "", false
}

// --- dgraph.Adapter -------------------------------------------------------

func (g *fakeGraph) Close()                           {}
func (g *fakeGraph) Ping(context.Context) error       { return nil }
func (g *fakeGraph) HealthCheck() error               { return nil }
func (g *fakeGraph) WithBestEffort() dgraph.DQLOption { return dgraph.WithBestEffort() }
func (g *fakeGraph) WithReadOnly() dgraph.DQLOption   { return dgraph.WithReadOnly() }
func (g *fakeGraph) WithResponseFormat(f dgraph.ResponseFormat) dgraph.DQLOption {
	return dgraph.WithResponseFormat(f)
}

func (g *fakeGraph) SetSchema(context.Context, string) error { return errUnsupported("SetSchema") }
func (g *fakeGraph) GetSchema(context.Context) (string, error) {
	return "", errUnsupported("GetSchema")
}
func (g *fakeGraph) RunDQLWithVars(context.Context, string, map[string]string, ...dgraph.DQLOption) (*dgraph.DQLResponse, error) {
	return nil, errUnsupported("RunDQLWithVars")
}
func (g *fakeGraph) CreateNamespace(context.Context) (uint64, error) {
	return 0, errUnsupported("CreateNamespace")
}
func (g *fakeGraph) DropNamespace(context.Context, uint64) error {
	return errUnsupported("DropNamespace")
}
func (g *fakeGraph) ListNamespaces(context.Context) ([]uint64, error) {
	return nil, errUnsupported("ListNamespaces")
}

func errUnsupported(op string) error {
	return fmt.Errorf("fakeGraph: %s is not used by the incremental ETL", op)
}

// lookupQueryPattern matches buildEventLookupQuery (helpers.go:48-63).
var lookupQueryPattern = regexp.MustCompile(`(?s)^query \{\s*nodes\(func: type\(Event\)\)\s*@filter\(eq\(unique_id,\s*(.*?)\s*\)\) \{\s*uid\s*unique_id\s*\}\s*\}$`)

func (g *fakeGraph) RunDQL(_ context.Context, dql string, _ ...dgraph.DQLOption) (*dgraph.DQLResponse, error) {
	m := lookupQueryPattern.FindStringSubmatch(dql)
	if m == nil {
		return nil, fmt.Errorf("fakeGraph: unrecognised DQL query:\n%s", dql)
	}
	ids, err := parseQuotedList(m[1])
	if err != nil {
		return nil, fmt.Errorf("fakeGraph: lookup ids: %w", err)
	}

	g.mu.Lock()
	defer g.mu.Unlock()
	sorted := append([]string(nil), ids...)
	sort.Strings(sorted)
	g.lookups = append(g.lookups, lookupCall{Step: g.step, UniqueIDs: sorted})

	want := map[string]bool{}
	for _, id := range ids {
		want[id] = true
	}
	type hit struct {
		UID      string `json:"uid"`
		UniqueID string `json:"unique_id"`
	}
	hits := []hit{}
	for uid, n := range g.nodes {
		if !containsString(n.preds["dgraph.type"], strconv.Quote("Event")) || len(n.preds["unique_id"]) == 0 {
			continue
		}
		id, err := strconv.Unquote(n.preds["unique_id"][0])
		if err == nil && want[id] {
			hits = append(hits, hit{UID: uid, UniqueID: id})
		}
	}
	sort.Slice(hits, func(i, j int) bool { return hits[i].UID < hits[j].UID })
	raw, err := json.Marshal(map[string]any{"nodes": hits})
	if err != nil {
		return nil, err
	}
	return &dgraph.DQLResponse{JSON: string(raw)}, nil
}

func (g *fakeGraph) MutateRDF(_ context.Context, rdf string) (map[string]string, error) {
	return g.mutate("MutateRDF", rdf, "")
}

func (g *fakeGraph) MutateWithDeletesRDF(_ context.Context, setRdf, delRdf string) (map[string]string, error) {
	return g.mutate("MutateWithDeletesRDF", setRdf, delRdf)
}

func (g *fakeGraph) DeleteRDF(_ context.Context, rdf string) error {
	_, err := g.mutate("DeleteRDF", "", rdf)
	return err
}

// mutate records the call and applies it as one mutation: blank nodes are
// allocated across set and del together, sets are applied before deletes so
// a same-predicate set+del ends deleted (P0 report INV-3: "delete wins").
func (g *fakeGraph) mutate(op, set, del string) (map[string]string, error) {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.calls = append(g.calls, graphCall{Step: g.step, Op: op, Set: set, Del: del})

	setQ, err := parsePayload(set)
	if err != nil {
		return nil, fmt.Errorf("fakeGraph %s set: %w", op, err)
	}
	delQ, err := parsePayload(del)
	if err != nil {
		return nil, fmt.Errorf("fakeGraph %s del: %w", op, err)
	}

	labels := map[string]bool{}
	for _, q := range append(append([]nquad(nil), setQ...), delQ...) {
		for _, term := range []string{q.Subject, q.Object} {
			if strings.HasPrefix(term, "_:") {
				labels[term] = true
			}
		}
	}
	sortedLabels := make([]string, 0, len(labels))
	for l := range labels {
		sortedLabels = append(sortedLabels, l)
	}
	sort.Strings(sortedLabels)
	assigned := map[string]string{}
	for _, l := range sortedLabels {
		g.nextUID++
		assigned[l] = fmt.Sprintf("0x%x", g.nextUID)
	}
	resolve := func(term string) (string, error) {
		if uid, ok := assigned[term]; ok {
			return uid, nil
		}
		if uidTerm.MatchString(term) {
			return term[1 : len(term)-1], nil
		}
		return "", fmt.Errorf("term %q is not a node reference", term)
	}

	for _, q := range setQ {
		s, err := resolve(q.Subject)
		if err != nil {
			return nil, err
		}
		pred := strings.Trim(q.Predicate, "<>")
		n := g.node(s)
		switch {
		case pred == "links":
			o, err := resolve(q.Object)
			if err != nil {
				return nil, err
			}
			n.edges[o] = q.Facets
		case pred == "dgraph.type":
			if !containsString(n.preds[pred], q.Object) {
				n.preds[pred] = append(n.preds[pred], q.Object)
			}
		case strings.HasPrefix(q.Object, `"`) && !q.HasFacets:
			n.preds[pred] = []string{q.Object}
			if pred == "unique_id" {
				id, err := literalValue(q.Object)
				if err != nil {
					return nil, err
				}
				if prev, ok := g.registry[s]; ok && prev != id {
					return nil, fmt.Errorf("uid %s already holds unique_id %q, cannot become %q", s, prev, id)
				}
				g.registry[s] = id
			}
		default:
			return nil, fmt.Errorf("unsupported set N-Quad %q", q.String())
		}
	}

	for _, q := range delQ {
		s, err := resolve(q.Subject)
		if err != nil {
			return nil, err
		}
		n := g.node(s)
		switch {
		case q.Predicate == "*" && q.Object == "*":
			n.preds = map[string][]string{}
			n.edges = map[string]string{}
		case q.Object == "*" && q.Predicate == "<links>":
			n.edges = map[string]string{}
		case q.Object == "*":
			delete(n.preds, strings.Trim(q.Predicate, "<>"))
		default:
			return nil, fmt.Errorf("unsupported delete N-Quad %q", q.String())
		}
	}

	out := make(map[string]string, len(assigned))
	for label, uid := range assigned {
		out[strings.TrimPrefix(label, "_:")] = uid
	}
	return out, nil
}

// --- N-Quad parsing -------------------------------------------------------

type nquad struct {
	Subject, Predicate, Object string
	Facets                     string
	HasFacets                  bool
}

func (q nquad) String() string {
	s := q.Subject + " " + q.Predicate + " " + q.Object
	if q.HasFacets {
		s += " (" + q.Facets + ")"
	}
	return s + " ."
}

var uidTerm = regexp.MustCompile(`^<0x[0-9a-f]+>$`)

func parsePayload(payload string) ([]nquad, error) {
	var out []nquad
	for _, line := range strings.Split(payload, "\n") {
		if strings.TrimSpace(line) == "" {
			continue
		}
		q, err := parseNQuad(line)
		if err != nil {
			return nil, err
		}
		out = append(out, q)
	}
	return out, nil
}

// parseNQuad parses one N-Quad as the ETL writes it and rejects any line
// whose formatting is not exactly `S P O[ (facets)] .` with single spaces,
// so canonical comparison never hides a whitespace difference.
func parseNQuad(line string) (nquad, error) {
	p := &termScanner{s: line}
	var q nquad
	var err error
	if q.Subject, err = p.term(); err != nil {
		return q, fmt.Errorf("%q: subject: %w", line, err)
	}
	if q.Predicate, err = p.term(); err != nil {
		return q, fmt.Errorf("%q: predicate: %w", line, err)
	}
	if q.Object, err = p.term(); err != nil {
		return q, fmt.Errorf("%q: object: %w", line, err)
	}
	p.skipSpace()
	if p.peek() == '(' {
		if q.Facets, err = p.facets(); err != nil {
			return q, fmt.Errorf("%q: facets: %w", line, err)
		}
		q.HasFacets = true
		p.skipSpace()
	}
	if p.peek() != '.' {
		return q, fmt.Errorf("%q: missing terminating '.'", line)
	}
	p.i++
	if strings.TrimSpace(p.s[p.i:]) != "" {
		return q, fmt.Errorf("%q: trailing text", line)
	}
	if q.String() != line {
		return q, fmt.Errorf("%q: not in canonical single-space form %q", line, q.String())
	}
	return q, nil
}

type termScanner struct {
	s string
	i int
}

func (p *termScanner) peek() byte {
	if p.i >= len(p.s) {
		return 0
	}
	return p.s[p.i]
}

func (p *termScanner) skipSpace() {
	for p.i < len(p.s) && (p.s[p.i] == ' ' || p.s[p.i] == '\t') {
		p.i++
	}
}

func (p *termScanner) term() (string, error) {
	p.skipSpace()
	start := p.i
	switch c := p.peek(); {
	case c == '<':
		end := strings.IndexByte(p.s[p.i:], '>')
		if end < 0 {
			return "", fmt.Errorf("unterminated IRI")
		}
		p.i += end + 1
	case c == '*':
		p.i++
	case strings.HasPrefix(p.s[p.i:], "_:"):
		for p.i < len(p.s) && p.s[p.i] != ' ' {
			p.i++
		}
	case c == '"':
		if err := p.quoted(); err != nil {
			return "", err
		}
		if strings.HasPrefix(p.s[p.i:], "^^<") {
			end := strings.IndexByte(p.s[p.i:], '>')
			if end < 0 {
				return "", fmt.Errorf("unterminated datatype IRI")
			}
			p.i += end + 1
		}
	default:
		return "", fmt.Errorf("unexpected %q at offset %d", c, p.i)
	}
	return p.s[start:p.i], nil
}

func (p *termScanner) quoted() error {
	p.i++ // opening quote
	for p.i < len(p.s) {
		switch p.s[p.i] {
		case '\\':
			p.i += 2
		case '"':
			p.i++
			return nil
		default:
			p.i++
		}
	}
	return fmt.Errorf("unterminated string literal")
}

func (p *termScanner) facets() (string, error) {
	p.i++ // '('
	start := p.i
	for p.i < len(p.s) {
		switch p.s[p.i] {
		case '"':
			if err := p.quoted(); err != nil {
				return "", err
			}
		case ')':
			out := p.s[start:p.i]
			p.i++
			return out, nil
		default:
			p.i++
		}
	}
	return "", fmt.Errorf("unterminated facets")
}

// parseFacets splits `k=v, k2=v2` respecting quoted values.
func parseFacets(raw string) (map[string]string, error) {
	out := map[string]string{}
	if raw == "" {
		return out, nil
	}
	p := &termScanner{s: raw}
	for p.i < len(p.s) {
		p.skipSpace()
		eq := strings.IndexByte(p.s[p.i:], '=')
		if eq < 0 {
			return nil, fmt.Errorf("facet without '=' in %q", raw)
		}
		key := p.s[p.i : p.i+eq]
		p.i += eq + 1
		start := p.i
		if p.peek() == '"' {
			if err := p.quoted(); err != nil {
				return nil, err
			}
		} else {
			for p.i < len(p.s) && p.s[p.i] != ',' {
				p.i++
			}
		}
		out[key] = p.s[start:p.i]
		p.skipSpace()
		if p.peek() == ',' {
			p.i++
		}
	}
	return out, nil
}

func parseQuotedList(s string) ([]string, error) {
	var out []string
	p := &termScanner{s: s}
	for {
		for p.i < len(p.s) && strings.ContainsRune(" \t\n,", rune(p.s[p.i])) {
			p.i++
		}
		if p.i >= len(p.s) {
			return out, nil
		}
		if p.peek() != '"' {
			return nil, fmt.Errorf("expected quoted id at offset %d in %q", p.i, s)
		}
		start := p.i
		if err := p.quoted(); err != nil {
			return nil, err
		}
		v, err := strconv.Unquote(p.s[start:p.i])
		if err != nil {
			return nil, err
		}
		out = append(out, v)
	}
}

func literalValue(obj string) (string, error) {
	if i := strings.LastIndex(obj, `"^^<`); i >= 0 {
		obj = obj[:i+1]
	}
	return strconv.Unquote(obj)
}

func containsString(list []string, v string) bool {
	for _, x := range list {
		if x == v {
			return true
		}
	}
	return false
}

// --- canonical form (parity/README.md §Canonical form) ----------------------

type canonWrite struct {
	Step string   `json:"step"`
	Op   string   `json:"op"`
	Set  []string `json:"set"`
	Del  []string `json:"del"`
}

type canonLookup struct {
	Step      string   `json:"step"`
	UniqueIDs []string `json:"unique_ids"`
}

// canonicalWrites renders every captured write in canonical form.
func (g *fakeGraph) canonicalWrites() ([]canonWrite, error) {
	g.mu.Lock()
	defer g.mu.Unlock()
	out := make([]canonWrite, 0, len(g.calls))
	for i, c := range g.calls {
		set, del, err := canonicalizeCall(c.Set, c.Del, g.registry)
		if err != nil {
			return nil, fmt.Errorf("write %d (%s/%s): %w", i, c.Step, c.Op, err)
		}
		out = append(out, canonWrite{Step: c.Step, Op: c.Op, Set: set, Del: del})
	}
	return out, nil
}

func (g *fakeGraph) canonicalLookups() []canonLookup {
	g.mu.Lock()
	defer g.mu.Unlock()
	out := make([]canonLookup, 0, len(g.lookups))
	for _, l := range g.lookups {
		out = append(out, canonLookup{Step: l.Step, UniqueIDs: l.UniqueIDs})
	}
	return out
}

// canonicalizeCall rewrites one mutation's set and del payloads. A blank node
// is named after the unique_id its own mutation assigns it, and each
// unique_id may own at most one blank label per mutation (P4-3's batch-local
// uniqueness property); uids are named after the unique_id they hold.
func canonicalizeCall(set, del string, registry map[string]string) ([]string, []string, error) {
	setQ, err := parsePayload(set)
	if err != nil {
		return nil, nil, err
	}
	delQ, err := parsePayload(del)
	if err != nil {
		return nil, nil, err
	}

	blankToID := map[string]string{}
	idToBlank := map[string]string{}
	for _, q := range setQ {
		if strings.HasPrefix(q.Subject, "_:") && q.Predicate == "<unique_id>" {
			id, err := literalValue(q.Object)
			if err != nil {
				return nil, nil, err
			}
			if prev, ok := blankToID[q.Subject]; ok && prev != id {
				return nil, nil, fmt.Errorf("blank node %s assigned two unique_ids %q and %q", q.Subject, prev, id)
			}
			if prev, ok := idToBlank[id]; ok && prev != q.Subject {
				return nil, nil, fmt.Errorf("unique_id %q has two blank labels %s and %s in one mutation", id, prev, q.Subject)
			}
			blankToID[q.Subject] = id
			idToBlank[id] = q.Subject
		}
	}

	rewrite := func(term string) (string, error) {
		switch {
		case strings.HasPrefix(term, "_:"):
			id, ok := blankToID[term]
			if !ok {
				return "", fmt.Errorf("blank node %s has no <unique_id> in its mutation", term)
			}
			return "_:new:" + id, nil
		case uidTerm.MatchString(term):
			uid := term[1 : len(term)-1]
			if id, ok := registry[uid]; ok {
				return "<uid:" + id + ">", nil
			}
			return "<uid:?" + uid + ">", nil
		default:
			return term, nil
		}
	}
	render := func(qs []nquad) ([]string, error) {
		lines := make([]string, 0, len(qs))
		for _, q := range qs {
			var err error
			if q.Subject, err = rewrite(q.Subject); err != nil {
				return nil, err
			}
			if q.Object, err = rewrite(q.Object); err != nil {
				return nil, err
			}
			lines = append(lines, q.String())
		}
		sort.Strings(lines)
		return lines, nil
	}
	setLines, err := render(setQ)
	if err != nil {
		return nil, nil, err
	}
	delLines, err := render(delQ)
	if err != nil {
		return nil, nil, err
	}
	return setLines, delLines, nil
}

// --- end-state snapshot ---------------------------------------------------

type canonEdge struct {
	To     string            `json:"to"`
	Facets map[string]string `json:"facets"`
}

type canonNode struct {
	Predicates map[string][]string `json:"predicates"`
	Links      []canonEdge         `json:"links"`
}

// snapshot returns the graph keyed by unique_id. Nodes with no predicates
// and no edges (deleted, or never written) are omitted.
func (g *fakeGraph) snapshot() (map[string]canonNode, error) {
	g.mu.Lock()
	defer g.mu.Unlock()
	name := func(uid string) string {
		if id, ok := g.registry[uid]; ok {
			return id
		}
		return "?" + uid
	}
	out := map[string]canonNode{}
	for uid, n := range g.nodes {
		if len(n.preds) == 0 && len(n.edges) == 0 {
			continue
		}
		cn := canonNode{Predicates: map[string][]string{}, Links: []canonEdge{}}
		for p, v := range n.preds {
			vals := append([]string(nil), v...)
			sort.Strings(vals)
			cn.Predicates[p] = vals
		}
		for to, raw := range n.edges {
			facets, err := parseFacets(raw)
			if err != nil {
				return nil, err
			}
			cn.Links = append(cn.Links, canonEdge{To: name(to), Facets: facets})
		}
		sort.Slice(cn.Links, func(i, j int) bool { return cn.Links[i].To < cn.Links[j].To })
		key := name(uid)
		if _, dup := out[key]; dup {
			return nil, fmt.Errorf("two live nodes hold unique_id %q", key)
		}
		out[key] = cn
	}
	return out, nil
}

// edgeList renders the snapshot's edges as sorted "from->to" strings.
func edgeList(snap map[string]canonNode) []string {
	out := []string{}
	for from, n := range snap {
		for _, e := range n.Links {
			out = append(out, from+"->"+e.To)
		}
	}
	sort.Strings(out)
	return out
}
