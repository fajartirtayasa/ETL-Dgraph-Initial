// Package guards holds the P4-14 synthetic-data and credential guards (REFACTOR_PLAN.md PD-3, D4)
// for this repository. They are ordinary tests in the default `go test ./...` run: no build tag, no
// flag, no CI step (PD-10). They read only files in this checkout - the tracked list comes from
// parsing .git/index directly, so no git binary, no network, no AWS and no credential is involved.
//
//	(a) no tracked database dump or bulk extract outside parity/fixture/
//	(b) no tracked .env or credential-bearing file, no secret shape or high-entropy literal in tracked text
//	(c) every fixture row carries the SYN- marker, and nothing in the fixture or goldens looks real
//	(d) the §8.5 key contract is a Python-repository check: this Go code has no Secrets Manager
//	    loader (pkg/aws is S3-only), so there is no required-key list here to compare
//
// Every check has a negative test (TestNegative*) that plants what it forbids. The Python repository
// (ETL-Dgraph-Incremental tests/guards.py) implements the same rules.
package guards

import (
	"bytes"
	"crypto/sha1"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"os"
	"path"
	"path/filepath"
	"regexp"
	"runtime"
	"sort"
	"strings"
	"testing"
	"unicode/utf8"
)

const synthetic = "SYN-"

func repoRoot(t *testing.T) string {
	_, file, _, _ := runtime.Caller(0)
	return filepath.Clean(filepath.Join(filepath.Dir(file), "..", ".."))
}

// ---- tracked files, from .git/index (index-format.txt, versions 2-4) ----------------------------
func parseIndex(data []byte) ([]string, error) {
	if len(data) < 12 || string(data[:4]) != "DIRC" {
		return nil, errors.New("not a git index")
	}
	version, count := binary.BigEndian.Uint32(data[4:8]), binary.BigEndian.Uint32(data[8:12])
	if version < 2 || version > 4 {
		return nil, fmt.Errorf("unsupported index version %d", version)
	}
	pos, prev, seen := 12, []byte{}, map[string]bool{}
	for i := uint32(0); i < count; i++ {
		start := pos
		flags := binary.BigEndian.Uint16(data[pos+60 : pos+62])
		pos += 62
		if version >= 3 && flags&0x4000 != 0 {
			pos += 2
		}
		var name []byte
		if version == 4 {
			b := data[pos]
			pos++
			strip := int(b & 0x7f)
			for b&0x80 != 0 {
				b = data[pos]
				pos++
				strip = ((strip + 1) << 7) | int(b&0x7f)
			}
			end := pos + bytes.IndexByte(data[pos:], 0)
			name = append(append([]byte{}, prev[:len(prev)-strip]...), data[pos:end]...)
			pos = end + 1
		} else {
			end := pos + bytes.IndexByte(data[pos:], 0)
			name = data[pos:end]
			pos = start + ((end-start)/8+1)*8
		}
		prev = name
		seen[string(name)] = true
	}
	out := make([]string, 0, len(seen))
	for p := range seen {
		out = append(out, p)
	}
	sort.Strings(out)
	return out, nil
}

func trackedFiles(t *testing.T, root string) []string {
	t.Helper()
	gitDir := filepath.Join(root, ".git")
	if st, err := os.Stat(gitDir); err == nil && !st.IsDir() {
		raw, _ := os.ReadFile(gitDir)
		gitDir = filepath.Join(root, strings.TrimSpace(strings.TrimPrefix(string(raw), "gitdir:")))
	}
	data, err := os.ReadFile(filepath.Join(gitDir, "index"))
	if err != nil {
		t.Fatalf("the guards need a git checkout: %v", err)
	}
	paths, err := parseIndex(data)
	if err != nil {
		t.Fatal(err)
	}
	return paths
}

// ---- (a) dumps and bulk extracts --------------------------------------------------------------------
var dumpPatterns = []string{"*.sql", "*.sql.gz", "*.dump", "*.pgdump", "*.backup", "*.bak", "*.csv", "*.tsv",
	"*.parquet", "*.rdf", "*.rdf.gz", "*.nq", "*.nq.gz", "*.schema.gz"}

// fixtureDirs may hold .sql: the synthetic fixture's schema and generated seed (P1-6).
var fixtureDirs = []string{"parity/fixture/"}

func matchesAny(p string, patterns []string) bool {
	name := strings.ToLower(path.Base(p))
	for _, pat := range patterns {
		if ok, _ := path.Match(pat, name); ok {
			return true
		}
	}
	return false
}

func dumpViolations(paths, fixtures []string) []string {
	var out []string
	for _, p := range paths {
		exempt := false
		for _, d := range fixtures {
			exempt = exempt || strings.HasPrefix(p, d)
		}
		if matchesAny(p, dumpPatterns) && !exempt {
			out = append(out, p)
		}
	}
	return out
}

// ---- (b) credential files, secret shapes, high entropy ------------------------------------------------
var credentialPatterns = []string{".env", ".env.*", "*.env", "*.pem", "*.key", "*.p12", "*.pfx", "*.jks", "*.keystore",
	"id_rsa*", "id_dsa*", "id_ecdsa*", "id_ed25519*", "credentials", "credentials.json", "secrets.json", "*.secret",
	"*.secrets", ".pgpass", ".netrc", "kubeconfig"}

func credentialFileViolations(paths []string) []string {
	var out []string
	for _, p := range paths {
		if strings.ToLower(path.Base(p)) == ".env.example" {
			continue // placeholders only; scanned as text like everything else
		}
		if matchesAny(p, credentialPatterns) || strings.HasPrefix(p, ".aws/") || strings.Contains(p, "/.aws/") {
			out = append(out, p)
		}
	}
	return out
}

var secretShapes = map[string]*regexp.Regexp{
	"AWS access key id":             regexp.MustCompile(`\b(AKIA|ASIA|AGPA|AIDA|AROA)[0-9A-Z]{16}\b`),
	"private key block":             regexp.MustCompile(`-----BEGIN [A-Z ]*PRIVATE KEY-----`),
	"GitHub token":                  regexp.MustCompile(`\bgh[pousr]_[A-Za-z0-9]{36,}\b`),
	"Slack token":                   regexp.MustCompile(`\bxox[abprs]-[A-Za-z0-9-]{10,}`),
	"URL with an embedded password": regexp.MustCompile(`\b[a-z][a-z0-9+.-]*://[^\s:/@'"]+:([^\s@/'"$<{]{6,})@`),
}
var tokenRx = regexp.MustCompile(`[A-Za-z0-9+/=_\-]{20,}`)
var classRx = []*regexp.Regexp{regexp.MustCompile(`[a-z]`), regexp.MustCompile(`[A-Z]`), regexp.MustCompile(`[0-9]`)}

func shannon(s string) float64 {
	counts := map[rune]int{}
	for _, r := range s {
		counts[r]++
	}
	n, h := float64(utf8.RuneCountInString(s)), 0.0
	for _, c := range counts {
		p := float64(c) / n
		h -= p * math.Log2(p)
	}
	return h
}

// looksRandom: high entropy AND all three character classes. Lower-case hex (sha256, git SHAs) has no
// upper-case letter and is never flagged; identifiers and prose fall far below 4.5 bits/char.
// An alphabet constant (a random-string generator's charset, pkg/util/random_generator.go) has maximal
// entropy by construction; a run of ten ascending characters marks it, and never occurs in a real secret.
var charsetRuns = []string{"0123456789", "ABCDEFGHIJ", "abcdefghij"}

func looksRandom(tok string) bool {
	core := strings.Trim(tok, "=-_")
	for _, run := range charsetRuns {
		if strings.Contains(core, run) {
			return false
		}
	}
	if len(core) < 20 {
		return false
	}
	for _, rx := range classRx {
		if !rx.MatchString(core) {
			return false
		}
	}
	return shannon(core) >= 4.5
}

// textExempt are tracked files that are not source and hold checksums by design.
var textExempt = map[string]string{
	"go.sum": "module checksums (h1: base64 SHA-256 of public modules), not credentials",
}

func entropyViolations(path, text string) []string {
	var out []string
	for n, line := range strings.Split(text, "\n") {
		for kind, rx := range secretShapes {
			if rx.MatchString(line) {
				out = append(out, fmt.Sprintf("%s:%d: %s", path, n+1, kind))
			}
		}
		for _, tok := range tokenRx.FindAllString(line, -1) {
			if looksRandom(tok) {
				out = append(out, fmt.Sprintf("%s:%d: high-entropy literal %.12s…", path, n+1, tok))
			}
		}
	}
	return out
}

// ---- (c) the synthetic marker and real-data heuristics --------------------------------------------------
var realData = map[string]*regexp.Regexp{
	"email address":            regexp.MustCompile(`[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}`),
	"UUID":                     regexp.MustCompile(`\b[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}\b`),
	"IPv4 address":             regexp.MustCompile(`\b(?:\d{1,3}\.){3}\d{1,3}\b`),
	"AWS resource":             regexp.MustCompile(`(?i)arn:aws:|amazonaws\.com`),
	"Indonesian phone number":  regexp.MustCompile(`(?:^|\D)(?:\+62|62|0)8\d{8,11}(?:\D|$)`),
	"Indonesian tax id (NPWP)": regexp.MustCompile(`\b\d{2}\.\d{3}\.\d{3}\.\d-\d{3}\.\d{3}\b`),
	"company legal form":       regexp.MustCompile(`\bPT\.?\s+[A-Z][A-Za-z]+|\bTbk\b|\bCV\.?\s+[A-Z][a-z]+`),
	"real organisation name":   regexp.MustCompile(`(?i)sinar\s*mas|golden\s*agri|dattabot|hara\.ag`),
}
var exampleDomain = regexp.MustCompile(`(?i)@([a-z0-9-]+\.)*(example\.(com|org|net)|[a-z0-9-]+\.(example|invalid|test|localhost))$`)
var safeIPv4 = regexp.MustCompile(`^(127\.|0\.0\.0\.0$|192\.0\.2\.|198\.51\.100\.|203\.0\.113\.)`)

func realDataHits(value string) []string {
	var out []string
	for kind, rx := range realData {
		for _, m := range rx.FindAllString(value, -1) {
			if kind == "email address" && exampleDomain.MatchString(m) {
				continue
			}
			if kind == "IPv4 address" {
				bad := false
				for _, o := range strings.Split(m, ".") {
					var v int
					fmt.Sscanf(o, "%d", &v)
					bad = bad || v > 255
				}
				if bad || safeIPv4.MatchString(m) {
					continue
				}
			}
			out = append(out, fmt.Sprintf("%s: %q", kind, m))
		}
	}
	sort.Strings(out)
	return out
}

// sqlRow is one VALUES tuple of the generated seed: column name -> value (nil = NULL).
type sqlRow struct {
	table string
	cells map[string]*string
}

var insertRx = regexp.MustCompile(`(?s)INSERT INTO (\w+) \(([^)]*)\) VALUES`)

// parseSeed reads the INSERT ... VALUES tuples of parity/fixture/sql/02_seed.sql: 'text' with ''
// escapes, E'text' with backslash escapes, NULL and bare numbers.
func parseSeed(sql string) ([]sqlRow, error) {
	var rows []sqlRow
	for _, loc := range insertRx.FindAllStringSubmatchIndex(sql, -1) {
		table := sql[loc[2]:loc[3]]
		var cols []string
		for _, c := range strings.Split(sql[loc[4]:loc[5]], ",") {
			cols = append(cols, strings.TrimSpace(c))
		}
		i := loc[1]
		for {
			for i < len(sql) && strings.ContainsRune(" \t\r\n,", rune(sql[i])) {
				i++
			}
			if i >= len(sql) || sql[i] == ';' {
				break
			}
			if sql[i] != '(' {
				return nil, fmt.Errorf("%s: expected '(' at offset %d", table, i)
			}
			i++
			var vals []*string
			for {
				for sql[i] == ' ' {
					i++
				}
				switch {
				case strings.HasPrefix(sql[i:], "NULL"):
					vals = append(vals, nil)
					i += 4
				case sql[i] == '\'' || strings.HasPrefix(sql[i:], "E'"):
					escapes := sql[i] == 'E'
					if escapes {
						i++
					}
					i++
					var b strings.Builder
					for {
						c := sql[i]
						if escapes && c == '\\' {
							b.WriteByte(sql[i+1])
							i += 2
							continue
						}
						if c == '\'' {
							if i+1 < len(sql) && sql[i+1] == '\'' {
								b.WriteByte('\'')
								i += 2
								continue
							}
							i++
							break
						}
						b.WriteByte(c)
						i++
					}
					s := b.String()
					vals = append(vals, &s)
				default:
					j := i
					for j < len(sql) && sql[j] != ',' && sql[j] != ')' {
						j++
					}
					s := strings.TrimSpace(sql[i:j])
					vals = append(vals, &s)
					i = j
				}
				for sql[i] == ' ' {
					i++
				}
				if sql[i] == ',' {
					i++
					continue
				}
				if sql[i] == ')' {
					i++
					break
				}
				return nil, fmt.Errorf("%s: unexpected %q at offset %d", table, sql[i], i)
			}
			if len(vals) != len(cols) {
				return nil, fmt.Errorf("%s: %d values for %d columns", table, len(vals), len(cols))
			}
			row := sqlRow{table: table, cells: map[string]*string{}}
			for k, c := range cols {
				row.cells[c] = vals[k]
			}
			rows = append(rows, row)
		}
	}
	return rows, nil
}

var idColumn = regexp.MustCompile(`^(unique_id|event[12]_id|[a-z_]*_id)$`)

// synthetic integer keys: links_bc.id is 9S000k, S = scenario 1-7 (fixture/gen/main.go). Not
// CHECK-enforced in 01_schema.sql - only text identifiers are.
var syntheticNumericID = regexp.MustCompile(`^9[1-7]\d{4}$`)

func seedViolations(rows []sqlRow) []string {
	var out []string
	for n, r := range rows {
		marked := false
		for col, v := range r.cells {
			if v == nil {
				continue
			}
			if strings.HasPrefix(*v, synthetic) {
				marked = true
			}
			if idColumn.MatchString(col) && *v != "" && !strings.HasPrefix(*v, synthetic) {
				out = append(out, fmt.Sprintf("%s row %d: %s=%.20q is not %s-marked", r.table, n, col, *v, synthetic))
			}
			if col == "id" && !syntheticNumericID.MatchString(*v) {
				out = append(out, fmt.Sprintf("%s row %d: id=%s is outside the synthetic block 9S000k", r.table, n, *v))
			}
			for _, h := range realDataHits(*v) {
				out = append(out, fmt.Sprintf("%s row %d: %s", r.table, n, h))
			}
		}
		if !marked {
			out = append(out, fmt.Sprintf("%s row %d carries no %s value", r.table, n, synthetic))
		}
	}
	return out
}

func jsonStrings(v any, out *[]string) {
	switch x := v.(type) {
	case string:
		*out = append(*out, x)
	case map[string]any:
		for k, val := range x {
			*out = append(*out, k)
			jsonStrings(val, out)
		}
	case []any:
		for _, val := range x {
			jsonStrings(val, out)
		}
	}
}

func graphSeedViolations(raw []byte) []string {
	var seed struct {
		Nodes []struct {
			UniqueID string `json:"unique_id"`
		} `json:"nodes"`
		Edges []struct {
			From string `json:"from"`
			To   string `json:"to"`
		} `json:"edges"`
	}
	if err := json.Unmarshal(raw, &seed); err != nil {
		return []string{"graph_seed.json: " + err.Error()}
	}
	var out []string
	for _, n := range seed.Nodes {
		if !strings.HasPrefix(n.UniqueID, synthetic) {
			out = append(out, fmt.Sprintf("graph_seed node %.20q is not %s-marked", n.UniqueID, synthetic))
		}
	}
	for _, e := range seed.Edges {
		if !strings.HasPrefix(e.From, synthetic) || !strings.HasPrefix(e.To, synthetic) {
			out = append(out, fmt.Sprintf("graph_seed edge %.20q -> %.20q is not %s-marked", e.From, e.To, synthetic))
		}
	}
	return append(out, jsonDataHits("graph_seed.json", raw)...)
}

func jsonDataHits(name string, raw []byte) []string {
	var v any
	if err := json.Unmarshal(raw, &v); err != nil {
		return []string{name + ": " + err.Error()}
	}
	var strs, out []string
	jsonStrings(v, &strs)
	for _, s := range strs {
		for _, h := range realDataHits(s) {
			out = append(out, name+": "+h)
		}
	}
	return out
}

// ==== the guards over this repository ====================================================================
func TestGuardA_NoDumpOrExtractIsTracked(t *testing.T) {
	if v := dumpViolations(trackedFiles(t, repoRoot(t)), fixtureDirs); len(v) > 0 {
		t.Fatalf("tracked dump / extract files outside %v: %v", fixtureDirs, v)
	}
}

func TestGuardB_NoCredentialFileIsTracked(t *testing.T) {
	tracked := trackedFiles(t, repoRoot(t))
	if v := credentialFileViolations(tracked); len(v) > 0 {
		t.Fatalf("tracked credential files: %v", v)
	}
}

func TestGuardB_NoSecretOrHighEntropyLiteralInTrackedText(t *testing.T) {
	root := repoRoot(t)
	var hits []string
	for _, p := range trackedFiles(t, root) {
		if _, exempt := textExempt[p]; exempt {
			continue
		}
		raw, err := os.ReadFile(filepath.Join(root, filepath.FromSlash(p)))
		if err != nil || !utf8.Valid(raw) || bytes.IndexByte(raw, 0) >= 0 {
			continue // binary: not source
		}
		hits = append(hits, entropyViolations(p, string(raw))...)
	}
	if len(hits) > 0 {
		t.Fatalf("secret-looking content in tracked text:\n%s", strings.Join(hits, "\n"))
	}
}

func TestGuardC_EveryFixtureRowIsSyntheticAndNothingLooksReal(t *testing.T) {
	root := repoRoot(t)
	read := func(rel string) []byte {
		raw, err := os.ReadFile(filepath.Join(root, filepath.FromSlash(rel)))
		if err != nil {
			t.Fatal(err)
		}
		return raw
	}
	rows, err := parseSeed(string(read("parity/fixture/sql/02_seed.sql")))
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) < 30 {
		t.Fatalf("parsed only %d seed rows: the parser lost some", len(rows))
	}
	v := seedViolations(rows)
	v = append(v, graphSeedViolations(read("parity/fixture/graph_seed.json"))...)
	v = append(v, jsonDataHits("scenarios.json", read("parity/fixture/scenarios.json"))...)
	goldens := 0
	for _, p := range trackedFiles(t, root) {
		if strings.HasPrefix(p, "parity/golden/") && strings.HasSuffix(p, ".json") {
			v = append(v, jsonDataHits(p, read(p))...)
			goldens++
		}
	}
	if goldens != 28 {
		t.Fatalf("expected the 28 goldens, found %d", goldens)
	}
	if len(v) > 0 {
		t.Fatalf("fixture violations:\n%s", strings.Join(v, "\n"))
	}
}

// ==== negative tests: each check fails against a planted fixture ===========================================
func buildIndex(version uint32, names []string) []byte {
	var b bytes.Buffer
	b.WriteString("DIRC")
	binary.Write(&b, binary.BigEndian, version)
	binary.Write(&b, binary.BigEndian, uint32(len(names)))
	prev := ""
	for _, n := range names {
		var e bytes.Buffer
		e.Write(make([]byte, 40))
		sum := sha1.Sum([]byte(n))
		e.Write(sum[:])
		binary.Write(&e, binary.BigEndian, uint16(len(n)))
		if version == 4 {
			common := 0
			for common < len(prev) && common < len(n) && prev[common] == n[common] {
				common++
			}
			e.WriteByte(byte(len(prev) - common))
			e.WriteString(n[common:])
			e.WriteByte(0)
		} else {
			e.WriteString(n)
			e.Write(make([]byte, 8-e.Len()%8))
		}
		b.Write(e.Bytes())
		prev = n
	}
	return b.Bytes()
}

func TestIndexParserReadsEveryPath(t *testing.T) {
	names := []string{"a.txt", "docs/x.md", "docs/xy.md", "parity/golden/l1/int_literal.json", "z/very/deep/path.go"}
	for _, v := range []uint32{2, 3, 4} {
		got, err := parseIndex(buildIndex(v, names))
		if err != nil || strings.Join(got, ",") != strings.Join(names, ",") {
			t.Fatalf("v%d: %v %v", v, got, err)
		}
	}
}

func TestNegativeA_PlantedDumpIsReported(t *testing.T) {
	planted := []string{"main.go", "backup/events_bc.sql", "out/links.rdf.gz", "exports/prod.csv", "x/db.dump",
		"parity/fixture/sql/02_seed.sql"}
	got := dumpViolations(planted, fixtureDirs)
	if strings.Join(got, ",") != strings.Join(planted[1:5], ",") {
		t.Fatalf("got %v", got)
	}
}

func TestNegativeB_PlantedCredentialFileIsReported(t *testing.T) {
	planted := []string{".env", "config/.env.production", "deploy/stg.env", "keys/server.pem", "id_rsa",
		".aws/credentials", "secrets.json", ".env.example", "docs/env.md"}
	got := credentialFileViolations(planted)
	if strings.Join(got, ",") != strings.Join(planted[:7], ",") {
		t.Fatalf("got %v", got)
	}
}

func TestNegativeB_PlantedSecretIsReported(t *testing.T) {
	// assembled from parts so this file never holds one whole
	planted := map[string]string{
		"AWS access key id":             "AKIA" + "Z7QW3RTY5UIO9PAS",
		"private key block":             "-----BEGIN " + "RSA PRIVATE KEY-----",
		"URL with an embedded password": "postgres://etl:" + "Pr0dPassw0rd" + "@db.internal:5432/x",
		"high-entropy literal":          "wJalrXUtnFEMI" + "K7MDENGbPxRf" + "iCYzEXAMPLEKEY9q",
	}
	for kind, secret := range planted {
		hits := entropyViolations("planted.go", "x := \""+secret+"\"")
		if !strings.Contains(strings.Join(hits, "\n"), kind) {
			t.Errorf("%s not reported: %v", kind, hits)
		}
	}
	benign := "b487294e04cdb6fea5e49224364830e9762d2f58 your_secret_password DB_CLEAN_PASSWORD=$(DB_CLEAN_PASSWORD) " +
		"postgres://postgres@host.docker.internal:55432/parity_fixture TestParityL3PipelineEndStateIndependentOfChunking"
	if hits := entropyViolations("benign.txt", benign); len(hits) > 0 {
		t.Errorf("benign content flagged: %v", hits)
	}
}

func TestNegativeC_UnmarkedSeedRowIsReported(t *testing.T) {
	sql := "INSERT INTO events_bc (unique_id, product_name, status) VALUES\n" +
		"    ('SYN-S9-E01', E'SYN \\\\ ok', 'valid'),\n" +
		"    ('EVT-2024-000123', 'x', 'valid');\n" +
		"INSERT INTO links_bc (id, event1_id, event2_id) VALUES\n" +
		"    (970001, 'SYN-A', 'SYN-B'),\n" +
		"    (18234567, 'SYN-A', 'SYN-B');\n"
	rows, err := parseSeed(sql)
	if err != nil || len(rows) != 4 {
		t.Fatalf("parse: %v %d", err, len(rows))
	}
	got := strings.Join(seedViolations(rows), "\n")
	for _, want := range []string{`events_bc row 1: unique_id="EVT-2024-000123"`, "events_bc row 1 carries no SYN- value",
		"links_bc row 3: id=18234567 is outside"} {
		if !strings.Contains(got, want) {
			t.Errorf("missing %q in:\n%s", want, got)
		}
	}
	if strings.Contains(got, "links_bc row 2") {
		t.Errorf("970001 is inside the synthetic block (S=7) and must not be reported:\n%s", got)
	}
}

func TestNegativeC_EachRealDataHeuristicFires(t *testing.T) {
	cases := map[string]string{
		"email address":            "budi.santoso@" + "perusahaan.co.id",
		"UUID":                     "b3a1c2d4-5e6f-4a7b-8c9d-0e1f2a3b4c5d",
		"IPv4 address":             "172.30." + "0.117:9080",
		"AWS resource":             "arn:aws:secretsmanager:ap-southeast-3:000000000000:secret:x",
		"Indonesian phone number":  "call 0812" + "34567890",
		"Indonesian tax id (NPWP)": "01.234.567.8-901.000",
		"company legal form":       "PT " + "Contoh Sawit Tbk",
		"real organisation name":   "Sinar " + "Mas mill",
	}
	for kind, v := range cases {
		if !strings.Contains(strings.Join(realDataHits(v), "\n"), kind) {
			t.Errorf("%s did not fire on %q", kind, v)
		}
		rows := []sqlRow{{table: "events_bc", cells: map[string]*string{"unique_id": ptr("SYN-X"), "product_name": ptr(v)}}}
		if !strings.Contains(strings.Join(seedViolations(rows), "\n"), kind) {
			t.Errorf("%s not reported for a seed row", kind)
		}
	}
	for _, ok := range []string{"SYN Mill One", "12345678901234567890.125", "ops@example.com", "127.0.0.1:55432", "1700000000"} {
		if h := realDataHits(ok); len(h) > 0 {
			t.Errorf("synthetic value %q flagged: %v", ok, h)
		}
	}
}

func TestNegativeC_UnmarkedGraphSeedIsReported(t *testing.T) {
	raw := []byte(`{"nodes":[{"unique_id":"SYN-A"},{"unique_id":"EV-77"}],"edges":[{"from":"SYN-A","to":"BL-1"}]}`)
	if v := graphSeedViolations(raw); len(v) != 2 {
		t.Fatalf("got %v", v)
	}
}

func ptr(s string) *string { return &s }
