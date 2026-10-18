package incremental

// Golden-file plumbing for the P1 parity harness (REFACTOR_PLAN.md §4).
// Goldens live under parity/golden as language-neutral JSON; the format is
// specified in parity/README.md. Regenerate with:
//
//	go test ./internal/etl/incremental -run Parity -args -update

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"io/fs"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"runtime"
	"strings"
	"sync"
	"testing"
)

var updateGolden = flag.Bool("update", false, "rewrite parity golden files under parity/golden")

const (
	goldenFormat = "dgraph-etl-parity/golden/v1"
	// baselineCommit is the P1 baseline (REFACTOR_PLAN.md P0-0).
	baselineCommit = "b487294e04cdb6fea5e49224364830e9762d2f58"
)

// Comparison rules, one per golden kind. They are written into every file so
// a consumer in another language needs nothing but the file to compare.
const (
	compareL1 = "exact: for every case, the function's output must equal `expected` byte-for-byte " +
		"(strings compared as Unicode text; see each file's notes for non-parity cases)"
	compareWrites = "canonical: `writes` is ordered and compared element by element. Each write's `set` and `del` are " +
		"its N-Quad lines after canonicalisation (parity/README.md §Canonical form): split on newline, blank lines " +
		"dropped, `<0x..>` rewritten to `<uid:UNIQUE_ID>`, blank nodes rewritten to `_:new:UNIQUE_ID`, sorted bytewise. " +
		"Line text is otherwise byte-exact, including literal escaping and facet order. Blank-node label strings are " +
		"NOT compared (P4-3); one label per unique_id per mutation IS required."
	compareRowset = "set: `rows` compared as a multiset (they are stored sorted by their JSON encoding); SQL text must " +
		"be byte-identical (`sql_sha256`); values are as scanned by the Go ETL (see `value_encoding`)"
	comparePipeline = compareWrites + " `graph_after` and `sync_time_stamped` are compared exactly. " +
		"`lookups_informational` documents reads and is not part of the contract."
)

type goldenProvenance struct {
	GeneratorCommit string `json:"generator_commit"`
	BaselineCommit  string `json:"baseline_commit"`
	GeneratedBy     string `json:"generated_by"`
	GoVersion       string `json:"go_version"`
}

type goldenFile struct {
	Format      string            `json:"format"`
	Kind        string            `json:"kind"`
	Name        string            `json:"name"`
	Description string            `json:"description"`
	Provenance  *goldenProvenance `json:"provenance"`
	Comparison  string            `json:"comparison"`
	Data        any               `json:"data"`
}

var (
	repoRootOnce sync.Once
	repoRootPath string
	repoRootErr  error

	provenanceOnce sync.Once
	provenanceSHA  string
	provenanceErr  error
)

func repoRoot(t testing.TB) string {
	t.Helper()
	repoRootOnce.Do(func() {
		dir, err := filepath.Abs(filepath.Join("..", "..", ".."))
		if err != nil {
			repoRootErr = err
			return
		}
		if _, err := os.Stat(filepath.Join(dir, "go.mod")); err != nil {
			repoRootErr = fmt.Errorf("repository root not found at %s: %w", dir, err)
			return
		}
		repoRootPath = dir
	})
	if repoRootErr != nil {
		t.Fatal(repoRootErr)
	}
	return repoRootPath
}

func goldenPath(t testing.TB, rel string) string {
	return filepath.Join(repoRoot(t), "parity", "golden", filepath.FromSlash(rel))
}

func gitCmd(root string, args ...string) (string, error) {
	cmd := exec.Command("git", append([]string{"-C", root}, args...)...)
	out, err := cmd.CombinedOutput()
	return strings.TrimSpace(string(out)), err
}

// etlSourcePathspec selects the Go sources whose behaviour the goldens pin:
// everything that is not a test file and not the parity harness itself.
var etlSourcePathspec = []string{"--", "main.go", "go.mod", "go.sum", "internal", "pkg", ":(exclude,glob)**/*_test.go"}

// currentGeneratorCommit returns HEAD, refusing when the ETL sources differ
// from it: the stamped SHA must describe the code that produced the output.
func currentGeneratorCommit(t testing.TB) string {
	t.Helper()
	provenanceOnce.Do(func() {
		root := repoRoot(t)
		sha, err := gitCmd(root, "rev-parse", "HEAD")
		if err != nil {
			provenanceErr = fmt.Errorf("git rev-parse HEAD: %v: %s", err, sha)
			return
		}
		status, err := gitCmd(root, append([]string{"status", "--porcelain", "--untracked-files=all"}, etlSourcePathspec...)...)
		if err != nil {
			provenanceErr = fmt.Errorf("git status: %v: %s", err, status)
			return
		}
		if status != "" {
			provenanceErr = fmt.Errorf("refusing to stamp goldens: ETL sources differ from HEAD %s:\n%s", sha, status)
			return
		}
		provenanceSHA = sha
	})
	if provenanceErr != nil {
		t.Fatal(provenanceErr)
	}
	return provenanceSHA
}

func marshalJSON(v any) ([]byte, error) {
	var b bytes.Buffer
	enc := json.NewEncoder(&b)
	enc.SetEscapeHTML(false)
	enc.SetIndent("", "  ")
	if err := enc.Encode(v); err != nil {
		return nil, err
	}
	return b.Bytes(), nil
}

// normalizeJSON re-encodes any JSON value through a generic decode so two
// documents compare equal regardless of Go struct field order.
func normalizeJSON(t testing.TB, v any) string {
	t.Helper()
	raw, err := marshalJSON(v)
	if err != nil {
		t.Fatal(err)
	}
	var generic any
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.UseNumber()
	if err := dec.Decode(&generic); err != nil {
		t.Fatal(err)
	}
	out, err := marshalJSON(generic)
	if err != nil {
		t.Fatal(err)
	}
	return string(out)
}

// assertGolden writes (-update) or verifies one golden file.
func assertGolden(t *testing.T, rel, kind, description, comparison string, data any) {
	t.Helper()
	path := goldenPath(t, rel)
	name := strings.TrimSuffix(rel, ".json")

	if *updateGolden {
		doc := goldenFile{
			Format:      goldenFormat,
			Kind:        kind,
			Name:        name,
			Description: description,
			Provenance: &goldenProvenance{
				GeneratorCommit: currentGeneratorCommit(t),
				BaselineCommit:  baselineCommit,
				GeneratedBy:     t.Name(),
				GoVersion:       runtime.Version(),
			},
			Comparison: comparison,
			Data:       data,
		}
		raw, err := marshalJSON(doc)
		if err != nil {
			t.Fatal(err)
		}
		if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, raw, 0o644); err != nil {
			t.Fatal(err)
		}
		return
	}

	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read golden %s: %v (regenerate with -args -update)", rel, err)
	}
	var stored struct {
		goldenFile
		Data json.RawMessage `json:"data"`
	}
	if err := json.Unmarshal(raw, &stored); err != nil {
		t.Fatalf("parse golden %s: %v", rel, err)
	}
	if err := checkProvenance(stored.Provenance); err != nil {
		t.Errorf("golden %s: %v", rel, err)
	}

	header := func(f, k, n, d, c string) string { return strings.Join([]string{f, k, n, d, c}, "\n") }
	if got, want := header(goldenFormat, kind, name, description, comparison),
		header(stored.Format, stored.Kind, stored.Name, stored.Description, stored.Comparison); got != want {
		t.Errorf("golden %s header differs (regenerate with -args -update):\n got: %s\nwant: %s", rel, got, want)
	}

	var storedData any
	dec := json.NewDecoder(bytes.NewReader(stored.Data))
	dec.UseNumber()
	if err := dec.Decode(&storedData); err != nil {
		t.Fatalf("parse golden %s data: %v", rel, err)
	}
	got, want := normalizeJSON(t, data), normalizeJSON(t, storedData)
	if got != want {
		t.Errorf("golden %s mismatch:\n%s", rel, lineDiff(want, got))
	}
}

var shaPattern = regexp.MustCompile(`^[0-9a-f]{40}$`)

func checkProvenance(p *goldenProvenance) error {
	if p == nil {
		return fmt.Errorf("missing provenance")
	}
	if !shaPattern.MatchString(p.GeneratorCommit) {
		return fmt.Errorf("provenance.generator_commit %q is not a full commit SHA", p.GeneratorCommit)
	}
	if p.BaselineCommit != baselineCommit {
		return fmt.Errorf("provenance.baseline_commit %q, want %s", p.BaselineCommit, baselineCommit)
	}
	return nil
}

// lineDiff renders the first differing region of two multi-line strings.
func lineDiff(want, got string) string {
	w, g := strings.Split(want, "\n"), strings.Split(got, "\n")
	i := 0
	for i < len(w) && i < len(g) && w[i] == g[i] {
		i++
	}
	var b strings.Builder
	from := max(0, i-3)
	for j := from; j < i; j++ {
		fmt.Fprintf(&b, "  %s\n", w[j])
	}
	for j := i; j < min(len(w), i+8); j++ {
		fmt.Fprintf(&b, "- %s\n", w[j])
	}
	for j := i; j < min(len(g), i+8); j++ {
		fmt.Fprintf(&b, "+ %s\n", g[j])
	}
	return b.String()
}

// TestParityGoldenProvenance checks every committed golden carries a full
// commit SHA that resolves in this repository, and that the ETL sources have
// not changed since that commit (otherwise the goldens describe other code).
func TestParityGoldenProvenance(t *testing.T) {
	root := repoRoot(t)
	dir := filepath.Join(root, "parity", "golden")
	shas := map[string][]string{}
	err := filepath.WalkDir(dir, func(path string, d fs.DirEntry, err error) error {
		if err != nil || d.IsDir() || !strings.HasSuffix(path, ".json") {
			return err
		}
		raw, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		var doc goldenFile
		if err := json.Unmarshal(raw, &doc); err != nil {
			return fmt.Errorf("%s: %w", path, err)
		}
		rel, _ := filepath.Rel(dir, path)
		if doc.Format != goldenFormat {
			t.Errorf("%s: format %q, want %q", rel, doc.Format, goldenFormat)
		}
		if err := checkProvenance(doc.Provenance); err != nil {
			t.Errorf("%s: %v", rel, err)
			return nil
		}
		shas[doc.Provenance.GeneratorCommit] = append(shas[doc.Provenance.GeneratorCommit], rel)
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(shas) == 0 {
		t.Fatal("no golden files found under parity/golden")
	}

	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("git not available; SHA resolution not checked")
	}
	if out, err := gitCmd(root, "rev-parse", "--git-dir"); err != nil {
		t.Skipf("not a git checkout (%s); SHA resolution not checked", out)
	}
	for sha, files := range shas {
		if out, err := gitCmd(root, "cat-file", "-t", sha); err != nil || out != "commit" {
			t.Errorf("generator_commit %s does not resolve to a commit (%q, %v); cited by %d files", sha, out, err, len(files))
			continue
		}
		if out, err := gitCmd(root, "merge-base", "--is-ancestor", baselineCommit, sha); err != nil {
			t.Errorf("generator_commit %s does not descend from baseline %s: %s", sha, baselineCommit, out)
		}
		if out, err := gitCmd(root, append([]string{"diff", "--stat", sha}, etlSourcePathspec...)...); err != nil || out != "" {
			t.Errorf("ETL sources changed since generator_commit %s; regenerate goldens:\n%s", sha, out)
		}
	}
}
