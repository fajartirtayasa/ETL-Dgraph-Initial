package main

import (
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
)

// TestGeneratedFilesUpToDate fails when the committed fixture files differ
// from what the generator produces, so the fixture can only change through
// the generator.
func TestGeneratedFilesUpToDate(t *testing.T) {
	files, err := render()
	if err != nil {
		t.Fatal(err)
	}
	for name, want := range files {
		got, err := os.ReadFile(filepath.Join("..", filepath.FromSlash(name)))
		if err != nil {
			t.Fatalf("%s: %v (run `go run ./parity/fixture/gen` from the repository root)", name, err)
		}
		if strings.ReplaceAll(string(got), "\r\n", "\n") != string(want) {
			t.Errorf("%s is stale: run `go run ./parity/fixture/gen` from the repository root", name)
		}
	}
}

// TestSeedIsSynthetic checks every identifier in the seed carries the SYN-
// marker. The schema's CHECK constraints enforce the same at load time.
func TestSeedIsSynthetic(t *testing.T) {
	files, err := render()
	if err != nil {
		t.Fatal(err)
	}
	seed := string(files["sql/02_seed.sql"])
	tuple := regexp.MustCompile(`(?m)^    \((?:\d+, )?'([^']*)'`)
	matches := tuple.FindAllStringSubmatch(seed, -1)
	if len(matches) == 0 {
		t.Fatal("no rows found in seed")
	}
	for _, m := range matches {
		if !strings.HasPrefix(m[1], "SYN-") {
			t.Errorf("row identifier %q lacks the SYN- synthetic marker", m[1])
		}
	}
}
