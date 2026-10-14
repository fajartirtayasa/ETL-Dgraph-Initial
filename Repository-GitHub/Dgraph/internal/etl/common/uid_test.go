package common

import (
	"strings"
	"testing"
)

func TestBlankNodeAlphanumericUnchanged(t *testing.T) {
	if got := BlankNode("event", "ABC123"); got != "_:event_ABC123" {
		t.Fatalf("got %q, want %q", got, "_:event_ABC123")
	}
}

func TestBlankNodeDistinctValuesDoNotCollide(t *testing.T) {
	values := []string{"A-1", "A_1", "A.1", "A 1", "A--1", "A1", "-A1", "--", "!!"}
	seen := make(map[string]string, len(values))
	for _, v := range values {
		node := BlankNode("event", v)
		if prev, ok := seen[node]; ok {
			t.Fatalf("collision: %q and %q both map to %q", prev, v, node)
		}
		seen[node] = v
	}
}

func TestBlankNodeDeterministic(t *testing.T) {
	if BlankNode("event", "SO-001/2024") != BlankNode("event", "SO-001/2024") {
		t.Fatal("expected deterministic blank node")
	}
}

func TestBlankNodeOnlySafeCharacters(t *testing.T) {
	node := strings.TrimPrefix(BlankNode("event", "a/b:c d#e"), "_:")
	for _, r := range node {
		if !(r >= 'a' && r <= 'z' || r >= 'A' && r <= 'Z' || r >= '0' && r <= '9' || r == '_') {
			t.Fatalf("unexpected character %q in %q", r, node)
		}
	}
}

func TestBlankNodeEmpty(t *testing.T) {
	if got := BlankNode("", ""); got != "_:node_unknown" {
		t.Fatalf("got %q", got)
	}
}
