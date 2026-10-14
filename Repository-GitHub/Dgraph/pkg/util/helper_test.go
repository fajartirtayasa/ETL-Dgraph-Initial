// Package util
package util

import "testing"

func TestPathExist(t *testing.T) {
	t.Parallel()
	testCase := map[string]bool{
		"../util":     true,
		"jakarta-123": false,
	}

	for k, v := range testCase {
		if r := PathExist(k); r == v {
			t.Logf("this path %s exist expected %v, got %v", k, v, r)
		} else {
			t.Fatalf("this path %s exist expected %v, got %v", k, v, r)
		}

	}
}
