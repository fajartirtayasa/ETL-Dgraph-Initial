// Package util
package util

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestReplacer(t *testing.T) {
	t.Parallel()
	testCase := map[string]string{
		"testCase#1": "testCase_1",
		"testCase@1": "testCase-1",
	}

	rule := map[string]string{
		"#": "_",
		"@": "-",
	}

	for k, v := range testCase {
		assert.Equal(t, v, Replacer(rule,k))
	}
}
