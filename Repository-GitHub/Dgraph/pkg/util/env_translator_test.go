// Package util
package util

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestEnvironmentTransform(t *testing.T) {
	t.Parallel()
	testCase := map[string]string{
		"production":  "prod",
		"staging":     "stg",
		"development": "dev",
		"prod":        "prod",
		"stg":         "stg",
		"dev":         "dev",
		"devs":        "",
	}

	for k, v := range testCase {
		assert.Equal(t, v, EnvironmentTransform(k))
	}
}
