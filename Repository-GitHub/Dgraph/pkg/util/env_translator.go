// Package util
package util

import (
	"strings"
)

var envArr = map[string]string{
	"production":  "prod",
	"staging":     "stg",
	"development": "dev",
	"prod":        "prod",
	"stg":         "stg",
	"dev":         "dev",
}

// EnvironmentTransform transformer
func EnvironmentTransform(s string) string {
	v, ok := envArr[strings.ToLower(strings.Trim(s, " "))]

	if !ok {
		return ""
	}

	return v
}
