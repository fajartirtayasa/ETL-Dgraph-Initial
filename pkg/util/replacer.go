// Package util
package util

import "strings"

// Replacer string replacer helper
func Replacer(r map[string]string, msg string) string {
	for k, v := range r {
		msg = strings.ReplaceAll(msg, k, v)
	}
	return msg
}
