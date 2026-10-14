// Package utils

package util

import (
	"fmt"
	"strconv"
	"strings"
)

// ToString converts a value to string.
func ToString(value any) string {
	switch value := value.(type) {
	case string:
		return value
	case int:
		return strconv.FormatInt(int64(value), 10)
	case int8:
		return strconv.FormatInt(int64(value), 10)
	case int16:
		return strconv.FormatInt(int64(value), 10)
	case int32:
		return strconv.FormatInt(int64(value), 10)
	case int64:
		return strconv.FormatInt(int64(value), 10)
	case uint:
		return strconv.FormatUint(uint64(value), 10)
	case uint8:
		return strconv.FormatUint(uint64(value), 10)
	case uint16:
		return strconv.FormatUint(uint64(value), 10)
	case uint32:
		return strconv.FormatUint(uint64(value), 10)
	case uint64:
		return strconv.FormatUint(uint64(value), 10)
	case float32:
		return strconv.FormatFloat(float64(value), 'g', -1, 64)
	case float64:
		return strconv.FormatFloat(float64(value), 'g', -1, 64)
	case bool:
		return strconv.FormatBool(value)
	default:
		return fmt.Sprintf("%+v", value)
	}
}

func SubString(input string, start int, length int) string {
	sr := []rune(input)

	if start >= len(sr) {
		return ""
	}

	if start+length > len(sr) {
		length = len(sr) - start
	}

	return string(sr[start : start+length])
}

// StringJoin helper
func StringJoin(elems []string, sep, lastSep string) string {
	switch len(elems) {
	case 0:
		return ""
	case 1:
		return fmt.Sprintf("%s%s", elems[0], lastSep)
	}
	n := len(sep) * (len(elems) - 1)
	for i := 0; i < len(elems); i++ {
		n += len(elems[i])
	}

	var b strings.Builder
	b.Grow(n)
	b.WriteString(elems[0])
	for _, s := range elems[1:] {
		b.WriteString(sep)
		b.WriteString(s)
	}

	if lastSep != "" {
		b.WriteString(lastSep)
	}

	return b.String()
}

// Split String to Array
func SplitString(s, delimiter string, trimWhitespace, removeEmpty bool) []string {
	// If the input string is empty, return an empty slice immediately.
	if s == "" {
		return []string{}
	}

	// Split the string into a slice using the provided delimiter.
	split := strings.Split(s, delimiter)

	// Use a new slice to store the results. This is efficient because we
	// pre-allocate a capacity, avoiding multiple re-allocations if we filter.
	result := make([]string, 0, len(split))

	for _, item := range split {
		// Optionally trim whitespace from the item.
		if trimWhitespace {
			item = strings.TrimSpace(item)
		}

		// Optionally skip adding the item if it's empty.
		if removeEmpty && item == "" {
			continue
		}

		result = append(result, item)
	}

	return result
}

func ToSnakeCase(str string) string {
	var result []rune
	for i, r := range str {
		if i > 0 && r >= 'A' && r <= 'Z' {
			result = append(result, '_')
		}
		result = append(result, r)
	}
	return strings.ToLower(string(result))
}
