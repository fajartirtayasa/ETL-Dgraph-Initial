package common

import (
	"crypto/sha1"
	"encoding/hex"
	"strings"
)

const blankNodePrefix = "_:"

// BlankNode builds a deterministic blank node identifier using the provided
// prefix and value. Purely alphanumeric values are used as-is. Any other value
// is sanitised to alphanumeric characters and suffixed with a SHA-1 hash of
// the original value, so distinct values such as "A-1" and "A_1" never collapse
// into the same blank node.
func BlankNode(prefix, value string) string {
	if prefix == "" {
		prefix = "node"
	}

	if value == "" {
		return blankNodePrefix + prefix + "_unknown"
	}

	sanitized := sanitize(value)
	if sanitized != value {
		// Pure alphanumeric values never contain '_', so hashed identifiers
		// cannot collide with them.
		hash := sha1.Sum([]byte(value))
		suffix := hex.EncodeToString(hash[:])
		if sanitized == "" {
			sanitized = suffix
		} else {
			sanitized = sanitized + "_" + suffix
		}
	}

	return blankNodePrefix + prefix + "_" + sanitized
}

func sanitize(value string) string {
	if value == "" {
		return ""
	}

	builder := strings.Builder{}
	builder.Grow(len(value))

	for _, r := range value {
		switch {
		case r >= 'a' && r <= 'z':
			builder.WriteRune(r)
		case r >= 'A' && r <= 'Z':
			builder.WriteRune(r)
		case r >= '0' && r <= '9':
			builder.WriteRune(r)
		default:
			builder.WriteByte('_')
		}
	}

	// Collapse multiple underscores to a single underscore to avoid creating
	// excessively long identifiers consisting only of underscores.
	sanitized := strings.ReplaceAll(builder.String(), "__", "_")
	sanitized = strings.Trim(sanitized, "_")
	return sanitized
}
