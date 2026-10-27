package etl

import (
	"fmt"
	"strconv"
	"strings"
)

// ParseStampSyncTime reads INITIAL_STAMP_SYNC_TIME. An empty or whitespace-only value means the default, true;
// otherwise the value (whitespace around it and letter case are ignored) must be one strconv.ParseBool accepts.
// Anything else is an error that quotes the raw input, so a value such as "no" never leaves stamping silently on.
func ParseStampSyncTime(value string) (bool, error) {
	trimmed := strings.TrimSpace(value)
	if trimmed == "" {
		return true, nil
	}

	enabled, err := strconv.ParseBool(strings.ToLower(trimmed))
	if err != nil {
		return false, fmt.Errorf("unsupported INITIAL_STAMP_SYNC_TIME %q: use true or false (empty means true)", value)
	}
	return enabled, nil
}
