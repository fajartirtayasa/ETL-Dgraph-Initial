package etl

import (
	"errors"
	"fmt"
	"strings"
)

type Mode string

const (
	ModeInitial Mode = "initial"
)

// ParseMode accepts only "initial" (whitespace around it and letter case are ignored). Anything else, including an
// empty value, is an error that says what to set; the unsupported-value message quotes the raw input.
func ParseMode(value string) (Mode, error) {
	switch strings.ToLower(strings.TrimSpace(value)) {
	case string(ModeInitial):
		return ModeInitial, nil
	case "":
		return "", errors.New("APP_MODE is empty; set APP_MODE=initial")
	case "incremental":
		return "", errors.New("APP_MODE=incremental is no longer supported here: the incremental ETL lives in ETL-Dgraph-Incremental")
	default:
		return "", fmt.Errorf("unsupported APP_MODE %q: only \"initial\" is supported", value)
	}
}
