package etl

import "strings"

type Mode string

const (
	ModeInitial     Mode = "initial"
	ModeIncremental Mode = "incremental"
)

func ParseMode(value string) Mode {
	switch strings.ToLower(strings.TrimSpace(value)) {
	case string(ModeIncremental):
		return ModeIncremental
	default:
		return ModeInitial
	}
}
