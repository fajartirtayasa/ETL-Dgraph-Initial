package incremental

import (
	"database/sql"
	"strings"

	"hara/traceability-dgraph-etl/internal/etl/common"
)

type normalizer struct {
	interner *common.StringInterner
}

func newNormalizer(interner *common.StringInterner) *normalizer {
	return &normalizer{interner: interner}
}

func (n *normalizer) String(value sql.NullString) string {
	if !value.Valid {
		return ""
	}
	return n.RawString(value.String)
}

func (n *normalizer) RawString(value string) string {
	trimmed := strings.TrimSpace(value)
	if trimmed == "" {
		return ""
	}
	return n.interner.Intern(trimmed)
}

func (n *normalizer) Int64(value sql.NullInt64) (int64, bool) {
	if !value.Valid {
		return 0, false
	}
	return value.Int64, true
}

func (n *normalizer) Float64(value sql.NullFloat64) (float64, bool) {
	if !value.Valid {
		return 0, false
	}
	return value.Float64, true
}
