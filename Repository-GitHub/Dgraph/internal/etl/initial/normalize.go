package initial

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

func (n *normalizer) InternedString(v sql.NullString) string {
	if !v.Valid {
		return ""
	}
	trimmed := strings.TrimSpace(v.String)
	if trimmed == "" {
		return ""
	}
	return n.interner.Intern(trimmed)
}

func (n *normalizer) String(v sql.NullString) string {
	if !v.Valid {
		return ""
	}
	return n.RawString(v.String)
}

func (n *normalizer) RawString(value string) string {
	return strings.TrimSpace(value)
}

func (n *normalizer) Int64(v sql.NullInt64) (int64, bool) {
	if !v.Valid {
		return 0, false
	}
	return v.Int64, true
}

func (n *normalizer) Float64(v sql.NullFloat64) (float64, bool) {
	if !v.Valid {
		return 0, false
	}
	return v.Float64, true
}
