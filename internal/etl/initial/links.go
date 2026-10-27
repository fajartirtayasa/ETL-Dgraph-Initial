package initial

import (
	"context"
	"database/sql"
	"fmt"
	"path/filepath"
	"strconv"
	"strings"
	"sync"

	"hara/traceability-dgraph-etl/internal/etl/common"
	"hara/traceability-dgraph-etl/pkg/database"
)

const linksQuery = `
SELECT
	l.id,
	l.event1_id,
	l.event2_id,
	l.event1_version,
	l.event2_version,
	l.role,
	l.qty,
	l.unit,
	l.source` + exportedLinksFilter + `
`

type linkRow struct {
	ID            sql.NullString
	Event1ID      sql.NullString
	Event2ID      sql.NullString
	Event1Version sql.NullString
	Event2Version sql.NullString
	Role          sql.NullString
	Qty           sql.NullFloat64
	Unit          sql.NullString
	Source        sql.NullString
}

func streamLinks(ctx context.Context, db database.Adapter, interner *common.StringInterner, outputDir string, chunkSize int) (err error) {
	writer, err := common.NewChunkedGzipRDFWriter(filepath.Join(outputDir, "links.rdf.gz"), chunkSize)
	if err != nil {
		return fmt.Errorf("create links writer: %w", err)
	}
	defer func() {
		closeErr := writer.Close()
		if err == nil {
			err = closeErr
		}
	}()

	rows, err := db.QueryRows(ctx, linksQuery)
	if err != nil {
		return fmt.Errorf("query links: %w", err)
	}
	defer rows.Close()

	norm := newNormalizer(interner)
	builderPool := sync.Pool{New: func() any { return &strings.Builder{} }}

	for rows.Next() {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		record := &linkRow{}
		if err := rows.Scan(
			&record.ID,
			&record.Event1ID,
			&record.Event2ID,
			&record.Event1Version,
			&record.Event2Version,
			&record.Role,
			&record.Qty,
			&record.Unit,
			&record.Source,
		); err != nil {
			return fmt.Errorf("scan link row: %w", err)
		}

		event1 := norm.String(record.Event1ID)
		event2 := norm.String(record.Event2ID)
		if event1 == "" || event2 == "" {
			continue
		}

		facets := buildLinkFacets(norm, record)

		builder := builderPool.Get().(*strings.Builder)
		builder.Reset()

		subject := common.BlankNode("event", event1)
		object := common.BlankNode("event", event2)

		builder.WriteString(subject)
		builder.WriteString(" <links> ")
		builder.WriteString(object)
		if facets != "" {
			builder.WriteByte(' ')
			builder.WriteByte('(')
			builder.WriteString(facets)
			builder.WriteByte(')')
		}
		builder.WriteString(" .\n")

		if writeErr := writer.Write(ctx, builder.String()); writeErr != nil {
			builder.Reset()
			builderPool.Put(builder)
			return writeErr
		}

		builder.Reset()
		builderPool.Put(builder)
	}

	if err := rows.Err(); err != nil {
		return fmt.Errorf("iterate link rows: %w", err)
	}

	return err
}

func buildLinkFacets(norm *normalizer, row *linkRow) string {
	facets := make([]string, 0, 6)

	if v := norm.String(row.Event1Version); v != "" {
		facets = append(facets, fmt.Sprintf("event1_version=%s", common.StringLiteral(v)))
	}
	if v := norm.String(row.Event2Version); v != "" {
		facets = append(facets, fmt.Sprintf("event2_version=%s", common.StringLiteral(v)))
	}
	if v := norm.String(row.Role); v != "" {
		facets = append(facets, fmt.Sprintf("role=%s", common.StringLiteral(v)))
	}
	if qty, ok := norm.Float64(row.Qty); ok {
		facets = append(facets, fmt.Sprintf("qty=%s", strconv.FormatFloat(qty, 'f', -1, 64)))
	}
	if v := norm.InternedString(row.Unit); v != "" {
		facets = append(facets, fmt.Sprintf("unit=%s", common.StringLiteral(v)))
	}
	if v := norm.String(row.Source); v != "" {
		facets = append(facets, fmt.Sprintf("source=%s", common.StringLiteral(v)))
	}

	return strings.Join(facets, ", ")
}
