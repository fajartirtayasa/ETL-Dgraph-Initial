package initial

import (
	"context"
	"errors"
	"fmt"
	"log/slog"

	"hara/traceability-dgraph-etl/pkg/database"
)

// trimSpaceChars holds every character strings.TrimSpace removes (unicode.IsSpace). The export skips an event whose
// unique_id, and a link whose event1_id or event2_id, is empty after TrimSpace; the stamping statements pass this set
// to btrim as $1 so they skip exactly the same rows.
const trimSpaceChars = "\t\n\v\f\r \u0085  " +
	"           " +
	"    　"

// stampEventsStatement stamps the events_bc rows eventsQuery exported: the members of UnsyncedUniqueIDs (its LEFT
// JOINs never drop a row) whose unique_id is not blank. Only valid rows that are not stamped yet are changed.
const stampEventsStatement = exportedEventIDsCTE + `
UPDATE events_bc e SET sync_time = NOW()
WHERE e.unique_id IN (SELECT unique_id FROM UnsyncedUniqueIDs)
  AND btrim(e.unique_id, $1) <> ''
  AND e.status = 'valid'
  AND e.sync_time IS NULL;
`

// stampLinksStatement stamps, by links_bc.id, the rows linksQuery exported: exportedLinksFilter less the links with a
// blank endpoint. Only valid rows that are not stamped yet are changed.
const stampLinksStatement = `
UPDATE links_bc SET sync_time = NOW()
WHERE id IN (
    SELECT l.id` + exportedLinksFilter + `
      AND btrim(l.event1_id, $1) <> ''
      AND btrim(l.event2_id, $1) <> ''
)
  AND status = 'valid'
  AND sync_time IS NULL;
`

// stampSyncTime sets sync_time = NOW() on the rows the export wrote, with one UPDATE per table, events_bc first. Each
// statement commits on its own, so a failed one leaves its own table as it was. links_bc is attempted even when
// events_bc fails, and both errors are returned. The statements re-select the exported rows, which is exact only while
// events_bc and links_bc are unchanged since the export started.
func stampSyncTime(ctx context.Context, db database.Adapter, log *slog.Logger) error {
	var errs error
	for _, stamp := range []struct{ table, statement string }{
		{"events_bc", stampEventsStatement},
		{"links_bc", stampLinksStatement},
	} {
		result, err := db.Exec(ctx, stamp.statement, trimSpaceChars)
		if err != nil {
			errs = errors.Join(errs, fmt.Errorf("stamp %s sync_time: %w", stamp.table, err))
			continue
		}

		stamped, err := result.RowsAffected()
		if err != nil {
			log.Warn("sync_time stamped, row count unavailable", slog.String("table", stamp.table), slog.Any("error", err))
			continue
		}
		log.Info("sync_time stamped", slog.String("table", stamp.table), slog.Int64("rows", stamped))
	}
	return errs
}
