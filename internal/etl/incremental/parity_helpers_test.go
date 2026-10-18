package incremental

import (
	"context"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"reflect"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"hara/traceability-dgraph-etl/internal/etl/common"
	"hara/traceability-dgraph-etl/pkg/database"
	"hara/traceability-dgraph-etl/pkg/dgraph"
)

func newParityMigrator(graph dgraph.Adapter, db database.Adapter, cfg Config) *Migrator {
	cfg.ApplyDefaults()
	return &Migrator{
		db:    db,
		graph: graph,
		cfg:   cfg,
		log:   slog.New(slog.NewTextHandler(io.Discard, nil)),
		norm:  newNormalizer(common.NewStringInterner()),
	}
}

func testContext(t *testing.T) context.Context {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	t.Cleanup(cancel)
	return ctx
}

// singleBatchConfig makes every chunk and batch hold the whole input and runs
// one worker, so call order is deterministic. Goldens are generated with it.
func singleBatchConfig() Config {
	const all = 10000
	return Config{
		FetchChunkSize: all,
		Removal:        RemovalConfig{LookupBatchSize: all, LookupConcurrency: 1, DeleteBatchSize: all, DeleteConcurrency: 1},
		Events:         EventsConfig{LookupBatchSize: all, LookupConcurrency: 1, UpsertBatchSize: all, UpsertConcurrency: 1},
		Links: LinksConfig{LookupBatchSize: all, LookupConcurrency: 1, UpsertBatchSize: all, UpsertConcurrency: 1,
			CleanupBatchSize: all, CleanupConcurrency: 1},
	}
}

// tinyBatchConfig splits everything as finely as possible and runs workers
// concurrently. End state must not depend on it.
func tinyBatchConfig() Config {
	return Config{
		FetchChunkSize: 1,
		Removal:        RemovalConfig{LookupBatchSize: 1, LookupConcurrency: 4, DeleteBatchSize: 1, DeleteConcurrency: 4},
		Events:         EventsConfig{LookupBatchSize: 1, LookupConcurrency: 4, UpsertBatchSize: 1, UpsertConcurrency: 4},
		Links: LinksConfig{LookupBatchSize: 1, LookupConcurrency: 4, UpsertBatchSize: 1, UpsertConcurrency: 4,
			CleanupBatchSize: 1, CleanupConcurrency: 4},
	}
}

// eventFieldPredicates maps every optional eventRecord field to the predicate
// upsertEvents writes for it (events.go:428-638). UniqueID is excluded: it is
// always set together with dgraph.type (events.go:425-426).
var eventFieldPredicates = []struct{ Field, Predicate string }{
	{"ClientID", "client_id"}, {"BLNumber", "bl_number"}, {"SalesOrder", "sales_order"},
	{"ProductID", "product_id"}, {"ProductName", "product_name"}, {"Plant", "plant"},
	{"ProductionLabel", "production_label"}, {"MaterialType", "material_type"},
	{"MaterialCode", "material_code"}, {"Unit", "unit"}, {"SrcPath", "src_path"},
	{"SubDataSource", "sub_data_source"}, {"Version", "version"}, {"Flag", "flag"},
	{"MaterialDocument", "material_document"}, {"PurchaseOrder", "purchase_order"},
	{"UMLID", "uml_id"}, {"MillID", "mill_id"}, {"LocationID", "location_id"},
	{"Vendor", "vendor"}, {"Source", "source"},
	{"LocationSubjectType", "location_subject_type"}, {"LocationMasterID", "location_master_id"},
	{"LocationMasterType", "location_master_type"},
	{"LocationFacilitySubjectID", "location_facility_subject_id"},
	{"LocationFacilitySubjectType", "location_facility_subject_type"},
	{"LocationFacilityMasterID", "location_facility_master_id"},
	{"LocationFacilityMasterType", "location_facility_master_type"},
	{"SourceSubjectType", "source_subject_type"}, {"SourceMasterID", "source_master_id"},
	{"SourceMasterType", "source_master_type"},
	{"SourceFacilitySubjectID", "source_facility_subject_id"},
	{"SourceFacilitySubjectType", "source_facility_subject_type"},
	{"SourceFacilityMasterID", "source_facility_master_id"},
	{"SourceFacilityMasterType", "source_facility_master_type"},
	{"MovementType", "movement_type"}, {"MovementTypeText", "movement_type_text"},
	{"StorageLocation", "storage_location"}, {"PartnerPCA", "partner_pca"}, {"MillName", "mill_name"},
	{"ProcessDate", "process_date"}, {"Amount", "amount"},
}

func int64p(v int64) *int64       { return &v }
func float64p(v float64) *float64 { return &v }

// fullEventRecord returns a record with every optional field populated.
func fullEventRecord(uniqueID string) eventRecord {
	r := eventRecord{UniqueID: uniqueID}
	v := reflect.ValueOf(&r).Elem()
	for _, fp := range eventFieldPredicates {
		f := v.FieldByName(fp.Field)
		switch f.Kind() {
		case reflect.String:
			f.SetString("SYN-" + fp.Predicate)
		}
	}
	r.ProcessDate = int64p(1700000000)
	r.Amount = float64p(10.5)
	return r
}

// clearField empties one optional field the way normalisation would.
func clearField(r *eventRecord, field string) {
	f := reflect.ValueOf(r).Elem().FieldByName(field)
	if f.Kind() == reflect.String {
		f.SetString("")
	} else {
		f.Set(reflect.Zero(f.Type()))
	}
}

// eventRecordJSON renders a record with predicate-named keys for goldens.
func eventRecordJSON(r eventRecord) map[string]any {
	out := map[string]any{"unique_id": r.UniqueID}
	v := reflect.ValueOf(r)
	for _, fp := range eventFieldPredicates {
		f := v.FieldByName(fp.Field)
		switch {
		case f.Kind() == reflect.String:
			out[fp.Predicate] = f.String()
		case f.IsNil():
			out[fp.Predicate] = nil
		case fp.Field == "ProcessDate":
			out[fp.Predicate] = strconv.FormatInt(*r.ProcessDate, 10)
		default:
			out[fp.Predicate] = strconv.FormatFloat(*r.Amount, 'f', -1, 64)
		}
	}
	return out
}

func linkRecordJSON(r linkRecord) map[string]any {
	var qty any
	if r.Qty != nil {
		qty = strconv.FormatFloat(*r.Qty, 'f', -1, 64)
	}
	return map[string]any{
		"id": r.ID, "event1_id": r.Event1ID, "event2_id": r.Event2ID,
		"event1_version": r.Event1Version, "event2_version": r.Event2Version,
		"role": r.Role, "qty": qty, "unit": r.Unit, "source": r.Source,
	}
}

func configJSON(c Config) map[string]any {
	c.ApplyDefaults()
	return map[string]any{
		"fetch_chunk_size": c.FetchChunkSize,
		"removal": map[string]int{"lookup_batch_size": c.Removal.LookupBatchSize, "lookup_concurrency": c.Removal.LookupConcurrency,
			"delete_batch_size": c.Removal.DeleteBatchSize, "delete_concurrency": c.Removal.DeleteConcurrency},
		"events": map[string]int{"lookup_batch_size": c.Events.LookupBatchSize, "lookup_concurrency": c.Events.LookupConcurrency,
			"upsert_batch_size": c.Events.UpsertBatchSize, "upsert_concurrency": c.Events.UpsertConcurrency},
		"links": map[string]int{"lookup_batch_size": c.Links.LookupBatchSize, "lookup_concurrency": c.Links.LookupConcurrency,
			"upsert_batch_size": c.Links.UpsertBatchSize, "upsert_concurrency": c.Links.UpsertConcurrency,
			"cleanup_batch_size": c.Links.CleanupBatchSize, "cleanup_concurrency": c.Links.CleanupConcurrency},
	}
}

// schemaEventPredicates parses the Event type's predicate list from schema.dql.
func schemaEventPredicates(t *testing.T) []string {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join(repoRoot(t), "schema.dql"))
	if err != nil {
		t.Fatal(err)
	}
	m := regexp.MustCompile(`(?s)type Event \{(.*?)\}`).FindStringSubmatch(string(raw))
	if m == nil {
		t.Fatal("type Event not found in schema.dql")
	}
	preds := strings.Fields(m[1])
	sort.Strings(preds)
	return preds
}

func sortedKeys[V any](m map[string]V) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}
