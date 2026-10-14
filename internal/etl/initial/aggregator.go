package initial

import (
	"context"
	"path/filepath"
	"strings"
	"sync"

	"hara/traceability-dgraph-etl/internal/etl/common"
)

type aggregator struct {
	norm        *normalizer
	eventWriter *common.ChunkedGzipRDFWriter
	builderPool sync.Pool
}

func newAggregator(outputDir string, interner *common.StringInterner, chunkSize int) (*aggregator, error) {
	eventWriter, err := common.NewChunkedGzipRDFWriter(filepath.Join(outputDir, "events.rdf.gz"), chunkSize)
	if err != nil {
		return nil, err
	}

	agg := &aggregator{
		norm:        newNormalizer(interner),
		eventWriter: eventWriter,
		builderPool: sync.Pool{
			New: func() any { return &strings.Builder{} },
		},
	}

	return agg, nil
}

func (a *aggregator) Close() error {
	if a.eventWriter == nil {
		return nil
	}
	return a.eventWriter.Close()
}

func (a *aggregator) Process(ctx context.Context, rows <-chan *EventRow) error {
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case row, ok := <-rows:
			if !ok {
				return nil
			}
			if row == nil {
				continue
			}
			if err := a.handleRow(ctx, row); err != nil {
				return err
			}
		}
	}
}

func (a *aggregator) handleRow(ctx context.Context, row *EventRow) error {
	eventID := a.norm.String(row.UniqueID)
	if eventID == "" {
		return nil
	}

	return a.emitEvent(ctx, row, eventID)
}

func (a *aggregator) emitEvent(ctx context.Context, row *EventRow, eventID string) error {
	builder := a.getBuilder()
	defer a.releaseBuilder(builder)

	subject := common.BlankNode("event", eventID)

	common.AppendTriple(builder, subject, "<dgraph.type>", common.StringLiteral("Event"))
	common.AppendTriple(builder, subject, "<unique_id>", common.StringLiteral(eventID))

	if v := a.norm.String(row.ClientID); v != "" {
		common.AppendTriple(builder, subject, "<client_id>", common.StringLiteral(v))
	}
	if v := a.norm.String(row.BLNumber); v != "" {
		common.AppendTriple(builder, subject, "<bl_number>", common.StringLiteral(v))
	}
	if v := a.norm.String(row.SalesOrder); v != "" {
		common.AppendTriple(builder, subject, "<sales_order>", common.StringLiteral(v))
	}
	if v := a.norm.InternedString(row.ProductID); v != "" {
		common.AppendTriple(builder, subject, "<product_id>", common.StringLiteral(v))
	}
	if v := a.norm.InternedString(row.ProductName); v != "" {
		common.AppendTriple(builder, subject, "<product_name>", common.StringLiteral(v))
	}
	if v := a.norm.InternedString(row.Plant); v != "" {
		common.AppendTriple(builder, subject, "<plant>", common.StringLiteral(v))
	}
	if v := a.norm.InternedString(row.ProductionLabel); v != "" {
		common.AppendTriple(builder, subject, "<production_label>", common.StringLiteral(v))
	}
	if processDate, ok := a.norm.Int64(row.ProcessDate); ok {
		common.AppendTriple(builder, subject, "<process_date>", common.IntLiteral(processDate))
	}
	if v := a.norm.InternedString(row.MaterialType); v != "" {
		common.AppendTriple(builder, subject, "<material_type>", common.StringLiteral(v))
	}
	if v := a.norm.String(row.MaterialCode); v != "" {
		common.AppendTriple(builder, subject, "<material_code>", common.StringLiteral(v))
	}
	if amount, ok := a.norm.Float64(row.Amount); ok {
		common.AppendTriple(builder, subject, "<amount>", common.FloatLiteral(amount))
	}
	if v := a.norm.InternedString(row.Unit); v != "" {
		common.AppendTriple(builder, subject, "<unit>", common.StringLiteral(v))
	}
	if v := a.norm.String(row.SrcPath); v != "" && v != "{}" {
		common.AppendTriple(builder, subject, "<src_path>", common.StringLiteral(v))
	}
	if v := a.norm.InternedString(row.SubDataSource); v != "" {
		common.AppendTriple(builder, subject, "<sub_data_source>", common.StringLiteral(v))
	}
	if v := a.norm.String(row.Version); v != "" {
		common.AppendTriple(builder, subject, "<version>", common.StringLiteral(v))
	}
	if v := a.norm.InternedString(row.Flag); v != "" {
		common.AppendTriple(builder, subject, "<flag>", common.StringLiteral(v))
	}
	if v := a.norm.String(row.MaterialDocument); v != "" {
		common.AppendTriple(builder, subject, "<material_document>", common.StringLiteral(v))
	}
	if v := a.norm.String(row.PurchaseOrder); v != "" {
		common.AppendTriple(builder, subject, "<purchase_order>", common.StringLiteral(v))
	}
	if v := a.norm.String(row.UMLID); v != "" {
		common.AppendTriple(builder, subject, "<uml_id>", common.StringLiteral(v))
	}
	if v := a.norm.String(row.MillID); v != "" {
		common.AppendTriple(builder, subject, "<mill_id>", common.StringLiteral(v))
	}
	if v := a.norm.String(row.LocationID); v != "" {
		common.AppendTriple(builder, subject, "<location_id>", common.StringLiteral(v))
	}
	if v := a.norm.InternedString(row.Vendor); v != "" {
		common.AppendTriple(builder, subject, "<vendor>", common.StringLiteral(v))
	}
	if v := a.norm.String(row.Source); v != "" {
		common.AppendTriple(builder, subject, "<source>", common.StringLiteral(v))
	}
	if v := a.norm.InternedString(row.LocationSubjectType); v != "" {
		common.AppendTriple(builder, subject, "<location_subject_type>", common.StringLiteral(v))
	}
	if v := a.norm.String(row.LocationMasterID); v != "" {
		common.AppendTriple(builder, subject, "<location_master_id>", common.StringLiteral(v))
	}
	if v := a.norm.InternedString(row.LocationMasterType); v != "" {
		common.AppendTriple(builder, subject, "<location_master_type>", common.StringLiteral(v))
	}
	if v := a.norm.String(row.LocationFacilitySubjectID); v != "" {
		common.AppendTriple(builder, subject, "<location_facility_subject_id>", common.StringLiteral(v))
	}
	if v := a.norm.InternedString(row.LocationFacilitySubjectType); v != "" {
		common.AppendTriple(builder, subject, "<location_facility_subject_type>", common.StringLiteral(v))
	}
	if v := a.norm.String(row.LocationFacilityMasterID); v != "" {
		common.AppendTriple(builder, subject, "<location_facility_master_id>", common.StringLiteral(v))
	}
	if v := a.norm.InternedString(row.LocationFacilityMasterType); v != "" {
		common.AppendTriple(builder, subject, "<location_facility_master_type>", common.StringLiteral(v))
	}
	if v := a.norm.InternedString(row.SourceSubjectType); v != "" {
		common.AppendTriple(builder, subject, "<source_subject_type>", common.StringLiteral(v))
	}
	if v := a.norm.String(row.SourceMasterID); v != "" {
		common.AppendTriple(builder, subject, "<source_master_id>", common.StringLiteral(v))
	}
	if v := a.norm.InternedString(row.SourceMasterType); v != "" {
		common.AppendTriple(builder, subject, "<source_master_type>", common.StringLiteral(v))
	}
	if v := a.norm.String(row.SourceFacilitySubjectID); v != "" {
		common.AppendTriple(builder, subject, "<source_facility_subject_id>", common.StringLiteral(v))
	}
	if v := a.norm.InternedString(row.SourceFacilitySubjectType); v != "" {
		common.AppendTriple(builder, subject, "<source_facility_subject_type>", common.StringLiteral(v))
	}
	if v := a.norm.String(row.SourceFacilityMasterID); v != "" {
		common.AppendTriple(builder, subject, "<source_facility_master_id>", common.StringLiteral(v))
	}
	if v := a.norm.InternedString(row.SourceFacilityMasterType); v != "" {
		common.AppendTriple(builder, subject, "<source_facility_master_type>", common.StringLiteral(v))
	}
	if v := a.norm.String(row.MovementType); v != "" {
		common.AppendTriple(builder, subject, "<movement_type>", common.StringLiteral(v))
	}
	if v := a.norm.String(row.MovementTypeText); v != "" {
		common.AppendTriple(builder, subject, "<movement_type_text>", common.StringLiteral(v))
	}
	if v := a.norm.String(row.StorageLocation); v != "" {
		common.AppendTriple(builder, subject, "<storage_location>", common.StringLiteral(v))
	}
	if v := a.norm.InternedString(row.PartnerPCA); v != "" {
		common.AppendTriple(builder, subject, "<partner_pca>", common.StringLiteral(v))
	}
	if v := a.norm.InternedString(row.MillName); v != "" {
		common.AppendTriple(builder, subject, "<mill_name>", common.StringLiteral(v))
	}

	return a.eventWriter.Write(ctx, builder.String())
}

func (a *aggregator) getBuilder() *strings.Builder {
	builder := a.builderPool.Get().(*strings.Builder)
	builder.Reset()
	return builder
}

func (a *aggregator) releaseBuilder(builder *strings.Builder) {
	builder.Reset()
	a.builderPool.Put(builder)
}
