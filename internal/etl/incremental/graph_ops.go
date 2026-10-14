package incremental

import (
	"context"
	"fmt"
	"strings"
	"sync"
)

func (m *Migrator) lookupEventUIDs(ctx context.Context, ids []string, batchSize, concurrency int) (map[string]string, error) {
	uniq := stringSet{}
	for _, id := range ids {
		uniq.Add(id)
	}
	uniqueIDs := uniq.Values()
	if len(uniqueIDs) == 0 {
		return map[string]string{}, nil
	}

	batches := make([][]string, 0, len(uniqueIDs))
	for _, idx := range chunkIndices(len(uniqueIDs), batchSize) {
		segment := uniqueIDs[idx[0]:idx[1]]
		if len(segment) > 0 {
			batches = append(batches, segment)
		}
	}

	result := make(map[string]string, len(uniqueIDs))
	var mu sync.Mutex

	worker := func(ctx context.Context, batch []string) error {
		query := buildEventLookupQuery(batch)
		out, err := runLookup(ctx, m.graph, query)
		if err != nil {
			return fmt.Errorf("lookup events: %w", err)
		}
		if len(out) == 0 {
			return nil
		}
		mu.Lock()
		for k, v := range out {
			result[k] = v
		}
		mu.Unlock()
		return nil
	}

	if err := fanOutBatches(ctx, batches, concurrency, worker); err != nil {
		return nil, err
	}
	return result, nil
}

func (m *Migrator) deleteNodes(ctx context.Context, uids []string, batchSize, concurrency int) error {
	if len(uids) == 0 {
		return nil
	}
	batches := make([][]string, 0, len(uids))
	for _, idx := range chunkIndices(len(uids), batchSize) {
		segment := uids[idx[0]:idx[1]]
		if len(segment) > 0 {
			batches = append(batches, segment)
		}
	}

	return fanOutBatches(ctx, batches, concurrency, func(ctx context.Context, batch []string) error {
		lines := make([]string, 0, len(batch))
		for _, uid := range batch {
			if uid == "" {
				continue
			}
			lines = append(lines, fmt.Sprintf("<%s> * * .", uid))
		}
		if len(lines) == 0 {
			return nil
		}
		rdf := strings.Join(lines, "\n")
		err := m.graph.DeleteRDF(ctx, rdf)
		if err != nil {
			return fmt.Errorf("delete nodes: %w", err)
		}
		return nil
	})
}

func (m *Migrator) mutateSet(ctx context.Context, payload string) (map[string]string, error) {
	mapped, err := m.graph.MutateRDF(ctx, payload)
	if err != nil {
		return nil, fmt.Errorf("apply mutation: %w", err)
	}
	return mapped, nil
}

func (m *Migrator) mutateSetAndDel(ctx context.Context, setPayload string, delPayload string) (map[string]string, error) {
	mapped, err := m.graph.MutateWithDeletesRDF(ctx, setPayload, delPayload)
	if err != nil {
		return nil, fmt.Errorf("apply mutation and deletes: %w", err)
	}
	return mapped, nil
}

func blankName(value string) string {
	return strings.TrimPrefix(value, "_:")
}

func buildCleanupRDF(uids []string) string {
	lines := make([]string, 0, len(uids))
	for _, uid := range uids {
		if uid == "" {
			continue
		}
		lines = append(lines, fmt.Sprintf("<%s> <links> * .", uid))
	}
	return strings.Join(lines, "\n")
}
