package incremental

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"sync"

	"golang.org/x/sync/errgroup"

	"hara/traceability-dgraph-etl/pkg/dgraph"
)

type stringSet map[string]struct{}

func (s stringSet) Add(value string) {
	if value == "" {
		return
	}
	s[value] = struct{}{}
}

func (s stringSet) Values() []string {
	out := make([]string, 0, len(s))
	for value := range s {
		out = append(out, value)
	}
	return out
}

func chunkIndices(length, size int) [][2]int {
	if size <= 0 {
		size = length
	}
	chunks := make([][2]int, 0, (length+size-1)/size)
	for start := 0; start < length; start += size {
		end := start + size
		if end > length {
			end = length
		}
		chunks = append(chunks, [2]int{start, end})
	}
	return chunks
}

func buildEventLookupQuery(ids []string) string {
	quoted := make([]string, 0, len(ids))
	for _, id := range ids {
		quoted = append(quoted, strconv.Quote(id))
	}
	values := strings.Join(quoted, ",\n    ")
	return fmt.Sprintf(`query {
  nodes(func: type(Event))
  @filter(eq(unique_id,
    %s
  )) {
    uid
    unique_id
  }
}`, values)
}

func runLookup(ctx context.Context, graph dgraph.Adapter, query string) (map[string]string, error) {
	resp, err := graph.RunDQL(ctx, query, graph.WithBestEffort(), graph.WithReadOnly())
	if err != nil {
		return nil, err
	}
	if resp == nil || resp.JSON == "" {
		return map[string]string{}, nil
	}
	return parseLookupJSON(resp.JSON), nil
}

type lookupResponse struct {
	Nodes []struct {
		UID      string `json:"uid"`
		UniqueID string `json:"unique_id"`
	} `json:"nodes"`
}

func parseLookupJSON(jsonStr string) map[string]string {
	var data lookupResponse
	if err := json.Unmarshal([]byte(jsonStr), &data); err != nil {
		return map[string]string{}
	}

	result := make(map[string]string, len(data.Nodes))
	for _, node := range data.Nodes {
		if node.UID != "" && node.UniqueID != "" {
			result[node.UniqueID] = node.UID
		}
	}
	return result
}

func fanOutBatches[T any](ctx context.Context, batches [][]T, concurrency int, worker func(context.Context, []T) error) error {
	g, gctx := errgroup.WithContext(ctx)
	jobs := make(chan []T)

	g.Go(func() error {
		defer close(jobs)
		for _, batch := range batches {
			select {
			case <-gctx.Done():
				return gctx.Err()
			case jobs <- batch:
			}
		}
		return nil
	})

	if concurrency <= 0 {
		concurrency = 1
	}
	for i := 0; i < concurrency; i++ {
		g.Go(func() error {
			for batch := range jobs {
				if len(batch) == 0 {
					continue
				}
				if err := worker(gctx, batch); err != nil {
					return err
				}
			}
			return nil
		})
	}

	return g.Wait()
}

type uidMap struct {
	sync.RWMutex
	inner map[string]string
}

func newUIDMap(initial map[string]string) *uidMap {
	m := &uidMap{inner: make(map[string]string)}
	for k, v := range initial {
		if k != "" && v != "" {
			m.inner[k] = v
		}
	}
	return m
}

func (m *uidMap) Get(id string) (string, bool) {
	m.RLock()
	defer m.RUnlock()
	uid, ok := m.inner[id]
	return uid, ok
}

func (m *uidMap) Set(id, uid string) {
	if id == "" || uid == "" {
		return
	}
	m.Lock()
	m.inner[id] = uid
	m.Unlock()
}

func (m *uidMap) Merge(other map[string]string) {
	m.Lock()
	for id, uid := range other {
		if id != "" && uid != "" {
			m.inner[id] = uid
		}
	}
	m.Unlock()
}

func (m *uidMap) Values() map[string]string {
	m.RLock()
	defer m.RUnlock()
	copy := make(map[string]string, len(m.inner))
	for k, v := range m.inner {
		copy[k] = v
	}
	return copy
}
