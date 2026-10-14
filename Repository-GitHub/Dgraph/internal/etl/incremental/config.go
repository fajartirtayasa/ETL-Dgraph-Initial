package incremental

// Config groups all adjustable knobs for the incremental migration flow.
type Config struct {
	Removal        RemovalConfig
	Events         EventsConfig
	Links          LinksConfig
	FetchChunkSize int
}

// RemovalConfig controls lookup and deletion behaviour when purging
// invalidated events from Dgraph.
type RemovalConfig struct {
	LookupBatchSize   int
	LookupConcurrency int
	DeleteBatchSize   int
	DeleteConcurrency int
}

// EventsConfig controls lookup and upsert behaviour for event nodes.
type EventsConfig struct {
	LookupBatchSize   int
	LookupConcurrency int
	UpsertBatchSize   int
	UpsertConcurrency int
}

// LinksConfig controls lookup, upsert and cleanup behaviour for event links.
type LinksConfig struct {
	LookupBatchSize    int
	LookupConcurrency  int
	UpsertBatchSize    int
	UpsertConcurrency  int
	CleanupBatchSize   int
	CleanupConcurrency int
}

// ApplyDefaults ensures sane positive values for all tunables.
func (c *Config) ApplyDefaults() {
	c.Removal.LookupBatchSize = ensurePositive(c.Removal.LookupBatchSize, 100)
	c.Removal.LookupConcurrency = ensurePositive(c.Removal.LookupConcurrency, 4)
	c.Removal.DeleteBatchSize = ensurePositive(c.Removal.DeleteBatchSize, 100)
	c.Removal.DeleteConcurrency = ensurePositive(c.Removal.DeleteConcurrency, 4)

	c.Events.LookupBatchSize = ensurePositive(c.Events.LookupBatchSize, 100)
	c.Events.LookupConcurrency = ensurePositive(c.Events.LookupConcurrency, 4)
	c.Events.UpsertBatchSize = ensurePositive(c.Events.UpsertBatchSize, 50)
	c.Events.UpsertConcurrency = ensurePositive(c.Events.UpsertConcurrency, 4)

	c.Links.LookupBatchSize = ensurePositive(c.Links.LookupBatchSize, 100)
	c.Links.LookupConcurrency = ensurePositive(c.Links.LookupConcurrency, 4)
	c.Links.UpsertBatchSize = ensurePositive(c.Links.UpsertBatchSize, 50)
	c.Links.UpsertConcurrency = ensurePositive(c.Links.UpsertConcurrency, 4)
	c.Links.CleanupBatchSize = ensurePositive(c.Links.CleanupBatchSize, 100)
	c.Links.CleanupConcurrency = ensurePositive(c.Links.CleanupConcurrency, 4)

	c.FetchChunkSize = ensurePositive(c.FetchChunkSize, 10000)
}

func ensurePositive(value, fallback int) int {
	if value <= 0 {
		return fallback
	}
	return value
}
