package dgraph

import (
	"context"
	"fmt"
	"math/rand"
	"strings"
	"time"

	"github.com/dgraph-io/dgo/v250"
	api_v2 "github.com/dgraph-io/dgo/v250/protos/api"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/encoding/gzip"
	"google.golang.org/grpc/status"
)

type dgraphClient struct {
	client *dgo.Dgraph
	config *Config
}

// NewDgraphConnection creates a new Dgraph connection using RoundRobinClient for multiple alphas
func NewDgraphConnection(config *Config) (Adapter, error) {
	if len(config.Alphas) == 0 {
		return nil, fmt.Errorf("at least one alpha endpoint is required")
	}

	// Build connection options
	var opts []dgo.ClientOption

	// Add SSL/TLS configuration
	switch SSLMode(config.SSLMode) {
	case SSLModeDisable:
		opts = append(opts, dgo.WithGrpcOption(grpc.WithTransportCredentials(insecure.NewCredentials())))
	case SSLModeRequire, SSLModeVerifyCA:
		// For production, you would configure proper TLS credentials here
		// This is a placeholder - you should implement proper TLS configuration
		opts = append(opts, dgo.WithGrpcOption(grpc.WithTransportCredentials(insecure.NewCredentials())))
	default:
		opts = append(opts, dgo.WithGrpcOption(grpc.WithTransportCredentials(insecure.NewCredentials())))
	}

	opts = append(opts, dgo.WithGrpcOption(grpc.WithDefaultCallOptions(grpc.UseCompressor(gzip.Name))))

	// Create client based on number of alphas
	var client *dgo.Dgraph
	var err error

	if len(config.Alphas) == 1 {
		// Single alpha connection
		client, err = dgo.NewClient(config.Alphas[0], opts...)
	} else {
		// Multiple alphas with round-robin
		client, err = dgo.NewRoundRobinClient(config.Alphas, opts...)
	}

	if err != nil {
		return nil, fmt.Errorf("failed to create dgraph client: %w", err)
	}

	return &dgraphClient{
		client: client,
		config: config,
	}, nil
}

// Close closes the Dgraph connection
func (d *dgraphClient) Close() {
	d.client.Close()
}

// Ping checks if the Dgraph cluster is reachable
func (d *dgraphClient) Ping(ctx context.Context) error {
	// Simple query to test connectivity
	_, err := d.client.RunDQL(ctx, `{ _dummy(func: has(_dummy)) { uid } }`)
	if err != nil {
		return fmt.Errorf("ping failed: %w", err)
	}
	return nil
}

// HealthCheck provides a simple, context-free way to check Dgraph health
func (d *dgraphClient) HealthCheck() error {
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(d.config.TimeoutSecond)*time.Second)
	defer cancel()
	return d.Ping(ctx)
}

// SetSchema sets the Dgraph schema
func (d *dgraphClient) SetSchema(ctx context.Context, schema string) error {
	err := d.client.SetSchema(ctx, schema)
	if err != nil {
		return fmt.Errorf("failed to set schema: %w", err)
	}
	return nil
}

// GetSchema retrieves the current Dgraph schema
func (d *dgraphClient) GetSchema(ctx context.Context) (string, error) {
	// This would need to be implemented based on the specific Dgraph version
	// For now, we'll return an empty string as a placeholder
	return "", fmt.Errorf("get schema not implemented yet")
}

// RunDQL executes a DQL query/mutation
func (d *dgraphClient) RunDQL(ctx context.Context, dql string, options ...DQLOption) (*DQLResponse, error) {
	// Apply options
	opts := &DQLOptions{}
	for _, option := range options {
		option.Apply(opts)
	}

	// Build dgo options
	var dgoOpts []dgo.TxnOption

	if opts.BestEffort {
		dgoOpts = append(dgoOpts, dgo.WithBestEffort())
	}

	if opts.ReadOnly {
		dgoOpts = append(dgoOpts, dgo.WithReadOnly())
	}

	if opts.ResponseFormat == ResponseFormatRDF {
		dgoOpts = append(dgoOpts, dgo.WithResponseFormat(api_v2.Request_RDF))
	}

	maxRetries := d.config.MaxRetries
	if maxRetries <= 0 {
		maxRetries = 3 // Default to 3 retries for queries
	}

	var lastErr error
	for attempt := 0; attempt <= maxRetries; attempt++ {
		if attempt > 0 {
			backoff := time.Duration(1<<uint(attempt-1)) * 100 * time.Millisecond
			if backoff > 2*time.Second {
				backoff = 2 * time.Second
			}
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-time.After(backoff):
			}
		}

		resp, err := d.client.RunDQL(ctx, dql, dgoOpts...)
		if err != nil {
			lastErr = err
			if isRetryableError(err) && attempt < maxRetries {
				continue
			}
			return nil, fmt.Errorf("failed to run DQL: %w", err)
		}

		out := &DQLResponse{Uids: resp.GetUids()}
		if opts.ResponseFormat == ResponseFormatRDF {
			out.RDF = string(resp.GetRdf())
		} else {
			out.JSON = string(resp.GetJson())
		}

		return out, nil
	}

	return nil, fmt.Errorf("failed to run DQL: max retries (%d) exceeded: %w", maxRetries, lastErr)
}

// RunDQLWithVars executes a DQL query/mutation with variables
func (d *dgraphClient) RunDQLWithVars(ctx context.Context, dql string, vars map[string]string, options ...DQLOption) (*DQLResponse, error) {
	// Apply options
	opts := &DQLOptions{}
	for _, option := range options {
		option.Apply(opts)
	}

	// Build dgo options
	var dgoOpts []dgo.TxnOption

	if opts.BestEffort {
		dgoOpts = append(dgoOpts, dgo.WithBestEffort())
	}

	if opts.ReadOnly {
		dgoOpts = append(dgoOpts, dgo.WithReadOnly())
	}

	if opts.ResponseFormat == ResponseFormatRDF {
		dgoOpts = append(dgoOpts, dgo.WithResponseFormat(api_v2.Request_RDF))
	}

	maxRetries := d.config.MaxRetries
	if maxRetries <= 0 {
		maxRetries = 3 // Default to 3 retries for queries
	}

	var lastErr error
	for attempt := 0; attempt <= maxRetries; attempt++ {
		if attempt > 0 {
			backoff := time.Duration(1<<uint(attempt-1)) * 100 * time.Millisecond
			if backoff > 2*time.Second {
				backoff = 2 * time.Second
			}
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-time.After(backoff):
			}
		}

		resp, err := d.client.RunDQLWithVars(ctx, dql, vars, dgoOpts...)
		if err != nil {
			lastErr = err
			if isRetryableError(err) && attempt < maxRetries {
				continue
			}
			return nil, fmt.Errorf("failed to run DQL with vars: %w", err)
		}

		out := &DQLResponse{Uids: resp.GetUids()}
		if opts.ResponseFormat == ResponseFormatRDF {
			out.RDF = string(resp.GetRdf())
		} else {
			out.JSON = string(resp.GetJson())
		}

		return out, nil
	}

	return nil, fmt.Errorf("failed to run DQL with vars: max retries (%d) exceeded: %w", maxRetries, lastErr)
}

func (d *dgraphClient) MutateRDF(ctx context.Context, rdf string) (map[string]string, error) {
	maxRetries := d.config.MaxRetries
	if maxRetries <= 0 {
		maxRetries = 7 // Default to 7 retries if not configured (recommended for transaction conflicts)
	}

	// Set transaction timeout - configurable, defaults to 45 seconds
	// If TransactionTimeoutSecond is set, use it; otherwise default to 45s or TimeoutSecond if smaller
	txnTimeout := 45 * time.Second
	if d.config.TransactionTimeoutSecond > 0 {
		txnTimeout = time.Duration(d.config.TransactionTimeoutSecond) * time.Second
	} else if d.config.TimeoutSecond > 0 && time.Duration(d.config.TimeoutSecond)*time.Second < txnTimeout {
		txnTimeout = time.Duration(d.config.TimeoutSecond) * time.Second
	}

	var lastErr error
	for attempt := 0; attempt <= maxRetries; attempt++ {
		if attempt > 0 {
			// Exponential backoff with jitter: base * 2^(attempt-1) + random jitter
			// This helps spread out retrying transactions to reduce contention
			baseBackoff := time.Duration(1<<uint(attempt-1)) * 200 * time.Millisecond
			jitter := time.Duration(rand.Intn(100)) * time.Millisecond // 0-100ms random jitter
			backoff := baseBackoff + jitter

			// Cap maximum backoff at 5 seconds
			if backoff > 5*time.Second {
				backoff = 5 * time.Second
			}

			select {
			case <-ctx.Done():
				return nil, fmt.Errorf("context cancelled: %w", ctx.Err())
			case <-time.After(backoff):
				// Continue with retry
			}
		}

		// Create a timeout context for this transaction attempt
		// This prevents transactions from hanging and being aborted by Dgraph
		txnCtx, cancel := context.WithTimeout(ctx, txnTimeout)

		txn := d.client.NewTxn()
		mu := &api_v2.Mutation{
			SetNquads: []byte(rdf),
			CommitNow: true,
		}

		resp, err := txn.Mutate(txnCtx, mu)
		cancel() // Cancel context immediately after mutation attempt

		if err != nil {
			_ = txn.Discard(txnCtx)
			lastErr = err

			// Check if error is retryable (transaction abort or timeout)
			if isRetryableError(err) && attempt < maxRetries {
				continue // Retry
			}

			return nil, fmt.Errorf("mutate rdf: %w", err)
		}

		// Success
		return resp.GetUids(), nil
	}

	return nil, fmt.Errorf("mutate rdf: max retries (%d) exceeded: %w", maxRetries, lastErr)
}

func (d *dgraphClient) MutateWithDeletesRDF(ctx context.Context, setRdf string, delRdf string) (map[string]string, error) {
	maxRetries := d.config.MaxRetries
	if maxRetries <= 0 {
		maxRetries = 7 // Default to 7 retries if not configured (recommended for transaction conflicts)
	}

	// Set transaction timeout - configurable, defaults to 45 seconds
	// If TransactionTimeoutSecond is set, use it; otherwise default to 45s or TimeoutSecond if smaller
	txnTimeout := 45 * time.Second
	if d.config.TransactionTimeoutSecond > 0 {
		txnTimeout = time.Duration(d.config.TransactionTimeoutSecond) * time.Second
	} else if d.config.TimeoutSecond > 0 && time.Duration(d.config.TimeoutSecond)*time.Second < txnTimeout {
		txnTimeout = time.Duration(d.config.TimeoutSecond) * time.Second
	}

	var lastErr error
	for attempt := 0; attempt <= maxRetries; attempt++ {
		if attempt > 0 {
			// Exponential backoff with jitter: base * 2^(attempt-1) + random jitter
			// This helps spread out retrying transactions to reduce contention
			baseBackoff := time.Duration(1<<uint(attempt-1)) * 200 * time.Millisecond
			jitter := time.Duration(rand.Intn(100)) * time.Millisecond // 0-100ms random jitter
			backoff := baseBackoff + jitter

			// Cap maximum backoff at 5 seconds
			if backoff > 5*time.Second {
				backoff = 5 * time.Second
			}

			select {
			case <-ctx.Done():
				return nil, fmt.Errorf("context cancelled: %w", ctx.Err())
			case <-time.After(backoff):
				// Continue with retry
			}
		}

		// Create a timeout context for this transaction attempt
		// This prevents transactions from hanging and being aborted by Dgraph
		txnCtx, cancel := context.WithTimeout(ctx, txnTimeout)

		txn := d.client.NewTxn()
		mu := &api_v2.Mutation{
			SetNquads: []byte(setRdf),
			DelNquads: []byte(delRdf),
			CommitNow: true,
		}

		resp, err := txn.Mutate(txnCtx, mu)
		cancel() // Cancel context immediately after mutation attempt

		if err != nil {
			_ = txn.Discard(txnCtx)
			lastErr = err

			// Check if error is retryable (transaction abort or timeout)
			if isRetryableError(err) && attempt < maxRetries {
				continue // Retry
			}

			return nil, fmt.Errorf("mutate and delete rdf: %w", err)
		}

		// Success
		return resp.GetUids(), nil
	}

	return nil, fmt.Errorf("mutate and delete rdf: max retries (%d) exceeded: %w", maxRetries, lastErr)
}

func (d *dgraphClient) DeleteRDF(ctx context.Context, rdf string) error {
	maxRetries := d.config.MaxRetries
	if maxRetries <= 0 {
		maxRetries = 7 // Default to 7 retries if not configured (recommended for transaction conflicts)
	}

	// Set transaction timeout - configurable, defaults to 45 seconds
	// If TransactionTimeoutSecond is set, use it; otherwise default to 45s or TimeoutSecond if smaller
	txnTimeout := 45 * time.Second
	if d.config.TransactionTimeoutSecond > 0 {
		txnTimeout = time.Duration(d.config.TransactionTimeoutSecond) * time.Second
	} else if d.config.TimeoutSecond > 0 && time.Duration(d.config.TimeoutSecond)*time.Second < txnTimeout {
		txnTimeout = time.Duration(d.config.TimeoutSecond) * time.Second
	}

	var lastErr error
	for attempt := 0; attempt <= maxRetries; attempt++ {
		if attempt > 0 {
			// Exponential backoff with jitter: base * 2^(attempt-1) + random jitter
			// This helps spread out retrying transactions to reduce contention
			baseBackoff := time.Duration(1<<uint(attempt-1)) * 200 * time.Millisecond
			jitter := time.Duration(rand.Intn(100)) * time.Millisecond // 0-100ms random jitter
			backoff := baseBackoff + jitter

			// Cap maximum backoff at 5 seconds
			if backoff > 5*time.Second {
				backoff = 5 * time.Second
			}

			select {
			case <-ctx.Done():
				return fmt.Errorf("context cancelled: %w", ctx.Err())
			case <-time.After(backoff):
				// Continue with retry
			}
		}

		// Create a timeout context for this transaction attempt
		// This prevents transactions from hanging and being aborted by Dgraph
		txnCtx, cancel := context.WithTimeout(ctx, txnTimeout)

		txn := d.client.NewTxn()
		mu := &api_v2.Mutation{
			DelNquads: []byte(rdf),
			CommitNow: true,
		}

		_, err := txn.Mutate(txnCtx, mu)
		cancel() // Cancel context immediately after mutation attempt

		if err != nil {
			_ = txn.Discard(txnCtx)
			lastErr = err

			// Check if error is retryable (transaction abort or timeout)
			if isRetryableError(err) && attempt < maxRetries {
				continue // Retry
			}

			return fmt.Errorf("delete rdf: %w", err)
		}

		// Success
		return nil
	}

	return fmt.Errorf("delete rdf: max retries (%d) exceeded: %w", maxRetries, lastErr)
}

// CreateNamespace creates a new namespace
func (d *dgraphClient) CreateNamespace(ctx context.Context) (uint64, error) {
	namespaceID, err := d.client.CreateNamespace(ctx)
	if err != nil {
		return 0, fmt.Errorf("failed to create namespace: %w", err)
	}
	return namespaceID, nil
}

// DropNamespace drops a specific namespace
func (d *dgraphClient) DropNamespace(ctx context.Context, namespaceID uint64) error {
	err := d.client.DropNamespace(ctx, namespaceID)
	if err != nil {
		return fmt.Errorf("failed to drop namespace %d: %w", namespaceID, err)
	}
	return nil
}

// ListNamespaces lists all available namespaces
func (d *dgraphClient) ListNamespaces(ctx context.Context) ([]uint64, error) {
	nsMap, err := d.client.ListNamespaces(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to list namespaces: %w", err)
	}
	ids := make([]uint64, 0, len(nsMap))
	for id := range nsMap {
		ids = append(ids, id)
	}
	return ids, nil
}

// Option methods for backward compatibility
func (d *dgraphClient) WithBestEffort() DQLOption {
	return WithBestEffort()
}

func (d *dgraphClient) WithReadOnly() DQLOption {
	return WithReadOnly()
}

func (d *dgraphClient) WithResponseFormat(format ResponseFormat) DQLOption {
	return WithResponseFormat(format)
}

// isRetryableError checks if an error is a retryable transaction error.
// Dgraph transaction aborts can occur due to conflicts, timeouts, or concurrent modifications.
func isRetryableError(err error) bool {
	if err == nil {
		return false
	}

	// Check gRPC status codes first (more reliable)
	if st, ok := status.FromError(err); ok {
		code := st.Code()
		// Retry on these gRPC status codes:
		// - Aborted: transaction conflict/abort
		// - Unavailable: service temporarily unavailable
		// - DeadlineExceeded: timeout
		// - ResourceExhausted: rate limiting or resource constraints
		if code == codes.Aborted || code == codes.Unavailable ||
			code == codes.DeadlineExceeded || code == codes.ResourceExhausted {
			return true
		}
	}

	// Fallback to string matching for non-gRPC errors or additional patterns
	errStr := strings.ToLower(err.Error())
	retryablePatterns := []string{
		"transaction has been aborted",
		"please retry",
		"transaction aborted",
		"txn conflict",
		"conflict",
		"deadline exceeded",
		"context deadline exceeded",
		"context canceled",
		"unavailable",
		"connection refused",
		"transport is closing",
		"resource exhausted",
		"old transactions",
		"abort old transactions",
		"eof",
		"broken pipe",
		"i/o timeout",
		"timeout",
	}

	for _, pattern := range retryablePatterns {
		if strings.Contains(errStr, pattern) {
			return true
		}
	}

	return false
}
