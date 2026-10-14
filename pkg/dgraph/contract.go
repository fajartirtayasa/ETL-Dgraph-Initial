package dgraph

import (
	"context"
)

type Config struct {
	Alphas                  []string
	Username                string
	Password                string
	SSLMode                 string
	BearerToken             string
	TimeoutSecond           int // General connection timeout in seconds
	TransactionTimeoutSecond int // Transaction-specific timeout in seconds (0 = use default)
	MaxRetries              int
}

type SSLMode string

const (
	SSLModeDisable  SSLMode = "disable"
	SSLModeRequire  SSLMode = "require"
	SSLModeVerifyCA SSLMode = "verify-ca"
)

// DQLResponse represents the response from Dgraph DQL operations
// Matches fields available in github.com/dgraph-io/dgo/v250/protos/api Response
// JSON and RDF are mutually exclusive depending on the response format.
type DQLResponse struct {
	JSON string            `json:"json,omitempty"`
	RDF  string            `json:"rdf,omitempty"`
	Uids map[string]string `json:"uids,omitempty"`
}

// Adapter defines the interface for Dgraph operations
type Adapter interface {
	// Connection management
	Close()
	Ping(ctx context.Context) error
	HealthCheck() error

	// Schema operations
	SetSchema(ctx context.Context, schema string) error
	GetSchema(ctx context.Context) (string, error)

	// DQL operations
	RunDQL(ctx context.Context, dql string, options ...DQLOption) (*DQLResponse, error)
	RunDQLWithVars(ctx context.Context, dql string, vars map[string]string, options ...DQLOption) (*DQLResponse, error)
	MutateRDF(ctx context.Context, rdf string) (map[string]string, error)
	MutateWithDeletesRDF(ctx context.Context, setRdf string, delRdf string) (map[string]string, error)
	DeleteRDF(ctx context.Context, rdf string) error

	// Namespace operations
	CreateNamespace(ctx context.Context) (uint64, error)
	DropNamespace(ctx context.Context, namespaceID uint64) error
	ListNamespaces(ctx context.Context) ([]uint64, error)

	// Transaction options
	WithBestEffort() DQLOption
	WithReadOnly() DQLOption
	WithResponseFormat(format ResponseFormat) DQLOption
}

// DQLOption represents options for DQL operations
type DQLOption interface {
	Apply(*DQLOptions)
}

// DQLOptions holds configuration for DQL operations
type DQLOptions struct {
	BestEffort     bool
	ReadOnly       bool
	ResponseFormat ResponseFormat
}

// ResponseFormat represents the format of the response
type ResponseFormat int

const (
	ResponseFormatJSON ResponseFormat = iota
	ResponseFormatRDF
)

// Option implementations
type bestEffortOption struct{}
type readOnlyOption struct{}
type responseFormatOption struct {
	format ResponseFormat
}

func (o bestEffortOption) Apply(opts *DQLOptions) {
	opts.BestEffort = true
}

func (o readOnlyOption) Apply(opts *DQLOptions) {
	opts.ReadOnly = true
}

func (o responseFormatOption) Apply(opts *DQLOptions) {
	opts.ResponseFormat = o.format
}

// Option constructors
func WithBestEffort() DQLOption {
	return bestEffortOption{}
}

func WithReadOnly() DQLOption {
	return readOnlyOption{}
}

func WithResponseFormat(format ResponseFormat) DQLOption {
	return responseFormatOption{format: format}
}
