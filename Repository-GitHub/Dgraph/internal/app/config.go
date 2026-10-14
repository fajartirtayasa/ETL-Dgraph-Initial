package app

import (
	"fmt"
	"hara/traceability-dgraph-etl/internal/app/consts"
	"hara/traceability-dgraph-etl/pkg/file"
	"hara/traceability-dgraph-etl/pkg/util"
)

func NewConfig() (*Config, error) {
	var config *Config
	pathFile := fmt.Sprintf("%s/%s", consts.ConfigPath, consts.ConfigFile)

	if err := file.ReadFileFromYaml(pathFile, &config); err != nil {
		return nil, fmt.Errorf("failed to read config file: %w", err)
	}

	if config == nil {
		return nil, fmt.Errorf("config is nil")
	}

	overrideWithEnv(config)

	return config, nil
}

// overrideWithEnv updates the config struct with values from environment variables.
func overrideWithEnv(cfg *Config) {
	// AppConfig overrides
	cfg.App.Debug = util.GetEnvAsBool("APP_DEBUG", cfg.App.Debug)
	cfg.App.Mode = util.GetEnv("APP_MODE", cfg.App.Mode)

	// LoggerConfig overrides
	cfg.Logger.Level = util.GetEnv("LOGGER_LEVEL", cfg.Logger.Level)

	// DB Clean (PostgreSql Config) overrides
	cfg.DBClean.Host = util.GetEnv("DB_CLEAN_HOST", cfg.DBClean.Host)
	cfg.DBClean.Port = util.GetEnvAsInt("DB_CLEAN_PORT", cfg.DBClean.Port)
	cfg.DBClean.Name = util.GetEnv("DB_CLEAN_NAME", cfg.DBClean.Name)
	cfg.DBClean.User = util.GetEnv("DB_CLEAN_USER", cfg.DBClean.User)
	cfg.DBClean.Password = util.GetEnv("DB_CLEAN_PASSWORD", cfg.DBClean.Password)
	cfg.DBClean.Schema = util.GetEnv("DB_CLEAN_SCHEMA", cfg.DBClean.Schema)
	cfg.DBClean.MaxConnectionOpen = util.GetEnvAsInt("DB_CLEAN_MAX_CONNECTION_OPEN", cfg.DBClean.MaxConnectionOpen)
	cfg.DBClean.MaxConnectionIdle = util.GetEnvAsInt("DB_CLEAN_MAX_CONNECTION_IDLE", cfg.DBClean.MaxConnectionIdle)
	cfg.DBClean.MaxLifeTimeMS = util.GetEnvAsInt("DB_CLEAN_MAX_LIFE_TIME_MS", cfg.DBClean.MaxLifeTimeMS)
	cfg.DBClean.TimeoutSecond = util.GetEnvAsInt("DB_CLEAN_TIMEOUT_SECOND", cfg.DBClean.TimeoutSecond)
	cfg.DBClean.Charset = util.GetEnv("DB_CLEAN_CHARSET", cfg.DBClean.Charset)

	// DB Clean Replica (PostgreSql Config) overrides
	cfg.DBCleanReplica.Host = util.GetEnv("DB_CLEAN_REPLICA_HOST", cfg.DBCleanReplica.Host)
	cfg.DBCleanReplica.Port = util.GetEnvAsInt("DB_CLEAN_REPLICA_PORT", cfg.DBCleanReplica.Port)
	cfg.DBCleanReplica.Name = util.GetEnv("DB_CLEAN_REPLICA_NAME", cfg.DBCleanReplica.Name)
	cfg.DBCleanReplica.User = util.GetEnv("DB_CLEAN_REPLICA_USER", cfg.DBCleanReplica.User)
	cfg.DBCleanReplica.Password = util.GetEnv("DB_CLEAN_REPLICA_PASSWORD", cfg.DBCleanReplica.Password)
	cfg.DBCleanReplica.Schema = util.GetEnv("DB_CLEAN_REPLICA_SCHEMA", cfg.DBCleanReplica.Schema)
	cfg.DBCleanReplica.MaxConnectionOpen = util.GetEnvAsInt("DB_CLEAN_REPLICA_MAX_CONNECTION_OPEN", cfg.DBCleanReplica.MaxConnectionOpen)
	cfg.DBCleanReplica.MaxConnectionIdle = util.GetEnvAsInt("DB_CLEAN_REPLICA_MAX_CONNECTION_IDLE", cfg.DBCleanReplica.MaxConnectionIdle)
	cfg.DBCleanReplica.MaxLifeTimeMS = util.GetEnvAsInt("DB_CLEAN_REPLICA_MAX_LIFE_TIME_MS", cfg.DBCleanReplica.MaxLifeTimeMS)
	cfg.DBCleanReplica.TimeoutSecond = util.GetEnvAsInt("DB_CLEAN_REPLICA_TIMEOUT_SECOND", cfg.DBCleanReplica.TimeoutSecond)
	cfg.DBCleanReplica.Charset = util.GetEnv("DB_CLEAN_REPLICA_CHARSET", cfg.DBCleanReplica.Charset)

	// Dgraph Config overrides
	cfg.Dgraph.Alphas = util.GetEnvAsStringSlice("DGRAPH_ALPHAS", cfg.Dgraph.Alphas)
	cfg.Dgraph.Username = util.GetEnv("DGRAPH_USERNAME", cfg.Dgraph.Username)
	cfg.Dgraph.Password = util.GetEnv("DGRAPH_PASSWORD", cfg.Dgraph.Password)
	cfg.Dgraph.SSLMode = util.GetEnv("DGRAPH_SSL_MODE", cfg.Dgraph.SSLMode)
	cfg.Dgraph.BearerToken = util.GetEnv("DGRAPH_BEARER_TOKEN", cfg.Dgraph.BearerToken)
	cfg.Dgraph.TimeoutSecond = util.GetEnvAsInt("DGRAPH_TIMEOUT_SECOND", cfg.Dgraph.TimeoutSecond)
	cfg.Dgraph.TransactionTimeoutSecond = util.GetEnvAsInt("DGRAPH_TRANSACTION_TIMEOUT_SECOND", cfg.Dgraph.TransactionTimeoutSecond)
	cfg.Dgraph.MaxRetries = util.GetEnvAsInt("DGRAPH_MAX_RETRIES", cfg.Dgraph.MaxRetries)

	// Incremental overrides
	if cfg.Incremental == nil {
		cfg.Incremental = &IncrementalConfig{}
	}

	// Initial overrides
	if cfg.Initial == nil {
		cfg.Initial = &InitialConfig{}
	}
	cfg.Initial.WorkerCount = util.GetEnvAsInt("INITIAL_WORKER_COUNT", cfg.Initial.WorkerCount)
	cfg.Initial.ChunkSize = util.GetEnvAsInt("INITIAL_CHUNK_SIZE", cfg.Initial.ChunkSize)

	if cfg.Incremental.Removal == nil {
		cfg.Incremental.Removal = &IncrementalRemovalConfig{}
	}
	if cfg.Incremental.Events == nil {
		cfg.Incremental.Events = &IncrementalEventsConfig{}
	}
	if cfg.Incremental.Links == nil {
		cfg.Incremental.Links = &IncrementalLinksConfig{}
	}

	cfg.Incremental.Removal.LookupBatchSize = util.GetEnvAsInt("INCREMENTAL_REMOVAL_LOOKUP_BATCH_SIZE", cfg.Incremental.Removal.LookupBatchSize)
	cfg.Incremental.Removal.LookupConcurrency = util.GetEnvAsInt("INCREMENTAL_REMOVAL_LOOKUP_CONCURRENCY", cfg.Incremental.Removal.LookupConcurrency)
	cfg.Incremental.Removal.DeleteBatchSize = util.GetEnvAsInt("INCREMENTAL_REMOVAL_DELETE_BATCH_SIZE", cfg.Incremental.Removal.DeleteBatchSize)
	cfg.Incremental.Removal.DeleteConcurrency = util.GetEnvAsInt("INCREMENTAL_REMOVAL_DELETE_CONCURRENCY", cfg.Incremental.Removal.DeleteConcurrency)

	cfg.Incremental.Events.LookupBatchSize = util.GetEnvAsInt("INCREMENTAL_EVENTS_LOOKUP_BATCH_SIZE", cfg.Incremental.Events.LookupBatchSize)
	cfg.Incremental.Events.LookupConcurrency = util.GetEnvAsInt("INCREMENTAL_EVENTS_LOOKUP_CONCURRENCY", cfg.Incremental.Events.LookupConcurrency)
	cfg.Incremental.Events.UpsertBatchSize = util.GetEnvAsInt("INCREMENTAL_EVENTS_UPSERT_BATCH_SIZE", cfg.Incremental.Events.UpsertBatchSize)
	cfg.Incremental.Events.UpsertConcurrency = util.GetEnvAsInt("INCREMENTAL_EVENTS_UPSERT_CONCURRENCY", cfg.Incremental.Events.UpsertConcurrency)

	cfg.Incremental.Links.LookupBatchSize = util.GetEnvAsInt("INCREMENTAL_LINKS_LOOKUP_BATCH_SIZE", cfg.Incremental.Links.LookupBatchSize)
	cfg.Incremental.Links.LookupConcurrency = util.GetEnvAsInt("INCREMENTAL_LINKS_LOOKUP_CONCURRENCY", cfg.Incremental.Links.LookupConcurrency)
	cfg.Incremental.Links.UpsertBatchSize = util.GetEnvAsInt("INCREMENTAL_LINKS_UPSERT_BATCH_SIZE", cfg.Incremental.Links.UpsertBatchSize)
	cfg.Incremental.Links.UpsertConcurrency = util.GetEnvAsInt("INCREMENTAL_LINKS_UPSERT_CONCURRENCY", cfg.Incremental.Links.UpsertConcurrency)
	cfg.Incremental.Links.CleanupBatchSize = util.GetEnvAsInt("INCREMENTAL_LINKS_CLEANUP_BATCH_SIZE", cfg.Incremental.Links.CleanupBatchSize)
	cfg.Incremental.Links.CleanupConcurrency = util.GetEnvAsInt("INCREMENTAL_LINKS_CLEANUP_CONCURRENCY", cfg.Incremental.Links.CleanupConcurrency)
	cfg.Incremental.FetchChunkSize = util.GetEnvAsInt("INCREMENTAL_FETCH_CHUNK_SIZE", cfg.Incremental.FetchChunkSize)

	// AWS S3 overrides
	cfg.AWS.S3.Region = util.GetEnv("AWS_S3_REGION", cfg.AWS.S3.Region)
	cfg.AWS.S3.Bucket = util.GetEnv("AWS_S3_BUCKET", cfg.AWS.S3.Bucket)
	cfg.AWS.S3.AccessKeyID = util.GetEnv("AWS_ACCESS_KEY_ID", cfg.AWS.S3.AccessKeyID)
	cfg.AWS.S3.SecretAccessKey = util.GetEnv("AWS_SECRET_ACCESS_KEY", cfg.AWS.S3.SecretAccessKey)
	cfg.AWS.S3.SessionToken = util.GetEnv("AWS_SESSION_TOKEN", cfg.AWS.S3.SessionToken)
	cfg.AWS.S3.Endpoint = util.GetEnv("AWS_S3_ENDPOINT", cfg.AWS.S3.Endpoint)
	cfg.AWS.S3.ForcePathStyle = util.GetEnvAsBool("AWS_S3_FORCE_PATH_STYLE", cfg.AWS.S3.ForcePathStyle)
}

type Config struct {
	App            *AppConfig         `yaml:"app" json:"app"`
	Logger         *LoggerConfig      `yaml:"logger" json:"logger"`
	DBClean        *PostgresConfig    `yaml:"db_clean" json:"db_clean"`
	DBCleanReplica *PostgresConfig    `yaml:"db_clean_replica" json:"db_clean_replica"`
	Dgraph         *DgraphConfig      `yaml:"dgraph" json:"dgraph"`
	Initial        *InitialConfig     `yaml:"initial" json:"initial"`
	Incremental    *IncrementalConfig `yaml:"incremental" json:"incremental"`
	AWS            *AWSConfig         `yaml:"aws" json:"aws"`
}

type AppConfig struct {
	Debug bool   `yaml:"debug" json:"debug"`
	Mode  string `yaml:"mode" json:"mode"`
}

type LoggerConfig struct {
	Level string `yaml:"level" json:"level"`
}

type PostgresConfig struct {
	Host              string `yaml:"host" json:"host"`
	Port              int    `yaml:"port" json:"port"`
	Name              string `yaml:"name" json:"name"`
	User              string `yaml:"user" json:"user"`
	Password          string `yaml:"password" json:"password"`
	Schema            string `yaml:"schema" json:"schema"`
	MaxConnectionOpen int    `yaml:"max_connection_open" json:"max_connection_open"`
	MaxConnectionIdle int    `yaml:"max_connection_idle" json:"max_connection_idle"`
	MaxLifeTimeMS     int    `yaml:"max_life_time_ms" json:"max_life_time_ms"`
	TimeoutSecond     int    `yaml:"timeout_second" json:"timeout_second"`
	Charset           string `yaml:"charset" json:"charset"`
}

type DgraphConfig struct {
	Alphas                   []string `yaml:"alphas" json:"alphas"`
	Username                 string   `yaml:"username" json:"username"`
	Password                 string   `yaml:"password" json:"password"`
	SSLMode                  string   `yaml:"ssl_mode" json:"ssl_mode"`
	BearerToken              string   `yaml:"bearer_token" json:"bearer_token"`
	TimeoutSecond            int      `yaml:"timeout_second" json:"timeout_second"`
	TransactionTimeoutSecond int      `yaml:"transaction_timeout_second" json:"transaction_timeout_second"`
	MaxRetries               int      `yaml:"max_retries" json:"max_retries"`
}

type InitialConfig struct {
	WorkerCount int `yaml:"worker_count" json:"worker_count"`
	ChunkSize   int `yaml:"chunk_size" json:"chunk_size"`
}

type IncrementalConfig struct {
	Removal        *IncrementalRemovalConfig `yaml:"removal" json:"removal"`
	Events         *IncrementalEventsConfig  `yaml:"events" json:"events"`
	Links          *IncrementalLinksConfig   `yaml:"links" json:"links"`
	FetchChunkSize int                       `yaml:"fetch_chunk_size" json:"fetch_chunk_size"`
}

type IncrementalRemovalConfig struct {
	LookupBatchSize   int `yaml:"lookup_batch_size" json:"lookup_batch_size"`
	LookupConcurrency int `yaml:"lookup_concurrency" json:"lookup_concurrency"`
	DeleteBatchSize   int `yaml:"delete_batch_size" json:"delete_batch_size"`
	DeleteConcurrency int `yaml:"delete_concurrency" json:"delete_concurrency"`
}

type IncrementalEventsConfig struct {
	LookupBatchSize   int `yaml:"lookup_batch_size" json:"lookup_batch_size"`
	LookupConcurrency int `yaml:"lookup_concurrency" json:"lookup_concurrency"`
	UpsertBatchSize   int `yaml:"upsert_batch_size" json:"upsert_batch_size"`
	UpsertConcurrency int `yaml:"upsert_concurrency" json:"upsert_concurrency"`
}

type IncrementalLinksConfig struct {
	LookupBatchSize    int `yaml:"lookup_batch_size" json:"lookup_batch_size"`
	LookupConcurrency  int `yaml:"lookup_concurrency" json:"lookup_concurrency"`
	UpsertBatchSize    int `yaml:"upsert_batch_size" json:"upsert_batch_size"`
	UpsertConcurrency  int `yaml:"upsert_concurrency" json:"upsert_concurrency"`
	CleanupBatchSize   int `yaml:"cleanup_batch_size" json:"cleanup_batch_size"`
	CleanupConcurrency int `yaml:"cleanup_concurrency" json:"cleanup_concurrency"`
}

type AWSConfig struct {
	S3 *S3Config `yaml:"s3" json:"s3"`
}

type S3Config struct {
	Region          string `yaml:"region" json:"region"`
	Bucket          string `yaml:"bucket" json:"bucket"`
	AccessKeyID     string `yaml:"access_key_id" json:"access_key_id"`
	SecretAccessKey string `yaml:"secret_access_key" json:"secret_access_key"`
	SessionToken    string `yaml:"session_token" json:"session_token"`
	Endpoint        string `yaml:"endpoint" json:"endpoint"`
	ForcePathStyle  bool   `yaml:"force_path_style" json:"force_path_style"`
}
