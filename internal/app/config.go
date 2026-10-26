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

	// Initial overrides
	if cfg.Initial == nil {
		cfg.Initial = &InitialConfig{}
	}
	cfg.Initial.WorkerCount = util.GetEnvAsInt("INITIAL_WORKER_COUNT", cfg.Initial.WorkerCount)
	cfg.Initial.ChunkSize = util.GetEnvAsInt("INITIAL_CHUNK_SIZE", cfg.Initial.ChunkSize)

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
	Initial        *InitialConfig     `yaml:"initial" json:"initial"`
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

type InitialConfig struct {
	WorkerCount int `yaml:"worker_count" json:"worker_count"`
	ChunkSize   int `yaml:"chunk_size" json:"chunk_size"`
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
