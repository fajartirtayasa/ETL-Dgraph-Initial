package database

import (
	"fmt"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/jmoiron/sqlx"
)

func NewConnectionPostgres(config *Config) (*sqlx.DB, error) {
	connectionString := fmt.Sprintf("postgres://%s:%s@%s:%d/%s?sslmode=disable&search_path=%s&connect_timeout=%d&client_encoding=%s",
		config.User,
		config.Password,
		config.Host,
		config.Port,
		config.Name,
		config.Schema,
		config.TimeoutSecond,
		config.Charset,
	)

	db, err := sqlx.Open("pgx", connectionString)
	if err != nil {
		return nil, fmt.Errorf("failed to open postgres connection: %w", err)
	}

	db.SetMaxOpenConns(config.MaxConnectionOpen)
	db.SetMaxIdleConns(config.MaxConnectionIdle)
	db.SetConnMaxLifetime(time.Duration(config.MaxLifeTimeMS) * time.Millisecond)

	if err := db.Ping(); err != nil {
		db.Close()
		return nil, fmt.Errorf("failed to ping postgres connection: %w", err)
	}

	return db, nil
}
