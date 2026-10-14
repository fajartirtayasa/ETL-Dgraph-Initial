package database

import (
	"database/sql"
	"fmt"

	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/pressly/goose/v3"
)

type LanguageType string

const (
	LanguageSQL LanguageType = "sql"
	LanguageGo  LanguageType = "go"
)

type Migration struct {
	db        *sql.DB
	directory string
}

func NewMigration(driver DatabaseDriver, config *Config, directory string) (*Migration, error) {
	switch driver {
	case Postgres:
		connectionString := fmt.Sprintf("postgres://%s:%s@%s:%d/%s?sslmode=disable&search_path=%s", config.User, config.Password, config.Host, config.Port, config.Name, config.Schema)
		db, err := goose.OpenDBWithDriver("pgx", connectionString)
		if err != nil {
			return nil, fmt.Errorf("failed to open database: %w", err)
		}

		return &Migration{
			db,
			directory,
		}, nil

	default:
		return nil, fmt.Errorf("unsupported database driver: %s", driver)
	}
}

func (p *Migration) Ping() error {
	err := p.db.Ping()
	if err != nil {
		return fmt.Errorf("failed to ping database: %w", err)
	}

	return nil
}

func (m *Migration) Close() error {
	if err := m.db.Close(); err != nil {
		return fmt.Errorf("failed to close DB: %v", err)
	}
	return nil
}

func (m *Migration) SetMigrationDirectory(directory string) {
	m.directory = directory
}

func (m *Migration) Up() error {
	if err := goose.Up(m.db, m.directory); err != nil {
		return fmt.Errorf("migrate up: %v", err)
	}

	return nil
}

func (m *Migration) UpByOne() error {
	if err := goose.UpByOne(m.db, m.directory); err != nil {
		return fmt.Errorf("migrate up by one: %v", err)
	}

	return nil
}

func (m *Migration) UpTo(version int64) error {
	if err := goose.UpTo(m.db, m.directory, version); err != nil {
		return fmt.Errorf("migrate up to: %v", err)
	}

	return nil
}

func (m *Migration) Down() error {
	if err := goose.Down(m.db, m.directory); err != nil {
		return fmt.Errorf("migrate down: %v", err)
	}

	return nil
}

func (m *Migration) DownTo(version int64) error {
	if err := goose.DownTo(m.db, m.directory, version); err != nil {
		return fmt.Errorf("migrate down to: %v", err)
	}

	return nil
}

func (m *Migration) Redo() error {
	if err := goose.Redo(m.db, m.directory); err != nil {
		return fmt.Errorf("migrate redo: %v", err)
	}

	return nil
}

func (m *Migration) Reset() error {
	if err := goose.Reset(m.db, m.directory); err != nil {
		return fmt.Errorf("migrate reset: %v", err)
	}

	return nil
}

func (m *Migration) Status() error {
	if err := goose.Status(m.db, m.directory); err != nil {
		return fmt.Errorf("get status: %v", err)
	}

	return nil
}

func (m *Migration) Version() error {
	if err := goose.Version(m.db, m.directory); err != nil {
		return fmt.Errorf("get version: %v", err)
	}

	return nil
}

func (m *Migration) Create(name string, languageType LanguageType) error {
	switch languageType {
	case LanguageSQL, LanguageGo:
		if err := goose.Create(nil, m.directory, name, string(languageType)); err != nil {
			return fmt.Errorf("migrate create: %v", err)
		}
		return nil
	default:
		return fmt.Errorf("language type %s is not supported", languageType)
	}
}

func (m *Migration) Fix() error {
	if err := goose.Fix(m.directory); err != nil {
		return fmt.Errorf("fix migration: %v", err)
	}

	return nil
}

func ToLanguageType(input string) (LanguageType, error) {
	languageType := LanguageType(input)
	switch languageType {
	case LanguageSQL, LanguageGo:
		return languageType, nil
	default:
		// The input is not one of the allowed values.
		return "", fmt.Errorf("invalid language type '%s': please use 'sql' or 'go'", input)
	}
}
