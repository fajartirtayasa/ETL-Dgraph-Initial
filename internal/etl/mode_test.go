package etl

import (
	"context"
	"strings"
	"testing"
)

const (
	errEmptyMode       = "APP_MODE is empty; set APP_MODE=initial"
	errIncrementalMode = "APP_MODE=incremental is no longer supported here: the incremental ETL lives in ETL-Dgraph-Incremental"
)

func TestParseMode(t *testing.T) {
	tests := []struct {
		name    string
		input   string
		want    Mode
		wantErr string
	}{
		{"lower case", "initial", ModeInitial, ""},
		{"surrounding spaces and a capital", " Initial ", ModeInitial, ""},
		{"upper case", "INITIAL", ModeInitial, ""},
		{"empty", "", "", errEmptyMode},
		{"whitespace only", "   ", "", errEmptyMode},
		{"incremental", "incremental", "", errIncrementalMode},
		{"incremental with a capital", "Incremental", "", errIncrementalMode},
		{"typo", "initail", "", `unsupported APP_MODE "initail": only "initial" is supported`},
		{"unexpanded shell reference", "$(APP_MODE)", "", `unsupported APP_MODE "$(APP_MODE)": only "initial" is supported`},
		{"raw value is quoted as set, not trimmed", "  bogus ", "", `unsupported APP_MODE "  bogus ": only "initial" is supported`},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := ParseMode(tt.input)

			if tt.wantErr == "" {
				if err != nil {
					t.Fatalf("ParseMode(%q) returned error %q, want none", tt.input, err)
				}
			} else {
				if err == nil {
					t.Fatalf("ParseMode(%q) = %q with no error, want error %q", tt.input, got, tt.wantErr)
				}
				if err.Error() != tt.wantErr {
					t.Fatalf("ParseMode(%q) error = %q, want %q", tt.input, err.Error(), tt.wantErr)
				}
			}

			if got != tt.want {
				t.Fatalf("ParseMode(%q) = %q, want %q", tt.input, got, tt.want)
			}
		})
	}
}

// Run must refuse any mode but initial before it looks at the database adapter, so the zero Dependencies below
// prove the rejection comes from the mode check.
func TestRunRejectsEveryModeButInitial(t *testing.T) {
	for _, mode := range []Mode{"", "incremental", "Initial", "bogus"} {
		err := Run(context.Background(), RunnerConfig{Mode: mode}, Dependencies{})
		if err == nil {
			t.Fatalf("Run with mode %q returned no error", mode)
		}
		if !strings.Contains(err.Error(), "unsupported ETL mode") {
			t.Fatalf("Run with mode %q error = %q, want it to say \"unsupported ETL mode\"", mode, err)
		}
	}
}
