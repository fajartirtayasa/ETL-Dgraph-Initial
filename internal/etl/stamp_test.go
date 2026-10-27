package etl

import "testing"

func TestParseStampSyncTime(t *testing.T) {
	tests := []struct {
		name    string
		input   string
		want    bool
		wantErr string
	}{
		{"empty means the default", "", true, ""},
		{"whitespace only means the default", "  ", true, ""},
		{"true", "true", true, ""},
		{"upper case with spaces", " TRUE ", true, ""},
		{"one", "1", true, ""},
		{"false", "false", false, ""},
		{"capitalised false", "False", false, ""},
		{"zero", "0", false, ""},
		{"no is not a boolean", "no", false, `unsupported INITIAL_STAMP_SYNC_TIME "no": use true or false (empty means true)`},
		{"off is not a boolean", "off", false, `unsupported INITIAL_STAMP_SYNC_TIME "off": use true or false (empty means true)`},
		{"raw value is quoted as set, not trimmed", " yes ", false, `unsupported INITIAL_STAMP_SYNC_TIME " yes ": use true or false (empty means true)`},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := ParseStampSyncTime(tt.input)

			if tt.wantErr == "" {
				if err != nil {
					t.Fatalf("ParseStampSyncTime(%q) returned error %q, want none", tt.input, err)
				}
			} else {
				if err == nil {
					t.Fatalf("ParseStampSyncTime(%q) = %t with no error, want error %q", tt.input, got, tt.wantErr)
				}
				if err.Error() != tt.wantErr {
					t.Fatalf("ParseStampSyncTime(%q) error = %q, want %q", tt.input, err.Error(), tt.wantErr)
				}
			}

			if got != tt.want {
				t.Fatalf("ParseStampSyncTime(%q) = %t, want %t", tt.input, got, tt.want)
			}
		})
	}
}
