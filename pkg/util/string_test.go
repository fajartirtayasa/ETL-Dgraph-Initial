package util

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestToString(t *testing.T) {
	t.Parallel()
	scenarios := []struct {
		Input    any
		Expected string
	}{
		{
			Input:    int(1),
			Expected: "1",
		},
		{
			Input:    int8(2),
			Expected: "2",
		},
		{
			Input:    int16(3),
			Expected: "3",
		},
		{
			Input:    int32(4),
			Expected: "4",
		},
		{
			Input:    int64(5),
			Expected: "5",
		},
		{
			Input:    uint(6),
			Expected: "6",
		},
		{
			Input:    uint8(7),
			Expected: "7",
		},
		{
			Input:    uint16(8),
			Expected: "8",
		},
		{
			Input:    uint32(9),
			Expected: "9",
		},
		{
			Input:    uint64(10),
			Expected: "10",
		},
		{
			Input:    float32(11),
			Expected: "11",
		},
		{
			Input:    float64(12),
			Expected: "12",
		},
		{
			Input:    bool(true),
			Expected: "true",
		},
		{
			Input: struct {
			}{},
			Expected: "{}",
		},

		{
			Input:    string(`test`),
			Expected: "test",
		},
	}

	for _, sc := range scenarios {
		r := ToString(sc.Input)

		assert.Equal(t, sc.Expected, r)
	}
}

func TestStringJoin(t *testing.T) {
	type args struct {
		elems   []string
		sep     string
		lastSep string
	}
	tests := []struct {
		name string
		args args
		want string
	}{
		{
			name: "#1",
			args: args{
				elems:   []string{"name", "email"},
				sep:     "=?,",
				lastSep: "=?",
			},
			want: "name=?,email=?",
		},
		{
			name: "#2",
			args: args{
				elems:   []string{"name"},
				sep:     "=?,",
				lastSep: "=?",
			},
			want: "name=?",
		},
		{
			name: "#3",
			args: args{
				elems:   []string{},
				sep:     "=?,",
				lastSep: "=?",
			},
			want: "",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := StringJoin(tt.args.elems, tt.args.sep, tt.args.lastSep); got != tt.want {
				t.Errorf("StringJoin() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestSubString(t *testing.T) {
	type args struct {
		input  string
		start  int
		length int
	}
	tests := []struct {
		name string
		args args
		want string
	}{
		{
			name: "#1",
			args: args{
				input:  "",
				start:  1,
				length: 0,
			},
			want: "",
		},
		{
			name: "#2",
			args: args{
				input:  "to the moon",
				start:  0,
				length: 2,
			},
			want: "to",
		},
		{
			name: "#3",
			args: args{
				input:  "L",
				start:  0,
				length: 2,
			},
			want: "L",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := SubString(tt.args.input, tt.args.start, tt.args.length); got != tt.want {
				t.Errorf("SubString() = %v, want %v", got, tt.want)
			}
		})
	}
}
