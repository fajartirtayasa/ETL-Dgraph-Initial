// Package util

package util

import (
	"fmt"
	"testing"
	"time"
)

func TestStructToMap(t *testing.T) {
	type (
		Contoh struct {
			ID        int64     `json:"id" store:"id"`
			Name      string    `json:"name" store:"name,omitempty"`
			CreatedAt time.Time `json:"created_at"`
			alamat    string
		}
	)

	testCase := []struct {
		Input    any
		CountCol int
		Error    error
	}{
		{
			Input: Contoh{
				ID:        1,
				Name:      "test",
				CreatedAt: time.Now(),
				alamat:    "test",
			},
			CountCol: 2,
			Error:    nil,
		},
		{
			Input: &Contoh{
				ID:        1,
				Name:      "",
				CreatedAt: time.Now(),
				alamat:    "test",
			},
			CountCol: 1,
			Error:    nil,
		},
		{
			Input: map[string]any{
				"name": "test 2",
			},
			CountCol: 0,
			Error:    fmt.Errorf("only accepted struct, got map"),
		},
		{
			Input:    "test",
			CountCol: 0,
			Error:    fmt.Errorf("only accepted struct, got map"),
		},
	}

	for _, x := range testCase {
		c, e := StructToMap(x.Input, "store")
		if fmt.Sprintf("%T", x.Error) == fmt.Sprintf("%T", e) {

			t.Logf("expected '%v', but got '%v'", x.Error, e)
		} else {
			t.Errorf("expected '%v', but got '%v'", x.Error, e)
		}

		if len(c) == x.CountCol {
			t.Logf("expected %v, but got %v", x.CountCol, len(c))
		} else {
			t.Errorf("expected %v, but got %v", x.CountCol, len(c))
		}

	}
}

func TestToColumnsValues(t *testing.T) {

	type (
		Contoh struct {
			ID        int64     `json:"id" store:"id"`
			Name      string    `json:"name" store:"name,omitempty"`
			CreatedAt time.Time `json:"created_at"`
			alamat    string
		}
	)

	testCase := []struct {
		Input    any
		CountCol int
		Error    error
	}{
		{
			Input: Contoh{
				ID:        1,
				Name:      "test",
				CreatedAt: time.Now(),
				alamat:    "test",
			},
			CountCol: 2,
			Error:    nil,
		},
		{
			Input: &Contoh{
				ID:        1,
				Name:      "",
				CreatedAt: time.Now(),
				alamat:    "test",
			},
			CountCol: 1,
			Error:    nil,
		},
		{
			Input: map[string]any{
				"name": "test 2",
			},
			CountCol: 0,
			Error:    fmt.Errorf("only accepted struct, got map"),
		},
		{
			Input:    "test",
			CountCol: 0,
			Error:    fmt.Errorf("only accepted struct, got map"),
		},
	}

	for _, x := range testCase {
		c, v, e := ToColumnsValues(x.Input, "store")
		if fmt.Sprintf("%T", x.Error) == fmt.Sprintf("%T", e) {

			t.Logf("expected '%v', but got '%v'", x.Error, e)
		} else {
			t.Errorf("expected '%v', but got '%v'", x.Error, e)
		}

		if len(c) == x.CountCol {
			t.Logf("expected %v, but got %v", x.CountCol, len(c))
		} else {
			t.Errorf("expected %v, but got %v", x.CountCol, len(c))
		}

		if len(v) == x.CountCol {
			t.Logf("expected %v, but got %v", x.CountCol, len(v))
		} else {
			t.Errorf("expected %v, but got %v", x.CountCol, len(v))
		}

	}

}
