// Package utils

package util

import (
	"strings"
	"testing"
)

const failedMsg = "expected %v, got %v"

func TestDumpToString(t *testing.T) {
	t.Parallel()
	result, err := DumpToString("test")
	if err != nil {
		t.Fatalf("DumpToString error %v", err)
	}

	if result == "test" {
		t.Logf("expected: %v, got: %v", "test", result)
	} else {
		t.Fatalf("expected: %v, got: %v", "test", result)
	}

	result, err = DumpToString(1)
	if err != nil {
		t.Fatalf("DumpToString error %v", err)
	}

	if strings.Trim(result, "\n") == "1" {
		t.Logf("expected: %v, got: %s", "1", result)
	} else {
		t.Fatalf("expected: %v, got: %v", "1", result)
	}
}

func TestDebugPrint(t *testing.T) {
	DebugPrint("test print")
}

func TestToJSONByte(t *testing.T) {
	testCase := []struct {
		Input    any
		Expected []byte
	}{
		{
			Input: struct {
				ID   uint64 `json:"id"`
				Name string `json:"name"`
			}{
				ID:   1,
				Name: "testcase1",
			},
			Expected: []byte{123, 34, 105, 100, 34, 58, 49, 44, 34, 110, 97, 109, 101, 34, 58, 34, 116, 101, 115, 116, 99, 97, 115, 101, 49, 34, 125, 10},
		},
		{
			Input:    "test",
			Expected: []byte{116, 101, 115, 116},
		},

		{
			Input:    []byte{116, 101, 115, 116},
			Expected: []byte{116, 101, 115, 116},
		},
	}

	for _, v := range testCase {

		r, err := ToJSONByte(v.Input)
		if err != nil {
			t.Errorf("ToJSONByte error %v", err)
		}
		if string(v.Expected) == string(r) {
			t.Logf(failedMsg, v.Expected, r)
		} else {
			t.Errorf(failedMsg, v.Expected, r)
		}
	}
}
func TestToJSONByteCompact(t *testing.T) {
	testCase := []struct {
		Input    any
		Expected []byte
	}{
		{
			Input: struct {
				ID   uint64 `json:"id"`
				Name string `json:"name"`
			}{
				ID:   1,
				Name: "testcase1",
			},
			Expected: []byte{123, 34, 105, 100, 34, 58, 49, 44, 34, 110, 97, 109, 101, 34, 58, 34, 116, 101, 115, 116, 99, 97, 115, 101, 49, 34, 125},
		},
	}

	for _, v := range testCase {
		r, err := ToJSONByteCompact(v.Input)
		if err != nil {
			t.Errorf("ToJSONByteCompact error %v", err)
		}

		if string(v.Expected) == string(r) {
			t.Logf(failedMsg, v.Expected, r)
		} else {
			t.Errorf(failedMsg, v.Expected, r)
		}
	}
}
