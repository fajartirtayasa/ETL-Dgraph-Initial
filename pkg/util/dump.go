// Package utils

package util

import (
	"bytes"
	"encoding/json"
	"fmt"
)

// DumpToString interface to string
func DumpToString(v any) (string, error) {

	str, ok := v.(string)
	if !ok {
		buff := &bytes.Buffer{}
		if err := json.NewEncoder(buff).Encode(v); err != nil {
			return "", err
		}
		return buff.String(), nil
	}

	return str, nil
}

// DebugPrint for debug print on terminal
func DebugPrint(v ...any) {
	for _, x := range v {
		str, err := DumpToString(x)
		if err != nil {
			fmt.Println("failed to debug ", err)
			break
		}
		fmt.Println(str)
	}
}

// Data To json Bytes
func ToJSONByte(v any) ([]byte, error) {

	switch v := v.(type) {
	case []byte:
		return v, nil
	case string:
		return []byte(v), nil
	}

	buff := &bytes.Buffer{}
	if err := json.NewEncoder(buff).Encode(v); err != nil {
		return nil, err
	}
	return buff.Bytes(), nil
}

// ToJSONByteCompact for insignificant space characters elided
func ToJSONByteCompact(v any) ([]byte, error) {
	buff := bytes.Buffer{}
	b, err := ToJSONByte(v)
	if err != nil {
		return nil, err
	}

	if err := json.Compact(&buff, b); err != nil {
		return nil, err
	}
	return buff.Bytes(), nil
}
