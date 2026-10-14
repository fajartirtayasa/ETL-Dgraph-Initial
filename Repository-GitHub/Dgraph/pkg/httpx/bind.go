package httpx

import (
	"encoding/json"
	"io"
	"net/http"
	"reflect"
	"strconv"
	"strings"
)

// Bind attempts to populate dst from the request. If the request has a non-empty
// JSON body, it decodes the JSON into dst. Otherwise, it falls back to mapping
// URL query parameters into the struct fields of dst using `json` tags.
// This function correctly handles both value and pointer fields in the destination struct.
//
// The dst argument must be a pointer to a struct.
func Bind(r *http.Request, dst any) error {
	// Check if the request has a body that looks like JSON.
	if r.Body != nil && r.ContentLength > 0 {
		// Read the body to check if it's non-empty.
		data, err := io.ReadAll(r.Body)
		if err != nil {
			return err
		}
		// It's crucial to restore the body so it can be read again if needed elsewhere.
		r.Body = io.NopCloser(strings.NewReader(string(data)))

		// If the body has content after trimming whitespace, unmarshal as JSON.
		trimmed := strings.TrimSpace(string(data))
		if len(trimmed) > 0 && (trimmed[0] == '{' || trimmed[0] == '[') {
			return json.Unmarshal(data, dst)
		}
	}

	// If no JSON body, fall back to binding from URL query parameters.
	v := reflect.ValueOf(dst)
	if v.Kind() != reflect.Ptr || v.Elem().Kind() != reflect.Struct {
		// dst must be a non-nil pointer to a struct.
		return nil
	}
	v = v.Elem()
	t := v.Type()

	q := r.URL.Query()

	for i := 0; i < t.NumField(); i++ {
		field := t.Field(i)
		fv := v.Field(i)

		if !fv.CanSet() {
			continue
		}

		// Get the json tag, falling back to the field name.
		tag := field.Tag.Get("json")
		if tag == "" {
			tag = field.Name
		}
		// Ignore tag options like ",omitempty".
		tag = strings.Split(tag, ",")[0]

		// Get the value from the query parameters.
		vals, ok := q[tag]
		if !ok || len(vals) == 0 {
			continue
		}
		val := vals[0] // Use the first value for a given key.

		// This logic now handles both pointer and value fields correctly.
		targetField := fv
		targetKind := fv.Kind()

		// If the field is a pointer, we need to create a new value for it to point to.
		if targetKind == reflect.Ptr {
			// Initialize the pointer if it's nil.
			if fv.IsNil() {
				fv.Set(reflect.New(fv.Type().Elem()))
			}
			// Dereference the pointer to set the underlying value.
			targetField = fv.Elem()
			targetKind = targetField.Kind()
		}

		// Set the value based on the (potentially dereferenced) field's kind.
		switch targetKind {
		case reflect.String:
			targetField.SetString(val)
		case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
			if ival, err := strconv.ParseInt(val, 10, 64); err == nil {
				targetField.SetInt(ival)
			}
		case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
			if uval, err := strconv.ParseUint(val, 10, 64); err == nil {
				targetField.SetUint(uval)
			}
		case reflect.Bool:
			if bval, err := strconv.ParseBool(val); err == nil {
				targetField.SetBool(bval)
			}
		case reflect.Float32, reflect.Float64:
			if fval, err := strconv.ParseFloat(val, 64); err == nil {
				targetField.SetFloat(fval)
			}
		}
	}

	return nil
}
