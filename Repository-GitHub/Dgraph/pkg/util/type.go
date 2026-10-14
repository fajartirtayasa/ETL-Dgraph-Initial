// Package util
package util

import "reflect"

func IsSameType(src, dest any) bool {
	return reflect.TypeOf(src) == reflect.TypeOf(dest)
}
