package util

import (
	"path/filepath"
	"strings"
)

// IsValidImageExtension checks if the file extension is a valid image format
func IsValidImageExtension(filename string) bool {
	ext := strings.ToLower(filepath.Ext(filename))
	validExtensions := []string{".jpg", ".jpeg", ".png", ".gif", ".bmp", ".webp"}

	for _, validExt := range validExtensions {
		if ext == validExt {
			return true
		}
	}

	return false
}
