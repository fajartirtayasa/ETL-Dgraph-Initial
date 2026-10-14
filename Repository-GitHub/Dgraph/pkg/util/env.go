package util

import (
	"os"
	"strconv"
	"strings"
)

// GetEnv retrieves a string from environment variables or returns a fallback value.
func GetEnv(key, fallback string) string {
	if value, exists := os.LookupEnv(key); exists {
		return value
	}
	return fallback
}

// GetEnvAsInt retrieves an integer from environment variables or returns a fallback value.
func GetEnvAsInt(key string, fallback int) int {
	if valueStr, exists := os.LookupEnv(key); exists {
		if value, err := strconv.Atoi(valueStr); err == nil {
			return value
		}
	}
	return fallback
}

// GetEnvAsBool retrieves a boolean from environment variables or returns a fallback value.
func GetEnvAsBool(key string, fallback bool) bool {
	if valueStr, exists := os.LookupEnv(key); exists {
		// strconv.ParseBool is flexible, accepts "1", "t", "true", "TRUE", etc.
		if value, err := strconv.ParseBool(strings.ToLower(valueStr)); err == nil {
			return value
		}
	}
	return fallback
}

// GetEnvAsStringSlice retrieves a string slice from environment variables or returns a fallback value.
func GetEnvAsStringSlice(key string, fallback []string) []string {
	if valueStr, exists := os.LookupEnv(key); exists {
		if valueStr == "" {
			return fallback
		}
		// Split by comma and trim whitespace
		values := strings.Split(valueStr, ",")
		for i, v := range values {
			values[i] = strings.TrimSpace(v)
		}
		return values
	}
	return fallback
}
