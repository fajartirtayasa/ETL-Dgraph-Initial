package util

import (
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestGetEnv(t *testing.T) {
	t.Parallel()

	// Test case 1: Environment variable exists
	os.Setenv("TEST_STRING_ENV", "hello")
	assert.Equal(t, "hello", GetEnv("TEST_STRING_ENV", "default"))
	os.Unsetenv("TEST_STRING_ENV")

	// Test case 2: Environment variable does not exist, fallback used
	assert.Equal(t, "default", GetEnv("NON_EXISTENT_STRING_ENV", "default"))
}

func TestGetEnvAsInt(t *testing.T) {
	t.Parallel()

	// Test case 1: Environment variable exists and is a valid integer
	os.Setenv("TEST_INT_ENV", "123")
	assert.Equal(t, 123, GetEnvAsInt("TEST_INT_ENV", 999))
	os.Unsetenv("TEST_INT_ENV")

	// Test case 2: Environment variable exists but is not a valid integer, fallback used
	os.Setenv("TEST_INT_ENV_INVALID", "abc")
	assert.Equal(t, 999, GetEnvAsInt("TEST_INT_ENV_INVALID", 999))
	os.Unsetenv("TEST_INT_ENV_INVALID")

	// Test case 3: Environment variable does not exist, fallback used
	assert.Equal(t, 999, GetEnvAsInt("NON_EXISTENT_INT_ENV", 999))
}

func TestGetEnvAsBool(t *testing.T) {
	t.Parallel()

	// Test case 1: Environment variable exists and is "true"
	os.Setenv("TEST_BOOL_ENV_TRUE", "true")
	assert.Equal(t, true, GetEnvAsBool("TEST_BOOL_ENV_TRUE", false))
	os.Unsetenv("TEST_BOOL_ENV_TRUE")

	// Test case 2: Environment variable exists and is "false"
	os.Setenv("TEST_BOOL_ENV_FALSE", "false")
	assert.Equal(t, false, GetEnvAsBool("TEST_BOOL_ENV_FALSE", true))
	os.Unsetenv("TEST_BOOL_ENV_FALSE")

	// Test case 3: Environment variable does not exist, fallback used
	assert.Equal(t, false, GetEnvAsBool("NON_EXISTENT_BOOL_ENV", false))
}
