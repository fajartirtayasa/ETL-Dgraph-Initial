// Package logger
package logger

import (
	"context"
	"hara/traceability-dgraph-etl/pkg/util"
	"log/slog"
	"os"
)

// Define a type for our context key to avoid collisions.
type contextKey string

const accessLogKey = contextKey("access")

// --- Core Logging Functions ---

// Info logs an informational message with structured attributes.
func Info(msg string, args ...slog.Attr) {
	slog.Default().LogAttrs(context.Background(), slog.LevelInfo, msg, args...)
}

// Error logs an error message with structured attributes.
func Error(msg string, err any, args ...slog.Attr) {
	args = append(args, slog.Any("error", err))
	slog.Default().LogAttrs(context.Background(), slog.LevelError, msg, args...)
}

// Debug logs a debug message.
func Debug(msg string, args ...slog.Attr) {
	slog.Default().LogAttrs(context.Background(), slog.LevelDebug, msg, args...)
}

// Warn logs a warning message.
func Warn(msg string, args ...slog.Attr) {
	slog.Default().LogAttrs(context.Background(), slog.LevelWarn, msg, args...)
}

// Fatal logs an error message and then calls os.Exit(1).
func Fatal(msg string, err error, args ...slog.Attr) {
	args = append(args, slog.Any("error", err))
	slog.Default().LogAttrs(context.Background(), slog.LevelError, msg, args...)
	os.Exit(1)
}

// --- Context-Aware Logging ---

// InfoWithContext logs an informational message, automatically including attributes from the context.
func InfoWithContext(ctx context.Context, msg string, args ...slog.Attr) {
	slog.Default().LogAttrs(ctx, slog.LevelInfo, msg, args...)
}

// ErrorWithContext logs an error message, automatically including attributes from the context.
func ErrorWithContext(ctx context.Context, msg string, err error, args ...slog.Attr) {
	args = append(args, slog.Any("error", err))
	slog.Default().LogAttrs(ctx, slog.LevelError, msg, args...)
}

// WarnWithContext logs a warning message, automatically including attributes from the context.
func WarnWithContext(ctx context.Context, msg string, args ...slog.Attr) {
	slog.Default().LogAttrs(ctx, slog.LevelWarn, msg, args...)
}

// FatalWithContext logs an error message with context and then calls os.Exit(1).
func FatalWithContext(ctx context.Context, msg string, err error, args ...slog.Attr) {
	args = append(args, slog.Any("error", err))
	slog.Default().LogAttrs(ctx, slog.LevelError, msg, args...)
	os.Exit(1)
}

// --- Attribute Helper Functions (No changes needed here) ---

// Any creates a generic slog attribute.
func Any(key string, value any) slog.Attr {
	return slog.Any(key, value)
}

// Struct creates an attribute by dumping a struct to a string.
func Struct(key string, value any) slog.Attr {
	str, err := util.DumpToString(value)
	if err != nil {
		return slog.String(key, "{}")
	}
	return slog.String(key, str)
}

// EventName creates an attribute for an event name.
func EventName(v any) slog.Attr {
	return slog.Any("name", v)
}

// EventState creates an attribute for an event state.
func EventState(v string) slog.Attr {
	return slog.String("state", v)
}

// EventId creates an attribute for an event ID.
func EventId(v any) slog.Attr {
	return slog.Any("id", v)
}

// EventUserID creates an attribute for a user ID.
func EventUserID(v any) slog.Attr {
	return slog.Any("user_id", v)
}

// --- Structured Input/Output Attributes (No changes needed here) ---

// EventInputHttp creates a structured attribute for an HTTP request.
func EventInputHttp(v any) slog.Attr {
	return slog.Group("input",
		slog.Group("http",
			slog.Group("request",
				slog.Group("body",
					slog.Any("object", v),
				),
			),
		),
	)
}

// EventOutputHttp creates a structured attribute for an HTTP response.
func EventOutputHttp(statusCode any, vObject any, vBodyRaw any) slog.Attr {
	return slog.Group("output",
		slog.Group("http",
			slog.Group("response",
				slog.Any("http_status", statusCode),
				slog.Group("body",
					slog.Any("object", vObject),
					slog.Any("raw", vBodyRaw),
				),
			),
		),
	)
}

// --- Context-Aware Handler (No changes needed here) ---

// ContextHandler is an slog.Handler that automatically adds values from the context.
type ContextHandler struct {
	slog.Handler
}

// Handle adds the "access" log fields from the context to the log record.
func (h *ContextHandler) Handle(ctx context.Context, r slog.Record) error {
	if accessAttrs, ok := ctx.Value(accessLogKey).([]slog.Attr); ok {
		r.AddAttrs(accessAttrs...)
	}
	return h.Handler.Handle(ctx, r)
}

// NewContextHandler creates a new handler that wraps a base handler.
func NewContextHandler(baseHandler slog.Handler) *ContextHandler {
	return &ContextHandler{baseHandler}
}

// --- Initialization (No changes needed here) ---

// Init initializes the default logger with a custom handler.
func Init() {
	// Create a default JSON handler.
	baseHandler := slog.NewJSONHandler(os.Stdout, nil)
	// Wrap it with our custom context-aware handler.
	contextHandler := NewContextHandler(baseHandler)
	// Set the default logger for the application.
	slog.SetDefault(slog.New(contextHandler))
}
