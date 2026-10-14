package logger

import (
	"log/slog"
	"os"
)

const (
	MessageKey = "message"
)

func SetupJsonFormatter() {
	opts := &slog.HandlerOptions{
		// AddSource enables the inclusion of file and line number.
		AddSource: false,

		// ReplaceAttr allows us to customize attribute keys.
		ReplaceAttr: func(groups []string, a slog.Attr) slog.Attr {
			// Remap the default message key "msg" to our custom "message".
			if a.Key == slog.MessageKey {
				a.Key = MessageKey
			}
			return a
		},
	}

	// Create a new JSON handler with our custom options.
	handler := slog.NewJSONHandler(os.Stdout, opts)

	// Set this handler as the default for the entire application.
	slog.SetDefault(slog.New(handler))
}
