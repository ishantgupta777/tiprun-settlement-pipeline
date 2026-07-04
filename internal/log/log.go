// Package log configures a structured JSON slog logger used by every service.
// Every key pipeline event is emitted as a single structured line so the flow
// can be reconstructed from logs alone.
package log

import (
	"log/slog"
	"os"
	"strings"
)

// New returns a JSON structured logger tagged with the component name.
// LOG_LEVEL (debug|info|warn|error) controls verbosity; defaults to info.
func New(component string) *slog.Logger {
	var level slog.Level
	switch strings.ToLower(os.Getenv("LOG_LEVEL")) {
	case "debug":
		level = slog.LevelDebug
	case "warn":
		level = slog.LevelWarn
	case "error":
		level = slog.LevelError
	default:
		level = slog.LevelInfo
	}

	handler := slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: level})
	logger := slog.New(handler).With("component", component)
	return logger
}
