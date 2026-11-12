package service

import (
	"io"
	"log/slog"
	"os"
	"strings"

	"crawler/internal/config"
)

// Service framework logging: a single structured logger on the stdlib
// log/slog package (no new dependencies). The severity level comes from the
// configuration (log.level; the CRAWLER_LOG_LEVEL override is applied inside
// config.Load), the service name context is the service attribute on each record.

// NewLogger returns the logger for service name: text format on stderr
// (readable in docker logs), level from cfg.Log.Level (a nil config or an
// empty level means info), each record tagged with the service=name attribute.
// Run installs the logger as the default (slog.SetDefault): then both the
// migrations and service code via slog.Default write to the shared stream
// with context.
func NewLogger(name string, cfg *config.Config) *slog.Logger {
	level := slog.LevelInfo
	if cfg != nil {
		level = logLevel(cfg.Log.Level)
	}
	return newLogger(os.Stderr, name, level)
}

// newLogger is NewLogger with an explicit write sink (unit tests).
func newLogger(w io.Writer, name string, level slog.Level) *slog.Logger {
	h := slog.NewTextHandler(w, &slog.HandlerOptions{Level: level})
	return slog.New(h).With("service", name)
}

// logLevel maps the log.level value to slog.Level. An unknown value is
// unreachable after config validation; just in case, treat it as info.
func logLevel(s string) slog.Level {
	switch strings.ToLower(strings.TrimSpace(s)) {
	case "debug":
		return slog.LevelDebug
	case "warn":
		return slog.LevelWarn
	case "error":
		return slog.LevelError
	default: // "info" and ""
		return slog.LevelInfo
	}
}
