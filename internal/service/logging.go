package service

import (
	"io"
	"log/slog"
	"os"
	"strings"

	"crawler/internal/config"
)

// Логирование каркаса сервисов: единый структурированный логгер на stdlib
// log/slog (без новых зависимостей). Уровень важности задаётся конфигурацией
// (log.level, переопределение CRAWLER_LOG_LEVEL применяется внутри
// config.Load), контекст имени сервиса — атрибут service в каждой записи.

// NewLogger возвращает логгер сервиса name: текстовый формат на stderr
// (читаемо в docker logs), уровень из cfg.Log.Level (nil-конфиг или пустой
// уровень — info), каждая запись помечена атрибутом service=name.
// Run устанавливает логгер дефолтным (slog.SetDefault): тогда и миграции,
// и код сервисов через slog.Default пишут в общий поток с контекстом.
func NewLogger(name string, cfg *config.Config) *slog.Logger {
	level := slog.LevelInfo
	if cfg != nil {
		level = logLevel(cfg.Log.Level)
	}
	return newLogger(os.Stderr, name, level)
}

// newLogger — NewLogger с явным приёмником записи (юнит-тесты).
func newLogger(w io.Writer, name string, level slog.Level) *slog.Logger {
	h := slog.NewTextHandler(w, &slog.HandlerOptions{Level: level})
	return slog.New(h).With("service", name)
}

// logLevel переводит значение log.level в slog.Level. Неизвестное значение
// после валидации config недостижимо; на всякий случай считаем его info.
func logLevel(s string) slog.Level {
	switch strings.ToLower(strings.TrimSpace(s)) {
	case "debug":
		return slog.LevelDebug
	case "warn":
		return slog.LevelWarn
	case "error":
		return slog.LevelError
	default: // "info" и ""
		return slog.LevelInfo
	}
}
