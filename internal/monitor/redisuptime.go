// Источник аптайма Redis для алерта redis_restart (T-270, ТЗ-fsm §3
// ФТ-2.2): uptime_in_seconds из INFO server того же клиента go-redis,
// что читает память (redismem.go) и глубины стримов (cmd/monitor).
// Рестарт Redis при отсутствии персистентности (docs/redis.md §5 —
// осознанный выбор) теряет ВСЕ недоставленные записи стримов: потерянные
// q:dns самолечатся lease-проходом T-029 (строки в queued), а q:http при
// уже записанном done зависают до компенсатора T-270 — рестарт остаётся
// главной причиной класса потерь «статус повис» (инцидент 22.10:
// когорта 1865 доменов, gauge T-256). Алерт даёт оператору сигнал в
// момент события, не когорту gauge спустя сутки.
//
// Разбор — вручную по строкам «ключ:значение» (секции «# …» —
// комментарии), как parseRedisMemoryINFO: нужно одно поле, формат
// фиксирован сервером.
package monitor

import (
	"context"
	"fmt"
	"strconv"
	"strings"
)

// RedisUptime — снимок аптайма Redis из INFO server.
type RedisUptime struct {
	// Seconds — uptime_in_seconds: секунды с запуска сервера; рестарт
	// обнуляет — «меньше окна» ловит недавний рестарт без памяти машины.
	Seconds int64
}

// RedisUptimeSource — источник аптайма Redis; интерфейс для подмены в
// юнит-тестах (реальное чтение — INFO живого Redis, в тестах его нет).
// Стиль RedisMemorySource.
type RedisUptimeSource interface {
	// RedisUptime возвращает снимок INFO server. Ошибка = «не измеряется»
	// с причиной.
	RedisUptime(ctx context.Context) (RedisUptime, error)
}

// compile-time: RedisInfo — и источник памяти, и источник аптайма (INFO
// разных секций одного клиента).
var _ RedisUptimeSource = RedisInfo{}

// RedisUptime читает секцию server ответа INFO и разбирает её.
func (s RedisInfo) RedisUptime(ctx context.Context) (RedisUptime, error) {
	if s.RDB == nil {
		return RedisUptime{}, fmt.Errorf("monitor: аптайм Redis: клиент не задан")
	}
	raw, err := s.RDB.Info(ctx, "server").Result()
	if err != nil {
		return RedisUptime{}, fmt.Errorf("monitor: INFO server: %w", err)
	}
	return parseRedisUptimeINFO(raw)
}

// parseRedisUptimeINFO разбирает ответ INFO: строки «ключ:значение»,
// заголовки секций («# Server») и пустые строки пропускаются; нужно поле
// uptime_in_seconds. Отсутствие поля или нечисловое/отрицательное
// значение — ошибка: формат фиксирован сервером, кривой ответ — не
// молчать (стиль parseRedisMemoryINFO). Выделен чистой функцией для
// табличного теста без Redis.
func parseRedisUptimeINFO(raw string) (RedisUptime, error) {
	fields := make(map[string]string)
	for _, line := range strings.Split(raw, "\n") {
		if k, v, ok := strings.Cut(strings.TrimRight(line, "\r"), ":"); ok {
			fields[k] = v
		}
	}
	secs, err := infoSeconds(fields, "uptime_in_seconds")
	if err != nil {
		return RedisUptime{}, err
	}
	return RedisUptime{Seconds: secs}, nil
}

// infoSeconds — числовое поле секунд из разобранного INFO
// (неотрицательное; парный infoBytes — байты, redismem.go).
func infoSeconds(fields map[string]string, key string) (int64, error) {
	v, ok := fields[key]
	if !ok {
		return 0, fmt.Errorf("monitor: INFO server: поле %s отсутствует", key)
	}
	n, err := strconv.ParseInt(v, 10, 64)
	if err != nil || n < 0 {
		return 0, fmt.Errorf("monitor: INFO server: %s=%q не число секунд", key, v)
	}
	return n, nil
}
