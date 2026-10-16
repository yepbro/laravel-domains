// Package queue — каркас очередей задач на Redis Streams (ТЗ 4.2).
//
// Реализация интерфейса Queue с consumer groups, MAXLEN, подтверждением
// XACK только после записи результата в БД и возвратом зависших сообщений
// через XAUTOCLAIM — задачи T-024…T-026. Сейчас пакет фиксирует имена
// стримов и способ создания клиента Redis.
package queue

import (
	"github.com/redis/go-redis/v9"
)

// Имена стримов Redis Streams (ТЗ 4.2).
const (
	StreamDNS     = "q:dns"     // пачка доменов (id + имя) → dns-worker
	StreamHTTP    = "q:http"    // домен с DNS-живыми субдоменами → http-worker
	StreamRender  = "q:render"  // результат для рендера → render-worker
	StreamAnalyze = "q:analyze" // результат на анализ → analyzer
	StreamDead    = "q:dead"    // задачи, исчерпавшие попытки (ручной разбор)
)

// NewClient создаёт клиент Redis по адресу addr (например "127.0.0.1:6379").
// Пул подключений и таймауты будут настраиваться конфигом (T-006).
func NewClient(addr string) *redis.Client {
	return redis.NewClient(&redis.Options{Addr: addr})
}
