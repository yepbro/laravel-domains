// Package queue — очередь задач на Redis Streams с consumer groups
// (ТЗ 4.2).
//
// Состав пакета:
//
//   - queue.go — имена стримов и создание клиента Redis;
//   - stream.go — интерфейс Queue (транспорт заменяем: допустима замена
//     на RabbitMQ за тем же интерфейсом, ТЗ 4.2) и Delivery;
//   - message.go — форматы сообщений стримов: только идентификаторы и
//     минимальные данные (имя домена), JSON в единственном поле data;
//   - redisq.go — реализация RedisQueue: XADD, XREADGROUP ">", XACK,
//     ленивое создание группы (XGROUP CREATE MKSTREAM, BUSYGROUP —
//     не ошибка);
//   - reliab.go — надёжность доставки (T-025): политика усечения MAXLEN
//     (DefaultStreamMaxLen), паттерн Processing (XACK только после записи
//     результата в БД), возврат зависших записей XAUTOCLAIM со счётчиком
//     попыток (Reliab.Claim) и переводом исчерпавших попытки в q:dead.
//
// Backpressure по длине очереди — T-026. Живой Redis в юнит-тестах
// не используется (интеграционные тесты очередей — T-032).
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

// NewClient создаёт клиент Redis по адресу addr (например "127.0.0.1:6379";
// секция redis конфигурации, T-006). Пул подключений и таймауты — настройки
// go-redis по умолчанию. Клиент передаётся NewRedisQueue; закрытие —
// забота владельца клиента (каркас сервиса).
func NewClient(addr string) *redis.Client {
	return redis.NewClient(&redis.Options{Addr: addr})
}
