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
//   - trim.go — освобождение подтверждённого префикса стримов (T-141,
//     ТЗ 4.2 «Дополнение 2026-10-23»): XTRIM MINID по старейшей записи,
//     обязанной остаться (старейшая pending группы / last-delivered-id), —
//     подтверждённая история не копится до потолка MAXLEN и бюджета
//     Redis (ТЗ 3.2).
//   - consumers.go — гигиена консьюмеров групп (T-155): XGROUP
//     DELCONSUMER строго пустых (pending = 0) и неактивных (idle ≥
//     redis.consumer_idle) консьюмеров, накопившихся после рестартов
//     воркеров; pending-записи не трогаются (DELCONSUMER с pending
//     теряет их навсегда).
//
// Backpressure по длине очереди — T-026. Живой Redis в юнит-тестах
// не используется (интеграционные тесты очередей — T-032).
package queue

import (
	"github.com/redis/go-redis/v9"
)

// Имена стримов Redis Streams (ТЗ 4.2).
const (
	StreamDNS            = "q:dns"             // пачка доменов (id + имя) → dns-worker
	StreamHTTP           = "q:http"            // домен с DNS-живыми субдоменами → http-worker
	StreamRender         = "q:render"          // результат для рендера → render-worker
	StreamRenderFallback = "q:render-fallback" // результат, не прошедший рендер на основном движке, → chromium-воркер B (ТЗ 4.3, ФТ-6)
	StreamAnalyze        = "q:analyze"         // результат на анализ → analyzer
	StreamDead           = "q:dead"            // задачи, исчерпавшие попытки (ручной разбор)
)

// allStreams — все стримы очереди: единственный источник имён, по нему
// строятся knownStream (принадлежность очереди) и workingStreams
// (освобождение подтверждённого префикса, trim.go) — новый стрим
// добавляется одной строкой и сразу известен всей очереди.
var allStreams = []string{StreamDNS, StreamHTTP, StreamRender, StreamRenderFallback, StreamAnalyze, StreamDead}

// AllStreams — копия полного списка стримов очереди (allStreams) для
// потребителей наблюдаемости: перечень вывода status/crawlerctl, отчёт
// нагрузки монитора. Один источник вместо расходящихся копий (S-P3-1:
// q:render-fallback T-237 выпадал из сводок, пока списки писались
// руками): стрим, добавленный в allStreams, автоматически виден во всех
// перечнях наблюдаемости. Подмножества с иной семантикой (alertStreams —
// только стримы, ограниченные гейтом backpressure) остаются у
// владельцев. Копия, а не срез: список — значение, вызывающий не может
// изменить константы пакета.
func AllStreams() []string {
	return append([]string(nil), allStreams...)
}

// NewClient создаёт клиент Redis по адресу addr (например "127.0.0.1:6379";
// секция redis конфигурации, T-006). Пул подключений и таймауты — настройки
// go-redis по умолчанию. Клиент передаётся NewRedisQueue; закрытие —
// забота владельца клиента (каркас сервиса).
func NewClient(addr string) *redis.Client {
	return redis.NewClient(&redis.Options{Addr: addr})
}
