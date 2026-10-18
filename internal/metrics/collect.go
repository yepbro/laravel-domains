// Коллектор глубины очередей (T-077, ТЗ 5.9 «длины очередей»):
// периодическая глубина рабочих стримов q:* → gauge crawler_queue_depth
// {queue}.
//
// Архитектурное решение (зафиксировано по ТЗ 5.9): коллектор живёт в
// сервисе-потребителе стрима — dns-worker меряет q:dns, http-worker —
// q:http, render-worker — q:render, analyzer — q:analyze. Глубина —
// backlog групп потребления (непрочитанный остаток lag плюс
// неподтверждённые доставки pending, источник — гейт backpressure
// T-026, который уже держит клиент Redis): это истинная «длина очереди»
// — XLEN стрима считает и подтверждённую историю до обрезки MAXLEN.
// Привязка к потребителю гарантирует: очередь живёт (читается) ⇔ её
// глубина публикуется. Стрим q:dead потребителя не имеет (ручной
// разбор, ТЗ 4.2) — его глубину показывает crawlerctl status (T-079),
// метрика появится вместе с потребителем. Источник глубины внедряется
// функцией LenFunc (метод Len гейта queue.BackpressureGate) — пакет не
// зависит от Redis.
package metrics

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

// DefaultQueueCollectInterval — период опроса глубины: 15 с — типовой
// интервал скрейпа Prometheus (dev-конфиг deploy/docker/prometheus):
// каждый скрейп видит свежее значение, чаще — лишние обращения к Redis.
const DefaultQueueCollectInterval = 15 * time.Second

// LenFunc — источник глубины стрима: удовлетворяется методом Len гейта
// backpressure (*queue.BackpressureGate, T-026) — тот же клиент Redis,
// что у очереди сервиса; юнит-тесты подменяют фейком.
type LenFunc func(ctx context.Context, stream string) (int64, error)

// NewQueueGauge создаёт gauge глубины очередей crawler_queue_depth{queue}
// в реестре reg (метка service подставится фабрикой); nil — nil:
// сервис без реестра (изолированные сборки) коллектор не поднимает.
// Идемпотентен (дубль-регистрация T-076 возвращает существующий вектор).
func NewQueueGauge(reg *Registry) *prometheus.GaugeVec {
	if reg == nil {
		return nil
	}
	return reg.MustGauge(prometheus.GaugeOpts{
		Name: "crawler_queue_depth",
		Help: "Глубина очереди задач: непрочитанный группой остаток и неподтверждённые доставки (backlog групп потребления).",
	}, LabelQueue)
}

// CollectQueueDepths — один проход коллектора: глубина каждого стрима
// streams через lenOf → Set значения gauge (метка queue — имя стрима).
// Ошибки стримов независимы: сбой одного не мешает остальным, все ошибки
// собираются в один возврат (errors.Join); gauge сбойного стрима хранит
// последнее успешное значение.
func CollectQueueDepths(ctx context.Context, gauge *prometheus.GaugeVec, lenOf LenFunc, streams []string) error {
	if gauge == nil || lenOf == nil {
		return errors.New("metrics: сбор глубины очередей: gauge и источник глубины обязательны")
	}
	var errs []error
	for _, stream := range streams {
		n, err := lenOf(ctx, stream)
		if err != nil {
			errs = append(errs, fmt.Errorf("metrics: глубина %s: %w", stream, err))
			continue
		}
		gauge.WithLabelValues(stream).Set(float64(n))
	}
	return errors.Join(errs...)
}

// CollectQueueLoop — периодический цикл сборов глубины очередей: первый
// проход сразу при старте (свежие значения после рестарта не ждут тика),
// далее — по ticker с периодом interval (0 — DefaultQueueCollectInterval).
// Ошибка прохода прерывает только его: логируется Warn, цикл живёт —
// очередной проход повторит (контракт OnRun каркаса, T-012); gauge тем
// временем отдаёт последние известные значения. Отмена ctx — чистый
// выход (graceful shutdown, ТЗ 3.4). nil-зависимости и пустой набор
// стримов — no-op: сервис без реестра не должен падать на старте.
func CollectQueueLoop(
	ctx context.Context,
	reg *Registry,
	lenOf LenFunc,
	streams []string,
	interval time.Duration,
	log *slog.Logger,
) error {
	gauge := NewQueueGauge(reg)
	if gauge == nil || lenOf == nil || len(streams) == 0 {
		return nil
	}
	if interval <= 0 {
		interval = DefaultQueueCollectInterval
	}
	if log == nil {
		log = slog.New(slog.DiscardHandler)
	}
	log.Info("коллектор глубины очередей", "streams", streams, "interval", interval.String())

	pass := func() {
		if err := CollectQueueDepths(ctx, gauge, lenOf, streams); err != nil && ctx.Err() == nil {
			log.Warn("сбор глубины очередей", "err", err)
		}
	}

	pass()
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			pass()
		}
	}
}
