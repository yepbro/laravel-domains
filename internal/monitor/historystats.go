// Счётчики истории детекции Laravel (T-118): кумулятивные метрики
// crawler_laravel_appeared_total / crawler_laravel_lost_total.
// События (строки append-only журнала laravel_history, пишет analyzer
// при свежих эпизодах HTTP) инкрементируют строки crawler_counters
// той же транзакцией (T-178, analyzer/aggregate.go), авторитетное
// значение периодически перезаписывает совмещённая реконсиляция
// (zonestats.go, агрегат laravel_history) — часовой seq scan по
// журналу ушёл из цикла, проход читает две точки PK (ТЗИ §8.5).
// gauge с counter-семантикой корректен для rate()/increase() в
// Prometheus (значения монотонны; решение T-118 — метрики отнесены
// internal/monitor, а не реестру analyzer).
package monitor

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"crawler/internal/db"
	"crawler/internal/metrics"
)

// DefaultHistoryStatsInterval — период цикла HistoryStatsLoop: 1 ч
// (свежесть кумулятивов между реконсиляциями; проход — точечные чтения,
// интервал задаёт не цену, а частоту Debug-строк).
const DefaultHistoryStatsInterval = time.Hour

// historyStatsSQL — чтение обеих строк счётчиков одним оператором:
// FILTER-позиции вместо двух Lookup; отсутствующая строка (до первой
// реконсиляции) честно даёт 0 — как пустой журнал раньше.
const historyStatsSQL = "SELECT coalesce(max(value) FILTER (WHERE name = 'laravel_appeared_total'), 0), " +
	"coalesce(max(value) FILTER (WHERE name = 'laravel_lost_total'), 0) " +
	"FROM crawler_counters WHERE name IN ('laravel_appeared_total', 'laravel_lost_total')"

// HistoryStatsOptions — параметры цикла HistoryStatsLoop; нулевые значения
// заменяются дефолтами (стиль NearStatsOptions).
type HistoryStatsOptions struct {
	// Interval — период цикла (DefaultHistoryStatsInterval).
	Interval time.Duration
	// Log — логгер цикла; nil — записи подавлены (slog.DiscardHandler).
	Log *slog.Logger
}

func (o HistoryStatsOptions) withDefaults() HistoryStatsOptions {
	if o.Interval <= 0 {
		o.Interval = DefaultHistoryStatsInterval
	}
	return o
}

// HistoryStats — итог одного прохода: кумулятивные числа переходов.
type HistoryStats struct {
	Appeared int64
	Lost     int64
}

// HistoryStatsOnce выполняет один проход: числа appeared/lost одним
// агрегатом (стиль NearStatsOnce — db.DB без QueryRow). Идемпотентен
// (ТЗ 3.4): журнал append-only, повтор даёт те же значения.
func HistoryStatsOnce(ctx context.Context, pool db.DB) (HistoryStats, error) {
	if pool == nil {
		return HistoryStats{}, errors.New("monitor: счётчики истории детекции: пул не задан")
	}
	rows, err := pool.Query(ctx, historyStatsSQL)
	if err != nil {
		return HistoryStats{}, fmt.Errorf("monitor: счётчики истории детекции: %w", err)
	}
	defer rows.Close()
	var st HistoryStats
	if !rows.Next() {
		return HistoryStats{}, fmt.Errorf("monitor: счётчики истории детекции: пустой ответ агрегата")
	}
	if err := rows.Scan(&st.Appeared, &st.Lost); err != nil {
		return HistoryStats{}, fmt.Errorf("monitor: счётчики истории детекции: чтение: %w", err)
	}
	if err := rows.Err(); err != nil {
		return HistoryStats{}, fmt.Errorf("monitor: счётчики истории детекции: %w", err)
	}
	return st, nil
}

// NewHistoryGauges создаёт пару gauge crawler_laravel_appeared_total /
// crawler_laravel_lost_total в реестре reg (метка service подставит
// фабрика); nil — nil: сервис без реестра цикл не поднимает.
// Идемпотентны (T-076). Имена с суффиксом _total — counter-семантика
// (значения монотонны), тип gauge — значения задаёт проход из БД.
func NewHistoryGauges(reg *metrics.Registry) (appeared, lost *prometheus.GaugeVec) {
	if reg == nil {
		return nil, nil
	}
	appeared = reg.MustGauge(prometheus.GaugeOpts{
		Name: "crawler_laravel_appeared_total",
		Help: "Кумулятивное число появлений строк laravel_domains (laravel_history appeared, T-118): монотонный счётчик из append-only журнала.",
	})
	lost = reg.MustGauge(prometheus.GaugeOpts{
		Name: "crawler_laravel_lost_total",
		Help: "Кумулятивное число исчезновений строк laravel_domains (laravel_history NOT appeared, T-118): монотонный счётчик из append-only журнала.",
	})
	return appeared, lost
}

// mutator-disable-regexp log\.Info\("цикл statement/remove
// mutator-disable-regexp log\.Debug\("проход statement/remove
// Обоснование: служебные записи цикла истории (старт Info, итог
// успешного прохода Debug) — поведение счётчиков не меняют, гасятся по
// docs/mutation-testing.md §4 (логирование не тестируется).

// HistoryStatsLoop — периодический цикл: HistoryStatsOnce по ticker,
// значения — в пару gauge. Первый проход — сразу при старте (свежие
// значения после простоя/рестарта); ошибка прохода прерывает только его
// (Warn, цикл живёт — контракт OnRun, T-012), gauge хранит последнее
// успешное значение. Отмена ctx — чистый nil (graceful shutdown, ТЗ 3.4).
// nil gauge/пула — no-op (стиль NearStatsLoop).
func HistoryStatsLoop(ctx context.Context, pool db.DB, reg *metrics.Registry, opts HistoryStatsOptions) error {
	appearedG, lostG := NewHistoryGauges(reg)
	// Пара gauge рождается и гибнет вместе: NewHistoryGauges возвращает
	// (nil, nil) при nil-реестре или два живых указателя — условия
	// appearedG == nil и lostG == nil неотделимы ни на одном входе,
	// мутанты-близнецы `false ||` эквивалентны (дубль — страховка
	// будущих правок конструктора).
	// mutator-disable-next-line expression/remove
	if appearedG == nil || lostG == nil || pool == nil {
		return nil
	}
	o := opts.withDefaults()
	log := o.Log
	if log == nil {
		log = slog.New(slog.DiscardHandler)
	}
	log.Info("цикл счётчиков истории детекции Laravel",
		"interval", o.Interval.String())

	pass := func() {
		st, err := HistoryStatsOnce(ctx, pool)
		switch {
		// Тело ветки пустое (только комментарий): branch/case подменяет
		// его на noop-EmptyStmt, печать которого идентична оригиналу, —
		// мутант с пустым диффом, тестом не убить; подавление генерации —
		// единственная альтернатива блэклисту чексумм (§4).
		// mutator-disable-next-line branch/case
		case err != nil && ctx.Err() != nil:
			// Отмена во время прохода — часть остановки, не тревога.
		case err != nil:
			log.Warn("проход счётчиков истории детекции", "err", err)
		default:
			appearedG.WithLabelValues().Set(float64(st.Appeared))
			lostG.WithLabelValues().Set(float64(st.Lost))
			log.Debug("проход счётчиков истории детекции",
				"appeared", st.Appeared, "lost", st.Lost)
		}
	}

	pass()
	ticker := time.NewTicker(o.Interval)
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
