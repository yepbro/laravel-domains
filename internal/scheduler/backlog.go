// Scheduler: коллектор глубины backlog (T-077, ТЗ 5.9): периодический
// подсчёт строк domains по check_state → gauge crawler_backlog_domains
// {state=pending|queued}.
//
// Архитектурное решение (где живёт фоновый SQL-опрос): в сервисе
// scheduler — он владелец состояний domains (переводит pending → queued
// в EnqueueBatch T-028, возвращает queued → pending по lease T-029 и
// done → pending перепроверками T-030/T-031), имеет пул БД с самого
// старта и не занят обменами; monitor не трогается (его территория —
// счётчики зон T-078 и алерты T-080). Длина стримов q:* — отдельная
// метрика crawler_queue_depth у потребителей (internal/metrics/
// collect.go); backlog из БД — «истинная» глубина: Redis-очередь
// усечётся MAXLEN, потеряет задачи при сбое (восстановит планировщик,
// ТЗ 3.4), а pending/queued в БД — полный объём невыполненной работы,
// включая строки, ещё не попавшие в очередь.
//
// Счётчики отслеживают текущее состояние (gauge), а не наращиваются
// (counter): backlog — уровень, а не событие; алерты T-080 (остановка
// прогресса, переполнение) читают уровень. deleted исключаются (§13.1 —
// удалённые не проверяются), done не считается: прогресс по зонам —
// счётчики domains_checked (monitor, T-078).
package scheduler

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"crawler/internal/db"
	"crawler/internal/metrics"
	"crawler/internal/model"
)

// DefaultBacklogInterval — период опроса: 30 с, нижняя граница коридора
// ТЗ 5.9 (30–60 с) — алертам остановки прогресса (T-080) нужна свежесть,
// а проход — SELECT-only группировкой (без UPDATE пересчёта зон T-078)
// и стоит не дороже одной агрегации по партициям.
const DefaultBacklogInterval = 30 * time.Second

// backlogSQL — один проход: счётчики строк по check_state живых доменов.
// GROUP BY по ключу партиционирования (zone_id в листах HASH-субпартиций
// T-005 входит в PK) агрегируется по партициям; deleted исключены —
// невыполненной работой они не являются (§13.1).
const backlogSQL = "SELECT check_state, count(*) FROM domains " +
	"WHERE status <> $1 GROUP BY check_state"

// BacklogOptions — параметры цикла; нулевые значения заменяются
// дефолтами (withDefaults, стиль EnqueueOptions).
type BacklogOptions struct {
	// Interval — период цикла BacklogLoop (DefaultBacklogInterval).
	Interval time.Duration
	// Log — логгер цикла; nil — записи подавлены (slog.DiscardHandler).
	Log *slog.Logger
}

func (o BacklogOptions) withDefaults() BacklogOptions {
	if o.Interval <= 0 {
		o.Interval = DefaultBacklogInterval
	}
	return o
}

// BacklogResult — итог одного прохода BacklogOnce.
type BacklogResult struct {
	// Pending — домены в ожидании выборки планировщиком (check_state =
	// pending): весь накопленный объём работы цикла.
	Pending int64
	// Queued — домены в работе (check_state = queued, действует lease):
	// поставлены в очередь или обрабатываются этапами.
	Queued int64
}

// NewBacklogGauge создаёт gauge глубины backlog crawler_backlog_domains
// {state} в реестре reg (метка service подставится фабрикой); nil — nil.
// Идемпотентен (T-076).
func NewBacklogGauge(reg *metrics.Registry) *prometheus.GaugeVec {
	if reg == nil {
		return nil
	}
	return reg.MustGauge(prometheus.GaugeOpts{
		Name: "crawler_backlog_domains",
		Help: "Глубина backlog из БД: домены в ожидании выборки (pending) и в работе с lease (queued), без удалённых.",
	}, metrics.LabelType)
}

// BacklogOnce — один проход: GROUP BY check_state живых доменов → gauge
// обеих серий (отсутствующее в агрегате состояние — 0: пустой backlog
// обязан экспортироваться, а не пропадать до следующего непустого).
func BacklogOnce(ctx context.Context, pool db.DB, gauge *prometheus.GaugeVec) (BacklogResult, error) {
	if pool == nil {
		return BacklogResult{}, errors.New("scheduler: глубина backlog: пул не задан")
	}
	if gauge == nil {
		return BacklogResult{}, errors.New("scheduler: глубина backlog: gauge не задан")
	}

	rows, err := pool.Query(ctx, backlogSQL, int16(model.StatusDeleted))
	if err != nil {
		return BacklogResult{}, fmt.Errorf("scheduler: глубина backlog: %w", err)
	}
	defer rows.Close()

	var out BacklogResult
	for rows.Next() {
		var (
			state int16
			n     int64
		)
		if err := rows.Scan(&state, &n); err != nil {
			return BacklogResult{}, fmt.Errorf("scheduler: глубина backlog: чтение: %w", err)
		}
		switch model.CheckState(state) {
		case model.CheckStatePending:
			out.Pending = n
		case model.CheckStateQueued:
			out.Queued = n
		}
	}
	if err := rows.Err(); err != nil {
		return BacklogResult{}, fmt.Errorf("scheduler: глубина backlog: %w", err)
	}

	gauge.WithLabelValues(model.CheckStatePending.String()).Set(float64(out.Pending))
	gauge.WithLabelValues(model.CheckStateQueued.String()).Set(float64(out.Queued))
	return out, nil
}

// BacklogLoop — периодический цикл проходов: первый сразу при старте
// (свежая глубина после простоя/рестарта), далее по ticker. Ошибка
// прохода прерывает только его: логируется Warn, цикл живёт (контракт
// OnRun, T-012) — очередной проход повторит; gauge тем временем отдаёт
// последние известные значения. Отмена ctx — чистый nil (graceful
// shutdown, ТЗ 3.4). nil-реестр/пул — ошибка старта: коллектор без
// точек записи бессмыслен (стиль NewRecorder T-058).
func BacklogLoop(ctx context.Context, pool db.DB, reg *metrics.Registry, opts BacklogOptions) error {
	gauge := NewBacklogGauge(reg)
	if pool == nil {
		return errors.New("scheduler: цикл backlog: пул не задан")
	}
	if gauge == nil {
		return errors.New("scheduler: цикл backlog: реестр метрик не задан")
	}
	o := opts.withDefaults()
	log := o.Log
	if log == nil {
		log = slog.New(slog.DiscardHandler)
	}
	log.Info("цикл глубины backlog", "interval", o.Interval.String())

	pass := func() {
		res, err := BacklogOnce(ctx, pool, gauge)
		switch {
		case err != nil && ctx.Err() != nil:
			// Отмена во время прохода — часть остановки, не тревога.
		case err != nil:
			log.Warn("проход глубины backlog", "err", err)
		default:
			log.Debug("проход глубины backlog",
				"pending", res.Pending, "queued", res.Queued)
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
