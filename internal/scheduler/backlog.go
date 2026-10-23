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

// DefaultBacklogInterval — период опроса: 5 минут. Исходные 30 с (нижняя
// граница коридора ТЗ 5.9 30–60 с) рассчитаны на малый корпус: замер
// §12 docs/load-measurements.md (47 млн строк) — проход GROUP BY 17+ с
// каждые 30 с, вместе с пересчётом зон и агрегатами status — большая
// часть CPU postgres; gauge глубины на дашборде допускает пятиминутную
// свежесть (постановка T-161). Коридор ТЗ остаётся доступным конфигом
// scheduler.backlog_interval для малых корпусов.
const DefaultBacklogInterval = 5 * time.Minute

// backlogSQL — один проход: счётчики строк по check_state живых доменов.
// GROUP BY по ключу партиционирования (zone_id в листах HASH-субпартиций
// T-005 входит в PK) агрегируется по партициям; deleted исключены —
// невыполненной работой они не являются (§13.1). Индекса у прохода нет
// и быть не может (T-104): агрегат по всем строкам обоих состояний с
// фильтром status не сводится к предикату частичного индекса — цена
// метрики O(таблица) за проход, интервал задаёт баланс свежести и
// нагрузки (scheduler.backlog_interval, коридор 30с..1ч; ТЗ 5.9
// допускал 30–60 с на малом корпусе).
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
	}, metrics.LabelState)
}

// backlogLoopMetrics — приборы цикла (T-161): длительность прохода
// (наблюдаемость «проход дольше интервала» без gdb на pg_stat_activity)
// и счётчик пропущенных тиков; nil-приёмник — no-op (цикл без реестра,
// стиль monitor.collectorMetrics).
type backlogLoopMetrics struct {
	seconds prometheus.ObserverVec
	skipped *prometheus.CounterVec
}

// newBacklogLoopMetrics — приборы в реестре reg (nil — nil). Корзины —
// экспонента 0.5 с…~8.5 мин: замер §12 — проход по 47 млн строк 17+ с,
// дефолтные корзины Prometheus (потолок 10 с) малы.
func newBacklogLoopMetrics(reg *metrics.Registry) *backlogLoopMetrics {
	if reg == nil {
		return nil
	}
	return &backlogLoopMetrics{
		seconds: reg.MustHistogram(prometheus.HistogramOpts{
			Name:    "crawler_backlog_pass_seconds",
			Help:    "Длительность прохода агрегата глубины backlog (GROUP BY check_state по всем живым доменам, T-077).",
			Buckets: prometheus.ExponentialBuckets(0.5, 2, 11),
		}),
		skipped: reg.MustCounter(prometheus.CounterOpts{
			Name: "crawler_backlog_skipped_ticks_total",
			Help: "Пропущенные тики цикла backlog: проход длился дольше интервала, накопившийся тик не запускает новый проход (single-flight, T-161).",
		}),
	}
}

// observe — фиксация длительности прохода; nil — no-op.
func (m *backlogLoopMetrics) observe(seconds float64) {
	if m == nil {
		return
	}
	m.seconds.WithLabelValues().Observe(seconds)
}

// skip — пропуск тика; nil — no-op.
func (m *backlogLoopMetrics) skip() {
	if m == nil {
		return
	}
	m.skipped.WithLabelValues().Inc()
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
//
// Single-flight (T-161): проходы выполняются в горутине самого цикла —
// новый проход структурно не может начаться, пока не завершился
// предыдущий. Хвост картины «проход дольше интервала» (замер §12: 17+ с
// по 47 млн строк): ticker буферизует один тик, и накопившийся тик
// запускал бы новый проход сразу за медленным — агрегат молотил бы БД
// непрерывно. Поэтому проход дольше интервала сбрасывает накопившийся
// тик (следующий проход — по следующему тику: проходы не чаще
// интервала), Warn и счётчик crawler_backlog_skipped_total поднимают
// шум.
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
	m := newBacklogLoopMetrics(reg)
	// Мутант логирования (T-M06): стартовый Info не меняет наблюдаемого
	// поведения цикла — гасится по §4 docs/mutation-testing.md
	// (логирование не тестируется).
	// mutator-disable-next-line statement/remove
	log.Info("цикл глубины backlog", "interval", o.Interval.String())

	// Тикер до стартового прохода: тики отсчитываются от старта цикла,
	// дренаж накопившегося тика в pass видит один и тот же тикер.
	ticker := time.NewTicker(o.Interval)
	defer ticker.Stop()
	pass := func() {
		started := time.Now()
		res, err := BacklogOnce(ctx, pool, gauge)
		switch {
		// Тело ветки пустое (только комментарий): branch/case подменяет
		// его на noop-EmptyStmt с идентичной печатью — мутант с пустым
		// диффом, тестом не убить; подавление генерации — единственная
		// альтернатива блэклисту чексумм (§4 docs/mutation-testing.md;
		// прецедент — monitor/zonestats.go, T-M01).
		// mutator-disable-next-line branch/case
		case err != nil && ctx.Err() != nil:
			// Отмена во время прохода — часть остановки, не тревога.
		case err != nil:
			log.Warn("проход глубины backlog", "err", err)
		default:
			log.Debug("проход глубины backlog",
				"pending", res.Pending, "queued", res.Queued)
		}
		took := time.Since(started)
		m.observe(took.Seconds())
		if took > o.Interval {
			// Single-flight: сбросить тик, накопившийся за медленный
			// проход, — иначе новый проход стартовал бы сразу за этим
			// (select/default — тика может и не быть: Warn честен
			// по длительности, дренаж в этом случае no-op).
			select {
			case <-ticker.C:
			default:
			}
			log.Warn("проход глубины backlog дольше интервала — накопившийся тик пропущен",
				"took", took.String(), "interval", o.Interval.String())
			m.skip()
		}
	}

	pass()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			pass()
		}
	}
}
