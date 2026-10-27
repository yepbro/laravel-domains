// Рендер-долг (T-175, ТЗ 5.9): наблюдаемость накопления работы рендера
// без ручного SQL. Долг 443 тыс. (стрим q:render + флаги render_pending,
// морозка 24.10) нашёл оператор руками: счёт по битовой маске — полный
// seq scan 41,8 млн строк (десятки секунд CPU БД), алерт T-162
// «замороженная очередь» ловит остановку доставок, но не накопление —
// очередь движется и растёт одновременно.
//
// Долг = флаги + лаг:
//
//   - флаги — домены с битом render_pending (бит 1 domains.flags,
//     T-066: очередь рендера переполнена, домен дорабатывается позже) —
//     инкрементальный счётчик crawler_counters['render_pending']
//     (миграция 000015): +1/−1 в транзакции перехода бита
//     (internal/httpworker/renderq.go), чтение — точечный Lookup по PK;
//     частичный индекс (flags & 2) <> 0 отвергнут — ломает HOT массовых
//     апдейтов flags (docs/schema.md «HOT-апдейты», обоснование —
//     миграция 000015);
//   - лаг — глубина стрима q:render (непотреблённый остаток + pending,
//     формула гейта backpressure): тот же StreamLener, что у алертов
//     очередей; зеркало в метриках — crawler_queue_depth{queue="q:render"}
//     (коллектор render-worker, T-077), здесь только читается.
//
// Три потребителя одного снимка (renderDebtOf — семантика не расходится):
// цикл RenderDebtLoop (gauge + строка сводки в журнал),
// строка «Рендер-долг» отчёта нагрузки (load.go, T-173) и алерт
// накопления render_debt (alerts.go): долг выше порога И растёт
// N проходов подряд — накопление, а не разовый всплеск переполнения.
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
	"crawler/internal/queue"
)

// DefaultRenderDebtInterval — период цикла рендер-долга: 5 минут
// (прежний паритет с пересчётом зон закончился — T-199 разредил
// реконсиляцию до 30 мин). Проход — точечный Lookup по
// PK счётчика + XLEN q:render (миллисекунды — дороже не бывает), так
// что период выбирает не стоимость, а свежесть: долг — медленная
// величина (дренаж q:render ≈1–2 стр/с, замер §12, docs/render.md),
// быстрее пятиминутного окна он не меняется; коридор 30s..1h — по
// образцу zone_stats_interval. Период касается ТОЛЬКО gauge и сводки
// этого цикла: алерт render_debt оценивается проходом AlertsLoop по
// monitor.alerts_interval (ревью T-175, P3-6; T-206).
const DefaultRenderDebtInterval = 5 * time.Minute

// renderDebtFlagsSQL — чтение счётчика флагов render_pending: точечный
// Lookup по PK crawler_counters (миграция 000015). Пустой результат —
// строка счётчика отсутствует (создаёт миграция; пропасть может только
// при ручной правке таблицы, runbooks §6) — ошибка источника, а не ноль:
// молчаливый ноль делал бы gauge долга ложью.
const renderDebtFlagsSQL = "SELECT value FROM crawler_counters WHERE name = 'render_pending'"

// RenderDebt — снимок рендер-долга: флаги render_pending (счётчик) +
// лаг стрима q:render.
type RenderDebt struct {
	// Flags — домены с битом render_pending (crawler_counters).
	Flags int64
	// Lag — глубина стрима q:render (backlog группы потребления:
	// min(lag, XLEN) + pending — формула BackpressureGate.Len, T-168).
	Lag int64
}

// Total — долг целиком: флаги + лаг (работа, ждущая рендера).
func (d RenderDebt) Total() int64 { return d.Flags + d.Lag }

// String — сводка одной строкой (журнал цикла, деталь алерта, строка
// отчёта нагрузки — один и тот же текст).
func (d RenderDebt) String() string {
	return fmt.Sprintf("флаги %d + лаг %d = %d", d.Flags, d.Lag, d.Total())
}

// RenderDebtSource — источник рендер-долга; интерфейс для подмены в
// юнит-тестах (реальное чтение — БД + Redis). Стиль StreamGroupsSource.
type RenderDebtSource interface {
	// RenderDebt возвращает снимок долга. Ошибка = «не измеряется»
	// с причиной.
	RenderDebt(ctx context.Context) (RenderDebt, error)
}

// RenderDebtReader — RenderDebtSource поверх пула БД (счётчик флагов)
// и источника глубин стримов (лаг q:render).
type RenderDebtReader struct {
	// Pool — БД счётчика crawler_counters; nil — ошибка источника.
	Pool db.DB
	// Redis — глубина стрима q:render (StreamLener: гейт backpressure
	// monitor'а); nil — ошибка источника.
	Redis StreamLener
}

// compile-time: RenderDebtReader — RenderDebtSource.
var _ RenderDebtSource = RenderDebtReader{}

// RenderDebt читает обе составляющие долга. Снимок не атомарен между БД
// и Redis (два хранилища) — и не обязан: компоненты сходятся к нулю
// независимо, долю секунды между чтениями алерт роста не замечает
// (порог — десятки тысяч, окно — проходы).
func (r RenderDebtReader) RenderDebt(ctx context.Context) (RenderDebt, error) {
	return renderDebtOf(ctx, r.Pool, r.Redis)
}

// renderDebtOf — общий снимок долга: цикл gauge, отчёт нагрузки и алерт
// читают одинаково (расхождение семантики сделало бы три цифры
// взаимно противоречивыми).
func renderDebtOf(ctx context.Context, pool db.DB, redis StreamLener) (RenderDebt, error) {
	if pool == nil {
		return RenderDebt{}, errors.New("monitor: рендер-долг: пул БД не задан")
	}
	if redis == nil {
		return RenderDebt{}, errors.New("monitor: рендер-долг: источник глубины q:render не задан")
	}
	rows, err := pool.Query(ctx, renderDebtFlagsSQL)
	if err != nil {
		return RenderDebt{}, fmt.Errorf("monitor: рендер-долг: счётчик: %w", err)
	}
	defer rows.Close()
	if !rows.Next() {
		// Ошибка итерации с нулём прочитанных строк — не «строки нет»
		// (ревью T-175, P3-2: диагностика не должна уводить в runbooks
		// при сбое соединения).
		if err := rows.Err(); err != nil {
			return RenderDebt{}, fmt.Errorf("monitor: рендер-долг: счётчик: %w", err)
		}
		return RenderDebt{}, errors.New("monitor: рендер-долг: строки 'render_pending' в crawler_counters нет (миграция 000015; ручные операции — runbooks §6)")
	}
	var d RenderDebt
	if err := rows.Scan(&d.Flags); err != nil {
		return RenderDebt{}, fmt.Errorf("monitor: рендер-долг: счётчик: чтение: %w", err)
	}
	if err := rows.Err(); err != nil {
		return RenderDebt{}, fmt.Errorf("monitor: рендер-долг: счётчик: %w", err)
	}
	lag, err := redis.Len(ctx, queue.StreamRender)
	if err != nil {
		return RenderDebt{}, fmt.Errorf("monitor: рендер-долг: глубина q:render: %w", err)
	}
	d.Lag = lag
	return d, nil
}

// --------------------------------------------------------------- метрики

// renderDebtMetrics — прибор цикла: gauge crawler_render_pending_domains
// (флаги). Лаг не дублируется — его публикует коллектор render-worker
// (crawler_queue_depth{queue="q:render"}, T-077); nil-приёмник — no-op
// (цикл без реестра, стиль zoneStatsLoopMetrics).
type renderDebtMetrics struct {
	flags *prometheus.GaugeVec
}

func newRenderDebtMetrics(reg *metrics.Registry) *renderDebtMetrics {
	if reg == nil {
		return nil
	}
	return &renderDebtMetrics{
		flags: reg.MustGauge(prometheus.GaugeOpts{
			Name: "crawler_render_pending_domains",
			Help: "Рендер-долг (T-175): домены с битом render_pending (счётчик crawler_counters['render_pending'], ведут переходы бита — миграция 000015). Лаг стрима q:render — crawler_queue_depth{queue=\"q:render\"}; долг = сумма. Рост — накопление (алерт render_debt), разбор — docs/runbooks.md.",
		}),
	}
}

func (m *renderDebtMetrics) observe(d RenderDebt) {
	if m == nil {
		return
	}
	m.flags.WithLabelValues().Set(float64(d.Flags))
}

// ------------------------------------------------------------------- цикл

// RenderDebtOptions — параметры цикла; нулевые значения заменяются
// дефолтами (withDefaults, стиль ZoneStatsOptions).
type RenderDebtOptions struct {
	// Interval — период цикла (DefaultRenderDebtInterval); рабочее
	// значение — monitor.render_debt_interval из конфига.
	Interval time.Duration
	// Log — логгер цикла; nil — записи подавлены (slog.DiscardHandler).
	Log *slog.Logger
	// Metrics — реестр сервиса; nil — без gauge.
	Metrics *metrics.Registry
}

func (o RenderDebtOptions) withDefaults() RenderDebtOptions {
	if o.Interval <= 0 {
		o.Interval = DefaultRenderDebtInterval
	}
	return o
}

// RenderDebtLoop — периодический цикл рендер-долга (T-175): снимок сразу
// при старте (свежий gauge после рестарта) и далее по ticker. Проход —
// миллисекунды (PK + XLEN), single-flight механика T-161 (сброс
// накопившегося тика) не нужна — проход короче любого интервала
// коридора. Ошибка источника — Warn, цикл живёт (контракт OnRun,
// T-012): очередной проход повторит. Сводка прохода — Debug (не Info:
// цикл здоров всегда, когда живы БД и Redis, — пятиминутный Info был бы
// фоном, критерий T-175 «журнал monitor не флудит»; оператору долг виден
// строкой отчёта нагрузки T-173 и алертом render_debt). Отмена ctx —
// чистый nil (graceful shutdown, ТЗ 3.4).
func RenderDebtLoop(ctx context.Context, src RenderDebtSource, opts RenderDebtOptions) error {
	if src == nil {
		return errors.New("monitor: рендер-долг: источник не задан")
	}
	o := opts.withDefaults()
	log := o.Log
	if log == nil {
		log = slog.New(slog.DiscardHandler)
	}
	m := newRenderDebtMetrics(o.Metrics)
	log.Info("цикл рендер-долга", "interval", o.Interval.String())

	pass := func() {
		d, err := src.RenderDebt(ctx)
		if err != nil {
			if ctx.Err() == nil {
				log.Warn("проход рендер-долга", "err", err)
			}
			return
		}
		m.observe(d)
		log.Debug("рендер-долг: " + d.String())
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
