// Счётчик находок Laravel в серой зоне порога (T-115, по итогам анализа
// larasites 2026-10-19): laravel_findings хранит все срабатывания правил
// независимо от итога (T-071), находки с score ниже порога
// analyzer.laravel_min_score — материал калибровки весов и порога
// (T-116/T-117) и индикатор качества набора правил в бою: стабильная
// серая зона — норма (слабые признаки частых не-Laravel стеков),
// всплеск — сигнал деградации точности после смены правил.
//
// Метрика — gauge crawler_laravel_near_findings: текущее число findings
// с min_score/2 <= score < min_score (половина порога — нижняя граница
// «почти-срабатываний»: ниже шум одиночных слабых правил, аналогично
// one.log старой реализации — «хосты с единственным признаком»).
// Командная строка разбора тех же строк — crawlerctl near (T-115):
// gauge отвечает «сколько», CLI — «какие именно»; SQL обеих сторон
// держит одна семантика диапазона, текст запроса здесь — единственный
// источник правды о границах для метрики.
//
// Период — час, а не 30–60 с пересчёта зон (T-078): проход считает
// агрегат по laravel_findings (последовательное чтение партиций, на
// боевом корпусе — десятки миллионов строк), а серая зона — материал
// калибровки, не оперативный сигнал: часовая свежесть достаточна,
// цена прохода — как один проход zonestats, но в 60 раз реже.
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

// DefaultNearStatsInterval — период цикла NearStatsLoop: 1 ч
// (обоснование цены прохода — в комментарии пакета).
const DefaultNearStatsInterval = time.Hour

// nearStatsSQL — один проход: число находок серой зоны. Границы —
// параметры (real в SQL, float32 в таблице): порог читается из
// конфигурации на каждом запуске цикла, смена analyzer.laravel_min_score
// подхватывается рестартом monitor; параметр, а не литерал, потому что
// значение — конфигурация оператора, а не код схемы (в отличие от кодов
// check_state/status в проходах планировщика).
const nearStatsSQL = "SELECT count(*) FROM laravel_findings WHERE score >= $1::real AND score < $2::real"

// NearStatsOptions — параметры цикла NearStatsLoop; нулевые значения
// заменяются дефолтами (withDefaults, стиль ZoneStatsOptions).
type NearStatsOptions struct {
	// Interval — период цикла (DefaultNearStatsInterval).
	Interval time.Duration
	// MinScore — порог is_laravel (analyzer.laravel_min_score); серая
	// зона — [MinScore/2, MinScore). Валидируется NearStatsOnce.
	MinScore float64
	// Log — логгер цикла; nil — записи подавлены (slog.DiscardHandler).
	Log *slog.Logger
}

func (o NearStatsOptions) withDefaults() NearStatsOptions {
	if o.Interval <= 0 {
		o.Interval = DefaultNearStatsInterval
	}
	return o
}

// NearStatsOnce выполняет один проход: число находок laravel_findings в
// серой зоне [minScore/2, minScore) одним запросом (стиль
// analyzer.ZoneIDByName — db.DB без QueryRow). Идемпотентен (ТЗ 3.4):
// повтор даёт то же значение при том же состоянии таблицы.
func NearStatsOnce(ctx context.Context, pool db.DB, minScore float64) (int64, error) {
	if pool == nil {
		return 0, errors.New("monitor: счётчик серой зоны: пул не задан")
	}
	if !(minScore > 0 && minScore < 1) {
		return 0, fmt.Errorf("monitor: счётчик серой зоны: порог %v вне (0, 1)", minScore)
	}
	lo, hi := minScore/2, minScore
	rows, err := pool.Query(ctx, nearStatsSQL, float32(lo), float32(hi))
	if err != nil {
		return 0, fmt.Errorf("monitor: счётчик серой зоны: %w", err)
	}
	defer rows.Close()
	var n int64
	if !rows.Next() {
		return 0, fmt.Errorf("monitor: счётчик серой зоны: пустой ответ агрегата")
	}
	if err := rows.Scan(&n); err != nil {
		return 0, fmt.Errorf("monitor: счётчик серой зоны: чтение: %w", err)
	}
	if err := rows.Err(); err != nil {
		return 0, fmt.Errorf("monitor: счётчик серой зоны: %w", err)
	}
	return n, nil
}

// NewNearGauge создаёт gauge crawler_laravel_near_findings в реестре reg
// (метка service подставит фабрика); nil — nil: сервис без реестра
// (изолированные сборки) цикл не поднимает (стиль metrics.NewQueueGauge).
// Идемпотентен (T-076).
func NewNearGauge(reg *metrics.Registry) *prometheus.GaugeVec {
	if reg == nil {
		return nil
	}
	return reg.MustGauge(prometheus.GaugeOpts{
		Name: "crawler_laravel_near_findings",
		Help: "Находки Laravel в серой зоне порога: laravel_findings с min_score/2 <= score < min_score (материал калибровки правил, T-115).",
	})
}

// NearStatsLoop — периодический цикл пересчёта: NearStatsOnce по ticker,
// значение — в gauge. Первый проход — сразу при старте (свежее значение
// после простоя/рестарта, стиль ZoneStatsLoop); ошибка прохода
// прерывает только его (Warn, цикл живёт — контракт OnRun, T-012),
// gauge хранит последнее успешное значение. Отмена ctx — чистый nil
// (graceful shutdown, ТЗ 3.4). nil gauge/пула — no-op: запуск без
// реестра не должен падать (стиль CollectQueueLoop).
func NearStatsLoop(ctx context.Context, pool db.DB, reg *metrics.Registry, opts NearStatsOptions) error {
	gauge := NewNearGauge(reg)
	if gauge == nil || pool == nil {
		return nil
	}
	o := opts.withDefaults()
	log := o.Log
	if log == nil {
		log = slog.New(slog.DiscardHandler)
	}
	log.Info("цикл счётчика серой зоны Laravel-находок",
		"interval", o.Interval.String(), "min_score", o.MinScore)

	pass := func() {
		n, err := NearStatsOnce(ctx, pool, o.MinScore)
		switch {
		case err != nil && ctx.Err() != nil:
			// Отмена во время прохода — часть остановки, не тревога.
		case err != nil:
			log.Warn("проход счётчика серой зоны", "err", err)
		default:
			gauge.WithLabelValues().Set(float64(n))
			log.Debug("проход счётчика серой зоны", "near", n)
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
