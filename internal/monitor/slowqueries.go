// Обнаружение медленных запросов PostgreSQL (T-177, ТЗ 5.9): цикл
// monitor.slow_query_interval (по умолчанию 10 с) опрашивает
// pg_stat_activity — клиентские операторы, выполняемые дольше
// monitor.slow_query_min_duration (по умолчанию 3 с — порог боевого
// лога log_min_duration_statement, deploy/postgres/postgresql.conf),
// уходит одним сообщением в Telegram (Notify-хук — TelegramNotifier.Send,
// та же доставка, что алерты T-162 и отчёт нагрузки T-173), дублируется
// Warn в журнал monitor и метриками crawler_pg_slow_queries*.
//
// Две наблюдательные поверхности не дублируют друг друга:
//
//   - журнал сервера (log_min_duration_statement) — полный посмертный
//     след: каждый оператор дольше порога с текстом и длительностью,
//     ничего не пропускает, но оператор его не читает;
//   - этот цикл — оперативный сигнал: запрос, который выполняется
//     прямо сейчас дольше порога. Опрос мгновенный снимок, а не поток
//     событий — короче интервала цикла эпизоды ловятся частично (3–10 с
//     при периоде 10 с), дольше — всегда: патология этой нагрузки
//     (точечные индексные запросы конвейера) начинается с десятков
//     секунд, 3 с — чувствительность к самому факту деградации.
//
// Дебаунс — пара (pid, query_start): оператор сообщается один раз при
// первом наблюдении; пока выполняется — молчит в следующих проходах.
// query_start уникален моменту старта оператора (повторное исполнение —
// новое значение), исчезнувший из снимка завершившийся оператор из базы
// дебаунса уходит: карта хранит только выполняющиеся сейчас (десятки
// записей максимум — лимит соединений 50). Сбой источника базу не
// роняет: возврат снимка не рассылает повторно то, что не заканчивалось.
//
// Собственные проходы monitor исключены application_name пула
// («monitor», internal/service): реконсиляция — UPDATE-агрегат по
// 47 млн строк (замер §12, десятки секунд каждые 30 мин, T-199) —
// штатная известная нагрузка, а не патология; проходы алертов
// (каждые 5 мин, T-206) и рендер-долга — точечные чтения
// (T-178/T-182). Чужие
// сервисы помечены своим application_name и в сообщении читаются по
// метке.
// Роль БД нуждается в pg_read_all_stats (deploy.md §4.1): без гранта
// pg_stat_activity чужие сессии видны, но текст запроса скрыт.
package monitor

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strconv"
	"strings"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"crawler/internal/db"
	"crawler/internal/metrics"
)

// DefaultSlowQueryInterval — период опроса pg_stat_activity: 10 с.
// Снимок представления — микросекунды (≤ 50 соединений), частота ничего
// не стоит; дольше — теряются эпизоды короче интервала.
const DefaultSlowQueryInterval = 10 * time.Second

// DefaultSlowQueryMinDuration — порог медленного запроса: 3 с, порог
// боевого log_min_duration_statement (deploy/postgres/postgresql.conf):
// одна граница «патология» в журнале сервера и в Telegram.
const DefaultSlowQueryMinDuration = 3 * time.Second

// slowQueryLimit — потолок строк одного снимка (max_connections = 50 —
// больше и не бывает; лимит страхует от раздутого сообщения).
const slowQueryLimit = 20

// slowQueryTopLines — строк запросов в сообщении: три самых долгих,
// хвост — «…и ещё N». Шторм блокировок виден числом, текст верхних
// ведёт к причине, группа не тонет в простынях.
const slowQueryTopLines = 3

// slowQueryMaxChars — обрезка текста запроса в сообщении: начало
// оператора (UPDATE "domains" …) достаточно для опознания, полный текст
// — в журнале сервера; 4096 символов лимита Telegram не приближаем.
const slowQueryMaxChars = 160

// SlowQuery — выполняемый дольше порога оператор чужой сессии.
type SlowQuery struct {
	// PID — бэкенд PostgreSQL (pg_cancel_backend/pg_terminate_backend
	// оператора адресуют именно его).
	PID int
	// App — application_name сессии (метка сервиса конвейера; пустая —
	// соединение без метки: psql, миграции со своим тегом).
	App string
	// Seconds — возраст оператора на момент снимка (now() − query_start).
	Seconds float64
	// StartedAt — query_start: момент старта оператора, половина ключа
	// дебаунса (уникален исполнению).
	StartedAt time.Time
	// Query — текст оператора (NULL без pg_read_all_stats — пустая
	// строка, сообщение честно показывает «текст недоступен»).
	Query string
}

// key — идентификатор выполнения оператора для дебаунса.
func (q SlowQuery) key() string {
	return strconv.Itoa(q.PID) + "@" + strconv.FormatInt(q.StartedAt.UnixNano(), 10)
}

// slowQuerySQL — снимок выполняемых дольше порога клиентских операторов,
// самые долгие первыми. Свой PID исключён (сам опрос — active-запрос);
// $1 — application_name пула monitor (” — фильтр выключен), $2 — порог
// в секундах, $3 — потолок строк.
const slowQuerySQL = "SELECT pid, COALESCE(application_name, ''), " +
	"EXTRACT(epoch FROM now() - query_start), query_start, COALESCE(query, '') " +
	"FROM pg_stat_activity " +
	"WHERE backend_type = 'client backend' " +
	"AND state = 'active' " +
	"AND query_start IS NOT NULL " +
	"AND pid <> pg_backend_pid() " +
	"AND ($1 = '' OR application_name <> $1) " +
	"AND EXTRACT(epoch FROM now() - query_start) > $2 " +
	"ORDER BY now() - query_start DESC " +
	"LIMIT $3"

// SlowQuerySource — источник медленных запросов; интерфейс для подмены
// в юнит-тестах (реальное чтение — pg_stat_activity живого PostgreSQL).
// Стиль LoadSource/UnitEventsSource.
type SlowQuerySource interface {
	// SlowQueries возвращает выполняемые дольше longerThan операторы.
	// Ошибка — «не измеряется» с причиной.
	SlowQueries(ctx context.Context, longerThan time.Duration) ([]SlowQuery, error)
}

// PGSlowQueries — SlowQuerySource поверх пула БД monitor: один SELECT
// pg_stat_activity на проход.
type PGSlowQueries struct {
	// Pool — пул БД; nil — ошибка источника.
	Pool db.DB
	// ExcludeApp — application_name соединений мониторинга (свои
	// агрегаты — штатная нагрузка, не патология); "" — не исключать.
	ExcludeApp string
	// Limit — потолок строк снимка (0 — slowQueryLimit).
	Limit int
}

// compile-time: PGSlowQueries — SlowQuerySource.
var _ SlowQuerySource = PGSlowQueries{}

// SlowQueries снимает выполняемые дольше порога операторы. NULL текста
// (нет pg_read_all_stats) читается пустой строкой: длительность и метка
// сессии честны, текст в сообщении помечается недоступным.
func (s PGSlowQueries) SlowQueries(ctx context.Context, longerThan time.Duration) ([]SlowQuery, error) {
	if s.Pool == nil {
		return nil, errors.New("monitor: медленные запросы: пул не задан")
	}
	limit := s.Limit
	if limit <= 0 {
		limit = slowQueryLimit
	}
	rows, err := s.Pool.Query(ctx, slowQuerySQL, s.ExcludeApp, longerThan.Seconds(), limit)
	if err != nil {
		return nil, fmt.Errorf("monitor: медленные запросы: %w", err)
	}
	defer rows.Close()
	var out []SlowQuery
	for rows.Next() {
		var q SlowQuery
		if err := rows.Scan(&q.PID, &q.App, &q.Seconds, &q.StartedAt, &q.Query); err != nil {
			return nil, fmt.Errorf("monitor: медленные запросы: чтение: %w", err)
		}
		out = append(out, q)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("monitor: медленные запросы: %w", err)
	}
	return out, nil
}

// SlowQueryOptions — параметры цикла; Interval <= 0 выключает цикл
// (стиль LoadReportOptions: у частоты опроса нет «верного» значения —
// выбор оператора, конфиг monitor.slow_query_interval).
type SlowQueryOptions struct {
	// Interval — период опроса (DefaultSlowQueryInterval).
	Interval time.Duration
	// MinDuration — порог медленного запроса
	// (DefaultSlowQueryMinDuration; 0 — дефолт).
	MinDuration time.Duration
	// Log — логгер цикла; nil — записи подавлены (slog.DiscardHandler).
	Log *slog.Logger
	// Metrics — реестр сервиса; nil — без метрик.
	Metrics *metrics.Registry
	// Notify — получатель сообщения (Telegram-доставка T-162/T-173);
	// nil — только журнал. Вызывается в горутине прохода — обязан быть
	// безопасен для конкурентного вызова (TelegramNotifier.Send).
	Notify func(text string)
}

// withDefaults заменяет неположительный порог дефолтом; Interval
// проверяет сам цикл (0 — выключено) — здесь дефолта нет.
func (o SlowQueryOptions) withDefaults() SlowQueryOptions {
	if o.MinDuration <= 0 {
		o.MinDuration = DefaultSlowQueryMinDuration
	}
	return o
}

// SlowQueryLoop — цикл медленных запросов (T-177): проход сразу при
// старте (запрос, уже выполняющийся дольше порога в момент рестарта
// monitor, — событие, а не состояние: сообщается) и далее по ticker.
// Ошибка источника — Warn, цикл живёт (контракт OnRun, ТЗ 3.4): снапшот
// повторится следующим проходом, база дебаунса не роняется. Отмена ctx —
// чистый nil (graceful shutdown).
func SlowQueryLoop(ctx context.Context, src SlowQuerySource, opts SlowQueryOptions) error {
	if src == nil {
		return errors.New("monitor: медленные запросы: источник не задан")
	}
	if opts.Interval <= 0 {
		return nil // выключен конфигурацией (monitor.slow_query_interval)
	}
	o := opts.withDefaults()
	log := o.Log
	if log == nil {
		log = slog.New(slog.DiscardHandler)
	}
	m := newSlowQueryMetrics(o.Metrics)
	log.Info("цикл медленных запросов",
		"interval", o.Interval.String(), "порог", o.MinDuration.String())

	// seen — выполняющиеся сейчас (ключ pid@query_start): завершившийся
	// оператор уходит из снимка и базы разом, память карты — десятки
	// записей на пике. Проходы последовательны в горутине цикла —
	// мьютекс не нужен (стиль loadPassState).
	seen := make(map[string]struct{})
	pass := func() {
		queries, err := src.SlowQueries(ctx, o.MinDuration)
		if err != nil {
			if ctx.Err() == nil {
				log.Warn("проход медленных запросов", "err", err)
			}
			return
		}
		cur := make(map[string]struct{}, len(queries))
		fresh := make([]SlowQuery, 0, len(queries))
		for _, q := range queries {
			k := q.key()
			cur[k] = struct{}{}
			if _, ok := seen[k]; !ok {
				fresh = append(fresh, q)
			}
		}
		seen = cur

		m.observe(len(queries), fresh)
		if len(fresh) == 0 {
			return
		}
		report := formatSlowQueryReport(fresh, len(queries), o.MinDuration)
		log.Warn("медленные запросы",
			"выполняются", len(queries), "новых", len(fresh),
			"report", strings.ReplaceAll(report, "\n", " | "))
		if o.Notify != nil {
			o.Notify(report)
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

// formatSlowQueryReport — сообщение оператору (Telegram/журнал):
// шапка с числом новых и порогом (длящиеся с прошлых проходов —
// дополнением «всего в работе»), slowQueryTopLines самых долгих строк
// «длительность [сервис] pid: текст», хвост — «…и ещё N». Снимок уже
// отсортирован сервером по длительности; fresh — его новые элементы,
// порядок сохранён.
func formatSlowQueryReport(fresh []SlowQuery, total int, threshold time.Duration) string {
	var b strings.Builder
	fmt.Fprintf(&b, "🐌 PostgreSQL: %d %s (порог %sс)",
		len(fresh), slowQueriesPhrase(len(fresh)), slowDurString(threshold))
	if total > len(fresh) {
		fmt.Fprintf(&b, ", всего в работе %d", total)
	}
	b.WriteByte('\n')
	for i, q := range fresh {
		if i == slowQueryTopLines {
			fmt.Fprintf(&b, "…и ещё %d\n", len(fresh)-i)
			break
		}
		fmt.Fprintf(&b, "• %.1fс", q.Seconds)
		if q.App != "" {
			fmt.Fprintf(&b, " [%s]", q.App)
		}
		fmt.Fprintf(&b, " pid %d: %s\n", q.PID, slowQueryText(q.Query))
	}
	return b.String()
}

// slowQueriesPhrase — согласование «медленный запрос / медленных
// запроса / медленных запросов» со счётом (21 медленный запрос).
func slowQueriesPhrase(n int) string {
	switch {
	case n%10 == 1 && n%100 != 11:
		return "медленный запрос"
	case n%10 >= 2 && n%10 <= 4 && (n%100 < 10 || n%100 >= 20):
		return "медленных запроса"
	default:
		return "медленных запросов"
	}
}

// slowDurString — длительность порога целыми секундами без дробного
// хвоста («3с», не «3.000с»); нецелые (2.5с) — как есть.
func slowDurString(d time.Duration) string {
	sec := d.Seconds()
	if sec == float64(int64(sec)) {
		return strconv.FormatInt(int64(sec), 10)
	}
	return strconv.FormatFloat(sec, 'g', -1, 64)
}

// slowQueryText — одна строка текста оператора: пробелы схлопнуты,
// хвост длиннее slowQueryMaxChars обрезан многоточием (полный текст —
// журнал сервера). Пустой текст (нет pg_read_all_stats) — пометка.
func slowQueryText(q string) string {
	if strings.TrimSpace(q) == "" {
		return "(текст недоступен — нет pg_read_all_stats)"
	}
	q = strings.Join(strings.Fields(q), " ")
	if r := []rune(q); len(r) > slowQueryMaxChars {
		return string(r[:slowQueryMaxChars]) + "…"
	}
	return q
}

// slowQueryMetrics — приборы цикла: gauge текущих медленных (снимок
// прохода; hold-last при сбое источника — стиль psimetrics) и счётчик
// обнаруженных с меткой сервиса; nil-приёмник — no-op (цикл без
// реестра, стиль zoneStatsLoopMetrics).
type slowQueryMetrics struct {
	current *prometheus.GaugeVec
	total   *prometheus.CounterVec
}

// slowQueryAppUnknown — метка app соединений без application_name.
const slowQueryAppUnknown = "unknown"

// newSlowQueryMetrics — приборы в реестре reg (nil — nil).
func newSlowQueryMetrics(reg *metrics.Registry) *slowQueryMetrics {
	if reg == nil {
		return nil
	}
	return &slowQueryMetrics{
		current: reg.MustGauge(prometheus.GaugeOpts{
			Name: "crawler_pg_slow_queries",
			Help: "Клиентские запросы PostgreSQL, выполняемые дольше monitor.slow_query_min_duration на момент прохода цикла (pg_stat_activity, T-177); собственные агрегаты monitor исключены.",
		}),
		total: reg.MustCounter(prometheus.CounterOpts{
			Name: "crawler_pg_slow_queries_total",
			Help: "Обнаруженные медленные запросы по application_name сервиса (новые операторы дольше порога, дебаунс pid+query_start, T-177).",
		}, "app"),
	}
}

// observe публикует снимок прохода; nil — no-op.
func (m *slowQueryMetrics) observe(current int, fresh []SlowQuery) {
	if m == nil {
		return
	}
	m.current.WithLabelValues().Set(float64(current))
	for _, q := range fresh {
		app := q.App
		if app == "" {
			app = slowQueryAppUnknown
		}
		m.total.WithLabelValues(app).Inc()
	}
}
