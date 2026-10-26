// Package service — общий каркас Go-сервисов Domain Crawler.
//
// Каркас берёт на себя всё, что не относится к рабочей логике сервиса:
// структурированное логирование (log/slog, уровень из конфигурации),
// применение миграций и пул подключений PostgreSQL (internal/migrate,
// internal/db), HTTP-сервер с пробами /livez + /healthz и метриками
// /metrics (internal/metrics, ТЗ 4.1, 5.9; T-110) и graceful shutdown
// по ТЗ 3.4: при SIGTERM/SIGINT сначала
// отменяется контекст рабочей логики (Options.OnRun) и каркас ждёт её
// завершения, затем останавливается HTTP-сервер метрик, последним
// закрывается пул БД. Повторный сигнал прерывает ожидание — принудительный
// выход. SIGHUP — перезагрузка правил без рестарта (ТЗ 5.8). Рабочая логика
// подключается опцией OnRun (воркеры этапов 2+); сейчас сервисы-заглушки
// живут до сигнала завершения.
package service

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"runtime/debug"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"crawler/internal/config"
	"crawler/internal/db"
	"crawler/internal/metrics"
	"crawler/internal/migrate"
)

// DefaultShutdownTimeout — бюджет graceful shutdown по умолчанию: время,
// за которое рабочая логика обязана доработать текущую задачу или вернуть
// её в очередь (ТЗ 3.4).
const DefaultShutdownTimeout = 10 * time.Second

// Options — параметры запуска сервиса.
type Options struct {
	// Name — имя сервиса (ingest, analyzer, ...), используется в логах и метриках.
	Name string
	// MetricsAddr — адрес HTTP-сервера метрик, например ":9101".
	MetricsAddr string
	// Config — конфигурация сервиса (internal/config, ТЗ 5.10): загружается
	// в main через config.Load (приоритет: env > YAML > defaults). Если
	// задан, DSN PostgreSQL берётся из него.
	Config *config.Config
	// PGDSN — DSN PostgreSQL напрямую; имеет приоритет над Config
	// (используется утилитами, которым конфиг-файл не нужен). Если задан,
	// сервис перед стартом применяет встроенные миграции схемы
	// (internal/migrate): все долгоживущие сервисы проекта работают с БД
	// (ТЗ 6.3), healthcheck отвечает только на актуальной схеме. Пустая
	// строка вместе с пустым Config.Postgres.DSN — сервис без БД.
	PGDSN string
	// OnDBPool — инициализация БД-части сервиса (T-007): если задан и DSN
	// известен, каркас после применения миграций создаёт пул подключений
	// internal/db (postgres.dsn + postgres.pool_size из Config, либо
	// PGDSN с размером по умолчанию) и вызывает OnDBPool с готовым пулом.
	// Пул закрывается каркасом при graceful shutdown — последним, после
	// завершения OnRun и остановки HTTP-сервера метрик. nil — сервису пул
	// не нужен (текущие заглушки): соединения не открываются.
	OnDBPool func(pool *pgxpool.Pool)
	// DBAfterConnect — настройка каждого соединения пула БД
	// (db.AfterConnectFunc, T-171): nil — по умолчанию. Хуки сессии:
	// db.NoParallelGather — сервисы с агрегатными/сканирующими фоновыми
	// проходами (T-171 — мониторинг; T-201 — http-worker:
	// retention-дренаж), write-пулы конвейера (db.LockTimeout —
	// потолок ожидания строковых блокировок, T-192); несколько хуков —
	// композицией db.JoinHooks (перечень по сервисам — docs/db.md
	// §«AfterConnect-хуки»).
	DBAfterConnect db.AfterConnectFunc
	// OnRun — рабочая логика сервиса (воркеры этапов 2+): вызывается после
	// полной инициализации (миграции, пул, HTTP метрик). Полученный
	// контекст отменяется при SIGTERM/SIGINT — по отмене воркер дорабатывает
	// текущую задачу или возвращает её в очередь (ТЗ 3.4) и возвращается;
	// каркас ждёт возврата в пределах ShutdownTimeout. Возврат без сигнала
	// останавливает сервис: nil — чистое завершение (одноразовые задачи),
	// ошибка — завершение с кодом 1. nil — заглушка без рабочей логики:
	// сервис живёт до сигнала завершения.
	OnRun func(ctx context.Context) error
	// OnSIGHUP вызывается при получении SIGHUP (перезагрузка правил Laravel).
	OnSIGHUP func()
	// ExtraMux позволяет зарегистрировать дополнительные хендлеры.
	ExtraMux func(mux *http.ServeMux)
	// Metrics — реестр Prometheus сервиса (internal/metrics): если задан,
	// каркас отдаёт через /metrics именно его вместо собственного
	// metrics.New(Name). main создаёт реестр заранее, чтобы построить
	// приборы рабочей логики до старта OnRun (воркеры и коллекторы
	// получают его же, T-077); nil — каркас создаёт реестр сам
	// (сервисам без собственных приборов).
	Metrics *metrics.Registry
	// Logger — структурированный логгер сервиса (см. NewLogger). nil —
	// каркас создаёт логгер сам и делает его дефолтным (slog.SetDefault).
	Logger *slog.Logger
	// ShutdownTimeout — бюджет graceful shutdown (ожидание OnRun и остановка
	// HTTP-сервера метрик); 0 или отрицательное — DefaultShutdownTimeout.
	ShutdownTimeout time.Duration
}

// errForcedExit — повторный сигнал завершения во время graceful shutdown:
// Run возвращает её вызывающему немедленно, не дожидаясь OnRun и очистки
// ресурсов (HTTP-сервер метрик, пул) — main выходит с кодом 1, как и для
// любой ошибки Run.
var errForcedExit = errors.New("повторный сигнал завершения")

// migrateUp — применение миграций при старте (internal/migrate.Up);
// перемененная для подмены в тестах жизненного цикла (шов в духе
// lockTimeout/connectTimeout в internal/migrate).
var migrateUp = migrate.Up

// newRegistry — реестр метрик сервиса: Options.Metrics, если main создал
// его сам (приборы рабочей логики строятся до старта OnRun, T-077), иначе
// собственный metrics.New по имени сервиса.
func newRegistry(opts Options) *metrics.Registry {
	if opts.Metrics != nil {
		return opts.Metrics
	}
	return metrics.New(opts.Name)
}

// EnvOr возвращает значение переменной окружения или defaultVal.
func EnvOr(key, defaultVal string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return defaultVal
}

// Run поднимает сервис (миграции → пул БД → HTTP метрик → OnRun) и
// блокирует до завершения: SIGTERM/SIGINT (graceful shutdown), ошибка
// рабочей логики или HTTP-сервера либо нормальное завершение OnRun.
// Все ошибки Run логирует сам (уровень Error, с контекстом сервиса) и
// возвращает вызывающему main — тому достаточно выйти с кодом 1:
//
//	if err := service.Run(opts); err != nil {
//		os.Exit(1)
//	}
//
// Сигнал завершения во время применения миграций не убивает процесс
// посреди миграции: отменяются фазы ожидания, а применяющаяся миграция
// прерывается снятием SQL-запроса на стороне PostgreSQL (internal/migrate,
// T-112) — недоведённый файл откатывается целиком, версия схемы
// возвращается к предыдущей, и сервис останавливается штатно (Run
// возвращает nil); повторный старт доводит применение. Если снять запрос
// не удалось, миграция дорабатывает до конца (поведение C-04).
//
// Повторный сигнал завершения во время graceful shutdown — Run немедленно
// возвращает ошибку принудительного выхода без ожидания OnRun и очистки
// ресурсов; main выходит с кодом 1, как и для любой ошибки Run.
func Run(opts Options) error {
	if opts.Name == "" {
		return errors.New("service: имя сервиса обязательно")
	}
	if opts.MetricsAddr == "" {
		opts.MetricsAddr = ":9090"
	}
	if opts.ShutdownTimeout <= 0 {
		opts.ShutdownTimeout = DefaultShutdownTimeout
	}
	if opts.Logger == nil {
		opts.Logger = NewLogger(opts.Name, opts.Config)
	}
	// Дефолтный логгер — логгер сервиса: миграции (ниже) и код сервиса
	// через slog.Default пишут с контекстом имени.
	slog.SetDefault(opts.Logger)

	// Обработчики сигналов ставятся ДО миграций: дефолтная реакция на
	// SIGTERM/SIGINT убила бы процесс посреди применения миграции, а
	// golang-migrate помечает версию dirty до выполнения файла и снимает
	// флаг только после успеха — следующий старт ВСЕХ сервисов упал бы
	// на migrate.ErrDirty до ручного TRUNCATE schema_migrations. Сигнал
	// во время миграций отменяет фазы ожидания (подключение к БД,
	// advisory-блокировка) по контексту; применение прерывается снятием
	// SQL-запроса на стороне PostgreSQL (internal/migrate, T-112):
	// недоведённая миграция откатывается целиком, версия — к предыдущей.
	// Если снятие невозможно (не-URL DSN, БД недоступна) — миграция
	// дорабатывает (C-04). В обоих случаях сервис останавливается
	// штатно, не запуская рабочую логику.
	term := make(chan os.Signal, 1)
	signal.Notify(term, os.Interrupt, syscall.SIGTERM)
	defer signal.Stop(term)
	hup := make(chan os.Signal, 1)
	signal.Notify(hup, syscall.SIGHUP)
	defer signal.Stop(hup)

	// Схема БД применяется до старта HTTP-сервера: healthcheck отвечает
	// только на актуальной схеме. Параллельный старт нескольких сервисов
	// безопасен (advisory-блокировки в internal/migrate).
	// DSN: явный Options.PGDSN > конфигурация (env-переопределения уже
	// применены в config.Load: CRAWLER_POSTGRES_DSN/APP_PG_DSN > postgres.dsn).
	dsn := opts.PGDSN
	if dsn == "" && opts.Config != nil {
		dsn = opts.Config.Postgres.DSN
	}
	if dsn != "" {
		// Отдельный канал для фазы миграций: term остаётся нетронутым
		// для serve (сигнал доставляется во все зарегистрированные каналы).
		migTerm := make(chan os.Signal, 1)
		signal.Notify(migTerm, os.Interrupt, syscall.SIGTERM)
		defer signal.Stop(migTerm)

		migCtx, cancelMig := context.WithCancel(context.Background())
		defer cancelMig()
		var migSignalled atomic.Bool
		migDone := make(chan struct{})
		go func() {
			select {
			case <-migTerm:
				// Флаг до cancelMig: миграция, вернувшаяся по отмене,
				// уже видит его установленным — без гонки с проверкой ниже.
				migSignalled.Store(true)
				cancelMig()
			case <-migDone:
			}
		}()
		migErr := migrateUp(migCtx, dsn)
		close(migDone)

		switch {
		case errors.Is(migErr, migrate.ErrInterrupted):
			// Сигнал прервал применение: миграция откатана на стороне
			// PostgreSQL, версия схемы — последняя применённая. Это
			// штатная быстрая остановка, а не сбой (T-112).
			opts.Logger.Info("сигнал завершения во время применения схемы — миграция прервана и откатана, повторный старт доведёт")
			return nil
		case migSignalled.Load():
			opts.Logger.Info("сигнал завершения во время применения схемы — останавливаюсь после миграций")
			return nil
		case migErr != nil:
			opts.Logger.Error("миграции", "err", migErr)
			return migErr
		}
	}

	// Пул подключений создаётся после миграций и только если сервис
	// запросил его (OnDBPool): заглушки продолжают работать без БД.
	// Размер — из конфигурации, если она есть (бюджет ТЗ 6.3 — суммарно
	// 20–50 соединений на все сервисы; pool_size задаётся на сервис).
	var pool *pgxpool.Pool
	if dsn != "" && opts.OnDBPool != nil {
		poolSize := 0
		if opts.Config != nil {
			poolSize = opts.Config.Postgres.PoolSize
		}
		openCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		p, err := db.Open(openCtx, db.Options{
			DSN:          dsn,
			PoolSize:     poolSize,
			AfterConnect: opts.DBAfterConnect,
			// Метка сервиса в pg_stat_activity (T-177: цикл медленных
			// запросов monitor исключает свои агрегаты и читает метку
			// сервиса в сообщениях оператору).
			AppName: opts.Name,
		})
		cancel()
		if err != nil {
			opts.Logger.Error("пул PostgreSQL", "err", err)
			return err
		}
		pool = p
		opts.OnDBPool(pool)
	}

	mux := http.NewServeMux()
	registerProbes(mux, pool)
	// /metrics — реестр Prometheus (internal/metrics): базовые метрики
	// каркаса + коллекторы Go/runtime; метрики рабочей логики добавляются
	// фабрикой реестра (Counter/Gauge/Histogram с автометкой service,
	// T-076; приборы этапов — T-077). Options.Metrics — реестр, созданный
	// main-ом для приборов рабочей логики; nil — собственный реестр
	// каркаса (newRegistry).
	registry := newRegistry(opts)
	mux.Handle("/metrics", metrics.Handler(registry))
	if opts.ExtraMux != nil {
		opts.ExtraMux(mux)
	}

	srv := &http.Server{
		Addr:              opts.MetricsAddr,
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
		ErrorLog:          slog.NewLogLogger(opts.Logger.Handler(), slog.LevelError),
	}

	// Пул закрывается каркасом при graceful shutdown — последним, после
	// завершения OnRun и остановки HTTP-сервера метрик, а также на путях
	// ошибки после его создания (занятый адрес метрик ниже).
	closePool := func() {}
	if pool != nil {
		closePool = func() {
			pool.Close()
			opts.Logger.Info("пул PostgreSQL закрыт")
		}
	}

	// Listen отделён от Serve: ошибка занятого адреса — до старта OnRun.
	ln, err := net.Listen("tcp", opts.MetricsAddr)
	if err != nil {
		opts.Logger.Error("HTTP-сервер метрик", "addr", opts.MetricsAddr, "err", err)
		closePool() // пул уже создан — не оставлять его соединения
		return err
	}

	err = serve(opts, srv, ln, closePool, term, hup)
	if errors.Is(err, errForcedExit) {
		// Раньше здесь был прямой os.Exit(1) — обход defer'ов и
		// непроверяемость в тестах. Ошибка доезжает до main: его реакция
		// на любую ошибку Run и так os.Exit(1), код выхода не меняется.
		opts.Logger.Warn("повторный сигнал — принудительное завершение без ожидания остановки")
	}
	return err
}

// registerProbes — маршруты проб каркаса на общем mux (порт метрик; отдельного
// слушателя для проб нет — ТЗ 5.9 один HTTP-эндпоинт на сервис). /livez —
// liveness, /healthz — readiness (C-03 → T-110).
func registerProbes(mux *http.ServeMux, pool *pgxpool.Pool) {
	mux.HandleFunc("/livez", livezHandler())
	mux.HandleFunc("/healthz", healthzHandler(pool))
}

// livezHandler — liveness-проба: процесс жив и его HTTP-цикл отвечает.
// Зависимости (БД, Redis) сознательно не проверяются: их недоступность —
// не смерть процесса, 503 здесь говорил бы оркестратору «перезапусти»
// при сбое PostgreSQL/Redis и маскировал бы причину рестартами воркеров.
//
// Отдельный признак «фоновый цикл не паниковал» не нужен: в каркасе такое
// состояние не живёт дольше бюджета остановки — паника/ошибка/завершение
// OnRun перехватываются serve и останавливают весь процесс (Run
// возвращается, main выходит, рестарт-политика оркестратора поднимает
// сервис), поэтому ответ этой пробы уже означает «Run активен» (в окне
// graceful shutdown, ≤ ShutdownTimeout, процесс и так останавливается).
func livezHandler() http.HandlerFunc {
	return func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok\n"))
	}
}

// healthzHandler — readiness-проба (зависимости; её проверяет оператор
// и мониторинг): сервис с пулом БД проверяет PostgreSQL коротким Ping —
// недоступная БД видна как 503/unhealthy, а не «готов» с бесконечными
// ошибками в логе воркера (C-03). Сервис без пула (Options без
// OnDBPool/DSN) всегда готов: зависимостей нет.
//
// Redis в пробу не входит (T-110): клиенты Redis живут внутри OnRun
// воркеров — каркас их не видит, а втягивание зависимости в путь пробы
// меняло бы готовность на «обе инфраструктуры ответили» ценой новой
// связности в каркасе. Потребителя «впускать задачи» тоже нет — воркеры
// тянут их из стримов сами; доступность Redis видна по метрикам очередей
// (crawler_queue_depth) и логам.
func healthzHandler(pool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if pool != nil {
			ctx, cancel := context.WithTimeout(r.Context(), time.Second)
			defer cancel()
			if err := pool.Ping(ctx); err != nil {
				http.Error(w, "db unavailable", http.StatusServiceUnavailable)
				return
			}
		}
		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok\n"))
	}
}

// serve — жизненный цикл после инициализации: HTTP-сервер метрик на ln и
// рабочая логика opts.OnRun. Завершается по первому из: сигнал term,
// ошибка HTTP-сервера, возврат OnRun. Порядок остановки (ТЗ 3.4):
//  1. рабочая логика: отмена контекста OnRun и ожидание её завершения
//     в пределах ShutdownTimeout (повторный term прерывает ожидание);
//  2. HTTP-сервер метрик — дожидается начатых запросов;
//  3. пул БД (closePool).
//
// Ошибки остановки логируются, но не прерывают последовательность.
func serve(opts Options, srv *http.Server, ln net.Listener, closePool func(), term <-chan os.Signal, hup <-chan os.Signal) error {
	logger := opts.Logger
	runCtx, cancelRun := context.WithCancel(context.Background())
	defer cancelRun()

	// SIGHUP — перезагрузка правил без рестарта (make rules-reload).
	hupDone := make(chan struct{})
	defer close(hupDone)
	go func() {
		for {
			select {
			case <-hupDone:
				return
			case <-hup:
				logger.Info("получен SIGHUP")
				if opts.OnSIGHUP != nil {
					opts.OnSIGHUP()
				}
			}
		}
	}()

	httpErr := make(chan error, 1)
	go func() {
		if err := srv.Serve(ln); err != nil && !errors.Is(err, http.ErrServerClosed) {
			httpErr <- err
		}
	}()

	var onRunDone <-chan error
	if opts.OnRun != nil {
		done := make(chan error, 1)
		onRunDone = done
		go func() {
			defer func() {
				// Паника рабочей логики не должна ронять процесс без
				// лога и очистки: превращаем в ошибку, дальше — обычный
				// graceful shutdown (переведёт в код выхода 1).
				if r := recover(); r != nil {
					logger.Error("паника в рабочей логике", "panic", r, "stack", string(debug.Stack()))
					done <- fmt.Errorf("OnRun: паника: %v", r)
				}
			}()
			done <- opts.OnRun(runCtx)
		}()
	}

	// Стадия ожидания: работаем до сигнала, ошибки HTTP/OnRun или
	// самостоятельного завершения OnRun (одноразовая задача).
	var (
		onRunErr      error
		onRunFinished = opts.OnRun == nil
		httpFail      error
	)
	select {
	case sig := <-term:
		logger.Info("сигнал завершения — graceful shutdown", "signal", sig.String())
	case err := <-httpErr:
		logger.Error("HTTP-сервер метрик", "err", err)
		httpFail = fmt.Errorf("HTTP-сервер метрик: %w", err)
	case err := <-onRunDone:
		onRunFinished = true
		onRunErr = err
		if err != nil {
			logger.Error("рабочая логика", "err", err)
		} else {
			logger.Info("рабочая логика завершена — останавливаю сервис")
		}
	}
	cancelRun()

	// Стадия остановки. 1) Рабочая логика: доработать текущую задачу или
	// вернуть её в очередь (ТЗ 3.4) в пределах общего бюджета остановки;
	// повторный сигнал — принудительный выход.
	deadline := time.Now().Add(opts.ShutdownTimeout)
	timedOut := false
	if !onRunFinished {
		timer := time.NewTimer(time.Until(deadline))
		defer timer.Stop()
		select {
		case err := <-onRunDone:
			onRunErr = err
			// Основной ошибкой выхода при httpFail остаётся сбой HTTP
			// (именно он остановил сервис), но финализация воркера
			// (возврат задач в очередь, ТЗ 3.4) тоже заслуживает лога.
			if err != nil {
				logger.Error("рабочая логика при остановке", "err", err)
			}
		case <-term:
			return errForcedExit
		case <-timer.C:
			timedOut = true
			logger.Warn("рабочая логика не остановилась за таймаут — продолжаю остановку",
				"timeout", opts.ShutdownTimeout.String())
		}
	}

	// 2) HTTP-сервер метрик: остаток бюджета — закончить начатые скрейпы
	// /metrics; если бюджет исчерпан зависшей рабочей логикой, соединения
	// закрываются немедленно (общий бюджет остановки не превышается).
	if remain := time.Until(deadline); remain > 0 {
		httpCtx, cancelHTTP := context.WithTimeout(context.Background(), remain)
		defer cancelHTTP()
		if err := srv.Shutdown(httpCtx); err != nil {
			logger.Warn("остановка HTTP-сервера метрик", "err", err)
		}
	} else if err := srv.Close(); err != nil {
		logger.Warn("закрытие HTTP-сервера метрик", "err", err)
	}

	// 3) Пул БД — последним.
	closePool()

	switch {
	case httpFail != nil:
		return httpFail
	case timedOut:
		return fmt.Errorf("рабочая логика не остановилась за %s", opts.ShutdownTimeout)
	default:
		return onRunErr
	}
}
