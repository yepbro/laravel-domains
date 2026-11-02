// Долгоживущий контур живой проверки (T-220). До T-220 конвейер Run
// собирал всё окружение на каждый вызов: цепь резолверов (1 основной +
// dns.fallback_resolvers клиентов, каждый с пулом udp_pool_size 2000 —
// до 6000 UDP-сокетов на один запрос формы при LimitNOFILE=65535 юнита
// crawler-web: ~10 параллельных проверок исчерпывали fd вместе с
// /healthz), свой httpfetch.Fetcher с локальными limitState (потолки
// секции http действовали только внутри одного конвейера — глобального
// лимита на сервис не было), правила с диска и render.Manager со
// Start() при настроенном render.remote_url.
//
// Runtime собирает это окружение один раз на время жизни сервиса
// (публичная форма web): одна цепь резолверов с малым пулом UDP
// (LiveUDPPoolSize), один Fetcher с общими limitState, Reloader правил
// с фоновой перезагрузкой и один render.Manager. Run гоняет конвейер
// по готовому контуру и своего фонда не строит; владелец закрывает
// контур Close'ом на остановке (web.Server.Run → closeLive). Одноразовый
// потребитель (CLI crawlerctl check) собирает контур на один вызов
// функцией Run.
package checker

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"crawler/internal/config"
	"crawler/internal/dnsclient"
	"crawler/internal/httpfetch"
	"crawler/internal/httpworker"
	"crawler/internal/render"
	"crawler/internal/rules"
)

// LiveUDPPoolSize — пул UDP-сокетов каждого клиента цепи живой проверки:
// параллелизм формы ограничен семафором web (единицы проверок), а DNS-фаза
// одной проверки последовательна (CheckSubdomains идёт по субдоменам друг
// за другом) — массовый фонд udp_pool_size боевого конвейера здесь не
// нужен. 3 клиента цепи × 16 сокетов = 48 fd на сервис против 6000 на
// запрос до T-220.
const LiveUDPPoolSize = 16

// DefaultRulesReload — период фоновой перезагрузки правил контура.
// Публичной формой никто не управляет (SIGHUP правил шлёт оператор
// анализатору, make rules-reload), набор актуализируется таймером;
// неизменный rules_version — no-op (контракт Reloader).
const DefaultRulesReload = 5 * time.Minute

// RuntimeOptions — параметры контура; нулевые значения — нормы живой
// проверки формы.
type RuntimeOptions struct {
	// UDPPoolSize — пул UDP-сокетов каждого клиента цепи; 0 —
	// LiveUDPPoolSize.
	UDPPoolSize int
	// RulesReload — период фоновой перезагрузки правил; 0 —
	// DefaultRulesReload; отрицательный — без таймера (одноразовый
	// вызов Run: набор читается на сборке и не меняется).
	RulesReload time.Duration
}

// LiveCircuit — долгоживущий контур живой проверки для сервиса-владельца
// (T-220): Run совместим по сигнатуре с LiveFunc (web в продакшн вызывает
// метод контура, тесты подменяют поле liveCheck), Close останавливает
// контур целиком. Удовлетворяется *Runtime.
type LiveCircuit interface {
	Run(ctx context.Context, cfg *config.Config, pool Pool, domain string, log Logger) (*Result, error)
	Close() error
}

// Runtime — собранное один раз и переиспользуемое между проверками
// окружение конвейера. Безопасен для одновременного использования из
// горутин: цепь резолверов, Fetcher, Reloader и render.Manager
// потокобезопасны; состояние отдельной проверки (справочник субдоменов,
// хранилище страниц) живёт внутри Run.
type Runtime struct {
	cfg      *config.Config
	log      Logger
	resolve  dnsclient.Resolver
	closeDNS func() error
	fetcher  *httpfetch.Fetcher
	// renderer — nil, если рендер не настроен (render.remote_url пуст).
	renderer *render.Manager
	rules    *rules.Reloader

	life     context.Context // контекст жизни: фоновая перезагрузка, браузеры рендера
	stopTick chan struct{}   // останавливает фоновую перезагрузку правил
	tickDone chan struct{}   // закрыт по выходу фонового цикла (nil — цикла нет)

	closeOnce sync.Once
	closeErr  error
}

// compile-time check: *Runtime — LiveCircuit.
var _ LiveCircuit = (*Runtime)(nil)

// newResolverChainFn — шов сборки цепи резолверов для тестов контура
// (подсчёт клиентов и размеры пулов без реальных сокетов); продакшн —
// newResolverChain (resolve.go).
var newResolverChainFn = newResolverChain

// NewRuntime собирает контур живой проверки: цепь резолверов с малым
// пулом UDP под лимитёром T-035, один Fetcher с общими limitState
// (лимиты секции http действуют на все проверки сервиса сразу), один
// render.Manager при настроенном render.remote_url и Reloader правил.
// Рендер стартует лениво — первым SPA-рендером (Manager.Run поднимает
// поколение сам): недоступный на старте Chromium не деградирует сервис
// навсегда, ошибка рендера оставляет анализ по исходному HTML (семантика
// Run до T-220). ctx — контекст жизни контура (отмена останавливает
// фоновую перезагрузку и браузеры рендера). Ошибка — контур не создан,
// частично открытые ресурсы закрыты.
func NewRuntime(ctx context.Context, cfg *config.Config, log Logger, opts RuntimeOptions) (*Runtime, error) {
	if cfg == nil {
		return nil, errors.New("checker: контур живой проверки без конфигурации")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if log == nil {
		log = nopLogger{}
	}
	if opts.UDPPoolSize <= 0 {
		opts.UDPPoolSize = LiveUDPPoolSize
	}

	chain, closeDNS, err := newResolverChainFn(ctx, cfg.DNS, opts.UDPPoolSize)
	if err != nil {
		return nil, fmt.Errorf("checker: %w", err)
	}
	resolve, err := dnsclient.NewLimitResolver(chain, dnsclient.NewLimitOptions(cfg.DNS))
	if err != nil {
		_ = closeDNS()
		return nil, fmt.Errorf("checker: %w", err)
	}

	// Транспорт как у боевого http-worker (T-049/T-050/T-047), но один
	// на контур: общий limitState — глобальные потолки соединений/RPS
	// на все живые проверки сервиса; кэш резолва хостов редиректов
	// ограничен DefaultCacheEntries, общим на сервис.
	hopts := httpfetch.NewOptions(cfg.HTTP)
	hopts.TLSClientConfig = &tls.Config{InsecureSkipVerify: true}
	hopts.Resolver = httpfetch.NewCachingResolver(
		httpworker.NewDNSLookup(resolve, cfg.DNS.IPv6), httpfetch.CacheOptions{})
	fetcher := httpfetch.NewFetcher(hopts)

	var renderer *render.Manager
	if cfg.Render.RemoteURL != "" {
		renderer = render.NewManager(ctx, render.NewOptions(*cfg))
	}

	rulesRel, err := rules.NewReloader(cfg.Analyzer.RulesDir, slogOf(log))
	if err != nil {
		if renderer != nil {
			renderer.Close()
		}
		fetcher.CloseIdleConnections()
		_ = closeDNS()
		return nil, fmt.Errorf("checker: %w", err)
	}

	rt := &Runtime{
		cfg:      cfg,
		log:      log,
		resolve:  resolve,
		closeDNS: closeDNS,
		fetcher:  fetcher,
		renderer: renderer,
		rules:    rulesRel,
		life:     ctx,
	}
	if opts.RulesReload == 0 {
		opts.RulesReload = DefaultRulesReload
	}
	if opts.RulesReload > 0 {
		rt.stopTick = make(chan struct{})
		rt.tickDone = make(chan struct{})
		go rt.reloadLoop(opts.RulesReload)
	}
	return rt, nil
}

// reloadLoop — фоновая перезагрузка правил: выход по Close (stopTick)
// или отмене контекста жизни; ошибка загрузки не меняет текущий набор
// (контракт Reloader — сервис работает на прежних правилах).
func (rt *Runtime) reloadLoop(interval time.Duration) {
	defer close(rt.tickDone)
	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		select {
		case <-rt.stopTick:
			return
		case <-rt.life.Done():
			return
		case <-t.C:
			_ = rt.rules.Reload()
		}
	}
}

// Close останавливает контур: фоновая перезагрузка правил, рендер
// (текущее поколение дорабатывает свои страницы — graceful, ТЗ 3.4),
// соединения транспорта и пулы UDP-сокетов цепи. Идемпотентен;
// повторные вызовы возвращают ошибку первого.
func (rt *Runtime) Close() error {
	rt.closeOnce.Do(func() {
		if rt.tickDone != nil {
			close(rt.stopTick)
			<-rt.tickDone
		}
		if rt.renderer != nil {
			rt.renderer.Close()
		}
		rt.fetcher.CloseIdleConnections()
		rt.closeErr = rt.closeDNS()
	})
	return rt.closeErr
}

// ReloadRules перечитывает каталог правил (смена набора — например,
// оператором по образцу анализатора); ошибка не меняет текущий набор.
// Фоновому таймеру контура метод не нужен — точка входа для владельца.
func (rt *Runtime) ReloadRules() error { return rt.rules.Reload() }

// slogOf — *slog.Logger для Reloader (его поле — конкретный тип):
// логгер контура, если он slog, иначе тихий — Reloader пишет свои
// Info/Warn сам.
func slogOf(log Logger) *slog.Logger {
	if l, ok := log.(*slog.Logger); ok {
		return l
	}
	return slog.New(slog.DiscardHandler)
}

// nopLogger — тихий логгер контура при nil-параметре (NewRuntime
// допускает nil, как httpfetch.NewOptions подставляет дефолты).
type nopLogger struct{}

func (nopLogger) Info(string, ...any) {}
func (nopLogger) Warn(string, ...any) {}

// Run — одноразовый конвейер одного имени (CLI crawlerctl check):
// контур собирается на вызов и закрывается по завершении — правилам
// и сокетам одного домена долгая жизнь не нужна. Сервису-владельцу
// (форма web) нужен долгоживущий контур: NewRuntime + Run + Close
// (T-220).
func Run(ctx context.Context, cfg *config.Config, pool Pool, domain string, log Logger) (*Result, error) {
	rt, err := NewRuntime(ctx, cfg, log, RuntimeOptions{RulesReload: -1})
	if err != nil {
		return nil, err // ошибки NewRuntime уже с префиксом checker:
	}
	defer func() { _ = rt.Close() }()
	return rt.Run(ctx, cfg, pool, domain, log)
}
