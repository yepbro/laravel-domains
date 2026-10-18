// Package config — единая конфигурация сервисов Domain Crawler (ТЗ 5.10
// основного ТЗ).
//
// Источники значений с приоритетом: переменные окружения > YAML-файл >
// значения по умолчанию. Путь к файлу задаётся переменной окружения
// CRAWLER_CONFIG (устаревший аналог APP_CONFIG); если она не задана,
// используется config.yaml в текущем каталоге, когда такой файл существует;
// иначе конфигурация собирается из значений по умолчанию и окружения.
//
// Схема имён переменных: CRAWLER_<СЕКЦИЯ>_<ПОЛЕ> (CRAWLER_HTTP_TOTAL_TIMEOUT,
// CRAWLER_SCHEDULER_LEASE, ...). Полный список имён, типов и ограничений —
// в docs/configuration.md. Пустое значение переменной считается незаданным.
package config

import (
	"fmt"
	"net"
	"net/url"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"
)

// maxSubdomains — предел числа субдоменов: битовая маска dns_mask
// хранится в int8, бит = subdomains.id (ТЗ 5.10, §5.2 п. 9).
const maxSubdomains = 63

// maxUserAgentLen — разумная длина http.user_agent (ТЗ 5.5, §9): имя
// продукта с контактами укладывается в десятки байт; 512 — щедрый
// потолок, отсекающий мусор вместо контактов.
const maxUserAgentLen = 512

// DefaultUserAgent — честный User-Agent HTTP-этапа по умолчанию (ТЗ 5.5,
// §9). Контакты — заглушки на зарезервированных домене и адресе
// (RFC 2606, зона .example): реальных владельцев краулера ими не
// представить, поэтому оператор обязан подставить свои контакты
// конфигом http.user_agent — дефолт лишь не даёт сервису быть
// анонимным. Форма — общепринятая «имя/версия (+URL; e-mail)».
const DefaultUserAgent = "DomainCrawler/1.0 (+https://example.example/contact; admin@example.example)"

// Config — корень конфигурации; секции соответствуют ТЗ 5.10.
type Config struct {
	Postgres   Postgres  `yaml:"postgres"`
	Redis      Redis     `yaml:"redis"`
	Ingest     Ingest    `yaml:"ingest"`
	Scheduler  Scheduler `yaml:"scheduler"`
	DNS        DNS       `yaml:"dns"`
	HTTP       HTTP      `yaml:"http"`
	Render     Render    `yaml:"render"`
	Content    Content   `yaml:"content"`
	Analyzer   Analyzer  `yaml:"analyzer"`
	Subdomains []string  `yaml:"subdomains"`
	Monitor    Monitor   `yaml:"monitor"`
	Log        Log       `yaml:"log"`
}

// Postgres — подключение к БД (ТЗ 6.3).
type Postgres struct {
	DSN      string `yaml:"dsn"`
	PoolSize int    `yaml:"pool_size"`
}

// Redis — адрес и параметры надёжности очередей задач (ТЗ 4.2).
type Redis struct {
	Addr string `yaml:"addr"`
	// MaxLen — явный предел длины всех стримов при публикации
	// (XADD MAXLEN ~); 0 — пределы по стримам из пакета queue
	// (queue.DefaultStreamMaxLen, T-025).
	MaxLen int `yaml:"maxlen"`
	// MaxAttempts — максимальное число доставок задачи до перевода в
	// q:dead (ТЗ 4.2: настраивается, по умолчанию 3; счётчик — T-025).
	MaxAttempts int `yaml:"max_attempts"`
	// ClaimMinIdle — минимальный простой зависшей (pending без XACK)
	// записи до возврата в работу клеймом XAUTOCLAIM (T-025).
	ClaimMinIdle Duration `yaml:"claim_min_idle"`
}

// Ingest — приём файлов доменов (ТЗ 5.1).
type Ingest struct {
	Dir           string `yaml:"dir"`
	BatchSize     int    `yaml:"batch_size"`
	StableSeconds int    `yaml:"stable_seconds"`
	// ScanInterval — интервал периодического полного обхода incoming как
	// страховки к fsnotify (T-013; расширение к минимальному составу
	// ТЗ 5.10, поэтому длительность, а не секунды, как у stable_seconds).
	ScanInterval Duration `yaml:"scan_interval"`
	// MaxErrorRatio — порог доли отклонённых строк файла (T-016, ТЗ 5.1:
	// «файл не считается ошибочным из-за единичных плохих строк»):
	// при превышении файл целиком считается ошибочным (failed/, T-021).
	// Доля 0..1; расширение к минимальному составу ТЗ 5.10.
	MaxErrorRatio float64 `yaml:"max_error_ratio"`
}

// Scheduler — планировщик проверок (ТЗ 5.3).
type Scheduler struct {
	RecheckInterval     Duration `yaml:"recheck_interval"`
	BatchSize           int      `yaml:"batch_size"`
	QueueHighWatermark  int      `yaml:"queue_high_watermark"`
	Lease               Duration `yaml:"lease"`
	TempErrorRetryAfter Duration `yaml:"temp_error_retry_after"`
	// TempErrorMaxRetries — максимум повторных проверок temp_error подряд
	// (ТЗ 5.3: «не более N раз подряд»); после исчерпания домен получает
	// итоговый статус по результату последней проверки (T-031). ТЗ число
	// не задаёт; 3 — как счётчик попыток доставки очереди (T-025).
	TempErrorMaxRetries int `yaml:"temp_error_max_retries"`
	// ZoneWeights — веса зон выборки пачек (ТЗ 5.3: «веса зон —
	// опционально в конфиге», T-106): имя зоны (punycode, как в
	// справочнике zones) → вес 1..1000; зона с весом w занимает в пачке
	// долю, пропорциональную w, вес 1 у всех — равный круговой порядок.
	// nil или пустая карта — веса не заданы, поведение по умолчанию.
	// cmd/scheduler разрешает имена в zone_id по справочнику на старте
	// (scheduler.ResolveZoneWeights): имени может не быть в справочнике
	// (зону создаёт ingest первым файлом, ТЗ 5.1) — Warn, зона
	// проверяется с весом 1; неактивные зоны разрешаются тоже — вес
	// вступает в силу при включении зоны (crawlerctl zones enable)
	// без рестарта. Расширение к минимальному составу ТЗ 5.10: новое
	// поле внутри секции scheduler, секция верхнего уровня не вводится.
	ZoneWeights map[string]int `yaml:"zone_weights"`
}

// DNS — параметры DNS-этапа (ТЗ 5.4). Клиент — internal/dnsclient
// (T-034): miekg/dns, пул UDP-сокетов, TCP-fallback при TC=1, ретраи.
type DNS struct {
	Resolvers         []string `yaml:"resolvers"`
	FallbackResolvers []string `yaml:"fallback_resolvers"`
	Concurrency       int      `yaml:"concurrency"`
	MaxQPS            int      `yaml:"max_qps"`
	IPv6              bool     `yaml:"ipv6"`
	// Timeout — таймаут одной UDP-попытки обмена. Расширение к
	// минимальному составу ТЗ 5.10 (таймауты транспорта там не
	// перечислены, но ТЗ 5.4 требует ретраев при таймаутах).
	Timeout Duration `yaml:"timeout"`
	// TCPTimeout — таймаут TCP-fallback при усечении ответа (TC=1):
	// установка соединения + обмен, отдельный от UDP.
	TCPTimeout Duration `yaml:"tcp_timeout"`
	// Retries — повторы запроса сверх первой попытки при таймауте/ошибке
	// сети; «SERVFAIL/таймаут после ретраев» → temp_error (ТЗ 5.4 п. 1).
	// Минимум 1: ТЗ ретраи требует, «без ретраев» не рабочий режим.
	Retries int `yaml:"retries"`
	// RetryDelay — пауза между попытками.
	RetryDelay Duration `yaml:"retry_delay"`
	// UDPPoolSize — размер пула UDP-сокетов клиента. По умолчанию равен
	// concurrency: пул меньше планового параллелизма сам становится
	// ограничителем ниже настроенного.
	UDPPoolSize int `yaml:"udp_pool_size"`
}

// HTTP — параметры HTTP-этапа (ТЗ 5.5, таблица лимитов).
type HTTP struct {
	Concurrency    int      `yaml:"concurrency"`
	PerIPLimit     int      `yaml:"per_ip_limit"`
	ConnectTimeout Duration `yaml:"connect_timeout"`
	// TLSHandshakeTimeout — таймаут TLS-рукопожатия (5 с из таблицы
	// лимитов ТЗ 5.5). В минимальном составе ТЗ 5.10 поля нет — как и
	// других таймаутов таблицы, все они «настраиваются» по ТЗ 5.5.
	TLSHandshakeTimeout Duration `yaml:"tls_handshake_timeout"`
	HeaderTimeout       Duration `yaml:"header_timeout"`
	TotalTimeout        Duration `yaml:"total_timeout"`
	MaxRedirects        int      `yaml:"max_redirects"`
	MaxBodyBytes        ByteSize `yaml:"max_body_bytes"`
	UserAgent           string   `yaml:"user_agent"`
	// MinBodySpeed — мин. скорость чтения тела после первого байта,
	// байт/с (ТЗ 5.5: «защита от тарпитов: мин. скорость 2 КБ/с»).
	// Расширение к минимальному составу ТЗ 5.10 — значение из таблицы
	// лимитов ТЗ 5.5 (T-049).
	MinBodySpeed ByteSize `yaml:"min_body_speed"`
	// MaxRPS — глобальный темп запросов (ТЗ 5.5: «ограничение исходящей
	// нагрузки по глобальному числу соединений и RPS»). Числа в ТЗ нет:
	// дефолт — порядок естественного темпа этапа (страница ~1–2 с при
	// concurrency 2000). Расширение к минимальному составу ТЗ 5.10
	// (T-049).
	MaxRPS int `yaml:"max_rps"`
	// BlockInternalAddrs — запрет подключения к внутренним адресам
	// (loopback/RFC1918/ULA/link-local/unspecified/multicast) по хостам
	// HTTP-редиректов: SSRF-защита — недоверенный сайт может увести
	// краулер на 127.0.0.1, cloud metadata или адрес сети БД/Redis.
	// Адреса DNS-этапа не фильтруются. По умолчанию false (testbed и
	// dev-стенды резолвят сценарии во внутренние сети 172.28.100.0/24);
	// для Internet-боевой эксплуатации рекомендуется включать.
	// Расширение к минимальному составу ТЗ 5.10.
	BlockInternalAddrs bool `yaml:"block_internal_addrs"`
}

// Render — параметры рендера (ТЗ 5.6). RemoteURL — расширение dev-окружения:
// адрес внешнего Chromium по CDP (ws://…); в боевом режиме воркер запускает
// браузер сам и поле остаётся пустым.
type Render struct {
	RemoteURL         string   `yaml:"remote_url"`
	Contexts          int      `yaml:"contexts"`
	Timeout           Duration `yaml:"timeout"`
	RestartEveryPages int      `yaml:"restart_every_pages"`
	// RestartMemoryBytes — порог перезапуска браузера по памяти (ТЗ 5.6
	// «при превышении порога памяти»): суммарный RSS процессов Chromium
	// ≥ порога → перезапуск (T-065). Расширение к минимальному составу
	// ТЗ 5.10: числа ТЗ не задаёт, ориентир — бюджет Chromium ≈ 2 ГБ
	// (ТЗ 3.2, cgroup MemoryMax), порог берётся ниже лимита контейнера.
	// 0 — контроль отключён; в remote-режиме (dev) RSS недоступен и
	// контроль не работает (потолок держит mem_limit контейнера).
	RestartMemoryBytes ByteSize `yaml:"restart_memory_bytes"`
}

// Content — хранилище контента (ТЗ 5.7).
type Content struct {
	Dir            string   `yaml:"dir"`
	PackSize       ByteSize `yaml:"pack_size"`
	MaxStoreBytes  ByteSize `yaml:"max_store_bytes"`
	ZstdDictionary string   `yaml:"zstd_dictionary"`
	// DiskFreePercent — порог реакции на переполнение диска (ТЗ 8):
	// при доле свободного места каталога контента ниже порога запись
	// контента приостанавливается (ErrStoreFull) и сигнализируется
	// алерт. Расширение к минимальному составу ТЗ 5.10, имя — по смыслу
	// настраиваемого порога ТЗ 8 («порог настраивается»): доля свободного
	// места, %.
	DiskFreePercent float64 `yaml:"disk_free_percent"`
}

// Analyzer — анализатор Laravel (ТЗ 5.8).
type Analyzer struct {
	RulesDir        string  `yaml:"rules_dir"`
	LaravelMinScore float64 `yaml:"laravel_min_score"`
}

// Monitor — служебный сервис мониторинга (ТЗ 5.9).
type Monitor struct {
	Listen            string   `yaml:"listen"`
	ZoneStatsInterval Duration `yaml:"zone_stats_interval"`
	// IngestMetricsURL — адрес /metrics сервиса ingest (T-099): алерт
	// мусорных content_hash очистки читает счётчик из экспозиции ingest
	// зондом раз в период алертов. Тот же адрес, что скрейпит Prometheus;
	// пустая строка — алерт «не измеряется».
	IngestMetricsURL string `yaml:"ingest_metrics_url"`
}

// Log — уровень логирования сервисов (каркас internal/service). Расширение
// к минимальному составу ТЗ 5.10: уровень нужен каркасу, а не отдельному
// этапу, поэтому вынесен в свою секцию верхнего уровня.
type Log struct {
	Level string `yaml:"level"` // debug | info | warn | error
}

// Default возвращает конфигурацию со значениями по умолчанию из ТЗ 5.10
// (кроме postgres.dsn: он обязан быть задан в YAML или окружении).
func Default() *Config {
	return &Config{
		Postgres: Postgres{
			DSN:      "",
			PoolSize: 20,
		},
		Redis: Redis{
			Addr: "127.0.0.1:6379",
			// Пределы по стримам — из queue.DefaultStreamMaxLen (T-025).
			MaxLen:      0,
			MaxAttempts: 3, // ТЗ 4.2: по умолчанию 3
			// Больше худшего окна удержания доставок живым воркером
			// (рендер: 16 записей × 25 с при contexts = 1 ≈ 6,7 мин,
			// см. queue.DefaultClaimMinIdle).
			ClaimMinIdle: Duration(10 * time.Minute),
		},
		Ingest: Ingest{
			Dir:           "/data/incoming",
			BatchSize:     50000,
			StableSeconds: 10,
			ScanInterval:  Duration(30 * time.Second),
			// Единичный мусор файлу не вредит, а файл не в той зоне
			// или битый дамп дают долю ошибок ~1 (T-016).
			MaxErrorRatio: 0.1,
		},
		Scheduler: Scheduler{
			RecheckInterval:     Duration(4380 * time.Hour), // ≈ 6 месяцев
			BatchSize:           5000,
			QueueHighWatermark:  100000,
			Lease:               Duration(30 * time.Minute),
			TempErrorRetryAfter: Duration(24 * time.Hour),
			TempErrorMaxRetries: 3,
			// Веса зон не заданы — равный круговой порядок (ТЗ 5.3:
			// «опционально»).
			ZoneWeights: nil,
		},
		DNS: DNS{
			Resolvers:         []string{"127.0.0.1:53"},
			FallbackResolvers: []string{"1.1.1.1:53", "9.9.9.9:53"},
			Concurrency:       2000,
			MaxQPS:            10000,
			IPv6:              true,
			// Локальный unbound (ТЗ 5.4) отвечает быстро даже на холодном
			// кэше; TCP-fallback реже и дороже — установка соединения.
			Timeout:    Duration(2 * time.Second),
			TCPTimeout: Duration(5 * time.Second),
			// Как попытки доставки очереди (T-025) и перепроверки
			// temp_error (T-031): единая политика повторов системы.
			Retries:    2,
			RetryDelay: Duration(200 * time.Millisecond),
			// = concurrency: пул не должен резать плановый параллелизм.
			UDPPoolSize: 2000,
		},
		HTTP: HTTP{
			Concurrency:    2000,
			PerIPLimit:     4,
			ConnectTimeout: Duration(5 * time.Second),
			// Таблица лимитов ТЗ 5.5: TLS handshake 5 с.
			TLSHandshakeTimeout: Duration(5 * time.Second),
			HeaderTimeout:       Duration(10 * time.Second),
			TotalTimeout:        Duration(20 * time.Second),
			MaxRedirects:        8,
			MaxBodyBytes:        ByteSize(2 << 20), // 2 MiB
			UserAgent:           DefaultUserAgent,
			// Таблица лимитов ТЗ 5.5: мин. скорость 2 КБ/с (тарпиты).
			MinBodySpeed: ByteSize(2 << 10),
			// Естественный темп этапа ~1000–2000 RPS (страница 1–2 с,
			// concurrency 2000): дефолт держит порядок исходящей нагрузки
			// одного IP (тюнинг сети — ТЗ 10.2).
			MaxRPS: 1000,
		},
		Render: Render{
			RemoteURL:         "",
			Contexts:          3,
			Timeout:           Duration(25 * time.Second),
			RestartEveryPages: 500,
			// 1,5 ГиБ = 75 % бюджета Chromium ≈ 2 ГБ (ТЗ 3.2, cgroup
			// MemoryMax ≈ 2 ГБ): перезапуск — до cgroup-OOM; запас — на
			// рост между проверками (раз в задачу) и tmpfs /dev/shm,
			// который учитывается в памяти cgroup, но не в RSS процессов.
			RestartMemoryBytes: ByteSize(1536 << 20),
		},
		Content: Content{
			Dir:            "/data/content",
			PackSize:       ByteSize(512 << 20), // 512 MiB
			MaxStoreBytes:  ByteSize(2 << 20),   // 2 MiB
			ZstdDictionary: "/data/content/dict.zst",
			// ТЗ 8 числа не задаёт: 5% — типовой нижний предел для томов
			// данных (место хватает и на компактификацию, и на журналы);
			// валидация не даёт выключить контроль нулём.
			DiskFreePercent: 5,
		},
		Analyzer: Analyzer{
			RulesDir:        "/etc/crawler/rules",
			LaravelMinScore: 0.6,
		},
		// Начальное наполнение таблицы subdomains; "" — апекс домена.
		Subdomains: []string{"", "www", "admin", "crm", "api", "app"},
		Monitor: Monitor{
			Listen:            ":9100",
			ZoneStatsInterval: Duration(60 * time.Second),
			// Loopback ingest по METRICS_ADDR (systemd-юниты привязывают
			// 127.0.0.1:9101); docker-сети переопределяют именем сервиса
			// (config.dev.yaml: http://ingest:9101/metrics).
			IngestMetricsURL: "http://127.0.0.1:9101/metrics",
		},
		Log: Log{
			Level: "info",
		},
	}
}

// normalize приводит значения к каноничной форме (регистр/пробелы).
func (c *Config) normalize() {
	for i, s := range c.Subdomains {
		c.Subdomains[i] = strings.ToLower(strings.TrimSpace(s))
	}
	c.Log.Level = strings.ToLower(strings.TrimSpace(c.Log.Level))
	// Ключи весов зон — punycode-имена в нижнем регистре (как zones.name);
	// карта пересобирается в порядке возрастания ключей, чтобы столкновение
	// после приведения («Com» и «com» в одном YAML) разрешалось
	// детерминированно — побеждает лексикографически больший ключ.
	if len(c.Scheduler.ZoneWeights) > 0 {
		names := make([]string, 0, len(c.Scheduler.ZoneWeights))
		for name := range c.Scheduler.ZoneWeights {
			names = append(names, name)
		}
		sort.Strings(names)
		w := make(map[string]int, len(names))
		for _, name := range names {
			w[strings.ToLower(strings.TrimSpace(name))] = c.Scheduler.ZoneWeights[name]
		}
		c.Scheduler.ZoneWeights = w
	}
}

var subdomainLabelRe = regexp.MustCompile(`^[a-z0-9]([a-z0-9-]{0,61}[a-z0-9])?$`)

// zoneNameRe — ключ scheduler.zone_weights: имя зоны в punycode нижним
// регистром, как хранит справочник zones (ingest нормализует через
// idn.NormalizeZone, ТЗ 5.2): DNS-метки через точку, составные зоны
// (co.uk) и IDN-зоны (xn--p1ai) допустимы, unicode-форма — нет (её
// справочник не содержит, ключ никогда бы не сработал). Полная длина
// отдельно — ≤ 253 октет (ТЗ 5.2 п. 4).
var zoneNameRe = regexp.MustCompile(`^[a-z0-9]([a-z0-9-]{0,61}[a-z0-9])?(\.[a-z0-9]([a-z0-9-]{0,61}[a-z0-9])?)*$`)

// Validate проверяет значения конфигурации и возвращает ошибку со списком
// всех проблем сразу (формат «секция.поле: что не так»).
func (c *Config) Validate() error {
	var problems []string

	// postgres
	if strings.TrimSpace(c.Postgres.DSN) == "" {
		problems = append(problems, "postgres.dsn: обязателен (postgres.dsn в YAML или переменные CRAWLER_POSTGRES_DSN/APP_PG_DSN)")
	}
	if !inRange(c.Postgres.PoolSize, 1, 100) {
		problems = append(problems, rangeMsg("postgres.pool_size", c.Postgres.PoolSize, 1, 100)+"; рекомендация ТЗ: 20–50 на все сервисы")
	}

	// redis
	if err := checkHostPort(c.Redis.Addr); err != nil {
		problems = append(problems, fmt.Sprintf("redis.addr: ожидается host:port, задано %q", c.Redis.Addr))
	}
	// 0 — пределы по стримам из пакета queue (T-025); явный предел ниже
	// 1000 усекал бы рабочий набор даже маленькой очереди.
	if c.Redis.MaxLen != 0 && !inRange(c.Redis.MaxLen, 1000, 100000000) {
		problems = append(problems, rangeMsg("redis.maxlen", c.Redis.MaxLen, 1000, 100000000)+" (0 — пределы по стримам по умолчанию)")
	}
	// Минимум 1: ноль доставок означал бы q:dead до первой обработки.
	if !inRange(c.Redis.MaxAttempts, 1, 100) {
		problems = append(problems, rangeMsg("redis.max_attempts", c.Redis.MaxAttempts, 1, 100)+" (ТЗ 4.2: по умолчанию 3)")
	}
	if c.Redis.ClaimMinIdle <= 0 {
		problems = append(problems, nonPositiveDur("redis.claim_min_idle", c.Redis.ClaimMinIdle))
	}

	// ingest
	if c.Ingest.Dir == "" {
		problems = append(problems, "ingest.dir: не задан (наблюдаемая папка incoming)")
	}
	if !inRange(c.Ingest.BatchSize, 1, 1000000) {
		problems = append(problems, rangeMsg("ingest.batch_size", c.Ingest.BatchSize, 1, 1000000))
	}
	if !inRange(c.Ingest.StableSeconds, 1, 3600) {
		problems = append(problems, rangeMsg("ingest.stable_seconds", c.Ingest.StableSeconds, 1, 3600))
	}
	if c.Ingest.ScanInterval < Duration(10*time.Millisecond) || c.Ingest.ScanInterval > Duration(time.Hour) {
		problems = append(problems, fmt.Sprintf("ingest.scan_interval: ожидается %s..%s, задано %s",
			Duration(10*time.Millisecond), Duration(time.Hour), c.Ingest.ScanInterval))
	}
	if c.Ingest.MaxErrorRatio <= 0 || c.Ingest.MaxErrorRatio > 1 {
		problems = append(problems, fmt.Sprintf("ingest.max_error_ratio: ожидается (0, 1], задано %v", c.Ingest.MaxErrorRatio))
	}

	// scheduler
	if c.Scheduler.RecheckInterval <= 0 {
		problems = append(problems, nonPositiveDur("scheduler.recheck_interval", c.Scheduler.RecheckInterval))
	}
	// ТЗ 5.3: пачки планировщика 1000–10000 — границы нормативные
	// (T-027), не рекомендация: пачка задаёт память выборки (ТЗ 3.5) и
	// размер сообщений q:dns.
	if !inRange(c.Scheduler.BatchSize, 1000, 10000) {
		problems = append(problems, rangeMsg("scheduler.batch_size", c.Scheduler.BatchSize, 1000, 10000)+" (ТЗ 5.3)")
	}
	if c.Scheduler.QueueHighWatermark < 1 {
		problems = append(problems, "scheduler.queue_high_watermark: ожидается положительное число, задано "+strconv.Itoa(c.Scheduler.QueueHighWatermark))
	}
	if c.Scheduler.Lease <= 0 {
		problems = append(problems, nonPositiveDur("scheduler.lease", c.Scheduler.Lease))
	}
	if c.Scheduler.TempErrorRetryAfter <= 0 {
		problems = append(problems, nonPositiveDur("scheduler.temp_error_retry_after", c.Scheduler.TempErrorRetryAfter))
	}
	if !inRange(c.Scheduler.TempErrorMaxRetries, 1, 100) {
		problems = append(problems, rangeMsg("scheduler.temp_error_max_retries", c.Scheduler.TempErrorMaxRetries, 1, 100)+"; минимум 1 — temp_error по определению требует повтор (ТЗ 6.2)")
	}
	// Веса зон (ТЗ 5.3, T-106): ключи по возрастанию — детерминированный
	// список проблем. Имя, которого нет в справочнике zones, — не ошибка
	// конфигурации (зоны создаёт ingest первым файлом, ТЗ 5.1):
	// взвешивание просто не срабатывает, разрешение в cmd/scheduler
	// предупреждает Warn-ом. Синтаксически невозможное имя ловим здесь —
	// справочник такого имени содержать не может.
	for _, name := range sortedKeys(c.Scheduler.ZoneWeights) {
		if name == "" || len(name) > 253 || !zoneNameRe.MatchString(name) {
			problems = append(problems, fmt.Sprintf(
				"scheduler.zone_weights: %q — ожидается имя зоны в punycode нижним регистром, как в справочнике zones (составные — co.uk, IDN — xn--p1ai)", name))
			continue
		}
		if w := c.Scheduler.ZoneWeights[name]; !inRange(w, 1, 1000) {
			problems = append(problems, fmt.Sprintf(
				"scheduler.zone_weights: зона %q: ожидается вес 1..1000, задано %d (доля зоны в пачке пропорциональна весу; 1 — равный круговой порядок)", name, w))
		}
	}

	// dns
	if len(c.DNS.Resolvers) == 0 {
		problems = append(problems, "dns.resolvers: нужен хотя бы один резолвер (например, локальный unbound)")
	}
	for _, r := range c.DNS.Resolvers {
		if err := checkHostPort(r); err != nil {
			problems = append(problems, fmt.Sprintf("dns.resolvers: %q: ожидается host:port", r))
		}
	}
	for _, r := range c.DNS.FallbackResolvers {
		if err := checkHostPort(r); err != nil {
			problems = append(problems, fmt.Sprintf("dns.fallback_resolvers: %q: ожидается host:port", r))
		}
	}
	if !inRange(c.DNS.Concurrency, 1, 100000) {
		problems = append(problems, rangeMsg("dns.concurrency", c.DNS.Concurrency, 1, 100000))
	}
	if !inRange(c.DNS.MaxQPS, 1, 1000000) {
		problems = append(problems, rangeMsg("dns.max_qps", c.DNS.MaxQPS, 1, 1000000))
	}
	if c.DNS.Timeout <= 0 {
		problems = append(problems, nonPositiveDur("dns.timeout", c.DNS.Timeout))
	}
	if c.DNS.TCPTimeout <= 0 {
		problems = append(problems, nonPositiveDur("dns.tcp_timeout", c.DNS.TCPTimeout))
	}
	// Минимум 1 — ТЗ 5.4 ретраи требует; максимум — dnsclient.MaxRetries.
	if !inRange(c.DNS.Retries, 1, 10) {
		problems = append(problems, rangeMsg("dns.retries", c.DNS.Retries, 1, 10)+"; минимум 1 — ТЗ 5.4 требует ретраев")
	}
	if c.DNS.RetryDelay <= 0 {
		problems = append(problems, nonPositiveDur("dns.retry_delay", c.DNS.RetryDelay))
	}
	if !inRange(c.DNS.UDPPoolSize, 1, 100000) {
		problems = append(problems, rangeMsg("dns.udp_pool_size", c.DNS.UDPPoolSize, 1, 100000))
	}

	// http
	if !inRange(c.HTTP.Concurrency, 1, 100000) {
		problems = append(problems, rangeMsg("http.concurrency", c.HTTP.Concurrency, 1, 100000))
	}
	if !inRange(c.HTTP.PerIPLimit, 1, 64) {
		problems = append(problems, rangeMsg("http.per_ip_limit", c.HTTP.PerIPLimit, 1, 64)+"; рекомендация ТЗ: 3–4")
	}
	if c.HTTP.ConnectTimeout <= 0 {
		problems = append(problems, nonPositiveDur("http.connect_timeout", c.HTTP.ConnectTimeout))
	}
	if c.HTTP.TLSHandshakeTimeout <= 0 {
		problems = append(problems, nonPositiveDur("http.tls_handshake_timeout", c.HTTP.TLSHandshakeTimeout))
	}
	if c.HTTP.HeaderTimeout <= 0 {
		problems = append(problems, nonPositiveDur("http.header_timeout", c.HTTP.HeaderTimeout))
	}
	if c.HTTP.TotalTimeout <= 0 {
		problems = append(problems, nonPositiveDur("http.total_timeout", c.HTTP.TotalTimeout))
	}
	if !inRange(c.HTTP.MaxRedirects, 0, 100) {
		problems = append(problems, rangeMsg("http.max_redirects", c.HTTP.MaxRedirects, 0, 100)+" (0 — не следовать редиректами)")
	}
	if c.HTTP.MaxBodyBytes <= 0 {
		problems = append(problems, "http.max_body_bytes: ожидается положительный размер, задано "+c.HTTP.MaxBodyBytes.String())
	}
	if c.HTTP.MinBodySpeed <= 0 {
		problems = append(problems, "http.min_body_speed: ожидается положительная скорость (байт/с), задано "+c.HTTP.MinBodySpeed.String())
	}
	if !inRange(c.HTTP.MaxRPS, 1, 1000000) {
		problems = append(problems, rangeMsg("http.max_rps", c.HTTP.MaxRPS, 1, 1000000))
	}
	if strings.TrimSpace(c.HTTP.UserAgent) == "" {
		problems = append(problems, "http.user_agent: обязателен (честный User-Agent с контактами, ТЗ 5.5/§9)")
	}
	// Значение идёт в заголовок запроса одним полем: переводы строк и
	// управляющие символы ломали бы запрос (а \r\n — инъекция заголовков).
	for _, r := range c.HTTP.UserAgent {
		if r < 0x20 || r == 0x7f {
			problems = append(problems, "http.user_agent: переводы строк и управляющие символы недопустимы (значение ставится в заголовок)")
			break
		}
	}
	if n := len(c.HTTP.UserAgent); n > maxUserAgentLen {
		problems = append(problems, fmt.Sprintf("http.user_agent: слишком длинный (%d байт, максимум %d) — имя и контакты, не текст", n, maxUserAgentLen))
	}

	// render
	if c.Render.RemoteURL != "" {
		u, err := url.Parse(c.Render.RemoteURL)
		if err != nil || u.Host == "" {
			problems = append(problems, fmt.Sprintf("render.remote_url: ожидается URL вида ws://host:9222, задано %q", c.Render.RemoteURL))
		}
	}
	if !inRange(c.Render.Contexts, 1, 16) {
		problems = append(problems, rangeMsg("render.contexts", c.Render.Contexts, 1, 16)+"; рекомендация ТЗ: 2–4")
	}
	if c.Render.Timeout <= 0 {
		problems = append(problems, nonPositiveDur("render.timeout", c.Render.Timeout))
	}
	if !inRange(c.Render.RestartEveryPages, 1, 1000000) {
		problems = append(problems, rangeMsg("render.restart_every_pages", c.Render.RestartEveryPages, 1, 1000000))
	}
	// 0 — контроль отключён (remote-режим и малые машины); рабочий диапазон —
	// от холостого RSS headless-shell с контекстами (ниже — краш-луп
	// перезапусков) до разумного бюджета одного воркера (ТЗ 3.2).
	if t := c.Render.RestartMemoryBytes; t != 0 && (t < 256<<20 || t > 16<<30) {
		problems = append(problems, fmt.Sprintf("render.restart_memory_bytes: ожидается 0 (отключено) или %s..%s, задано %s",
			ByteSize(256<<20), ByteSize(16<<30), t))
	}

	// content
	if c.Content.Dir == "" {
		problems = append(problems, "content.dir: не задан (каталог пак-файлов)")
	}
	if c.Content.PackSize < 1<<20 || c.Content.PackSize > 8<<30 {
		problems = append(problems, fmt.Sprintf("content.pack_size: ожидается %s..%s, задано %s (рекомендация ТЗ: 512 МБ–1 ГБ)",
			ByteSize(1<<20), ByteSize(8<<30), c.Content.PackSize))
	}
	if c.Content.MaxStoreBytes <= 0 || c.Content.MaxStoreBytes > 16<<20 {
		problems = append(problems, fmt.Sprintf("content.max_store_bytes: ожидается %s..%s, задано %s (рекомендация ТЗ: 1–2 МБ)",
			ByteSize(1), ByteSize(16<<20), c.Content.MaxStoreBytes))
	}
	// Порог переполнения диска (ТЗ 8): 0 выключал бы обязательную для ТЗ
	// паузу записи, > 95 останавливал бы запись на заведомо живом томе.
	if c.Content.DiskFreePercent <= 0 || c.Content.DiskFreePercent > 95 {
		problems = append(problems, fmt.Sprintf("content.disk_free_percent: ожидается (0, 95], задано %v", c.Content.DiskFreePercent))
	}

	// analyzer
	if c.Analyzer.RulesDir == "" {
		problems = append(problems, "analyzer.rules_dir: не задан (каталог YAML-правил Laravel)")
	}
	if c.Analyzer.LaravelMinScore <= 0 || c.Analyzer.LaravelMinScore > 1 {
		problems = append(problems, fmt.Sprintf("analyzer.laravel_min_score: ожидается (0, 1], задано %v", c.Analyzer.LaravelMinScore))
	}

	// subdomains
	switch {
	case len(c.Subdomains) == 0:
		problems = append(problems, "subdomains: список не может быть пустым (нужен хотя бы апекс — пустая строка)")
	case len(c.Subdomains) > maxSubdomains:
		problems = append(problems, fmt.Sprintf("subdomains: не более %d (битовая маска dns_mask), задано %d", maxSubdomains, len(c.Subdomains)))
	}
	seen := make(map[string]bool, len(c.Subdomains))
	for _, s := range c.Subdomains {
		if s != "" && !subdomainLabelRe.MatchString(s) {
			problems = append(problems, fmt.Sprintf("subdomains: %q — ожидается DNS-метка строчными буквами без точек (пустая строка — апекс)", s))
		}
		if seen[s] {
			problems = append(problems, fmt.Sprintf("subdomains: повтор %q", s))
		}
		seen[s] = true
	}

	// monitor
	if err := checkHostPort(c.Monitor.Listen); err != nil {
		problems = append(problems, fmt.Sprintf("monitor.listen: ожидается [host]:port, задано %q", c.Monitor.Listen))
	}
	if c.Monitor.ZoneStatsInterval <= 0 {
		problems = append(problems, nonPositiveDur("monitor.zone_stats_interval", c.Monitor.ZoneStatsInterval))
	}
	if c.Monitor.IngestMetricsURL != "" {
		if u, err := url.Parse(c.Monitor.IngestMetricsURL); err != nil ||
			(u.Scheme != "http" && u.Scheme != "https") || u.Host == "" {
			problems = append(problems, fmt.Sprintf(
				"monitor.ingest_metrics_url: ожидается абсолютный http(s)-адрес, задано %q", c.Monitor.IngestMetricsURL))
		}
	}

	// log (расширение каркаса, T-012)
	switch c.Log.Level {
	case "", "debug", "info", "warn", "error": // "" — конфиг без Default()
	default:
		problems = append(problems, fmt.Sprintf("log.level: ожидается debug|info|warn|error, задано %q", c.Log.Level))
	}

	if len(problems) == 0 {
		return nil
	}
	return fmt.Errorf("невалидные значения (%d): %s", len(problems), strings.Join(problems, "; "))
}

// checkHostPort проверяет форму host:port (адреса резолверов, redis,
// адрес прослушивания): пустой host допустим (":9100" — все интерфейсы).
func checkHostPort(s string) error {
	_, port, err := net.SplitHostPort(s)
	if err != nil {
		return err
	}
	n, err := strconv.Atoi(port)
	if err != nil || n < 1 || n > 65535 {
		return fmt.Errorf("порт %q", port)
	}
	return nil
}

func inRange(v, lo, hi int) bool { return v >= lo && v <= hi }

// sortedKeys — ключи карты по возрастанию (детерминированный порядок
// сообщений валидации; Go-карты не упорядочены).
func sortedKeys(m map[string]int) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

func rangeMsg(field string, v, lo, hi int) string {
	return fmt.Sprintf("%s: ожидается %d..%d, задано %d", field, lo, hi, v)
}

func nonPositiveDur(field string, d Duration) string {
	return fmt.Sprintf("%s: ожидается положительная длительность, задано %s", field, d)
}
