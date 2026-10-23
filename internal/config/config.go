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

// Границы нормативной вилки пачек планировщика (ТЗ 5.3: «пачками
// (по умолчанию 1000–10 000)»). Неэкспортированные константы — единый
// источник для валидации и пин-теста бюджетного инварианта стримов
// (TestSchedulerBatchFitsStreamBudget): потолок q:dns пропорционален
// пачке, и расширение вилки обязано перепроверять бюджет.
const (
	minSchedulerBatchSize = 1000
	maxSchedulerBatchSize = 10000
)

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
	Web        Web       `yaml:"web"`
	Dashboard  Dashboard `yaml:"dashboard"`
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
	// RecoverBatch — пачка клейма XAUTOCLAIM за один проход RecoverLoop
	// (T-153): темп возврата зависших записей в работу — batch/interval
	// за проход; задаёт время самовосстановления после инцидентов
	// (≈ размер PEL / (batch/interval)). Вместе с пачкой соблюдается
	// инвариант «время последовательного обхода пачки (худшая запись
	// этапа × batch) < claim_min_idle», иначе хвост пачки перклеймивается
	// соседними воркерами и жжёт попытки — расчёт в docs/configuration.md
	// (раздел redis).
	RecoverBatch int `yaml:"recover_batch"`
	// RecoverInterval — период опроса зависших записей RecoverLoop при
	// неполной пачке (T-153); полная пачка дренируется без паузы.
	RecoverInterval Duration `yaml:"recover_interval"`
	// TrimInterval — период освобождения подтверждённого префикса
	// стримов (T-141, ТЗ 4.2 «Дополнение 2026-10-23»): XACK не удаляет
	// записи из стрима, и подтверждённая история копилась бы до потолка
	// MAXLEN, переполняя бюджет Redis (ТЗ 3.2 — инцидент T-093).
	// Цикл планировщика (queue.TrimLoop) раз в период делает XTRIM MINID
	// по старейшей записи, обязанной остаться (старейшая pending группы;
	// при пустом pending — last-delivered-id): удаляется только
	// доставленное и подтверждённое. Стоимость прохода — три команды
	// Redis на стрим.
	TrimInterval Duration `yaml:"trim_interval"`
	// ConsumerIdle — порог неактивности консьюмера группы для чистки
	// (T-155): после рестартов воркеров их консьюмеры (имена с pid)
	// остаются в группах навсегда; цикл планировщика
	// (queue.CleanupConsumersLoop, период — тот же TrimInterval)
	// удаляет XGROUP DELCONSUMER строго пустых (pending = 0) и
	// неактивных дольше порога. Порог — защита консьюмеров живых
	// воркеров под нагрузкой: удаление пустого неактивного безвредно
	// (имя пересоздаётся первым чтением с записями). Диапазон
	// 1m..720h (30 дней).
	ConsumerIdle Duration `yaml:"consumer_idle"`
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
	// ArchiveMaxUncompressed — потолок суммарного несжатого объёма членов
	// входного архива (T-133, защита от zip-бомб): для zip проверяется до
	// чтения по заголовку UncompressedSize64, для tar/gz — счётчиком по
	// потоку. Расширение к минимальному составу ТЗ 5.10.
	ArchiveMaxUncompressed ByteSize `yaml:"archive_max_uncompressed"`
	// ArchiveMaxFiles — потолок числа записей входного архива (T-133):
	// превышение — архив целиком в failed/; для zip проверяется по
	// центральному каталогу (он читается в память — число записей и есть
	// предел расходов), для tar — по потоку. Расширение к минимальному
	// составу ТЗ 5.10.
	ArchiveMaxFiles int `yaml:"archive_max_files"`
}

// Scheduler — планировщик проверок (ТЗ 5.3).
type Scheduler struct {
	RecheckInterval     Duration `yaml:"recheck_interval"`
	BatchSize           int      `yaml:"batch_size"`
	QueueHighWatermark  int      `yaml:"queue_high_watermark"`
	Lease               Duration `yaml:"lease"`
	TempErrorRetryAfter Duration `yaml:"temp_error_retry_after"`
	// BacklogInterval — период цикла коллектора глубины backlog
	// (scheduler.BacklogLoop, T-077): gauge crawler_backlog_domains из
	// GROUP BY check_state по всем живым доменам. Дефолт 5 минут
	// (T-161): исходные 30 с коридора ТЗ 5.9 (30–60 с) рассчитаны на
	// малый корпус — на 47 млн строк (замер §12
	// docs/load-measurements.md) проход 17+ с каждые 30 с вместе с
	// пересчётом зон и агрегатами status — большая часть CPU postgres;
	// gauge глубины на дашборде допускает пятиминутную свежесть.
	// Коридор валидации 30с..1ч: меньше 30 с — агрегат чаще, чем
	// допускает ТЗ 5.9, больше часа — глубина перестаёт быть наблюдаемой
	// величиной. Расширение к минимальному составу ТЗ 5.10: новое поле
	// внутри секции scheduler.
	BacklogInterval Duration `yaml:"backlog_interval"`
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
	// TaskConcurrency — одновременных задач q:http (доменов) в цикле
	// http-worker (T-139): задачный параллелизм этапа, в отличие от
	// транспортного Concurrency (соединения/RPS — внутренний httpfetch,
	// T-049). Потребление скользящее — потолок этапа ≈ задач /
	// средняя длительность задачи; в памяти каждая задача держит
	// результат домена (тела записей-оригиналов, каждое ≤ MaxBodyBytes)
	// + буферы, бюджет памяти этапа = TaskConcurrency × пиковая память
	// задачи. Дефолт 192 (T-157; прежний 256 — по замерам T-092,
	// docs/load-measurements.md §3): сутки на 256 слотах дали два
	// фатала юнита (замер §12: 05:48 OOM-kill, 09:17 выход на пик
	// 900.5 MiB = MemoryMax; MemoryHigh=850M — 120 095 событий
	// троттлинга, reclaim-штормы и swap замедляют воркер) при
	// ≈3.34 MiB на слот; 192 слота устойчивы (пропитка этапа 22:
	// 860–893 MiB, oom=0, NRestarts=0), скорость ≈17–19 дом/с
	// против ≈22 на 256 — устойчивость важнее отложенной цели
	// 50 дом/с. Временно до T-163 (стриминг тел — снижение пиковой
	// памяти слота), после него вернуть 256 по замеру;
	// 256 × TaskPerDomain 8 = 2048 ≈ транспортный Concurrency 2000 —
	// окно задач заполняет бюджет соединений, не превышая. Расширение
	// к минимальному составу ТЗ 5.10 (ТЗ §13.2: значения по умолчанию
	// уточняются замерами).
	TaskConcurrency int `yaml:"task_concurrency"`
	// TaskConsume — записей q:http за одно чтение (T-139): размер пачки
	// XREADGROUP и буфера предвыборки скользящего потребления — чтение
	// идёт вперёд обработки не глубже пары пачек (текущая в руке
	// читателя + буфер), следующее чтение происходит при освобождении
	// слотов задач; окно чтения параллелизм не ограничивает (16 ≈ 3
	// чтения/с при фактических 47.6 дом/с, §11). Это же число —
	// порог предохранителя цикла: столько доставок подряд без единого
	// подтверждения приостанавливает чтение с удвоением паузы (малые
	// значения делают предохранитель чувствительнее — меняйте осознанно).
	TaskConsume int `yaml:"task_consume"`
	// TaskPerDomain — параллельных проверок (субдомен × протокол) внутри
	// задачи домена (пул горутин ProcessDomain): субдомены одного домена
	// почти всегда живут на одних и тех же IP — реальный параллелизм
	// подключения к одному IP ограничен пер-IP семафором транспорта
	// (PerIPLimit, рекомендация ТЗ 5.5: 3–4); 8 = удвоенный потолок
	// покрывает домены с несколькими адресами, не выделяя одному домену
	// несправедливую долю бюджета. Расширение к минимальному составу
	// ТЗ 5.10 (T-139).
	TaskPerDomain int `yaml:"task_per_domain"`
}

// Render — параметры рендера (ТЗ 5.6). RemoteURL — расширение dev-окружения:
// адрес внешнего Chromium по CDP (ws://…); в боевом режиме воркер запускает
// браузер сам и поле остаётся пустым.
type Render struct {
	RemoteURL         string   `yaml:"remote_url"`
	Contexts          int      `yaml:"contexts"`
	Timeout           Duration `yaml:"timeout"`
	TimeoutAttempts   int      `yaml:"timeout_attempts"`
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
	// Retention — окно жизни контента и jsonb не-находок (T-166; ТЗ 5.7
	// «контент хранится не менее времени, пока нужно переприменять
	// правила», решение оператора 2026-10-23: диск без новых мощностей):
	// записи subdomain_results старше окна, не Laravel-находки и не ждущие
	// рендера теряют content_hash и headers/cookies/tls/redirects/addrs/
	// cname → NULL; паспорт строки остаётся всегда. Стационарный объём
	// контента = суточный приток × окно (20 GiB/сут: 6 ч ≈ 5 GiB, 48 ч
	// съели бы весь том). 0 — retention выключен. Расширение к
	// минимальному составу ТЗ 5.10.
	Retention Duration `yaml:"retention"`
	// RetentionInterval — период проходов цикла RetentionLoop (T-166);
	// место контенту возвращает часовая компактификация паков (T-045),
	// retention её «кормит» — чаще часа незачем.
	RetentionInterval Duration `yaml:"retention_interval"`
	// RetentionBatch — строк в одной пачке-транзакции retention
	// (T-166): короткие транзакции и HOT-апдейты (fillfactor 85,
	// миграция 000002); рабочая вилка 10–50 тыс. по постановке.
	RetentionBatch int `yaml:"retention_batch"`
	// RetentionAnalyzeWatermark — порог интерлока retention: глубина
	// q:analyze (backlog группы потребления) ≥ порога — анализатор не
	// успевает, тела и headers/cookies ещё нужны, проход пропускается.
	// 0 — общий порог scheduler.queue_high_watermark (тем же критерием
	// «строго ниже порога» работает гейт публикации q:analyze, T-026);
	// интерлок выключается явным большим значением на свой страх.
	RetentionAnalyzeWatermark int `yaml:"retention_analyze_watermark"`
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

// Web — публичный сервис лендинга (этап 13, T-121; продуктовое
// расширение вне основного ТЗ — перенос из larasites). Публичный
// слушатель — собственный http.Server внутри OnRun: каркас service даёт
// один порт на сервис и вешает туда /metrics (выставлять наружу нельзя),
// поэтому MetricsAddr остаётся на loopback, а web.listen обслуживает
// посетителя (прецедент управления жизненным циклом внутри OnRun —
// клиенты воркеров).
type Web struct {
	// Listen — публичный адрес лендинга: [host]:port или абсолютный путь
	// unix-сокета (вариант деплоя за nginx, T-124). Дефолт — loopback:
	// публикация наружу — reverse-proxy с TLS (deploy/nginx/).
	Listen string `yaml:"listen"`
	// RateLimit — лимит живых проверок формы на IP, запросов в минуту
	// (зафиксированное решение этапа 13: 4/мин, как larasites).
	RateLimit int `yaml:"rate_limit"`
	// Timeout — потолок живой проверки формы (T-123): полный конвейер
	// одного имени (DNS → HTTP → рендер SPA → анализ), ответ посетителю
	// не позже потолка.
	Timeout Duration `yaml:"timeout"`
}

// Log — уровень логирования сервисов (каркас internal/service). Расширение
// к минимальному составу ТЗ 5.10: уровень нужен каркасу, а не отдельному
// этапу, поэтому вынесен в свою секцию верхнего уровня.
type Log struct {
	Level string `yaml:"level"` // debug | info | warn | error
}

// Dashboard — операторский веб-дашборд конвейера (ТЗД §8.1; этап 15,
// T-127). Username/PasswordHash общим Validate не проверяются:
// обязательность кредов — проверка запуска cmd/dashboard (ТЗД §7.7),
// иначе требование коснулось бы всех сервисов проекта.
type Dashboard struct {
	// Listen — публичный адрес дашборда ([host]:port): страницы и API
	// (ТЗД §4.2). Дефолт — loopback: публикация наружу — reverse-proxy
	// с TLS (ТЗД §9.1), не сам сервис.
	Listen string `yaml:"listen"`
	// Refresh — период фоновой сборки снимков состояния (ТЗД §4.4),
	// коридор 1s..5m (ТЗД §8.3).
	Refresh Duration `yaml:"refresh"`
	// StatusCacheTTL — TTL кэша тяжёлых агрегатов status (зоны/темп/
	// статусы — полные проходы domains, T-161): в окне TTL проходы
	// refresh переиспользуют последний ответ БД, по истечении — одна
	// выборка на секцию. Коридор 1m..5m (постановка T-161): меньше
	// минуты кэш не спасает на дефолтном refresh 5 с, больше 5 минут —
	// сводка теряет оперативность (числа конвейера меняются секундами).
	StatusCacheTTL Duration `yaml:"status_cache_ttl"`
	// Username — логин оператора (HTTP Basic, ТЗД §7.1).
	Username string `yaml:"username"`
	// PasswordHash — хэш пароля формата ТЗД §7.2 (генерация —
	// crawlerctl dashboard-secret, разбор/сверка — internal/dashboard/
	// auth); открытый пароль в конфиге и окружении запрещён.
	PasswordHash string `yaml:"password_hash"`
	// Probes — пробы живости сервисов конвейера для секции здоровья
	// (ТЗД §4.3, §5.8): имя → базовый URL метрик сервиса (каркас
	// internal/service отдаёт /livez на METRICS_ADDR; в docker-сети —
	// http://<имя-сервиса>:<порт>). nil/пустая карта — секция «н/д».
	Probes map[string]string `yaml:"probes"`
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
			// (рендер, скользящее потребление T-142: предвыборка до
			// 2 пачек +Concurrency ≈ 40 записей; при дефолтных
			// contexts=8/timeout=25 с хвост ждёт ≤ ~2,1 мин; при
			// contexts=1 — до ~13,8 мин, порог поднимают вместе с
			// понижением contexts (числа пересчитаны под дефолт 8,
			// T-169), см. queue.DefaultClaimMinIdle).
			ClaimMinIdle: Duration(10 * time.Minute),
			// Темп возврата зависших (T-153): 64 за проход — нижняя грань
			// поднятой пачки, согласованная с инвариантом «обход пачки
			// < claim_min_idle»: типовая запись рендера ~5 с × 64 ≈ 5,3 мин
			// < 10 мин (запас ×1,9); 128 типовым случаем уже не укладывается
			// (≈10,7 мин). Обоснование и цена повтора — queue.
			// DefaultRecoverBatch (синхронный дефолт очереди).
			RecoverBatch: 64,
			// 30 с — латентность возврата MinIdle + до одного интервала
			// (queue.DefaultRecoverInterval, синхронный дефолт очереди).
			RecoverInterval: Duration(30 * time.Second),
			// Минута — единицы процентов бюджета Redis между проходами
			// при текущих темпах этапов (queue.DefaultTrimInterval).
			TrimInterval: Duration(time.Minute),
			// 2 ч — с запасом поверх затишья между доставками активного
			// этапа (секунды) и короче междеплойного интервала
			// (queue.DefaultConsumerIdle, T-155).
			ConsumerIdle: Duration(2 * time.Hour),
		},
		Ingest: Ingest{
			Dir:           "/data/incoming",
			BatchSize:     50000,
			StableSeconds: 10,
			ScanInterval:  Duration(30 * time.Second),
			// Единичный мусор файлу не вредит, а файл не в той зоне
			// или битый дамп дают долю ошибок ~1 (T-016).
			MaxErrorRatio: 0.1,
			// Несжатая зона .com ≈ 1,5 ГБ — потолок с запасом на рост
			// и склейки, без чрезмерного окна для zip-бомб (T-133).
			ArchiveMaxUncompressed: ByteSize(8 << 30),
			// Реальные дампы зон — единицы файлов (T-133).
			ArchiveMaxFiles: 1000,
		},
		Scheduler: Scheduler{
			RecheckInterval: Duration(4380 * time.Hour), // ≈ 6 месяцев
			// Нижняя нормативная граница ТЗ 5.3 (1000–10000) и размер
			// замеренной записи q:dns ≈ 64 КиБ (T-141): пачка — единица
			// повторной обработки при возврате зависшей доставки
			// (XAUTOCLAIM возвращает пачку целиком) и повторной выборки
			// по lease, меньшая пачка — меньшее окно повтора; потолки
			// памяти стримов согласованы с этой величиной
			// (queue.DefaultStreamMaxLen, docs/redis.md §3.1).
			BatchSize:           1000,
			QueueHighWatermark:  100000,
			Lease:               Duration(30 * time.Minute),
			TempErrorRetryAfter: Duration(24 * time.Hour),
			TempErrorMaxRetries: 3,
			// 5 минут (T-161, замер §12): проход GROUP BY по 47 млн строк
			// 17+ с каждые 30 с — одна из трёх главных статей CPU
			// postgres; gauge дашборда допускает пятиминутную свежесть
			// (обоснование — internal/scheduler/backlog.go).
			BacklogInterval: Duration(5 * time.Minute),
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
			// Задачный параллелизм (T-139; исходный дефолт 256 —
			// замеры T-092 §3/§11: 47.6 дом/с, anon ≈855 MiB).
			// Понижен до 192 (T-157): сутки на 256 слотах — два
			// фатала юнита (§12: 05:48 OOM-kill, 09:17 пик
			// 900.5 MiB = MemoryMax; MemoryHigh 850M — 120 095
			// reclaim-событий); 192 слота устойчивы (этап 22:
			// 860–893 MiB, oom=0, NRestarts=0), скорость
			// ≈17–19 дом/с. Временно до T-163 (стриминг тел —
			// ниже память слота), после — вернуть 256 по замеру
			// (256 × 8 проверок = 2048 ≈ транспортный потолок
			// соединений 2000).
			TaskConcurrency: 192,
			// Пачка чтения и буфер предвыборки: окно чтения не
			// ограничивает параллелизм (скользящее потребление), 16
			// сглаживает круговые пути к Redis.
			TaskConsume: 16,
			// Пул внутри задачи: per-IP семафор (4) всё равно режет
			// реальный параллелизм подключений (ТЗ 5.5: 3–4 на IP).
			TaskPerDomain: 8,
		},
		Render: Render{
			RemoteURL: "",
			// 8 (T-169) — осознанное превышение рекомендации ТЗ 5.6
			// (2–4) по замеру, прецедент ступени 3→4 — T-142. Спрос при
			// свободном гейте ≈1,2 стр/с (замер §12,
			// docs/load-measurements.md), ёмкость 4 контекстов ≈0,8
			// стр/с (5,1 с/страница, T-092 §4): дренаж q:render 29–34
			// зад/мин сравнялся с притоком — лаг ~99 тыс. у watermark
			// 100К не таял с 22.10 (диск сцеплен с дренажом: когорта
			// retention ждёт потребления сообщений). 8 ≈ 1,6 стр/с —
			// выше спроса. Память: пик RSS Chromium при contexts=3 —
			// 1024 MiB (T-092 §6), при 4 контекстах ~550 МБ (замер
			// 23.10) — рост сублинейный (базовый процесс общий),
			// порог перезапуска restart_memory_bytes 1,5 ГиБ и cgroup
			// 2 ГБ (ТЗ 3.2) держат; контроль RSS раз в задачу (T-065)
			// + повторный замер после деплоя — при систематическом
			// проживании у порога один Chromium насыщен, ответ —
			// второй Chromium (:9223) и второй воркер против него
			// (deploy.md §2.2), не порог. Юнит render-worker держит
			// DOM в полёте ≤ Concurrency × max_store_bytes (≈16 МиБ +
			// transient-копии): 128M уже впритык (§6: max 128 MiB, к
			// 23.10 117–128 МБ + swap) — MemoryMax 256M (T-169,
			// deploy.md §2).
			Contexts: 8,
			Timeout:  Duration(25 * time.Second),
			// Тарпит-бюджет (T-148): 2 доставки из 3 (redis.max_attempts)
			// могут завершиться таймаутом страницы, тарпит на следующей —
			// запись финализируется без рендера: работа не теряется в
			// q:dead, слоты не заняты сутками. Первая попытка может
			// таймаутнуться от затора (замер 2026-10-23: 0.34 стр/с, слоты
			// в таймаутах) — повтор через redis.claim_min_idle даёт
			// странице шанс в изменившихся условиях, дальше повтор
			// предсказуем. Инцидент 2026-10-23: 5507 dead-записей q:render
			// «исчерпан лимит попыток 3» за часы — все таймауты.
			TimeoutAttempts:   2,
			RestartEveryPages: 500,
			// 1,5 ГиБ = 75 % бюджета Chromium ≈ 2 ГБ (ТЗ 3.2, cgroup
			// MemoryMax ≈ 2 ГБ): перезапуск — до cgroup-OOM; запас — на
			// рост между проверками (раз в задачу) и tmpfs /dev/shm,
			// который учитывается в памяти cgroup, но не в RSS процессов.
			// При contexts=8 порог не поднимается (T-169): проверка раз
			// в задачу — страницы в полёте наращивают RSS и после
			// последней проверки, запас 512 МиБ покрывает этот рост;
			// сузить его подъёмом порога — приблизить мягкий перезапуск
			// к жёсткому OOM cgroup. Систематическое проживание RSS выше
			// порога — сигнал насыщения одного Chromium: ответ — второй
			// Chromium (:9223) и второй воркер (deploy.md §2.2), не
			// подъём порога.
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
			// Окно жизни контента не-находок (T-166, замеры §12
			// docs/load-measurements.md): 6 ч ≈ 5 GiB стационарно при
			// притоке 20 GiB/сут — влезает в том без расширения, лаги
			// потребителей (анализ — минуты, рендер-очередь — часы)
			// перекрываются с запасом; обоснование — docs/content.md
			// «Retention контента и трим не-находок».
			Retention: Duration(6 * time.Hour),
			// Час, как компактификация паков (T-045): место возвращает её
			// проход, retention лишь готовит мёртвые блоки.
			RetentionInterval: Duration(time.Hour),
			// Нижняя граница вилки постановки (10–50 тыс.): ~13 пачек
			// в часовом проходе дренируют приток ≈3 млн строк/сут.
			RetentionBatch: 10000,
			// 0 — общий порог scheduler.queue_high_watermark (интерлок
			// тем же критерием, что гейт публикации q:analyze, T-026).
			RetentionAnalyzeWatermark: 0,
		},
		Analyzer: Analyzer{
			RulesDir:        "/etc/crawler/rules",
			LaravelMinScore: 0.6,
		},
		// Начальное наполнение таблицы subdomains; "" — апекс домена.
		Subdomains: []string{"", "www", "admin", "crm", "api", "app"},
		Monitor: Monitor{
			Listen: ":9100",
			// 5 минут (T-161, замер §12): UPDATE-агрегат по 47 млн строк
			// каждые 60 с — одна из трёх главных статей CPU postgres;
			// счётчики зон читают status/лендинг/партиционирование —
			// пятиминутная свежесть достаточна (обоснование —
			// internal/monitor/zonestats.go). Тот же период у цикла
			// алертов (порог остановки прогресса 5× интервал = 25 мин).
			ZoneStatsInterval: Duration(5 * time.Minute),
			// Loopback ingest по METRICS_ADDR (systemd-юниты привязывают
			// 127.0.0.1:9101); docker-сети переопределяют именем сервиса
			// (config.dev.yaml: http://ingest:9101/metrics).
			IngestMetricsURL: "http://127.0.0.1:9101/metrics",
		},
		Web: Web{
			// Loopback: публикация наружу — reverse-proxy с TLS (T-124),
			// не сам сервис.
			Listen: "127.0.0.1:8080",
			// Зафиксированное решение этапа 13 — как троттлинг формы
			// larasites: 4 проверки в минуту с одного IP.
			RateLimit: 4,
			// Полный конвейер одного имени: DNS-эпизод (секунды) + HTTP
			// обеих схем (2 × http.total_timeout 20 с) + рендер SPA
			// (render.timeout 25 с) + анализ (мгновения) — 60 с покрывает
			// худший случай без вечного ожидания посетителя.
			Timeout: Duration(60 * time.Second),
		},
		Dashboard: Dashboard{
			// Loopback: публикация наружу — reverse-proxy с TLS
			// (ТЗД §9.1), не сам сервис.
			Listen: "127.0.0.1:8090",
			// «Сейчас» операторской сводки (ТЗД §6.4: автообновление
			// фронта ~5 с) без лишней нагрузки на БД (ТЗД §4.4).
			Refresh: Duration(5 * time.Second),
			// Нижняя граница коридора T-161 (1–5 мин): при refresh 5 с —
			// один полный проход domains в минуту вместо двенадцати
			// (обоснование — internal/status/cache.go).
			StatusCacheTTL: Duration(time.Minute),
			// Креды не заданы: обязательность проверяет cmd/dashboard
			// на старте (ТЗД §7.7), не общий Validate.
			Username:     "",
			PasswordHash: "",
			// Пробы не заданы — секция здоровья «н/д» (ТЗД §5.8).
			Probes: nil,
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
	// Ключи проб дашборда — имена сервисов в нижнем регистре без пробелов
	// (как имена в docker-сети и заголовок секции здоровья); значения —
	// URL проб с обрезанными краевыми пробелами. Карта пересобирается по
	// возрастанию ключей: столкновение после приведения («Mon» и «mon» в
	// одном YAML) разрешается детерминированно — побеждает
	// лексикографически больший ключ (как у весов зон).
	if len(c.Dashboard.Probes) > 0 {
		names := make([]string, 0, len(c.Dashboard.Probes))
		for name := range c.Dashboard.Probes {
			names = append(names, name)
		}
		sort.Strings(names)
		p := make(map[string]string, len(names))
		for _, name := range names {
			p[strings.ToLower(strings.TrimSpace(name))] = strings.TrimSpace(c.Dashboard.Probes[name])
		}
		c.Dashboard.Probes = p
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
	// Темп возврата зависших (T-153): минимум 1 — клейм за проход;
	// потолок 1000 — от опечаток (реальный потолок — инвариант
	// «batch × худшее время записи < claim_min_idle», он арифметический,
	// а не диапазонный — см. docs/configuration.md, раздел redis).
	if !inRange(c.Redis.RecoverBatch, 1, 1000) {
		problems = append(problems, rangeMsg("redis.recover_batch", c.Redis.RecoverBatch, 1, 1000))
	}
	// Чаще секунды — холостые проходы по четырём стримам (как у
	// trim_interval); реже часа — обход PEL после инцидента затягивается
	// на сутки (5К записей / 64 за проход × 1 ч ≈ 78 ч).
	if c.Redis.RecoverInterval < Duration(time.Second) || c.Redis.RecoverInterval > Duration(time.Hour) {
		problems = append(problems, fmt.Sprintf("redis.recover_interval: ожидается %s..%s, задано %s",
			Duration(time.Second), Duration(time.Hour), c.Redis.RecoverInterval))
	}
	// Коридор осмысленных значений: чаще секунды — холостые проходы по
	// четырём стримам, реже суток — история копится до потолка MAXLEN
	// (защита всё равно есть, но теряется смысл механизма).
	if c.Redis.TrimInterval < Duration(time.Second) || c.Redis.TrimInterval > Duration(24*time.Hour) {
		problems = append(problems, fmt.Sprintf("redis.trim_interval: ожидается %s..%s, задано %s",
			Duration(time.Second), Duration(24*time.Hour), c.Redis.TrimInterval))
	}
	// Гигиена консьюмеров (T-155): порог меньше минуты вычищал бы
	// консьюмеров живых воркеров между чтениями пустого стрима (XREADGROUP
	// без записей idle консьюмера не обновляет) — холостой churn
	// пересоздания; больше 30 дней — мёртвые копятся быстрее деплоев,
	// механизму нет смысла.
	if c.Redis.ConsumerIdle < Duration(time.Minute) || c.Redis.ConsumerIdle > Duration(720*time.Hour) {
		problems = append(problems, fmt.Sprintf("redis.consumer_idle: ожидается %s..%s, задано %s",
			Duration(time.Minute), Duration(720*time.Hour), c.Redis.ConsumerIdle))
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
	// Лимиты архивов (T-133) нельзя отключить нулём: защита от zip-бомб
	// обязательна; верхняя граница — от опечаток, не от эксплуатации.
	if c.Ingest.ArchiveMaxUncompressed <= 0 || c.Ingest.ArchiveMaxUncompressed > 1<<40 {
		problems = append(problems, fmt.Sprintf(
			"ingest.archive_max_uncompressed: ожидается %s..%s, задано %s (потолок суммарного несжатого объёма членов архива)",
			ByteSize(1), ByteSize(1<<40), c.Ingest.ArchiveMaxUncompressed))
	}
	if !inRange(c.Ingest.ArchiveMaxFiles, 1, 1000000) {
		problems = append(problems, rangeMsg("ingest.archive_max_files", c.Ingest.ArchiveMaxFiles, 1, 1000000))
	}

	// scheduler
	if c.Scheduler.RecheckInterval <= 0 {
		problems = append(problems, nonPositiveDur("scheduler.recheck_interval", c.Scheduler.RecheckInterval))
	}
	// Период коллектора глубины backlog (T-161): коридор 30с..1ч —
	// нижняя граница коридора ТЗ 5.9 (чаще — агрегат по всей domains
	// без индекса, T-104, дороже самой метрики), верхняя — от
	// абсурда: глубина старее часа перестаёт быть наблюдаемой
	// величиной (алерт остановки прогресса — zones.updated_at, не этот
	// gauge).
	if c.Scheduler.BacklogInterval < Duration(30*time.Second) || c.Scheduler.BacklogInterval > Duration(time.Hour) {
		problems = append(problems, fmt.Sprintf("scheduler.backlog_interval: ожидается %s..%s, задано %s (период gauge глубины backlog, T-161)",
			Duration(30*time.Second), Duration(time.Hour), c.Scheduler.BacklogInterval))
	}
	// ТЗ 5.3: пачки планировщика 1000–10000 — границы нормативные
	// (T-027), не рекомендация: пачка задаёт память выборки (ТЗ 3.5) и
	// размер сообщений q:dns. Потолки стримов против бюджета Redis
	// (T-141, ТЗ 3.2) согласованы для всей вилки (константы
	// min/maxSchedulerBatchSize): инвариант «сумма потолков ≤ бюджета»
	// пин-тестом TestSchedulerBatchFitsStreamBudget (config_test.go) —
	// срабатывает при изменении констант политики или границ вилки.
	// Явный redis.maxlen — сознательный выход из политики: его
	// арифметику держит оператор (арифметика — docs/redis.md §3.1).
	if !inRange(c.Scheduler.BatchSize, minSchedulerBatchSize, maxSchedulerBatchSize) {
		problems = append(problems, rangeMsg("scheduler.batch_size", c.Scheduler.BatchSize, minSchedulerBatchSize, maxSchedulerBatchSize)+" (ТЗ 5.3)")
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
	// Потолки стримов (T-141) — см. комментарий у batch_size.
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
	// Задачный параллелизм (T-139): потолок 10000 — от опечаток, не от
	// эксплуатации (реальный потолок — память: каждая задача держит
	// результат домена с телами записей-оригиналов); пачка чтения —
	// размер буфера предвыборки; пул внутри задачи шире числа проверок
	// (2 × субдомены ≤ 126, §5.10) смысла не имеет.
	if !inRange(c.HTTP.TaskConcurrency, 1, 10000) {
		problems = append(problems, rangeMsg("http.task_concurrency", c.HTTP.TaskConcurrency, 1, 10000))
	}
	if !inRange(c.HTTP.TaskConsume, 1, 1000) {
		problems = append(problems, rangeMsg("http.task_consume", c.HTTP.TaskConsume, 1, 1000))
	}
	if !inRange(c.HTTP.TaskPerDomain, 1, 126) {
		problems = append(problems, rangeMsg("http.task_per_domain", c.HTTP.TaskPerDomain, 1, 126)+" (проверок в задаче ≤ 2 × 63 субдомена)")
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
	// Тарпит-лимит (T-148): минимум 1 — ноль означал бы финализацию до
	// первой попытки; потолок — от опечаток. Выше redis.max_attempts
	// бессмыслен: запись умирала бы в q:dead по общему лимиту до
	// финализации без рендера (footgun инцидента 2026-10-23 — задача
	// как раз про непотерянные тарпиты), поэтому превышение — ошибка
	// конфигурации, а не молчаливый откат к старому поведению.
	if !inRange(c.Render.TimeoutAttempts, 1, 100) {
		problems = append(problems, rangeMsg("render.timeout_attempts", c.Render.TimeoutAttempts, 1, 100))
	}
	if c.Render.TimeoutAttempts > c.Redis.MaxAttempts {
		problems = append(problems, fmt.Sprintf(
			"render.timeout_attempts: %d > redis.max_attempts %d — тарпит ушёл бы в q:dead до финализации без рендера (T-148); поднимите redis.max_attempts или снизьте render.timeout_attempts",
			c.Render.TimeoutAttempts, c.Redis.MaxAttempts))
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
	// Retention (T-166): 0 — выключено; коридор 1m..30d: окно меньше
	// минуты тонет в латентности потребителей (анализ — секунды), больше
	// 30 суток — не retention, а архив (для него оператор поднимает
	// отдельные мощности, решение 2026-10-23). Минутная граница заодно
	// держит формат interval PostgreSQL свободным от микросекундных
	// компонент Duration.String() («µs» в interval-синтаксисе сервера).
	if r := c.Content.Retention; r != 0 && (r < Duration(time.Minute) || r > Duration(720*time.Hour)) {
		problems = append(problems, fmt.Sprintf("content.retention: ожидается 0 (выключено) или %s..%s, задано %s",
			Duration(time.Minute), Duration(720*time.Hour), r))
	}
	// Период проходов: чаще минуты — лишние сканы старения (пачка и так
	// до 50 тыс. строк), реже суток — окно плавает сутками.
	if c.Content.RetentionInterval < Duration(time.Minute) || c.Content.RetentionInterval > Duration(24*time.Hour) {
		problems = append(problems, fmt.Sprintf("content.retention_interval: ожидается %s..%s, задано %s",
			Duration(time.Minute), Duration(24*time.Hour), c.Content.RetentionInterval))
	}
	// Пачка: 1000 — не тонуть в раунд-триппах при притоке ≈3 млн
	// строк/сут, 100 тыс. — потолок вилки постановки × 2 (транзакция
	// держит блокировки строк пачки, гипертрофить её ни к чему).
	if !inRange(c.Content.RetentionBatch, 1000, 100000) {
		problems = append(problems, rangeMsg("content.retention_batch", c.Content.RetentionBatch, 1000, 100000)+" (рабочая вилка 10–50 тыс., T-166)")
	}
	// Порог интерлока: 0 — общий порог scheduler.queue_high_watermark;
	// положительное значение не выше потолка стримов (10⁸ — граница
	// валидации redis.maxlen, тот же порядок).
	if w := c.Content.RetentionAnalyzeWatermark; w != 0 && !inRange(w, 1, 100000000) {
		problems = append(problems, fmt.Sprintf("content.retention_analyze_watermark: ожидается 0 (порог scheduler.queue_high_watermark) или %d..%d, задано %d",
			1, 100000000, w))
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
	// Период пересчёта зон и цикла алертов (T-161 поднял дефолт 60с→5м,
	// замер §12): коридор 30с..1ч — нижняя граница коридора ТЗ 5.9
	// (чаще — UPDATE-агрегат по всей domains), верхняя ограничивает и
	// алерты (остановка прогресса: 5 × интервал, 25 мин на дефолте).
	if c.Monitor.ZoneStatsInterval < Duration(30*time.Second) || c.Monitor.ZoneStatsInterval > Duration(time.Hour) {
		problems = append(problems, fmt.Sprintf("monitor.zone_stats_interval: ожидается %s..%s, задано %s",
			Duration(30*time.Second), Duration(time.Hour), c.Monitor.ZoneStatsInterval))
	}
	if c.Monitor.IngestMetricsURL != "" {
		if u, err := url.Parse(c.Monitor.IngestMetricsURL); err != nil ||
			(u.Scheme != "http" && u.Scheme != "https") || u.Host == "" {
			problems = append(problems, fmt.Sprintf(
				"monitor.ingest_metrics_url: ожидается абсолютный http(s)-адрес, задано %q", c.Monitor.IngestMetricsURL))
		}
	}

	// web (этап 13, T-121)
	// listen — [host]:port (TCP) или абсолютный путь unix-сокета
	// (вариант деплоя за nginx, T-124): Run различает форму по «/».
	if err := checkHostPort(c.Web.Listen); err != nil {
		if !strings.HasPrefix(c.Web.Listen, "/") {
			problems = append(problems, fmt.Sprintf("web.listen: ожидается [host]:port или абсолютный путь unix-сокета, задано %q", c.Web.Listen))
		} else if len(c.Web.Listen) > 100 {
			// Потолок sockaddr_un.sun_path — 108 байт: длинный путь упал
			// бы при Listen уже на запуске, валидация даёт ошибку конфига.
			problems = append(problems, fmt.Sprintf("web.listen: путь unix-сокета длиннее 100 байт, задано %q", c.Web.Listen))
		}
	}
	// Минимум 1 — ноль означал бы неограниченную форму; потолок — от
	// абсурда, не от эксплуатации.
	if !inRange(c.Web.RateLimit, 1, 1000) {
		problems = append(problems, rangeMsg("web.rate_limit", c.Web.RateLimit, 1, 1000)+" (проверок формы в минуту на IP)")
	}
	if c.Web.Timeout <= 0 || c.Web.Timeout > Duration(10*time.Minute) {
		problems = append(problems, fmt.Sprintf("web.timeout: ожидается %s..%s, задано %s",
			Duration(time.Second), Duration(10*time.Minute), c.Web.Timeout))
	}

	// dashboard (ТЗД §8.1/§8.3, T-127). Username/PasswordHash общим
	// Validate не проверяются: обязательность кредов — проверка запуска
	// cmd/dashboard (ТЗД §7.7), иначе требование коснулось бы всех
	// сервисов проекта; сам формат password_hash разбирает
	// internal/dashboard/auth на старте дашборда.
	if err := checkHostPort(c.Dashboard.Listen); err != nil {
		problems = append(problems, fmt.Sprintf("dashboard.listen: ожидается [host]:port, задано %q", c.Dashboard.Listen))
	}
	if c.Dashboard.Refresh < Duration(time.Second) || c.Dashboard.Refresh > Duration(5*time.Minute) {
		problems = append(problems, fmt.Sprintf("dashboard.refresh: ожидается %s..%s, задано %s",
			Duration(time.Second), Duration(5*time.Minute), c.Dashboard.Refresh))
	}
	// TTL кэша тяжёлых агрегатов (T-161): коридор постановки 1–5 мин —
	// меньше минуты кэш не спасает на дефолтном refresh 5 с, больше
	// 5 минут — сводка теряет оперативность.
	if c.Dashboard.StatusCacheTTL < Duration(time.Minute) || c.Dashboard.StatusCacheTTL > Duration(5*time.Minute) {
		problems = append(problems, fmt.Sprintf("dashboard.status_cache_ttl: ожидается %s..%s, задано %s",
			Duration(time.Minute), Duration(5*time.Minute), c.Dashboard.StatusCacheTTL))
	}
	for _, name := range sortedStringKeys(c.Dashboard.Probes) {
		if name == "" {
			problems = append(problems, "dashboard.probes: имя пробы не может быть пустым")
			continue
		}
		u, err := url.Parse(c.Dashboard.Probes[name])
		if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" {
			problems = append(problems, fmt.Sprintf(
				"dashboard.probes: %s=%q — ожидается абсолютный http(s)-адрес (базовый URL метрик сервиса, ТЗД §8.3)", name, c.Dashboard.Probes[name]))
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

// sortedStringKeys — то же для карт строковых значений (пробы дашборда).
func sortedStringKeys(m map[string]string) []string {
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
