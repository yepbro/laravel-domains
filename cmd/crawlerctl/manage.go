// ---------------------------------------------------------------- управление
//
// Команды управления из ТЗ 4.1: reanalyze (T-074), subdomains (T-083);
// status — в status.go (T-079), domain — в domain.go (T-081), zones и
// recheck — в zones.go/recheck.go (T-084). Команда разбирает аргументы
// и валидирует вход (имена — через internal/idn); выборки из БД
// выполняют сами команды.

package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"strconv"
	"strings"

	"github.com/jackc/pgx/v5/pgxpool"

	"crawler/internal/analyzer"
	"crawler/internal/config"
	"crawler/internal/db"
	"crawler/internal/idn"
	"crawler/internal/model"
	"crawler/internal/queue"
	"crawler/internal/rules"
)

// ---------------------------------------------------------------- reanalyze

// reanalyzeHelp — справка crawlerctl reanalyze --help.
const reanalyzeHelp = `crawlerctl reanalyze — переанализ сохранённых результатов (ТЗ 5.8)

Использование:
  crawlerctl reanalyze [--zone ИМЯ]

Находит subdomain_results с устаревшим rules_version (включая никогда не
анализированные) и ставит их в q:analyze пачками — повторное скачивание
не выполняется, анализ идёт по сохранённому контенту. Без --zone — все
зоны. Версия набора правил берётся из каталога analyzer.rules_dir на
момент запуска (живой сервис analyzer перезагружает правила SIGHUP и
переанализирует сам); воркер analyzer фиксирует новые результаты.
Коды возврата: 0 — успех, 1 — ошибка, 2 — использование.
`

func cmdReanalyze(args []string, stdout, stderr io.Writer) error {
	fs := newFlagSet("reanalyze")
	zone := fs.String("zone", "", "переанализ по зоне (пусто — все)")
	if handled, err := parseFlags(fs, reanalyzeHelp, args, stdout); handled {
		return err
	}
	zoneName := ""
	if *zone != "" {
		// Валидация входа — тем же конвейером, что и каталоги incoming
		// (internal/idn.NormalizeZone): зона хранится в punycode.
		normalized, err := idn.NormalizeZone(*zone)
		if err != nil {
			return usagef("reanalyze: некорректное имя зоны %q: %v", *zone, err)
		}
		zoneName = normalized
	}
	if fs.NArg() > 0 {
		return usagef("reanalyze: лишние аргументы %v (команда принимает только --zone)", fs.Args())
	}

	// Конфигурация — единый YAML + окружение (internal/config): DSN
	// PostgreSQL, адрес Redis, каталог правил и порог backpressure.
	cfg, err := config.Load()
	if err != nil {
		return fmt.Errorf("reanalyze: %w", err)
	}

	// Версия набора — критерий устаревания; каталог читается разово.
	set, err := rules.LoadDir(cfg.Analyzer.RulesDir)
	if err != nil {
		return fmt.Errorf("reanalyze: %w", err)
	}

	// Одноразовый CLI: Ctrl-C/SIGTERM прерывают дренаж (опубликованное
	// остаётся, at-least-once).
	ctx, stop := signalCtx()
	defer stop()

	poolOpts := db.NewOptions(cfg.Postgres)
	poolOpts.PoolSize = 2 // бюджет соединений ТЗ 6.3 — у сервисов
	pool, err := db.Open(ctx, poolOpts)
	if err != nil {
		return fmt.Errorf("reanalyze: %w", err)
	}
	defer pool.Close()

	zoneDesc := "все"
	var zoneID *int16
	if zoneName != "" {
		id, found, err := analyzer.ZoneIDByName(ctx, pool, zoneName)
		if err != nil {
			return fmt.Errorf("reanalyze: %w", err)
		}
		if !found {
			return fmt.Errorf("reanalyze: зона %s не найдена в справочнике (зоны создаёт ingest при первом файле)", zoneName)
		}
		zoneID, zoneDesc = &id, zoneName
	}

	// Очередь — как у сервисов (RedisQueue + гейт backpressure T-026);
	// публикация q:analyze ждёт ёмкости (Warn о паузе — в stderr), отмена
	// ctx прерывает.
	log := slog.New(slog.NewTextHandler(stderr, nil))
	rdb := queue.NewClient(cfg.Redis.Addr)
	defer func() { _ = rdb.Close() }()
	gate := queue.NewBackpressureGate(rdb, queue.BackpressureOptions{
		Watermark: int64(cfg.Scheduler.QueueHighWatermark),
		MaxLen:    int64(cfg.Redis.MaxLen), // пределы обрезки — для порога гейта (T-026/T-025)
		Log:       log,
	})
	q := queue.NewWatermarkPublisher(queue.NewRedisQueue(rdb, queue.RedisOptions{
		MaxLen: int64(cfg.Redis.MaxLen), // XADD MAXLEN ~ на каждом стриме (T-025)
	}), gate)

	out(stdout, "reanalyze: набор правил v%d, зона: %s\n", set.Version, zoneDesc)
	res, err := analyzer.ReanalyzeDrain(ctx, pool, q, set.Version, zoneID, analyzer.ReanalyzeOptions{})
	if err != nil {
		return fmt.Errorf("reanalyze: поставлено в q:analyze %d, прервано: %w", res.Published, err)
	}
	if res.Published == 0 {
		out(stdout, "reanalyze: устаревших результатов нет — переанализ не требуется\n")
		return nil
	}
	out(stdout, "reanalyze: поставлено в q:analyze: %d\n", res.Published)
	return nil
}

// ---------------------------------------------------------------- subdomains
//
// Управление справочником проверяемых субдоменов (ТЗ 5.10, T-083).
// Справочник — таблица subdomains (миграция 000001): id smallint PK —
// номер бита dns_mask (0..62, максимум 63 субдомена — бит 63 знаковый),
// label text UNIQUE ('' — апекс, id 0), enabled boolean. Последовательности
// id нет: свободный бит назначает crawlerctl (max(id)+1), целостность
// охраняют PK и UNIQUE — гонка двух add завершается ошибкой вставки.
//
// Добавление/выключение не требуют миграции данных (ТЗ 13.1): бит
// закреплён за строкой навсегда, dns-worker перечитывает справочник на
// каждую пачку q:dns (T-039) — изменения подхватываются без рестарта
// сервисов. Проверка нового субдомена по уже проверенным доменам —
// subdomains recheck (ТЗ 13.1): done-домены переводятся в pending, дальше
// домен проходит штатный конвейер (выборка T-027 → q:dns T-028), и
// DNS-этап проверяет его уже с новым субдоменом из справочника.

// subdomainsHelp — справка crawlerctl subdomains [--help]; она же печатается
// для действий subdomains <действие> --help.
const subdomainsHelp = `crawlerctl subdomains — управление проверяемыми субдоменами (ТЗ 5.10)

Использование:
  crawlerctl subdomains list
  crawlerctl subdomains add <метка>
  crawlerctl subdomains disable <метка|бит>
  crawlerctl subdomains recheck <метка> [--zone ИМЯ]

Справочник субдоменов — таблица subdomains: каждый субдомен занимает бит
dns_mask (id = номер бита, максимум 63 — бит 63 знаковый), '' — апекс
(бит 0). Добавление субдомена — новая строка со свободным битом, миграция
данных не нужна (ТЗ 13.1); dns-worker перечитывает справочник на каждую
пачку — изменения подхватываются без рестарта сервисов.

  list                таблица справочника: бит, метка, состояние
  add <метка>         добавить субдомен (enabled = true); метка — короткое
                      имя без точек (www, admin, …), unicode приводится
                      к punycode
  disable <метка|бит> выключить проверку (enabled = false): бит dns_mask
                      и накопленные результаты не меняются — маски
                      стабильны (ТЗ 13.1); апекс (бит 0) — основа
                      проверки домена, выключить нельзя. Аргумент сперва
                      ищется как метка, затем — как номер бита
  recheck <метка> [--zone ИМЯ]
                      проверка субдомена по уже проверенным доменам
                      (ТЗ 13.1): done-домены зоны (--zone) или всех зон
                      (deleted исключаются) пачками переводятся в pending
                      и проходят DNS/HTTP-этап заново уже с новым
                      субдоменом; домены в pending/queued проверятся с ним
                      и так. Темп задаёт планировщик (backpressure q:dns)

Коды возврата: 0 — успех, 1 — ошибка, 2 — использование.
`

func cmdSubdomains(args []string, stdout, stderr io.Writer) error {
	if len(args) == 0 {
		return usagef("укажите действие: crawlerctl subdomains list|add|disable|recheck")
	}
	action, rest := args[0], args[1:]
	switch action {
	case "list":
		return subdomainsList(rest, stdout, stderr)
	case "add":
		return subdomainsAdd(rest, stdout, stderr)
	case "disable":
		return subdomainsDisable(rest, stdout, stderr)
	case "recheck":
		return subdomainsRecheckCmd(rest, stdout, stderr)
	case "-h", "--help", "help":
		out(stdout, "%s", subdomainsHelp)
		return flag.ErrHelp
	default:
		return usagef("неизвестное действие %q (допустимы: list, add, disable, recheck)", action)
	}
}

// ---------------------------------------------------------------- SQL

// subdomainsSelectSQL — весь справочник по возрастанию бита; тот же
// запрос читает dns-worker (T-039), здесь — вывод list.
const subdomainsSelectSQL = "SELECT id, label, enabled FROM subdomains ORDER BY id"

// subdomainByLabelSQL — строка по метке (UNIQUE, миграция 000001):
// проверка «метка уже есть» при add, разыменование disable и субдомен
// команды recheck.
const subdomainByLabelSQL = "SELECT id, label, enabled FROM subdomains WHERE label = $1"

// subdomainByIDSQL — строка по номеру бита: disable <бит> после
// неудачного поиска по метке.
const subdomainByIDSQL = "SELECT id, label, enabled FROM subdomains WHERE id = $1"

// subdomainNextIDSQL — следующий свободный бит max(id)+1 (пустой
// справочник дал бы 0 — апекс). Диапазон 0..62 проверяет Go
// (model.ValidateSubdomainID) до вставки.
const subdomainNextIDSQL = "SELECT coalesce(max(id), -1) + 1 FROM subdomains"

// subdomainInsertSQL — вставка нового субдомена назначенным битом,
// enabled = true сразу (ТЗ 5.10: добавить = включить). Гонка двух add
// ловится PK/UNIQUE на этом операторе.
const subdomainInsertSQL = "INSERT INTO subdomains (id, label, enabled) VALUES ($1, $2, true) RETURNING id"

// subdomainDisableSQL — выключение: меняется только enabled; id (бит)
// и результаты не трогаются — маски стабильны (ТЗ 13.1). Адресация по
// битам после разыменования аргумента (метка → строка → id).
const subdomainDisableSQL = "UPDATE subdomains SET enabled = false WHERE id = $1 RETURNING id, label, enabled"

// subdomainRecheckAllSQL / subdomainRecheckZoneSQL — один проход перевода
// готовых доменов в pending (стиль scheduler.recheckUpdateSQL, T-030):
// LIMIT — пачка (короткие блокировки, автокоммит одного оператора),
// ORDER BY по ключу PK — детерминированные пачки, охрана
// check_state/status и в подзапросе, и во внешнем WHERE (гонка с
// delete-файлом T-019 и вторым проходом не переведёт изменившуюся строку).
// Отличия от суточной перепроверки: нет порога возраста last_checked_at
// (перепроверить надо все готовые домены зоны, а не созревшие) и есть
// фильтр зоны. Коды check_state/status — параметры, как в T-030.
const subdomainRecheckAllSQL = "UPDATE domains AS d " +
	"SET check_state = $1, queued_at = NULL " +
	"FROM (SELECT zone_id, id FROM domains " +
	"WHERE check_state = $2 AND status <> $3 " +
	"ORDER BY zone_id, id LIMIT $4) AS b " +
	"WHERE d.zone_id = b.zone_id AND d.id = b.id " +
	"AND d.check_state = $2 AND d.status <> $3"

const subdomainRecheckZoneSQL = "UPDATE domains AS d " +
	"SET check_state = $1, queued_at = NULL " +
	"FROM (SELECT zone_id, id FROM domains " +
	"WHERE check_state = $2 AND status <> $3 AND zone_id = $4 " +
	"ORDER BY zone_id, id LIMIT $5) AS b " +
	"WHERE d.zone_id = b.zone_id AND d.id = b.id " +
	"AND d.check_state = $2 AND d.status <> $3 AND d.zone_id = $4"

// subdomainRecheckBatch — доменов в одной пачке перевода: диапазон пачек
// планировщика (ТЗ 5.3: 1000–10 000), совпадает с дефолтом суточной
// перепроверки (scheduler.DefaultRecheckBatch).
const subdomainRecheckBatch = 5000

// ---------------------------------------------------------------- данные

// subdomainRow — строка справочника для CLI (колонки миграции 000001).
type subdomainRow struct {
	id      int16  // бит dns_mask
	label   string // "" — апекс
	enabled bool
}

// fetchSubdomains — весь справочник по возрастанию бита (list).
func fetchSubdomains(ctx context.Context, pool db.DB) ([]subdomainRow, error) {
	rows, err := pool.Query(ctx, subdomainsSelectSQL)
	if err != nil {
		return nil, fmt.Errorf("справочник субдоменов: %w", err)
	}
	defer rows.Close()
	var subs []subdomainRow
	for rows.Next() {
		var s subdomainRow
		if err := rows.Scan(&s.id, &s.label, &s.enabled); err != nil {
			return nil, fmt.Errorf("справочник субдоменов: чтение: %w", err)
		}
		subs = append(subs, s)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("справочник субдоменов: %w", err)
	}
	return subs, nil
}

// lookupSubdomainLabel — строка по метке; found = false — метки нет.
func lookupSubdomainLabel(ctx context.Context, pool db.DB, label string) (subdomainRow, bool, error) {
	rows, err := pool.Query(ctx, subdomainByLabelSQL, label)
	if err != nil {
		return subdomainRow{}, false, fmt.Errorf("субдомен по метке %q: %w", label, err)
	}
	defer rows.Close()
	if rows.Next() {
		var s subdomainRow
		if err := rows.Scan(&s.id, &s.label, &s.enabled); err != nil {
			return subdomainRow{}, false, fmt.Errorf("субдомен по метке %q: чтение: %w", label, err)
		}
		return s, true, nil
	}
	if err := rows.Err(); err != nil {
		return subdomainRow{}, false, fmt.Errorf("субдомен по метке %q: %w", label, err)
	}
	return subdomainRow{}, false, nil
}

// lookupSubdomainID — строка по номеру бита; found = false — бита нет.
func lookupSubdomainID(ctx context.Context, pool db.DB, id int16) (subdomainRow, bool, error) {
	rows, err := pool.Query(ctx, subdomainByIDSQL, id)
	if err != nil {
		return subdomainRow{}, false, fmt.Errorf("субдомен по биту %d: %w", id, err)
	}
	defer rows.Close()
	if rows.Next() {
		var s subdomainRow
		if err := rows.Scan(&s.id, &s.label, &s.enabled); err != nil {
			return subdomainRow{}, false, fmt.Errorf("субдомен по биту %d: чтение: %w", id, err)
		}
		return s, true, nil
	}
	if err := rows.Err(); err != nil {
		return subdomainRow{}, false, fmt.Errorf("субдомен по биту %d: %w", id, err)
	}
	return subdomainRow{}, false, nil
}

// nextSubdomainBit — свободный бит max(id)+1.
func nextSubdomainBit(ctx context.Context, pool db.DB) (int, error) {
	rows, err := pool.Query(ctx, subdomainNextIDSQL)
	if err != nil {
		return 0, fmt.Errorf("следующий бит субдомена: %w", err)
	}
	defer rows.Close()
	if !rows.Next() {
		return 0, fmt.Errorf("следующий бит субдомена: пустой ответ")
	}
	var next int16
	if err := rows.Scan(&next); err != nil {
		return 0, fmt.Errorf("следующий бит субдомена: чтение: %w", err)
	}
	if err := rows.Err(); err != nil {
		return 0, fmt.Errorf("следующий бит субдомена: %w", err)
	}
	return int(next), nil
}

// insertSubdomain — вставка субдомена назначенным битом.
func insertSubdomain(ctx context.Context, pool db.DB, id int, label string) error {
	rows, err := pool.Query(ctx, subdomainInsertSQL, int16(id), label)
	if err != nil {
		return fmt.Errorf("вставка субдомена (бит %d): %w", id, err)
	}
	defer rows.Close()
	if !rows.Next() {
		return fmt.Errorf("вставка субдомена (бит %d): строка не вставлена", id)
	}
	var got int16
	if err := rows.Scan(&got); err != nil {
		return fmt.Errorf("вставка субдомена (бит %d): чтение: %w", id, err)
	}
	return rows.Err()
}

// disableSubdomainByID — выключение по биту; возвращает строку после
// обновления (none — гонка удаления строки между lookup и UPDATE).
func disableSubdomainByID(ctx context.Context, pool db.DB, id int16) (subdomainRow, error) {
	rows, err := pool.Query(ctx, subdomainDisableSQL, id)
	if err != nil {
		return subdomainRow{}, fmt.Errorf("выключение субдомена (бит %d): %w", id, err)
	}
	defer rows.Close()
	if !rows.Next() {
		return subdomainRow{}, fmt.Errorf("выключение субдомена (бит %d): строка не обновлена", id)
	}
	var s subdomainRow
	if err := rows.Scan(&s.id, &s.label, &s.enabled); err != nil {
		return subdomainRow{}, fmt.Errorf("выключение субдомена (бит %d): чтение: %w", id, err)
	}
	return s, rows.Err()
}

// resolveSubdomainArg — разыменование аргумента disable: сперва метка
// (нормализация; приоритет — цифры бывают и метками), не нашлось — номер
// бита (аргумент из цифр). Метки нет и не число — found = false.
func resolveSubdomainArg(ctx context.Context, pool db.DB, arg string) (subdomainRow, bool, error) {
	if label, lerr := normalizeSubdomainLabel(arg); lerr == nil {
		s, found, err := lookupSubdomainLabel(ctx, pool, label)
		if err != nil || found {
			return s, found, err
		}
	}
	// Диапазон 0..62 проверяется до поиска (ValidateSubdomainID, как в add):
	// без проверки усечение int16(id) молча свело бы 65537 к биту 1 —
	// disable выключил бы чужой субдомен вместо «не найден».
	if id, perr := strconv.Atoi(strings.TrimSpace(arg)); perr == nil {
		if err := model.ValidateSubdomainID(id); err != nil {
			return subdomainRow{}, false, nil
		}
		return lookupSubdomainID(ctx, pool, int16(id))
	}
	return subdomainRow{}, false, nil
}

// ---------------------------------------------------------------- действия

// subdomainsListRun — вывод справочника: бит, метка, состояние.
func subdomainsListRun(ctx context.Context, pool db.DB, stdout io.Writer) error {
	subs, err := fetchSubdomains(ctx, pool)
	if err != nil {
		return err
	}
	enabled := 0
	out(stdout, "бит  метка              акт\n")
	for _, s := range subs {
		mark := "-"
		if s.enabled {
			mark = "+"
			enabled++
		}
		label := s.label
		if label == "" {
			label = "(апекс)"
		}
		out(stdout, "%3d  %-18s  %s\n", s.id, label, mark)
	}
	out(stdout, "всего %d из %d, включено %d\n", len(subs), model.MaxSubdomains, enabled)
	return nil
}

// subdomainsAddRun — добавление субдомена свободным битом.
func subdomainsAddRun(ctx context.Context, pool db.DB, label string, stdout io.Writer) error {
	// Метка уникальна (миграция 000001): дубль — ошибка с битом
	// (включён или выключен — вставлять всё равно нечего).
	if s, found, err := lookupSubdomainLabel(ctx, pool, label); err != nil {
		return err
	} else if found {
		state := "включён"
		if !s.enabled {
			state = "выключен"
		}
		return fmt.Errorf("метка уже есть в справочнике: бит %d, %s", s.id, state)
	}
	// Свободный бит: max(id)+1, диапазон 0..62 — бит 63 знаковый
	// (model.ValidateSubdomainID, ТЗ 5.10).
	id, err := nextSubdomainBit(ctx, pool)
	if err != nil {
		return err
	}
	if err := model.ValidateSubdomainID(id); err != nil {
		return err
	}
	if err := insertSubdomain(ctx, pool, id, label); err != nil {
		return err
	}
	out(stdout, "субдомен %s добавлен: бит %d\n", label, id)
	out(stdout, "проверка уже проверенных доменов: crawlerctl subdomains recheck %s [--zone ИМЯ]\n", label)
	return nil
}

// subdomainApexGuard — запрет выключения апекса: без A/NS-запроса апекса
// невозможны NXDOMAIN-ветка и wildcard-детект (ТЗ 5.4 п. 1, 3) — апекс
// основа проверки домена.
func subdomainApexGuard() error {
	return fmt.Errorf("апекс (бит 0) — основа проверки домена (ТЗ 5.4 п. 1), выключение не предусмотрено")
}

// subdomainsDisableRun — выключение субдомена (enabled = false).
func subdomainsDisableRun(ctx context.Context, pool db.DB, arg string, stdout io.Writer) error {
	if strings.TrimSpace(arg) == "" {
		return subdomainApexGuard()
	}
	s, found, err := resolveSubdomainArg(ctx, pool, arg)
	if err != nil {
		return err
	}
	if !found {
		return fmt.Errorf("субдомен %q не найден в справочнике (см. subdomains list)", arg)
	}
	if s.id == 0 {
		return subdomainApexGuard()
	}
	// Идемпотентная ветка — до записи: «уже выключен» не делает UPDATE
	// (лишняя запись ни к чему, сообщение соответствует состоянию lookup).
	if !s.enabled {
		out(stdout, "субдомен %s (бит %d) уже выключен\n", s.label, s.id)
		return nil
	}
	after, err := disableSubdomainByID(ctx, pool, s.id)
	if err != nil {
		return err
	}
	out(stdout, "субдомен %s (бит %d) выключен: бит и накопленные маски/результаты не меняются (ТЗ 13.1)\n",
		after.label, after.id)
	return nil
}

// subdomainsRecheckRun — запуск проверки субдомена по готовым доменам:
// дренаж done → pending (механизм — перевод состояния: публикацию в q:dns
// делает планировщик, он владеет backpressure и lease, а DNS-воркер берёт
// список субдоменов из справочника сам, T-039, — публикация особых задач
// со списком субдоменов дублировала бы конвейер).
func subdomainsRecheckRun(ctx context.Context, pool db.DB, label string, zoneID *int16, zoneDesc string, stdout io.Writer) error {
	s, found, err := lookupSubdomainLabel(ctx, pool, label)
	if err != nil {
		return err
	}
	if !found {
		return fmt.Errorf("субдомена %s нет в справочнике — сначала subdomains add %s (ТЗ 5.10)", label, label)
	}
	if !s.enabled {
		return fmt.Errorf("субдомен %s (бит %d) выключен — DNS-воркер его не проверяет (ТЗ 5.4 п. 2)", label, s.id)
	}

	out(stdout, "субдомен %s (бит %d), зона: %s\n", label, s.id, zoneDesc)
	total, err := drainSubdomainRecheck(ctx, pool, zoneID)
	if err != nil {
		if total > 0 {
			return fmt.Errorf("переведено в pending %d доменов, прервано: %w", total, err)
		}
		return err
	}
	if total == 0 {
		out(stdout, "готовых к перепроверке доменов нет (done-домены, кроме deleted)\n")
		return nil
	}
	out(stdout, "переведено в проверку (pending): %d доменов\n", total)
	out(stdout, "домены пройдут DNS/HTTP-этап заново с текущим справочником; темп задаёт планировщик (backpressure q:dns)\n")
	return nil
}

// drainSubdomainRecheck — дренаж пачками до исчерпания done-доменов:
// переведённые строки не удовлетворяют подзапросу, частичная пачка —
// множество исчерпано (стиль RecheckOnce/RecheckDaily, T-030). Прерванный
// дренаж частичен, но не разрушителен: перевод идемпотентен, повтор
// команды продолжает с того же места.
func drainSubdomainRecheck(ctx context.Context, pool db.DB, zoneID *int16) (int64, error) {
	var total int64
	for {
		n, err := subdomainRecheckOnce(ctx, pool, zoneID)
		if err != nil {
			return total, err
		}
		total += n
		if n < subdomainRecheckBatch {
			return total, nil
		}
	}
}

// subdomainRecheckOnce — один проход перевода пачки done → pending
// (SQL — subdomainRecheckAllSQL/subdomainRecheckZoneSQL).
func subdomainRecheckOnce(ctx context.Context, pool db.DB, zoneID *int16) (int64, error) {
	if zoneID == nil {
		tag, err := pool.Exec(ctx, subdomainRecheckAllSQL,
			int16(model.CheckStatePending), int16(model.CheckStateDone),
			int16(model.StatusDeleted), subdomainRecheckBatch)
		if err != nil {
			return 0, fmt.Errorf("перевод done → pending: %w", err)
		}
		return tag.RowsAffected(), nil
	}
	z := *zoneID
	tag, err := pool.Exec(ctx, subdomainRecheckZoneSQL,
		int16(model.CheckStatePending), int16(model.CheckStateDone),
		int16(model.StatusDeleted), z, subdomainRecheckBatch)
	if err != nil {
		return 0, fmt.Errorf("перевод done → pending (зона %d): %w", z, err)
	}
	return tag.RowsAffected(), nil
}

// ---------------------------------------------------------------- команды

// openManagePool — пул БД для команд управления (subdomains, zones,
// recheck): единый YAML + окружение (internal/config), pool_size 2 —
// бюджет соединений ТЗ 6.3 (стиль reanalyze). Закрывает вызывающий
// (defer pool.Close()).
func openManagePool(ctx context.Context) (*pgxpool.Pool, error) {
	cfg, err := config.Load()
	if err != nil {
		return nil, err
	}
	poolOpts := db.NewOptions(cfg.Postgres)
	poolOpts.PoolSize = 2
	return db.Open(ctx, poolOpts)
}

func subdomainsList(args []string, stdout, stderr io.Writer) error {
	fs := newFlagSet("subdomains list")
	if handled, err := parseFlags(fs, subdomainsHelp, args, stdout); handled {
		return err
	}
	if fs.NArg() > 0 {
		return usagef("subdomains list: лишние аргументы %v (команда без аргументов)", fs.Args())
	}
	ctx, stop := signalCtx()
	defer stop()
	pool, err := openManagePool(ctx)
	if err != nil {
		return fmt.Errorf("subdomains list: %w", err)
	}
	defer pool.Close()
	if err := subdomainsListRun(ctx, pool, stdout); err != nil {
		return fmt.Errorf("subdomains list: %w", err)
	}
	return nil
}

func subdomainsAdd(args []string, stdout, stderr io.Writer) error {
	fs := newFlagSet("subdomains add")
	if handled, err := parseFlags(fs, subdomainsHelp, args, stdout); handled {
		return err
	}
	if fs.NArg() != 1 {
		return usagef("укажите метку субдомена: crawlerctl subdomains add www")
	}
	label, err := normalizeSubdomainLabel(fs.Arg(0))
	if err != nil {
		return usagef("subdomains add: %v", err)
	}
	ctx, stop := signalCtx()
	defer stop()
	pool, err := openManagePool(ctx)
	if err != nil {
		return fmt.Errorf("subdomains add %s: %w", label, err)
	}
	defer pool.Close()
	if err := subdomainsAddRun(ctx, pool, label, stdout); err != nil {
		return fmt.Errorf("subdomains add %s: %w", label, err)
	}
	return nil
}

func subdomainsDisable(args []string, stdout, stderr io.Writer) error {
	fs := newFlagSet("subdomains disable")
	if handled, err := parseFlags(fs, subdomainsHelp, args, stdout); handled {
		return err
	}
	if fs.NArg() != 1 {
		return usagef("укажите субдомен: crawlerctl subdomains disable www|бит")
	}
	ctx, stop := signalCtx()
	defer stop()
	pool, err := openManagePool(ctx)
	if err != nil {
		return fmt.Errorf("subdomains disable %s: %w", fs.Arg(0), err)
	}
	defer pool.Close()
	if err := subdomainsDisableRun(ctx, pool, fs.Arg(0), stdout); err != nil {
		return fmt.Errorf("subdomains disable %s: %w", fs.Arg(0), err)
	}
	return nil
}

// subdomainsRecheckCmd — crawlerctl subdomains recheck: запуск проверки
// субдомена по зоне/всем доменам (ТЗ 13.1).
func subdomainsRecheckCmd(args []string, stdout, stderr io.Writer) error {
	fs := newFlagSet("subdomains recheck")
	zone := fs.String("zone", "", "зона проверки (пусто — все зоны)")
	if handled, err := parseFlags(fs, subdomainsHelp, args, stdout); handled {
		return err
	}
	if fs.NArg() != 1 {
		return usagef("укажите метку субдомена: crawlerctl subdomains recheck www [--zone ИМЯ]")
	}
	label, err := normalizeSubdomainLabel(fs.Arg(0))
	if err != nil {
		return usagef("subdomains recheck: %v", err)
	}
	zoneName := ""
	if *zone != "" {
		// Валидация входа — тем же конвейером, что и reanalyze
		// (internal/idn.NormalizeZone): зона хранится в punycode.
		normalized, zerr := idn.NormalizeZone(*zone)
		if zerr != nil {
			return usagef("subdomains recheck: некорректное имя зоны %q: %v", *zone, zerr)
		}
		zoneName = normalized
	}

	// Одноразовый CLI: Ctrl-C/SIGTERM прерывают дренаж — переведённое
	// остаётся (перевод идемпотентен), повтор команды продолжает.
	ctx, stop := signalCtx()
	defer stop()

	pool, err := openManagePool(ctx)
	if err != nil {
		return fmt.Errorf("subdomains recheck %s: %w", label, err)
	}
	defer pool.Close()

	zoneDesc := "все"
	var zoneID *int16
	if zoneName != "" {
		id, found, zerr := analyzer.ZoneIDByName(ctx, pool, zoneName)
		if zerr != nil {
			return fmt.Errorf("subdomains recheck %s: %w", label, zerr)
		}
		if !found {
			return fmt.Errorf("subdomains recheck %s: зона %s не найдена в справочнике (зоны создаёт ingest при первом файле)", label, zoneName)
		}
		zoneID, zoneDesc = &id, zoneName
	}

	if err := subdomainsRecheckRun(ctx, pool, label, zoneID, zoneDesc, stdout); err != nil {
		return fmt.Errorf("subdomains recheck %s: %w", label, err)
	}
	return nil
}

// normalizeSubdomainLabel приводит метку субдомена к канонической форме:
// unicode → punycode (ТЗ 5.2, HostToASCII — короткая метка допустимый
// «хост»), затем проверка DNS-метки: латиница/цифры/дефис внутри,
// 1..63 октета. Пустая метка — апекс (id 0) — уже есть в справочнике,
// добавлению не подлежит.
func normalizeSubdomainLabel(label string) (string, error) {
	if strings.TrimSpace(label) == "" {
		return "", fmt.Errorf("метка субдомена пуста: апекс '' уже есть в справочнике (id 0)")
	}
	ascii, err := idn.HostToASCII(label)
	if err != nil {
		return "", fmt.Errorf("некорректная метка субдомена %q: %v", label, err)
	}
	if !validSubdomainLabel(ascii) {
		return "", fmt.Errorf("некорректная метка субдомена %q: допустимы a-z, 0-9 и дефис внутри, длина 1..63", label)
	}
	return ascii, nil
}

// validSubdomainLabel проверяет ASCII-метку: [a-z0-9-], дефис только
// внутри, длина 1..63 (длина DNS-метки, ТЗ 5.2 п. 4).
func validSubdomainLabel(ascii string) bool {
	if len(ascii) == 0 || len(ascii) > 63 {
		return false
	}
	for i := 0; i < len(ascii); i++ {
		c := ascii[i]
		switch {
		case c >= 'a' && c <= 'z', c >= '0' && c <= '9':
		case c == '-':
			if i == 0 || i == len(ascii)-1 {
				return false
			}
		default:
			return false
		}
	}
	return true
}
