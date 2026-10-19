// ---------------------------------------------------------------- zones sync
//
// crawlerctl zones sync (T-125, этап 14): начальное наполнение и
// актуализация справочника зон из машинно-читаемого среза реестра
// корневой зоны IANA (tlds-alpha-by-domain.txt, ~1460 делегированных TLD,
// IDN-зоны уже в punycode — совпадает с контрактом zones.name, ТЗ 5.2).
// Это справочник имён TLD, а НЕ источник доменов: домены по-прежнему
// попадают в систему только через ingest (var/incoming).
//
// Семантика записи: одна пакетная вставка новых зон (is_active = true —
// как автосоздание ingest'ом, ТЗ 5.1: пустая зона планировщику безразлична,
// DispatchBatch идёт от доменов) и, по явному флагу --deactivate-missing,
// выключение зон БД, отсутствующих в срезе (retired TLD). Существующие
// строки sync не обновляет вовсе: is_active — владение оператора
// (zones enable/disable), счётчики — monitor (T-078). Идемпотентно:
// повторный запуск ничего не меняет.

package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"sort"
	"strings"
	"time"

	"crawler/internal/db"
	"crawler/internal/idn"
)

// zoneListURL — канонический срез реестра корневой зоны IANA: то же
// содержимое, что страница iana.org/domains/root/db, без парсинга HTML.
// Формат: первая строка-комментарий с версией файла, далее одна зона на
// строку в верхнем регистре (IDN — в punycode).
const zoneListURL = "https://data.iana.org/TLD/tlds-alpha-by-domain.txt"

// Параметры загрузки среза. Реальный файл ~30 КиБ; потолок страхует от
// ошибочного URL с крупным ответом (взята круглая величина с запасом на
// рост реестра). Таймаут — на весь обмен http.Client (соединение + тело).
const (
	zoneSyncHTTPTimeout = 30 * time.Second
	zoneListMaxBytes    = 1 << 20 // 1 МиБ
)

// ---------------------------------------------------------------- SQL

// zonesSyncInsertSQL — одна пакетная вставка справочника из unnest-пачки
// (принцип «запись пачками» ТЗ 6): name — punycode, is_active = true —
// явно, как в автосоздании ingest'ом (ТЗ 5.1; DEFAULT схемы совпадает,
// но контракт задаётся здесь), name_unicode — nullif(u.unicode, ”):
// unicode-форма только для XN--зон (idna.ToUnicode на лету, ТЗ 5.2 п. 7),
// ASCII-зона хранит NULL — информации не несёт (стиль displayUnicode,
// internal/ingest/zones.go).
//
// NOT EXISTS отсеивает уже существующие зоны до вставки: идентичность
// GENERATED ALWAYS AS IDENTITY (миграция 000001) исполняет дефолт id для
// каждой строки-кандидата — конфликтуемые строки тоже расходуют значения
// последовательности, и повторные синхронизации (~1460 строк за запуск)
// исчерпали бы smallint (32767) за ~20 запусков; с фильтром расход —
// только действительно новые зоны (~единицы в год у IANA). ON CONFLICT (name)
// DO NOTHING остаётся страховкой гонки: зона, вставленная конкурентно
// (ingest EnsureZone, второй sync) между проверкой и вставкой, — не
// ошибка, RowsAffected считает только реально вставленные строки.
const zonesSyncInsertSQL = "INSERT INTO zones (name, name_unicode, is_active) " +
	"SELECT u.name, nullif(u.unicode, ''), true " +
	"FROM unnest($1::text[], $2::text[]) AS u(name, unicode) " +
	"WHERE NOT EXISTS (SELECT 1 FROM zones AS z WHERE z.name = u.name) " +
	"ON CONFLICT (name) DO NOTHING"

// zonesDeactivateMissingSQL — выключение зон БД, отсутствующих в срезе
// (retired TLD): отдельный оператор, только по явному флагу
// --deactivate-missing. Охрана is_active — идемпотентность (повторный
// запуск выключает 0 строк) и честный отчёт: RETURNING возвращает только
// фактически выключенные. Домены и счётчики не трогаются — семантика
// zones disable (ТЗ 13.1); updated_at — владение monitor (T-078).
// name <> ALL($1) — вычитание множеств на стороне БД (NOT IN без
// NULL-ловушек: массив имён NOT NULL).
const zonesDeactivateMissingSQL = "UPDATE zones SET is_active = false " +
	"WHERE is_active AND name <> ALL($1::text[]) " +
	"RETURNING id, name, coalesce(name_unicode, '') AS name_unicode"

// ---------------------------------------------------------------- парсер

// zoneListFile — разобранный срез реестра: версия из первой
// строки-комментария, зоны в punycode без дублей в порядке файла и число
// дублей (одна зона в двух написаниях — unicode и punycode — сходятся
// в одну после нормализации).
type zoneListFile struct {
	Version    string
	Zones      []string
	Duplicates int
}

// parseZoneList разбирает срез IANA: строки-комментарии «#» пропускаются,
// первая из них — шапка файла до первой зоны — несёт версию файла (в
// результат; комментарий после строк зон версией не считается), пустые
// строки не считаются данными (хвостовой перевод строки — не мусор),
// каждая строка зон валидируется idn.NormalizeZone — тем же конвейером,
// что каталоги incoming и аргументы zones enable/disable: мусор — ошибка
// с номером строки, тихих пропусков нет (битый срез не должен доходить
// до БД).
func parseZoneList(data []byte) (zoneListFile, error) {
	var res zoneListFile
	seen := make(map[string]bool)
	firstComment := true
	for i, line := range strings.Split(string(data), "\n") {
		num := i + 1
		s := strings.TrimSpace(line)
		if s == "" {
			continue
		}
		if strings.HasPrefix(s, "#") {
			if firstComment {
				res.Version = zoneListVersion(s)
				firstComment = false
			}
			continue
		}
		firstComment = false // пошли данные — шапка файла закончилась
		zone, err := idn.NormalizeZone(s)
		if err != nil {
			return zoneListFile{}, fmt.Errorf("строка %d: %q: %v", num, s, err)
		}
		if seen[zone] {
			res.Duplicates++
			continue
		}
		seen[zone] = true
		res.Zones = append(res.Zones, zone)
	}
	// Пустой список — отказ до БД: вставка пустой пачки бессмысленна, а
	// --deactivate-missing с пустым списком выключил бы все зоны разом.
	if len(res.Zones) == 0 {
		return zoneListFile{}, errors.New("зон в файле нет (только комментарии и пустые строки)")
	}
	return res, nil
}

// zoneListVersion извлекает версию файла из первой строки-комментария
// среза: канонический формат — «# Version 2026101900, Last Updated …»,
// версия — первое слово после «Version» без запятой. Комментарий другого
// формата (свой файл) возвращается целиком: это всё, что файл сообщает
// о себе.
func zoneListVersion(comment string) string {
	s := strings.TrimSpace(strings.TrimPrefix(strings.TrimSpace(comment), "#"))
	if s == "" {
		return ""
	}
	fields := strings.Fields(s)
	if len(fields) >= 2 && strings.EqualFold(fields[0], "version") {
		return strings.TrimSuffix(fields[1], ",")
	}
	return s
}

// ---------------------------------------------------------------- загрузка

// fetchZoneList скачивает срез по URL: stdlib net/http с таймаутом на весь
// обмен, отмена ctx прерывает загрузку (Ctrl-C). Не-200 — ошибка (редкий
// для файлов статус описывает проблему источника точнее тела ответа),
// тело — с потолком размера zoneListMaxBytes.
func fetchZoneList(ctx context.Context, url string) ([]byte, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, fmt.Errorf("загрузка %s: %w", url, err)
	}
	client := &http.Client{Timeout: zoneSyncHTTPTimeout}
	resp, err := client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("загрузка %s: %w", url, err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("загрузка %s: HTTP %s", url, resp.Status)
	}
	data, err := io.ReadAll(io.LimitReader(resp.Body, zoneListMaxBytes+1))
	if err != nil {
		return nil, fmt.Errorf("загрузка %s: чтение тела: %w", url, err)
	}
	if len(data) > zoneListMaxBytes {
		return nil, fmt.Errorf("загрузка %s: файл больше лимита %d байт", url, zoneListMaxBytes)
	}
	return data, nil
}

// readZoneListFile читывает файл среза с тем же потолком zoneListMaxBytes,
// что и загрузка по URL: --file и --url — один и тот же вход, защита одна
// (ошибочно указанный гигантский файл не должен попадать в память целиком).
func readZoneListFile(path string) ([]byte, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer func() { _ = f.Close() }()
	data, err := io.ReadAll(io.LimitReader(f, zoneListMaxBytes+1))
	if err != nil {
		return nil, err
	}
	if len(data) > zoneListMaxBytes {
		return nil, fmt.Errorf("файл больше лимита %d байт", zoneListMaxBytes)
	}
	return data, nil
}

// ---------------------------------------------------------------- запись

// zonesSyncInsert выполняет пакетную вставку новых зон (zonesSyncInsertSQL)
// и возвращает число вставленных (уже былые отсечены NOT EXISTS/ON CONFLICT).
func zonesSyncInsert(ctx context.Context, pool db.DB, list zoneListFile) (int64, error) {
	names := list.Zones
	unicodes := make([]string, len(names))
	for i, name := range names {
		// displayUnicode (стиль ingest): unicode-форма, если отличается от
		// punycode; ASCII-зона — пустая строка → NULL (nullif в SQL).
		if u := idn.ToUnicode(name); u != name {
			unicodes[i] = u
		}
	}
	tag, err := pool.Exec(ctx, zonesSyncInsertSQL, names, unicodes)
	if err != nil {
		return 0, fmt.Errorf("вставка зон: %w", err)
	}
	return tag.RowsAffected(), nil
}

// zonesDeactivateMissing выключает зоны БД, отсутствующие в срезе
// (zonesDeactivateMissingSQL), и возвращает их строки для отчёта
// (отсортированы по имени — порядок RETURNING не определён).
func zonesDeactivateMissing(ctx context.Context, pool db.DB, names []string) ([]zoneRow, error) {
	rows, err := pool.Query(ctx, zonesDeactivateMissingSQL, names)
	if err != nil {
		return nil, fmt.Errorf("выключение отсутствующих в срезе: %w", err)
	}
	defer rows.Close()
	var off []zoneRow
	for rows.Next() {
		// Три приёмника — ровно колонки RETURNING (id, name, name_unicode):
		// pgx отвергает рассинхрон, is_active после UPDATE всегда false.
		var z zoneRow
		if err := rows.Scan(&z.id, &z.name, &z.nameUnicode); err != nil {
			return nil, fmt.Errorf("выключение отсутствующих в срезе: чтение: %w", err)
		}
		off = append(off, z)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("выключение отсутствующих в срезе: %w", err)
	}
	sort.Slice(off, func(i, j int) bool { return off[i].name < off[j].name })
	return off, nil
}

// zonesSyncRun — запись справочника: вставка новых зон, отчёт
// прочитано/добавлено/уже было, по флагу — выключение отсутствующих
// в срезе с их списком.
func zonesSyncRun(ctx context.Context, pool db.DB, list zoneListFile, deactivateMissing bool, stdout io.Writer) error {
	inserted, err := zonesSyncInsert(ctx, pool, list)
	if err != nil {
		return err
	}
	existed := int64(len(list.Zones)) - inserted
	out(stdout, "прочитано зон %d (дублей в файле %d), добавлено %d, уже было %d\n",
		len(list.Zones), list.Duplicates, inserted, existed)
	if !deactivateMissing {
		return nil
	}
	off, err := zonesDeactivateMissing(ctx, pool, list.Zones)
	if err != nil {
		return err
	}
	if len(off) == 0 {
		out(stdout, "отсутствующих в срезе активных зон нет\n")
		return nil
	}
	names := make([]string, len(off))
	for i, z := range off {
		names[i] = zoneDesc(z)
	}
	out(stdout, "выключено отсутствующих в срезе (retired TLD): %d: %s\n",
		len(off), strings.Join(names, ", "))
	out(stdout, "домены выключенных зон не выбираются планировщиком до crawlerctl zones enable <зона>\n")
	return nil
}

// ---------------------------------------------------------------- команда

// zonesSync — crawlerctl zones sync: источник --file (офлайн, прод без
// исходящего интернета — файл качается заранее, например curl'ом) или
// --url (по умолчанию — канонический data.iana.org); чтение и разбор
// среза идут до соединения с БД — отказ источника/мусорная строка не
// трогают конфигурацию и пул.
func zonesSync(args []string, stdout, stderr io.Writer) error {
	fs := newFlagSet("zones sync")
	file := fs.String("file", "", "файл среза реестра IANA (офлайн; вместо --url)")
	listURL := fs.String("url", "", "URL среза реестра (по умолчанию "+zoneListURL+")")
	deactivate := fs.Bool("deactivate-missing", false,
		"выключить зоны БД, отсутствующие в срезе (retired TLD)")
	if handled, err := parseFlags(fs, zonesHelp, args, stdout); handled {
		return err
	}
	if fs.NArg() > 0 {
		return usagef("zones sync: лишние аргументы %v (источник задаётся флагами --file/--url)", fs.Args())
	}
	if *file != "" && *listURL != "" {
		return usagef("zones sync: --file и --url несовместимы — укажите один источник")
	}

	// Одноразовый CLI: Ctrl-C/SIGTERM прерывают загрузку/запись.
	ctx, stop := signalCtx()
	defer stop()

	var data []byte
	src := *listURL
	if *file != "" {
		src = *file
		raw, err := readZoneListFile(*file)
		if err != nil {
			return fmt.Errorf("zones sync: чтение %s: %w", *file, err)
		}
		data = raw
	} else {
		if src == "" {
			src = zoneListURL
		}
		raw, err := fetchZoneList(ctx, src)
		if err != nil {
			return fmt.Errorf("zones sync: %w", err)
		}
		data = raw
	}
	list, err := parseZoneList(data)
	if err != nil {
		return fmt.Errorf("zones sync: %w", err)
	}
	version := list.Version
	if version == "" {
		version = "неизвестна (комментария версии нет)"
	}
	out(stdout, "источник: %s, версия файла: %s\n", src, version)

	pool, err := openManagePool(ctx)
	if err != nil {
		return fmt.Errorf("zones sync: %w", err)
	}
	defer pool.Close()
	if err := zonesSyncRun(ctx, pool, list, *deactivate, stdout); err != nil {
		return fmt.Errorf("zones sync: %w", err)
	}
	return nil
}
