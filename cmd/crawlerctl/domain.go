// ---------------------------------------------------------------- domain
//
// crawlerctl domain (T-081, ТЗ 5.11): информация о домене по имени —
// для оператора (сводка по разделам) и скриптов (--json). Единственный
// источник данных — SQL-функция domain_info(text) (миграция 000006,
// T-082): у CLI и psql одна точка правды. Роль команды — нормализовать
// вход (internal/idn, тот же конвейер, что и для строк incoming),
// украсить вывод и по флагу --content распаковать контент из хранилища
// (content_hash в результатах — ссылка, байты отдаёт ContentStore).
//
// Решения (зафиксированы по ТЗ 5.11):
//
//   - не найдено «что именно»: domain_info на пустой случай отвечает
//     NULL без различия «нет зоны»/«нет домена в зоне»; команда доуточняет
//     одним лёгким запросом к справочнику zones (длиннейший суффикс
//     имени, присутствующий в zones, — так же зону отделяет функция);
//   - --json — сырой ответ domain_info (pretty-print); домен не найден —
//     объект {"found":false,…} и код 1;
//   - --content — stdout получает только байты выбранной записи (итоговый
//     DOM рендера либо сырой HTML), сводка и пометка о выбранной записи —
//     в stderr: поток можно перенаправлять в файл. Выбор записи — первая
//     «осмысленная»: приоритет отрендеренной (DOM — главный результат
//     конвейера для SPA, ТЗ 5.6), далее первая запись с контентом
//     в порядке субдомен×вариант. --json и --content несовместимы:
//     машинный вывод не смешивается с байтами контента;
//   - cookies и заголовки (ТЗ 5.11) в вывод не входят: они живут
//     в subdomain_results, а не в хранилище контента, и в ответ
//     domain_info не попали (контракт T-082); сводка и так не байтовая,
//     а --content обязан оставаться чистым потоком.

package main

import (
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"strconv"
	"strings"
	"time"

	"crawler/internal/config"
	"crawler/internal/content"
	"crawler/internal/db"
	"crawler/internal/idn"
	"crawler/internal/model"
)

// domainHelp — справка crawlerctl domain --help.
const domainHelp = `crawlerctl domain — информация о домене по имени (ТЗ 5.11)

Использование:
  crawlerctl domain <имя> [--json]
  crawlerctl domain <имя> --content

Имя — в любой форме: unicode или punycode, любой регистр, точка на конце
(нормализация по ТЗ 5.2 через internal/idn; в БД имя хранится punycode).

Данные — SQL-функция domain_info (миграция 000006): зона, статус,
состояние проверки, последняя проверка, DNS-живые субдомены (расшифровка
dns_mask), результаты по субдоменам и вариантам (схема, HTTP-статус,
итоговый URL, алиас, контент DOM/HTML, ошибка, время проверки) и признаки
Laravel: score, диапазон версий, сработавшие правила с фрагментами-
свидетельствами; слабые срабатывания — отдельной секцией с пометкой
is_laravel (домены только со слабыми срабатываниями ниже порога записи
в laravel_domains не имеют и функцией не раскрываются).

--json     ответ domain_info как есть, pretty-print (2 пробела);
           не найдено — {"found":false,…} и код 1.
--content  вместо сводки вывести в stdout сохранённый контент выбранной
           записи как есть (байты; приоритет — отрендеренный DOM, иначе
           первая запись с контентом в порядке субдомен×вариант).
           Сводка и пометка о выбранной записи — в stderr, stdout можно
           перенаправлять в файл. Несовместим с --json.

Неизвестный домен или зона — понятное сообщение (что именно не найдено),
код 1; некорректное имя — код 2. Команда работает на стеке dev-окружения
(PostgreSQL; --content — ещё и каталог контента).
Коды возврата: 0 — успех, 1 — не найдено/ошибка, 2 — использование.
`

// ---------------------------------------------------------------- SQL

// domainInfoSQL — единственная выборка команды: SQL-функция domain_info
// (миграция 000006, T-082) возвращает сводку домена одним jsonb —
// зона/статусы/dns_alive/результаты/laravel-агрегат с findings; домен
// или зона не найдены — NULL, не ошибка. Функция сама отделяет зону
// (длиннейший суффикс имени из справочника zones) и ищет домен по
// уникальному ключу (zone_id, label) без полного сканирования.
const domainInfoSQL = "SELECT domain_info($1) AS info"

// zoneLongestSQL — уточнение для «не найдено» (ТЗ 5.11: сообщение
// указывает, что именно не найдено): длиннейший суффикс имени,
// присутствующий в справочнике zones. Суффиксы-кандидаты (все, кроме
// полной длины — у домена есть хоть одна метка перед зоной) передаёт
// команда; зона без доменов не бывает (зоны создаёт ingest вместе
// с доменами), поэтому кандидатов достаточно.
const zoneLongestSQL = "SELECT z.name FROM zones AS z " +
	"WHERE z.name = ANY($1::text[]) " +
	"ORDER BY length(z.name) DESC, z.name LIMIT 1"

// ---------------------------------------------------------------- данные

// domainSubRef — {id, label} справочника subdomains из jsonb
// (dns_alive, laravel.found_on).
type domainSubRef struct {
	ID    int16  `json:"id"`
	Label string `json:"label"`
}

// domainResult — строка results: результат по паре (субдомен, вариант).
// Nullable-колонки (http_status, final_url, alias, error, content_hash)
// приходят json null → нулевые значения Go; для вывода это то же
// «значения нет» (http_status 0 и error 0 не встречаются как данные).
type domainResult struct {
	SubdomainID int16  `json:"subdomain_id"`
	Subdomain   string `json:"subdomain"`
	Variant     int16  `json:"variant"`
	SchemeName  string `json:"scheme_name"`
	HTTPStatus  int16  `json:"http_status"`
	FinalURL    string `json:"final_url"`
	AliasKind   int16  `json:"alias_kind"`
	Alias       string `json:"alias"`
	Rendered    bool   `json:"rendered"`
	Error       int16  `json:"error"`
	ContentHash string `json:"content_hash"` // hex; "" — контента нет
	CheckedAt   string `json:"checked_at"`
}

// domainSignal — свидетельство из laravel_findings.signals
// ([{rule, weight, evidence}, …] — формат этапа записи, T-071).
type domainSignal struct {
	Rule     string  `json:"rule"`
	Weight   float64 `json:"weight"`
	Evidence string  `json:"evidence"`
}

// domainFinding — строка laravel_findings: срабатывания правил по одному
// результату; слабые (is_laravel = false, score ниже порога) хранятся
// так же, как и подтвердившие (ТЗ 5.8, 13.1).
type domainFinding struct {
	SubdomainID int16          `json:"subdomain_id"`
	Subdomain   string         `json:"subdomain"`
	Variant     int16          `json:"variant"`
	Score       float64        `json:"score"`
	VerMin      string         `json:"ver_min"`
	VerMax      string         `json:"ver_max"`
	IsLaravel   bool           `json:"is_laravel"`
	Signals     []domainSignal `json:"signals"`
}

// domainLaravel — агрегат laravel_domains с расшифровкой found_on
// и всеми findings домена по убыванию score (контракт T-082).
type domainLaravel struct {
	Score        float64         `json:"score"`
	VerMin       string          `json:"ver_min"`
	VerMax       string          `json:"ver_max"`
	FoundOnMask  int64           `json:"found_on_mask"`
	FoundOn      []domainSubRef  `json:"found_on"`
	RulesVersion int32           `json:"rules_version"`
	UpdatedAt    string          `json:"updated_at"`
	Findings     []domainFinding `json:"findings"`
}

// domainInfo — ответ domain_info. Метки времени — строки как есть
// (last_checked_at имеет точность дня, timestamptz — ISO 8601;
// оформление — formatDomainTS, разбор в time.Time ничего не добавляет).
type domainInfo struct {
	Zone struct {
		ID          int16  `json:"id"`
		Name        string `json:"name"`
		NameUnicode string `json:"name_unicode"`
	} `json:"zone"`
	Domain struct {
		ID             int64  `json:"id"`
		Label          string `json:"label"`
		Name           string `json:"name"`
		StatusName     string `json:"status_name"`
		CheckStateName string `json:"check_state_name"`
		IsIDN          bool   `json:"is_idn"`
		RenderPending  bool   `json:"render_pending"`
		DNSMask        int64  `json:"dns_mask"`
		TempAttempts   int16  `json:"temp_attempts"`
		QueuedAt       string `json:"queued_at"`
		LastCheckedAt  string `json:"last_checked_at"`
	} `json:"domain"`
	DNSAlive []domainSubRef `json:"dns_alive"`
	Results  []domainResult `json:"results"`
	Laravel  *domainLaravel `json:"laravel"`
}

// fetchDomainInfo вызывает domain_info(имя punycode): сырой jsonb (для
// --json) и разобранная копия (для человекочитаемого вывода). NULL —
// домен или зона не найдены: found=false без ошибки.
func fetchDomainInfo(ctx context.Context, pool db.DB, name string) (info domainInfo, raw []byte, found bool, err error) {
	if pool == nil {
		return domainInfo{}, nil, false, errors.New("domain: domain_info: пул не задан")
	}
	rows, err := pool.Query(ctx, domainInfoSQL, name)
	if err != nil {
		return domainInfo{}, nil, false, fmt.Errorf("domain: domain_info: %w", err)
	}
	defer rows.Close()
	if !rows.Next() {
		return domainInfo{}, nil, false, errors.New("domain: domain_info: пустой ответ")
	}
	if err := rows.Scan(&raw); err != nil {
		return domainInfo{}, nil, false, fmt.Errorf("domain: domain_info: чтение: %w", err)
	}
	if err := rows.Err(); err != nil {
		return domainInfo{}, nil, false, fmt.Errorf("domain: domain_info: %w", err)
	}
	if raw == nil { // NULL: не найдено — не ошибка (контракт T-082)
		return domainInfo{}, nil, false, nil
	}
	if err := json.Unmarshal(raw, &info); err != nil {
		return domainInfo{}, nil, false, fmt.Errorf("domain: domain_info: разбор json: %w", err)
	}
	return info, raw, true, nil
}

// zoneSuffixCandidates — суффиксы имени-кандидаты в зону (длиннейший
// первым): example.crawl.test → [crawl.test, test]. Полное имя не входит:
// у домена перед зоной есть хотя бы одна метка (как в domain_info).
func zoneSuffixCandidates(name string) []string {
	labels := strings.Split(name, ".")
	cands := make([]string, 0, len(labels)-1)
	for i := 1; i < len(labels); i++ {
		cands = append(cands, strings.Join(labels[i:], "."))
	}
	return cands
}

// fetchCatalogZone ищет длиннейший суффикс имени в справочнике zones
// (второй запрос команды — только когда domain_info вернул NULL).
// Возвращает best (найденная зона, иначе длиннейший кандидат — для
// сообщения «какой зоны не хватает») и found.
func fetchCatalogZone(ctx context.Context, pool db.DB, name string) (best string, found bool, err error) {
	if pool == nil {
		return "", false, errors.New("domain: зоны: пул не задан")
	}
	cands := zoneSuffixCandidates(name)
	if len(cands) == 0 {
		return "", false, nil
	}
	rows, err := pool.Query(ctx, zoneLongestSQL, cands)
	if err != nil {
		return "", false, fmt.Errorf("domain: зоны: %w", err)
	}
	defer rows.Close()
	var zone []byte
	if !rows.Next() {
		// Не найдено — не ошибка: best остаётся длиннейшим кандидатом.
		return cands[0], false, rows.Err()
	}
	if err := rows.Scan(&zone); err != nil {
		return "", false, fmt.Errorf("domain: зоны: чтение: %w", err)
	}
	if err := rows.Err(); err != nil {
		return "", false, fmt.Errorf("domain: зоны: %w", err)
	}
	if zone == nil {
		return cands[0], false, nil
	}
	return string(zone), true, nil
}

// ---------------------------------------------------------------- вывод

// subLabel — метка субдомена для вывода: ” — апекс домена (id 0).
func subLabel(label string) string {
	if label == "" {
		return "(апекс)"
	}
	return label
}

// orEmDash — пустая строка → «—» (значения нет). Локальное имя, а не
// orDash из test-rules («?»): в сводке домена пустые поля — именно
// «нет значения».
func orEmDash(s string) string {
	if s == "" {
		return "—"
	}
	return s
}

// parseDomainTS разбирает метку времени из jsonb: timestamptz приходит
// ISO 8601 (RFC 3339); date («2006-01-02», last_checked_at) времени не
// содержит и на разбор не попадает — выводится как есть. Чужой формат
// не роняет вывод: строка показывается сырой.
func parseDomainTS(s string) (time.Time, error) {
	return time.Parse(time.RFC3339Nano, s)
}

// formatDomainTS — метка времени из jsonb для вывода: timestamptz (ISO
// 8601) → «2006-01-02 15:04:05», date остаётся как есть.
func formatDomainTS(s string) string {
	if t, err := parseDomainTS(s); err == nil {
		return t.Format("2006-01-02 15:04:05")
	}
	return s
}

// formatDomainTSShort — то же без секунд (колонка «проверен» таблицы
// результатов).
func formatDomainTSShort(s string) string {
	if t, err := parseDomainTS(s); err == nil {
		return t.Format("2006-01-02 15:04")
	}
	return s
}

// httpStatusDesc — HTTP-статус результата: 0/null — «—» (нулевого
// статуса не бывает — статус есть у ответа, а не у ошибки запроса).
func httpStatusDesc(r domainResult) string {
	if r.HTTPStatus == 0 {
		return "—"
	}
	return strconv.FormatInt(int64(r.HTTPStatus), 10)
}

// verRange — диапазон версий Laravel: «9.0 – 11», «≥ 9.0», «≤ 11»;
// обе границы пусты — «—» (нашлись признаки без версии).
func verRange(min, max string) string {
	switch {
	case min != "" && max != "":
		if min == max {
			return min
		}
		return min + " – " + max
	case min != "":
		return "≥ " + min
	case max != "":
		return "≤ " + max
	default:
		return "—"
	}
}

// httpErrorName — имя класса ошибки субдомена по коду
// subdomain_results.error. Коды — internal/httpworker.ErrorClass (кладёт
// T-058); имена продублированы, чтобы CLI не тянул пакет воркера.
func httpErrorName(code int16) string {
	switch code {
	case 0:
		return "none"
	case 1:
		return "timeout"
	case 2:
		return "redirects"
	case 3:
		return "tls"
	case 4:
		return "body"
	case 5:
		return "network"
	default:
		return fmt.Sprintf("код %d", code)
	}
}

// contentKind — что сохранено в записи: итоговый DOM рендера или сырой
// HTML (ТЗ 5.6); «—» — контента нет (проверка не дошла до загрузки тел).
func contentKind(r domainResult) string {
	switch {
	case r.ContentHash == "":
		return "—"
	case r.Rendered:
		return "DOM"
	default:
		return "HTML"
	}
}

// aliasDesc — связь субдомена с другим субдоменом (ТЗ 5.5 п. 6):
// «≡ метка» — идентичен, «→ метка» — редирект.
func aliasDesc(r domainResult) string {
	switch model.AliasKind(r.AliasKind) {
	case model.AliasIdentical:
		return "≡ " + subLabel(r.Alias)
	case model.AliasRedirect:
		return "→ " + subLabel(r.Alias)
	default:
		return "—"
	}
}

// resultFinalDesc — итоговый URL либо ошибка проверки (код и класс,
// ТЗ 5.11 «ошибка»), «—» — ни того ни другого.
func resultFinalDesc(r domainResult) string {
	desc := r.FinalURL
	if r.Error != 0 {
		msg := fmt.Sprintf("ошибка %d (%s)", r.Error, httpErrorName(r.Error))
		if desc != "" {
			desc += " — " + msg
		} else {
			desc = msg
		}
	}
	return orEmDash(desc)
}

// writeDomainText — человекочитаемая сводка: шапка (домен/зона/статусы),
// DNS-живые субдомены, таблица результатов, признаки Laravel.
// minScore — порог laravel_domains из конфигурации (аналогично
// отображается в §5.8: score ≥ порога попадает в агрегат).
func writeDomainText(w io.Writer, info domainInfo, minScore float64) {
	d, z := info.Domain, info.Zone

	out(w, "домен:        %s\n", d.Name)
	if u := idn.ToUnicode(d.Name); u != d.Name {
		out(w, "unicode:      %s\n", u)
	}
	if z.NameUnicode != "" && z.NameUnicode != z.Name {
		out(w, "зона:         %s (unicode: %s, id %d)\n", z.Name, z.NameUnicode, z.ID)
	} else {
		out(w, "зона:         %s (id %d)\n", z.Name, z.ID)
	}
	out(w, "статус:       %s\nпроверка:     %s\nпроверен:     %s\n",
		orEmDash(d.StatusName), orEmDash(d.CheckStateName), orEmDash(d.LastCheckedAt))
	if d.QueuedAt != "" {
		out(w, "в очереди с:  %s\n", formatDomainTS(d.QueuedAt))
	}
	var flags []string
	if d.IsIDN {
		flags = append(flags, "is_idn")
	}
	if d.RenderPending {
		flags = append(flags, "render_pending")
	}
	if len(flags) > 0 {
		out(w, "флаги:        %s\n", strings.Join(flags, ", "))
	}
	if d.TempAttempts > 0 {
		out(w, "temp-попытки: %d\n", d.TempAttempts)
	}

	// Расшифровка dns_mask — labels справочника subdomains (ТЗ 5.11).
	if len(info.DNSAlive) == 0 {
		out(w, "\nDNS-живые субдомены (dns_mask = %d): нет\n", d.DNSMask)
	} else {
		labels := make([]string, len(info.DNSAlive))
		for i, s := range info.DNSAlive {
			labels[i] = subLabel(s.Label)
		}
		out(w, "\nDNS-живые субдомены (dns_mask = %d): %s\n", d.DNSMask, strings.Join(labels, ", "))
	}

	// Результаты по субдоменам × вариантам — моноширинная таблица
	// (стиль writeStatusTable: ширины по содержимому, последняя колонка
	// без паддинга).
	out(w, "\nрезультаты по субдоменам и вариантам:\n")
	if len(info.Results) == 0 {
		out(w, "  — нет (домен не проверялся или DNS не делегирован)\n")
	} else {
		head := []string{"субдомен", "вар.", "схема", "http", "контент", "алиас", "проверен", "итоговый URL / ошибка"}
		rows := [][]string{head}
		for _, r := range info.Results {
			rows = append(rows, []string{
				subLabel(r.Subdomain),
				model.Variant(r.Variant).String(),
				orEmDash(r.SchemeName),
				httpStatusDesc(r),
				contentKind(r),
				aliasDesc(r),
				formatDomainTSShort(r.CheckedAt),
				resultFinalDesc(r),
			})
		}
		widths := make([]int, len(head))
		for _, row := range rows {
			for i, c := range row {
				if len(c) > widths[i] {
					widths[i] = len(c)
				}
			}
		}
		for _, row := range rows {
			var b strings.Builder
			for i, c := range row {
				if i > 0 {
					b.WriteByte(' ')
				}
				if i == len(row)-1 {
					b.WriteString(c) // последняя колонка — как есть
				} else {
					fmt.Fprintf(&b, "%-*s", widths[i], c)
				}
			}
			out(w, "%s\n", b.String())
		}
	}

	writeLaravelText(w, info.Laravel, minScore)
}

// writeLaravelText — раздел признаков Laravel: агрегат (score, диапазон
// версий, субдомены, версия правил), подтвердившие срабатывания с
// фрагментами-свидетельствами и отдельной секцией слабые (is_laravel =
// false, ниже порога) — все срабатывания хранятся (ТЗ 13.1).
func writeLaravelText(w io.Writer, l *domainLaravel, minScore float64) {
	out(w, "\nLaravel:\n")
	if l == nil {
		out(w, "  — признаков не найдено (нет записи в laravel_domains;\n"+
			"    домены только со слабыми срабатываниями ниже порога %s не раскрываются)\n",
			scoreFmt(minScore))
		return
	}

	labels := make([]string, len(l.FoundOn))
	for i, s := range l.FoundOn {
		labels[i] = subLabel(s.Label)
	}
	out(w, "  score:       %s (порог %s)\n", scoreFmt(l.Score), scoreFmt(minScore))
	out(w, "  версии:      %s\n", verRange(l.VerMin, l.VerMax))
	out(w, "  субдомены:   %s\n", orEmDash(strings.Join(labels, ", ")))
	out(w, "  правила:     версия %d, обновлено %s\n", l.RulesVersion, orEmDash(formatDomainTS(l.UpdatedAt)))

	var weak, strong []domainFinding
	for _, f := range l.Findings {
		if f.IsLaravel {
			strong = append(strong, f)
		} else {
			weak = append(weak, f)
		}
	}
	if len(strong) > 0 {
		out(w, "  срабатывания (is_laravel = true):\n")
		for _, f := range strong {
			writeFindingText(w, f)
		}
	}
	if len(weak) > 0 {
		out(w, "  слабые срабатывания (is_laravel = false, score < %s):\n", scoreFmt(minScore))
		for _, f := range weak {
			writeFindingText(w, f)
		}
	}
}

// writeFindingText — одно срабатывание: субдомен, вариант, score,
// диапазон версий (если сузился) и правила с фрагментами-свидетельствами.
func writeFindingText(w io.Writer, f domainFinding) {
	ver := verRange(f.VerMin, f.VerMax)
	if ver != "—" {
		out(w, "    %s %s  score %s, версии %s:\n", subLabel(f.Subdomain), model.Variant(f.Variant), scoreFmt(f.Score), ver)
	} else {
		out(w, "    %s %s  score %s:\n", subLabel(f.Subdomain), model.Variant(f.Variant), scoreFmt(f.Score))
	}
	for _, s := range f.Signals {
		out(w, "      %s (+%s): «%s»\n", s.Rule, scoreFmt(s.Weight), s.Evidence)
	}
}

// scoreFmt — score/вес с тремя знаками: точность real в laravel_*
// (T-071) короче, дальнейший хвост не информативен.
func scoreFmt(v float64) string {
	return strconv.FormatFloat(v, 'f', 3, 64)
}

// writeDomainJSON — машинный вывод: сырой ответ domain_info
// (pretty-print, 2 пробела — нормализованный вид для глаз, json
// валиден и для jq). Ответ приходит из jsonb — всегда валиден; отказ
// json.Indent (чужой источник) страхуется выводом как есть.
func writeDomainJSON(w io.Writer, raw []byte) error {
	var buf bytes.Buffer
	if err := json.Indent(&buf, raw, "", "  "); err != nil {
		buf.Reset()
		buf.Write(raw)
	}
	buf.WriteByte('\n')
	if _, err := w.Write(buf.Bytes()); err != nil {
		return fmt.Errorf("domain: вывод json: %w", err)
	}
	return nil
}

// writeDomainNotFoundJSON — машинный ответ на «не найдено»:
// found=false и что удалось выяснить о зоне (для скриптов).
func writeDomainNotFoundJSON(w io.Writer, name, zone string, zoneFound bool) error {
	enc := json.NewEncoder(w)
	enc.SetEscapeHTML(false)
	if err := enc.Encode(struct {
		Found     bool   `json:"found"`
		Name      string `json:"name"`
		Zone      string `json:"zone"`
		ZoneFound bool   `json:"zone_found"`
	}{false, name, zone, zoneFound}); err != nil {
		return fmt.Errorf("domain: вывод json: %w", err)
	}
	return nil
}

// ---------------------------------------------------------------- контент

// domainContentStore — часть ContentStore, нужная domain --content:
// только Get. *content.PackStore (T-042) удовлетворяет; выделено
// интерфейсом для тестов без хранилища.
type domainContentStore interface {
	Get(ctx context.Context, hash content.Hash) ([]byte, error)
}

// selectContentResult — запись, чей контент показывает --content:
// первая «осмысленная». Приоритет — отрендеренной записи (итоговый DOM
// SPA — главный результат конвейера, ТЗ 5.6), при равенстве — первая
// запись с контентом в порядке субдомен×вариант (domain_info
// сортирует по subdomain_id, variant: апекс и основной вариант раньше).
func selectContentResult(results []domainResult) (domainResult, bool) {
	first := -1
	for i, r := range results {
		if r.ContentHash == "" {
			continue
		}
		if first < 0 {
			first = i
		}
		if r.Rendered {
			return r, true
		}
	}
	if first < 0 {
		return domainResult{}, false
	}
	return results[first], true
}

// writeDomainContent — вывод контента по --content: сводка домена и
// пометка о выбранной записи — в stderr, байты — в stdout как есть
// (перенаправление в файл; ТЗ 5.11 «распаковываются из хранилища»).
func writeDomainContent(ctx context.Context, store domainContentStore, info domainInfo, minScore float64, stdout, stderr io.Writer) error {
	sel, ok := selectContentResult(info.Results)
	if !ok {
		return errors.New("domain: у домена нет сохранённого контента (content_hash пуст во всех результатах — проверка не дошла до загрузки тел)")
	}
	rawHash, err := hex.DecodeString(sel.ContentHash)
	if err != nil || len(rawHash) != content.HashLen {
		return fmt.Errorf("domain: некорректный content_hash %q в результате (субдомен %s, вариант %s)",
			sel.ContentHash, subLabel(sel.Subdomain), model.Variant(sel.Variant))
	}
	var hash content.Hash
	copy(hash[:], rawHash)
	data, err := store.Get(ctx, hash)
	if err != nil {
		return fmt.Errorf("domain: контент %s: %w", sel.ContentHash, err)
	}

	kind := "HTML"
	if sel.Rendered {
		kind = "DOM (рендер)"
	}
	writeDomainText(stderr, info, minScore)
	out(stderr, "\nконтент (--content): субдомен %s, вариант %s, %s, %d байт, хэш %s\n",
		subLabel(sel.Subdomain), model.Variant(sel.Variant), kind, len(data), sel.ContentHash)
	if _, err := stdout.Write(data); err != nil {
		return fmt.Errorf("domain: вывод контента: %w", err)
	}
	return nil
}

// ---------------------------------------------------------------- команда

// cmdDomain — crawlerctl domain: нормализация имени (ТЗ 5.2), выборка
// domain_info, вывод (--json / --content / человекочитаемый).
func cmdDomain(args []string, stdout, stderr io.Writer) error {
	fs := newFlagSet("domain")
	asJSON := fs.Bool("json", false, "машинный вывод: ответ domain_info как есть (pretty-print)")
	withContent := fs.Bool("content", false, "вывести сохранённый контент записи (байты DOM/HTML) вместо сводки")
	if handled, err := parseFlags(fs, domainHelp, args, stdout); handled {
		return err
	}
	if fs.NArg() != 1 {
		return usagef("укажите имя домена: crawlerctl domain example.crawl.test")
	}
	if *asJSON && *withContent {
		return usagef("domain: флаги --json и --content несовместимы (машинный вывод не смешивается с байтами контента)")
	}
	// Нормализация ввода оператора — Normalize из internal/idn: тот же
	// конвейер, что у строк incoming (ТЗ 5.2). До конфигурации: неверное
	// имя — код 2 без обращения к стеку.
	res, err := idn.Normalize(fs.Arg(0))
	if err != nil {
		return usagef("некорректное имя домена %q: %v", fs.Arg(0), err)
	}

	// Конфигурация — единый YAML + окружение (internal/config): DSN
	// PostgreSQL, каталог контента и словарь zstd для --content,
	// порог laravel_min_score для раздела Laravel (стиль status, T-079).
	cfg, err := config.Load()
	if err != nil {
		return fmt.Errorf("domain: %w", err)
	}
	ctx := context.Background()

	// CLI pool_size = 2 — бюджет соединений ТЗ 6.3 у сервисов (стиль
	// manage.go, T-074).
	poolOpts := db.NewOptions(cfg.Postgres)
	poolOpts.PoolSize = 2
	pool, err := db.Open(ctx, poolOpts)
	if err != nil {
		return fmt.Errorf("domain: %w", err)
	}
	defer pool.Close()

	info, raw, found, err := fetchDomainInfo(ctx, pool, res.ASCII)
	if err != nil {
		return err
	}
	if !found {
		return domainNotFound(ctx, pool, res.ASCII, *asJSON, stdout)
	}

	if *asJSON {
		return writeDomainJSON(stdout, raw)
	}

	if *withContent {
		// Читатель хранилища контента без записи — стиль analyzer
		// (T-071): тот же словарь zstd и PackStore, что у пишущих этапов
		// (иначе распаковка чужих блоков невозможна), лимиты Put не нужны.
		// ReadOnly: чтение не создаёт каталог и файл блокировки — работает
		// и без прав на запись в content.dir, опечатка в каталоге не
		// создаёт посторонних путей (runbook: проверка чтения контента).
		if _, err := content.SetDictFile(cfg.Content.ZstdDictionary, slog.New(slog.NewTextHandler(stderr, nil))); err != nil {
			return fmt.Errorf("domain: %w", err)
		}
		store, err := content.OpenPackStore(pool, content.PackOptions{Dir: cfg.Content.Dir, ReadOnly: true})
		if err != nil {
			return fmt.Errorf("domain: %w", err)
		}
		defer func() { _ = store.Close() }()
		content.WarnDictMismatch(store.Dir(), slog.New(slog.NewTextHandler(stderr, nil)))
		return writeDomainContent(ctx, store, info, cfg.Analyzer.LaravelMinScore, stdout, stderr)
	}

	writeDomainText(stdout, info, cfg.Analyzer.LaravelMinScore)
	return nil
}

// domainNotFound — понятное сообщение, что именно не найдено (ТЗ 5.11):
// domain_info на оба случая отвечает NULL, команда доуточняет по
// справочнику zones. --json печатает found:false (скрипты), человек
// получает сообщение в stderr через run (код 1).
func domainNotFound(ctx context.Context, pool db.DB, name string, asJSON bool, stdout io.Writer) error {
	zone, zoneFound, err := fetchCatalogZone(ctx, pool, name)
	if err != nil {
		return err
	}
	if asJSON {
		if err := writeDomainNotFoundJSON(stdout, name, zone, zoneFound); err != nil {
			return err
		}
	}
	if zoneFound {
		label := strings.TrimSuffix(name, "."+zone)
		return fmt.Errorf("domain: домен %s не найден: в зоне %s нет домена с меткой %s (домен не загружен ingest-ом или зона каталога не совпадает со списком publicsuffix)", name, zone, label)
	}
	return fmt.Errorf("domain: домен %s не найден: зона %s отсутствует в справочнике zones (зоны создаёт ingest при первом файле)", name, zone)
}
