// ---------------------------------------------------------------- domain
//
// Каркас crawlerctl domain (ТЗ 5.11, задача T-081). Нормализация имени
// работает уже сейчас (internal/idn — тот же конвейер, что и для строк
// incoming); выборка статуса, результатов и признаков Laravel из БД —
// в T-081.

package main

import (
	"encoding/json"
	"fmt"
	"io"

	"crawler/internal/idn"
)

// domainHelp — справка crawlerctl domain --help.
const domainHelp = `crawlerctl domain — информация о домене по имени (ТЗ 5.11)

Использование:
  crawlerctl domain <имя> [--json] [--content]

Имя — в любой форме: unicode или punycode, любой регистр, точка на конце
(нормализация по ТЗ 5.2, зона отделяется по списку публичных суффиксов).

Сейчас выводит нормализованное имя: punycode, unicode, зона, метка.
Статус домена, результаты по субдоменам и признакам Laravel и контент
(--content) появятся в задаче T-081 — после записи результатов в БД.
`

func cmdDomain(args []string, stdout, stderr io.Writer) error {
	fs := newFlagSet("domain")
	asJSON := fs.Bool("json", false, "машинный вывод (для скриптов)")
	withContent := fs.Bool("content", false, "показать сохранённый контент (T-081)")
	if handled, err := parseFlags(fs, domainHelp, args, stdout); handled {
		return err
	}
	if fs.NArg() != 1 {
		return usagef("укажите имя домена: crawlerctl domain example.crawl.test")
	}

	// Нормализация ввода оператора — Normalize из internal/idn: та же
	// логика, что будет у T-081 (единый источник, а не копия idna здесь).
	res, err := idn.Normalize(fs.Arg(0))
	if err != nil {
		return usagef("некорректное имя домена %q: %v", fs.Arg(0), err)
	}

	if *asJSON {
		// Заготовка машинного вывода T-081: пока только нормализация,
		// поля статуса и признаков добавит T-081.
		enc := json.NewEncoder(stdout)
		enc.SetEscapeHTML(false)
		if err := enc.Encode(struct {
			ASCII   string `json:"ascii"`
			Unicode string `json:"unicode"`
			Zone    string `json:"zone"`
			Label   string `json:"label"`
			IsIDN   bool   `json:"is_idn"`
		}{res.ASCII, res.Unicode, res.Zone, res.Label, res.IsIDN}); err != nil {
			return fmt.Errorf("domain: вывод json: %w", err)
		}
		return nil
	}

	out(stdout, "домен:      %s\npunycode:   %s\nunicode:    %s\nзона:       %s\nметка:      %s\n\n",
		fs.Arg(0), res.ASCII, res.Unicode, res.Zone, res.Label)
	note := "статус и признаки Laravel"
	if *withContent {
		note += ", контент (--content)"
	}
	out(stdout, "%s: задача T-081 (запись результатов в PostgreSQL ещё не реализована).\n", note)
	return nil
}
