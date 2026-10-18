// crawlerctl — CLI управления Domain Crawler (ТЗ 4.1 основного ТЗ).
//
// Команды управления (status, domain, subdomains, zones, reanalyze,
// recheck): status/domain/reanalyze работают с dev-стеком (PostgreSQL,
// Redis, хранилище контента), остальные — каркасы: команды разбирают
// аргументы и флаги, валидируют вход (для имён доменов и зон — пакет
// internal/idn, тот же конвейер, что и для строк incoming) и сообщают,
// в какой задаче появится функциональность. Команды dev-окружения seed
// и test-rules реализованы полностью.
//
// Коды возврата: 0 — успех, включая --help; 1 — ошибка выполнения, в том
// числе «не реализовано» (вызывающий скрипт не должен считать действие
// выполненным); 2 — ошибка использования (неверные аргументы, флаги или
// вход команды).
package main

import (
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"strings"
)

// Коды возврата crawlerctl (полный контракт — в комментарии пакета).
const (
	exitOK    = 0
	exitError = 1
	exitUsage = 2
)

// commandFunc — обработчик команды: аргументы после имени команды и потоки
// вывода. Контракт ошибок: usageErr → код 2, flag.ErrHelp (справка уже
// напечатана) → код 0, nil → код 0, прочее → код 1.
type commandFunc func(args []string, stdout, stderr io.Writer) error

// commands — таблица команд: роутинг в run и список в usageText.
var commands = map[string]commandFunc{
	"status":     cmdStatus,
	"domain":     cmdDomain,
	"subdomains": cmdSubdomains,
	"zones":      cmdZones,
	"reanalyze":  cmdReanalyze,
	"recheck":    cmdRecheck,
	"dict":       cmdDict,
	"seed":       cmdSeed,
	"test-rules": cmdTestRules,
}

// usageText — справка верхнего уровня: без аргументов, help, неизвестная
// команда. Команды, реализуемые будущими задачами, помечены их номерами.
const usageText = `crawlerctl — CLI Domain Crawler

Команды управления (основное ТЗ, 4.1):
  status                        сводка по зонам, очередям, скорости (T-079)
  domain <имя> [--json] [--content]
                                информация о домене: статус, признаки
                                Laravel, контент (ТЗ 5.11)
  subdomains list|add|disable|recheck
                                 управление проверяемыми субдоменами
                                 (T-083)
  zones list|enable|disable     управление зонами (T-084)
  reanalyze [--zone ИМЯ]        переанализ результатов по новым правилам (T-074)
  recheck <домен> [домен …]     принудительная постановка домена на
                                проверку (T-084)
  dict train <файл> [--samples ПУТЬ]…
                                обучение/переобучение словаря сжатия zstd
                                по HTML-образцам (ТЗ 5.7)

Команды dev-окружения:
  seed [--incoming DIR] [--scenarios FILE] [--bulk N]
                                тестовые списки доменов в incoming
  test-rules [--rules DIR] [--samples DIR] [--min-score F]
                                прогон правил Laravel по корпусу образцов

Справка по команде: crawlerctl <команда> --help
`

func main() {
	os.Exit(run(os.Args[1:], os.Stdout, os.Stderr))
}

// run — роутинг команд; возвращает код возврата. Выделен из main для
// тестов: проверяются коды, вывод в stdout/stderr — без запуска процесса.
func run(args []string, stdout, stderr io.Writer) int {
	if len(args) == 0 {
		out(stderr, "%s", usageText)
		return exitUsage
	}
	switch args[0] {
	case "help", "-h", "--help":
		out(stdout, "%s", usageText)
		return exitOK
	}
	cmd, ok := commands[args[0]]
	if !ok {
		out(stderr, "crawlerctl: неизвестная команда %q\n\n%s", args[0], usageText)
		return exitUsage
	}
	err := cmd(args[1:], stdout, stderr)
	switch {
	case err == nil:
		return exitOK
	case errors.Is(err, flag.ErrHelp):
		return exitOK // справка уже напечатана командой
	case isUsageErr(err):
		out(stderr, "crawlerctl: %v\n", err)
		return exitUsage
	default:
		out(stderr, "crawlerctl: %v\n", err)
		return exitError
	}
}

// ---------------------------------------------------------------- ошибки

// usageErr — ошибка использования: неверные аргументы, флаги или вход
// команды. run возвращает по ней код 2 (как и справка без аргументов).
type usageErr struct{ err error }

func (e *usageErr) Error() string { return e.err.Error() }
func (e *usageErr) Unwrap() error { return e.err }

func isUsageErr(err error) bool {
	var u *usageErr
	return errors.As(err, &u)
}

// usagef — ошибка использования с форматированием.
func usagef(format string, a ...any) error {
	return &usageErr{fmt.Errorf(format, a...)}
}

// ---------------------------------------------------------------- вывод

// out — печать в поток команды. Ошибка вывода не проверяется намеренно:
// поток — os.Stdout/os.Stderr процесса, реакции на отказ записи нет
// (явное игнорирование — вместо подавления errcheck по всему пакету).
func out(w io.Writer, format string, a ...any) {
	_, _ = fmt.Fprintf(w, format, a...)
}

// ---------------------------------------------------------------- флаги

// newFlagSet — FlagSet в едином стиле crawlerctl: ошибки парсинга
// возвращаются, а не завершают процесс, и не пишут лишнего в вывод
// (справку печатаем сами — только по явному запросу, см. parseFlags).
func newFlagSet(name string) *flag.FlagSet {
	fs := flag.NewFlagSet(name, flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	fs.Usage = func() {}
	return fs
}

// parseFlags парсит флаги команды; help — её человекочитаемая справка.
// Явный запрос справки печатает её и возвращает (true, flag.ErrHelp) —
// run превращает это в код 0. Флаги -h/--help распознаются только там,
// где их считал бы флагами сам flag (flagHelpRequested): токен-значение
// флага (--samples --help — путь «--help») и всё после терминатора «--»
// или первого позиционного аргумента — не запрос справки, а обычный
// аргумент команды. Голое слово help — только первым аргументом команды,
// иначе это позиционный аргумент (crawlerctl domain --json help — имя
// домена). Ошибка парсинга — (true, usageErr), т.е. код 2.
func parseFlags(fs *flag.FlagSet, help string, args []string, stdout io.Writer) (bool, error) {
	if len(args) > 0 && args[0] == "help" {
		out(stdout, "%s", help)
		return true, flag.ErrHelp
	}
	if flagHelpRequested(fs, args) {
		out(stdout, "%s", help)
		return true, flag.ErrHelp
	}
	if err := fs.Parse(args); err != nil {
		return true, usagef("%s: %v", fs.Name(), err)
	}
	return false, nil
}

// flagHelpRequested — есть ли в аргументах -h/--help как флаг. Разбор
// повторяет правила пакета flag: на первом позиционном аргументе (и на
// терминаторе «--») разбор флагов заканчивается; за флагом, требующим
// отдельного значения (не булевым, без «=»), следующий токен — значение,
// даже если выглядит как --help (crawlerctl seed --incoming --help —
// каталог «--help», а не справка).
func flagHelpRequested(fs *flag.FlagSet, args []string) bool {
	for i := 0; i < len(args); i++ {
		a := args[i]
		if a == "--" || len(a) < 2 || a[0] != '-' {
			return false // терминатор/позиционный: дальше флагов нет
		}
		if a == "-h" || a == "--help" {
			return true
		}
		if flagTakesValue(fs, a) {
			i++ // следующий токен — значение флага
		}
	}
	return false
}

// flagTakesValue — требует ли токен флага отдельного значения: булевы
// флаги (flag.Bool) значения не берут, остальные (--zone, --samples,
// включая fs.Func) — берут; форма -name=value самодостаточна.
func flagTakesValue(fs *flag.FlagSet, a string) bool {
	if strings.Contains(a, "=") {
		return false
	}
	f := fs.Lookup(strings.TrimLeft(a, "-"))
	if f == nil {
		return false // неизвестный флаг — ошибка разбора в fs.Parse
	}
	type boolFlag interface{ IsBoolFlag() bool }
	bf, ok := f.Value.(boolFlag)
	return !ok || !bf.IsBoolFlag()
}

// flagExplicit — задан ли флаг явно в командной строке (fs.Visit обходит
// только флаги, разобранные Parse). Различает «передали» и «дефолт» —
// дефолт можно дополнить источником из конфигурации (test-rules).
func flagExplicit(fs *flag.FlagSet, name string) bool {
	set := false
	fs.Visit(func(f *flag.Flag) {
		if f.Name == name {
			set = true
		}
	})
	return set
}
