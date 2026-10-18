// ---------------------------------------------------------------- check
//
// Живая проверка одного домена вне цикла планировщика (T-119): конвейер
// вынесен в библиотечный пакет internal/checker (T-123 — общий для CLI и
// формы лендинга), команда печатает итог. Read-only к БД и очередям;
// UA/таймауты — из конфигурации; никаких активных зондов (этика §5.8/§9).

package main

import (
	"fmt"
	"io"

	"crawler/internal/checker"
	"crawler/internal/config"
	"crawler/internal/db"
	"crawler/internal/idn"
)

// checkHelp — справка crawlerctl check --help.
const checkHelp = `crawlerctl check — живая проверка домена конвейером (T-119)

Использование:
  crawlerctl check <домен>

Полный цикл для одного имени: DNS (справочник subdomains из БД) → HTTP
обоих схем DNS-живых субдоменов (рендер SPA-страниц при настроенном
render.remote_url) → пассивный анализ правилами Laravel. Вывод по
субдоменам: статус, цепочка редиректов, сводка TLS, сработавшие правила
с evidence, score, диапазон версий.

Read-only: ничего не пишет в БД и не публикует в очереди — постановка
домена на штатную проверку отдельной командой (crawlerctl recheck).
User-Agent, таймауты и лимиты — из конфигурации (секции dns/http/render);
анализ — актуальный набор правил analyzer.rules_dir и порог
analyzer.laravel_min_score. Никаких активных зондов: только GET /
живых субдоменов (пассивная этика ТЗ §5.8/§9).
Коды возврата: 0 — проверка выполнена, 1 — ошибка, 2 — использование.
`

func cmdCheck(args []string, stdout, stderr io.Writer) error {
	fs := newFlagSet("check")
	if handled, err := parseFlags(fs, checkHelp, args, stdout); handled {
		return err
	}
	if fs.NArg() != 1 {
		return usagef("укажите ровно один домен: crawlerctl check example.com")
	}
	if _, err := idn.Normalize(fs.Arg(0)); err != nil {
		return usagef("check: некорректное имя домена %q: %v", fs.Arg(0), err)
	}

	cfg, err := config.Load()
	if err != nil {
		return fmt.Errorf("check: %w", err)
	}
	ctx, stop := signalCtx()
	defer stop()

	poolOpts := db.NewOptions(cfg.Postgres)
	poolOpts.PoolSize = 2 // бюджет соединений ТЗ 6.3 — у сервисов
	pool, err := db.Open(ctx, poolOpts)
	if err != nil {
		return fmt.Errorf("check: %w", err)
	}
	defer pool.Close()

	res, err := checker.Run(ctx, cfg, pool, fs.Arg(0), checkLog{out: stderr})
	if err != nil {
		return fmt.Errorf("check: %w", err)
	}
	checkPrintResult(stdout, res, cfg.Analyzer.LaravelMinScore, cfg.Analyzer.RulesDir)
	return nil
}

// checkLog — минимальный логгер пакета checker в stderr команды.
type checkLog struct{ out io.Writer }

func (l checkLog) Warn(msg string, args ...any) {
	out(l.out, "check: %s", msg)
	for i := 0; i+1 < len(args); i += 2 {
		out(l.out, " %v=%v", args[i], args[i+1])
	}
	out(l.out, "\n")
}
func (l checkLog) Info(msg string, args ...any) {}

// checkPrintResult — вывод итога проверки.
func checkPrintResult(w io.Writer, res *checker.Result, minScore float64, rulesDir string) {
	out(w, "check: %s (зона %s", res.Domain, res.Zone)
	if res.Unicode != res.Domain {
		out(w, ", unicode %s", res.Unicode)
	}
	out(w, ") — правила %s, порог %.2f, DNS %s\n\n", rulesDir, minScore, res.Apex)

	if res.Terminal != "" {
		out(w, "итог: терминальный статус DNS-этапа — %s (адресов нет)\n", res.Terminal)
		return
	}

	for i := range res.Records {
		r := &res.Records[i]
		label := r.Label
		if label == "" {
			label = "(апекс)"
		}
		out(w, "%s [%s] запись %s/%s: %s %s\n", r.Host, label, r.Variant, r.Scheme, r.Status, r.FinalURL)
		if len(r.Redirects) > 1 {
			out(w, "    редиректы:")
			for j, step := range r.Redirects {
				if j == len(r.Redirects)-1 {
					break
				}
				out(w, " %d→%s", step.Status, step.URL)
			}
			out(w, "\n")
		}
		if r.HasTLS {
			verdict := "валиден"
			if !r.TLSValid {
				verdict = "ПРОБЛЕМЫ"
			}
			out(w, "    TLS: %s, истекает %s\n", verdict, r.TLSNotAfter.Format("2006-01-02"))
		}
		src := "исходный HTML"
		if r.Rendered {
			src = "DOM после рендера"
		}
		out(w, "    laravel: score=%.3f ver=[%s..%s] правил=%d (%s)\n",
			r.Score, orDash(r.VerMin), orDash(r.VerMax), len(r.Signals), src)
		for _, ev := range r.Signals {
			out(w, "      - %-28s w=%.2f  «%s»\n", ev.Rule, ev.Weight, ev.Evidence)
		}
	}
	out(w, "\ncheck: %s — итог score=%.3f found=%v, DNS %d мс, всего %d мс (ничего не записано)\n",
		res.Domain, res.Score, res.Found, res.DNSMS, res.TotalMS)
}
