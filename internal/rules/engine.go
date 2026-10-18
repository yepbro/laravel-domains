package rules

import (
	"context"
	"errors"
	"regexp"
	"sort"
	"sync"
)

// Cookie — cookie со страницы (достаточно для пассивного анализа).
type Cookie struct {
	Name  string
	Value string
}

// Page — данные, по которым применяются правила: контент, cookies и заголовки.
type Page struct {
	HTML    []byte
	Cookies []Cookie
	Headers map[string][]string // имена в канонической форме MIME-заголовка
}

// Evidence — сработавшее правило и краткое свидетельство; формат колонки
// laravel_findings.signals (jsonb, миграция 000001: [{rule, weight, evidence}]).
type Evidence struct {
	Rule     string
	Weight   float64
	Evidence string
}

// Result — итог применения набора правил к странице.
type Result struct {
	Score    float64 // noisy-OR по сработавшим правилам
	Signals  []Evidence
	VerMin   string
	VerMax   string
	Conflict bool // пересечение диапазонов версий пустое
}

// metaTagRe вырезает meta-теги из HTML. Закрывающий «>» ищется вне
// кавычек значений атрибутов: ветка «не > и не кавычка» чередуется с
// закавыченными кускками ("…" / '…'), поэтому content="a>b" не обрывает
// тег посреди значения (AN-08). Незакрытая кавычка — тег не выделяется
// вовсе, а не усечённым обрывом. Матчинг линеен (RE2, без бэктрекинга).
var metaTagRe = regexp.MustCompile(`(?is)<meta\b(?:[^>"']|"[^"]*"|'[^']*')*>`)

// errPatternTooLong — паттерн не проходит по Limits.MaxPatternBytes (ТЗ §9:
// ограничение недоверенного входа; правило молча пропускается, как и
// некомпилируемое — валидация набора на этапе загрузки за T-068, движок
// подстраховывается сам).
var errPatternTooLong = errors.New("rules: паттерн длиннее лимита")

// Evaluate применяет все правила набора к странице с ограничениями по
// умолчанию (DefaultLimits, ТЗ §9). Основной вход анализатора.
func (s *Set) Evaluate(page Page) Result {
	return s.EvaluateCtx(context.Background(), page, DefaultLimits())
}

// EvaluateCtx применяет правила с явным контекстом и ограничениями
// (анализатор задаёт бюджет отмены shutdown/потребления, тесты — узкие
// лимиты). Нулевые Limits целиком включают DefaultLimits; контекст
// проверяется между правилами и между значениями cookie/заголовков; при
// отмене просмотр прекращается, результат содержит сработавшее до отмены.
func (s *Set) EvaluateCtx(ctx context.Context, page Page, lim Limits) Result {
	if lim == (Limits{}) {
		lim = DefaultLimits()
	}
	if lim.EvalTimeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, lim.EvalTimeout)
		defer cancel()
	}

	res := Result{}

	var matched []Rule
	for _, r := range s.Rules {
		if ctx.Err() != nil {
			break // бюджет времени исчерпан — что нашли, то нашли
		}
		ev, ok := matchRule(ctx, r, page, lim)
		if !ok {
			continue
		}
		res.Signals = append(res.Signals, Evidence{Rule: r.ID, Weight: r.Weight, Evidence: ev})
		matched = append(matched, r)
	}

	res.Score, res.VerMin, res.VerMax, res.Conflict = ScoreRules(matched)
	return res
}

// compileCache кэширует скомпилированные паттерны (sync.Map — наборы
// перезагружаются по SIGHUP без перезапуска, Evaluate зовут параллельные
// горутины воркера): без кэша каждая страница платила бы компиляцию каждого
// правила. Ошибки компиляции кэшируются тоже — некорректный паттерн не
// перекомпилируется на каждом вызове. Время жизни кэша — текущий набор:
// смена rules_version очищает его (resetCompileCache из Reloader.Reload),
// записи ушедших паттернов и осевшие ошибки не накапливаются весь срок
// жизни процесса.
var compileCache sync.Map // string → compileResult

// resetCompileCache очищает кэш компиляции при смене набора правил:
// паттерны (и кэшированные ошибки компиляции) не живут дольше набора,
// их породившего. Безопасна при параллельных Evaluate — конкурентный
// compileCached по промаху просто перекомпилирует паттерн.
func resetCompileCache() {
	compileCache.Clear()
}

type compileResult struct {
	re  *regexp.Regexp
	err error
}

func compileCached(pattern string) (*regexp.Regexp, error) {
	if v, ok := compileCache.Load(pattern); ok {
		cr := v.(compileResult)
		return cr.re, cr.err
	}
	re, err := regexp.Compile(pattern) // RE2: линейный матчинг, без бэктрекинга
	compileCache.Store(pattern, compileResult{re, err})
	return re, err
}

// ruleRegex компилирует паттерн правила с учётом лимитов; nil-regex —
// правило точного совпадения (match) либо пропускаемое (лимит/ошибка).
func ruleRegex(r Rule, lim Limits) (*regexp.Regexp, error) {
	if r.Regex == "" {
		return nil, nil
	}
	if !lim.patternAllowed(r.Regex) {
		return nil, errPatternTooLong
	}
	return compileCached(r.Regex)
}

// sortedHeaderNames — детерминированный порядок имён заголовков: обход map
// в Go случайный, а свидетельства переанализа должны быть воспроизводимы.
func sortedHeaderNames(h map[string][]string) []string {
	names := make([]string, 0, len(h))
	for name := range h {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}

// matchRule ищет первое совпадение правила на странице: cookie имя/значение
// и заголовки — точное совпадение (match) или regex; html/meta — только
// regex (ТЗ 5.8: «регулярное выражение по HTML»). Возвращает свидетельство
// сработавшего совпадения — оно строится из тех же обрезанных лимитами
// строк, по которым шёл матчинг (работа над свидетельством ограничена
// MaxValueBytes, ТЗ §9). Порядок обхода детерминирован (см. evidence.go).
func matchRule(ctx context.Context, r Rule, page Page, lim Limits) (string, bool) {
	if ctx.Err() != nil {
		return "", false // бюджет исчерпан — правило не смотрится
	}
	regex, err := ruleRegex(r, lim)
	if err != nil {
		return "", false // некорректные/раздутые выражения отсеиваются
	}
	matches := func(s string) bool {
		if r.Match != "" {
			return s == r.Match
		}
		return regex != nil && regex.MatchString(s)
	}

	switch r.Target {
	case TargetCookieName:
		for _, c := range page.Cookies {
			if ctx.Err() != nil {
				return "", false
			}
			name := lim.valueCap(c.Name)
			if matches(name) {
				return finalizeEvidence(evidenceCookieName(name), lim), true
			}
		}
	case TargetCookieValue:
		for _, c := range page.Cookies {
			if ctx.Err() != nil {
				return "", false
			}
			value := lim.valueCap(c.Value)
			if matches(value) {
				return finalizeEvidence(evidenceCookieValue(c.Name, value), lim), true
			}
		}
	case TargetHeaderName:
		for _, name := range sortedHeaderNames(page.Headers) {
			if ctx.Err() != nil {
				return "", false
			}
			capped := lim.valueCap(name)
			if matches(capped) {
				return finalizeEvidence(evidenceHeaderName(capped), lim), true
			}
		}
	case TargetHeaderValue:
		for _, name := range sortedHeaderNames(page.Headers) {
			for _, v := range page.Headers[name] {
				if ctx.Err() != nil {
					return "", false
				}
				value := lim.valueCap(v)
				if matches(value) {
					return finalizeEvidence(evidenceHeaderValue(name, value), lim), true
				}
			}
		}
	case TargetHTML:
		if regex != nil {
			// вход ограничен до матчинга (ТЗ §9); Find возвращает первый
			// фрагмент — он и есть свидетельство
			if m := regex.Find(lim.htmlCap(page.HTML)); m != nil {
				return finalizeEvidence(string(m), lim), true
			}
		}
	case TargetMeta:
		if regex != nil {
			// скользящий поиск meta-тегов без материализации всех совпадений:
			// выход на первом сработавшем, каждый шаг двигает позицию за конец
			// предыдущего тега
			html := lim.htmlCap(page.HTML)
			pos := 0
			for pos < len(html) {
				if ctx.Err() != nil {
					return "", false
				}
				loc := metaTagRe.FindIndex(html[pos:])
				if loc == nil {
					break
				}
				tag := html[pos+loc[0] : pos+loc[1]]
				if regex.Match(tag) {
					return finalizeEvidence(string(tag), lim), true
				}
				pos += loc[1]
			}
		}
	}
	return "", false
}
