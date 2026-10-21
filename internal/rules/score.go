// score.go — расчёт итогов применения правил (ТЗ 5.8): score по «noisy-OR»,
// диапазон версий как пересечение диапазонов сработавших правил и порог
// is_laravel.

package rules

import (
	"math"
	"strconv"
	"strings"
)

// DefaultMinScore — порог is_laravel по умолчанию (analyzer.laravel_min_score,
// ТЗ 5.8/5.10; конфиг валидирует значение в (0, 1]).
const DefaultMinScore = 0.6

// ScoreRules — расчёт по сработавшим правилам страницы:
//
//   - score — «noisy-OR» 1 − Π(1 − wᵢ): слабые признаки накапливаются,
//     но не дают 100 %;
//   - диапазон версий — пересечение [ver_min, ver_max] сработавших правил:
//     нижняя граница — максимум из ver_min, верхняя — минимум из ver_max
//     (правило без границы диапазон не сужает); строки границ сохраняются
//     из правил как есть («9.x» не превращается в «9»);
//   - пустое пересечение (ver_max < ver_min) — conflict=true, границы
//     сбрасываются: диапазон неопределён (при записи в БД это ver_min и
//     ver_max NULL, признак конфликта виден по обеим пустым границам).
//
// Некорректные строки версий (parseVersion = nil) игнорируются — набор
// правил уже прошёл валидацию загрузчика, здесь это защита от опечаток.
func ScoreRules(matched []Rule) (score float64, verMin, verMax string, conflict bool) {
	var (
		minV, maxV       version
		minStr, maxStr   string
		haveMin, haveMax bool
	)
	for _, r := range matched {
		score = noisyOR(score, r.Weight)

		if v := parseVersion(r.VerMin); v != nil {
			if !haveMin || minV.less(v) {
				minV, minStr, haveMin = v, r.VerMin, true
			}
		}
		if v := parseVersion(r.VerMax); v != nil {
			if !haveMax || v.less(maxV) {
				maxV, maxStr, haveMax = v, r.VerMax, true
			}
		}
	}

	if haveMin {
		verMin = minStr
	}
	if haveMax {
		verMax = maxStr
	}
	if haveMin && haveMax && maxV.less(minV) {
		verMin, verMax, conflict = "", "", true
	}
	return score, verMin, verMax, conflict
}

// IsLaravel — уверенное срабатывание: score ≥ порога (колонка
// laravel_findings.is_laravel, ТЗ 5.8/6.1). minScore —
// analyzer.laravel_min_score; неположительное значение заменяется
// DefaultMinScore (валидация конфига такого не допускает — защита для
// прямых вызовов с неинициализированным порогом).
func (res Result) IsLaravel(minScore float64) bool {
	if minScore <= 0 {
		minScore = DefaultMinScore
	}
	return res.Score >= minScore
}

// noisyOR: 1 − (1−a)(1−b) — слабые признаки накапливаются, но не дают 100%.
func noisyOR(a, b float64) float64 {
	return 1 - (1-a)*(1-b)
}

// ScoreFixed — noisy-OR по набору весов.
func ScoreFixed(weights []float64) float64 {
	s := 0.0
	for _, w := range weights {
		s = noisyOR(s, w)
	}
	return s
}

// ---------------------------------------------------------------- версии

// version — числовые компоненты версии; "x" означает +∞ (граница «до конца
// мажорной ветки», например ver_max: "9.x"). Отсутствующий компонент — 0.
type version []float64

func parseVersion(s string) version {
	// Эквивалентный мутант (T-M12): обрезка всей строки избыточна —
	// каждый компонент отдельно обрезается strings.TrimSpace ниже, а
	// пробельное начало/конец строки — это начало первого и конец
	// последнего компонента.
	// mutator-disable-next-line statement/remove
	s = strings.TrimSpace(s)
	// Эквивалентный мутант (T-M12): без раннего возврата пустая строка
	// расщепляется на [""], ParseFloat("") даёт ErrSyntax — тот же
	// итоговый nil. Аннотация стоит на строке if, а не тела: якорь
	// branch/if — оператор if (урок контрольного прогона T-M10).
	// mutator-disable-next-line branch/if
	if s == "" {
		return nil
	}
	parts := strings.Split(s, ".")
	out := make(version, 0, len(parts))
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p == "x" || p == "*" {
			// Эквивалентные мутанты (T-M12): math.Inf возвращает +Inf при
			// любом знаке ≥ 0 — Inf(0) и Inf(2) тот же +Inf, что и Inf(1)
			// (документация stdlib).
			// mutator-disable-next-line numbers/decrementer, numbers/incrementer
			out = append(out, math.Inf(1))
			continue
		}
		// Эквивалентные мутанты (T-M12): strconv.ParseFloat различает
		// только bitSize 32 (отдельная ветка разбора), прочие значения
		// идут путём 64 — 63 и 65 ведут себя как 64, включая ErrRange
		// на «1e400» (проверено эмпирически).
		// mutator-disable-next-line numbers/decrementer, numbers/incrementer
		n, err := strconv.ParseFloat(p, 64)
		if err != nil {
			return nil // некорректная версия игнорируется
		}
		out = append(out, n)
	}
	return out
}

func (v version) less(o version) bool {
	// Эквивалентные мутанты (T-M12): дополнительная итерация при
	// i == len сравнивает два отсутствующих компонента (0 и 0) — они
	// равны, цикл выходит без результата; итог тот же. Вырезание левого
	// операнда OR убиваемо — хвост приёмника с отрицательной компонентой
	// виден только полным перебором (TestVersionCompare, «9.-1»).
	// mutator-disable-next-line expression/comparison
	for i := 0; i < len(v) || i < len(o); i++ {
		var a, b float64
		if i < len(v) {
			a = v[i]
		}
		if i < len(o) {
			b = o[i]
		}
		if a != b {
			// Эквивалентный мутант (T-M12): ветка исполняется только при
			// a != b, где «a <= b» ≡ «a < b»; NaN-компоненты дают false в
			// обоих вариантах (несравнимость).
			// mutator-disable-next-line expression/comparison
			return a < b
		}
	}
	return false
}

// CompareVersions — сравнение строк версий по числовым компонентам
// (семантика границ правил, T-070): отсутствующий компонент — 0, «x»/«*» —
// +∞; возвращает -1 (a < b), 0 (эквивалентны, в том числе разная запись
// «9.19» и «9.19.0») или 1 (a > b). Экспортирован для агрегации
// laravel_domains (T-072: min(ver_min)/max(ver_max) по findings домена) —
// текстовое MIN/MAX в SQL упорядочило бы «10» раньше «5.3». Пустая или
// некорректная версия (parseVersion = nil) не упорядочена — 0; findings
// хранят только строки, прошедшие валидацию набора (T-068), вызывающий
// не передаёт сюда мусор.
func CompareVersions(a, b string) int {
	va, vb := parseVersion(a), parseVersion(b)
	switch {
	case va == nil || vb == nil:
		return 0
	case va.less(vb):
		return -1
	case vb.less(va):
		return 1
	default:
		return 0
	}
}

// Contains — попадает ли ожидаемый диапазон [expMin, expMax] внутрь
// вычисленного (используется make test-rules).
func (res Result) Contains(expMin, expMax string) bool {
	if expMin != "" {
		e := parseVersion(expMin)
		lo := parseVersion(res.VerMin)
		if e == nil {
			return false
		}
		if lo != nil && e.less(lo) {
			return false
		}
	}
	if expMax != "" {
		e := parseVersion(expMax)
		hi := parseVersion(res.VerMax)
		if e == nil {
			return false
		}
		if hi != nil && hi.less(e) {
			return false
		}
	}
	return true
}
