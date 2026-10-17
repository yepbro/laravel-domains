package rules

import (
	"math"
	"testing"
)

func TestNoisyOR(t *testing.T) {
	cases := []struct {
		ws   []float64
		want float64
	}{
		{nil, 0}, // ни одного сработавшего правила
		{[]float64{0.25}, 0.25},
		{[]float64{0.25, 0.45}, 1 - 0.75*0.55},
		{[]float64{0.2, 0.2, 0.2, 0.2, 0.2}, 1 - 0.8*0.8*0.8*0.8*0.8},
		{[]float64{1, 0.2}, 1}, // вес 1 — уверенный признак закрывает score
		{[]float64{0.15, 0.15}, 1 - 0.85*0.85},
	}
	for _, c := range cases {
		if got := ScoreFixed(c.ws); diff(got, c.want) > 1e-9 {
			t.Errorf("noisyOR(%v) = %v, want %v", c.ws, got, c.want)
		}
	}
}

func diff(a, b float64) float64 {
	if a-b < 0 {
		return b - a
	}
	return a - b
}

// scoreRulesCases — правила без цели/выражения: ScoreRules смотрит только на
// weight и границы версий, матчинг здесь не участвует.
func scoreRulesCases() []struct {
	name           string
	rules          []Rule
	verMin, verMax string
	conflict       bool
} {
	return []struct {
		name           string
		rules          []Rule
		verMin, verMax string
		conflict       bool
	}{
		{
			name: "нет правил",
		},
		{
			name:  "без границ — диапазон не ограничен",
			rules: []Rule{{ID: "a", Weight: 0.25}, {ID: "b", Weight: 0.2}},
		},
		{
			name:   "только ver_min — берётся максимум",
			rules:  []Rule{{ID: "a", Weight: 0.3, VerMin: "9.19"}, {ID: "b", Weight: 0.2, VerMin: "10"}, {ID: "c", Weight: 0.2}},
			verMin: "10",
		},
		{
			name:   "только ver_max — берётся минимум",
			rules:  []Rule{{ID: "a", Weight: 0.3, VerMax: "11.x"}, {ID: "b", Weight: 0.2, VerMax: "9.x"}},
			verMax: "9.x",
		},
		{
			name:   "пересечение [9.19,∞) и (−∞,9.x] = [9.19,9.x]",
			rules:  []Rule{{ID: "a", Weight: 0.15, VerMin: "9.19"}, {ID: "b", Weight: 0.15, VerMax: "9.x"}},
			verMin: "9.19", verMax: "9.x",
		},
		{
			name:   "равные границы — диапазон из одной версии, не конфликт",
			rules:  []Rule{{ID: "a", Weight: 0.3, VerMin: "9.19"}, {ID: "b", Weight: 0.3, VerMax: "9.19"}},
			verMin: "9.19", verMax: "9.19",
		},
		{
			name:   "равные границы в разной записи — 9.19 = 9.19.0",
			rules:  []Rule{{ID: "a", Weight: 0.3, VerMin: "9.19"}, {ID: "b", Weight: 0.3, VerMax: "9.19.0"}},
			verMin: "9.19", verMax: "9.19.0",
		},
		{
			name:   "пустое пересечение [10,∞) ∩ (−∞,9.x] — конфликт, диапазон сброшен",
			rules:  []Rule{{ID: "a", Weight: 0.3, VerMin: "10"}, {ID: "b", Weight: 0.3, VerMax: "9.x"}},
			verMin: "10", verMax: "9.x",
			conflict: true,
		},
		{
			name:   "точечный конфликт 9.19.1 < 10",
			rules:  []Rule{{ID: "a", Weight: 0.3, VerMin: "10"}, {ID: "b", Weight: 0.3, VerMax: "9.19.1"}},
			verMin: "10", verMax: "9.19.1",
			conflict: true,
		},
		{
			name:   "некорректные версии игнорируются",
			rules:  []Rule{{ID: "a", Weight: 0.3, VerMin: "lts"}, {ID: "b", Weight: 0.3, VerMax: "9..1"}, {ID: "c", Weight: 0.3, VerMin: "9.19"}},
			verMin: "9.19",
		},
	}
}

// В случаях с конфликтом verMin/verMax в таблице — ожидаемые границы ДО
// сброса: ScoreRules обязан их обнулить.
func TestScoreRules(t *testing.T) {
	for _, c := range scoreRulesCases() {
		weights := make([]float64, len(c.rules))
		for i, r := range c.rules {
			weights[i] = r.Weight
		}

		score, verMin, verMax, conflict := ScoreRules(c.rules)
		if want := ScoreFixed(weights); diff(score, want) > 1e-9 {
			t.Errorf("%s: score = %v, want %v", c.name, score, want)
		}
		if c.conflict {
			if !conflict {
				t.Errorf("%s: ожидался конфликт диапазонов", c.name)
			}
			if verMin != "" || verMax != "" {
				t.Errorf("%s: при конфликте диапазон должен сбрасываться, got [%s, %s]", c.name, verMin, verMax)
			}
			continue
		}
		if conflict {
			t.Errorf("%s: неожиданный конфликт диапазонов", c.name)
		}
		if verMin != c.verMin || verMax != c.verMax {
			t.Errorf("%s: диапазон = [%s, %s], want [%s, %s]", c.name, verMin, verMax, c.verMin, c.verMax)
		}
	}
}

func TestIsLaravel(t *testing.T) {
	cases := []struct {
		name            string
		score, minScore float64
		want            bool
	}{
		{"ровно порог", 0.6, 0.6, true}, // сравнение нестрогое: score >= порога
		{"ниже порога", 0.59, 0.6, false},
		{"выше порога", 0.61, 0.6, true},
		{"порог из конфига", 0.75, 0.75, true},
		{"максимальный порог", 1, 1, true},
		{"нулевой порог — дефолт 0.6", 0.6, 0, true},
		{"нулевой порог — дефолт 0.6, score ниже", 0.5, 0, false},
		{"отрицательный порог — дефолт 0.6", 0.55, -1, false},
	}
	for _, c := range cases {
		res := Result{Score: c.score}
		if got := res.IsLaravel(c.minScore); got != c.want {
			t.Errorf("%s: IsLaravel(%v, порог %v) = %v, want %v", c.name, c.score, c.minScore, got, c.want)
		}
	}
}

func TestVersionParse(t *testing.T) {
	inf := math.Inf(1)
	cases := []struct {
		in   string
		want version
	}{
		{"", nil},
		{"   ", nil},
		{"9.19", version{9, 19}},
		{"10", version{10}},
		{" 9.19 ", version{9, 19}}, // пробелы вокруг и внутри компонент
		{"9. 19", version{9, 19}},
		{"9.x", version{9, inf}},
		{"9.*", version{9, inf}},
		{"8.83.27", version{8, 83, 27}},
		{"lts", nil},  // не число
		{"9..1", nil}, // пустой компонент
		{"9.19.1.2.x", version{9, 19, 1, 2, inf}},
	}
	for _, c := range cases {
		if got := parseVersion(c.in); !versionEq(got, c.want) {
			t.Errorf("parseVersion(%q) = %v, want %v", c.in, got, c.want)
		}
	}
}

func versionEq(a, b version) bool {
	if (a == nil) != (b == nil) {
		return false
	}
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func TestVersionCompare(t *testing.T) {
	cases := []struct {
		a, b string
		less bool
	}{
		{"9.19", "11", true}, // числовое сравнение, не строковое
		{"11", "9.19", false},
		{"9.19", "9.x", true}, // 9.19 < всё девятое
		{"9.x", "10", true},   // бесконечность только в последнем компоненте
		{"9", "9.1", true},    // отсутствующий компонент = 0
		{"9.19", "9.19", false},
		{"9.19", "9.19.1", true},  // 9.19 = 9.19.0 < 9.19.1
		{"9.19.1", "9.19", false}, // и наоборот
		{"10.x", "10.1", false},   // 10.x — до конца мажорной ветки
		{"10.1", "10.x", true},
	}
	for _, c := range cases {
		va, vb := parseVersion(c.a), parseVersion(c.b)
		if va == nil || vb == nil {
			t.Fatalf("parseVersion(%q или %q) вернул nil", c.a, c.b)
		}
		if got := va.less(vb); got != c.less {
			t.Errorf("%s < %s = %v, want %v", c.a, c.b, got, c.less)
		}
	}
}

func TestResultContains(t *testing.T) {
	res := Result{VerMin: "9.19", VerMax: ""}
	if !res.Contains("9.19", "") {
		t.Error("[9.19,∞) должен включать 9.19")
	}
	if !res.Contains("10", "11") {
		t.Error("[9.19,∞) должен включать [10,11]")
	}
	if res.Contains("9.18", "") {
		t.Error("[9.19,∞) не должен включать 9.18")
	}
	res2 := Result{VerMin: "9.19", VerMax: "11.x"}
	if !res2.Contains("10", "11") {
		t.Error("[9.19,11.x] должен включать [10,11]")
	}
	if res2.Contains("11", "12") {
		t.Error("[9.19,11.x] не должен включать 12")
	}
	res3 := Result{} // неограниченный диапазон включает всё
	if !res3.Contains("5", "12") {
		t.Error("(−∞,∞) должен включать любой диапазон")
	}
	if res3.Contains("lts", "") {
		t.Error("некорректное ожидание не может входить ни в какой диапазон")
	}
}
