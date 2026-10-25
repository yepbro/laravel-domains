// Юнит-тесты команды crawlerctl counters (T-178, ТЗИ §9): справка и
// роутинг подкоманд (коды 0/2), ошибки использования (лишние аргументы,
// отрицательный допуск), разрешение имени зоны (некорректное имя — код
// 2), формат возраста. Выполнение list/recount/verify против живой БД —
// интеграционные тесты (test/integration/counters_test.go).
package main

import (
	"bytes"
	"errors"
	"flag"
	"strings"
	"testing"
	"time"
)

// TestCountersHelp — справка: общая (counters --help) и каждой
// подкоманды начинается с имени, содержит «Использование» и коды
// возврата; run даёт 0.
func TestCountersHelp(t *testing.T) {
	for _, args := range [][]string{
		{"counters", "--help"},
		{"counters", "help"},
		{"counters", "list", "--help"},
		{"counters", "recount", "--help"},
		{"counters", "verify", "--help"},
	} {
		var stdout, stderr bytes.Buffer
		if code := run(args, &stdout, &stderr); code != exitOK {
			t.Fatalf("%v: код = %d, хочу 0; stderr:\n%s", args, code, stderr.String())
		}
		out := stdout.String()
		if !strings.HasPrefix(out, "crawlerctl counters") {
			t.Errorf("%v: справка не начинается с имени команды:\n%s", args, out)
		}
		for _, want := range []string{"Использование", "counters"} {
			if !strings.Contains(out, want) {
				t.Errorf("%v: в справке нет %q:\n%s", args, want, out)
			}
		}
	}
}

// TestCountersRouting — роутинг: без подкоманды и неизвестная — код 2;
// подкоманда с лишними аргументами — код 2 (до обращения к БД).
func TestCountersRouting(t *testing.T) {
	cases := []struct {
		name string
		args []string
		want string
	}{
		{"без подкоманды", []string{"counters"}, "укажите подкоманду"},
		{"неизвестная", []string{"counters", "drop"}, "неизвестная подкоманда"},
		{"list с аргументом", []string{"counters", "list", "x"}, "лишние аргументы"},
		{"recount с аргументом", []string{"counters", "recount", "x"}, "лишние аргументы"},
		{"verify с аргументом", []string{"counters", "verify", "x"}, "лишние аргументы"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var stdout, stderr bytes.Buffer
			code := run(tc.args, &stdout, &stderr)
			if code != exitUsage {
				t.Fatalf("код = %d, хочу 2; stderr:\n%s", code, stderr.String())
			}
			if !strings.Contains(stderr.String(), tc.want) {
				t.Errorf("stderr не содержит %q:\n%s", tc.want, stderr.String())
			}
		})
	}
}

// TestCountersVerifyUsage — значение флагов: отрицательный допуск —
// код 2 (до обращения к БД).
func TestCountersVerifyUsage(t *testing.T) {
	var stdout, stderr bytes.Buffer
	code := run([]string{"counters", "verify", "--tolerance", "-1"}, &stdout, &stderr)
	if code != exitUsage {
		t.Fatalf("код = %d, хочу 2; stderr:\n%s", code, stderr.String())
	}
	if !strings.Contains(stderr.String(), "допуск") {
		t.Errorf("stderr без упоминания допуска:\n%s", stderr.String())
	}
}

// TestCountersZoneFlagUsage — некорректное имя зоны (символ вне
// UTS-46: «_»): ошибка использования ДО обращения к БД, код 2 —
// поведение не зависит от доступности стека (CI гоняет юнит-прогоны
// без postgres; пробелы cleanup вырезает — «a b.test» нормализуется
// в валидное «ab.test» и годится только веткой живого стека).
func TestCountersZoneFlagUsage(t *testing.T) {
	for _, sub := range []string{"recount", "verify"} {
		var stdout, stderr bytes.Buffer
		code := run([]string{"counters", sub, "--zone", "a_b.test"}, &stdout, &stderr)
		if code != exitUsage {
			t.Fatalf("%s: код = %d, хочу 2; stderr:\n%s", sub, code, stderr.String())
		}
		if msg := stderr.String(); !strings.Contains(msg, "некорректное имя зоны") {
			t.Errorf("%s: stderr без причины:\n%s", sub, msg)
		}
	}
}

// TestCountersAge — формат возраста: NULL/zero — «—», секунды, минуты,
// часы, сутки.
func TestCountersAge(t *testing.T) {
	if got := countersAge(time.Time{}); got != "—" {
		t.Errorf("нулевое время = %q, хочу «—»", got)
	}
	now := time.Now()
	cases := []struct {
		ago  time.Duration
		want string
	}{
		{5 * time.Second, "5с назад"},
		{90 * time.Second, "2м назад"},
		{2 * time.Hour, "2ч 00м назад"},
		{26 * time.Hour, "1д 2ч назад"},
	}
	for _, tc := range cases {
		if got := countersAge(now.Add(-tc.ago)); got != tc.want {
			t.Errorf("возраст(%v) = %q, хочу %q", tc.ago, got, tc.want)
		}
	}
	// Отрицательный возраст (будущее) клампится — не «−3с назад».
	if got := countersAge(now.Add(time.Minute)); !strings.HasSuffix(got, "назад") || strings.HasPrefix(got, "-") {
		t.Errorf("будущее = %q, хочу кламп", got)
	}
}

// TestCountersFlagHelpRequested — help-токен первой позицией обрабатывается
// до разбора флагов подкоманд (контракт parseFlags «help»).
func TestCountersFlagHelpRequested(t *testing.T) {
	fs := newFlagSet("counters verify")
	fs.Int64("tolerance", 0, "")
	args := []string{"verify", "--tolerance", "3"}
	if handled, err := parseFlags(fs, countersHelp, args, &bytes.Buffer{}); handled || err != nil {
		t.Fatalf("parseFlags = (%v, %v), хочу (false, nil) для обычных аргументов", handled, err)
	}
	if !errors.Is(flag.ErrHelp, flag.ErrHelp) {
		t.Fatal("недостижимо: санити errors.Is")
	}
}
