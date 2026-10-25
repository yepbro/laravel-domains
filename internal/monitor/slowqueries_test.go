// Юнит-тесты цикла медленных запросов (T-177): формат сообщения
// (шапка, строки с меткой сервиса и pid, обрезка текста, согласование
// числительных, хвост «…и ещё»), дебаунс pid+query_start (новый
// оператор — одно сообщение, дляящийся — тишина, завершившийся покидает
// базу, новый старт того же pid — снова сообщение, сбой источника базу
// не роняет), выключение интервалом 0. Живой pg_stat_activity — вне
// юнит-тестов (герметичность, стиль load_test.go / zonestats_test.go).
package monitor

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"
)

// fakeSlowSource — источник с запланированными снимками по проходам
// (стиль fakeLoadSource); исчерпав план, отдаёт пустые снимки.
// Счётчик под мьютексом: пишет горутина цикла, читает горутина теста
// (waitSlowPasses) — без замка это гонка под -race (T-190; стиль
// zonestatsFakePool). План passes/errs заполняется до старта цикла и
// дальше только читается — замка не требует.
type fakeSlowSource struct {
	mu     sync.Mutex
	passes [][]SlowQuery
	errs   []error
	calls  int
}

func (s *fakeSlowSource) SlowQueries(context.Context, time.Duration) ([]SlowQuery, error) {
	s.mu.Lock()
	i := s.calls
	s.calls++
	s.mu.Unlock()
	if i < len(s.errs) && s.errs[i] != nil {
		return nil, s.errs[i]
	}
	if i < len(s.passes) {
		return s.passes[i], nil
	}
	return nil, nil
}

// count — число выполненных проходов (потокобезопасное чтение счётчика).
func (s *fakeSlowSource) count() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.calls
}

// slowQueryAt — оператор с ключом дебаунса (pid, старт).
func slowQueryAt(pid int, app string, sec float64, started time.Time, query string) SlowQuery {
	return SlowQuery{PID: pid, App: app, Seconds: sec, StartedAt: started, Query: query}
}

// waitSlowPasses ждёт, пока источник отдаст минимум n снимков (стиль
// TestZoneStatsLoopTicker), и отменяет цикл: после исчерпания плана
// снимки пусты — рассылок больше не появляется, проверки числа
// сообщений детерминированы. Счётчик читается через count() под
// мьютексом фейка: пишет его горутина цикла (гонка под -race, T-190);
// сам отчёт (reports) читается только после <-done — канал даёт
// happens-before для всех рассылок цикла.
func waitSlowPasses(t *testing.T, src *fakeSlowSource, n int, cancel context.CancelFunc, done <-chan error) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for src.count() < n {
		if time.Now().After(deadline) {
			cancel()
			t.Fatalf("цикл не дошёл до прохода %d: %d", n, src.count())
		}
		time.Sleep(time.Millisecond)
	}
	cancel()
	if err := <-done; err != nil {
		t.Fatalf("цикл: %v", err)
	}
}

func TestSlowQueryKey(t *testing.T) {
	base := time.Date(2026, 10, 24, 12, 0, 0, 0, time.UTC)
	a := slowQueryAt(7, "scheduler", 5, base, "SELECT 1")
	b := slowQueryAt(7, "scheduler", 9, base.Add(time.Second), "SELECT 1")
	if a.key() == b.key() {
		t.Error("ключ: новый старт того же pid обязан отличаться")
	}
	if a.key() != slowQueryAt(7, "http-worker", 5, base, "SELECT 2").key() {
		t.Error("ключ: pid+старт совпали — тот же оператор")
	}
}

// TestSlowQueryLoop_Passes — полный прогон проходов: дебаунс pid+
// query_start: длящийся оператор молчит, новый старт того же pid —
// снова событие, завершившийся покидает базу.
func TestSlowQueryLoop_Passes(t *testing.T) {
	t0 := time.Date(2026, 10, 24, 12, 0, 0, 0, time.UTC)
	long := slowQueryAt(11, "scheduler", 42.3, t0, "UPDATE domains SET …")
	other := slowQueryAt(12, "http-worker", 7.8, t0.Add(2*time.Second), "SELECT …")
	restart := slowQueryAt(11, "scheduler", 4.0, t0.Add(time.Minute), "SELECT count(*) …")

	var reports []string
	src := &fakeSlowSource{passes: [][]SlowQuery{
		{long},          // старт: 1 новый
		{long, other},   // длящийся молчит, другой новый
		{long, other},   // оба длятся — тишина
		{},              // все закончились
		{long, restart}, // pid 11: новый старт оператора — событие
		{long, restart}, // тишина
	}}
	opts := SlowQueryOptions{
		Interval: time.Millisecond,
		Notify:   func(text string) { reports = append(reports, text) },
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- SlowQueryLoop(ctx, src, opts) }()
	waitSlowPasses(t, src, len(src.passes), cancel, done)

	if len(reports) != 3 {
		t.Fatalf("сообщений = %d, want 3 (новый оператор + другой оператор + новый старт)", len(reports))
	}
	// Шапка считает новые (события), длящиеся с прошлых проходов —
	// дополнением «всего в работе». Проход 5: после пустого прохода 4
	// оба оператора снова «новые» (ключ evicted — на практике
	// завершившийся оператор не возвращается).
	for i, want := range []string{
		"1 медленный запрос (порог 3с)",
		"1 медленный запрос (порог 3с), всего в работе 2",
		"2 медленных запроса (порог 3с)",
	} {
		if !strings.Contains(reports[i], want) {
			t.Errorf("сообщение %d без %q:\n%s", i, want, reports[i])
		}
	}
}

// TestSlowQueryLoop_ErrorKeepsDebounce — сбой источника не роняет базу
// дебаунса: оператор, наблюдавшийся до ошибки и вернувшийся после (тот
// же pid+старт — не заканчивался), повторного сообщения не порождает.
func TestSlowQueryLoop_ErrorKeepsDebounce(t *testing.T) {
	t0 := time.Date(2026, 10, 24, 12, 0, 0, 0, time.UTC)
	long := slowQueryAt(21, "analyzer", 31.5, t0, "SELECT …")
	var reports []string
	src := &fakeSlowSource{passes: [][]SlowQuery{
		{long}, // старт: 1 сообщение
		{long}, // длящийся: тишина
		{long}, // ошибка источника: Warn без рассылки, база живёт
		{long}, // возврат снимка: тот же ключ — тишина
	}}
	src.errs = []error{nil, nil, errors.New("БД недоступна"), nil}

	opts := SlowQueryOptions{Interval: time.Millisecond, Notify: func(text string) { reports = append(reports, text) }}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- SlowQueryLoop(ctx, src, opts) }()
	waitSlowPasses(t, src, len(src.passes), cancel, done)

	if len(reports) != 1 {
		t.Fatalf("сообщений = %d, want 1 (сбой не роняет базу дебаунса)", len(reports))
	}
}

// TestSlowQueryLoop_FinishedEvicted — пустой снимок без ошибки — честное
// «оператор закончился»: ключ покидает базу, повторное наблюдение того
// же ключа (вырожденный случай — на практике query_start уникален)
// снова считается событием.
func TestSlowQueryLoop_FinishedEvicted(t *testing.T) {
	t0 := time.Date(2026, 10, 24, 12, 0, 0, 0, time.UTC)
	long := slowQueryAt(21, "analyzer", 31.5, t0, "SELECT …")
	var reports []string
	src := &fakeSlowSource{passes: [][]SlowQuery{
		{long}, // старт: 1 сообщение
		{},     // закончился: ключ покинул базу
		{long}, // то же наблюдение снова — ключ уже не в базе: сообщение
	}}
	opts := SlowQueryOptions{Interval: time.Millisecond, Notify: func(text string) { reports = append(reports, text) }}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- SlowQueryLoop(ctx, src, opts) }()
	waitSlowPasses(t, src, len(src.passes), cancel, done)

	if len(reports) != 2 {
		t.Fatalf("сообщений = %d, want 2 (новое наблюдение после завершения)", len(reports))
	}
}

func TestSlowQueryLoop_Disabled(t *testing.T) {
	src := &fakeSlowSource{}
	err := SlowQueryLoop(context.Background(), src, SlowQueryOptions{Interval: 0})
	if err != nil {
		t.Fatalf("выключенный цикл: %v", err)
	}
	if src.count() != 0 {
		t.Errorf("выключенный цикл: проходов = %d, want 0", src.count())
	}
	if err := SlowQueryLoop(context.Background(), nil, SlowQueryOptions{Interval: time.Minute}); err == nil {
		t.Error("источник не задан: ожидается ошибка")
	}
}

// TestSlowQueryLoop_SourceError — ошибка каждого прохода не убивает
// цикл и не рассылает пустых сообщений.
func TestSlowQueryLoop_SourceError(t *testing.T) {
	var reports []string
	src := &fakeSlowSource{errs: []error{errors.New("БД недоступна")}}
	opts := SlowQueryOptions{Interval: time.Millisecond, Notify: func(text string) { reports = append(reports, text) }}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- SlowQueryLoop(ctx, src, opts) }()
	waitSlowPasses(t, src, 3, cancel, done)

	if len(reports) != 0 {
		t.Fatalf("сообщений = %d, want 0 (ошибка источника не рассылается)", len(reports))
	}
}

func TestFormatSlowQueryReport(t *testing.T) {
	t0 := time.Date(2026, 10, 24, 12, 0, 0, 0, time.UTC)
	fresh := []SlowQuery{
		slowQueryAt(101, "scheduler", 42.34, t0, "UPDATE \"domains\"\n  SET  check_state = 1,\n      queued_at = now()"),
		slowQueryAt(202, "http-worker", 7.8, t0, "SELECT * FROM domains WHERE id = 42"),
		slowQueryAt(203, "", 4.2, t0, strings.Repeat("x", 300)),
		slowQueryAt(204, "web", 3.4, t0, "SELECT 1"),
		slowQueryAt(205, "web", 3.1, t0, "SELECT 2"),
	}
	r := formatSlowQueryReport(fresh, len(fresh), 3*time.Second)
	for _, want := range []string{
		"🐌 PostgreSQL: 5 медленных запросов (порог 3с)",
		"• 42.3с [scheduler] pid 101: UPDATE \"domains\" SET check_state = 1, queued_at = now()", // пробелы схлопнуты
		"• 7.8с [http-worker] pid 202: SELECT * FROM domains WHERE id = 42",
		"• 4.2с pid 203: " + strings.Repeat("x", slowQueryMaxChars) + "…", // без метки, обрезка
		"…и ещё 2",
	} {
		if !strings.Contains(r, want) {
			t.Errorf("сообщение не содержит %q:\n%s", want, r)
		}
	}
	if n := strings.Count(r, "pid "); n != 3 {
		t.Errorf("строк запросов = %d, want 3 (потолок slowQueryTopLines)", n)
	}
	if s := ", всего в работе"; strings.Contains(r, s) {
		t.Errorf("тотальное число равно новым — дополнение %q не нужно:\n%s", s, r)
	}

	// Длящиеся с прошлых проходов: 2 в работе, 1 новое.
	r = formatSlowQueryReport(fresh[:1], 2, 3*time.Second)
	if !strings.Contains(r, "1 медленный запрос (порог 3с), всего в работе 2") {
		t.Errorf("шапка без «всего в работе 2»:\n%s", r)
	}

	// Один запрос — единственное число; нецелый порог — как есть;
	// скрытый текст — пометка.
	r = formatSlowQueryReport([]SlowQuery{slowQueryAt(7, "monitor-cli", 9.1, t0, "")}, 1, 2500*time.Millisecond)
	for _, want := range []string{
		"1 медленный запрос (порог 2.5с",
		"• 9.1с [monitor-cli] pid 7: (текст недоступен — нет pg_read_all_stats)",
	} {
		if !strings.Contains(r, want) {
			t.Errorf("сообщение не содержит %q:\n%s", want, r)
		}
	}

	// Согласование числительных: 23 — «запроса» (2–4), 11 и 12 —
	// «запросов» (исключение десятков), 21 — «запрос».
	for n, want := range map[int]string{
		2: "медленных запроса", 5: "медленных запросов",
		11: "медленных запросов", 21: "медленный запрос", 23: "медленных запроса",
	} {
		if got := slowQueriesPhrase(n); got != want {
			t.Errorf("slowQueriesPhrase(%d) = %q, want %q", n, got, want)
		}
	}
}

func TestSlowQueryText(t *testing.T) {
	if got := slowQueryText("  SELECT\t1\n  FROM\tx  "); got != "SELECT 1 FROM x" {
		t.Errorf("пробелы не схлопнуты: %q", got)
	}
	if got := slowQueryText(strings.Repeat("б", slowQueryMaxChars+10)); len([]rune(got)) != slowQueryMaxChars+1 {
		t.Errorf("обрезка: %d рун, want %d (+многоточие)", len([]rune(got)), slowQueryMaxChars)
	}
	if exact := strings.Repeat("б", slowQueryMaxChars); slowQueryText(exact) != exact {
		t.Errorf("текст ровно в лимит не обрезается: %q", slowQueryText(exact))
	}
	if got := slowQueryText("  "); !strings.Contains(got, "текст недоступен") {
		t.Errorf("пустой текст: %q", got)
	}
}
