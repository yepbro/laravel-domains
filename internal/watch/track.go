package watch

import (
	"sort"
	"time"
)

// fileSig — состояние файла, стабильность которого проверяется: пока
// размер и mtime неизменны, запись считается завершённой (ТЗ 5.1).
type fileSig struct {
	size    int64
	modTime time.Time
}

// candidate — файл-кандидат: замечен в состоянии sig с момента since.
type candidate struct {
	sig   fileSig
	since time.Time
}

// tracker решает, какие файлы готовы к выдаче: чистая логика без I/O,
// время — через now (заглушка в тестах). Повторная выдача одного файла
// исключена: выданный файл запоминается по пути и сигнатуре. Если по тому
// же пути появляется файл с другим размером/mtime — это новый файл, он
// будет выдан ещё раз; постоянная идемпотентность (по содержимому,
// независимо от имени) — ingest_files в БД, задача T-021.
type tracker struct {
	stableAfter time.Duration
	now         func() time.Time

	candidates map[string]candidate
	delivered  map[string]fileSig
}

func newTracker(stableAfter time.Duration, now func() time.Time) *tracker {
	return &tracker{
		stableAfter: stableAfter,
		now:         now,
		candidates:  make(map[string]candidate),
		delivered:   make(map[string]fileSig),
	}
}

// observe фиксирует состояние файла (size/modTime). event=true — состояние
// пришло из события записи (Create/Write): окно стабильности отсчитывается
// заново всегда, даже при неизменной сигнатуре — на ФС с грубым mtime
// дозапись может не поменять размер/mtime, а событие гарантирует, что файл
// трогали. event=false — плановый обход: окно сбрасывается только когда
// сигнатура изменилась.
//
// false — файл с этим состоянием уже был выдан и повторно выдаваться не
// должен; такой путь не отслеживается.
func (t *tracker) observe(path string, size int64, modTime time.Time, event bool) bool {
	sig := fileSig{size: size, modTime: modTime}
	if d, ok := t.delivered[path]; ok && d == sig {
		return false
	}
	c, ok := t.candidates[path]
	switch {
	case !ok || c.sig != sig:
		t.candidates[path] = candidate{sig: sig, since: t.now()}
	case event:
		c.since = t.now()
		t.candidates[path] = c
	}
	return true
}

// ready проверяет кандидата path: если его состояние прямо сейчас (по
// свежему stat) совпадает с наблюдаемым и живёт дольше stableAfter — файл
// готов. Кандидат при этом потребляется и переходит в выданные: второй раз
// тот же файл не выдаётся.
func (t *tracker) ready(path string, size int64, modTime time.Time) (File, bool) {
	c, ok := t.candidates[path]
	if !ok {
		return File{}, false
	}
	sig := fileSig{size: size, modTime: modTime}
	if c.sig != sig || t.now().Sub(c.since) < t.stableAfter {
		return File{}, false
	}
	delete(t.candidates, path)
	t.delivered[path] = sig
	return File{Path: path, Size: size, ModTime: modTime}, true
}

// forget убирает кандидата — файл исчез, не став стабильным. История
// выданных сохраняется: исчезнувший и снова появившийся файл с той же
// сигнатурой повторно не выдаётся.
func (t *tracker) forget(path string) {
	delete(t.candidates, path)
}

// paths возвращает пути кандидатов в детерминированном порядке.
func (t *tracker) paths() []string {
	out := make([]string, 0, len(t.candidates))
	for p := range t.candidates {
		out = append(out, p)
	}
	sort.Strings(out)
	return out
}
