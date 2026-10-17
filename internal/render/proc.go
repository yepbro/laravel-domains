package render

import (
	"os"
	"strconv"
	"strings"
)

// Контроль памяти Chromium (T-065, ТЗ 5.6 «перезапуск … при превышении
// порога памяти»): CDP не отдаёт RSS процесса, поэтому порог считается по
// /proc — суммарный RSS дерева процессов Chromium (главный процесс плюс
// все потомки: рендереры, GPU, crashpad — в бюджете ТЗ 3.2 участвуют все).
// Linux-only по построению; деплой проекта — Linux (systemd/cgroups,
// ТЗ 10.1), в остальных средах чтение возвращает «неизвестно» (-1) и
// перезапуск по памяти не срабатывает (Browser.RSS).

// procStat — поля /proc/<pid>/stat, нужные для дерева и памяти.
type procStat struct {
	ppid int   // PPid — родитель, по нему строится дерево
	rss  int64 // RSS в байтах (поле stat «rss», страницы × размер страницы)
}

// procTreeRSS — суммарный RSS дерева процессов с корнем root, байт.
// -1 — прочитать /proc не удалось; корень отсутствует в снимке (процесс
// уже умер) даёт 0.
func procTreeRSS(root int) int64 {
	if root <= 0 {
		return -1
	}
	procs, err := readProcStats()
	if err != nil {
		return -1
	}
	return treeRSS(procs, root)
}

// readProcStats — один проход по /proc: снимок {pid → ppid, rss}.
// Процессы, исчезнувшие между листингом и чтением, пропускаются: снимок
// консистентен по построению, задержки между записями для RSS-порога
// незначимы (проверка — раз в задачу, ТЗ 5.6).
func readProcStats() (map[int]procStat, error) {
	entries, err := os.ReadDir("/proc")
	if err != nil {
		return nil, err
	}
	procs := make(map[int]procStat, len(entries))
	for _, e := range entries {
		pid, err := strconv.Atoi(e.Name())
		if err != nil {
			continue // нечисловое имя — не процесс
		}
		data, err := os.ReadFile("/proc/" + e.Name() + "/stat")
		if err != nil {
			continue // процесс ушёл между листингом и чтением
		}
		if st, ok := parseProcStat(data); ok {
			procs[pid] = st
		}
	}
	if len(procs) == 0 {
		return nil, os.ErrNotExist
	}
	return procs, nil
}

// parseProcStat разбирает строку /proc/<pid>/stat. Имя процесса берётся по
// последней «)» — comm может содержать скобки и пробелы; далее поля идут
// с state: ppid — поле 4, rss — поле 24 (в страницах, man proc).
func parseProcStat(data []byte) (procStat, bool) {
	s := string(data)
	i := strings.LastIndexByte(s, ')')
	if i < 0 || i+2 > len(s) {
		return procStat{}, false
	}
	f := strings.Fields(s[i+2:])
	if len(f) < 22 { // до rss включительно
		return procStat{}, false
	}
	ppid, err := strconv.Atoi(f[1])
	if err != nil {
		return procStat{}, false
	}
	pages, err := strconv.ParseInt(f[21], 10, 64)
	if err != nil {
		return procStat{}, false
	}
	return procStat{ppid: ppid, rss: pages * int64(os.Getpagesize())}, true
}

// treeRSS суммирует RSS дерева от root по снимку процессов. Замкнутые
// циклы в ppid (повреждённый снимок) невозможны физически, но «посещен»
// гарантирует завершение на любых входных данных.
func treeRSS(procs map[int]procStat, root int) int64 {
	children := make(map[int][]int, len(procs))
	for pid, st := range procs {
		children[st.ppid] = append(children[st.ppid], pid)
	}
	var total int64
	seen := map[int]bool{root: true}
	queue := []int{root}
	for len(queue) > 0 {
		pid := queue[0]
		queue = queue[1:]
		st, ok := procs[pid]
		if !ok {
			continue
		}
		total += st.rss
		for _, c := range children[pid] {
			if !seen[c] {
				seen[c] = true
				queue = append(queue, c)
			}
		}
	}
	return total
}
