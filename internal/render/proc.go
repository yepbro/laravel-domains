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
	// Мутанты ветки (T-M07): отказ чтения /proc в CI недостижим —
	// контейнер всегда имеет читаемый /proc (TestProcTreeRSSSelf и
	// TestReadProcStatsSkipsNonNumeric читают его), скрыть или
	// отмонтировать его из теста нельзя. Вырезание return (branch/if)
	// — та же недостижимая ветка. Значение -1 — контракт «неизвестно»,
	// закреплён тестами через rootPID <= 0 выше.
	// mutator-disable-next-line branch/if
	if err != nil {
		// mutator-disable-next-line numbers/decrementer, numbers/incrementer
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
	// Недостижимо в CI (T-M07): /proc в контейнере всегда листируется;
	// отказ ReadDir требует скрытого/отмонтированного /proc. Вырезание
	// return (branch/if) — та же недостижимая ветка.
	// mutator-disable-next-line branch/if
	if err != nil {
		return nil, err
	}
	procs := make(map[int]procStat, len(entries))
	for _, e := range entries {
		pid, err := strconv.Atoi(e.Name())
		if err != nil {
			// Мутант loop/break эквивалентен (T-M07): os.ReadDir отдаёт
			// записи отсортированными по имени, а нечисловые записи /proc
			// («acpi», «bus», «cmdline»…) алфавитные и идут после всех
			// pid — break на первой нечисловой равен continue (дальше
			// pid уже нет). Сам пропуск закреплён тестом
			// TestReadProcStatsSkipsNonNumeric.
			// mutator-disable-next-line loop/break
			continue // нечисловое имя — не процесс
		}
		data, err := os.ReadFile("/proc/" + e.Name() + "/stat")
		// Вырезание continue (branch/if) эквивалентно (T-M07): без
		// continue падение на parseProcStat(data) с data == nil безопасно —
		// пустая строка отвергается разбором (LastIndex = -1), запись
		// не добавляется. loop/break недетерминирован: срабатывание
		// требует смерти процесса между листингом и чтением — гонка,
		// управляемо не воспроизводится в CI.
		// mutator-disable-next-line branch/if
		if err != nil {
			// mutator-disable-next-line loop/break
			continue // процесс ушёл между листингом и чтением
		}
		if st, ok := parseProcStat(data); ok {
			procs[pid] = st
		}
	}
	// Недостижимо в CI (T-M07): пустой снимок требует, чтобы в /proc
	// не было ни одного читаемого процесса — сам тестовый процесс
	// всегда виден; ровно один (мутант `== 1`) в контейнере go test
	// тоже недостижим (минимум PID 1 и сам тестовый бинарник).
	// Вырезание return (branch/if) — та же недостижимая ветка.
	// mutator-disable-next-line numbers/decrementer, numbers/incrementer, branch/if
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
	// Эквивалентные мутанты (T-M07): `i+2 >= len(s)` и `i+3 > len(s)`
	// отличаются от оригинала только при i+2 == len(s) — и там обе ветви
	// дают отказ (Fields(s[len:]) пуст → < 22 полей). Аннотации попутно
	// гасят убиваемых `i <= 0` и `i < 1` (')' первым символом) — trade-off
	// строчных аннотаций (§4 docs/mutation-testing.md); поведение закреплено
	// TestParseProcStat.
	// mutator-disable-next-line expression/comparison, numbers/incrementer
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
			// loop/break эквивалентен (T-M07): !ok возможен только для
			// корня — первого элемента очереди (потомки попадают в неё
			// лишь из children, построенного по ключам снимка); после
			// извлечения корня очередь пуста, break и continue неразличимы.
			// Сам пропуск (в т. ч. с потомками корня в снимке) закреплён
			// TestTreeRSS.
			// mutator-disable-next-line loop/break
			continue
		}
		total += st.rss
		for _, c := range children[pid] {
			if !seen[c] {
				// Эквивалентный мутант (T-M07): каждый pid входит ровно
				// в один список children (ppid единственный); повторно в
				// очередь может попасть только корень, а он отмечен при
				// инициализации (seen = {root: true}) — вырезание
				// маркировки ненаблюдаемо.
				// mutator-disable-next-line statement/remove
				seen[c] = true
				queue = append(queue, c)
			}
		}
	}
	return total
}
