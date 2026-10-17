package render

import (
	"fmt"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"
)

func TestParseProcStat(t *testing.T) {
	// Поля после «)» начинаются с state: ppid — 2-е, rss (страницы) — 22-е;
	// vsize (21-е) не должен попадать в rss. Имя процесса — со скобками
	// и пробелами (разбор по последней «)»).
	line := fmt.Sprintf("42 (chromium (renderer)) R 7 %s999999 128 0 0 0 0 0 0 0 0 0",
		strings.Repeat("0 ", 18))
	st, ok := parseProcStat([]byte(line))
	if !ok {
		t.Fatal("строка stat не разобрана")
	}
	if st.ppid != 7 {
		t.Errorf("ppid = %d, хочу 7", st.ppid)
	}
	if want := int64(128 * os.Getpagesize()); st.rss != want {
		t.Errorf("rss = %d, хочу %d (128 страниц)", st.rss, want)
	}

	// Обрезанные и битые строки отвергаются, а не паникуют.
	for name, bad := range map[string]string{
		"без скобок":    "1 2 3 4",
		"пустое имя":    "1 () R 7",
		"мало полей":    "1 (p) R 7 0",
		"ppid не число": "1 (p) R x " + strings.Repeat("0 ", 20),
		"rss не число":  "1 (p) R 7 " + strings.Repeat("0 ", 19) + "y",
	} {
		t.Run(name, func(t *testing.T) {
			if _, ok := parseProcStat([]byte(bad)); ok {
				t.Fatalf("битая строка принята: %q", bad)
			}
		})
	}
}

func TestTreeRSS(t *testing.T) {
	procs := map[int]procStat{
		1: {ppid: 0, rss: 100},
		2: {ppid: 1, rss: 200},
		3: {ppid: 1, rss: 50},
		4: {ppid: 2, rss: 10},  // косвенный потомок (zygote/рендерер)
		9: {ppid: 8, rss: 999}, // чужое дерево
	}
	if got := treeRSS(procs, 1); got != 360 {
		t.Errorf("treeRSS(1) = %d, хочу 360 (дерево 1+2+3+4)", got)
	}
	if got := treeRSS(procs, 2); got != 210 {
		t.Errorf("treeRSS(2) = %d, хочу 210", got)
	}
	if got := treeRSS(procs, 42); got != 0 {
		t.Errorf("treeRSS(несуществующий корень) = %d, хочу 0", got)
	}
	// Цикл в ppid не зацикливает обход.
	cycle := map[int]procStat{1: {ppid: 2, rss: 1}, 2: {ppid: 1, rss: 2}}
	if got := treeRSS(cycle, 1); got != 3 {
		t.Errorf("treeRSS(цикл) = %d, хочу 3", got)
	}
}

func TestProcTreeRSSSelf(t *testing.T) {
	if _, err := os.Stat("/proc"); err != nil {
		t.Skip("/proc недоступен")
	}
	if got := procTreeRSS(0); got != -1 {
		t.Errorf("procTreeRSS(0) = %d, хочу -1 (нет корня)", got)
	}
	// Живой процесс — сам тест: RSS положителен, дерево включает потомков.
	if got := procTreeRSS(os.Getpid()); got <= 0 {
		t.Fatalf("RSS собственного процесса %d, хочу > 0", got)
	}
}

func TestProcTreeRSSRealChild(t *testing.T) {
	if _, err := os.Stat("/proc"); err != nil {
		t.Skip("/proc недоступен")
	}
	// Живой дочерний процесс: разбор реального /proc/<pid>/stat свежего
	// процесса (не кэша тестового процесса).
	cmd := exec.Command("sleep", "5")
	if err := cmd.Start(); err != nil {
		t.Skipf("sleep не запустился: %v", err)
	}
	defer func() { _ = cmd.Process.Kill(); _ = cmd.Wait() }()

	var rss int64
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if rss = procTreeRSS(cmd.Process.Pid); rss > 0 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if rss <= 0 {
		t.Fatalf("RSS дерева процесса sleep = %d, хочу > 0", rss)
	}
}
