// Тесты источника PSI (T-162): парсер файла давления (some avg10/60/300,
// полный файл CPU без full, лишние строки, битые значения), деградации
// (ядро без PSI — errNoPSI, прочие ошибки чтения), отмена ctx, обёртка
// метрик.
package monitor

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"crawler/internal/metrics"
)

// TestParsePSI — разбор строки some: три средних, total пропускается;
// строка full (у памяти/IO; у CPU её нет) мимо; отсутствие some или
// битые поля — ошибки.
func TestParsePSI(t *testing.T) {
	st, err := parsePSI("some avg10=1.25 avg60=2.50 avg300=3.75 total=562500\n")
	if err != nil {
		t.Fatalf("parsePSI: %v", err)
	}
	if st.Avg10 != 1.25 || st.Avg60 != 2.5 || st.Avg300 != 3.75 {
		t.Errorf("средние = %+v, want 1.25/2.5/3.75", st)
	}

	// Файл с full-строкой (ядро отдаёт её у памяти/IO — парсер CPU-файла
	// обязан пропускать).
	st, err = parsePSI("full avg10=0.00 avg60=0.00 avg300=0.00 total=0\n" +
		"some avg10=91.02 avg60=92.10 avg300=93.45 total=16815000000\n")
	if err != nil || st.Avg300 != 93.45 {
		t.Errorf("full+some: (%+v, %v), want some avg300=93.45", st, err)
	}

	// Пустой файл / только full / мусор — строки some нет.
	for _, bad := range []string{"", "full avg10=0 avg60=0 avg300=0 total=0\n", "garbage\n"} {
		if _, err := parsePSI(bad); err == nil || !strings.Contains(err.Error(), "some") {
			t.Errorf("%q: ошибка = %v, want «строка some отсутствует»", bad, err)
		}
	}

	// Битые значения: не число, отрицательное, >100, пропавшее поле —
	// формат фиксирован ядром, кривой файл не молчать.
	for _, bad := range []string{
		"some avg10=x avg60=1 avg300=1 total=0\n",
		"some avg10=-1 avg60=1 avg300=1 total=0\n",
		"some avg10=101 avg60=1 avg300=1 total=0\n",
		"some avg60=1 avg300=1 total=0\n",
	} {
		if _, err := parsePSI(bad); err == nil {
			t.Errorf("%q: ошибки нет, want ошибка формата", bad)
		}
	}
}

// TestOSPISource — чтение по заданному пути; отсутствие файла — errNoPSI
// (тихий класс деградации алерта), прочие ошибки — как есть; отмена ctx —
// до обращения к файлу.
func TestOSPISource(t *testing.T) {
	dir := t.TempDir()
	file := filepath.Join(dir, "cpu")
	if err := os.WriteFile(file, []byte("some avg10=4.00 avg60=4.00 avg300=4.00 total=6000000\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	st, err := (OSPSI{Path: file}).PSI(context.Background())
	if err != nil || st.Avg300 != 4 {
		t.Errorf("PSI по файлу: (%+v, %v), want avg300=4", st, err)
	}

	// Нет файла — ядро без PSI: тихий класс (алерт деградирует без Warn).
	_, err = (OSPSI{Path: filepath.Join(dir, "нет")}).PSI(context.Background())
	if !errors.Is(err, errNoPSI) {
		t.Errorf("нет файла: ошибка = %v, want errNoPSI", err)
	}

	// Каталог вместо файла — прочая ошибка (не errNoPSI).
	_, err = (OSPSI{Path: dir}).PSI(context.Background())
	if err == nil || errors.Is(err, errNoPSI) {
		t.Errorf("каталог вместо файла: ошибка = %v, want обычная ошибка чтения", err)
	}

	// Отмена ctx — до обращения к файлу.
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := (OSPSI{Path: file}).PSI(ctx); !errors.Is(err, context.Canceled) {
		t.Errorf("отмена ctx: ошибка = %v, want context.Canceled", err)
	}
}

// TestOSPSIDefaultPath — пустой Path читает боевой /proc/pressure/cpu:
// на CI-хосте с PSI в ядре ответ валиден (0–100, значения не фиксируются —
// давление хоста меняется между запусками), без PSI — errNoPSI; проверяется
// договорённость дефолта, не значения.
func TestOSPSIDefaultPath(t *testing.T) {
	st, err := (OSPSI{}).PSI(context.Background())
	if err != nil {
		if !errors.Is(err, errNoPSI) {
			t.Errorf("дефолтный путь: ошибка = %v, want errNoPSI или валидный снимок", err)
		}
		return
	}
	if st.Avg10 < 0 || st.Avg10 > 100 || st.Avg300 < 0 || st.Avg300 > 100 {
		t.Errorf("снимок вне 0–100: %+v", st)
	}
}

// TestPSIMetrics — обёртка зеркалит средние в crawler_psi_cpu_some_percent
// {kind=avg10/60/300}, ошибка источника — чистый транзит ошибки, gauge
// держит последнее значение (hold-last).
func TestPSIMetrics(t *testing.T) {
	reg := metrics.New("monitor")
	src := &fakePSI{psi: PSIStat{Avg10: 10, Avg60: 20, Avg300: 30}}
	m := NewPSIMetrics(reg, src)

	st, err := m.PSI(context.Background())
	if err != nil || st.Avg300 != 30 {
		t.Fatalf("PSI: (%+v, %v), want транзит снимка", st, err)
	}
	for kind, want := range map[string]float64{"avg10": 10, "avg60": 20, "avg300": 30} {
		v, ok := gaugeLabels(t, reg, "crawler_psi_cpu_some_percent", map[string]string{"kind": kind})
		if !ok || v != want {
			t.Errorf("crawler_psi_cpu_some_percent{kind=%s} = (%v, %v), want %v", kind, v, ok, want)
		}
	}

	// Ошибка источника: метрики не трогаются, ошибка возвращается.
	src.err = errors.New("отказ")
	if _, err := m.PSI(context.Background()); err == nil {
		t.Error("ошибка источника проглочена")
	}
	v, ok := gaugeLabels(t, reg, "crawler_psi_cpu_some_percent", map[string]string{"kind": "avg300"})
	if !ok || v != 30 {
		t.Errorf("после ошибки avg300 = (%v, %v), want 30 (hold-last)", v, ok)
	}

	// nil-реестр — чистый транзит.
	src.err = nil
	plain := NewPSIMetrics(nil, src)
	if st, err := plain.PSI(context.Background()); err != nil || st.Avg10 != 10 {
		t.Errorf("транзит без реестра = (%+v, %v), want источник как есть", st, err)
	}
}
