// Источник давления CPU по PSI (Pressure Stall Information, ядро ≥ 4.20 с
// CONFIG_PSI) для алерта psi_cpu (T-162, ТЗ 5.9): /proc/pressure/cpu на
// хосте — то же ядро, что читал сэмплер замеров §12 (some 91–95 % в
// эпизодах голода CPU). PSI some — доля времени, в которую хотя бы одна
// задача ждала CPU: прямая метрика голода планировщика, которую load
// average не различает (iowait и высокая утилизация без очереди).
//
// Контракт деградации — как у SMART NVMe (alerts.go): ядра/VPS без PSI
// (/proc/pressure отсутствует — CONFIG_PSI=y или параметр psi=1 не всякий
// хост включает) — ожидаемое окружение, а не авария: errNoPSI, алерт
// тихо «не измеряется» без Warn и без ложного срабатывания. В Docker
// /proc — хостовый псевдофс, в dev-контейнере значения те же, что на
// хосте CI: юнит-тесты источника подменяют путь временным файлом.
package monitor

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"strconv"
	"strings"
)

// errNoPSI — ядро без PSI: /proc/pressure отсутствует (не собран
// CONFIG_PSI или выключен параметром psi). Ожидаемая деградация части
// окружений, а не авария — алерт psi_cpu по ней тихо «не измеряется»
// (стиль errNoSystemd у событий юнитов, T-145).
var errNoPSI = errors.New("PSI не доступен (ядро без CONFIG_PSI или psi=0)")

// PSIStat — давление CPU по строке some файла /proc/pressure/cpu:
// средняя доля времени (%) задержки за 10 с / 60 с / 300 с.
type PSIStat struct {
	// Avg10, Avg60, Avg300 — some avg10/avg60/avg300, % (0–100).
	Avg10  float64
	Avg60  float64
	Avg300 float64
}

// PSISource — источник давления CPU; интерфейс для подмены в юнит-тестах
// (реальное чтение — файл /proc, значений в тестовом контейнере не
// хватает). Стиль WearSource/UnitEventsSource.
type PSISource interface {
	// PSI возвращает снимок some. Ошибка = «не измеряется» с причиной.
	PSI(ctx context.Context) (PSIStat, error)
}

// OSPSI — PSISource поверх /proc/pressure/cpu.
type OSPSI struct {
	// Path — путь файла давления CPU ("" — "/proc/pressure/cpu"); тесты
	// подменяют временным файлом (стиль SysFS/DevFS у OSWear).
	Path string
}

// compile-time: OSPSI — PSISource.
var _ PSISource = OSPSI{}

// PSI читает и разбирает файл давления CPU.
func (s OSPSI) PSI(ctx context.Context) (PSIStat, error) {
	if err := ctx.Err(); err != nil {
		return PSIStat{}, err
	}
	path := s.Path
	// Дефолт живёт только на боевом хосте: юнит-тесты всегда задают Path
	// файлом, чтение настоящего /proc негерметично (давление хоста CI
	// меняется между запусками).
	// mutator-disable-next-line branch/if
	if path == "" {
		// mutator-disable-next-line statement/remove
		path = "/proc/pressure/cpu"
	}
	b, err := os.ReadFile(path)
	if errors.Is(err, fs.ErrNotExist) {
		return PSIStat{}, fmt.Errorf("monitor: PSI: %w", errNoPSI)
	}
	if err != nil {
		return PSIStat{}, fmt.Errorf("monitor: PSI: %w", err)
	}
	return parsePSI(string(b))
}

// parsePSI разбирает файл PSI: строкa «some avg10=0.00 avg60=0.00
// avg300=0.00 total=0» (у CPU строки full нет — задачи не ждут друг друга).
// Строка full и поле total (микросекунды, для алерта не нужны)
// пропускаются. Отсутствие some или нечисловое/вне 0–100 значение —
// ошибка: формат фиксирован ядром, кривой файл — не молчать (стиль
// parseMemoryEvents). Выделена чистой функцией для табличного теста.
func parsePSI(data string) (PSIStat, error) {
	return parsePSIKind(data, "some")
}

// parsePSIKind — parsePSI с заданным видом строки PSI (some/full): полная
// строка давления памяти /proc/pressure/memory содержит обе — some ждёт
// «хотя бы одна задача», full — «все задачи» (отчёт нагрузки T-173).
func parsePSIKind(data, kind string) (PSIStat, error) {
	for _, line := range strings.Split(data, "\n") {
		prefix, rest, ok := strings.Cut(strings.TrimSpace(line), " ")
		if !ok || prefix != kind {
			continue
		}
		fields := make(map[string]string)
		for _, f := range strings.Fields(rest) {
			if k, v, ok := strings.Cut(f, "="); ok {
				fields[k] = v
			}
		}
		var st PSIStat
		for _, f := range []struct {
			key string
			val *float64
		}{
			{"avg10", &st.Avg10},
			{"avg60", &st.Avg60},
			{"avg300", &st.Avg300},
		} {
			v, ok := fields[f.key]
			if !ok {
				return PSIStat{}, fmt.Errorf("monitor: PSI: поле %s отсутствует", f.key)
			}
			n, err := strconv.ParseFloat(v, 64)
			if err != nil || n < 0 || n > 100 {
				return PSIStat{}, fmt.Errorf("monitor: PSI: %s=%q не доля процента", f.key, v)
			}
			*f.val = n
		}
		return st, nil
	}
	return PSIStat{}, fmt.Errorf("monitor: PSI: строка %s отсутствует", kind)
}
