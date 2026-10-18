// ---------------------------------------------------------------- словарь zstd
//
// Команда управления словарём сжатия контента (ТЗ 5.7, ТЗ 13.2: параметры
// сжатия — на усмотрение разработки). Обучение и переобучение — одно и то
// же действие (train): файл словаря перезаписывается атомарно, сервисы
// подхватывают его при следующем старте по пути content.zstd_dictionary.
// Реализована полностью.

package main

import (
	"flag"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"strings"

	"crawler/internal/content"
)

// dictHelp — справка crawlerctl dict --help.
const dictHelp = `crawlerctl dict — управление словарём сжатия zstd (ТЗ 5.7)

Использование:
  crawlerctl dict train <выходной-файл> [--samples ПУТЬ]…

Обучает словарь zstd по HTML-образцам (повторяющиеся шаблоны парковок и
заглушек уходят в словарь и не кодируются в каждой странице) и записывает
его в выходной файл атомарно. Путь --samples — файл *.html/*.htm или
каталог (обход рекурсивный); флаг можно повторять. По умолчанию образцы —
testdata/laravel-samples. Переобучение — повторный запуск команды.

Словарь подключается конфигурацией content.zstd_dictionary; страницы,
сжатые прежним словарём, для чтения требуют прежний словарь.
`

// dictTrainSampleDefault — каталог образцов по умолчанию (как у test-rules):
// в dev-контейнере tools репозиторий смонтирован в /src.
const dictTrainSampleDefault = "testdata/laravel-samples"

func cmdDict(args []string, stdout, stderr io.Writer) error {
	if len(args) == 0 {
		return usagef("укажите действие: crawlerctl dict train")
	}
	action, rest := args[0], args[1:]
	switch action {
	case "train":
		return dictTrain(rest, stdout, stderr)
	case "-h", "--help", "help":
		out(stdout, "%s", dictHelp)
		return flag.ErrHelp
	default:
		return usagef("неизвестное действие %q (допустимы: train)", action)
	}
}

func dictTrain(args []string, stdout, stderr io.Writer) error {
	if len(args) > 0 && args[0] == "help" {
		out(stdout, "%s", dictHelp)
		return flag.ErrHelp
	}
	fs := newFlagSet("dict train")
	var samplePaths []string
	fs.Func("samples", "файл или каталог с HTML-образцами (можно повторять)",
		func(v string) error {
			samplePaths = append(samplePaths, v)
			return nil
		})
	// Стандартный flag останавливает разбор на первом позиционном аргументе,
	// а по справке выходной файл идёт до --samples: флаги (и их значения)
	// переставляются в начало, позиционные — в конец.
	var pass, pos []string
	for i := 0; i < len(args); i++ {
		a := args[i]
		switch {
		case a == "--":
			// Терминатор флагов: остальное — позиционные аргументы.
			pos = append(pos, args[i+1:]...)
			i = len(args)
		case a == "-h" || a == "--help":
			pass = append(pass, a)
		case a == "-samples" || a == "--samples":
			if i+1 >= len(args) {
				return usagef("dict train: флаг --samples требует значение")
			}
			i++
			pass = append(pass, "--samples", args[i])
		case strings.HasPrefix(a, "-samples=") || strings.HasPrefix(a, "--samples="):
			pass = append(pass, a)
		case strings.HasPrefix(a, "-"):
			pass = append(pass, a) // неизвестный флаг — ошибка разбора с именем
		default:
			pos = append(pos, a)
		}
	}
	if handled, err := parseFlags(fs, dictHelp, pass, stdout); handled {
		return err
	}
	if len(pos) != 1 {
		return usagef("укажите выходной файл: crawlerctl dict train <файл> [--samples ПУТЬ]…")
	}
	outPath := pos[0]
	if len(samplePaths) == 0 {
		samplePaths = []string{dictTrainSampleDefault}
	}

	files, err := collectSampleFiles(samplePaths)
	if err != nil {
		return fmt.Errorf("dict train: %w", err)
	}
	if len(files) == 0 {
		return fmt.Errorf("dict train: в %s нет HTML-образцов (*.html, *.htm)",
			strings.Join(samplePaths, ", "))
	}
	samples, read := readSamples(files, content.DefaultTrainCorpusLimit)
	if len(samples) == 0 {
		return fmt.Errorf("dict train: образцы пусты")
	}
	out(stdout, "dict train: файлов-образцов %d из %d найденных, корпус %d байт (лимит обучения %d байт)\n",
		len(samples), len(files), read, content.DefaultTrainCorpusLimit)

	d, err := content.TrainDict(samples, content.TrainOptions{})
	if err != nil {
		return fmt.Errorf("dict train: %w", err)
	}
	out(stdout, "dict train: словарь обучен: %d байт, ID %08x\n", len(d.Raw()), d.ID())

	// Замер на обучающем корпусе — ориентир для оператора (реальный эффект
	// измеряется на новых страницах того же шаблона).
	var plainSum, dictSum, evalBytes int
	for _, s := range samples {
		plainSum += len(content.Compress(s))
		dictSum += len(d.Compress(s))
		evalBytes += len(s)
	}
	out(stdout, "dict train: оценка сжатия корпуса %d байт: без словаря %d, со словарём %d\n",
		evalBytes, plainSum, dictSum)

	if err := d.Save(outPath); err != nil {
		return fmt.Errorf("dict train: %w", err)
	}
	out(stdout, "dict train: словарь записан: %s\n", outPath)
	return nil
}

// collectSampleFiles собирает пути HTML-файлов: каждый путь — файл или
// каталог (рекурсивный обход, только *.html/*.htm). Порядок лексический
// (WalkDir) — обучение детерминировано.
func collectSampleFiles(paths []string) ([]string, error) {
	var files []string
	for _, p := range paths {
		st, err := os.Stat(p)
		if err != nil {
			// Путь образцов указывается в справке, а не в сырой ошибке os:
			// «stat …: no such file» без контекста не говорит, что это
			// именно --samples.
			return nil, fmt.Errorf("образцы %s: %w", p, err)
		}
		if !st.IsDir() {
			if isHTMLFile(p) {
				files = append(files, p)
			}
			continue
		}
		err = filepath.WalkDir(p, func(path string, d fs.DirEntry, err error) error {
			if err != nil {
				return err
			}
			if d.Type().IsRegular() && isHTMLFile(path) {
				files = append(files, path)
			}
			return nil
		})
		if err != nil {
			return nil, err
		}
	}
	return files, nil
}

// isHTMLFile — расширение файла .html/.htm.
func isHTMLFile(path string) bool {
	switch strings.ToLower(filepath.Ext(path)) {
	case ".html", ".htm":
		return true
	}
	return false
}

// readSamples читает файлы по порядку, пока суммарный размер не превысит
// limit: память переобучения ограничена даже на огромном каталоге образцов.
func readSamples(files []string, limit int) ([][]byte, int) {
	samples := make([][]byte, 0, len(files))
	var total int
	for _, f := range files {
		if total >= limit {
			break
		}
		b, err := os.ReadFile(f)
		if err != nil {
			continue // нечитаемый образец пропускаем: обучение не должно падать из-за одного файла
		}
		samples = append(samples, b)
		total += len(b)
	}
	return samples, total
}
