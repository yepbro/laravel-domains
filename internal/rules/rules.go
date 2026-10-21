// Package rules — загрузка и применение правил поиска Laravel (ТЗ 5.8 основного ТЗ).
//
// Правила лежат в YAML-файлах каталога analyzer.rules_dir и перезагружаются
// по SIGHUP без перезапуска (Reloader). Расчёт score — noisy-OR:
// 1 − Π(1 − wᵢ); версия — пересечение диапазонов сработавших правил.
//
// rules_version — 32-битный FNV-1a хэш канонического представления набора,
// приведённый к знаковому диапазону под колонку integer таблиц
// laravel_findings/laravel_domains (миграция 000001): одинаковым наборам —
// одинаковый хэш независимо от разбиения на файлы и порядка правил.
package rules

import (
	"bytes"
	"errors"
	"fmt"
	"hash/fnv"
	"io"
	"math"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"

	"gopkg.in/yaml.v3"
)

// Допустимые цели правила.
const (
	TargetCookieName  = "cookie_name"
	TargetCookieValue = "cookie_value"
	TargetHeaderName  = "header_name"
	TargetHeaderValue = "header_value"
	TargetHTML        = "html"
	TargetMeta        = "meta"
)

// targets — множество допустимых значений target (для валидации и сообщений).
var targets = map[string]bool{
	TargetCookieName:  true,
	TargetCookieValue: true,
	TargetHeaderName:  true,
	TargetHeaderValue: true,
	TargetHTML:        true,
	TargetMeta:        true,
}

// regexOnlyTargets — цели, где соответствие ищется только регулярным
// выражением (ТЗ 5.8: «регулярное выражение по HTML»; движок match по
// ним не применяет). Для остальных целей match и regex равноправны:
// движок применяет оба режима (engine.go matchRule), включая точное
// совпадение значений cookie/header — docs/analyzer.md «Цели и режимы
// матчинга».
var regexOnlyTargets = map[string]bool{
	TargetHTML: true,
	TargetMeta: true,
}

// Rule — одно правило поиска признака Laravel.
type Rule struct {
	ID     string  `yaml:"id"`
	Desc   string  `yaml:"description"`
	Target string  `yaml:"target"`
	Match  string  `yaml:"match"`  // точное совпадение (для имён cookie/заголовков)
	Regex  string  `yaml:"regex"`  // регулярное выражение
	Weight float64 `yaml:"weight"` // 0..1
	VerMin string  `yaml:"ver_min"`
	VerMax string  `yaml:"ver_max"`
}

// Set — загруженный набор правил. Неизменяем после загрузки: потребители
// держат снапшот, Reloader заменяет его целиком при перезагрузке.
type Set struct {
	Rules []Rule
	// Version — rules_version (ТЗ 5.8): FNV-32a канонического представления
	// набора, int32 под колонку integer. Отражает поведение детекции:
	// правка описаний и комментариев версию не меняет.
	Version int32
}

type rulesFile struct {
	Rules []Rule `yaml:"rules"`
}

// LoadDir загружает и валидирует все *.yaml/*.yml из каталога (в порядке
// имён файлов — детерминированно). Повторы id, некорректные target/regex/
// версии и веса вне (0, 1] — ошибка загрузки: набор применяется целиком
// или не применяется вовсе (ошибка перезагрузки не трогает прежний набор).
func LoadDir(dir string) (*Set, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, fmt.Errorf("rules: %w", err)
	}
	var names []string
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		if ext := strings.ToLower(filepath.Ext(e.Name())); ext == ".yaml" || ext == ".yml" {
			names = append(names, e.Name())
		}
	}
	// Эквивалентный мутант (T-M12): os.ReadDir возвращает записи,
	// отсортированные по имени (контракт stdlib), поэтому sort.Strings
	// по уже отсортированному names ничего не меняет; сортировка остаётся
	// защитой на случай смены способа чтения каталога.
	// mutator-disable-next-line statement/remove
	sort.Strings(names)
	if len(names) == 0 {
		return nil, fmt.Errorf("rules: в каталоге %s нет yaml-файлов", dir)
	}

	set := &Set{}
	for _, name := range names {
		data, err := os.ReadFile(filepath.Join(dir, name))
		if err != nil {
			return nil, fmt.Errorf("rules: %w", err)
		}
		// Строгий разбор: опечатка в имени поля — ошибка, а не молчаливо
		// отключённое правило (как и в конфигурации сервисов). Файл из
		// одних комментариев пропускается (первый Decode → io.EOF);
		// многоокументный файл (---) читается целиком: правила второго
		// документа не должны теряться молча (документы равноправны
		// файлам — канонический порядок и хэш от разбиения не зависят).
		dec := yaml.NewDecoder(bytes.NewReader(data))
		dec.KnownFields(true)
		for {
			var rf rulesFile
			err := dec.Decode(&rf)
			if errors.Is(err, io.EOF) {
				break
			}
			if err != nil {
				return nil, fmt.Errorf("rules: %s: %w", name, err)
			}
			for i := range rf.Rules {
				rf.Rules[i].ID = strings.TrimSpace(rf.Rules[i].ID)
			}
			set.Rules = append(set.Rules, rf.Rules...)
		}
	}

	// Канонический порядок правил — по id: состав набора не зависит от
	// разбиения на файлы и порядка внутри них. Компаратор строгий («<»):
	// при равных id порядок равных стабилен (insertion sort для малых n),
	// поэтому повтор идентифицируется по второй записи — см.
	// TestLoadDirDuplicateIDReported.
	sort.Slice(set.Rules, func(i, j int) bool { return set.Rules[i].ID < set.Rules[j].ID })

	if err := set.validate(); err != nil {
		return nil, err
	}
	set.Version = canonicalHash(set.Rules)
	return set, nil
}

// validate проверяет набор целиком: непустоту, уникальность id, типы target,
// применимость match/regex к цели, компилируемость и лимит длины regex
// (MaxPatternBytes — граница входа едина для загрузчика и движка),
// отбраковку паттернов, матчащих пустую строку, диапазон веса
// и корректность границ версий.
func (s *Set) validate() error {
	if len(s.Rules) == 0 {
		return fmt.Errorf("rules: набор правил пуст (нужен хотя бы один файл с rules:)")
	}
	seen := map[string]bool{}
	for _, r := range s.Rules {
		where := fmt.Sprintf("rules: %s", r.ID)
		switch {
		case r.ID == "":
			return fmt.Errorf("rules: правило без id: %+v", r)
		case seen[r.ID]:
			return fmt.Errorf("rules: повтор id %q", r.ID)
		case !targets[r.Target]:
			return fmt.Errorf("%s: неизвестный target %q (допустимы %s)",
				where, r.Target, strings.Join(targetNames(), ", "))
		case r.Match != "" && r.Regex != "":
			return fmt.Errorf("%s: match и regex одновременно — задайте одно", where)
		case r.Match == "" && r.Regex == "":
			return fmt.Errorf("%s: нужен match или regex", where)
		case regexOnlyTargets[r.Target] && r.Regex == "":
			return fmt.Errorf("%s: для target %s требуется regex", where, r.Target)
		case math.IsNaN(r.Weight) || r.Weight <= 0 || r.Weight > 1:
			return fmt.Errorf("%s: weight должен быть числом в (0, 1], задан %v", where, r.Weight)
		}
		if r.Regex != "" {
			if max := DefaultLimits().MaxPatternBytes; len(r.Regex) > max {
				return fmt.Errorf("%s: regex длиннее лимита %d байт (MaxPatternBytes), задан %d байт",
					where, max, len(r.Regex))
			}
			re, err := regexp.Compile(r.Regex)
			if err != nil {
				return fmt.Errorf("%s: некорректный regex: %w", where, err)
			}
			// Паттерн вида ^ / x* / (?:) срабатывал бы на любом входе с
			// пустым свидетельством — «правило-всегда-истина», незаметное
			// по свидетельству (AN-07). Отбраковываем при загрузке.
			if re.MatchString("") {
				return fmt.Errorf("%s: regex матчит пустую строку — правило срабатывало бы на любом входе", where)
			}
		}
		if r.VerMin != "" && parseVersion(r.VerMin) == nil {
			return fmt.Errorf("%s: некорректный ver_min %q", where, r.VerMin)
		}
		if r.VerMax != "" && parseVersion(r.VerMax) == nil {
			return fmt.Errorf("%s: некорректный ver_max %q", where, r.VerMax)
		}
		if vmin, vmax := parseVersion(r.VerMin), parseVersion(r.VerMax); vmin != nil && vmax != nil && vmax.less(vmin) {
			return fmt.Errorf("%s: ver_min %q больше ver_max %q", where, r.VerMin, r.VerMax)
		}
		seen[r.ID] = true
	}
	return nil
}

// targetNames — отсортированный список допустимых target для сообщений
// об ошибке.
func targetNames() []string {
	names := make([]string, 0, len(targets))
	for t := range targets {
		names = append(names, t)
	}
	sort.Strings(names)
	return names
}

// canonicalHash — rules_version: FNV-32a по каноническому представлению
// набора, приведённый к знаковому int32 (колонка integer). Коллизии 32 бит
// на практике незначимы: версия различает последовательные редакции одного
// набора, а не все возможные наборы мира.
func canonicalHash(rules []Rule) int32 {
	var b bytes.Buffer
	// Каждый элемент записи — с префиксом длины: поля могут содержать любые
	// байты (YAML-строки), разделители без длины допускали бы неоднозначное
	// представление разных наборов.
	writeField := func(s string) {
		b.WriteString(strconv.Itoa(len(s)))
		b.WriteByte(':')
		b.WriteString(s)
	}
	for _, r := range rules {
		// description сознательно не входит: версия отражает поведение
		// детекции (target/match/regex/weight/версии Laravel), правка
		// описания результатов анализа не меняет — переанализ не нужен.
		writeField(r.ID)
		writeField(r.Target)
		writeField(r.Match)
		writeField(r.Regex)
		// Каноническая запись вещественного: shortest round-trip, stable.
		// Эквивалентный мутант (T-M12): prec −2 ведёт себя как −1 — в
		// strconv «shortest»-режим активируется при prec < 0 (проверено
		// на весах 1e−5…1: строки совпадают). Гасим только incrementer;
		// decrementer (prec 0 — одна значащая цифра: 0.26 и 0.27 обе
		// «0.3») убит тестом TestCanonicalHashWeightPrecision.
		// mutator-disable-next-line numbers/incrementer
		writeField(strconv.FormatFloat(r.Weight, 'g', -1, 64))
		writeField(r.VerMin)
		writeField(r.VerMax)
		// Эквивалентный мутант (T-M12): разделитель правил избыточен для
		// контракта версии — кодирование «длина:значение» саморазделимо:
		// разбор слева направо детерминирован (цифры до первого «:» —
		// длина, за ними ровно N байт значения), правило — всегда 7 полей,
		// поэтому конкатенация без «;» декодируется единственным образом.
		// Контракт rules_version — только равенство одинаковых наборов и
		// неравенство разных (переанализ по смене); абсолютное значение
		// нигде не пиннуто и не интерпретируется, его изменение ненаблюдаемо.
		// mutator-disable-next-line statement/remove
		b.WriteByte(';')
	}
	h := fnv.New32a()
	_, _ = h.Write(b.Bytes())
	return int32(h.Sum32())
}
