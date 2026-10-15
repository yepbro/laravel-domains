// Package rules — загрузка и применение правил поиска Laravel (ТЗ 5.8 основного ТЗ).
//
// Правила лежат в YAML-файлах каталога rules/ и перезагружаются по SIGHUP.
// Расчёт score — noisy-OR: 1 − Π(1 − wᵢ); версия — пересечение диапазонов
// сработавших правил.
package rules

import (
	"crypto/sha256"
	"fmt"
	"os"
	"path/filepath"
	"sort"
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

// Set — загруженный набор правил.
type Set struct {
	Rules   []Rule
	Version string // первые 16 символов sha256 набора
}

type rulesFile struct {
	Rules []Rule `yaml:"rules"`
}

// LoadDir загружает и валидирует все *.yaml/*.yml из каталога.
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
	sort.Strings(names)
	if len(names) == 0 {
		return nil, fmt.Errorf("rules: в каталоге %s нет yaml-файлов", dir)
	}

	set := &Set{}
	var raw []byte
	for _, name := range names {
		data, err := os.ReadFile(filepath.Join(dir, name))
		if err != nil {
			return nil, fmt.Errorf("rules: %w", err)
		}
		raw = append(raw, data...)
		var rf rulesFile
		if err := yaml.Unmarshal(data, &rf); err != nil {
			return nil, fmt.Errorf("rules: %s: %w", name, err)
		}
		set.Rules = append(set.Rules, rf.Rules...)
	}

	sum := sha256.Sum256(raw)
	set.Version = fmt.Sprintf("%x", sum[:8])

	if err := set.validate(); err != nil {
		return nil, err
	}
	return set, nil
}

func (s *Set) validate() error {
	seen := map[string]bool{}
	for _, r := range s.Rules {
		switch {
		case r.ID == "":
			return fmt.Errorf("rules: правило без id: %+v", r)
		case seen[r.ID]:
			return fmt.Errorf("rules: повтор id %q", r.ID)
		case r.Weight <= 0 || r.Weight > 1:
			return fmt.Errorf("rules: %s: weight должен быть в (0, 1], задан %v", r.ID, r.Weight)
		case r.Match == "" && r.Regex == "":
			return fmt.Errorf("rules: %s: нужен match или regex", r.ID)
		}
		seen[r.ID] = true
	}
	return nil
}
