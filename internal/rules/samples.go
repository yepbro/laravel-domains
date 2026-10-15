package rules

import (
	"encoding/json"
	"fmt"
	"net/textproto"
	"os"
	"path/filepath"
	"sort"

	"gopkg.in/yaml.v3"
)

// Sample — образец страницы из testdata/laravel-samples/<id>/ (ТЗ 4.4.2):
//
//	index.html   — итоговый HTML/DOM в том виде, как его сохраняет сервис;
//	headers.json — заголовки ответа, объект имя -> массив значений;
//	cookies.json — массив {name, value, ...};
//	meta.yaml    — is_laravel, ожидаемый диапазон версий, примечание.
type Sample struct {
	ID      string
	HTML    []byte
	Headers map[string][]string
	Cookies []Cookie

	Expect SampleExpect
}

// SampleExpect — ожидания из meta.yaml (пустые поля — не проверяются).
type SampleExpect struct {
	IsLaravel *bool
	VerMin    string
	VerMax    string
	Note      string
	Source    string
}

type cookieJSON struct {
	Name  string `json:"name" yaml:"name"`
	Value string `json:"value" yaml:"value"`
}

// LoadSamples читает все подкаталоги с meta.yaml.
func LoadSamples(dir string) ([]Sample, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, fmt.Errorf("samples: %w", err)
	}
	var ids []string
	for _, e := range entries {
		if e.IsDir() {
			ids = append(ids, e.Name())
		}
	}
	sort.Strings(ids)

	out := make([]Sample, 0, len(ids))
	for _, id := range ids {
		s, err := loadSample(filepath.Join(dir, id), id)
		if err != nil {
			return nil, err
		}
		out = append(out, s)
	}
	return out, nil
}

func loadSample(dir, id string) (Sample, error) {
	s := Sample{ID: id, Headers: map[string][]string{}}

	if raw, err := os.ReadFile(filepath.Join(dir, "index.html")); err == nil {
		s.HTML = raw
	} else {
		return s, fmt.Errorf("samples/%s: нет index.html", id)
	}

	if raw, err := os.ReadFile(filepath.Join(dir, "headers.json")); err == nil {
		var h map[string][]string
		if err := json.Unmarshal(raw, &h); err != nil {
			return s, fmt.Errorf("samples/%s/headers.json: %w", id, err)
		}
		for k, v := range h {
			s.Headers[textproto.CanonicalMIMEHeaderKey(k)] = v
		}
	}

	if raw, err := os.ReadFile(filepath.Join(dir, "cookies.json")); err == nil {
		var cs []cookieJSON
		if err := json.Unmarshal(raw, &cs); err != nil {
			return s, fmt.Errorf("samples/%s/cookies.json: %w", id, err)
		}
		for _, c := range cs {
			s.Cookies = append(s.Cookies, Cookie(c))
		}
	}

	if raw, err := os.ReadFile(filepath.Join(dir, "meta.yaml")); err == nil {
		var m struct {
			IsLaravel *bool  `yaml:"is_laravel"`
			VerMin    string `yaml:"ver_min"`
			VerMax    string `yaml:"ver_max"`
			Note      string `yaml:"note"`
			Source    string `yaml:"source"`
		}
		if err := yaml.Unmarshal(raw, &m); err != nil {
			return s, fmt.Errorf("samples/%s/meta.yaml: %w", id, err)
		}
		s.Expect = SampleExpect{
			IsLaravel: m.IsLaravel,
			VerMin:    m.VerMin,
			VerMax:    m.VerMax,
			Note:      m.Note,
			Source:    m.Source,
		}
	} else {
		return s, fmt.Errorf("samples/%s: нет meta.yaml", id)
	}

	return s, nil
}

// Page превращает образец в вход движка правил.
func (s Sample) Page() Page {
	return Page{HTML: s.HTML, Cookies: s.Cookies, Headers: s.Headers}
}
