// Package rules — loading and applying the Laravel detection rules (spec 5.8
// of the main spec).
//
// The rules live in YAML files of the analyzer.rules_dir directory and are
// reloaded on SIGHUP without a restart (Reloader). The score is noisy-OR:
// 1 − Π(1 − wᵢ); the version is the intersection of the hit rules' ranges.
//
// rules_version is a 32-bit FNV-1a hash of the canonical representation of
// the set, cast to the signed range to fit the integer column of the
// laravel_findings/laravel_domains tables (migration 000001): identical sets
// hash identically regardless of the file splitting and the rule order.
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

// Allowed rule targets.
const (
	TargetCookieName  = "cookie_name"
	TargetCookieValue = "cookie_value"
	TargetHeaderName  = "header_name"
	TargetHeaderValue = "header_value"
	TargetHTML        = "html"
	TargetMeta        = "meta"
)

// targets — the set of allowed target values (for validation and messages).
var targets = map[string]bool{
	TargetCookieName:  true,
	TargetCookieValue: true,
	TargetHeaderName:  true,
	TargetHeaderValue: true,
	TargetHTML:        true,
	TargetMeta:        true,
}

// regexOnlyTargets — the targets where the match is looked up by a regular
// expression only (spec 5.8: "a regular expression over HTML"; the engine
// does not apply match to them). For the other targets match and regex are
// equal: the engine applies both modes (engine.go matchRule), including
// exact cookie/header value matches — docs/analyzer.md "Targets and
// matching modes".
var regexOnlyTargets = map[string]bool{
	TargetHTML: true,
	TargetMeta: true,
}

// Rule — a single rule searching for a Laravel sign.
type Rule struct {
	ID     string  `yaml:"id"`
	Desc   string  `yaml:"description"`
	Target string  `yaml:"target"`
	Match  string  `yaml:"match"`  // exact match (for cookie/header names)
	Regex  string  `yaml:"regex"`  // regular expression
	Weight float64 `yaml:"weight"` // 0..1
	VerMin string  `yaml:"ver_min"`
	VerMax string  `yaml:"ver_max"`
}

// Set — a loaded rule set. Immutable after loading: consumers hold the
// snapshot, the Reloader replaces it wholesale on reload.
type Set struct {
	Rules []Rule
	// Version — rules_version (spec 5.8): FNV-32a of the canonical
	// representation of the set, int32 for the integer column. It reflects
	// the detection behavior: editing descriptions and comments does not
	// change the version.
	Version int32
}

type rulesFile struct {
	Rules []Rule `yaml:"rules"`
}

// LoadDir loads and validates all *.yaml/*.yml from the directory (in file
// name order — deterministic). Duplicate ids, invalid target/regex/versions
// and weights outside (0, 1] are a load error: the set is applied wholesale
// or not applied at all (a reload error does not touch the previous set).
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
	// An equivalent mutant (T-M12): os.ReadDir returns entries sorted by
	// name (a stdlib contract), so sort.Strings over the already sorted
	// names changes nothing; the sort remains a guard against a change in
	// the way the directory is read.
	// mutator-disable-next-line statement/remove
	sort.Strings(names)
	if len(names) == 0 {
		return nil, fmt.Errorf("rules: no yaml files in directory %s", dir)
	}

	set := &Set{}
	for _, name := range names {
		data, err := os.ReadFile(filepath.Join(dir, name))
		if err != nil {
			return nil, fmt.Errorf("rules: %w", err)
		}
		// Strict parsing: a typo in a field name is an error, not a
		// silently disabled rule (same as in service configuration). A file
		// of comments only is skipped (the first Decode → io.EOF); a
		// multi-document file (---) is read whole: the rules of the second
		// document must not be lost silently (documents are equal to files —
		// the canonical order and the hash do not depend on the splitting).
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

	// The canonical rule order is by id: the set contents do not depend on
	// the file splitting or the order within the files. The comparator is
	// strict ("<"): with equal ids the order of equals is stable (insertion
	// sort for small n), so a duplicate is identified by the second entry —
	// see TestLoadDirDuplicateIDReported.
	sort.Slice(set.Rules, func(i, j int) bool { return set.Rules[i].ID < set.Rules[j].ID })

	if err := set.validate(); err != nil {
		return nil, err
	}
	set.Version = canonicalHash(set.Rules)
	return set, nil
}

// validate checks the set as a whole: non-emptiness, id uniqueness, target
// types, match/regex applicability to the target, compilability and the
// regex length limit (MaxPatternBytes — the input bound is the same for the
// loader and the engine), rejection of patterns matching the empty string,
// the weight range and the validity of the version bounds.
func (s *Set) validate() error {
	if len(s.Rules) == 0 {
		return fmt.Errorf("rules: rule set is empty (at least one file with rules: is required)")
	}
	seen := map[string]bool{}
	for _, r := range s.Rules {
		where := fmt.Sprintf("rules: %s", r.ID)
		switch {
		case r.ID == "":
			return fmt.Errorf("rules: rule without id: %+v", r)
		case seen[r.ID]:
			return fmt.Errorf("rules: duplicate id %q", r.ID)
		case !targets[r.Target]:
			return fmt.Errorf("%s: unknown target %q (allowed: %s)",
				where, r.Target, strings.Join(targetNames(), ", "))
		case r.Match != "" && r.Regex != "":
			return fmt.Errorf("%s: match and regex together — set only one", where)
		case r.Match == "" && r.Regex == "":
			return fmt.Errorf("%s: match or regex is required", where)
		case regexOnlyTargets[r.Target] && r.Regex == "":
			return fmt.Errorf("%s: target %s requires regex", where, r.Target)
		case math.IsNaN(r.Weight) || r.Weight <= 0 || r.Weight > 1:
			return fmt.Errorf("%s: weight must be a number in (0, 1], got %v", where, r.Weight)
		}
		if r.Regex != "" {
			if max := DefaultLimits().MaxPatternBytes; len(r.Regex) > max {
				return fmt.Errorf("%s: regex longer than the %d byte limit (MaxPatternBytes), got %d bytes",
					where, max, len(r.Regex))
			}
			re, err := regexp.Compile(r.Regex)
			if err != nil {
				return fmt.Errorf("%s: invalid regex: %w", where, err)
			}
			// A pattern like ^ / x* / (?:) would hit on any input with an
			// empty evidence — an "always-true rule" invisible in the
			// evidence (AN-07). Rejected at load time.
			if re.MatchString("") {
				return fmt.Errorf("%s: regex matches the empty string — the rule would hit on any input", where)
			}
		}
		if r.VerMin != "" && parseVersion(r.VerMin) == nil {
			return fmt.Errorf("%s: invalid ver_min %q", where, r.VerMin)
		}
		if r.VerMax != "" && parseVersion(r.VerMax) == nil {
			return fmt.Errorf("%s: invalid ver_max %q", where, r.VerMax)
		}
		if vmin, vmax := parseVersion(r.VerMin), parseVersion(r.VerMax); vmin != nil && vmax != nil && vmax.less(vmin) {
			return fmt.Errorf("%s: ver_min %q is greater than ver_max %q", where, r.VerMin, r.VerMax)
		}
		seen[r.ID] = true
	}
	return nil
}

// targetNames — the sorted list of allowed targets for error messages.
func targetNames() []string {
	names := make([]string, 0, len(targets))
	for t := range targets {
		names = append(names, t)
	}
	sort.Strings(names)
	return names
}

// canonicalHash — rules_version: FNV-32a over the canonical representation
// of the set, cast to a signed int32 (the integer column). 32-bit collisions
// are negligible in practice: the version distinguishes consecutive
// revisions of one set, not every possible set in the world.
func canonicalHash(rules []Rule) int32 {
	var b bytes.Buffer
	// Every record element carries a length prefix: fields may contain
	// arbitrary bytes (YAML strings); separators without a length would
	// allow ambiguous representations of different sets.
	writeField := func(s string) {
		b.WriteString(strconv.Itoa(len(s)))
		b.WriteByte(':')
		b.WriteString(s)
	}
	for _, r := range rules {
		// description is deliberately excluded: the version reflects the
		// detection behavior (target/match/regex/weight/Laravel versions);
		// editing the description does not change the analysis results —
		// no re-analysis is needed.
		writeField(r.ID)
		writeField(r.Target)
		writeField(r.Match)
		writeField(r.Regex)
		// Canonical float formatting: shortest round-trip, stable.
		// An equivalent mutant (T-M12): prec −2 behaves like −1 — in
		// strconv the "shortest" mode activates at prec < 0 (verified on
		// weights 1e−5…1: the strings match). Only the incrementer is
		// suppressed; the decrementer (prec 0 — one significant digit:
		// 0.26 and 0.27 both "0.3") is killed by
		// TestCanonicalHashWeightPrecision.
		// mutator-disable-next-line numbers/incrementer
		writeField(strconv.FormatFloat(r.Weight, 'g', -1, 64))
		writeField(r.VerMin)
		writeField(r.VerMax)
		// An equivalent mutant (T-M12): the rule separator is redundant for
		// the version contract — the "length:value" encoding is
		// self-delimiting: left-to-right parsing is deterministic (digits
		// up to the first ":" are the length, followed by exactly N bytes
		// of the value), a rule is always 7 fields, so a concatenation
		// without ";" decodes uniquely.
		// The rules_version contract is only the equality of identical sets
		// and the inequality of different ones (re-analysis on change); the
		// absolute value is pinned nowhere and never interpreted — its
		// change is unobservable.
		// mutator-disable-next-line statement/remove
		b.WriteByte(';')
	}
	h := fnv.New32a()
	_, _ = h.Write(b.Bytes())
	return int32(h.Sum32())
}
