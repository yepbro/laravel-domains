package config

import (
	"fmt"
	"math"
	"regexp"
	"strconv"
	"strings"
	"time"

	"gopkg.in/yaml.v3"
)

// Duration — длительность в конфигурации: строка вида "5s", "30m", "24h"
// (формат time.ParseDuration) и в YAML, и в переменных окружения. Число
// без единицы измерения — ошибка: его значение было бы двусмысленным.
type Duration time.Duration

// D возвращает значение как time.Duration.
func (d Duration) D() time.Duration { return time.Duration(d) }

// String возвращает каноничную форму time.Duration.
func (d Duration) String() string { return time.Duration(d).String() }

// UnmarshalYAML разбирает длительность из YAML-скаляра.
func (d *Duration) UnmarshalYAML(node *yaml.Node) error {
	if node.Kind != yaml.ScalarNode {
		return fmt.Errorf("duration: expected a string like \"30m\", not %s (line %d)", node.Tag, node.Line)
	}
	v, err := ParseDuration(node.Value)
	if err != nil {
		return fmt.Errorf("duration: %w", err)
	}
	*d = Duration(v)
	return nil
}

// ParseDuration разбирает длительность (обёртка над time.ParseDuration
// с понятным сообщением об ошибке).
func ParseDuration(s string) (time.Duration, error) {
	d, err := time.ParseDuration(strings.TrimSpace(s))
	if err != nil {
		return 0, fmt.Errorf("expected a duration like \"5s\", \"30m\", \"24h\", got %q", s)
	}
	return d, nil
}

// ByteSize — размер в конфигурации: количество байт (2097152) или строка
// с единицей ("512MB", "1GiB"). Десятичные единицы (KB, MB, GB) — степени
// 10³, двоичные (KiB, MiB, GiB) — степени 2¹⁰.
type ByteSize int64

// B возвращает значение в байтах.
func (b ByteSize) B() int64 { return int64(b) }

// String возвращает человекочитаемую форму (кратная двоичная единица или байты).
func (b ByteSize) String() string {
	switch {
	case b%(1<<30) == 0:
		return strconv.FormatInt(int64(b)/(1<<30), 10) + "GiB"
	case b%(1<<20) == 0:
		return strconv.FormatInt(int64(b)/(1<<20), 10) + "MiB"
	case b%(1<<10) == 0:
		return strconv.FormatInt(int64(b)/(1<<10), 10) + "KiB"
	default:
		return strconv.FormatInt(int64(b), 10)
	}
}

// UnmarshalYAML разбирает размер из YAML-скаляра: целое число или строка
// с единицей измерения.
func (b *ByteSize) UnmarshalYAML(node *yaml.Node) error {
	if node.Kind != yaml.ScalarNode {
		return fmt.Errorf("size: expected a number or a string like \"512MB\", not %s (line %d)", node.Tag, node.Line)
	}
	v, err := ParseSize(node.Value)
	if err != nil {
		return fmt.Errorf("size: %w", err)
	}
	*b = ByteSize(v)
	return nil
}

var sizeRe = regexp.MustCompile(`^([0-9]+)\s*([a-zA-Z]{0,3})$`)

var sizeUnits = map[string]int64{
	"":    1,
	"b":   1,
	"kb":  1000,
	"mb":  1000 * 1000,
	"gb":  1000 * 1000 * 1000,
	"kib": 1 << 10,
	"mib": 1 << 20,
	"gib": 1 << 30,
}

// ParseSize разбирает размер: "2097152", "512MB", "1GiB" (единица нечувствительна
// к регистру; между числом и единицей допустим пробел).
func ParseSize(s string) (int64, error) {
	m := sizeRe.FindStringSubmatch(strings.TrimSpace(s))
	if m == nil {
		return 0, fmt.Errorf("expected a byte count or a string with a unit (\"512MB\", \"1GiB\"), got %q", s)
	}
	n, err := strconv.ParseInt(m[1], 10, 64)
	if err != nil {
		return 0, fmt.Errorf("value %s is too large", m[1])
	}
	mult, ok := sizeUnits[strings.ToLower(m[2])]
	if !ok {
		return 0, fmt.Errorf("unknown unit %q (allowed: B, KB, MB, GB, KiB, MiB, GiB)", m[2])
	}
	if mult > 1 && n > math.MaxInt64/mult {
		return 0, fmt.Errorf("value %q exceeds the allowed size", s)
	}
	return n * mult, nil
}
