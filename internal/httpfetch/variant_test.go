// Юнит-тесты логики вариантов http/https (T-054; ТЗ 5.5 п. 5, §7).
// Сценарии §11.1 («http → https, http ведёт на другой сайт») и список
// T-059: bare→www — территория алиасов T-055 (разные субдомены), здесь
// проверяются варианты одного субдомена по обоим протоколам.

package httpfetch

import (
	"net/url"
	"testing"

	"crawler/internal/content"
	"crawler/internal/model"
)

// variantCheck — доступный итог проверки с итоговым host+path.
func variantCheck(scheme model.Scheme, host, path string, hash *content.Hash) VariantInput {
	return VariantInput{Scheme: scheme, Available: true, Host: host, Path: path, ContentHash: hash}
}

// variantDown — недоступный итог проверки (сетевая ошибка/таймаут/
// лимит редиректов — классификация за T-058, здесь только доступность).
func variantDown(scheme model.Scheme) VariantInput {
	return VariantInput{Scheme: scheme}
}

// variantHash — хэш контента для входа; важно nil-представление
// «контент не сохранён», значение — идентификатор сравнения.
func variantHash(s string) *content.Hash {
	h := content.HashOf([]byte(s))
	return &h
}

// variantRecordsEqual — сравнение без reflect: записи сравнимы целиком.
func variantRecordsEqual(got, want []VariantRecord) bool {
	if len(got) != len(want) {
		return false
	}
	for i := range got {
		if got[i] != want[i] {
			return false
		}
	}
	return true
}

// TestDecideVariants — таблица решений по §5.5 п. 5: оба недоступны,
// один протокол, один сайт (host+путь либо хэш), разные сайты. Каждый
// кейс прогоняется дважды с переставленными аргументами — пара
// различается полем Scheme, а не позицией.
func TestDecideVariants(t *testing.T) {
	cases := []struct {
		name  string
		https VariantInput
		http  VariantInput
		want  []VariantRecord
	}{
		{
			name:  "оба недоступны — записей нет (статус домена решает T-058)",
			https: variantDown(model.SchemeHTTPS),
			http:  variantDown(model.SchemeHTTP),
			want:  nil,
		},
		{
			name:  "работает только https — одна запись с его scheme",
			https: variantCheck(model.SchemeHTTPS, "example.com", "/", nil),
			http:  variantDown(model.SchemeHTTP),
			want:  []VariantRecord{{Variant: model.VariantMain, Scheme: model.SchemeHTTPS}},
		},
		{
			name:  "работает только http — одна запись с его scheme",
			https: variantDown(model.SchemeHTTPS),
			http:  variantCheck(model.SchemeHTTP, "example.com", "/", nil),
			want:  []VariantRecord{{Variant: model.VariantMain, Scheme: model.SchemeHTTP}},
		},
		{
			name: "http→https: одинаковый итоговый хост+путь — один сайт",
			https: variantCheck(model.SchemeHTTPS, "example.com", "/",
				variantHash("страница")),
			http: variantCheck(model.SchemeHTTP, "example.com", "/",
				variantHash("страница")),
			want: []VariantRecord{{Variant: model.VariantMain, Scheme: model.SchemeHTTPS}},
		},
		{
			name: "одинаковый хост+путь при разном контенте — один сайт (ИЛИ-критерий)",
			https: variantCheck(model.SchemeHTTPS, "example.com", "/",
				variantHash("https-версия")),
			http: variantCheck(model.SchemeHTTP, "example.com", "/",
				variantHash("http-версия")),
			want: []VariantRecord{{Variant: model.VariantMain, Scheme: model.SchemeHTTPS}},
		},
		{
			name:  "http ведёт на другой сайт (другой хост) — два варианта",
			https: variantCheck(model.SchemeHTTPS, "example.com", "/", nil),
			http:  variantCheck(model.SchemeHTTP, "parking.example.net", "/parked.html", nil),
			want: []VariantRecord{
				{Variant: model.VariantMain, Scheme: model.SchemeHTTPS},
				{Variant: model.VariantHTTP, Scheme: model.SchemeHTTP},
			},
		},
		{
			name:  "одинаковый хост, разные пути — два варианта",
			https: variantCheck(model.SchemeHTTPS, "example.com", "/", nil),
			http:  variantCheck(model.SchemeHTTP, "example.com", "/en", nil),
			want: []VariantRecord{
				{Variant: model.VariantMain, Scheme: model.SchemeHTTPS},
				{Variant: model.VariantHTTP, Scheme: model.SchemeHTTP},
			},
		},
		{
			name: "парковка: пути разные, хэш равен — один сайт (ИЛИ-критерий)",
			https: variantCheck(model.SchemeHTTPS, "example.com", "/",
				variantHash("заглушка парковки")),
			http: variantCheck(model.SchemeHTTP, "example.com", "/index2.html",
				variantHash("заглушка парковки")),
			want: []VariantRecord{{Variant: model.VariantMain, Scheme: model.SchemeHTTPS}},
		},
		{
			name:  "хэш пустого контента — не nil: равные пустые страницы — один сайт",
			https: variantCheck(model.SchemeHTTPS, "example.com", "/", variantHash("")),
			http:  variantCheck(model.SchemeHTTP, "example.com", "/empty", variantHash("")),
			want:  []VariantRecord{{Variant: model.VariantMain, Scheme: model.SchemeHTTPS}},
		},
		{
			name:  "оба хэша nil: критерий вырождается в host+путь — один сайт",
			https: variantCheck(model.SchemeHTTPS, "example.com", "/", nil),
			http:  variantCheck(model.SchemeHTTP, "example.com", "/", nil),
			want:  []VariantRecord{{Variant: model.VariantMain, Scheme: model.SchemeHTTPS}},
		},
		{
			name:  "оба хэша nil: пути разные — два варианта",
			https: variantCheck(model.SchemeHTTPS, "example.com", "/", nil),
			http:  variantCheck(model.SchemeHTTP, "example.com", "/en", nil),
			want: []VariantRecord{
				{Variant: model.VariantMain, Scheme: model.SchemeHTTPS},
				{Variant: model.VariantHTTP, Scheme: model.SchemeHTTP},
			},
		},
		{
			name:  "один хэш nil (контент не сохранён): хэш-ветка недоказуема — два варианта",
			https: variantCheck(model.SchemeHTTPS, "example.com", "/", nil),
			http: variantCheck(model.SchemeHTTP, "example.com", "/en",
				variantHash("страница")),
			want: []VariantRecord{
				{Variant: model.VariantMain, Scheme: model.SchemeHTTPS},
				{Variant: model.VariantHTTP, Scheme: model.SchemeHTTP},
			},
		},
		{
			name:  "регистр хоста не разваливает сайт (punycode уже нижний — страховка)",
			https: variantCheck(model.SchemeHTTPS, "www.example.com", "/", nil),
			http:  variantCheck(model.SchemeHTTP, "WWW.EXAMPLE.COM", "/", nil),
			want:  []VariantRecord{{Variant: model.VariantMain, Scheme: model.SchemeHTTPS}},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			for _, args := range [][2]VariantInput{{tc.https, tc.http}, {tc.http, tc.https}} {
				got, err := DecideVariants(args[0], args[1])
				if err != nil {
					t.Fatalf("DecideVariants(%v, %v): %v", args[0].Scheme, args[1].Scheme, err)
				}
				if !variantRecordsEqual(got, tc.want) {
					t.Errorf("DecideVariants(%v, %v) = %v, want %v",
						args[0].Scheme, args[1].Scheme, got, tc.want)
				}
			}
		})
	}
}

// TestDecideVariantsOneProtocolFinalURL — scheme записи = схема
// ПРОВЕРКИ: http-проверка могла закончиться на https-странице, запись
// всё равно несёт scheme=http (данные из http-проверки).
func TestDecideVariantsOneProtocolFinalURL(t *testing.T) {
	https, http := variantDown(model.SchemeHTTPS), variantCheck(model.SchemeHTTP,
		"example.com", "/", nil)
	got, err := DecideVariants(http, https)
	if err != nil {
		t.Fatal(err)
	}
	want := []VariantRecord{{Variant: model.VariantMain, Scheme: model.SchemeHTTP}}
	if !variantRecordsEqual(got, want) {
		t.Errorf("DecideVariants = %v, want %v", got, want)
	}
}

// TestDecideVariantsBadPair — аргументы обязаны образовывать пару
// http+https по полю Scheme: одинаковые или некорректные схемы — ошибка
// (молча перепутанные итоги дали бы неверные variant/scheme в БД).
func TestDecideVariantsBadPair(t *testing.T) {
	cases := []struct {
		name string
		a, b VariantInput
	}{
		{
			name: "дважды https",
			a:    variantCheck(model.SchemeHTTPS, "example.com", "/", nil),
			b:    variantCheck(model.SchemeHTTPS, "example.com", "/", nil),
		},
		{
			name: "дважды http",
			a:    variantCheck(model.SchemeHTTP, "example.com", "/", nil),
			b:    variantDown(model.SchemeHTTP),
		},
		{
			name: "нулевой код схемы",
			a:    variantDown(model.SchemeHTTPS),
			b:    VariantInput{Scheme: 0, Available: true, Host: "example.com", Path: "/"},
		},
		{
			name: "неизвестный код схемы",
			a:    VariantInput{Scheme: model.Scheme(9), Available: true, Host: "a.example"},
			b:    variantDown(model.SchemeHTTP),
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := DecideVariants(tc.a, tc.b)
			if err == nil {
				t.Fatalf("DecideVariants(%v, %v) = %v, want ошибка", tc.a.Scheme, tc.b.Scheme, got)
			}
			if got != nil {
				t.Errorf("DecideVariants: при ошибке записи = %v, want nil", got)
			}
		})
	}
}

// TestVariantSameSite — критерий одинаковости: обе ветки ИЛИ по
// отдельности и обязательная доступность итогов.
func TestVariantSameSite(t *testing.T) {
	cases := []struct {
		name string
		a, b VariantInput
		want bool
	}{
		{
			name: "недоступные итоги — не «тот же сайт»",
			a:    variantDown(model.SchemeHTTP),
			b:    variantDown(model.SchemeHTTPS),
			want: false,
		},
		{
			name: "доступный и недоступный — не один сайт",
			a:    variantCheck(model.SchemeHTTPS, "example.com", "/", nil),
			b:    variantDown(model.SchemeHTTP),
			want: false,
		},
		{
			name: "хост+путь совпали — хэш не нужен",
			a:    variantCheck(model.SchemeHTTPS, "example.com", "/", nil),
			b:    variantCheck(model.SchemeHTTP, "example.com", "/", nil),
			want: true,
		},
		{
			name: "путь другой, хэш равен — парковка, один сайт",
			a:    variantCheck(model.SchemeHTTPS, "example.com", "/", variantHash("p")),
			b:    variantCheck(model.SchemeHTTP, "example.com", "/x", variantHash("p")),
			want: true,
		},
		{
			name: "хост другой, хэш равен — один сайт",
			a:    variantCheck(model.SchemeHTTPS, "a.example.com", "/", variantHash("p")),
			b:    variantCheck(model.SchemeHTTP, "b.example.com", "/", variantHash("p")),
			want: true,
		},
		{
			name: "путь другой, хэши различны — разные",
			a:    variantCheck(model.SchemeHTTPS, "example.com", "/", variantHash("1")),
			b:    variantCheck(model.SchemeHTTP, "example.com", "/en", variantHash("2")),
			want: false,
		},
		{
			name: "один хэш nil — хэш-ветка недоказуема",
			a:    variantCheck(model.SchemeHTTPS, "example.com", "/", variantHash("1")),
			b:    variantCheck(model.SchemeHTTP, "example.com", "/en", nil),
			want: false,
		},
		{
			name: "оба хэша nil, пути разные — разные",
			a:    variantCheck(model.SchemeHTTPS, "example.com", "/", nil),
			b:    variantCheck(model.SchemeHTTP, "example.com", "/en", nil),
			want: false,
		},
		{
			name: "регистр хоста прощается, путь — байт-точно",
			a:    variantCheck(model.SchemeHTTPS, "EXAMPLE.com", "/a b", nil),
			b:    variantCheck(model.SchemeHTTP, "example.COM", "/a b", nil),
			want: true,
		},
		{
			name: "кодированная и декодированная форма пути — разные байты",
			a:    variantCheck(model.SchemeHTTPS, "example.com", "/a%20b", nil),
			b:    variantCheck(model.SchemeHTTP, "example.com", "/a b", nil),
			want: false,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := VariantSameSite(tc.a, tc.b); got != tc.want {
				t.Errorf("VariantSameSite(%q%q, %q%q) = %v, want %v",
					tc.a.Host, tc.a.Path, tc.b.Host, tc.b.Path, got, tc.want)
			}
		})
	}
}

// TestNewVariantInput — представление итогового URL в логике вариантов:
// хост без порта, путь без query и фрагмента, percent-декодирование
// url.URL.Path, nil-URL недоступной проверки.
func TestNewVariantInput(t *testing.T) {
	parse := func(t *testing.T, raw string) *url.URL {
		t.Helper()
		u, err := url.Parse(raw)
		if err != nil {
			t.Fatalf("url.Parse(%q): %v", raw, err)
		}
		return u
	}

	t.Run("порт срезается, query и фрагмент отбрасываются", func(t *testing.T) {
		in := NewVariantInput(model.SchemeHTTPS, true,
			parse(t, "https://www.example.com:443/a%20b?i=1#frag"), nil)
		if in.Host != "www.example.com" {
			t.Errorf("Host = %q, want www.example.com (без порта)", in.Host)
		}
		if in.Path != "/a b" {
			t.Errorf("Path = %q, want декодированный %q без query/фрагмента", in.Path, "/a b")
		}
	})

	t.Run("нестандартный порт не разваливает один сайт", func(t *testing.T) {
		https := NewVariantInput(model.SchemeHTTPS, true,
			parse(t, "https://example.com:8443/"), nil)
		http := NewVariantInput(model.SchemeHTTP, true,
			parse(t, "http://example.com:8080/"), nil)
		if !VariantSameSite(https, http) {
			t.Errorf("итоги example.com:8443 и example.com:8080 — не один сайт, want один")
		}
	})

	t.Run("query не входит в критерий: счётчиковые параметры — один сайт", func(t *testing.T) {
		https := NewVariantInput(model.SchemeHTTPS, true,
			parse(t, "https://example.com/"), nil)
		http := NewVariantInput(model.SchemeHTTP, true,
			parse(t, "http://example.com/?i=2"), nil)
		if !VariantSameSite(https, http) {
			t.Error("пути / и /?i=2 посчитаны разными сайтами, want один (query вне критерия)")
		}
	})

	t.Run("IPv6-литерал без скобок и порта", func(t *testing.T) {
		in := NewVariantInput(model.SchemeHTTP, true,
			parse(t, "http://[2001:db8::1]:8080/"), nil)
		if in.Host != "2001:db8::1" {
			t.Errorf("Host = %q, want 2001:db8::1", in.Host)
		}
	})

	t.Run("nil URL недоступной проверки — нулевые Host/Path", func(t *testing.T) {
		in := NewVariantInput(model.SchemeHTTP, false, nil, nil)
		if in.Host != "" || in.Path != "" || in.Available {
			t.Errorf("NewVariantInput(nil) = %+v, want нулевые Host/Path", in)
		}
		if VariantSameSite(in, in) {
			t.Error("недоступный итог назван тем же сайтом, want false")
		}
	})
}
