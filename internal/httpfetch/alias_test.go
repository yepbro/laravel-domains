package httpfetch

import (
	"reflect"
	"testing"

	"crawler/internal/model"
)

// aliasHash — тестовый хэш контента (16 байт, как content.HashOf).
func aliasHash(b byte) []byte {
	h := make([]byte, 16)
	for i := range h {
		h[i] = b
	}
	return h
}

// aliasGot — компактное представление решения для сравнения:
// kind, субдомен-оригинал (−1 — nil), признак хранения контента.
type aliasGot struct {
	kind  model.AliasKind
	orig  int
	store bool
}

func aliasResults(t *testing.T, domain string, in []AliasInput) []aliasGot {
	t.Helper()
	res := DetectAliases(domain, in)
	if len(res) != len(in) {
		t.Fatalf("DetectAliases: длина результата %d, хочу %d", len(res), len(in))
	}
	got := make([]aliasGot, len(res))
	for i, r := range res {
		if r.SubdomainID != in[i].SubdomainID || r.Variant != in[i].Variant {
			t.Errorf("результат[%d]: идентичность (%d, %v) не совпадает со входом (%d, %v)",
				i, r.SubdomainID, r.Variant, in[i].SubdomainID, in[i].Variant)
		}
		if (r.AliasSubdomainID == nil) != (r.AliasKind == model.AliasNone) {
			t.Errorf("результат[%d]: alias_subdomain_id = %v при alias_kind %v — пара должна быть согласована",
				i, r.AliasSubdomainID, r.AliasKind)
		}
		got[i] = aliasGot{kind: r.AliasKind, orig: -1, store: r.StoreContent}
		if r.AliasSubdomainID != nil {
			got[i].orig = int(*r.AliasSubdomainID)
		}
	}
	return got
}

func TestDetectAliasesBareToWWW(t *testing.T) {
	// Классика ТЗ 11.1/тестбед (ApexRedirect): bare редиректит на www,
	// итоговый URL обоих — адрес www. www — оригинал (дома), bare —
	// редирект на другой субдомен: alias_kind = 2, контент один раз.
	in := []AliasInput{
		{SubdomainID: 0, Label: "", FinalHost: "www.example.com", FinalPath: "/",
			Scheme: model.SchemeHTTPS, Variant: model.VariantMain, ContentHash: aliasHash(1)},
		{SubdomainID: 1, Label: "www", FinalHost: "www.example.com", FinalPath: "/",
			Scheme: model.SchemeHTTPS, Variant: model.VariantMain, ContentHash: aliasHash(1)},
	}
	want := []aliasGot{
		{kind: model.AliasRedirect, orig: 1, store: false},
		{kind: model.AliasNone, orig: -1, store: true},
	}
	if got := aliasResults(t, "example.com", in); !reflect.DeepEqual(got, want) {
		t.Errorf("bare → www: got %+v, want %+v", got, want)
	}
}

func TestDetectAliasesWWWToBare(t *testing.T) {
	// Обратная классика (тестбед WWWRedirect): www редиректит на bare.
	in := []AliasInput{
		{SubdomainID: 0, Label: "", FinalHost: "example.com", FinalPath: "/",
			Scheme: model.SchemeHTTPS, Variant: model.VariantMain, ContentHash: aliasHash(2)},
		{SubdomainID: 1, Label: "www", FinalHost: "example.com", FinalPath: "/",
			Scheme: model.SchemeHTTPS, Variant: model.VariantMain, ContentHash: aliasHash(2)},
	}
	want := []aliasGot{
		{kind: model.AliasNone, orig: -1, store: true},
		{kind: model.AliasRedirect, orig: 0, store: false},
	}
	if got := aliasResults(t, "example.com", in); !reflect.DeepEqual(got, want) {
		t.Errorf("www → bare: got %+v, want %+v", got, want)
	}
}

func TestDetectAliasesSameHashDifferentURLs(t *testing.T) {
	// Парковка/заглушка (ТЗ 5.7, §8): разные итоговые адреса (каждый
	// у себя), одинаковый хэш — идентичны, оригинал — минимальный
	// subdomain_id (апекс приоритетен), контент один раз.
	in := []AliasInput{
		{SubdomainID: 0, Label: "", FinalHost: "example.com", FinalPath: "/",
			Scheme: model.SchemeHTTP, Variant: model.VariantMain, ContentHash: aliasHash(7)},
		{SubdomainID: 2, Label: "admin", FinalHost: "admin.example.com", FinalPath: "/",
			Scheme: model.SchemeHTTP, Variant: model.VariantMain, ContentHash: aliasHash(7)},
	}
	want := []aliasGot{
		{kind: model.AliasNone, orig: -1, store: true},
		{kind: model.AliasIdentical, orig: 0, store: false},
	}
	if got := aliasResults(t, "example.com", in); !reflect.DeepEqual(got, want) {
		t.Errorf("парковка по хэшу: got %+v, want %+v", got, want)
	}
}

func TestDetectAliasesChainOfThree(t *testing.T) {
	t.Run("редирект-цепочка a→b→c", func(t *testing.T) {
		// a и b съезжаются на адрес c, c дома: оригинал один (c),
		// оба алиаса — редиректы на него.
		in := []AliasInput{
			{SubdomainID: 2, Label: "admin", FinalHost: "api.example.com", FinalPath: "/",
				Scheme: model.SchemeHTTPS, Variant: model.VariantMain, ContentHash: aliasHash(3)},
			{SubdomainID: 4, Label: "app", FinalHost: "api.example.com", FinalPath: "/",
				Scheme: model.SchemeHTTPS, Variant: model.VariantMain, ContentHash: aliasHash(3)},
			{SubdomainID: 3, Label: "api", FinalHost: "api.example.com", FinalPath: "/",
				Scheme: model.SchemeHTTPS, Variant: model.VariantMain, ContentHash: aliasHash(3)},
		}
		want := []aliasGot{
			{kind: model.AliasRedirect, orig: 3, store: false},
			{kind: model.AliasRedirect, orig: 3, store: false},
			{kind: model.AliasNone, orig: -1, store: true},
		}
		if got := aliasResults(t, "example.com", in); !reflect.DeepEqual(got, want) {
			t.Errorf("цепочка a=b=c: got %+v, want %+v", got, want)
		}
	})

	t.Run("хэш-цепочка a=b=c", func(t *testing.T) {
		// Все трое дома с одинаковым контентом: класс один, оригинал —
		// минимальный subdomain_id, остальные идентичны.
		in := []AliasInput{
			{SubdomainID: 3, Label: "api", FinalHost: "api.example.com", FinalPath: "/",
				Scheme: model.SchemeHTTP, Variant: model.VariantMain, ContentHash: aliasHash(5)},
			{SubdomainID: 2, Label: "admin", FinalHost: "admin.example.com", FinalPath: "/",
				Scheme: model.SchemeHTTP, Variant: model.VariantMain, ContentHash: aliasHash(5)},
			{SubdomainID: 4, Label: "app", FinalHost: "app.example.com", FinalPath: "/",
				Scheme: model.SchemeHTTP, Variant: model.VariantMain, ContentHash: aliasHash(5)},
		}
		want := []aliasGot{
			{kind: model.AliasIdentical, orig: 2, store: false},
			{kind: model.AliasNone, orig: -1, store: true},
			{kind: model.AliasIdentical, orig: 2, store: false},
		}
		if got := aliasResults(t, "example.com", in); !reflect.DeepEqual(got, want) {
			t.Errorf("хэш-цепочка a=b=c: got %+v, want %+v", got, want)
		}
	})
}

func TestDetectAliasesNilHashURLCriterion(t *testing.T) {
	// Субдомены без контента (хэш nil у обоих): работает URL-критерий.
	in := []AliasInput{
		{SubdomainID: 0, Label: "", FinalHost: "www.example.com", FinalPath: "/en",
			Scheme: model.SchemeHTTPS, Variant: model.VariantMain},
		{SubdomainID: 1, Label: "www", FinalHost: "www.example.com", FinalPath: "/en",
			Scheme: model.SchemeHTTPS, Variant: model.VariantMain},
	}
	want := []aliasGot{
		{kind: model.AliasRedirect, orig: 1, store: false},
		{kind: model.AliasNone, orig: -1, store: false}, // контента нет — хранить нечего
	}
	if got := aliasResults(t, "example.com", in); !reflect.DeepEqual(got, want) {
		t.Errorf("nil-хэш, URL-критерий: got %+v, want %+v", got, want)
	}
}

func TestDetectAliasesNoMatches(t *testing.T) {
	// Независимые субдомены: alias_kind 0, без ссылок; контент —
	// каждый свой; запись без хэша не хранит ничего.
	in := []AliasInput{
		{SubdomainID: 0, Label: "", FinalHost: "example.com", FinalPath: "/",
			Scheme: model.SchemeHTTPS, Variant: model.VariantMain, ContentHash: aliasHash(1)},
		{SubdomainID: 1, Label: "www", FinalHost: "www.example.com", FinalPath: "/",
			Scheme: model.SchemeHTTPS, Variant: model.VariantMain, ContentHash: aliasHash(2)},
		{SubdomainID: 3, Label: "api", FinalHost: "api.example.com", FinalPath: "/v1",
			Scheme: model.SchemeHTTP, Variant: model.VariantMain},
	}
	want := []aliasGot{
		{kind: model.AliasNone, orig: -1, store: true},
		{kind: model.AliasNone, orig: -1, store: true},
		{kind: model.AliasNone, orig: -1, store: false},
	}
	if got := aliasResults(t, "example.com", in); !reflect.DeepEqual(got, want) {
		t.Errorf("без совпадений: got %+v, want %+v", got, want)
	}
}

func TestDetectAliasesOriginChoice(t *testing.T) {
	t.Run("одинаковый контент без редиректа — оригинал апекс", func(t *testing.T) {
		// Тестбед WWWSame: оба дома, один хэш. Минимальный
		// subdomain_id — апекс (0), www идентичен ему.
		in := []AliasInput{
			{SubdomainID: 1, Label: "www", FinalHost: "www.example.com", FinalPath: "/",
				Scheme: model.SchemeHTTP, Variant: model.VariantMain, ContentHash: aliasHash(9)},
			{SubdomainID: 0, Label: "", FinalHost: "example.com", FinalPath: "/",
				Scheme: model.SchemeHTTP, Variant: model.VariantMain, ContentHash: aliasHash(9)},
		}
		want := []aliasGot{
			{kind: model.AliasIdentical, orig: 0, store: false},
			{kind: model.AliasNone, orig: -1, store: true},
		}
		if got := aliasResults(t, "example.com", in); !reflect.DeepEqual(got, want) {
			t.Errorf("WWWSame: got %+v, want %+v", got, want)
		}
	})

	t.Run("результат не зависит от порядка входа", func(t *testing.T) {
		// Параллельные загрузки субдоменов завершаются в произвольном
		// порядке — решения обязаны совпадать.
		inA := []AliasInput{
			{SubdomainID: 0, Label: "", FinalHost: "www.example.com", FinalPath: "/",
				Scheme: model.SchemeHTTPS, Variant: model.VariantMain, ContentHash: aliasHash(4)},
			{SubdomainID: 1, Label: "www", FinalHost: "www.example.com", FinalPath: "/",
				Scheme: model.SchemeHTTPS, Variant: model.VariantMain, ContentHash: aliasHash(4)},
		}
		inB := []AliasInput{inA[1], inA[0]}
		gotA := aliasResults(t, "example.com", inA)
		gotB := aliasResults(t, "example.com", inB)
		want := []aliasGot{
			{kind: model.AliasRedirect, orig: 1, store: false},
			{kind: model.AliasNone, orig: -1, store: true},
		}
		wantReversed := []aliasGot{want[1], want[0]}
		if !reflect.DeepEqual(gotA, want) {
			t.Errorf("порядок A: got %+v, want %+v", gotA, want)
		}
		if !reflect.DeepEqual(gotB, wantReversed) {
			t.Errorf("порядок B: got %+v, want %+v", gotB, wantReversed)
		}
	})

	t.Run("общий внешний URL — оригинал минимальный subdomain_id", func(t *testing.T) {
		// Оба уехали на парковку хостинга: «дома» никого, оригинал —
		// минимальный id, второй идентичен (не редирект на субдомен).
		in := []AliasInput{
			{SubdomainID: 1, Label: "www", FinalHost: "park.hosting.example.net", FinalPath: "/site",
				Scheme: model.SchemeHTTPS, Variant: model.VariantMain, ContentHash: aliasHash(6)},
			{SubdomainID: 0, Label: "", FinalHost: "park.hosting.example.net", FinalPath: "/site",
				Scheme: model.SchemeHTTPS, Variant: model.VariantMain, ContentHash: aliasHash(6)},
		}
		want := []aliasGot{
			{kind: model.AliasIdentical, orig: 0, store: false},
			{kind: model.AliasNone, orig: -1, store: true},
		}
		if got := aliasResults(t, "example.com", in); !reflect.DeepEqual(got, want) {
			t.Errorf("внешний URL: got %+v, want %+v", got, want)
		}
	})
}

func TestDetectAliasesNormalization(t *testing.T) {
	// Нормализация итогового URL: регистр хоста, завершающая точка
	// (FQDN), пустой путь = «/» — записи совпадают и без неё.
	in := []AliasInput{
		{SubdomainID: 0, Label: "", FinalHost: "WWW.Example.COM.", FinalPath: "",
			Scheme: model.SchemeHTTPS, Variant: model.VariantMain, ContentHash: aliasHash(1)},
		{SubdomainID: 1, Label: "www", FinalHost: "www.example.com", FinalPath: "/",
			Scheme: model.SchemeHTTPS, Variant: model.VariantMain, ContentHash: aliasHash(1)},
	}
	want := []aliasGot{
		{kind: model.AliasRedirect, orig: 1, store: false},
		{kind: model.AliasNone, orig: -1, store: true},
	}
	if got := aliasResults(t, "Example.COM.", in); !reflect.DeepEqual(got, want) {
		t.Errorf("нормализация: got %+v, want %+v", got, want)
	}
}

func TestDetectAliasesDegenerate(t *testing.T) {
	t.Run("пустой вход", func(t *testing.T) {
		if got := DetectAliases("example.com", nil); len(got) != 0 {
			t.Errorf("пустой вход: got %+v, хочу пусто", got)
		}
	})

	t.Run("запись без итогового URL независима", func(t *testing.T) {
		// Хост пуст — страницы нет (ошибка загрузки): ни URL-критерия,
		// ни алиаса, контент не хранится.
		in := []AliasInput{
			{SubdomainID: 0, Label: "", FinalHost: "", FinalPath: "",
				Scheme: model.SchemeHTTP, Variant: model.VariantMain},
			{SubdomainID: 1, Label: "www", FinalHost: "www.example.com", FinalPath: "/",
				Scheme: model.SchemeHTTPS, Variant: model.VariantMain, ContentHash: aliasHash(8)},
		}
		want := []aliasGot{
			{kind: model.AliasNone, orig: -1, store: false},
			{kind: model.AliasNone, orig: -1, store: true},
		}
		if got := aliasResults(t, "example.com", in); !reflect.DeepEqual(got, want) {
			t.Errorf("без итогового URL: got %+v, want %+v", got, want)
		}
	})

	t.Run("варианты одного субдомена алиасами не бывают", func(t *testing.T) {
		// Контракт вариант-логики (T-054): варианты одного субдомена —
		// разные сайты. Даже если они транзитивно попали в один класс
		// (через хэш третьего субдомена), ссылка на свой субдомен
		// запрещена (ТЗ п. 6 «другого субдомена») — запись независима.
		in := []AliasInput{
			{SubdomainID: 2, Label: "admin", FinalHost: "admin.example.com", FinalPath: "/",
				Scheme: model.SchemeHTTPS, Variant: model.VariantMain, ContentHash: aliasHash(5)},
			{SubdomainID: 2, Label: "admin", FinalHost: "admin.example.com:8080", FinalPath: "/old",
				Scheme: model.SchemeHTTP, Variant: model.VariantHTTP, ContentHash: aliasHash(5)},
			{SubdomainID: 4, Label: "app", FinalHost: "app.example.com", FinalPath: "/",
				Scheme: model.SchemeHTTP, Variant: model.VariantMain, ContentHash: aliasHash(5)},
		}
		want := []aliasGot{
			{kind: model.AliasNone, orig: -1, store: true}, // оригинал класса (мин id)
			{kind: model.AliasNone, orig: -1, store: true}, // тот же субдомен — независим
			{kind: model.AliasIdentical, orig: 2, store: false},
		}
		if got := aliasResults(t, "example.com", in); !reflect.DeepEqual(got, want) {
			t.Errorf("варианты одного субдомена: got %+v, want %+v", got, want)
		}
	})
}
