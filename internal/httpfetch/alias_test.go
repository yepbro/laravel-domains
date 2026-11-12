package httpfetch

import (
	"reflect"
	"testing"

	"crawler/internal/model"
)

// aliasHash is a test content hash (16 bytes, like content.HashOf).
func aliasHash(b byte) []byte {
	h := make([]byte, 16)
	for i := range h {
		h[i] = b
	}
	return h
}

// aliasGot is a compact representation of a decision for comparison:
// the kind, the original subdomain (−1 is nil), and the store-content flag.
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
	// The classic spec 11.1/testbed case (ApexRedirect): bare redirects to
	// www, the final URL of both is www's address. www is the original (at
	// home), bare is a redirect to another subdomain: alias_kind = 2,
	// content stored once.
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
	// The reverse classic (testbed WWWRedirect): www redirects to bare.
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
	// Parking/placeholder (spec 5.7, §8): different final addresses (each
	// at its own), the same hash — identical, the original is the minimal
	// subdomain_id (the apex has priority), content stored once.
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
		// a and b converge on c's address, c is at home: a single original
		// (c), both aliases are redirects to it.
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
		// All three are at home with identical content: one class, the
		// original is the minimal subdomain_id, the rest are identical.
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
	// Subdomains without content (nil hash for both): the URL criterion
	// applies.
	in := []AliasInput{
		{SubdomainID: 0, Label: "", FinalHost: "www.example.com", FinalPath: "/en",
			Scheme: model.SchemeHTTPS, Variant: model.VariantMain},
		{SubdomainID: 1, Label: "www", FinalHost: "www.example.com", FinalPath: "/en",
			Scheme: model.SchemeHTTPS, Variant: model.VariantMain},
	}
	want := []aliasGot{
		{kind: model.AliasRedirect, orig: 1, store: false},
		{kind: model.AliasNone, orig: -1, store: false}, // no content — nothing to store
	}
	if got := aliasResults(t, "example.com", in); !reflect.DeepEqual(got, want) {
		t.Errorf("nil-хэш, URL-критерий: got %+v, want %+v", got, want)
	}
}

func TestDetectAliasesNoMatches(t *testing.T) {
	// Independent subdomains: alias_kind 0, no references; the content
	// belongs to each; a record without a hash stores nothing.
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
		// Testbed WWWSame: both at home, one hash. The minimal
		// subdomain_id is the apex (0), www is identical to it.
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
		// Parallel subdomain fetches complete in arbitrary order — the
		// decisions must match.
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
		// Both moved to the hosting parking page: nobody is "at home", the
		// original is the minimal id, the second is identical (not a
		// redirect to a subdomain).
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
	// Final URL normalization: host case, the trailing dot (FQDN), an
	// empty path = "/" — the records match without it.
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
		// The host is empty — there is no page (fetch error): no URL
		// criterion, no alias, no content stored.
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
		// The folding contract (T-054): variants of one subdomain are
		// different sites. Even if they end up in one class transitively
		// (via the hash of a third subdomain), a reference to their own
		// subdomain is forbidden (spec item 6 "another subdomain") — the
		// record stays independent.
		in := []AliasInput{
			{SubdomainID: 2, Label: "admin", FinalHost: "admin.example.com", FinalPath: "/",
				Scheme: model.SchemeHTTPS, Variant: model.VariantMain, ContentHash: aliasHash(5)},
			{SubdomainID: 2, Label: "admin", FinalHost: "admin.example.com:8080", FinalPath: "/old",
				Scheme: model.SchemeHTTP, Variant: model.VariantHTTP, ContentHash: aliasHash(5)},
			{SubdomainID: 4, Label: "app", FinalHost: "app.example.com", FinalPath: "/",
				Scheme: model.SchemeHTTP, Variant: model.VariantMain, ContentHash: aliasHash(5)},
		}
		want := []aliasGot{
			{kind: model.AliasNone, orig: -1, store: true}, // the class original (minimal id)
			{kind: model.AliasNone, orig: -1, store: true}, // the same subdomain — independent
			{kind: model.AliasIdentical, orig: 2, store: false},
		}
		if got := aliasResults(t, "example.com", in); !reflect.DeepEqual(got, want) {
			t.Errorf("варианты одного субдомена: got %+v, want %+v", got, want)
		}
	})
}
