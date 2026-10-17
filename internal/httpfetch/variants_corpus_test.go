// Сквозной корпус логики вариантов и алиасов — задача T-059 (ТЗ 11.1:
// «логика вариантов http/https и алиасов www/без www (в т. ч. редирект
// bare → www, www → bare, http → https, http ведёт на другой сайт)»;
// критерий приёмки §11.4 п. 2: «проверка по каждому субдомену выполняется
// независимо; результаты соответствуют правилам вариантов и алиасов
// (раздел 5.5)»).
//
// В отличие от TestDecideVariants (T-054) и TestDetectAliases* (T-055),
// проверяющих каждую функцию отдельно, кейс корпуса — цельная история
// домена: субдомены × проверки http/https → DecideVariants по каждому
// субдомену независимо → записи-кандидаты всего домена → DetectAliases.
// Этапы соединены как у воркера T-053: scheme записи = проверка-источник
// её данных (вариант 0 при паре записей несёт https-итог), AliasInput
// собирается из той же VariantInput, которую получила запись.
//
// Ожидания фиксируют, какие записи subdomain_results возникают по
// §11.4 п. 2: variant/scheme (T-054) и alias_kind/alias_subdomain_id
// (T-055), плюс признак StoreContent «контент класса хранится один раз».
// Каждая запись прогоняется через валидацию модели
// (model.Variant/Scheme/AliasKind) — корпус держит контракт типов.
package httpfetch

import (
	"fmt"
	"net/url"
	"testing"

	"crawler/internal/content"
	"crawler/internal/model"
)

// corpusProbe — итог проверки субдомена по одному протоколу: доступность
// (сервер ответил любым статусом, ТЗ 5.5 п. 5), итоговый URL страницы в
// конце цепочки редиректов, тело итоговой страницы для хэша контента и
// флаг noHash «контент не сохранён» (лимит max_store_bytes T-056 или
// ошибка до сохранения — хэш nil). Недоступная проверка URL не имеет.
type corpusProbe struct {
	available bool
	final     string
	body      string
	noHash    bool
}

// corpusSubdomain — субдомен домена задачи и итоги его проверок по обоим
// протоколам: каждый субдомен проверяется независимо (§11.4 п. 2).
type corpusSubdomain struct {
	id    int16
	label string // '' — апекс
	https corpusProbe
	http  corpusProbe
}

// corpusRecord — ожидаемая запись субдомен-результата после вариант- и
// алиас-логики: субдомен, variant/scheme, alias_kind/alias_subdomain_id
// (aliasSub −1 — nil у независимой записи) и признак хранения контента.
type corpusRecord struct {
	subdomain int16
	variant   model.Variant
	scheme    model.Scheme
	aliasKind model.AliasKind
	aliasSub  int16
	store     bool
}

// corpusCase — история домена: субдомены с итогами проверок и ожидаемые
// записи (в порядке субдоменов; внутри субдомена — [variant 0, variant 1],
// как отдаёт DecideVariants).
type corpusCase struct {
	name   string
	domain string
	subs   []corpusSubdomain
	want   []corpusRecord
}

// corpusProbeHash — хэш контента итоговой страницы (content.HashOf —
// контракт T-041: Put обязан вернуть HashOf(data)); noHash или
// недоступность проверки дают nil.
func corpusProbeHash(p corpusProbe) *content.Hash {
	if !p.available || p.noHash {
		return nil
	}
	h := content.HashOf([]byte(p.body))
	return &h
}

// corpusHashBytes — хэш VariantInput в форме AliasInput.ContentHash;
// nil остаётся nil (контент не сохранён).
func corpusHashBytes(h *content.Hash) []byte {
	if h == nil {
		return nil
	}
	return h[:]
}

// corpusProbeIn — вход вариант-логики из итога проверки: итоговый URL
// прогоняется через NewVariantInput (порт срезается, query и фрагмент не
// входят), как это сделает воркер T-053; недоступная проверка — nil URL.
func corpusProbeIn(t *testing.T, scheme model.Scheme, p corpusProbe) VariantInput {
	t.Helper()
	var final *url.URL
	if p.available {
		u, err := url.Parse(p.final)
		if err != nil {
			t.Fatalf("corpus: url.Parse(%q): %v", p.final, err)
		}
		final = u
	}
	return NewVariantInput(scheme, p.available, final, corpusProbeHash(p))
}

// corpusRun — прогон истории домена по конвейеру «варианты → алиасы»:
//
//  1. DecideVariants для каждого субдомена независимо (§11.4 п. 2);
//     порядок аргументов чередуется — итоги проверок приходят воркеру
//     T-053 в порядке завершения, а пара различается полем Scheme;
//  2. DetectAliases по всем записям-кандидатам домена;
//  3. контракт модели: коды Variant/Scheme/AliasKind валидны,
//     alias_subdomain_id согласован с alias_kind, ссылается на ДРУГОЙ
//     субдомен (ТЗ 5.5 п. 6) и StoreContent не бывает при nil-хэше.
func corpusRun(t *testing.T, tc corpusCase) []corpusRecord {
	t.Helper()

	// Запись-кандидат: колонки variant/scheme и данные записи — итог
	// проверки её схемы (scheme записи = источник данных, T-054).
	type candidate struct {
		subdomain int16
		label     string
		variant   model.Variant
		scheme    model.Scheme
		data      VariantInput
	}
	var candidates []candidate
	for i, sub := range tc.subs {
		httpsIn := corpusProbeIn(t, model.SchemeHTTPS, sub.https)
		httpIn := corpusProbeIn(t, model.SchemeHTTP, sub.http)
		first, second := httpsIn, httpIn
		if i%2 == 1 {
			first, second = httpIn, httpsIn
		}
		records, err := DecideVariants(first, second)
		if err != nil {
			t.Fatalf("corpus %q: DecideVariants(субдомен %q): %v", tc.name, sub.label, err)
		}
		for _, r := range records {
			if !r.Variant.Valid() {
				t.Fatalf("corpus %q: вариант %d не проходит model.Variant.Valid()", tc.name, r.Variant)
			}
			if !r.Scheme.Valid() {
				t.Fatalf("corpus %q: схема %d не проходит model.Scheme.Valid()", tc.name, r.Scheme)
			}
			data := httpsIn
			if r.Scheme == model.SchemeHTTP {
				data = httpIn
			}
			candidates = append(candidates, candidate{
				subdomain: sub.id, label: sub.label,
				variant: r.Variant, scheme: r.Scheme, data: data,
			})
		}
	}

	in := make([]AliasInput, len(candidates))
	for i, c := range candidates {
		in[i] = AliasInput{
			SubdomainID: c.subdomain,
			Label:       c.label,
			FinalHost:   c.data.Host,
			FinalPath:   c.data.Path,
			Scheme:      c.scheme,
			Variant:     c.variant,
			ContentHash: corpusHashBytes(c.data.ContentHash),
		}
	}
	results := DetectAliases(tc.domain, in)
	if len(results) != len(in) {
		t.Fatalf("corpus %q: DetectAliases вернул %d решений, хочу %d", tc.name, len(results), len(in))
	}

	out := make([]corpusRecord, len(results))
	for i, res := range results {
		if !res.AliasKind.Valid() {
			t.Errorf("corpus %q: запись %d: алиас %d не проходит model.AliasKind.Valid()",
				tc.name, i, res.AliasKind)
		}
		if (res.AliasSubdomainID == nil) != (res.AliasKind == model.AliasNone) {
			t.Errorf("corpus %q: запись %d: alias_subdomain_id = %v при alias_kind %v — пара должна быть согласована",
				tc.name, i, res.AliasSubdomainID, res.AliasKind)
		}
		if res.AliasSubdomainID != nil && *res.AliasSubdomainID == in[i].SubdomainID {
			t.Errorf("corpus %q: запись %d: alias_subdomain_id указывает на свой субдомен %d (ТЗ 5.5 п. 6 — «другого субдомена»)",
				tc.name, i, in[i].SubdomainID)
		}
		if res.StoreContent && in[i].ContentHash == nil {
			t.Errorf("corpus %q: запись %d: StoreContent при nil-хэше — сохранять нечего", tc.name, i)
		}
		if res.SubdomainID != in[i].SubdomainID || res.Variant != in[i].Variant {
			t.Errorf("corpus %q: запись %d: идентичность (%d, %v) не параллельна входу (%d, %v)",
				tc.name, i, res.SubdomainID, res.Variant, in[i].SubdomainID, in[i].Variant)
		}
		rec := corpusRecord{
			subdomain: candidates[i].subdomain,
			variant:   candidates[i].variant,
			scheme:    candidates[i].scheme,
			aliasKind: res.AliasKind,
			aliasSub:  -1,
			store:     res.StoreContent,
		}
		if res.AliasSubdomainID != nil {
			rec.aliasSub = *res.AliasSubdomainID
		}
		out[i] = rec
	}
	return out
}

// corpusRecordString — человекочитаемая запись для сообщений об ошибке.
func corpusRecordString(r corpusRecord) string {
	orig := "—"
	if r.aliasSub >= 0 {
		orig = fmt.Sprintf("%d", r.aliasSub)
	}
	return fmt.Sprintf("{субдомен %d, variant %v, scheme %v, alias_kind %v, alias_subdomain_id %s, контент %v}",
		r.subdomain, r.variant, r.scheme, r.aliasKind, orig, r.store)
}

// TestVariantsCorpus — корпус историй приёмки §11.4 п. 2 (сценарии
// §11.1 + парковки, цепочки из трёх, nil-хэши, независимость субдоменов).
func TestVariantsCorpus(t *testing.T) {
	cases := []corpusCase{
		// ТЗ 11.1 «bare → www»: апекс редиректит на www — итоговый URL
		// обеих проверок апекса = адрес www, http/https сходятся → по
		// одной записи variant 0 на субдомен; итоговый хост апекса —
		// собственный адрес www ⇒ алиас-редирект (kind 2) на www,
		// контент хранится один раз у оригинала www.
		{
			name:   "bare → www: итоговый URL апекса — адрес www; апекс — редирект-алиас на www",
			domain: "example.com",
			subs: []corpusSubdomain{
				{
					id: 0, label: "",
					https: corpusProbe{available: true, final: "https://www.example.com/", body: "сайт"},
					http:  corpusProbe{available: true, final: "https://www.example.com/", body: "сайт"},
				},
				{
					id: 1, label: "www",
					https: corpusProbe{available: true, final: "https://www.example.com/", body: "сайт"},
					http:  corpusProbe{available: true, final: "https://www.example.com/", body: "сайт"},
				},
			},
			want: []corpusRecord{
				{subdomain: 0, variant: model.VariantMain, scheme: model.SchemeHTTPS,
					aliasKind: model.AliasRedirect, aliasSub: 1, store: false},
				{subdomain: 1, variant: model.VariantMain, scheme: model.SchemeHTTPS,
					aliasKind: model.AliasNone, aliasSub: -1, store: true},
			},
		},
		// ТЗ 11.1 «www → bare», зеркально: итоговый хост www — собственный
		// адрес апекса ⇒ оригинал класса апекс (id 0, минимальный
		// subdomain_id), www — редирект-алиас на него.
		{
			name:   "www → bare: зеркально — оригинал апекс, www — редирект-алиас",
			domain: "example.com",
			subs: []corpusSubdomain{
				{
					id: 0, label: "",
					https: corpusProbe{available: true, final: "https://example.com/", body: "сайт"},
					http:  corpusProbe{available: true, final: "https://example.com/", body: "сайт"},
				},
				{
					id: 1, label: "www",
					https: corpusProbe{available: true, final: "https://example.com/", body: "сайт"},
					http:  corpusProbe{available: true, final: "https://example.com/", body: "сайт"},
				},
			},
			want: []corpusRecord{
				{subdomain: 0, variant: model.VariantMain, scheme: model.SchemeHTTPS,
					aliasKind: model.AliasNone, aliasSub: -1, store: true},
				{subdomain: 1, variant: model.VariantMain, scheme: model.SchemeHTTPS,
					aliasKind: model.AliasRedirect, aliasSub: 0, store: false},
			},
		},
		// ТЗ 11.1 «http → https»: http-проверка редиректом сходится на ту
		// же страницу, что https — один сайт ⇒ одна запись variant 0 с
		// scheme https (канонический протокол, данные https-проверки).
		// Счётчиковый query итогового URL (?i=2) сайт не разваливает:
		// query вне критерия (ТЗ 5.5 п. 5 «хост и путь»).
		{
			name:   "http → https: обе проверки сходятся на одну страницу — одна запись variant 0 scheme https",
			domain: "example.com",
			subs: []corpusSubdomain{
				{
					id: 0, label: "",
					https: corpusProbe{available: true, final: "https://example.com/", body: "главная"},
					http:  corpusProbe{available: true, final: "https://example.com/?i=2", body: "главная"},
				},
			},
			want: []corpusRecord{
				{subdomain: 0, variant: model.VariantMain, scheme: model.SchemeHTTPS,
					aliasKind: model.AliasNone, aliasSub: -1, store: true},
			},
		},
		// ТЗ 11.1 «http ведёт на другой сайт»: итоговый хост http-проверки
		// чужой (парковка) ⇒ две записи: variant 0 (https, сайт апекса) и
		// variant 1 (http, чужой сайт). Записи одного субдомена алиасами
		// друг друга не бывают; обе независимы и хранят свой контент.
		// Явный :80 в итоговом URL не мешает — порт срезается.
		{
			name:   "http ведёт на другой сайт: чужой итоговый хост — две записи variant 0 (https) и variant 1 (http)",
			domain: "example.com",
			subs: []corpusSubdomain{
				{
					id: 0, label: "",
					https: corpusProbe{available: true, final: "https://example.com/", body: "главная"},
					http:  corpusProbe{available: true, final: "http://parking.example.net:80/parked.html", body: "парковка"},
				},
			},
			want: []corpusRecord{
				{subdomain: 0, variant: model.VariantMain, scheme: model.SchemeHTTPS,
					aliasKind: model.AliasNone, aliasSub: -1, store: true},
				{subdomain: 0, variant: model.VariantHTTP, scheme: model.SchemeHTTP,
					aliasKind: model.AliasNone, aliasSub: -1, store: true},
			},
		},
		// Парковка (ТЗ 5.7, §8): оба субдомена дома на РАЗНЫХ URL, контент
		// один (равный хэш) ⇒ класс один, оригинал — минимальный
		// subdomain_id (апекс), www идентичен ему (kind 1 — свой адрес,
		// чужой контент), контент класса сохраняется один раз.
		{
			name:   "парковка: равный хэш при разных URL — www идентичен апексу, контент один",
			domain: "example.com",
			subs: []corpusSubdomain{
				{
					id: 0, label: "",
					https: corpusProbe{available: true, final: "https://example.com/", body: "заглушка хостинга"},
					http:  corpusProbe{available: true, final: "https://example.com/", body: "заглушка хостинга"},
				},
				{
					id: 1, label: "www",
					https: corpusProbe{available: true, final: "https://www.example.com/", body: "заглушка хостинга"},
					http:  corpusProbe{available: true, final: "https://www.example.com/", body: "заглушка хостинга"},
				},
			},
			want: []corpusRecord{
				{subdomain: 0, variant: model.VariantMain, scheme: model.SchemeHTTPS,
					aliasKind: model.AliasNone, aliasSub: -1, store: true},
				{subdomain: 1, variant: model.VariantMain, scheme: model.SchemeHTTPS,
					aliasKind: model.AliasIdentical, aliasSub: 0, store: false},
			},
		},
		// Цепочка из трёх, редирект-вариант: admin и app съезжаются на
		// api (итоговый хост — собственный адрес api), api дома ⇒
		// оригинал один (api), оба остальных — редирект-алиасы на него;
		// транзитивность a=b=c даёт один класс.
		{
			name:   "цепочка из трёх: admin и app редиректят на api — оригинал один (api)",
			domain: "example.com",
			subs: []corpusSubdomain{
				{
					id: 2, label: "admin",
					https: corpusProbe{available: true, final: "https://api.example.com/", body: "api-сайт"},
					http:  corpusProbe{available: true, final: "https://api.example.com/", body: "api-сайт"},
				},
				{
					id: 3, label: "api",
					https: corpusProbe{available: true, final: "https://api.example.com/", body: "api-сайт"},
					http:  corpusProbe{available: true, final: "https://api.example.com/", body: "api-сайт"},
				},
				{
					id: 4, label: "app",
					https: corpusProbe{available: true, final: "https://api.example.com/", body: "api-сайт"},
					http:  corpusProbe{available: true, final: "https://api.example.com/", body: "api-сайт"},
				},
			},
			want: []corpusRecord{
				{subdomain: 2, variant: model.VariantMain, scheme: model.SchemeHTTPS,
					aliasKind: model.AliasRedirect, aliasSub: 3, store: false},
				{subdomain: 3, variant: model.VariantMain, scheme: model.SchemeHTTPS,
					aliasKind: model.AliasNone, aliasSub: -1, store: true},
				{subdomain: 4, variant: model.VariantMain, scheme: model.SchemeHTTPS,
					aliasKind: model.AliasRedirect, aliasSub: 3, store: false},
			},
		},
		// Цепочка из трёх, хэш-вариант: все дома на своих адресах,
		// контент один (общая заглушка) ⇒ класс один по хэшу, оригинал —
		// минимальный (subdomain_id, variant) — admin (id 2), остальные
		// идентичны (kind 1, свой адрес при равном хэше).
		{
			name:   "цепочка из трёх по хэшу: все дома, контент один — оригинал минимальный subdomain_id",
			domain: "example.com",
			subs: []corpusSubdomain{
				{
					id: 2, label: "admin",
					https: corpusProbe{available: true, final: "https://admin.example.com/", body: "одно и то же"},
					http:  corpusProbe{available: true, final: "https://admin.example.com/", body: "одно и то же"},
				},
				{
					id: 3, label: "api",
					https: corpusProbe{available: true, final: "https://api.example.com/", body: "одно и то же"},
					http:  corpusProbe{available: true, final: "https://api.example.com/", body: "одно и то же"},
				},
				{
					id: 4, label: "app",
					https: corpusProbe{available: true, final: "https://app.example.com/", body: "одно и то же"},
					http:  corpusProbe{available: true, final: "https://app.example.com/", body: "одно и то же"},
				},
			},
			want: []corpusRecord{
				{subdomain: 2, variant: model.VariantMain, scheme: model.SchemeHTTPS,
					aliasKind: model.AliasNone, aliasSub: -1, store: true},
				{subdomain: 3, variant: model.VariantMain, scheme: model.SchemeHTTPS,
					aliasKind: model.AliasIdentical, aliasSub: 2, store: false},
				{subdomain: 4, variant: model.VariantMain, scheme: model.SchemeHTTPS,
					aliasKind: model.AliasIdentical, aliasSub: 2, store: false},
			},
		},
		// nil-хэши: контент не сохранён у всех записей (лимит T-056) —
		// хэш-критерий недоступен, bare → www работает по итоговому URL;
		// контент класса хранить нечего (store false даже у оригинала).
		{
			name:   "nil-хэши: контент не сохранён — bare → www работает по итоговому URL, контент не хранится",
			domain: "example.com",
			subs: []corpusSubdomain{
				{
					id: 0, label: "",
					https: corpusProbe{available: true, final: "https://www.example.com/", body: "огромная страница", noHash: true},
					http:  corpusProbe{available: true, final: "https://www.example.com/", body: "огромная страница", noHash: true},
				},
				{
					id: 1, label: "www",
					https: corpusProbe{available: true, final: "https://www.example.com/", body: "огромная страница", noHash: true},
					http:  corpusProbe{available: true, final: "https://www.example.com/", body: "огромная страница", noHash: true},
				},
			},
			want: []corpusRecord{
				{subdomain: 0, variant: model.VariantMain, scheme: model.SchemeHTTPS,
					aliasKind: model.AliasRedirect, aliasSub: 1, store: false},
				{subdomain: 1, variant: model.VariantMain, scheme: model.SchemeHTTPS,
					aliasKind: model.AliasNone, aliasSub: -1, store: false},
			},
		},
		// nil-хэш с одной стороны: http-проверка апекса заканчивается на
		// другом пути, её контент не сохранён — хэш-ветка недоказуема
		// (двух nil нужно, а тут один) ⇒ «протоколы ведут на разные
		// сайты»: две записи апекса; https-итог апекса и www сходятся
		// (www → bare) — редирект-алиас на variant 0 апекса, у variant 1
		// свой контент, алиасов нет.
		{
			name:   "один хэш nil: хэш-ветка недоказуема — у апекса два варианта; www → bare — редирект на variant 0",
			domain: "example.com",
			subs: []corpusSubdomain{
				{
					id: 0, label: "",
					https: corpusProbe{available: true, final: "https://example.com/", body: "главная"},
					http:  corpusProbe{available: true, final: "http://example.com/en", body: "английская версия", noHash: true},
				},
				{
					id: 1, label: "www",
					https: corpusProbe{available: true, final: "https://example.com/", body: "главная"},
					http:  corpusProbe{available: true, final: "https://example.com/", body: "главная"},
				},
			},
			want: []corpusRecord{
				{subdomain: 0, variant: model.VariantMain, scheme: model.SchemeHTTPS,
					aliasKind: model.AliasNone, aliasSub: -1, store: true},
				{subdomain: 0, variant: model.VariantHTTP, scheme: model.SchemeHTTP,
					aliasKind: model.AliasNone, aliasSub: -1, store: false},
				{subdomain: 1, variant: model.VariantMain, scheme: model.SchemeHTTPS,
					aliasKind: model.AliasRedirect, aliasSub: 0, store: false},
			},
		},
		// Независимость проверок (§11.4 п. 2): апекс не ответил ни по
		// одному протоколу — записей для него нет вовсе (финальный статус
		// no_http решает агрегатор T-058 по §7), www при этом жив и даёт
		// свою запись без каких-либо алиасов.
		{
			name:   "субдомены независимы: апекс не отвечает вовсе — записей нет; www жив своей записью",
			domain: "example.com",
			subs: []corpusSubdomain{
				{
					id: 0, label: "",
					https: corpusProbe{},
					http:  corpusProbe{},
				},
				{
					id: 1, label: "www",
					https: corpusProbe{available: true, final: "https://www.example.com/", body: "сайт"},
					http:  corpusProbe{available: true, final: "https://www.example.com/", body: "сайт"},
				},
			},
			want: []corpusRecord{
				{subdomain: 1, variant: model.VariantMain, scheme: model.SchemeHTTPS,
					aliasKind: model.AliasNone, aliasSub: -1, store: true},
			},
		},
		// Смешанная история: https-проверка апекса редиректит на www
		// (алиас-редирект на оригинал www), http-проверка апекса уезжает
		// на парковку — «http ведёт на другой сайт» ⇒ у апекса две
		// записи; variant 1 (парковка) в алиас-класс не попадает и
		// хранит свой контент.
		{
			name:   "смешанный: https апекса сходится на www (алиас), http апекса уезжает в парковку (variant 1)",
			domain: "example.com",
			subs: []corpusSubdomain{
				{
					id: 0, label: "",
					https: corpusProbe{available: true, final: "https://www.example.com/", body: "сайт"},
					http:  corpusProbe{available: true, final: "http://parking.example.net/", body: "парковка"},
				},
				{
					id: 1, label: "www",
					https: corpusProbe{available: true, final: "https://www.example.com/", body: "сайт"},
					http:  corpusProbe{available: true, final: "https://www.example.com/", body: "сайт"},
				},
			},
			want: []corpusRecord{
				{subdomain: 0, variant: model.VariantMain, scheme: model.SchemeHTTPS,
					aliasKind: model.AliasRedirect, aliasSub: 1, store: false},
				{subdomain: 0, variant: model.VariantHTTP, scheme: model.SchemeHTTP,
					aliasKind: model.AliasNone, aliasSub: -1, store: true},
				{subdomain: 1, variant: model.VariantMain, scheme: model.SchemeHTTPS,
					aliasKind: model.AliasNone, aliasSub: -1, store: true},
			},
		},
		// ТЗ 5.5 п. 5 «работает только один протокол»: у апекса отвечает
		// только https, у www — только http (итог остаётся на своём
		// адресе); по одной записи variant 0 со схемой СВОЕЙ проверки —
		// scheme записи = источник данных. Сайты разные — алиасов нет.
		{
			name:   "работает один протокол: апекс только https, www только http — по одной записи со своей scheme",
			domain: "example.com",
			subs: []corpusSubdomain{
				{
					id: 0, label: "",
					https: corpusProbe{available: true, final: "https://example.com/", body: "сайт"},
					http:  corpusProbe{},
				},
				{
					id: 1, label: "www",
					https: corpusProbe{},
					http:  corpusProbe{available: true, final: "http://www.example.com/", body: "сайт www"},
				},
			},
			want: []corpusRecord{
				{subdomain: 0, variant: model.VariantMain, scheme: model.SchemeHTTPS,
					aliasKind: model.AliasNone, aliasSub: -1, store: true},
				{subdomain: 1, variant: model.VariantMain, scheme: model.SchemeHTTP,
					aliasKind: model.AliasNone, aliasSub: -1, store: true},
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := corpusRun(t, tc)
			if len(got) != len(tc.want) {
				t.Errorf("записей %d, хочу %d:\n  получено: %s\n  ожидалось: %s",
					len(got), len(tc.want), corpusRecordsString(got), corpusRecordsString(tc.want))
			}
			n := len(got)
			if len(tc.want) < n {
				n = len(tc.want)
			}
			for i := 0; i < n; i++ {
				if got[i] != tc.want[i] {
					t.Errorf("запись[%d] = %s, хочу %s", i, corpusRecordString(got[i]), corpusRecordString(tc.want[i]))
				}
			}
		})
	}
}

// corpusRecordsString — список записей одной строкой для диагностики.
func corpusRecordsString(rs []corpusRecord) string {
	out := "["
	for i, r := range rs {
		if i > 0 {
			out += " "
		}
		out += corpusRecordString(r)
	}
	return out + "]"
}
