package httpworker

import (
	"net/url"
	"strings"
	"testing"

	"crawler/internal/content"
	"crawler/internal/httpfetch"
	"crawler/internal/model"
)

// Матричные тесты автомата состояния проверки схемы (T-266, ТЗ-fsm §3
// ФТ-12): все легальные комбинации перечислены явно и принимаются
// валидаторами; нелегальные — отвергаются. Матрицы и нумерация строк —
// doc-комментарий schemestate.go.

// matrixURL — URL фикстуры матрицы (валиден по построению).
func matrixURL(raw string) *url.URL {
	u, err := url.Parse(raw)
	if err != nil {
		panic(err)
	}
	return u
}

// TestSchemeCheckValidateMatrix — все 14 легальных состояний проверки
// схемы (С1–С14): каждая строка матрицы принимается валидатором.
func TestSchemeCheckValidateMatrix(t *testing.T) {
	var (
		h   = content.HashOf([]byte("matrix body"))
		spa = httpfetch.SPAVerdict{}
	)
	cases := []struct {
		name  string
		check SchemeCheck
	}{
		{
			"С1 живая, ранний Put прошёл",
			SchemeCheck{Scheme: model.SchemeHTTPS, Available: true, FinalURL: matrixURL("https://x.example/"),
				Status: 200, ResponseMS: 40, ContentHash: &h, ContentStored: true, SPA: &spa},
		},
		{
			"С2 живая, пауза записи контента",
			SchemeCheck{Scheme: model.SchemeHTTP, Available: true, FinalURL: matrixURL("http://x.example/"),
				Status: 200, ResponseMS: 40, ContentHash: &h, SPA: &spa},
		},
		{
			"С3 живая, тело обрезано",
			SchemeCheck{Scheme: model.SchemeHTTPS, Available: true, FinalURL: matrixURL("https://x.example/"),
				Status: 200, ResponseMS: 40, ContentHash: &h, Truncated: true,
				ErrClass: ErrBody, ErrText: "тело обрезано потолком max_body_bytes (данные частичны)", SPA: &spa},
		},
		{
			"С4 кэш-попадание страницы С1",
			SchemeCheck{Scheme: model.SchemeHTTPS, Available: true, Cached: true,
				// FinalURL — стартовый URL проверки: осознанное
				// искажение при кэш-попадании (schemestate.go).
				FinalURL: matrixURL("https://x.example/"), Status: 200, ResponseMS: 40,
				ContentHash: &h, ContentStored: true, SPA: &spa,
				Redirects: []httpfetch.RedirectStep{{URL: "https://x.example/", Status: 200}}},
		},
		{
			"С5 кэш-попадание страницы С2 (пауза записи наследуется)",
			SchemeCheck{Scheme: model.SchemeHTTP, Available: true, Cached: true,
				FinalURL: matrixURL("http://x.example/"), Status: 200, ResponseMS: 40,
				ContentHash: &h, SPA: &spa,
				Redirects: []httpfetch.RedirectStep{{URL: "http://x.example/", Status: 200}}},
		},
		{
			"С6 кэш-попадание страницы С3 (обрезка переносится)",
			SchemeCheck{Scheme: model.SchemeHTTPS, Available: true, Cached: true,
				FinalURL: matrixURL("https://x.example/"), Status: 200, ResponseMS: 40,
				ContentHash: &h, Truncated: true, ErrClass: ErrBody,
				ErrText: "тело обрезано потолком max_body_bytes (данные частичны)", SPA: &spa,
				Redirects: []httpfetch.RedirectStep{{URL: "https://x.example/", Status: 200}}},
		},
		{
			"С7 мёртвый диал: connection refused",
			SchemeCheck{Scheme: model.SchemeHTTPS, ErrClass: ErrNetwork, ErrText: "dial tcp: connection refused", DeadDial: true},
		},
		{
			"С8 мёртвый диал: connect-таймаут",
			SchemeCheck{Scheme: model.SchemeHTTP, ErrClass: ErrTimeout, ErrText: "dial tcp: i/o timeout", DeadDial: true},
		},
		{
			"С9 таймаут чтения тела с частичным итогом",
			SchemeCheck{Scheme: model.SchemeHTTPS, ErrClass: ErrTimeout, ErrText: "timeout awaiting response body",
				FinalURL: matrixURL("https://x.example/"), Status: 200,
				Redirects: []httpfetch.RedirectStep{{URL: "https://x.example/", Status: 200}}},
		},
		{
			"С10 сбой TLS-рукопожатия",
			SchemeCheck{Scheme: model.SchemeHTTPS, ErrClass: ErrTLS, ErrText: "tls: handshake failure"},
		},
		{
			"С11 сбой хопа редиректа (цепочка непуста — не dead-host)",
			SchemeCheck{Scheme: model.SchemeHTTPS, ErrClass: ErrNetwork, ErrText: "dial tcp: lookup hop.example: no such host",
				Redirects: []httpfetch.RedirectStep{{URL: "https://x.example/", Status: 302}}},
		},
		{
			"С12 лимит редиректов",
			SchemeCheck{Scheme: model.SchemeHTTPS, ErrClass: ErrRedirects, ErrText: httpfetch.ErrTooManyRedirects.Error(),
				Redirects: []httpfetch.RedirectStep{
					{URL: "https://x.example/", Status: 301},
					{URL: "https://x.example/a", Status: 301},
				}},
		},
		{
			"С13 ошибка чтения тела после заголовков",
			SchemeCheck{Scheme: model.SchemeHTTPS, ErrClass: ErrBody, ErrText: "gzip: данные превышают лимит",
				FinalURL: matrixURL("https://x.example/"), Status: 200},
		},
		{
			"С14 синтетика dead-host cutoff",
			SchemeCheck{Scheme: model.SchemeHTTP, ErrClass: ErrCutoff,
				ErrText: "dead-host cutoff (T-164): apex и www не прошли connect-фазу — субдомен не проверялся"},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if err := tc.check.validate(); err != nil {
				t.Fatalf("легальная комбинация отклонена: %v", err)
			}
		})
	}
}

// TestSchemeCheckValidateIllegal — нелегальные комбинации отклоняются
// (метка — инвариант, чью ветку зажигает нарушение; порядок проверок в
// validate() подобран так, чтобы каждая ветка была достижима).
func TestSchemeCheckValidateIllegal(t *testing.T) {
	var (
		h   = content.HashOf([]byte("matrix body"))
		spa = httpfetch.SPAVerdict{}
	)
	// live — базовая живая проверка С1; dead — базовый мёртвый диал С7.
	live := func() SchemeCheck {
		return SchemeCheck{Scheme: model.SchemeHTTPS, Available: true, FinalURL: matrixURL("https://x.example/"),
			Status: 200, ResponseMS: 40, ContentHash: &h, ContentStored: true, SPA: &spa}
	}
	cases := []struct {
		name   string
		mutate func(*SchemeCheck)
	}{
		{"И1: класс вне перечисления", func(c *SchemeCheck) { c.ErrClass = ErrorClass(9) }},
		{"И1: схема вне перечисления", func(c *SchemeCheck) { c.Scheme = model.Scheme(9) }},
		{"И8: успех с текстом ошибки", func(c *SchemeCheck) { c.ErrText = "лишний текст" }},
		{"И8: ошибка без текста", func(c *SchemeCheck) {
			c.ErrClass, c.ErrText = ErrTimeout, ""
			c.Available, c.ContentHash, c.ContentStored, c.SPA = false, nil, false, nil
		}},
		{"И5: недоступная с хэшем", func(c *SchemeCheck) {
			c.Available, c.ContentStored, c.SPA, c.FinalURL = false, false, nil, nil
		}},
		{"И5: недоступная с обрезкой", func(c *SchemeCheck) {
			c.Available, c.ContentStored, c.SPA, c.FinalURL = false, false, nil, nil
			c.Truncated, c.ErrClass, c.ErrText = true, ErrBody, "тело обрезано"
		}},
		{"И5: недоступная с сохранённым телом", func(c *SchemeCheck) {
			c.Available, c.SPA, c.FinalURL = false, nil, nil
		}},
		{"И4: доступная без хэша", func(c *SchemeCheck) { c.ContentHash = nil }},
		{"И4: доступная без итогового URL", func(c *SchemeCheck) { c.FinalURL = nil }},
		{"И4: доступная без SPA-вердикта", func(c *SchemeCheck) { c.SPA = nil }},
		{"И4: ErrNone при обрезке", func(c *SchemeCheck) { c.Truncated = true }},
		{"И4: ErrBody без обрезки", func(c *SchemeCheck) { c.ErrClass, c.ErrText = ErrBody, "битая кодировка" }},
		{"И3: кэш-попадание без хэша", func(c *SchemeCheck) { c.Cached, c.ContentHash = true, nil }},
		{"И3: кэш-попадание недоступной проверки", func(c *SchemeCheck) {
			c.Cached = true
			c.Available, c.ContentStored, c.SPA, c.FinalURL = false, false, nil, nil
		}},
		{"И6: сохранено при обрезке", func(c *SchemeCheck) {
			c.Truncated, c.ErrClass, c.ErrText = true, ErrBody, "тело обрезано"
		}},
		{"И7: ErrCutoff у доступной", func(c *SchemeCheck) {
			c.ErrClass, c.ErrText = ErrCutoff, "dead-host cutoff"
		}},
		{"И2: DeadDial у доступной", func(c *SchemeCheck) { c.DeadDial = true }},
		{"И2: DeadDial с непустой цепочкой", func(c *SchemeCheck) {
			c.ErrClass, c.ErrText, c.DeadDial = ErrNetwork, "dial tcp: connection refused", true
			c.Available, c.ContentHash, c.ContentStored, c.SPA, c.FinalURL = false, nil, false, nil, nil
			c.Redirects = []httpfetch.RedirectStep{{URL: "https://x.example/", Status: 302}}
		}},
		{"И2: DeadDial с классом TLS", func(c *SchemeCheck) {
			c.ErrClass, c.ErrText, c.DeadDial = ErrTLS, "tls: handshake failure", true
			c.Available, c.ContentHash, c.ContentStored, c.SPA, c.FinalURL = false, nil, false, nil, nil
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			c := live()
			tc.mutate(&c)
			if err := c.validate(); err == nil {
				t.Fatal("нелегальная комбинация принята")
			}
		})
	}
}

// TestRecordValidateMatrix — контентная часть записи: все строки матрицы
// Р1–Р8 на своих границах (до ContentSaver / после нормализации) плюс
// минимальная запись прямого вызова newRecordBatch.
func TestRecordValidateMatrix(t *testing.T) {
	var (
		h       = content.HashOf([]byte("record body"))
		aliasTo = int16(1)
	)
	cases := []struct {
		name string
		rec  Record
	}{
		{
			"Р1 оригинал с контентом (обе границы)",
			Record{Variant: model.VariantMain, Scheme: model.SchemeHTTPS, SubdomainID: 0,
				FinalURL: "https://x.example/", ContentHash: &h, ContentStored: true, StoreContent: true},
		},
		{
			"Р2 до Save: оригинал в паузе записи контента",
			Record{Variant: model.VariantMain, Scheme: model.SchemeHTTPS, SubdomainID: 0,
				FinalURL: "https://x.example/", ContentHash: &h, StoreContent: true},
		},
		{
			"Р3 до Save: оригинал с обрезкой",
			Record{Variant: model.VariantMain, Scheme: model.SchemeHTTPS, SubdomainID: 0,
				FinalURL: "https://x.example/", ContentHash: &h, Truncated: true,
				ErrClass: ErrBody, StoreContent: true},
		},
		{
			"Р4 до Save: алиас сохранённого оригинала",
			Record{Variant: model.VariantMain, Scheme: model.SchemeHTTPS, SubdomainID: 1,
				FinalURL: "https://x.example/", ContentHash: &h, ContentStored: true,
				AliasKind: model.AliasRedirect, AliasSubdomainID: &aliasTo},
		},
		{
			"Р4 до Save: алиас оригинала в паузе записи",
			Record{Variant: model.VariantMain, Scheme: model.SchemeHTTPS, SubdomainID: 1,
				FinalURL: "https://x.example/", ContentHash: &h,
				AliasKind: model.AliasIdentical, AliasSubdomainID: &aliasTo},
		},
		{
			"Р5 до Save: алиас обрезанной страницы",
			Record{Variant: model.VariantMain, Scheme: model.SchemeHTTPS, SubdomainID: 1,
				FinalURL: "https://x.example/", ContentHash: &h, Truncated: true, ErrClass: ErrBody,
				AliasKind: model.AliasRedirect, AliasSubdomainID: &aliasTo},
		},
		{
			"Р6 после Save: контент не сохранён",
			Record{Variant: model.VariantMain, Scheme: model.SchemeHTTPS, SubdomainID: 0,
				FinalURL: "https://x.example/", ContentNotStored: true, StoreContent: true},
		},
		{
			"Р7 после Save: обрезанный оригинал без хэша",
			Record{Variant: model.VariantMain, Scheme: model.SchemeHTTPS, SubdomainID: 0,
				FinalURL: "https://x.example/", ContentNotStored: true, Truncated: true,
				ErrClass: ErrBody, StoreContent: true},
		},
		{
			"Р8 после Save: алиас (Truncated/ContentStored — копия src)",
			Record{Variant: model.VariantMain, Scheme: model.SchemeHTTPS, SubdomainID: 1,
				FinalURL: "https://x.example/", ContentStored: true,
				AliasKind: model.AliasRedirect, AliasSubdomainID: &aliasTo},
		},
		{
			"Р8 после Save: алиас обрезанной страницы",
			Record{Variant: model.VariantMain, Scheme: model.SchemeHTTPS, SubdomainID: 1,
				FinalURL: "https://x.example/", Truncated: true, ErrClass: ErrBody,
				AliasKind: model.AliasRedirect, AliasSubdomainID: &aliasTo},
		},
		{
			"минимальная запись прямого вызова (частичные фикстуры)",
			Record{Variant: model.VariantMain, Scheme: model.SchemeHTTPS, SubdomainID: 0},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if err := tc.rec.validate(); err != nil {
				t.Fatalf("легальная комбинация отклонена: %v", err)
			}
		})
	}
}

// TestRecordValidateIllegal — нелегальные комбинации контентной части
// отклоняются (по нарушению на инвариант П1–П7).
func TestRecordValidateIllegal(t *testing.T) {
	var (
		h       = content.HashOf([]byte("record body"))
		aliasTo = int16(1)
	)
	// origin — Р1 (оригинал с контентом); alias — Р4 (алиас до Save).
	origin := func() Record {
		return Record{Variant: model.VariantMain, Scheme: model.SchemeHTTPS, SubdomainID: 0,
			FinalURL: "https://x.example/", ContentHash: &h, ContentStored: true, StoreContent: true}
	}
	alias := func() Record {
		return Record{Variant: model.VariantMain, Scheme: model.SchemeHTTPS, SubdomainID: 1,
			FinalURL: "https://x.example/", ContentHash: &h,
			AliasKind: model.AliasRedirect, AliasSubdomainID: &aliasTo}
	}
	cases := []struct {
		name   string
		base   func() Record
		mutate func(*Record)
	}{
		{"П1: вариант вне перечисления", origin, func(r *Record) { r.Variant = model.Variant(7) }},
		{"П1: схема вне перечисления", origin, func(r *Record) { r.Scheme = model.Scheme(9) }},
		{"П1: вид алиаса вне перечисления", origin, func(r *Record) { r.AliasKind = model.AliasKind(5) }},
		{"П2: AliasNone с ссылкой", origin, func(r *Record) { r.AliasSubdomainID = &aliasTo }},
		{"П2: алиас без ссылки", alias, func(r *Record) { r.AliasSubdomainID = nil }},
		{"П3: StoreContent у алиаса", alias, func(r *Record) { r.StoreContent = true }},
		{"П4: ContentNotStored с хэшем", origin, func(r *Record) { r.ContentNotStored = true }},
		{"П4: ContentNotStored с сохранённым телом", origin, func(r *Record) {
			r.ContentHash, r.ContentNotStored, r.ContentStored = nil, true, true
		}},
		{"П4: ContentNotStored у алиаса", alias, func(r *Record) {
			r.ContentHash, r.ContentNotStored = nil, true
		}},
		{"П5: оригинал без хэша и признака", origin, func(r *Record) { r.ContentHash = nil }},
		{"П6: обрезка с сохранением", origin, func(r *Record) {
			r.Truncated, r.ErrClass = true, ErrBody
		}},
		{"П7: класс записи вне доступной проверки", origin, func(r *Record) { r.ErrClass = ErrTimeout }},
		{"П7: ErrBody без обрезки", origin, func(r *Record) { r.ErrClass = ErrBody }},
		{"П7: обрезка без ErrBody", alias, func(r *Record) { r.Truncated = true }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			r := tc.base()
			tc.mutate(&r)
			if err := r.validate(); err == nil {
				t.Fatal("нелегальная комбинация принята")
			}
		})
	}
}

// TestValidateResult — граница «перед ContentSaver» (конец ProcessDomain,
// ФТ-12): валидатор результата домена принимает легальный результат
// (живая проверка + запись Р1 + синтетика cutoff С14) и отвергает
// нелегальные проверку/запись с контекстом субдомена.
func TestValidateResult(t *testing.T) {
	var (
		h   = content.HashOf([]byte("result body"))
		spa = httpfetch.SPAVerdict{}
	)
	legal := &DomainResult{
		ZoneID: 7, DomainID: 91, Name: "ok.example",
		Subdomains: []SubdomainResult{
			{
				SubdomainID: 0, Label: "", Hostname: "ok.example",
				HTTPS: SchemeCheck{Scheme: model.SchemeHTTPS, Available: true, FinalURL: matrixURL("https://ok.example/"),
					Status: 200, ContentHash: &h, ContentStored: true, SPA: &spa},
				HTTP: SchemeCheck{Scheme: model.SchemeHTTP, ErrClass: ErrNetwork, ErrText: "dial tcp: connection refused"},
				Records: []Record{{Variant: model.VariantMain, Scheme: model.SchemeHTTPS, SubdomainID: 0,
					FinalURL: "https://ok.example/", ContentHash: &h, ContentStored: true, StoreContent: true}},
			},
			{
				// Синтетика dead-host cutoff (С14): записей нет.
				SubdomainID: 2, Label: "blog", Hostname: "blog.ok.example",
				HTTPS: SchemeCheck{Scheme: model.SchemeHTTPS, ErrClass: ErrCutoff, ErrText: "dead-host cutoff (T-164)"},
				HTTP:  SchemeCheck{Scheme: model.SchemeHTTP, ErrClass: ErrCutoff, ErrText: "dead-host cutoff (T-164)"},
			},
		},
	}
	// clone — копия результата с собственными срезами (мутации кейсов
	// не должны протекать в базовый legal).
	clone := func(res *DomainResult) *DomainResult {
		out := *res
		out.Subdomains = append([]SubdomainResult(nil), res.Subdomains...)
		for i := range out.Subdomains {
			out.Subdomains[i].Records = append([]Record(nil), res.Subdomains[i].Records...)
		}
		return &out
	}

	if err := validateResult(legal); err != nil {
		t.Fatalf("легальный результат отклонён: %v", err)
	}

	badCheck := clone(legal)
	badCheck.Subdomains[0].HTTPS.ContentHash = nil // И4: Available без хэша
	if err := validateResult(badCheck); err == nil || !strings.Contains(err.Error(), "ok.example") {
		t.Fatalf("ожидалась ошибка валидации проверки с контекстом субдомена: %v", err)
	}

	badRecord := clone(legal)
	badRecord.Subdomains[0].Records[0].ContentNotStored = true // П4: при живом хэше
	if err := validateResult(badRecord); err == nil || !strings.Contains(err.Error(), "T-056") {
		t.Fatalf("ожидалась ошибка валидации записи: %v", err)
	}
}
