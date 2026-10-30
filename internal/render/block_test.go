package render

import (
	"context"
	"errors"
	"iter"
	"maps"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/chromedp/cdproto"
	"github.com/chromedp/cdproto/cdp/jsonv2"
	"github.com/chromedp/cdproto/fetch"
	"github.com/chromedp/cdproto/network"
	"github.com/chromedp/chromedp"
)

// fakeCDPSession — сессия CDP для тестов блокировки: помнит команды
// (Fetch.enable/failRequest/continueRequest/disable) и может валить их
// по имени метода. Subscribe не используется — события подаются в насос
// собственным итератором (см. pumpEvents).
type fakeCDPSession struct {
	mu    sync.Mutex
	calls []fakeCDPCall
	fail  map[string]error
	// gate — необязательные шлюзы по методам: Call ждёт закрытия канала
	// (см. комментарий в Call).
	gate map[string]chan struct{}
}

// fakeCDPCall — выполненная команда: метод и параметры как есть, дедлайн
// контекста вызова (нулевой — вызов без дедлайна).
type fakeCDPCall struct {
	Method   string
	Params   any
	Deadline time.Time
}

func (f *fakeCDPSession) Call(ctx context.Context, method string, params, _ any) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}
	f.mu.Lock()
	call := fakeCDPCall{Method: method, Params: params}
	if dl, ok := ctx.Deadline(); ok {
		call.Deadline = dl
	}
	f.calls = append(f.calls, call)
	f.mu.Unlock()
	if err, ok := f.fail[method]; ok {
		return err
	}
	// gate — управляемая блокировка: Call с методом из gate не возвращается,
	// пока шлюз не закроют — безусловно, вопреки отмене контекста: тесту
	// нужна задержка слушателя ровно внутри blockRespond (отмена контекста
	// слушателя и есть штатный путь выхода, его и проверяем).
	if g, ok := f.gate[method]; ok {
		<-g
	}
	return nil
}

func (f *fakeCDPSession) Subscribe(string) (<-chan jsonv2.Value, func()) {
	panic("тесты подают события собственным итератором, не через Subscribe")
}

// calls — копия журнала команд.
func (f *fakeCDPSession) callsOf() []fakeCDPCall {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]fakeCDPCall(nil), f.calls...)
}

// countOf — число команд с данным методом.
func (f *fakeCDPSession) countOf(method string) int {
	n := 0
	for _, c := range f.callsOf() {
		if c.Method == method {
			n++
		}
	}
	return n
}

// waitFor — пока cond не истинна или не истечёт срок (детерминизм
// асинхронного насоса).
func waitFor(t *testing.T, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("условие не наступило за 2 с")
}

// TestBlockResourceType — перечень блокируемых и пропускаемых типов.
// Блокируются ровно Image/Media/Font (ТЗ 5.6); с опцией block_stylesheet
// (T-213) — и Stylesheet; полный список типов протокола перечислен явно:
// новый тип в cdproto по умолчанию проходит (консервативное поведение,
// не ломает загрузку страницы).
func TestBlockResourceType(t *testing.T) {
	all := []network.ResourceType{
		network.ResourceTypeDocument,
		network.ResourceTypeStylesheet,
		network.ResourceTypeImage,
		network.ResourceTypeMedia,
		network.ResourceTypeFont,
		network.ResourceTypeScript,
		network.ResourceTypeTextTrack,
		network.ResourceTypeXHR,
		network.ResourceTypeFetch,
		network.ResourceTypePrefetch,
		network.ResourceTypeEventSource,
		network.ResourceTypeWebSocket,
		network.ResourceTypeManifest,
		network.ResourceTypeSignedExchange,
		network.ResourceTypePing,
		network.ResourceTypeCSPViolationReport,
		network.ResourceTypePreflight,
		network.ResourceTypeFedCM,
		network.ResourceTypeOther,
	}
	baseWant := map[network.ResourceType]bool{
		network.ResourceTypeImage: true,
		network.ResourceTypeMedia: true,
		network.ResourceTypeFont:  true,
	}
	cssWant := maps.Clone(baseWant)
	cssWant[network.ResourceTypeStylesheet] = true
	for name, tc := range map[string]struct {
		types []network.ResourceType
		want  map[network.ResourceType]bool
	}{
		"без опции (T-062)":        {blockTypes(false), baseWant},
		"block_stylesheet (T-213)": {blockTypes(true), cssWant},
	} {
		t.Run(name, func(t *testing.T) {
			for _, rt := range all {
				if got := blockResourceType(tc.types, rt); got != tc.want[rt] {
					t.Errorf("blockResourceType(%s) = %v, хочу %v", rt, got, tc.want[rt])
				}
			}
			// Неизвестный тип — проходит.
			if blockResourceType(tc.types, network.ResourceType("SomethingNew")) {
				t.Error("неизвестный тип должен проходить")
			}
			// Пустой тип (Chrome его не присылает) — проходит.
			if blockResourceType(tc.types, network.ResourceType("")) {
				t.Error("пустой тип должен проходить")
			}
		})
	}
}

// TestBlockTypes — перечень типов сессии (T-213): без опции — базовый
// набор T-062 (Image/Media/Font, Stylesheet проходит), с опцией — плюс
// Stylesheet; копия не затрагивает общий базовый перечень (иначе
// выключенная опция подтянула бы CSS вслед за включённой).
func TestBlockTypes(t *testing.T) {
	base := blockTypes(false)
	if !slices.Equal(base, []network.ResourceType{
		network.ResourceTypeImage, network.ResourceTypeMedia, network.ResourceTypeFont,
	}) {
		t.Fatalf("без опции блокируются %v, хочу Image/Media/Font (T-062)", base)
	}
	css := blockTypes(true)
	if len(css) != len(base)+1 || css[len(css)-1] != network.ResourceTypeStylesheet {
		t.Fatalf("с опцией блокируются %v, хочу базовый набор + Stylesheet (T-213)", css)
	}
	if !slices.Equal(css[:len(base)], base) {
		t.Errorf("с опцией базовая часть %v изменилась, хочу %v", css[:len(base)], base)
	}
	// Копия, а не append в общий слайс: базовый перечень не вырос.
	if len(blockBlockedTypes) != 3 {
		t.Errorf("базовый перечень вырос до %d после blockTypes(true) — общая мутация", len(blockBlockedTypes))
	}
}

// TestBlockPatterns — паттерны Fetch.enable: по одному на блокируемый
// тип, стадия Request (отказ до отправки запроса), без URL-фильтра.
// Доменные паттерны трекеров (T-214) — отдельный TestBlockPatternsTrackers.
func TestBlockPatterns(t *testing.T) {
	for name, types := range map[string][]network.ResourceType{
		"без опции (T-062)":        blockTypes(false),
		"block_stylesheet (T-213)": blockTypes(true),
	} {
		t.Run(name, func(t *testing.T) {
			patterns := blockPatterns(types, false)
			if len(patterns) != len(types) {
				t.Fatalf("паттернов %d, хочу %d (по числу блокируемых типов)",
					len(patterns), len(types))
			}
			seen := make(map[network.ResourceType]bool)
			for _, p := range patterns {
				if !blockResourceType(types, p.ResourceType) {
					t.Errorf("паттерн с незаблокированным типом %s", p.ResourceType)
				}
				if seen[p.ResourceType] {
					t.Errorf("дублирующийся тип паттерна %s", p.ResourceType)
				}
				seen[p.ResourceType] = true
				if p.RequestStage != fetch.RequestStageRequest {
					t.Errorf("стадия паттерна %s: %s, хочу Request",
						p.ResourceType, p.RequestStage)
				}
				if p.URLPattern != "" {
					t.Errorf("URL-фильтр паттерна %s: %q, хочу пусто (только по типу)",
						p.ResourceType, p.URLPattern)
				}
			}
		})
	}
}

// TestBlockPatternsTrackers — доменные паттерны Fetch.enable (T-214):
// при включённой опции по одному на домен списка — urlPattern «домен в
// звёздках» (glob Fetch.enable с полным совпадением: без звёздок
// паттерн совпал бы только с адресом, равным домену), без resourceType
// (аналитика ходит любыми типами), стадия Request; при выключенной —
// доменных паттернов нет вовсе.
func TestBlockPatternsTrackers(t *testing.T) {
	on := blockPatterns(blockTypes(false), true)
	urlPatterns := make(map[string]bool)
	typePatterns := 0
	for _, p := range on {
		if p.URLPattern == "" {
			typePatterns++
			continue
		}
		urlPatterns[p.URLPattern] = true
		if p.ResourceType != "" {
			t.Errorf("доменный паттерн %q с типом %s — типом аналитика не ловится",
				p.URLPattern, p.ResourceType)
		}
		if p.RequestStage != fetch.RequestStageRequest {
			t.Errorf("стадия доменного паттерна %q: %s, хочу Request",
				p.URLPattern, p.RequestStage)
		}
	}
	if typePatterns != len(blockTypes(false)) {
		t.Errorf("типовых паттернов %d, хочу %d (доменные — сверх типов)",
			typePatterns, len(blockTypes(false)))
	}
	if len(urlPatterns) != len(blockTrackerDomains) {
		t.Errorf("доменных паттернов %d, хочу %d (по числу доменов списка)",
			len(urlPatterns), len(blockTrackerDomains))
	}
	for _, d := range blockTrackerDomains {
		if !urlPatterns["*"+d+"*"] {
			t.Errorf("домен %s без паттерна *%s*", d, d)
		}
	}

	for name, types := range map[string][]network.ResourceType{
		"без опции (T-062)":        blockTypes(false),
		"block_stylesheet (T-213)": blockTypes(true),
	} {
		t.Run("выключено: "+name, func(t *testing.T) {
			off := blockPatterns(types, false)
			if len(off) != len(types) {
				t.Fatalf("паттернов %d, хочу %d (только типы)",
					len(off), len(types))
			}
			for _, p := range off {
				if p.URLPattern != "" {
					t.Errorf("при выключенной опции доменный паттерн %q", p.URLPattern)
				}
			}
		})
	}
}

// TestBlockTrackerURL — точная хостовая проверка против списка
// blockTrackerDomains (T-214): хост-домен и его поддомены — трекеры;
// подстрока домена в пути/query/userinfo и трекер-суффикс чужого хоста —
// нет; неразобранный URL — нет (fail-open).
func TestBlockTrackerURL(t *testing.T) {
	cases := []struct {
		name string
		url  string
		want bool
	}{
		{"хост трекера", "https://www.google-analytics.com/g/collect?cid=1", true},
		{"апекс трекера", "https://google-analytics.com/analytics.js", true},
		{"поддомен трекера", "https://cdn4.mixpanel.com/lib.min.js", true},
		{"метрика RU", "https://mc.yandex.ru/metrika/tag.js", true},
		{"метрика международная", "https://mc.yandex.com/watch/12345678", true},
		{"sentry ingest-поддомен", "https://o123456.ingest.sentry.io/api/1/envelope/", true},
		{"путь с подстрокой домена", "https://site.example/js/google-analytics.com.js", false},
		{"query с подстрокой домена", "https://site.example/search?q=doubleclick.net", false},
		{"userinfo с доменом", "https://google-analytics.com@site.example/collect", false},
		{"трекер-суффикс чужого хоста", "https://google-analytics.com.evil.example/ga.js", false},
		{"граница домена (.co ≠ .com)", "https://api.mixpanel.co/track", false},
		{"обычный сайт", "https://laravel.example/js/app.js", false},
		{"неразобранный URL", "://нет-схемы", false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := blockTrackerURL(tc.url); got != tc.want {
				t.Errorf("blockTrackerURL(%q) = %v, хочу %v", tc.url, got, tc.want)
			}
		})
	}
}

// TestBlockTrackerDomainsList — гигиена списка доменов (T-214): каждый
// элемент — нижнерегистровый хост без схемы/подстановочных символов,
// без дублей; список накрывает и апекс, и поддомены каждого домена
// (инвариант пары «широкий паттерн *domain* → хостовая проверка»).
func TestBlockTrackerDomainsList(t *testing.T) {
	if n := len(blockTrackerDomains); n < 12 || n > 16 {
		t.Errorf("в списке %d доменов — задуман диапазон 12–16", n)
	}
	seen := make(map[string]bool, len(blockTrackerDomains))
	for _, d := range blockTrackerDomains {
		if d == "" || d != strings.ToLower(d) || !strings.Contains(d, ".") ||
			strings.ContainsAny(d, "/:?*") {
			t.Errorf("домен %q — не нижнерегистровый хост без схемы и звёздочек", d)
		}
		if seen[d] {
			t.Errorf("дубль домена %q", d)
		}
		seen[d] = true
		if !blockTrackerURL("https://" + d + "/tag.js") {
			t.Errorf("апекс %s не опознаётся списком", d)
		}
		if !blockTrackerURL("https://static." + d + "/tag.js") {
			t.Errorf("поддомен static.%s не опознаётся списком", d)
		}
	}
}

// TestBlockEnable — включение перехвата командой Fetch.enable с паттернами
// по блокируемым типам; ошибка команды возвращается вызывающему.
func TestBlockEnable(t *testing.T) {
	t.Run("команда и параметры", func(t *testing.T) {
		fake := &fakeCDPSession{}
		rules := blockRulesOf(Options{BlockStylesheet: true}) // с опцией: паттернов на единицу больше
		if err := blockEnable(context.Background(), fake, rules); err != nil {
			t.Fatalf("blockEnable: %v", err)
		}
		calls := fake.callsOf()
		if len(calls) != 1 || calls[0].Method != "Fetch.enable" {
			t.Fatalf("команды %v, хочу одну Fetch.enable", calls)
		}
		params, ok := calls[0].Params.(fetch.EnableParams)
		if !ok {
			t.Fatalf("параметры %T, хочу fetch.EnableParams", calls[0].Params)
		}
		if len(params.Patterns) != len(rules.types) {
			t.Fatalf("паттернов %d, хочу %d", len(params.Patterns), len(rules.types))
		}
		for _, p := range params.Patterns {
			if !blockResourceType(rules.types, p.ResourceType) || p.RequestStage != fetch.RequestStageRequest {
				t.Errorf("паттерн %+v вне блокируемого набора", p)
			}
		}
	})

	t.Run("ошибка CDP прокладывается", func(t *testing.T) {
		boom := errors.New("session gone")
		fake := &fakeCDPSession{fail: map[string]error{"Fetch.enable": boom}}
		err := blockEnable(context.Background(), fake, blockRulesOf(Options{}))
		if !errors.Is(err, boom) {
			t.Fatalf("ошибка %v, хочу %v", err, boom)
		}
	})
}

// TestBlockRespond — судьба приостановленного запроса по типу:
// блокируемые — Fetch.failRequest(BlockedByClient) с тем же RequestID,
// остальные — Fetch.continueRequest; Stylesheet блокируется только с
// опцией (T-213); ошибка команды не панике.
func TestBlockRespond(t *testing.T) {
	cases := []struct {
		typ  network.ResourceType
		fail bool
	}{
		{network.ResourceTypeImage, true},
		{network.ResourceTypeMedia, true},
		{network.ResourceTypeFont, true},
		{network.ResourceTypeDocument, false},
		{network.ResourceTypeStylesheet, false},
		{network.ResourceTypeScript, false},
		{network.ResourceTypeXHR, false},
		{network.ResourceTypeFetch, false},
		{network.ResourceTypeWebSocket, false},
		{network.ResourceTypeManifest, false},
		{network.ResourceTypeOther, false},
	}
	for _, tc := range cases {
		name := tc.typ.String() + "→" + map[bool]string{true: "fail", false: "continue"}[tc.fail]
		t.Run(name, func(t *testing.T) {
			fake := &fakeCDPSession{}
			ev := fetch.EventRequestPaused{RequestID: "req-1", ResourceType: tc.typ}
			blockRespond(context.Background(), fake, ev, blockRulesOf(Options{}))

			calls := fake.callsOf()
			if len(calls) != 1 {
				t.Fatalf("команд %d, хочу одну: %v", len(calls), calls)
			}
			want := "Fetch.continueRequest"
			if tc.fail {
				want = "Fetch.failRequest"
			}
			if calls[0].Method != want {
				t.Fatalf("команда %s, хочу %s", calls[0].Method, want)
			}
			if tc.fail {
				params, ok := calls[0].Params.(fetch.FailRequestParams)
				if !ok {
					t.Fatalf("параметры %T, хочу fetch.FailRequestParams", calls[0].Params)
				}
				if params.RequestID != ev.RequestID {
					t.Errorf("RequestID %s, хочу %s", params.RequestID, ev.RequestID)
				}
				if params.ErrorReason != network.ErrorReasonBlockedByClient {
					t.Errorf("ErrorReason %s, хочу BlockedByClient", params.ErrorReason)
				}
			} else {
				params, ok := calls[0].Params.(fetch.ContinueRequestParams)
				if !ok {
					t.Fatalf("параметры %T, хочу fetch.ContinueRequestParams", calls[0].Params)
				}
				if params.RequestID != ev.RequestID {
					t.Errorf("RequestID %s, хочу %s", params.RequestID, ev.RequestID)
				}
			}
		})
	}

	// Stylesheet с опцией block_stylesheet (T-213) — failRequest, как
	// базовые блокируемые типы.
	t.Run("Stylesheet→fail при опции (T-213)", func(t *testing.T) {
		fake := &fakeCDPSession{}
		ev := fetch.EventRequestPaused{RequestID: "css-1", ResourceType: network.ResourceTypeStylesheet}
		blockRespond(context.Background(), fake, ev, blockRulesOf(Options{BlockStylesheet: true}))
		calls := fake.callsOf()
		if len(calls) != 1 || calls[0].Method != "Fetch.failRequest" {
			t.Fatalf("команды %v, хочу одну Fetch.failRequest", calls)
		}
		params, ok := calls[0].Params.(fetch.FailRequestParams)
		if !ok {
			t.Fatalf("параметры %T, хочу fetch.FailRequestParams", calls[0].Params)
		}
		if params.RequestID != ev.RequestID {
			t.Errorf("RequestID %s, хочу %s", params.RequestID, ev.RequestID)
		}
		if params.ErrorReason != network.ErrorReasonBlockedByClient {
			t.Errorf("ErrorReason %s, хочу BlockedByClient", params.ErrorReason)
		}
	})

	// Трекер-домены (T-214): при включённой опции запрос ЛЮБОГО типа к
	// хосту-трекеру — failRequest (аналитика ходит Script/XHR — типом не
	// ловится), приостановленный подстрокой путь с чужим хостом —
	// continueRequest (точность сидит в слушателе, а не в паттерне);
	// при выключенной опции трекер-запрос неблокируемого типа проходит.
	t.Run("трекер-домены (T-214)", func(t *testing.T) {
		on := blockRulesOf(Options{BlockTrackerDomains: true})
		for _, tc := range []struct {
			name string
			ev   fetch.EventRequestPaused
			fail bool
		}{
			{"Script→fail", fetch.EventRequestPaused{
				RequestID: "ga-js", ResourceType: network.ResourceTypeScript,
				Request: &network.Request{URL: "https://www.google-analytics.com/analytics.js"},
			}, true},
			{"XHR→fail", fetch.EventRequestPaused{
				RequestID: "mc-xhr", ResourceType: network.ResourceTypeXHR,
				Request: &network.Request{URL: "https://mc.yandex.ru/watch/12345678"},
			}, true},
			{"Document→fail", fetch.EventRequestPaused{
				RequestID: "dc-doc", ResourceType: network.ResourceTypeDocument,
				Request: &network.Request{URL: "https://stats.g.doubleclick.net/r/collect"},
			}, true},
			{"Ping→fail", fetch.EventRequestPaused{
				RequestID: "cl-ping", ResourceType: network.ResourceTypePing,
				Request: &network.Request{URL: "https://www.clarity.ms/tag/xyz"},
			}, true},
			{"Image трекера→fail (тип и домен)", fetch.EventRequestPaused{
				RequestID: "ga-img", ResourceType: network.ResourceTypeImage,
				Request: &network.Request{URL: "https://www.google-analytics.com/collect.gif"},
			}, true},
			{"Script с доменом в пути→continue", fetch.EventRequestPaused{
				RequestID: "own-js", ResourceType: network.ResourceTypeScript,
				Request: &network.Request{URL: "https://site.example/js/google-analytics.com.js"},
			}, false},
			{"событие без request→continue", fetch.EventRequestPaused{
				RequestID: "anomaly", ResourceType: network.ResourceTypeScript,
			}, false},
		} {
			t.Run(tc.name, func(t *testing.T) {
				fake := &fakeCDPSession{}
				blockRespond(context.Background(), fake, tc.ev, on)
				calls := fake.callsOf()
				want := "Fetch.continueRequest"
				if tc.fail {
					want = "Fetch.failRequest"
				}
				if len(calls) != 1 || calls[0].Method != want {
					t.Fatalf("команды %v, хочу одну %s", calls, want)
				}
				if tc.fail {
					params, ok := calls[0].Params.(fetch.FailRequestParams)
					if !ok {
						t.Fatalf("параметры %T, хочу fetch.FailRequestParams", calls[0].Params)
					}
					if params.RequestID != tc.ev.RequestID {
						t.Errorf("RequestID %s, хочу %s", params.RequestID, tc.ev.RequestID)
					}
					if params.ErrorReason != network.ErrorReasonBlockedByClient {
						t.Errorf("ErrorReason %s, хочу BlockedByClient", params.ErrorReason)
					}
				}
			})
		}

		// Опция выключена — трекер-скрипт проходит: доменные паттерны не
		// только не попадают в Fetch.enable, но и не влияют на вердикты.
		fake := &fakeCDPSession{}
		blockRespond(context.Background(), fake, fetch.EventRequestPaused{
			RequestID: "ga-off", ResourceType: network.ResourceTypeScript,
			Request: &network.Request{URL: "https://www.google-analytics.com/analytics.js"},
		}, blockRulesOf(Options{}))
		if calls := fake.callsOf(); len(calls) != 1 || calls[0].Method != "Fetch.continueRequest" {
			t.Fatalf("команды %v, хочу одну Fetch.continueRequest (опция выключена)", calls)
		}
	})

	t.Run("ошибка команды проглатывается", func(t *testing.T) {
		fake := &fakeCDPSession{fail: map[string]error{"Fetch.failRequest": errors.New("dead")}}
		ev := fetch.EventRequestPaused{RequestID: "r", ResourceType: network.ResourceTypeImage}
		blockRespond(context.Background(), fake, ev, blockRulesOf(Options{})) // не паникует
	})
}

// TestBlockDisable — выключение перехвата: одна команда Fetch.disable под
// собственным коротким таймаутом blockDisableTimeout (обычный вызов CDP —
// миллисекунды, дольше ждать незачем), ошибка best-effort не возвращается.
func TestBlockDisable(t *testing.T) {
	t.Run("команда и таймаут", func(t *testing.T) {
		fake := &fakeCDPSession{}
		blockDisable(context.Background(), fake)
		calls := fake.callsOf()
		if len(calls) != 1 || calls[0].Method != "Fetch.disable" {
			t.Fatalf("команды %v, хочу одну Fetch.disable", calls)
		}
		rem := time.Until(calls[0].Deadline)
		if rem <= 1500*time.Millisecond || rem > 2500*time.Millisecond {
			t.Errorf("таймаут Fetch.disable: осталось %s, хочу ≈2 с (границы 1.5–2.5 с)", rem)
		}
	})

	t.Run("ошибка проглатывается", func(t *testing.T) {
		fake := &fakeCDPSession{fail: map[string]error{"Fetch.disable": errors.New("вкладка умерла")}}
		blockDisable(context.Background(), fake) // не паникует
		if fake.countOf("Fetch.disable") != 1 {
			t.Error("команда Fetch.disable не выполнена")
		}
	})
}

// pumpEvents — итератор событий в духе cdp.Events: значения из канала,
// ошибка при отмене ctx (терминально), конец при закрытии канала. Даёт
// тесту контроль над потоком событий без реальной подписки CDP; ctx
// передаёт сам blockPump (внутренний контекст слушателя).
func pumpEvents(events chan fetch.EventRequestPaused,
) func(context.Context) iter.Seq2[fetch.EventRequestPaused, error] {
	return func(ctx context.Context) iter.Seq2[fetch.EventRequestPaused, error] {
		return func(yield func(fetch.EventRequestPaused, error) bool) {
			for {
				select {
				case <-ctx.Done():
					yield(fetch.EventRequestPaused{}, ctx.Err())
					return
				case ev, ok := <-events:
					if !ok {
						return
					}
					if !yield(ev, nil) {
						return
					}
				}
			}
		}
	}
}

// TestBlockPumpLifecycle — насос событий: отвечает на события обоих
// категорий, завершается по stop без утечки горутины, не трогает события
// после остановки, а при внезапном закрытии канала выключает перехват
// (fail-open).
func TestBlockPumpLifecycle(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	events := make(chan fetch.EventRequestPaused)
	fake := &fakeCDPSession{}
	stop := blockPump(ctx, pumpEvents(events), fake, blockRulesOf(Options{}))

	// События обоих видов получают ответы.
	events <- fetch.EventRequestPaused{RequestID: "img", ResourceType: network.ResourceTypeImage}
	waitFor(t, func() bool { return fake.countOf("Fetch.failRequest") == 1 })
	events <- fetch.EventRequestPaused{RequestID: "js", ResourceType: network.ResourceTypeScript}
	waitFor(t, func() bool { return fake.countOf("Fetch.continueRequest") == 1 })

	// stop возвращает управление — горутина закончилась.
	stopped := make(chan struct{})
	go func() {
		stop()
		close(stopped)
	}()
	select {
	case <-stopped:
	case <-time.After(2 * time.Second):
		t.Fatal("stop не дождался выхода слушателя")
	}

	// После остановки события не обрабатываются.
	before := len(fake.callsOf())
	select {
	case events <- fetch.EventRequestPaused{RequestID: "late", ResourceType: network.ResourceTypeMedia}:
		t.Fatal("событие принято после stop")
	default:
	}
	if got := len(fake.callsOf()); got != before {
		t.Errorf("команд после stop: %d (было %d)", got, before)
	}
	if fake.countOf("Fetch.disable") != 0 {
		t.Error("обычный stop не делает fail-open disable — его делает startBlock")
	}
}

// TestBlockPumpCtxDone — отмена родительского контекста (конец страницы)
// завершает насос без fail-open disable (задача закончилась, сессию
// закрывает пул).
func TestBlockPumpCtxDone(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	events := make(chan fetch.EventRequestPaused)
	fake := &fakeCDPSession{}
	stop := blockPump(ctx, pumpEvents(events), fake, blockRulesOf(Options{}))

	events <- fetch.EventRequestPaused{RequestID: "i", ResourceType: network.ResourceTypeImage}
	waitFor(t, func() bool { return fake.countOf("Fetch.failRequest") == 1 })

	cancel()
	stopped := make(chan struct{})
	go func() {
		stop()
		close(stopped)
	}()
	select {
	case <-stopped:
	case <-time.After(2 * time.Second):
		t.Fatal("насос не вышел по отмене контекста")
	}
	if fake.countOf("Fetch.disable") != 0 {
		t.Error("отмена контекста не должна звать disable (сессию закрывает пул)")
	}
	// Ошибка итератора не проваливается в blockRespond: нулевое событие
	// ошибки не получает ответа (не было бы и лишнего continueRequest).
	if got := fake.countOf("Fetch.continueRequest"); got != 0 {
		t.Errorf("после отмены отвечено на нулевое событие: continueRequest %d, хочу 0", got)
	}
}

// TestBlockPumpEventsClosed — закрытие канала событий при живом контексте
// (вкладка умерла): насос выходит и выключает перехват, чтобы
// блокируемые запросы не зависли до таймаута страницы.
func TestBlockPumpEventsClosed(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	events := make(chan fetch.EventRequestPaused)
	fake := &fakeCDPSession{}
	stop := blockPump(ctx, pumpEvents(events), fake, blockRulesOf(Options{}))

	close(events)
	waitFor(t, func() bool { return fake.countOf("Fetch.disable") == 1 })
	stop() // после выхода насоса возвращает сразу
}

// TestBlockPumpEventError — ошибка потока событий при живом контексте
// (неразбираемое событие — yield ошибки терминален, контекст не отменен):
// насос выходит молча — без ответа на нулевое событие и без fail-open
// disable (жизненный цикл перехвата держит startBlock, а не мусор в
// потоке). Детерминизм — синхронность yield: тело итерации насоса
// отрабатывает ДО возврата из yield, поэтому зонд yieldDone гарантирует,
// что ответ мутанта был бы записан в фейк при ещё живом контексте.
func TestBlockPumpEventError(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	fake := &fakeCDPSession{}
	probe := &errPumpProbe{
		entered:   make(chan struct{}),
		yieldDone: make(chan struct{}),
	}
	stop := blockPump(ctx, probe.events(errors.New("событие не разбирается")), fake, blockRulesOf(Options{}))

	select {
	case <-probe.entered:
	case <-time.After(2 * time.Second):
		t.Fatal("насос не вошёл в итератор событий")
	}
	select {
	case <-probe.yieldDone:
	case <-time.After(2 * time.Second):
		t.Fatal("итерация ошибки не завершена (тело насоса не отработало)")
	}
	stop()
	if calls := fake.callsOf(); len(calls) != 0 {
		t.Fatalf("команды %v, хочу ноль: ошибка потока не отвечает на "+
			"событие и не выключает перехват", calls)
	}
}

// errPumpProbe — итератор в духе cdp.Events: одна терминальная ошибка при
// живом контексте (как decoding-ошибка полезной нагрузки) с зондами
// синхронности — entered при входе в итератор, yieldDone по возврату из
// yield (тело итерации потребителя к этому моменту отработало).
type errPumpProbe struct {
	entered   chan struct{}
	yieldDone chan struct{}
}

func (p *errPumpProbe) events(err error) func(context.Context) iter.Seq2[fetch.EventRequestPaused, error] {
	return func(context.Context) iter.Seq2[fetch.EventRequestPaused, error] {
		return func(yield func(fetch.EventRequestPaused, error) bool) {
			close(p.entered)
			yield(fetch.EventRequestPaused{}, err)
			close(p.yieldDone)
		}
	}
}

// TestBlockPumpRespondError — ошибка ответа не останавливает насос:
// следующие события обрабатываются (fail-open не нужен, страница жива).
func TestBlockPumpRespondError(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	events := make(chan fetch.EventRequestPaused, 2)
	fake := &fakeCDPSession{fail: map[string]error{"Fetch.failRequest": errors.New("boom")}}
	stop := blockPump(ctx, pumpEvents(events), fake, blockRulesOf(Options{}))

	events <- fetch.EventRequestPaused{RequestID: "a", ResourceType: network.ResourceTypeImage}
	events <- fetch.EventRequestPaused{RequestID: "b", ResourceType: network.ResourceTypeImage}
	waitFor(t, func() bool { return len(fake.callsOf()) >= 2 }) // обе попытки

	stop()
}

// TestBlockPumpStopWaitsForListener — stop дожидается выхода слушателя
// (контракт blockPump: «горутина не переживает вызов»): пока насос сидит в
// blockRespond за шлюзом, stop не возвращается; после освобождения —
// возвращается быстро.
func TestBlockPumpStopWaitsForListener(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	events := make(chan fetch.EventRequestPaused)
	gate := make(chan struct{})
	fake := &fakeCDPSession{gate: map[string]chan struct{}{"Fetch.continueRequest": gate}}
	stop := blockPump(ctx, pumpEvents(events), fake, blockRulesOf(Options{}))

	// Слушатель вошёл в blockRespond и ждёт освобождения шлюза: команда
	// записана в журнал до ожидания.
	events <- fetch.EventRequestPaused{RequestID: "gated", ResourceType: network.ResourceTypeScript}
	waitFor(t, func() bool { return len(fake.callsOf()) == 1 })

	stopped := make(chan struct{})
	go func() {
		stop()
		close(stopped)
	}()
	select {
	case <-stopped:
		t.Fatal("stop вернулся до выхода слушателя из blockRespond")
	case <-time.After(100 * time.Millisecond):
	}

	close(gate) // отпустить blockRespond — насос завершается по отмене
	select {
	case <-stopped:
	case <-time.After(2 * time.Second):
		t.Fatal("stop не дождался выхода слушателя после освобождения шлюза")
	}
}

// TestStartBlock — включение блокировки в конвейере сессии: отказы контекста
// вкладки и Fetch.enable, штатный stop (гасит слушателя и выключает
// перехват), расширение паттернов Stylesheet опцией Options.BlockStylesheet
// (T-213) и доменами трекеров опцией Options.BlockTrackerDomains (T-214).
// События перехвата подаются через fakeCDP — in-memory транспорт
// chromedp (cdpfake_test.go), живой браузер не нужен.
func TestStartBlock(t *testing.T) {
	// newSession — сессия с браузером, чьи опции читает startBlock
	// (BlockStylesheet); браузер как таковой не нужен — только opts.
	newSession := func(ctx context.Context, opts Options) *Session {
		return &Session{b: &Browser{opts: opts}, ctx: ctx}
	}

	t.Run("контекст сессии — не chromedp", func(t *testing.T) {
		s := newSession(context.Background(), Options{})
		_, err := s.startBlock(context.Background())
		if err == nil || !strings.Contains(err.Error(), "контекст вкладки не инициализирован") {
			t.Fatalf("ошибка %v, хочу «контекст вкладки не инициализирован»", err)
		}
	})

	t.Run("chromedp-контекст без вкладки", func(t *testing.T) {
		tr := newFakeCDP()
		allocCtx, cancelAlloc := chromedp.NewAllocatorContext(
			context.Background(), &fakeCDPAllocator{tr: tr})
		defer cancelAlloc()
		sctx, cancel := chromedp.NewContext(allocCtx) // без Do: Target не создан
		defer cancel()
		s := newSession(sctx, Options{})
		_, err := s.startBlock(context.Background())
		if err == nil || !strings.Contains(err.Error(), "контекст вкладки не инициализирован") {
			t.Fatalf("ошибка %v, хочу «контекст вкладки не инициализирован»", err)
		}
	})

	t.Run("ошибка Fetch.enable", func(t *testing.T) {
		ctx, tr := newFakeCDPContext(t)
		tr.setError("Fetch.enable", &cdproto.Error{Code: -32000, Message: "домен Fetch сломан"})
		s := newSession(ctx, Options{})
		_, err := s.startBlock(context.Background())
		if err == nil || !strings.Contains(err.Error(), "включение перехвата Fetch") {
			t.Fatalf("ошибка %v, хочу wrap «включение перехвата Fetch»", err)
		}
		if !strings.Contains(err.Error(), "домен Fetch сломан") {
			t.Errorf("исходная ошибка не пробита: %v", err)
		}
		// Слушатель погашен ещё на пути ошибки: события перехвата не
		// получают ответов (мутант вырезания stopListen оставил бы насос
		// живым — событие получило бы failRequest/continueRequest).
		tr.fireSession("Fetch.requestPaused",
			`{"requestId":"late","resourceType":"Image"}`)
		assertNoNewCDPCalls(t, tr, "Fetch.requestPaused-после-ошибки")
	})

	t.Run("stop гасит слушателя и выключает перехват", func(t *testing.T) {
		ctx, tr := newFakeCDPContext(t)
		s := newSession(ctx, Options{})
		stop, err := s.startBlock(context.Background())
		if err != nil {
			t.Fatalf("startBlock: %v", err)
		}
		if tr.countOf("Fetch.enable") != 1 {
			t.Fatalf("Fetch.enable выполнен %d раз, хочу 1", tr.countOf("Fetch.enable"))
		}

		// Событие перехвата получает ответ: слушатель работает.
		tr.fireSession("Fetch.requestPaused",
			`{"requestId":"img-1","resourceType":"Image"}`)
		waitFor(t, func() bool { return tr.countOf("Fetch.failRequest") == 1 })

		stop()
		waitFor(t, func() bool { return tr.countOf("Fetch.disable") == 1 })

		// После stop события не отвечаются (слушатель погашен) и новых
		// команд нет.
		tr.fireSession("Fetch.requestPaused",
			`{"requestId":"late","resourceType":"Media"}`)
		assertNoNewCDPCalls(t, tr, "Fetch.requestPaused-после-stop")
	})

	// Опция BlockStylesheet (T-213): паттерны Fetch.enable пополняются
	// Stylesheet, и приостановленный Stylesheet получает failRequest.
	t.Run("block_stylesheet расширяет перехват (T-213)", func(t *testing.T) {
		ctx, tr := newFakeCDPContext(t)
		s := newSession(ctx, Options{BlockStylesheet: true})
		stop, err := s.startBlock(context.Background())
		if err != nil {
			t.Fatalf("startBlock: %v", err)
		}
		defer stop()

		enable := tr.callsOf()
		n, params := 0, ""
		for _, c := range enable {
			if c.Method == "Fetch.enable" {
				n++
				params = c.Params
			}
		}
		if n != 1 {
			t.Fatalf("Fetch.enable выполнен %d раз, хочу 1", n)
		}
		if !strings.Contains(params, `"Stylesheet"`) {
			t.Errorf("паттерны Fetch.enable без Stylesheet: %s", params)
		}
		tr.fireSession("Fetch.requestPaused",
			`{"requestId":"css-1","resourceType":"Stylesheet"}`)
		waitFor(t, func() bool { return tr.countOf("Fetch.failRequest") == 1 })
	})

	// Опция BlockTrackerDomains (T-214): паттерны Fetch.enable пополняются
	// доменными, приостановленный Script к трекеру — failRequest, Script
	// с доменом трекера в пути чужого хоста — continueRequest.
	t.Run("block_tracker_domains расширяет перехват (T-214)", func(t *testing.T) {
		ctx, tr := newFakeCDPContext(t)
		s := newSession(ctx, Options{BlockTrackerDomains: true})
		stop, err := s.startBlock(context.Background())
		if err != nil {
			t.Fatalf("startBlock: %v", err)
		}
		defer stop()

		enable := tr.callsOf()
		n, params := 0, ""
		for _, c := range enable {
			if c.Method == "Fetch.enable" {
				n++
				params = c.Params
			}
		}
		if n != 1 {
			t.Fatalf("Fetch.enable выполнен %d раз, хочу 1", n)
		}
		if !strings.Contains(params, `"*google-analytics.com*"`) {
			t.Errorf("паттерны Fetch.enable без домена трекера: %s", params)
		}
		if !strings.Contains(params, `"*mc.yandex.ru*"`) {
			t.Errorf("паттерны Fetch.enable без mc.yandex.ru: %s", params)
		}

		tr.fireSession("Fetch.requestPaused",
			`{"requestId":"ga-1","resourceType":"Script",`+
				`"request":{"url":"https://www.google-analytics.com/analytics.js"}}`)
		waitFor(t, func() bool { return tr.countOf("Fetch.failRequest") == 1 })

		tr.fireSession("Fetch.requestPaused",
			`{"requestId":"own-1","resourceType":"Script",`+
				`"request":{"url":"https://site.example/js/google-analytics.com.js"}}`)
		waitFor(t, func() bool { return tr.countOf("Fetch.continueRequest") == 1 })
	})

	// Без опции доменных паттернов нет вовсе — Fetch.enable несёт только
	// типовые (T-062/T-213).
	t.Run("без block_tracker_domains доменных паттернов нет (T-214)", func(t *testing.T) {
		ctx, tr := newFakeCDPContext(t)
		s := newSession(ctx, Options{})
		stop, err := s.startBlock(context.Background())
		if err != nil {
			t.Fatalf("startBlock: %v", err)
		}
		defer stop()

		for _, c := range tr.callsOf() {
			if c.Method != "Fetch.enable" {
				continue
			}
			if strings.Contains(c.Params, "urlPattern") {
				t.Errorf("паттерны Fetch.enable содержат urlPattern: %s", c.Params)
			}
		}
	})
}

// assertNoNewCDPCalls — негативная проверка: с момента вызова новые команды
// через транспорт не проходят (окно 100 мс — порядок запаса против
// микросекунд цепочки доставки; в стиле пакета — негативные паузы
// TestWaitLoopStaleRequestsDrained).
func assertNoNewCDPCalls(t *testing.T, tr *fakeCDP, what string) {
	t.Helper()
	before := len(tr.callsOf())
	deadline := time.Now().Add(100 * time.Millisecond)
	for time.Now().Before(deadline) && len(tr.callsOf()) == before {
		time.Sleep(time.Millisecond)
	}
	if got := len(tr.callsOf()); got != before {
		t.Errorf("%s: прошли новые команды %+v", what, tr.callsOf()[before:])
	}
}
