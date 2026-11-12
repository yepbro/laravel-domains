package render

import (
	"context"
	"io"
	"log/slog"
	"os"
	"reflect"
	"strings"
	"testing"
	"time"

	"crawler/internal/config"
)

// TestMain silences the package's service log (the pool Warn on
// retries) in the go test output.
func TestMain(m *testing.M) {
	slog.SetDefault(slog.New(slog.NewTextHandler(io.Discard, nil)))
	os.Exit(m.Run())
}

// testBrowser — a browser without starting a process: only options and
// the page counter (the pool and classification unit tests; Close/Alive
// are safe on zeros).
func testBrowser(contexts int) *Browser {
	return &Browser{opts: Options{
		Contexts:            contexts,
		Timeout:             5 * time.Second,
		RestartEveryPages:   100,
		SessionStartTimeout: time.Second,
	}.withDefaults()}
}

func TestNewOptions(t *testing.T) {
	cfg := config.Default()
	cfg.Render = config.Render{
		RemoteURL:           "ws://chromium:9222",
		Contexts:            3,
		Timeout:             config.Duration(25 * time.Second),
		RestartEveryPages:   500,
		RestartMemoryBytes:  config.ByteSize(1536 << 20),
		BlockStylesheet:     true,
		BlockTrackerDomains: true,
	}
	cfg.HTTP.UserAgent = "Crawler/1.0 (+https://example.example; admin@example.example)"

	got := NewOptions(*cfg)
	want := Options{
		RemoteURL:           "ws://chromium:9222",
		Contexts:            3,
		Timeout:             25 * time.Second,
		RestartEveryPages:   500,
		MemoryThreshold:     1536 << 20,
		UserAgent:           "Crawler/1.0 (+https://example.example; admin@example.example)",
		MaxDOMBytes:         cfg.Content.MaxStoreBytes.B(), // R-02: the DOM cap — the store limit
		BlockStylesheet:     true,
		BlockTrackerDomains: true,
	}
	// LookupHost — the tests' seam (T-230): a build from the
	// configuration leaves it nil (the interception takes the system
	// resolver); Options with a function field is not comparable with
	// ==, hence DeepEqual.
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("NewOptions:\n got %+v\nwant %+v", got, want)
	}
	if got.LookupHost != nil {
		t.Errorf("NewOptions заполнил LookupHost из конфигурации: %p, хочу nil (дефолт — системный резолвер)", got.LookupHost)
	}
}

// TestNewOptionsEngine — the render engine wiring (the engines render
// spec §4.2, FR-4): render.engine is carried into Options as is, without
// normalization ("" = chromium — the semantics of validation, not of
// assembly); further on, only validate and the remote allocator read
// the engine.
func TestNewOptionsEngine(t *testing.T) {
	for _, engine := range []string{"", config.EngineChromium, config.EngineMoli, config.EngineLightpanda} {
		cfg := config.Default()
		cfg.Render.Engine = engine
		cfg.Render.RemoteURL = "ws://engine:9222"
		if got := NewOptions(*cfg).Engine; got != engine {
			t.Errorf("render.engine %q: NewOptions перенёс %q, хочу как есть", engine, got)
		}
	}
}

func TestOptionsWithDefaults(t *testing.T) {
	got := Options{}.withDefaults()
	if got.DialTimeout != 10*time.Second {
		t.Errorf("DialTimeout = %s, хочу 10s", got.DialTimeout)
	}
	if got.SessionStartTimeout != 15*time.Second {
		t.Errorf("SessionStartTimeout = %s, хочу 15s", got.SessionStartTimeout)
	}
	// The other fields — as set: hidden defaults do not mask the
	// configuration.
	if got.Contexts != 0 || got.Timeout != 0 {
		t.Errorf("withDefaults затёр пустые поля: %+v", got)
	}
}

func TestOptionsValidate(t *testing.T) {
	valid := Options{
		RemoteURL:         "ws://chromium:9222",
		Contexts:          2,
		Timeout:           25 * time.Second,
		RestartEveryPages: 500,
	}
	if err := valid.validate(); err != nil {
		t.Fatalf("валидные опции отклонены: %v", err)
	}
	// RemoteURL is not mandatory: empty — the local mode.
	local := valid
	local.RemoteURL = ""
	if err := local.validate(); err != nil {
		t.Fatalf("локальный режим отклонён: %v", err)
	}

	// Boolean options without a range: both values are legal — for
	// block_stylesheet (T-213) enabling is an A/B decision, for
	// block_tracker_domains (T-214) — a matter of savings, not
	// correctness.
	for _, b := range []bool{false, true} {
		opt := valid
		opt.BlockStylesheet = b
		opt.BlockTrackerDomains = b
		if err := opt.validate(); err != nil {
			t.Errorf("block_stylesheet/block_tracker_domains = %v отклонены: %v", b, err)
		}
	}

	// The allowed remote_url schemes: ws/wss/http/https — cutting any
	// of the checks out would treat its scheme as disallowed.
	for _, scheme := range []string{"ws", "wss", "http", "https"} {
		remote := valid
		remote.RemoteURL = scheme + "://chromium:9222"
		if err := remote.validate(); err != nil {
			t.Errorf("remote_url %s:// отклонён: %v", scheme, err)
		}
	}

	// The contexts boundaries: exactly 1 and exactly 16.
	for _, n := range []int{1, 16} {
		bound := valid
		bound.Contexts = n
		if err := bound.validate(); err != nil {
			t.Errorf("contexts = %d отклонён: %v", n, err)
		}
	}

	// The timeout boundary: 1 ns — a positive duration.
	nano := valid
	nano.Timeout = 1
	if err := nano.validate(); err != nil {
		t.Errorf("timeout = 1 нс отклонён: %v", err)
	}

	// The memory threshold boundaries: exactly the minimum (256 MiB)
	// and exactly the maximum (16 GiB) — the control is on but still in
	// the allowed range.
	for _, threshold := range []int64{256 << 20, 16 << 30} {
		bound := valid
		bound.MemoryThreshold = threshold
		if err := bound.validate(); err != nil {
			t.Errorf("memory_threshold = %d отклонён: %v", threshold, err)
		}
	}

	// The render engine (the engines render spec §4.2, FR-1/3): chromium
	// and "" allow both connection modes; moli|lightpanda — external CDP
	// only, remote_url is mandatory for them and the memory threshold is
	// off (the RSS of a foreign process is unavailable). The base
	// valid/local above are engine "".
	for _, engine := range []string{config.EngineChromium, config.EngineMoli, config.EngineLightpanda} {
		named := local
		named.Engine = engine
		if engine != config.EngineChromium {
			named.RemoteURL = valid.RemoteURL // the alternatives need an external CDP
		}
		if err := named.validate(); err != nil {
			t.Errorf("движок %s отклонён: %v", engine, err)
		}
	}
	// The combination "engine × remote_url × memory_threshold" (FR-3):
	// the engine errors name the field and the engine.
	for _, engine := range []string{config.EngineMoli, config.EngineLightpanda} {
		noRemote := valid
		noRemote.Engine = engine
		noRemote.RemoteURL = ""
		err := noRemote.validate()
		if err == nil || !strings.Contains(err.Error(), "engine: для "+engine+" обязателен remote_url") {
			t.Errorf("движок %s без remote_url: хочу ошибку связки, получено %v", engine, err)
		}
		withThreshold := valid
		withThreshold.Engine = engine
		withThreshold.MemoryThreshold = 256 << 20
		err = withThreshold.validate()
		if err == nil || !strings.Contains(err.Error(), "memory_threshold: для движка "+engine+" ожидается 0") {
			t.Errorf("движок %s с порогом памяти: хочу ошибку связки, получено %v", engine, err)
		}
	}
	// The remote_url scheme of the alternative engines — ws/wss only
	// (the P2 of the T-234 review): the configuration address
	// (lightpanda) or its authority (moli) go straight into the
	// WebSocket dial; http(s) is alive only through the chromium
	// refinement.
	for _, engine := range []string{config.EngineMoli, config.EngineLightpanda} {
		badScheme := valid
		badScheme.Engine = engine
		badScheme.RemoteURL = "http://engine:9222"
		err := badScheme.validate()
		if err == nil || !strings.Contains(err.Error(), "remote_url: для движка "+engine+" ожидается ws://") {
			t.Errorf("движок %s с http-адресом: хочу ошибку схемы, получено %v", engine, err)
		}
		// An empty host — the error of the "engine × remote_url"
		// combination, not only of the general host check below: the
		// alternative engines' allocator dials the configuration address
		// (lightpanda) or its authority (moli) directly; an authority is
		// mandatory (render spec §4.2).
		emptyHost := valid
		emptyHost.Engine = engine
		emptyHost.RemoteURL = "ws://"
		err = emptyHost.validate()
		if err == nil || !strings.Contains(err.Error(), "remote_url: для движка "+engine+" ожидается ws://") {
			t.Errorf("движок %s с ws:// без хоста: хочу ошибку связки, получено %v", engine, err)
		}
		wss := valid
		wss.Engine = engine
		wss.RemoteURL = "wss://engine:9222"
		if err := wss.validate(); err != nil {
			t.Errorf("движок %s с wss-адресом отклонён: %v", engine, err)
		}
	}

	cases := []struct {
		name  string
		opts  Options
		field string
	}{
		{"движок неизвестен", Options{Engine: "webkit", RemoteURL: "ws://x:9222", Contexts: 2, Timeout: time.Second, RestartEveryPages: 1}, "engine"},
		{"схема remote_url", Options{RemoteURL: "ftp://x:9222", Contexts: 2, Timeout: time.Second, RestartEveryPages: 1}, "remote_url"},
		{"remote_url без хоста", Options{RemoteURL: "ws://", Contexts: 2, Timeout: time.Second, RestartEveryPages: 1}, "remote_url"},
		{"contexts 0", Options{Contexts: 0, Timeout: time.Second, RestartEveryPages: 1}, "contexts"},
		{"contexts 17", Options{Contexts: 17, Timeout: time.Second, RestartEveryPages: 1}, "contexts"},
		{"timeout 0", Options{Contexts: 2, Timeout: 0, RestartEveryPages: 1}, "timeout"},
		{"timeout отрицательный", Options{Contexts: 2, Timeout: -time.Second, RestartEveryPages: 1}, "timeout"},
		{"restart_every_pages 0", Options{Contexts: 2, Timeout: time.Second, RestartEveryPages: 0}, "restart_every_pages"},
		{"memory_threshold отрицательный", Options{Contexts: 2, Timeout: time.Second, RestartEveryPages: 1, MemoryThreshold: -1}, "memory_threshold"},
		{"memory_threshold ниже минимума", Options{Contexts: 2, Timeout: time.Second, RestartEveryPages: 1, MemoryThreshold: 1 << 20}, "memory_threshold"},
		{"memory_threshold ниже минимума на границу", Options{Contexts: 2, Timeout: time.Second, RestartEveryPages: 1, MemoryThreshold: 255 << 20}, "memory_threshold"},
		{"memory_threshold выше максимума", Options{Contexts: 2, Timeout: time.Second, RestartEveryPages: 1, MemoryThreshold: 17 << 30}, "memory_threshold"},
		{"max_dom_bytes отрицательный", Options{Contexts: 2, Timeout: time.Second, RestartEveryPages: 1, MaxDOMBytes: -1}, "max_dom_bytes"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := tc.opts.validate()
			if err == nil {
				t.Fatalf("ожидана ошибка, её нет")
			}
			if !strings.Contains(err.Error(), tc.field) {
				t.Fatalf("ошибка не называет поле %s: %v", tc.field, err)
			}
		})
	}
	// The memory threshold error text pins the range boundaries with
	// literals: 256<<20 = 268435456, 16<<30 = 17179869184 (the shift
	// mutants and the ±1 of the format arguments would change the
	// numbers in the message).
	low := valid
	low.MemoryThreshold = 1 << 20
	err := low.validate()
	if err == nil || !strings.Contains(err.Error(),
		"memory_threshold: ожидается 0 (отключено) или 268435456..17179869184 байт, задано 1048576") {
		t.Fatalf("текст ошибки порога памяти: %v", err)
	}
	// 0 — the memory control is off (the remote mode and small
	// machines), not an error.
	off := Options{Contexts: 2, Timeout: time.Second, RestartEveryPages: 1}
	if err := off.validate(); err != nil {
		t.Fatalf("нулевой порог памяти должен быть допустим: %v", err)
	}
}

// TestValidateTaskURL — the render task scheme (R-09): only http/https
// are opened; the browser would accept file:/data:, and the DOM capture
// would carry the contents of a local resource into the content store.
func TestValidateTaskURL(t *testing.T) {
	for _, u := range []string{
		"http://example.com/", "https://example.com/deep/path?q=1",
		"https://spa.example:8443/",
	} {
		if err := validateTaskURL(u); err != nil {
			t.Errorf("%s: неожиданная ошибка: %v", u, err)
		}
	}
	for _, u := range []string{
		"file:///etc/passwd", "data:text/html,<html>", "ftp://example.com/",
		"chrome://version", "about:blank", "", "not a url", "/relative/path",
		"http://", "https://",
	} {
		err := validateTaskURL(u)
		if err == nil {
			t.Errorf("%s: ожидалась ошибка схемы", u)
			continue
		}
		if ClassOf(err) != ClassTask {
			t.Errorf("%s: класс %v, хочу task (повторная доставка уровня очереди)", u, ClassOf(err))
		}
	}
}

// TestSessionRunBlocksInternalTaskURL — the render SSRF guard (T-219):
// with http.block_internal_addrs on, a task with an internal host
// (including IPv4-in-IPv6 and the WHATWG numeric-like forms) is
// rejected with the task class at the start of Run — before navigating
// to the page (the session is assembled from options alone, no browser
// needed; this check does not cancel the CDP context creation of
// NewSession — see the T-219 review). An external literal and the flag
// off pass the address check (a failure, if it happens, is about the
// browser, not the guard).
func TestSessionRunBlocksInternalTaskURL(t *testing.T) {
	newSession := func(opts Options) *Session {
		return &Session{b: &Browser{opts: opts}, ctx: context.Background()}
	}
	for _, u := range []string{
		"http://127.0.0.1/",
		"http://10.0.0.5/",
		"http://169.254.169.254/latest/meta-data/",
		"http://[::1]/",
		"http://[::ffff:10.0.0.1]/x",
		"http://[::ffff:169.254.169.254]/latest/meta-data/",
		// Loopback names and the WHATWG numeric-like forms: netip does
		// not parse them, Chromium canonicalizes them into 127.0.0.1
		// (T-219, the P1 of the review) — renderHostInternal catches
		// them.
		"http://localhost/", "http://sub.localhost:8080/",
		"http://127.1/", "http://2130706433/", "http://0x7f.1/", "http://0177.0.0.1/",
	} {
		s := newSession(Options{Timeout: time.Second, BlockInternalAddrs: true})
		_, err := s.Run(context.Background(), RenderTask{URL: u})
		if err == nil {
			t.Errorf("%s: ожидался отказ задачи с внутренним хостом", u)
			continue
		}
		if ClassOf(err) != ClassTask {
			t.Errorf("%s: класс %v, хочу task (повторная доставка уровня очереди)", u, ClassOf(err))
		}
	}
	// External literals (TEST-NET-3, 2001:db8::/32), "numeric-like but
	// a name" (the last label is not a number) and a hostname do not
	// touch the guard: Run goes past the validation and fails on the
	// missing chromedp context of the tab, not on the address.
	for _, u := range []string{
		"http://203.0.113.7/", "http://[2001:db8::1]/", "http://ssrf.example.net/",
		"http://0.com/", "http://127.example.net/",
	} {
		s := newSession(Options{Timeout: time.Second, BlockInternalAddrs: true})
		_, err := s.Run(context.Background(), RenderTask{URL: u})
		if err != nil && strings.Contains(err.Error(), "внутренн") {
			t.Errorf("%s: внешний адрес ошибочно заблокирован: %v", u, err)
		}
	}
	// The flag is off — an internal literal is not blocked (dev/testbed):
	// a failure, if it happens, is about the browser, not the guard.
	s := newSession(Options{Timeout: time.Second})
	if _, err := s.Run(context.Background(), RenderTask{URL: "http://127.0.0.1/"}); err != nil &&
		strings.Contains(err.Error(), "внутренн") {
		t.Errorf("без флага внутренний адрес не блокируется: %v", err)
	}
}

// TestRenderHostInternal — the unified "is the host internal" check of
// the render SSRF guard (T-219): canonical literals (including
// IPv4-in-IPv6), loopback names and the WHATWG numeric-like forms are
// internal; external literals, regular names and "numeric-like but
// names" are not.
func TestRenderHostInternal(t *testing.T) {
	cases := []struct {
		host string
		want bool
	}{
		{"127.0.0.1", true},
		{"10.0.0.5", true},
		{"192.168.1.1", true},
		{"169.254.169.254", true},
		{"::1", true},
		{"fd00::1", true},
		{"::ffff:127.0.0.1", true},
		{"::ffff:169.254.169.254", true},
		{"::ffff:0.0.0.0", true}, // IsUnspecified without Unmap does not unfold 4-in-6
		{"localhost", true},
		{"LOCALHOST", true},
		{"sub.localhost", true},
		{"127.1", true},
		{"2130706433", true},
		{"0x7f.1", true},
		{"0177.0.0.1", true},
		{"0", true},
		{"127.0.0.1.", true}, // the trailing dot — WHATWG cuts it
		{"203.0.113.7", false},
		{"2001:db8::1", false},
		{"example.net", false},
		{"0.com", false}, // the last label is not a number — a name
		{"127.example.net", false},
		{"1.2.3.4", false},
		{"not a host", false},
		{"256.1.1.1", false}, // >255 in a non-last part — a name for WHATWG
	}
	for _, tc := range cases {
		if got := renderHostInternal(tc.host); got != tc.want {
			t.Errorf("renderHostInternal(%q) = %v, хочу %v", tc.host, got, tc.want)
		}
	}
}

// TestWhatwgParseIPv4 — the canonicalization of numeric-like hosts by
// the WHATWG algorithm (the Chromium behavior): the entry forms of
// 127.0.0.1, the part thresholds (non-last ≤ 255, the last <
// 256^(5−n)), "does not end with a number" — a name.
func TestWhatwgParseIPv4(t *testing.T) {
	cases := []struct {
		host string
		want string // "" — not numeric-like
	}{
		{"127.0.0.1", "127.0.0.1"},
		{"127.1", "127.0.0.1"},
		{"2130706433", "127.0.0.1"},
		{"0x7f.0.0.1", "127.0.0.1"},
		{"0177.0.0.1", "127.0.0.1"},
		{"0x7f.1", "127.0.0.1"},
		{"0", "0.0.0.0"},
		{"1", "0.0.0.1"},
		{"1.2.3", "1.2.0.3"},
		// The full 4-part form with a non-zero third octet: pins the
		// address assembly from the accumulated v (the high bytes —
		// shifts).
		{"1.2.3.4", "1.2.3.4"},
		// 255 in a non-last part is legal (the "strictly greater than
		// 255" threshold rejects 256+, not 255).
		{"255.1.1.1", "255.1.1.1"},
		{"0x1", "0.0.0.1"},
		{"010", "0.0.0.8"},
		{"0x", "0.0.0.0"},
		{"example.com", ""},
		{"0.com", ""},
		// A number only in the last part, an earlier one not a number:
		// rejected by the part parsing in the loop (the guard looks only
		// at the last one).
		{"a.1", ""},
		// Five parts with a numeric tail: rejected by the "> 4 parts"
		// limit itself (a tail of 0 would pass the last-part
		// threshold).
		{"1.2.3.4.0", ""},
		{"1.2.3.4.5", ""},
		{"256.1.1.1", ""},
		{"1.2.3.256", ""},
		{"1.2.3.4.", ""}, // the trailing dot is cut before whatwgParseIPv4
		{"", ""},
		{"..", ""},
		{"0xg", ""},
		{"09", ""}, // 9 is not an octal digit
	}
	for _, tc := range cases {
		ip, ok := whatwgParseIPv4(tc.host)
		if tc.want == "" {
			if ok {
				t.Errorf("whatwgParseIPv4(%q) = %v, хочу не-числоподобный", tc.host, ip)
			}
			continue
		}
		if !ok || ip.String() != tc.want {
			t.Errorf("whatwgParseIPv4(%q) = %v %v, хочу %s", tc.host, ip, ok, tc.want)
		}
	}
}
