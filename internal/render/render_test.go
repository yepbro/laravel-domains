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

// TestMain глушит служебный лог пакета (Warn пула при повторах) в выводе
// go test.
func TestMain(m *testing.M) {
	slog.SetDefault(slog.New(slog.NewTextHandler(io.Discard, nil)))
	os.Exit(m.Run())
}

// testBrowser — браузер без запуска процесса: только опции и счётчик
// страниц (юнит-тесты пула и классификации; Close/Alive безопасны на нулях).
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
		MaxDOMBytes:         cfg.Content.MaxStoreBytes.B(), // R-02: потолок DOM — лимит хранилища
		BlockStylesheet:     true,
		BlockTrackerDomains: true,
	}
	// LookupHost — шов тестов (T-230): сборка из конфигурации оставляет
	// nil (перехват берёт системный резолвер); Options с полем-функцией
	// не сравнима оператором ==, поэтому DeepEqual.
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("NewOptions:\n got %+v\nwant %+v", got, want)
	}
	if got.LookupHost != nil {
		t.Errorf("NewOptions заполнил LookupHost из конфигурации: %p, хочу nil (дефолт — системный резолвер)", got.LookupHost)
	}
}

// TestNewOptionsEngine — проводка движка рендера (ТЗР движков §4.2,
// ФТ-4): render.engine переносится в Options как есть, без нормализации
// («» = chromium — семантика валидации, не сборки); дальше движок читают
// только validate и remote-аллокатор.
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
	// Прочие поля — как заданы: скрытые дефолты не маскируют конфигурацию.
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
	// RemoteURL не обязателен: пусто — локальный режим.
	local := valid
	local.RemoteURL = ""
	if err := local.validate(); err != nil {
		t.Fatalf("локальный режим отклонён: %v", err)
	}

	// Булевы опции без диапазона: оба значения легальны — для
	// block_stylesheet (T-213) включение — вопрос A/B-решения, для
	// block_tracker_domains (T-214) — вопрос экономии, не корректности.
	for _, b := range []bool{false, true} {
		opt := valid
		opt.BlockStylesheet = b
		opt.BlockTrackerDomains = b
		if err := opt.validate(); err != nil {
			t.Errorf("block_stylesheet/block_tracker_domains = %v отклонены: %v", b, err)
		}
	}

	// Разрешённые схемы remote_url: ws/wss/http/https — вырезание любой
	// из проверок принимало бы её схему за неразрешённую.
	for _, scheme := range []string{"ws", "wss", "http", "https"} {
		remote := valid
		remote.RemoteURL = scheme + "://chromium:9222"
		if err := remote.validate(); err != nil {
			t.Errorf("remote_url %s:// отклонён: %v", scheme, err)
		}
	}

	// Границы contexts: ровно 1 и ровно 16.
	for _, n := range []int{1, 16} {
		bound := valid
		bound.Contexts = n
		if err := bound.validate(); err != nil {
			t.Errorf("contexts = %d отклонён: %v", n, err)
		}
	}

	// Граница timeout: 1 нс — положительная длительность.
	nano := valid
	nano.Timeout = 1
	if err := nano.validate(); err != nil {
		t.Errorf("timeout = 1 нс отклонён: %v", err)
	}

	// Границы порога памяти: ровно минимум (256 MiB) и ровно максимум
	// (16 GiB) — контроль включён, но ещё в допустимом диапазоне.
	for _, threshold := range []int64{256 << 20, 16 << 30} {
		bound := valid
		bound.MemoryThreshold = threshold
		if err := bound.validate(); err != nil {
			t.Errorf("memory_threshold = %d отклонён: %v", threshold, err)
		}
	}

	// Движок рендера (ТЗР движков §4.2, ФТ-1/3): chromium и "" допускают
	// оба режима подключения; moli|lightpanda — только внешний CDP, для
	// них обязателен remote_url и выключен порог памяти (RSS чужого
	// процесса недоступен). Базовые valid/local выше — движок "".
	for _, engine := range []string{config.EngineChromium, config.EngineMoli, config.EngineLightpanda} {
		named := local
		named.Engine = engine
		if engine != config.EngineChromium {
			named.RemoteURL = valid.RemoteURL // альтернативам нужен внешний CDP
		}
		if err := named.validate(); err != nil {
			t.Errorf("движок %s отклонён: %v", engine, err)
		}
	}
	// Связка «движок × remote_url × memory_threshold» (ФТ-3): движковые
	// ошибки называют поле и движок.
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
	// Схема remote_url альтернативных движков — только ws/wss (P2
	// ревью T-234): адрес конфигурации (lightpanda) или его authority
	// (moli) идут в WebSocket-диал напрямую, http(s) жив лишь
	// уточнением у chromium.
	for _, engine := range []string{config.EngineMoli, config.EngineLightpanda} {
		badScheme := valid
		badScheme.Engine = engine
		badScheme.RemoteURL = "http://engine:9222"
		err := badScheme.validate()
		if err == nil || !strings.Contains(err.Error(), "remote_url: для движка "+engine+" ожидается ws://") {
			t.Errorf("движок %s с http-адресом: хочу ошибку схемы, получено %v", engine, err)
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
	// Текст ошибки порога памяти пинит границы диапазона литералами:
	// 256<<20 = 268435456, 16<<30 = 17179869184 (мутанты сдвигов и ±1
	// аргументов формата меняли бы числа в сообщении).
	low := valid
	low.MemoryThreshold = 1 << 20
	err := low.validate()
	if err == nil || !strings.Contains(err.Error(),
		"memory_threshold: ожидается 0 (отключено) или 268435456..17179869184 байт, задано 1048576") {
		t.Fatalf("текст ошибки порога памяти: %v", err)
	}
	// 0 — контроль памяти отключён (remote-режим и малые машины), не ошибка.
	off := Options{Contexts: 2, Timeout: time.Second, RestartEveryPages: 1}
	if err := off.validate(); err != nil {
		t.Fatalf("нулевой порог памяти должен быть допустим: %v", err)
	}
}

// TestValidateTaskURL — схема задачи рендера (R-09): открываются только
// http/https; file:/data: браузер принял бы, а съём DOM унёс бы содержимое
// локального ресурса в хранилище контента.
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

// TestSessionRunBlocksInternalTaskURL — SSRF-контур рендера (T-219): при
// http.block_internal_addrs задача с внутренним хостом (включая
// IPv4-в-IPv6 и числоподобные формы WHATWG) отвергается классом task в
// начале Run — до перехода на страницу (сессия собрана из одних опций,
// браузер не нужен; создание CDP-контекста NewSession эта проверка не
// отменяет — см. ревью T-219). Внешний литерал и флаг-выключено
// проходят адресный контроль (отказ, если он случится, — про браузер,
// не про контур).
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
		// Loopback-имена и числоподобные формы WHATWG: netip их не
		// разбирает, Chromium канонизирует в 127.0.0.1 (T-219, P1
		// ревью) — ловит renderHostInternal.
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
	// Внешние литералы (TEST-NET-3, 2001:db8::/32), «числоподобное, но
	// имя» (последняя метка не число) и имя-хост контур не задевают: Run
	// уходит дальше валидации и падает на отсутствии chromedp-контекста
	// вкладки, не на адресе.
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
	// Флаг выключен — внутренний литерал не блокируется (dev/testbed):
	// отказ, если он случится, — про браузер, не про контур.
	s := newSession(Options{Timeout: time.Second})
	if _, err := s.Run(context.Background(), RenderTask{URL: "http://127.0.0.1/"}); err != nil &&
		strings.Contains(err.Error(), "внутренн") {
		t.Errorf("без флага внутренний адрес не блокируется: %v", err)
	}
}

// TestRenderHostInternal — единая проверка «хост внутренний» SSRF-контура
// рендера (T-219): канонические литералы (включая IPv4-в-IPv6),
// loopback-имена и числоподобные формы WHATWG — внутренние; внешние
// литералы, обычные и «числоподобные, но имена» — нет.
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
		{"::ffff:0.0.0.0", true}, // IsUnspecified без Unmap 4-in-6 не разворачивает
		{"localhost", true},
		{"LOCALHOST", true},
		{"sub.localhost", true},
		{"127.1", true},
		{"2130706433", true},
		{"0x7f.1", true},
		{"0177.0.0.1", true},
		{"0", true},
		{"127.0.0.1.", true}, // хвостовая точка — WHATWG срезает
		{"203.0.113.7", false},
		{"2001:db8::1", false},
		{"example.net", false},
		{"0.com", false}, // последняя метка не число — имя
		{"127.example.net", false},
		{"1.2.3.4", false},
		{"not a host", false},
		{"256.1.1.1", false}, // >255 в не-последней части — имя для WHATWG
	}
	for _, tc := range cases {
		if got := renderHostInternal(tc.host); got != tc.want {
			t.Errorf("renderHostInternal(%q) = %v, хочу %v", tc.host, got, tc.want)
		}
	}
}

// TestWhatwgParseIPv4 — канонизация числоподобных хостов по алгоритму
// WHATWG (поведение Chromium): формы записей 127.0.0.1, пороги частей
// (не-последние ≤ 255, последняя < 256^(5−n)), «не заканчивается
// числом» — имя.
func TestWhatwgParseIPv4(t *testing.T) {
	cases := []struct {
		host string
		want string // "" — не числоподобный
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
		{"0x1", "0.0.0.1"},
		{"010", "0.0.0.8"},
		{"0x", "0.0.0.0"},
		{"example.com", ""},
		{"0.com", ""},
		{"1.2.3.4.5", ""},
		{"256.1.1.1", ""},
		{"1.2.3.256", ""},
		{"1.2.3.4.", ""}, // хвостовая точка срезается до whatwgParseIPv4
		{"", ""},
		{"..", ""},
		{"0xg", ""},
		{"09", ""}, // 9 — не восьмеричная цифра
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
