// Сквозной корпус движка правил — задача T-075 (ТЗ §11.1: «движок правил:
// score, пересечение диапазонов версий, конфликты; прогон по корпусу
// реальных образцов Laravel заказчика (/testdata/laravel-samples, с
// ожидаемыми is_laravel и диапазонами версий) и по негативным образцам»;
// критерии приёмки §11.4 пп. 4, 11; образцы — §13.3).
//
// В отличие от laravel_test.go (порог, покрытие реальной версии образца,
// состав) и score_test.go (ScoreRules на изолированных правилах) кейс
// корпуса — цельная история «headers + cookies + HTML → EvaluateCtx
// (DefaultLimits) → ScoreRules → IsLaravel(0.6)», как её соберёт воркер
// анализатора: ожидания точные (score с допуском corpusScoreTol, строки
// границ вычисленного диапазона, конфликт), а не только покрытие.
//
// Подход к ожиданию score: значение в meta.yaml — noisy-OR весов правил
// из поля rules, округлённый до 3 знаков; тест допускает ±corpusScoreTol.
// Точных float-констант в мете нет: веса правил меняются при доработке
// набора, 3 знака ловят любое содержательное изменение и не хрупки к
// порядку накопления.
//
// Группы кейсов: полная история по всем образцам (+ пустой HTML у JSON-API
// образца), разбор слабых срабатываний негативных образцов, score-математика
// (накопление noisy-OR против одиночных весов, порог на границе),
// пересечение диапазонов и конфликтный набор правил на синтетической
// странице, лимиты (сверхлимитный HTML обрезается — признаки в хвосте
// не матчатся).
package rules

import (
	"context"
	"fmt"
	"os"
	"slices"
	"sort"
	"strings"
	"testing"
)

const (
	corpusRulesDir   = `../../rules`
	corpusSamplesDir = `../../testdata/laravel-samples`
	// corpusScoreTol — допуск сверки score с ожиданием meta.yaml
	// (округление до 3 знаков; float-порядок накопления шумит ~1e-16).
	corpusScoreTol = 0.001
)

// corpusSet — рабочий набор правил Laravel (rules/laravel.yaml).
func corpusSet(t *testing.T) *Set {
	t.Helper()
	if _, err := os.Stat(corpusRulesDir); err != nil {
		t.Skipf("набор правил недоступен: %v", err)
	}
	set, err := LoadDir(corpusRulesDir)
	if err != nil {
		t.Fatalf("набор правил не загружается: %v", err)
	}
	return set
}

// corpusSamples — корпус образцов (testdata/laravel-samples).
func corpusSamples(t *testing.T) []Sample {
	t.Helper()
	if _, err := os.Stat(corpusSamplesDir); err != nil {
		t.Skipf("корпус образцов недоступен: %v", err)
	}
	samples, err := LoadSamples(corpusSamplesDir)
	if err != nil {
		t.Fatalf("корпус не загружается: %v", err)
	}
	return samples
}

// corpusSampleByID — один образец корпуса по имени каталога.
func corpusSampleByID(t *testing.T, id string) Sample {
	t.Helper()
	for _, s := range corpusSamples(t) {
		if s.ID == id {
			return s
		}
	}
	t.Fatalf("образец %s отсутствует в корпусе", id)
	return Sample{}
}

// corpusFired — сработавшие правила (id, по сортировке — сравнение
// с ожиданием порядка набора не зависит).
func corpusFired(res Result) []string {
	fired := make([]string, 0, len(res.Signals))
	for _, ev := range res.Signals {
		fired = append(fired, ev.Rule)
	}
	sort.Strings(fired)
	return fired
}

// corpusSortedIDs — отсортированная копия списка id.
func corpusSortedIDs(ids []string) []string {
	out := slices.Clone(ids)
	sort.Strings(out)
	return out
}

// corpusMatchedRules — правила набора, сработавшие на странице, в порядке
// набора (том же, в котором их накопил EvaluateCtx) — вход прямого вызова
// ScoreRules, следующее звено полной истории.
func corpusMatchedRules(t *testing.T, set *Set, res Result) []Rule {
	t.Helper()
	fired := make(map[string]bool, len(res.Signals))
	for _, ev := range res.Signals {
		fired[ev.Rule] = true
	}
	var matched []Rule
	for _, r := range set.Rules {
		if fired[r.ID] {
			matched = append(matched, r)
		}
	}
	return matched
}

// TestCorpusSamplesFullHistory — полная история по каждому образцу корпуса:
// headers.json + cookies.json + index.html → Page → EvaluateCtx
// (DefaultLimits) → ScoreRules → IsLaravel(0.6). Точные ожидания движка
// (score/calc_ver_min/calc_ver_max/conflict) обязательны в каждом meta.yaml —
// корпус без них молча деградировал бы до проверки одного порога.
func TestCorpusSamplesFullHistory(t *testing.T) {
	set := corpusSet(t)
	samples := corpusSamples(t)

	// Целостность корпуса (ТЗ §11.4 пп. 4, 11; §13.3): позитивные и
	// негативные образцы, у негативных обязаны быть и «слабые» — со
	// срабатываниями ниже порога (контроль ложноположительных).
	var positives, negatives, weakNegatives int
	for _, s := range samples {
		if s.Expect.IsLaravel == nil {
			t.Fatalf("samples/%s/meta.yaml: нужно ожидание is_laravel", s.ID)
		}
		if s.Expect.Score == nil || s.Expect.CalcVerMin == nil || s.Expect.CalcVerMax == nil {
			t.Fatalf("samples/%s/meta.yaml: нужны точные ожидания движка score/calc_ver_min/calc_ver_max", s.ID)
		}
		if s.Expect.Rules == nil {
			t.Fatalf("samples/%s/meta.yaml: нужен полный состав сработавших правил (rules)", s.ID)
		}
		if *s.Expect.IsLaravel {
			positives++
		} else {
			negatives++
			if len(s.Expect.Rules) > 0 {
				weakNegatives++
			}
		}
	}
	if positives < 5 || negatives < 4 || weakNegatives < 1 {
		t.Fatalf("корпус подозрительно мал: позитивных %d (нужно >=5), негативных %d (нужно >=4), из них со слабыми срабатываниями %d (нужно >=1)",
			positives, negatives, weakNegatives)
	}

	for _, s := range samples {
		t.Run(s.ID, func(t *testing.T) {
			res := set.EvaluateCtx(context.Background(), s.Page(), DefaultLimits())

			// Состав: полный и точный.
			if fired := corpusFired(res); !slices.Equal(fired, corpusSortedIDs(s.Expect.Rules)) {
				t.Errorf("сработали %v, в meta.yaml %v", fired, s.Expect.Rules)
			}

			// Score — точное ожидание (± corpusScoreTol).
			if d := diff(res.Score, *s.Expect.Score); d > corpusScoreTol {
				t.Errorf("score = %.6f, в meta.yaml %.3f (расхождение %.6f > %g)",
					res.Score, *s.Expect.Score, d, corpusScoreTol)
			}

			// Звено ScoreRules: прямой вызов на сработавших правилах —
			// тот же итог, что собрал EvaluateCtx.
			score2, verMin2, verMax2, conflict2 := ScoreRules(corpusMatchedRules(t, set, res))
			if score2 != res.Score || verMin2 != res.VerMin || verMax2 != res.VerMax || conflict2 != res.Conflict {
				t.Errorf("ScoreRules не сходится с EvaluateCtx: (%v, %q, %q, %v) против (%v, %q, %q, %v)",
					score2, verMin2, verMax2, conflict2, res.Score, res.VerMin, res.VerMax, res.Conflict)
			}

			// Диапазон версий — точные строки границ из правил
			// ("" — границы нет; при записи в БД это NULL).
			if res.VerMin != *s.Expect.CalcVerMin || res.VerMax != *s.Expect.CalcVerMax {
				t.Errorf("диапазон = [%q..%q], в meta.yaml [%q..%q]",
					res.VerMin, res.VerMax, *s.Expect.CalcVerMin, *s.Expect.CalcVerMax)
			}
			if res.Conflict != s.Expect.Conflict {
				t.Errorf("conflict = %v, в meta.yaml %v", res.Conflict, s.Expect.Conflict)
			}
			if res.Conflict && (res.VerMin != "" || res.VerMax != "") {
				t.Errorf("при конфликте диапазон неопределён (обе границы NULL), got [%q..%q]", res.VerMin, res.VerMax)
			}

			// Порог is_laravel (ТЗ §13.1: 0.6); нулевой порог обязан
			// подставлять тот же дефолт.
			if got := res.IsLaravel(DefaultMinScore); got != *s.Expect.IsLaravel {
				t.Errorf("IsLaravel(0.6) = %v (score %.3f), в meta.yaml %v",
					got, res.Score, *s.Expect.IsLaravel)
			}
			if res.IsLaravel(0) != res.IsLaravel(DefaultMinScore) {
				t.Error("нулевой порог должен подставлять DefaultMinScore")
			}
		})
	}
}

// TestCorpusJSONAPIEmptyHTML — JSON-API образец без HTML: JSON-эпизод мог
// тела не сохранить (ошибка до чтения тела, контент сверх лимита), пассивный
// анализ по кукам обязан работать и на пустом контенте. htmlCap на nil-HTML
// безопасен, HTML-правила (json_message_key) не срабатывают, порог
// достигается одними куками: 1 − 0.5·0.65·0.85·0.75 = 0.793 ≥ 0.6
// (вес cookie_encrypted_payload 0.5 — калибровка T-116).
func TestCorpusJSONAPIEmptyHTML(t *testing.T) {
	set := corpusSet(t)
	page := corpusSampleByID(t, "laravel-9-json-api").Page()
	page.HTML = nil

	res := set.EvaluateCtx(context.Background(), page, DefaultLimits())

	want := []string{
		"cookie_encrypted_payload",
		"cookie_laravel_session",
		"cookie_session_suffix",
		"cookie_xsrf_token",
	}
	if fired := corpusFired(res); !slices.Equal(fired, want) {
		t.Fatalf("сработали %v, ожидались только куки %v", fired, want)
	}
	if wantScore := ScoreFixed([]float64{0.5, 0.35, 0.15, 0.25}); diff(res.Score, wantScore) > 1e-9 {
		t.Errorf("score = %v, want %v (куки без JSON-признака)", res.Score, wantScore)
	}
	if !res.IsLaravel(DefaultMinScore) {
		t.Errorf("куки одни дают уверенное срабатывание: score %.3f ≥ 0.6", res.Score)
	}
	if res.VerMin != "" || res.VerMax != "" || res.Conflict {
		t.Errorf("диапазон без версионных правил должен быть пуст: [%q..%q] conflict=%v",
			res.VerMin, res.VerMax, res.Conflict)
	}
}

// TestCorpusJSONMessageKey — широкое правило json_message_key (T-117,
// пассивный аналог активного 404-зонда larasites): матчит и точный
// «Unauthenticated.», и «Not Found» — ровно один раз на ответ (двойного
// учёта нет: точного правила рядом не существует), но поодиночке порога
// не достигает (вес 0.25 понижен против точного 0.3: паттерн пишут
// NestJS/Express — {"message":"Cannot GET /"}).
func TestCorpusJSONMessageKey(t *testing.T) {
	set := corpusSet(t)

	for _, body := range []string{
		`{"message":"Unauthenticated."}`, // точная форма (бывший json_unauthenticated)
		`{"message":"Not Found"}`,        // 404 API-поддомена (образец T-117)
		`{"message":"Too Many Attempts."}`,
	} {
		res := set.EvaluateCtx(context.Background(), Page{HTML: []byte(body)}, DefaultLimits())
		if fired := corpusFired(res); !slices.Equal(fired, []string{"json_message_key"}) {
			t.Errorf("тело %q: сработали %v, хочу ровно json_message_key (без дублей)", body, fired)
		}
		if res.Score != 0.25 {
			t.Errorf("тело %q: score = %v, want 0.25", body, res.Score)
		}
		if res.IsLaravel(DefaultMinScore) {
			t.Errorf("тело %q: одиночное правило 0.25 не должно достигать порога %.2f", body, DefaultMinScore)
		}
	}

	// Границы паттерна: длинное значение (>120) и ключ-не-строка —
	// несовпадение; пустое — тоже (квантификатор {1,120}).
	for _, body := range []string{
		`{"message":"` + strings.Repeat("x", 121) + `"}`,
		`{"message":""}`,
		`{"message":404}`,
		`{"messages":"Not Found"}`,
	} {
		res := set.EvaluateCtx(context.Background(), Page{HTML: []byte(body)}, DefaultLimits())
		if len(res.Signals) != 0 {
			t.Errorf("тело %.40q…: неожиданные срабатывания %v", body, corpusFired(res))
		}
	}

	// Чужой стек: NestJS/Express даёт ту же форму — поодиночке ниже порога,
	// в сочетании с куками T-116 детектируется (образец laravel-12-api-404).
	nest := set.EvaluateCtx(context.Background(),
		Page{HTML: []byte(`{"message":"Cannot GET /"}`)}, DefaultLimits())
	if nest.IsLaravel(DefaultMinScore) || nest.Score != 0.25 {
		t.Errorf("NestJS-ответ: score %.3f — одиночное правило ниже порога, не Laravel", nest.Score)
	}
}

// TestCorpusCMSSession — правило cookie_cms_session (T-134, сверка с
// larasites): CMS на Laravel ставят собственные имена сессии (october/
// winter/bagisto — проверено по исходникам CMS), которые до правила ловил
// только генерический суффикс *_session — пара «CMS-кука + шифро-конверт»
// без XSRF-TOKEN давала 0.15 + 0.5 = 0.575 < 0.6 (пропуск, не закрытый
// калибровкой T-116). Вес 0.35 — уровень laravel_session (имена
// Laravel-экосистемны, в отличие от суффикса, совпадающего с ci_session):
// 0.35 + 0.5 → 0.675 ≥ 0.6; двойной учёт с суффиксом консистентен с
// поведением laravel_session (совпадает с обоими). Префиксы __Secure-/
// __Host- (October ≥1.1 ставит автоматически на https) учтены и в
// cms-правиле, и в суффиксе *_session.
func TestCorpusCMSSession(t *testing.T) {
	set := corpusSet(t)

	// Образцы корпуса: October (префикс __Secure- — https-эпизод October
	// ≥1.1) и Winter (стоковое имя), оба без XSRF-TOKEN — детекция куками:
	// ровно три правила, шумовых HTML-признаков нет.
	for _, id := range []string{"october-cms", "winter-cms"} {
		res := set.EvaluateCtx(context.Background(), corpusSampleByID(t, id).Page(), DefaultLimits())
		want := []string{"cookie_cms_session", "cookie_encrypted_payload", "cookie_session_suffix"}
		if fired := corpusFired(res); !slices.Equal(fired, want) {
			t.Errorf("%s: сработали %v, ожидались %v", id, fired, want)
		}
		if wantScore := ScoreFixed([]float64{0.35, 0.15, 0.5}); diff(res.Score, wantScore) > 1e-9 {
			t.Errorf("%s: score = %v, want %v (0.35+0.15+0.5)", id, res.Score, wantScore)
		}
		if !res.IsLaravel(DefaultMinScore) {
			t.Errorf("%s: score %.3f ≥ порога — пропуск T-134 закрыт", id, res.Score)
		}
	}

	// Состав списка — только имена, проверяемые по исходникам: матчатся
	// october/winter/bagisto с префиксами __Secure-/__Host- и без.
	fires := func(name string) bool {
		t.Helper()
		res := set.EvaluateCtx(context.Background(),
			Page{Cookies: []Cookie{{Name: name, Value: "plain"}}}, DefaultLimits())
		return slices.Contains(corpusFired(res), "cookie_cms_session")
	}
	for _, name := range []string{
		"october_session", "winter_session", "bagisto_session",
		"__Secure-october_session", "__Host-winter_session", "__Secure-bagisto_session",
	} {
		if !fires(name) {
			t.Errorf("кука %s: правило cookie_cms_session должно срабатывать", name)
		}
	}
	// Не матчатся: подтверждённых имён нет (botble/pyrocms/asgard — состав
	// не расширен непроверяемыми), генерические имена и огранки паттерна —
	// якоря, постфиксы, чужая форма (статamic оставляет laravel_session).
	for _, name := range []string{
		"ci_session", "myapp_session", "laravel_session",
		"botble_session", "pyrocms_session", "asgard_session",
		"october_sessions",         // постфиксный хвост
		"foo_october_session",      // имя приложения перед cms-именем
		"october-session",          // дефис вместо подчёркивания
		"__secure-october_session", // префикс регистрозависим (RFC 6265bis)
	} {
		if fires(name) {
			t.Errorf("кука %s: правило cookie_cms_session срабатывать не должно", name)
		}
	}

	// laravel_session — эталон консистентности двойного учёта: совпадает
	// с cookie_laravel_session и суффиксом, но не с cms-правилом.
	res := set.EvaluateCtx(context.Background(),
		Page{Cookies: []Cookie{{Name: "laravel_session", Value: "plain"}}}, DefaultLimits())
	if fired := corpusFired(res); !slices.Equal(fired, []string{"cookie_laravel_session", "cookie_session_suffix"}) {
		t.Errorf("laravel_session: сработали %v, ожидались cookie_laravel_session и cookie_session_suffix", fired)
	}

	// Правка суффикса *_session под префиксы: __Secure-myapp_session (произвольное
	// имя, как в тесте larasites myapp_session) матчится суффиксом — 0.15;
	// __Secure-october_session — обоими правилами (двойной учёт).
	res = set.EvaluateCtx(context.Background(),
		Page{Cookies: []Cookie{{Name: "__Secure-myapp_session", Value: "plain"}}}, DefaultLimits())
	if fired := corpusFired(res); !slices.Equal(fired, []string{"cookie_session_suffix"}) {
		t.Errorf("__Secure-myapp_session: сработали %v, ожидался только cookie_session_suffix", fired)
	}
	if diff(res.Score, 0.15) > 1e-9 || res.IsLaravel(DefaultMinScore) {
		t.Errorf("__Secure-myapp_session: score %.3f — суффикс один ниже порога", res.Score)
	}

	// Серая зона (контроль T-115: новые пары не заносят шум):
	//
	//   - CMS-имя без конверта: 0.35 + 0.15 = 0.4475 < 0.6 (EncryptCookies
	//     отключён/кука от прокси) — находка в findings, но не laravel_domains;
	//   - ближайший негатив к новой паре: ci_session + конверт — 0.15 + 0.5 =
	//     0.575 < 0.6 (ci_session не входит в cms-список, суффиксу конверт
	//     безразличен — тот же класс пропуска для не-CMS имён остался ниже
	//     порога сознательно: без экосистемного имени конверт слабее).
	res = set.EvaluateCtx(context.Background(),
		Page{Cookies: []Cookie{{Name: "october_session", Value: "plain"}}}, DefaultLimits())
	if want := 1 - 0.65*0.85; diff(res.Score, want) > 1e-9 || res.IsLaravel(DefaultMinScore) {
		t.Errorf("CMS-имя без конверта: score %.4f, want %v, ниже порога", res.Score, want)
	}
	res = set.EvaluateCtx(context.Background(),
		Page{Cookies: []Cookie{{Name: "ci_session", Value: "eyJpdiI6Y2k6MTY6e30="}}}, DefaultLimits())
	if fired := corpusFired(res); !slices.Equal(fired, []string{"cookie_encrypted_payload", "cookie_session_suffix"}) {
		t.Fatalf("ci_session с конвертом: сработали %v, ожидались cookie_encrypted_payload и cookie_session_suffix", fired)
	}
	if want := 1 - 0.85*0.5; diff(res.Score, want) > 1e-9 || res.IsLaravel(DefaultMinScore) {
		t.Errorf("ci_session с конвертом: score %.4f, want %v — ниже порога", res.Score, want)
	}
}

// TestCorpusNegativesWeakSignals — негативные образцы (ТЗ §13.3: «в том
// числе на других фреймворках, использующих похожие признаки»): is_laravel
// обязана быть false, и тест разбирает, какими слабыми срабатываниями
// ограничивается движок и почему они ниже порога.
func TestCorpusNegativesWeakSignals(t *testing.T) {
	set := corpusSet(t)

	for _, s := range corpusSamples(t) {
		if s.Expect.IsLaravel == nil || *s.Expect.IsLaravel {
			continue
		}
		t.Run(s.ID, func(t *testing.T) {
			res := set.EvaluateCtx(context.Background(), s.Page(), DefaultLimits())

			if res.IsLaravel(DefaultMinScore) {
				t.Fatalf("ложноположительное срабатывание: score %.3f ≥ порога %.2f", res.Score, DefaultMinScore)
			}
			if s.Expect.Score == nil {
				t.Fatal("нет ожидания score")
			}
			if d := diff(res.Score, *s.Expect.Score); d > corpusScoreTol {
				t.Errorf("score = %.6f, в meta.yaml %.3f (расхождение %.6f)", res.Score, *s.Expect.Score, d)
			}
			if fired := corpusFired(res); !slices.Equal(fired, corpusSortedIDs(s.Expect.Rules)) {
				t.Errorf("сработали %v, в meta.yaml %v", fired, s.Expect.Rules)
			}

			// Почему ниже порога: каждый отдельный признак слаб — ни один
			// сработавший вес не достигает порога сам по себе, а накопление
			// (res.Score) осталось ниже. Рабочий набор устроен так
			// сознательно (header rules/laravel.yaml): правила, которые
			// встречаются и вне Laravel, получают вес < 0.6 — срабатывание
			// попадёт в laravel_findings, но не в laravel_domains.
			for _, ev := range res.Signals {
				if ev.Weight >= DefaultMinScore {
					t.Errorf("правило %s одним весом %.2f достигает порога: для похожего не-Laravel признака вес завышен",
						ev.Rule, ev.Weight)
				}
			}
		})
	}

	// Rails — эталонный «похожий» фреймворк (§13.3): csrf-token meta пишут
	// и Rails-шаблоны (0.2), кука _shop_session матчится суффиксным
	// правилом *_session (0.15, вес понижен именно из-за Rails и
	// CodeIgniter). Поодиночке и вместе — noisy-OR 1 − 0.8·0.85 = 0.32 —
	// ниже порога 0.6.
	t.Run("rails-negative: 0.32 = noisy-OR(0.2, 0.15)", func(t *testing.T) {
		res := set.EvaluateCtx(context.Background(),
			corpusSampleByID(t, "rails-negative").Page(), DefaultLimits())
		weights := map[string]float64{}
		for _, ev := range res.Signals {
			weights[ev.Rule] = ev.Weight
		}
		if len(weights) != 2 || weights["meta_csrf_token"] != 0.2 || weights["cookie_session_suffix"] != 0.15 {
			t.Fatalf("сработали %v, ожидались meta_csrf_token (0.2) и cookie_session_suffix (0.15)", weights)
		}
		if want := 1 - (1-0.2)*(1-0.15); diff(res.Score, want) > 1e-9 {
			t.Errorf("score = %v, want %v", res.Score, want)
		}
		if res.IsLaravel(DefaultMinScore) {
			t.Errorf("score %.3f не должен достигать порога %.2f", res.Score, DefaultMinScore)
		}
	})
}

// TestCorpusScoreMath — score-математика сквозным путём (синтетический
// набор, страница с маркерами — полная история до IsLaravel): накопление
// noisy-OR против одиночных весов и порог на границе. ScoreRules сам по
// себе покрыт score_test.go; здесь — поведение решения «Laravel/не Laravel»
// на странице целиком.
func TestCorpusScoreMath(t *testing.T) {
	// Накопление: страница добавляет по одному слабому признаку (вес 0.2),
	// score растёт как 1 − 0.8ⁿ; порог 0.6 достигается только пятым —
	// одиночные веса, пары и тройки остаются ниже. Слабые признаки
	// накапливаются, но не дают 100% (ТЗ §5.8).
	markers := `
rules:
  - id: m1
    target: html
    regex: "MARK1"
    weight: 0.2
  - id: m2
    target: html
    regex: "MARK2"
    weight: 0.2
  - id: m3
    target: html
    regex: "MARK3"
    weight: 0.2
  - id: m4
    target: html
    regex: "MARK4"
    weight: 0.2
  - id: m5
    target: html
    regex: "MARK5"
    weight: 0.2
`
	set := evalSet(t, markers)
	for n := 1; n <= 5; n++ {
		parts := make([]string, n)
		for i := range parts {
			parts[i] = fmt.Sprintf("MARK%d", i+1)
		}
		res := set.EvaluateCtx(context.Background(),
			Page{HTML: []byte(strings.Join(parts, " "))}, DefaultLimits())
		remain := 1.0 // Π(1 − 0.2) по n сработавшим
		for i := 0; i < n; i++ {
			remain *= 0.8
		}
		if want := 1 - remain; diff(res.Score, want) > 1e-9 {
			t.Errorf("%d маркера: score = %v, want %v", n, res.Score, want)
		}
		if got := res.IsLaravel(DefaultMinScore); got != (n == 5) {
			t.Errorf("%d маркера: IsLaravel = %v (score %.4f)", n, got, res.Score)
		}
	}
	// Накопление сильнее любой одиночной составляющей, но меньше единицы.
	five := set.EvaluateCtx(context.Background(),
		Page{HTML: []byte("MARK1 MARK2 MARK3 MARK4 MARK5")}, DefaultLimits())
	if five.Score <= 0.2 || five.Score >= 1 {
		t.Errorf("накопление пяти по 0.2 вне (0.2, 1): %v", five.Score)
	}

	// Одиночный вес: score равен весу (допуск на порядок операций).
	for _, w := range []float64{0.25, 0.45, 0.5, 0.9} {
		set := evalSet(t, fmt.Sprintf("rules:\n  - id: solo\n    target: html\n    regex: MARK\n    weight: %v\n", w))
		res := set.EvaluateCtx(context.Background(), Page{HTML: []byte("MARK")}, DefaultLimits())
		if diff(res.Score, w) > 1e-9 {
			t.Errorf("одиночный вес %v: score = %v", w, res.Score)
		}
	}

	// Порог на границе: вес ровно 0.6 — score без округлений равен 0.6
	// (1−(1−0)·(1−0.6) в IEEE-754 точно), нестрогое сравнение даёт true.
	set = evalSet(t, "rules:\n  - id: edge\n    target: html\n    regex: MARK\n    weight: 0.6\n")
	res := set.EvaluateCtx(context.Background(), Page{HTML: []byte("MARK")}, DefaultLimits())
	if diff(res.Score, 0.6) > 1e-12 {
		t.Fatalf("вес 0.6: score = %.17g, want ровно 0.6", res.Score)
	}
	if !res.IsLaravel(DefaultMinScore) {
		t.Error("score ровно на пороге: нестрогое ≥ должно давать is_laravel")
	}

	// Реальная пара cookie_xsrf_token (0.25) + cookie_encrypted_payload
	// (0.5, калибровка T-116): 1 − 0.75·0.5 = 0.625 ≥ порога —
	// stateless API (api-субдомен без сессии на главной) детектируется
	// двумя куками; до калибровки (вес 0.45) пара давала 0.5875 и сайт
	// выпадал из laravel_domains (дефект, найден сверкой с larasites).
	set = evalSet(t, `
rules:
  - id: a_xsrf
    target: cookie_name
    match: "XSRF-TOKEN"
    weight: 0.25
  - id: b_enc
    target: cookie_value
    regex: "^eyJpdiI6"
    weight: 0.5
`)
	res = set.EvaluateCtx(context.Background(),
		Page{Cookies: []Cookie{{Name: "XSRF-TOKEN", Value: "eyJpdiI6abc=="}}}, DefaultLimits())
	if want := 1 - 0.75*0.5; diff(res.Score, want) > 1e-9 {
		t.Errorf("score = %v, want %v", res.Score, want)
	}
	if !res.IsLaravel(DefaultMinScore) {
		t.Errorf("score %.4f обязан достигать порога %v (T-116: stateless API)", res.Score, DefaultMinScore)
	}

	// Ослабление не занесло шумовые пары (проверка T-116 по аналогии
	// серой зоны): сочетания БЕЗ шифрованной куки остаются ниже порога —
	// CodeIgniter-тройка ci_session (суффикс 0.15) + csrf-meta (0.2) +
	// форма _token (0.25): 1 − 0.85·0.8·0.75 = 0.49. Суффикс — копия
	// рабочего правила (с префиксами __Secure-/__Host- от T-134): кука
	// с префиксом ловится тем же весом и порог не меняет.
	set = evalSet(t, `
rules:
  - id: ci_sess
    target: cookie_name
    regex: '^(?:__Secure-|__Host-)?[a-z0-9_\-]+_session$'
    weight: 0.15
  - id: csrf_meta
    target: meta
    regex: 'name=["'']csrf-token["'']'
    weight: 0.2
  - id: csrf_form
    target: html
    regex: '<input[^>]+name="_token"'
    weight: 0.25
`)
	ciPage := Page{
		HTML:    []byte(`<meta name="csrf-token" content="x"><input type="hidden" name="_token" value="y">`),
		Cookies: []Cookie{{Name: "ci_session", Value: "aGVsbG8"}},
	}
	res = set.EvaluateCtx(context.Background(), ciPage, DefaultLimits())
	if want := 1 - 0.85*0.8*0.75; diff(res.Score, want) > 1e-9 {
		t.Errorf("score шумовой тройки = %v, want %v", res.Score, want)
	}
	if res.IsLaravel(DefaultMinScore) {
		t.Errorf("шумовая тройка без шифрованной куки: %.4f не должна достигать порога", res.Score)
	}
	ciPage.Cookies[0].Name = "__Secure-ci_session"
	res = set.EvaluateCtx(context.Background(), ciPage, DefaultLimits())
	if want := 1 - 0.85*0.8*0.75; diff(res.Score, want) > 1e-9 {
		t.Errorf("score тройки с __Secure-ci_session = %v, want %v (префикс вес не меняет)", res.Score, want)
	}

	// Вес 1 — уверенный признак закрывает score до единицы.
	set = evalSet(t, "rules:\n  - id: sure\n    target: html\n    regex: MARK\n    weight: 1\n")
	if res := set.EvaluateCtx(context.Background(), Page{HTML: []byte("MARK")}, DefaultLimits()); res.Score != 1 {
		t.Errorf("вес 1: score = %v, want 1", res.Score)
	}
}

// TestCorpusVersionRanges — пересечение диапазонов версий сквозным путём:
// заранее собранный Set применяется к синтетической странице, границы
// читаются из итога EvaluateCtx (не из ScoreRules напрямую). ТЗ §5.8:
// диапазон — пересечение [ver_min, ver_max] сработавших правил, пустое
// пересечение — конфликт и неопределённый диапазон.
func TestCorpusVersionRanges(t *testing.T) {
	cases := []struct {
		name             string
		body             string // yaml набора
		page             string // HTML синтетической страницы
		wantMin, wantMax string // точные строки границ ("" — границы нет)
		conflict         bool
	}{
		{
			name: "пересечение: нижняя = max(ver_min), верхняя = min(ver_max)",
			body: `
rules:
  - id: a_welcome
    target: html
    regex: "WELCOME"
    weight: 0.45
    ver_min: "5.3"
    ver_max: "7.x"
  - id: b_floor
    target: html
    regex: "FLOOR"
    weight: 0.2
    ver_min: "5.4"
  - id: c_ceiling
    target: html
    regex: "CEILING"
    weight: 0.1
    ver_max: "11.x"
  - id: d_plain
    target: html
    regex: "PLAIN"
    weight: 0.25
`,
			page:     "WELCOME FLOOR CEILING PLAIN",
			wantMin:  "5.4", // max(5.3, 5.4)
			wantMax:  "7.x", // min(7.x, 11.x); d_plain без границ не сужает
			conflict: false,
		},
		{
			name: "равные границы в разной записи — диапазон из одной версии, не конфликт",
			body: `
rules:
  - id: a_low
    target: html
    regex: "LOW"
    weight: 0.3
    ver_min: "9.19"
  - id: b_high
    target: html
    regex: "HIGH"
    weight: 0.3
    ver_max: "9.19.0"
`,
			page:     "LOW HIGH", // 9.19 = 9.19.0
			wantMin:  "9.19",
			wantMax:  "9.19.0",
			conflict: false,
		},
		{
			name: "строки границ сохраняются из правил как есть",
			body: `
rules:
  - id: a_new
    target: html
    regex: "NEW"
    weight: 0.2
    ver_min: "9.19"
  - id: b_old
    target: html
    regex: "OLD"
    weight: 0.15
    ver_max: "9.x"
`,
			page:     "NEW OLD", // [9.19,∞) ∩ (−∞,9.x]
			wantMin:  "9.19",
			wantMax:  "9.x", // «9.x» не превращается в «9»
			conflict: false,
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			res := evalSet(t, c.body).EvaluateCtx(context.Background(),
				Page{HTML: []byte(c.page)}, DefaultLimits())
			if res.VerMin != c.wantMin || res.VerMax != c.wantMax || res.Conflict != c.conflict {
				t.Errorf("диапазон = [%q..%q] conflict=%v, want [%q..%q] conflict=%v",
					res.VerMin, res.VerMax, res.Conflict, c.wantMin, c.wantMax, c.conflict)
			}
		})
	}

	// Конфликтный набор на одной синтетической странице: признаки с
	// несовместимыми границами (≥10 и ≤9.x) срабатывают вместе — пересечение
	// пусто, диапазон сбрасывается (обе границы NULL), но score считается
	// по всем сработавшим и уверенная детекция остаётся уверенной:
	// конфликт версий — не конфликт «Laravel/не Laravel».
	t.Run("конфликт: [10,∞) ∩ (−∞,9.x] — диапазон сброшен, порог в силе", func(t *testing.T) {
		set := evalSet(t, `
rules:
  - id: a_new
    target: html
    regex: "NEW"
    weight: 0.45
    ver_min: "10"
  - id: b_old
    target: html
    regex: "OLD"
    weight: 0.5
    ver_max: "9.x"
`)
		res := set.EvaluateCtx(context.Background(),
			Page{HTML: []byte("NEW OLD")}, DefaultLimits())
		if !res.Conflict {
			t.Fatal("ожидался конфликт диапазонов")
		}
		if res.VerMin != "" || res.VerMax != "" {
			t.Errorf("при конфликте границы сбрасываются, got [%q..%q]", res.VerMin, res.VerMax)
		}
		if len(res.Signals) != 2 {
			t.Errorf("оба срабатывания сохраняются (в laravel_findings пишутся все), got %d", len(res.Signals))
		}
		if want := 1 - 0.55*0.5; diff(res.Score, want) > 1e-9 {
			t.Errorf("score = %v, want %v (конфликт не меняет score)", res.Score, want)
		}
		if !res.IsLaravel(DefaultMinScore) {
			t.Errorf("score %.3f ≥ порога: конфликт версий не отменяет детекцию", res.Score)
		}
	})

	// Конфликт на рабочем наборе: страница Laravel 12 с унаследованной
	// классической welcome-разметкой — Vite (≥9.19) и welcome_classic
	// (≤7.x) вместе невозможны ни в одной версии.
	t.Run("рабочий набор: vite (≥9.19) + welcome_classic (≤7.x) — конфликт", func(t *testing.T) {
		set := corpusSet(t)
		res := set.EvaluateCtx(context.Background(), Page{HTML: []byte(`<!DOCTYPE html>
<html><head>
<link rel="stylesheet" href="/build/assets/app-Bk3kX93l.css">
</head>
<body>
<div class="flex-center position-ref full-height">
  <div class="title m-b-md">Laravel</div>
  <a href="https://laravel.com/docs">Docs</a>
</div>
</body></html>`)}, DefaultLimits())
		if fired := corpusFired(res); !slices.Equal(fired,
			[]string{"laravel_docs_link", "vite_assets", "welcome_classic"}) {
			t.Fatalf("сработали %v, ожидались laravel_docs_link, vite_assets, welcome_classic", fired)
		}
		if !res.Conflict {
			t.Fatal("ожидался конфликт: [9.19,∞) ∩ [5.3,7.x] пусто")
		}
		if res.VerMin != "" || res.VerMax != "" {
			t.Errorf("при конфликте границы сбрасываются, got [%q..%q]", res.VerMin, res.VerMax)
		}
		if want := 1 - 0.7*0.8*0.55; diff(res.Score, want) > 1e-9 {
			t.Errorf("score = %v, want %v", res.Score, want)
		}
		if !res.IsLaravel(DefaultMinScore) {
			t.Errorf("score %.3f ≥ порога несмотря на конфликт диапазона", res.Score)
		}
	})
}

// TestCorpusHTMLTailLimit — лимиты сквозным путём (ТЗ §9: парсеры имеют
// ограничения размера): сверхлимитный HTML обрезается до матчинга —
// признаки в хвосте не матчатся, остальные цели (куки) не затрагиваются.
func TestCorpusHTMLTailLimit(t *testing.T) {
	// Рабочий набор на реальном образце с 2 MiB мусора перед документом:
	// HTML-признаки (meta csrf-token, vite, inertia) уходят за пределы
	// DefaultLimits().MaxHTMLBytes и не срабатывают; куки лимитом HTML не
	// трогаются — их хватает на порог (0.772 ≥ 0.6), is_laravel сохраняется.
	t.Run("образец с хвостом за 2 MiB: HTML-признаки не видны, куки работают", func(t *testing.T) {
		set := corpusSet(t)
		page := corpusSampleByID(t, "laravel-11-vite").Page()
		padded := make([]byte, DefaultLimits().MaxHTMLBytes+len(page.HTML))
		copy(padded[DefaultLimits().MaxHTMLBytes:], page.HTML)

		res := set.EvaluateCtx(context.Background(),
			Page{HTML: padded, Cookies: page.Cookies, Headers: page.Headers}, DefaultLimits())

		want := []string{
			"cookie_encrypted_payload",
			"cookie_laravel_session",
			"cookie_session_suffix",
			"cookie_xsrf_token",
		}
		if fired := corpusFired(res); !slices.Equal(fired, want) {
			t.Fatalf("сработали %v, ожидались только куки %v", fired, want)
		}
		if wantScore := ScoreFixed([]float64{0.5, 0.35, 0.15, 0.25}); diff(res.Score, wantScore) > 1e-9 {
			t.Errorf("score = %v, want %v", res.Score, wantScore)
		}
		if !res.IsLaravel(DefaultMinScore) {
			t.Errorf("куки дают уверенное срабатывание даже с обрезанным HTML: %.3f", res.Score)
		}
	})

	// Граница обрезки байт в байт: признак, последний байт которого ровно
	// на лимите, ещё виден; на байт дальше — уже нет. Вес признака 0.6 —
	// обрезка напрямую решает is_laravel.
	t.Run("маркер на границе лимита", func(t *testing.T) {
		set := evalSet(t, "rules:\n  - id: needle\n    target: html\n    regex: NEEDLE\n    weight: 0.6\n")
		lim := DefaultLimits()
		lim.MaxHTMLBytes = 100

		// NEEDLE (6 байт) заканчивается ровно на байте с индексом 99.
		in := append([]byte(strings.Repeat(" ", 100-6)), []byte("NEEDLE")...)
		res := set.EvaluateCtx(context.Background(), Page{HTML: in}, lim)
		if len(res.Signals) != 1 || !res.IsLaravel(DefaultMinScore) {
			t.Errorf("признак целиком в пределах лимита должен срабатывать: %+v", res)
		}

		// Хвостовой байт NEEDLE за пределами — совпадения нет.
		out := append([]byte(strings.Repeat(" ", 101-6)), []byte("NEEDLE")...)
		res = set.EvaluateCtx(context.Background(), Page{HTML: out}, lim)
		if len(res.Signals) != 0 || res.Score != 0 || res.IsLaravel(DefaultMinScore) {
			t.Errorf("признак за пределами лимита не должен находиться: %+v", res)
		}
	})
}
