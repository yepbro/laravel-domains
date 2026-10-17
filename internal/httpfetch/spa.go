// Эвристика SPA-оболочки (T-057; ТЗ 5.5 п. 8 «решение о рендере»,
// ТЗ 13.2 «эвристика определения SPA-оболочки — на усмотрение
// разработки»). Чистая функция над итоговым телом страницы (UTF-8
// гарантирован T-051, потолок 2 МБ — T-049): по набору признаков
// решает, нужен ли рендер в Chromium (публикацию в q:render делает
// потребитель T-053 — здесь только вердикт и сработавшие признаки).
//
// Признаки:
//
//   - мало видимого текста — видимые символы (байты) после грубого
//     удаления тегов и содержимого script/style/noscript меньше
//     SPAVisibleTextMax;
//   - корневой контейнер — атрибут id="root|app|__next|__nuxt" (в
//     любых кавычках и регистре) у любого тега: точка монтирования
//     фреймворка (React/Vue/Next/Nuxt, «и т. п.» ТЗ 5.5 п. 8);
//   - noscript с требованием JS — <noscript>, чей текст просит
//     включить JavaScript/JS (список ключей — spaNoScriptKeys);
//   - JS-редирект — строковый скан на location.replace/location.assign/
//     location.href=…/location=… (window.location и document.location —
//     частные случаи «голого» location; исполнение не выполняется);
//   - meta refresh — <meta http-equiv="refresh"> в итоговом HTML.
//
// Таблица решений (прочтение ТЗ 5.5 п. 8: перечень через «;» даёт
// самодостаточные признаки, «мало видимого текста + корневой
// контейнер» — парное условие, noscript — усиливающий):
//
//	#  текст  контейнер  noscript  JS-редирект  meta refresh  SPA
//	1  *      *          *         да           *             да
//	2  *      *          *         *            да            да
//	3  мало   да         *         нет          нет           да
//	4  мало   нет        да        нет          нет           да
//	5  мало   нет        нет       нет          нет           нет
//	6  много  да         да        нет          нет           нет
//	7  много  нет        да        нет          нет           нет
//	8  много  нет        нет       нет          нет           нет
//
// Обоснования:
//
//   - JS-редирект достаточен один (строка 1): страница собственным
//     кодом уводит браузер на другой адрес — сырой HTML по замыслу
//     никогда не был итоговым содержимым, это трамплин.
//   - meta refresh достаточен один (строка 2): T-048 уже прошёл все
//     переходы по meta refresh с url, поэтому в итоговом HTML
//     остаётся только обновление без перехода — «сам на себя»
//     (включая цикл, оборванный детектом) или с недоступным для
//     следования url. Страница, обновляющая сама себя, рассчитана на
//     повторное исполнение в браузере (JS-логика, cookies), ТЗ
//     называет meta refresh отдельным признаком. Ложное срабатывание
//     (классическая страница с content="300") стоит один рендер,
//     ложный пропуск оставил бы заглушку итоговым контентом до
//     перепроверки домена.
//   - пара «мало текста + контейнер» (строка 3) — формулировка ТЗ:
//     контейнер в одиночку не показатель (id="app" встречается в
//     вёрстке классических сайтов), малый текст в одиночку — тоже
//     (парковочные и пустые дефолтные страницы: рендер бессмыслен,
//     добавлять нечего); вместе — каркас фреймворка без контента.
//   - noscript усиливающий (строка 4): «включите JavaScript» при
//     пустой странице — оболочка с любым, в т. ч. нестандартным,
//     контейнером («и т. п.» ТЗ). В одиночку (строка 7) — не
//     показатель: классические сайты ставят вежливое предупреждение,
//     работая и без JS; при богатом тексте (строки 6–8) рендер не
//     нужен — SSR/классика уже отдали контент.
//
// Цена ошибки асимметрична (§5.6, T-066): ложное срабатывание — один
// ограниченный рендер в очереди, пропуск — пустой контент в хранилище
// до следующей перепроверки. Поэтому сканеры признаков нарочито
// минимальны и терпимы к ложным срабатываниям.
//
// Реализация — один линейный проход по телу без копий и аллокаций
// на «тихих» страницах (нулевые аллокации закреплены тестом):
// подсчёт видимого текста чередуется с разбором тегов, содержимое
// script/style пропускается до закрывающего тега, noscript
// проверяется по ключевым словам в первых spaNoScriptLimit байтах.
// JS-редирект — независимый скан по всему документу (ловит и inline
// обработчики вроде onclick="location.href=…"); HTML-парсер не
// используется: признаки описываются байтовыми маркерами, полный
// разбор недоверенной разметки ради эвристики избыточен (§9 —
// ограниченные парсеры; стиль T-048).

package httpfetch

import (
	"bytes"
	"strings"
)

// SPAVisibleTextMax — порог «мало видимого текста»: видимые символы
// (байты, не руны: кириллица считается двойным весом — консервативная
// сторона) после грубого удаления тегов, script, style и noscript.
// Порог из практики каркасов: заглушки React (create-react-app),
// Next/Nuxt/Vue несут 0–~200 видимых байт — весь текст вынесен в
// noscript (не отображается при включённом JS) и в JS-бандлы;
// осмысленные главные страницы — от сотен байт. 300 разделяет классы
// с запасом в обе стороны; настраивается пересборкой константы
// (ТЗ 13.2 — эвристика на усмотрении разработки, отдельный конфиг-ключ
// не заводится до данных нагрузочных замеров §11.3).
const SPAVisibleTextMax = 300

// spaNoScriptLimit — сколько байтов текста <noscript> просматривать
// на требование JS: реальные предупреждения — одно-два предложения,
// длинные noscript (разметка-фолбэк) на признак не влияют.
const spaNoScriptLimit = 4096

// SPAReason — один сработавший признак SPA-оболочки (ТЗ 5.5 п. 8).
// Значения — стабильные строки: потребитель (T-053) кладёт их в
// задачу q:render и журналирует.
type SPAReason string

// Признаки эвристики. Порядок объявления — порядок перечисления в ТЗ.
const (
	// SPAReasonLowText — видимого текста меньше SPAVisibleTextMax.
	SPAReasonLowText SPAReason = "low_text"
	// SPAReasonRootContainer — найден корневой контейнер фреймворка
	// (id=root/app/__next/__nuxt).
	SPAReasonRootContainer SPAReason = "root_container"
	// SPAReasonNoScriptJS — <noscript> требует включить JavaScript.
	SPAReasonNoScriptJS SPAReason = "noscript_js"
	// SPAReasonJSRedirect — строковый след JS-редиректа
	// (location=…, location.href=…, location.replace(…, location.assign(…).
	SPAReasonJSRedirect SPAReason = "js_redirect"
	// SPAReasonMetaRefresh — meta refresh в итоговом HTML (без
	// перехода: переходы съел T-048, осталось «сам на себя»).
	SPAReasonMetaRefresh SPAReason = "meta_refresh"
)

// SPAVerdict — решение о рендере итоговой страницы (ТЗ 5.5 п. 8).
// SPA=true — страница похожа на SPA-оболочку, потребитель публикует
// задачу в q:render (T-053); SPA=false — сырой HTML итоговый.
// Reasons — все сработавшие признаки (не только решающие), VisibleText
// — насчитанные видимые символы (байты) для метрик и настройки порога.
type SPAVerdict struct {
	SPA         bool        `json:"spa"`
	Reasons     []SPAReason `json:"reasons,omitempty"`
	VisibleText int         `json:"visible_text"`
}

// DetectSPA решает, похож ли итоговый HTML на SPA-оболочку (ТЗ 5.5
// п. 8). Аргумент — тело итогового ответа цепочки (T-048) в UTF-8
// (T-051); контекст URL не нужен: признаки не зависят от адреса, а
// переходы по Location и meta refresh уже выполнены цепочкой — здесь
// фиксируется только «нужен ли рендер», цель перехода неважна.
// Функция чистая: не изменяет и не копирует вход, решения принимает
// по таблице решений из комментария пакета.
func DetectSPA(html []byte) SPAVerdict {
	s := spaScan(html)
	v := SPAVerdict{VisibleText: s.visibleText}
	low := s.visibleText < SPAVisibleTextMax
	if low {
		v.Reasons = append(v.Reasons, SPAReasonLowText)
	}
	if s.container {
		v.Reasons = append(v.Reasons, SPAReasonRootContainer)
	}
	if s.noscript {
		v.Reasons = append(v.Reasons, SPAReasonNoScriptJS)
	}
	if s.jsRedirect {
		v.Reasons = append(v.Reasons, SPAReasonJSRedirect)
	}
	if s.metaRefresh {
		v.Reasons = append(v.Reasons, SPAReasonMetaRefresh)
	}
	// Таблица решений: сильные признаки (JS-редирект, meta refresh)
	// достаточны в одиночку; пара «мало текста + (контейнер или
	// noscript)» — по формулировке ТЗ; контейнер/noscript при богатом
	// тексте рендера не требуют (строки 6–7).
	v.SPA = s.jsRedirect || s.metaRefresh || (low && (s.container || s.noscript))
	return v
}

// spaSigns — признаки, насчитанные сканом.
type spaSigns struct {
	visibleText int  // видимые символы (байты) вне тегов/script/style/noscript
	container   bool // корневой контейнер root/app/__next/__nuxt
	noscript    bool // <noscript> с требованием JS
	jsRedirect  bool // строковый след JS-редиректа
	metaRefresh bool // <meta http-equiv="refresh"> в итоговом HTML
}

// Маркеры сканера (байтовые — для EqualFold без конвертаций, стиль
// redirect.go).
var (
	spaCommentOpen = []byte("<!--")
	spaCommentEnd  = []byte("-->")
	spaScriptEnd   = []byte("</script")
	spaStyleEnd    = []byte("</style")
	spaNoScriptEnd = []byte("</noscript")
	spaJSWord      = []byte("location")

	spaNameScript   = []byte("script")
	spaNameStyle    = []byte("style")
	spaNameNoScript = []byte("noscript")
	spaNameMeta     = []byte("meta")

	spaAttrID    = []byte("id")
	spaAttrEquiv = []byte("http-equiv")

	// spaJSReplace/spaJSAssign/spaJSHref — члены location, означающие
	// переход; строковые литералы не сравниваются, чтобы не аллоцировать.
	spaJSReplace = []byte("replace")
	spaJSAssign  = []byte("assign")
	spaJSHref    = []byte("href")

	spaRootIDs = []string{"root", "app", "__next", "__nuxt"}

	// spaNoScriptKeys — ключевые слова требования JS в <noscript>.
	// Слово «JavaScript» пишется одинаково в любом языке и покрывает
	// большинство формулировок (EN/DE/FR/ES/RU: «Включите JavaScript»,
	// «JavaScript is required»); остальные ключи — короткие формы с
	// «JS» типовых фраз RU/EN. Поиск регистронезависим (включая
	// кириллицу — bytes.EqualFold), поэтому достаточно строчных
	// вариантов. Список минимальный: расширение экзотикой не окупается,
	// noscript — усиливающий признак, его пропуск компенсируется парой
	// «текст + контейнер».
	spaNoScriptKeys = [][]byte{
		[]byte("javascript"),
		[]byte("enable js"),
		[]byte("js required"),
		[]byte("включите js"),
		[]byte("включить js"),
		[]byte("требуется js"),
		[]byte("нужен js"),
	}
)

// spaScan — один проход по HTML: текст между тегами считается
// видимым (байты, кроме пробельных), теги разбираются по мере
// встречаемости. Содержимое script/style пропускается до
// закрывающего тега (не видимый текст), noscript — проверяется на
// требование JS (тоже не видимый: браузер с включённым JS его не
// показывает), комментарии пропускаются. Незакрытые script/style/
// комментарий съедают остаток документа (хвост — не текст).
// Переиспользует примитивы T-048: redirectTagEnd (учёт кавычек),
// redirectIndexFold, redirectIsSpace.
func spaScan(b []byte) spaSigns {
	var s spaSigns
	s.jsRedirect = spaHasJSRedirect(b)
	i := 0
	for i < len(b) {
		lt := bytes.IndexByte(b[i:], '<')
		if lt < 0 {
			s.visibleText += spaVisibleCount(b[i:])
			return s
		}
		lt += i
		s.visibleText += spaVisibleCount(b[i:lt])
		if bytes.HasPrefix(b[lt:], spaCommentOpen) {
			e := bytes.Index(b[lt+len(spaCommentOpen):], spaCommentEnd)
			if e < 0 {
				return s // незакрытый комментарий — остаток не текст
			}
			i = lt + len(spaCommentOpen) + e + len(spaCommentEnd)
			continue
		}
		end := redirectTagEnd(b, lt)
		if end < 0 {
			return s // тег не закрыт — остаток считаем разметкой
		}
		closing := false
		nameStart := lt + 1
		if b[nameStart] == '/' {
			closing = true // закрывающий: содержимое за ним уже пропущено
			nameStart++
		}
		nameEnd := nameStart
		for nameEnd < end && !redirectIsSpace(b[nameEnd]) && b[nameEnd] != '/' {
			nameEnd++
		}
		name := b[nameStart:nameEnd]
		switch {
		case !closing && bytes.EqualFold(name, spaNameScript):
			// Содержимое скрипта не видимо; незакрытый — до конца.
			if ce := redirectIndexFold(b, end+1, spaScriptEnd); ce >= 0 {
				i = ce // разбор продолжится с закрывающего тега
			} else {
				return s
			}
		case !closing && bytes.EqualFold(name, spaNameStyle):
			if ce := redirectIndexFold(b, end+1, spaStyleEnd); ce >= 0 {
				i = ce
			} else {
				return s
			}
		case !closing && bytes.EqualFold(name, spaNameNoScript):
			cs := end + 1
			ce := redirectIndexFold(b, cs, spaNoScriptEnd)
			regionEnd := len(b)
			if ce >= 0 {
				regionEnd = ce
			}
			if limit := cs + spaNoScriptLimit; regionEnd > limit {
				regionEnd = limit
			}
			if regionEnd > cs && spaNoScriptDemand(b[cs:regionEnd]) {
				s.noscript = true
			}
			if ce < 0 {
				return s
			}
			i = ce
		default:
			if bytes.EqualFold(name, spaNameMeta) &&
				strings.EqualFold(spaTagAttr(b, nameEnd, end, spaAttrEquiv), "refresh") {
				// Признак ловится по всему документу, не только в head:
				// T-048 ограничен head по HTML-спецификации действия
				// тега, здесь признак описывает замысел страницы —
				// битая вёрстка его не отменяет.
				s.metaRefresh = true
			}
			if spaIsRootID(spaTagAttr(b, nameEnd, end, spaAttrID)) {
				s.container = true
			}
			i = end + 1
		}
	}
	return s
}

// spaVisibleCount — видимые символы (байты) фрагмента текста:
// всё, кроме пробельных (HTML-пробелы redirectIsSpace). Сущности не
// разворачиваются: «грубое удаление тегов» по ТЗ 13.2, порог с
// запасом переживает инфляцию &nbsp;.
func spaVisibleCount(b []byte) int {
	n := 0
	for _, c := range b {
		if !redirectIsSpace(c) {
			n++
		}
	}
	return n
}

// spaNoScriptDemand — есть ли в тексте <noscript> требование JS
// (ключи spaNoScriptKeys, регистр не важен).
func spaNoScriptDemand(region []byte) bool {
	for _, k := range spaNoScriptKeys {
		if redirectIndexFold(region, 0, k) >= 0 {
			return true
		}
	}
	return false
}

// spaIsRootID — значение атрибута id из списка корневых контейнеров
// (сравнение без учёта регистра: HTML id регистрозависимы, но
// «ROOT» вместо «root» на признак не влияет, ложных срабатываний
// практически нет).
func spaIsRootID(id string) bool {
	for _, want := range spaRootIDs {
		if strings.EqualFold(id, want) {
			return true
		}
	}
	return false
}

// spaTagAttr — значение атрибута want тега в диапазоне [p, end)
// (стиль redirectTagAttrs: имена без учёта регистра, значения в
// '…'/"…"/без кавычек, '/' между атрибутами пропускается). Аллокация
// только на найденном значении; "" — атрибута нет.
func spaTagAttr(b []byte, p, end int, want []byte) string {
	for p < end {
		for p < end && (redirectIsSpace(b[p]) || b[p] == '/') {
			p++
		}
		if p >= end {
			break
		}
		ns := p
		for p < end && b[p] != '=' && !redirectIsSpace(b[p]) {
			p++
		}
		name := b[ns:p]
		for p < end && redirectIsSpace(b[p]) {
			p++ // HTML5 допускает пробелы вокруг '=': id = "root"
		}
		vs, ve := p, p
		if p < end && b[p] == '=' {
			p++
			for p < end && redirectIsSpace(b[p]) {
				p++
			}
			if p < end && (b[p] == '\'' || b[p] == '"') {
				q := b[p]
				p++
				vs = p
				for p < end && b[p] != q {
					p++
				}
				ve = p
				if p < end {
					p++ // закрывающая кавычка
				}
			} else {
				vs = p
				for p < end && !redirectIsSpace(b[p]) {
					p++
				}
				ve = p
			}
		}
		if bytes.EqualFold(name, want) {
			return string(b[vs:ve])
		}
	}
	return ""
}

// spaHasJSRedirect — строковый скан документа на JS-редирект:
// слово «location» (регистр важен: в JS идентификаторы
// регистрозависимы, window.Location — не существует), перед которым
// нет байта имени JS и байта URL-контекста (?location=…,
// &location=…, /location=…), с продолжением:
//
//	= «не ==»      — location = "https://…" (и window.location = …)
//	.replace( …    — location.replace('/login')
//	.assign(  …    — location.assign('/login')
//	.href = «не ==» — location.href = '/login' (window/document —
//	                 частные случаи «голого» location)
//
// Сканируется весь HTML, а не только <script>: inline-обработчики
// (onclick="location.href='/x'") — тот же трамплин. Исполнение не
// выполняется (§9): это след замысла, а не доказательство перехода.
// Минимализм осознан: ложное срабатывание (статья с примером
// window.location в видимом тексте) стоит один рендер, пропуск —
// пустой контент как итог страницы. Члены вида location.pathname=…,
// сравнения ==/=== и взятие в кавычки-примеры без «=» не считаются.
func spaHasJSRedirect(b []byte) bool {
	for from := 0; from < len(b); {
		j := bytes.Index(b[from:], spaJSWord)
		if j < 0 {
			return false
		}
		j += from
		from = j + 1
		if j > 0 {
			c := b[j-1]
			if spaJSNameByte(c) {
				continue // xlocation, location_ — не цель перехода
			}
			switch c {
			case '=', '?', '&', '/', '#':
				continue // URL-контекст: ?location=…, /location/…
			}
		}
		m := j + len(spaJSWord)
		for m < len(b) && spaJSBlank(b[m]) {
			m++
		}
		if m >= len(b) {
			continue
		}
		switch b[m] {
		case '.':
			m++
			for m < len(b) && spaJSBlank(b[m]) {
				m++ // location . href — пробелы вокруг точки допустимы
			}
			ns := m
			ne := ns
			for ne < len(b) && spaJSNameByte(b[ne]) {
				ne++
			}
			p := ne
			for p < len(b) && spaJSBlank(b[p]) {
				p++
			}
			member := b[ns:ne]
			if (bytes.EqualFold(member, spaJSReplace) || bytes.EqualFold(member, spaJSAssign)) &&
				p < len(b) && b[p] == '(' {
				return true
			}
			if bytes.EqualFold(member, spaJSHref) &&
				p < len(b) && b[p] == '=' && (p+1 >= len(b) || b[p+1] != '=') {
				return true
			}
		case '=':
			if m+1 >= len(b) || b[m+1] != '=' {
				return true // location = "https://…"
			}
		}
	}
	return false
}

// spaJSNameByte — байт, входящий в JS-идентификатор (латиница,
// цифры, _, $).
func spaJSNameByte(c byte) bool {
	return c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' ||
		c >= '0' && c <= '9' || c == '_' || c == '$'
}

// spaJSBlank — пробелы, допустимые в JS между токенами.
func spaJSBlank(c byte) bool {
	switch c {
	case ' ', '\t', '\n', '\r', '\f', '\v':
		return true
	}
	return false
}
