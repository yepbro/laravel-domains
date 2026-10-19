// Извлечение домена из произвольной строки (T-132 — расширение ТЗ 5.1,
// этап 16): реальные выгрузки регистраторов и публичные дампы несут
// посторонние данные («xxx.com 2029-11-11 TIMEWEB», CSV, URL), а не
// только полное имя домена. extractCandidates решает, подлежит ли
// строка извлечению, и собирает кандидатов; конвейер валидации имён
// (ТЗ 5.2 пп. 1–6) не меняется — извлечение лишь выбирает, какой токен
// строки идёт в него (интеграция — Validator.Line).
//
// Одиночная строка — без разделителей и без явной схемы http(s) —
// извлечению не подлежит и проходит прежний путь валидации без
// изменений: нулевая регрессия для сегодняшних файлов, точность причин
// одиночных строк сохранена.
package ingest

import (
	"net/url"
	"strings"
	"unicode"
)

// isSeparator — предикат разделителя токенов (T-132): пробельный символ
// (unicode.IsSpace — тот же предикат, что в idn.cleanup и парсере),
// запятая или точка с запятой. Пробел внутри строки — разделитель полей
// выгрузки, а не помарка в имени: склейка «при мер.рф» → «пример.рф»
// ушла вместе с прежней интерпретацией (кандидатом становится токен
// с точкой — мер.рф).
func isSeparator(r rune) bool {
	return unicode.IsSpace(r) || r == ',' || r == ';'
}

// hasSeparator — есть ли в строке хотя бы один разделитель токенов.
func hasSeparator(s string) bool {
	return strings.IndexFunc(s, isSeparator) >= 0
}

// splitTokens — разбиение строки на токены по разделителям isSeparator;
// пустые токены между соседними разделителями отбрасываются
// (strings.FieldsFunc): «"xxx.com",» даёт один токен-кандидат.
func splitTokens(line string) []string {
	return strings.FieldsFunc(line, isSeparator)
}

// isQuoteOrPunct — кавычки (обратная кавычка не входит в
// unicode.IsPunct — условная, отдельно) и пунктуация unicode, кроме
// дефисов (Pd) и соединителей (Pc, подчёркивание): обрамления CSV,
// концевые точки и скобки — не часть имени, а краевые «-»/«_» — части
// имени. Их срез подменял бы токен другим доменом («_dmarc.example.com»
// → «dmarc.example.com»): такое имя обязано идти в конвейер как есть и
// провалиться нормализацией (STD3), а не превращаться в чужое валидное.
func isQuoteOrPunct(r rune) bool {
	if r == '"' || r == '\'' || r == '`' {
		return true
	}
	return unicode.IsPunct(r) &&
		!unicode.Is(unicode.Pd, r) && !unicode.Is(unicode.Pc, r)
}

// trimToken снимает кавычки и пунктуацию с краёв токена («"xxx.com",» →
// xxx.com). Внутренние символы не трогаются: точки и дефисы внутри
// имени значимы, срезается только обрамление.
func trimToken(tok string) string {
	return strings.TrimFunc(tok, isQuoteOrPunct)
}

// hasSchemePrefix — начинается ли токен с явной схемы http(s):// —
// регистронезависимо (EqualFold, ASCII-префиксы) и без аллокации:
// извлечение спрашивает это у каждого токена, ingest потоковый
// (ТЗ 3.5), ToLower на каждый токен — лишняя работа.
func hasSchemePrefix(tok string) bool {
	for _, scheme := range [...]string{"http://", "https://"} {
		if len(tok) >= len(scheme) && strings.EqualFold(tok[:len(scheme)], scheme) {
			return true
		}
	}
	return false
}

// urlHost — хост токена с явной схемой http(s) (hasSchemePrefix, регистр
// схемы не важен). Токен-URL не является именем домена, но несёт его в
// хосте: net/url отделяет хост от схемы, userinfo, порта и пути
// (Hostname — порт срезан). ok = false — токен не начинается со схемы
// http(s): другие схемы («ftp://…») не заменяются — такой токен останется
// кандидатом и не пройдёт нормализацию. Ошибка разбора или пустой хост —
// пустая строка: домена в токене нет. IPv6-хост точек не содержит и
// кандидатом не станет; IPv4 (в том числе IPv4-mapped) — станет и будет
// отвергнут конвейером валидации, как и любой IP-литерал.
func urlHost(tok string) (host string, ok bool) {
	if !hasSchemePrefix(tok) {
		return "", false
	}
	u, err := url.Parse(tok)
	if err != nil {
		return "", true
	}
	return u.Hostname(), true
}

// extractCandidates — шаг извлечения T-132: возвращает кандидатов
// (токены, содержащие точку, в порядке слева направо) и признак того,
// что строка подлежит извлечению. Кандидат без точки — не домен
// (даты 2029-11-11, слова); проверку кандидата берёт на себя полный
// конвейер валидации — здесь только выбор токенов.
//
// Одиночная строка без разделителей: единственное исключение прежнего
// пути — явная схема http(s) (токен-URL доменом быть не может, прежняя
// причина «invalid» теряла хост с путём). Крайние пробелы снимаются до
// решения: строка из одних пробелов остаётся прежним случаем empty
// (парсер такие строки не доставляет, но контракт Line — для любого
// вызова).
func extractCandidates(line string) (cands []string, extract bool) {
	tok := strings.TrimFunc(line, unicode.IsSpace)
	if !hasSeparator(tok) {
		host, isURL := urlHost(tok)
		if !isURL {
			return nil, false // прежний путь: одиночный токен
		}
		if strings.Contains(host, ".") {
			return []string{host}, true
		}
		return nil, true // URL, но хост без точки — домена нет
	}
	for _, t := range splitTokens(tok) {
		t = trimToken(t)
		if host, isURL := urlHost(t); isURL {
			t = host // схема, порт и путь срезаны
		}
		if strings.Contains(t, ".") {
			cands = append(cands, t)
		}
	}
	return cands, true
}
