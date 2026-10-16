// Wildcard-детект DNS-этапа (T-038; ТЗ 5.4 п. 3, §8).

package dnsworker

import (
	"context"
	"crypto/rand"
	"fmt"
	"net"
	"strings"

	"github.com/miekg/dns"

	"crawler/internal/dnsclient"
	"crawler/internal/model"
)

// Зона под wildcard: домены-парковки и заглушки хостингов отвечают
// A-записью на любое имя внутри домена («*.example.com. A»). Для краулера
// это значит, что формально «резолвящиеся» субдомены admin/crm/api/…
// синтезированы wildcard-ом и ведут на одну и ту же заглушку: проверять
// каждый по HTTP — заведомо одинаковый результат и лишние запросы (ТЗ 9:
// минимум запросов на сайт). Детект (ТЗ 5.4 п. 3): запрос A для
// <random>.<домен> со случайной меткой, которая гарантированно не
// существует; если случайное имя получает A-записи — зона под wildcard,
// субдомены, чьи адреса покрываются wildcard-ответом, из HTTP-проверки
// исключаются (кроме апекса и www).
//
// Состав API (две чистые операции вместо одной комбинированной —
// «сделай детект сам» выливается в двусмысленность nil-адресов):
//
//	DetectWildcard(ctx, resolver, домен) → WildcardInfo  — детект-запрос;
//	MarkWildcardSubdomains(wildcard-адреса, субдомены)   — сравнение/маска.
//
// Потребитель (сборка задачи q:http, T-039) вызывает их подряд; вход
// субдоменов обобщён (WildSubdomain) и не зависит от типов T-037.

const (
	// wildLabelLength — длина случайной метки детект-запроса: 16 символов
	// a-z0-9 ≈ 82,7 бита энтропии (обоснование — wildRandomLabel).
	wildLabelLength = 16
	// wildLabelAlphabet — алфавит метки: строчные буквы и цифры. Без
	// дефиса: не нужны правила его позиций в метке, а префикс punycode
	// «xn--» (ТЗ 5.2 п. 3) невозможен в принципе. Сравнение имён DNS
	// нечувствительно к регистру — метка сразу строчная.
	wildLabelAlphabet = "abcdefghijklmnopqrstuvwxyz0123456789"
)

// WildSubdomain — обобщённый вход этапа: результат DNS-запросов по
// одному субдомену. Не зависит от типов T-037: воркер собирает вход из
// своих структур — ID/Label из справочника subdomains, Addrs — адреса
// A/AAAA после разворачивания цепочки CNAME (T-037).
type WildSubdomain struct {
	// ID — subdomains.id, номер бита в dns_mask (0..62, ТЗ 5.10).
	ID int
	// Label — метка субдомена; "" — апекс, "www" — исключение ТЗ 5.4
	// п. 3. Метки в системе хранятся в punycode-нижнем регистре (ТЗ 5.2).
	Label string
	// Addrs — адреса субдомена (A и AAAA); пустой набор — не DNS-живой,
	// в HTTP-этап всё равно не попадает.
	Addrs []net.IP
}

// WildcardInfo — итог детект-запроса (DetectWildcard).
type WildcardInfo struct {
	// Detected — зона под wildcard: случайное имя получило A-записи.
	Detected bool
	// Addrs — адреса wildcard-ответа, конечные (включая случай
	// CNAME-обёртки); пусто при Detected=false.
	Addrs []net.IP
	// Label — использованная случайная метка (диагностика, логи).
	Label string
}

// WildcardResult — итог MarkWildcardSubdomains: маска субдоменов,
// исключаемых из HTTP-проверки.
type WildcardResult struct {
	// Mask — биты помеченных субдоменов; семантика та же, что у
	// domains.dns_mask (ТЗ 5.4 п. 4): бит = subdomains.id. Потребитель
	// (T-039) исключает эти субдомены из задачи q:http.
	Mask model.DNSMask
}

// IsWildcard сообщает, помечен ли субдомен id как wildcard.
func (r WildcardResult) IsWildcard(id int) bool { return r.Mask.Test(id) }

// IDs возвращает номера помеченных субдоменов по возрастанию.
func (r WildcardResult) IDs() []int { return r.Mask.IDs() }

// DetectWildcard выполняет детект-запрос wildcard для домена (ТЗ 5.4
// п. 3): A-запись для <random>.<домен> по резолверу этапа
// (dnsclient.Resolver — тот же интерфейс, что у запросов апекса и
// субдоменов T-036/T-037; лимиты параллелизма/QPS T-035 действуют на
// детект автоматически, отдельной настройки не требуется).
//
// Итог:
//
//   - Detected=true — случайное имя получило A-записи, зона под
//     wildcard; Addrs — адреса wildcard-ответа, Label — метка;
//   - Detected=false, err=nil — авторитетное «wildcard нет»: NXDOMAIN
//     на случайное имя (обычная зона, все субдомены честные —
//     детект-отрицание) либо NOERROR без A-записей (NODATA или
//     wildcard без адресов: пустой набор ни с чем не совпадает —
//     под него не подходит ни один DNS-живой субдомен);
//   - err≠nil — детект не удался: сеть/отмена (ошибка dnsclient) либо
//     неавторитетный ответ (SERVFAIL и прочие RCODE). Проверку домена
//     это не роняет: потребитель трактует ошибку как «детект не удался»
//     и продолжает БЕЗ фильтрации — все DNS-живые субдомены идут в
//     HTTP. Направление выбрано осознанно: ложный отрицательный детект
//     стоит нескольких лишних HTTP-запросов к заглушке, ложный
//     положительный — пропущенных сайтов (маска исключает субдомены из
//     проверки целиком); temp_error для домена тут не нужен — сам домен
//     уже классифицирован апекс-запросом T-036.
//
// domain — имя домена (апекс) в punycode, как в вопросах T-036;
// нормализация (ТЗ 5.2) — ответственность вызывающего.
func DetectWildcard(ctx context.Context, res dnsclient.Resolver, domain string) (WildcardInfo, error) {
	if strings.TrimSpace(domain) == "" {
		return WildcardInfo{}, fmt.Errorf("dnsworker: wildcard-детект: пустое имя домена")
	}
	label, err := wildRandomLabel()
	if err != nil {
		return WildcardInfo{}, fmt.Errorf("dnsworker: wildcard-детект %s: %w", domain, err)
	}
	name := label + "." + domain
	r, err := res.Exchange(ctx, dnsclient.Question{Name: name, Type: dns.TypeA})
	if err != nil {
		return WildcardInfo{Label: label}, fmt.Errorf("dnsworker: wildcard-детект %s: %w", name, err)
	}
	switch r.RCode {
	case dnsclient.RCodeNXDOMAIN:
		// Авторитетное «имени нет»: wildcard в зоне отсутствует,
		// резолвящиеся субдомены честные (детект-отрицание).
		return WildcardInfo{Label: label}, nil
	case dnsclient.RCodeNOERROR:
		addrs := wildAnswerAddrs(r)
		if len(addrs) == 0 {
			// NOERROR без A-записей: wildcard без адресов не
			// синтезирует адресов и субдоменам — сравнивать нечего,
			// итог тот же, что без wildcard.
			return WildcardInfo{Label: label}, nil
		}
		return WildcardInfo{Detected: true, Addrs: addrs, Label: label}, nil
	default:
		// SERVFAIL/REFUSED/… — не авторитетный ответ ни «да», ни «нет»:
		// честный «детект не удался» (политика потребителя — в комментарии
		// функции). RCODE остаётся в сообщении для диагностики.
		return WildcardInfo{Label: label}, fmt.Errorf(
			"dnsworker: wildcard-детект %s: неавторитетный ответ %s", name, r.RCode)
	}
}

// MarkWildcardSubdomains помечает субдомены, чьи адреса объясняются
// wildcard-ответом (ТЗ 5.4 п. 3): возвращает маску битов субдоменов,
// исключаемых из HTTP-проверки. Функция чистая — сети нет, детект-запрос
// уже сделан (DetectWildcard); wildAddrs — адреса wildcard-ответа
// (WildcardInfo.Addrs), возможно пустые: тогда не помечается никто.
//
// Критерий совпадения — подмножество: субдомен wildcard ⇔ у него есть
// адреса и все они входят в набор адресов wildcard-ответа (равенство
// множеств — частный случай). Обоснование (ТЗ 5.4 п. 3 «чьи адреса
// совпадают с wildcard-ответом», §8): синтез wildcard-ом возвращает
// ровно RRset wildcard-записи, поэтому ответ субдомена, целиком
// покрытый этим набором, неотличим от синтезированного — считаем его
// wildcard; любой адрес вне набора доказывает явную запись — это
// настоящий хост, он уходит на HTTP-проверку. Подмножество, а не
// строгое равенство, нужно и для урезанных ответов (резолвер вернул
// часть round-robin-набора wildcard — все адреса субдомена всё равно
// из wildcard-набора). Пустой набор адресов субдомена не помечается:
// «∅ ⊆ чего угодно» пометило бы всех подряд, а DNS-живой субдомен
// адреса иметь обязан.
//
// Апекс (Label "") и www (Label "www") не помечаются никогда, какими бы
// ни были их адреса — прямое правило ТЗ 5.4 п. 3: «…не проверяются по
// HTTP (кроме апекса и www)»: главные имена домена проверяются всегда,
// даже если формально отвечают wildcard-ом (парковка на апексе — всё
// равно единственный содержательный контент домена). Идентификация по
// метке, а не по id: "" = апекс и "www" — контракт справочника
// subdomains (миграция 000001: бит 0 — апекс, бит 1 — www), метки
// хранятся строчными (ТЗ 5.2).
//
// Ошибка — только недопустимый ID субдомена (вне 0..62: модель маски,
// ТЗ 5.10): вход обязан происходить из справочника subdomains этим
// ограничением связанного, ошибка сигнализирует о баге вызывающего;
// частично заполненная маска при этом не возвращается.
func MarkWildcardSubdomains(wildAddrs []net.IP, subs []WildSubdomain) (WildcardResult, error) {
	var mask model.DNSMask
	for _, sub := range subs {
		if !wildExplainedByWildcard(sub, wildAddrs) {
			continue
		}
		if err := mask.Set(sub.ID); err != nil {
			return WildcardResult{}, fmt.Errorf("dnsworker: wildcard-пометка субдомена %d (%q): %w",
				sub.ID, sub.Label, err)
		}
	}
	return WildcardResult{Mask: mask}, nil
}

// wildExplainedByWildcard — критерий пометки одного субдомена (см.
// MarkWildcardSubdomains): не апекс и не www, адреса есть и каждый
// входит в набор адресов wildcard-ответа.
func wildExplainedByWildcard(sub WildSubdomain, wildAddrs []net.IP) bool {
	if sub.Label == "" || sub.Label == "www" {
		return false // ТЗ 5.4 п. 3: кроме апекса и www
	}
	if len(sub.Addrs) == 0 || len(wildAddrs) == 0 {
		return false
	}
	for _, addr := range sub.Addrs {
		if !wildContainsIP(wildAddrs, addr) {
			return false // адрес вне wildcard-набора — явная запись
		}
	}
	return true
}

// wildContainsIP ищет адрес в наборе. net.IP.Equal, а не сравнение
// байтов: одна и та же точка приходит 4-октетной формой (запись A) или
// 16-октетной (AAAA, IPv4-mapped) — Equal их не различает.
func wildContainsIP(addrs []net.IP, ip net.IP) bool {
	for _, a := range addrs {
		if a.Equal(ip) {
			return true
		}
	}
	return false
}

// wildRandomLabel генерирует случайную метку детект-запроса.
//
// crypto/rand, а не math/rand с сидом: метка обязана гарантированно не
// существовать, и источник не должен требовать управления сидом (общий
// math/rand до Go 1.20 без сида детерминирован, локальный — лишнее
// состояние на воркер, глобальный к тому же под мьютексом). Стоимость —
// один getrandom(2) на домен, на фоне DNS-обмена пренебрежима; ошибка
// чтения — явная ошибка детекта, потребитель продолжит без фильтрации.
//
// 16 символов a-z0-9 — log2(36^16) ≈ 82,7 бита энтропии: вероятность
// совпадения с существующей записью даже в зоне с миллионом имён
// ≈ 10^6·36^-16 ≈ 10^-19. Длина ≤ 63 октет — лимит метки (ТЗ 5.2 п. 4)
// соблюдён с запасом. Отображение октета в символ взятием остатка:
// модульный сдвиг (256 mod 36 = 4) смещает вероятности символов меньше
// чем на 2% — для задачи «не столкнуться с существующим именем»
// несущественно.
func wildRandomLabel() (string, error) {
	buf := make([]byte, wildLabelLength)
	if _, err := rand.Read(buf); err != nil {
		return "", fmt.Errorf("случайная метка: %w", err)
	}
	label := make([]byte, wildLabelLength)
	for i, b := range buf {
		label[i] = wildLabelAlphabet[int(b)%len(wildLabelAlphabet)]
	}
	return string(label), nil
}

// wildAnswerAddrs собирает конечные A-адреса ответа детект-запроса.
//
// Wildcard нередко CNAME-обёртка (random → CNAME → A, например на
// парковку хостинга). Догонять цель отдельными запросами не нужно:
// детект идёт через локальный Unbound — рекурсивный резолвер (ТЗ 5.4),
// который возвращает в секции answer уже развёрнутую цепочку (CNAME +
// A-записи цели, RFC 1034 §4.3.2), — все A-записи ответа и есть адреса
// wildcard-ответа, сравнивать надо именно конечные адреса, а не CNAME.
// Свои запросы добавили бы по обмену на каждый wildcard-домен (350 млн
// доменов; ТЗ 5.4 — минимум запросов) без новой информации; если же
// цель без адресов (CNAME без A), wildcard адресов не даёт —
// DetectWildcard вернёт Detected=false.
func wildAnswerAddrs(r *dnsclient.Result) []net.IP {
	var addrs []net.IP
	for _, rr := range r.Answer {
		if a, ok := rr.(*dns.A); ok {
			addrs = append(addrs, a.A)
		}
	}
	return addrs
}
