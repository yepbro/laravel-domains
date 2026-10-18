// Dns-worker: проверка апекса домена (T-036; ТЗ 5.4 п. 1, §6.2, §7).
//
// CheckApex — первый шаг DNS-этапа алгоритма ТЗ §7: запрос A апекса и
// классификация ответа по веткам §6.2. Здесь только запросы через
// dnsclient.Resolver и классификация: запись статуса/domains.dns_mask и
// публикация q:http — T-039; субдомены (A/AAAA/CNAME) и wildcard-детект —
// T-037/T-038, они наращивают конвейер после апекса.
//
// Решения классификации (зафиксированы по ТЗ):
//
//   - NXDOMAIN — единственный источник not_delegated: §6.2 прямо
//     определяет статус как «апекс NXDOMAIN», RFC 8020 распространяет
//     отрицательный ответ резолвера на всё поддерево имён — субдомены и
//     wildcard не запрашиваются (ApexStatus.Stop, «дальше не идти»);
//   - SERVFAIL, REFUSED и прочие коды (FORMERR, NOTIMP, …), а также сеть
//     после исчерпания ретраев dnsclient → temp_error: резолвер не
//     ответил о зоне. REFUSED — заявление резолвера о себе (ACL, лимит
//     запросов, перегрузка), а не об отсутствии делегирования: ложный
//     терминальный not_delegated по вине резолвера исключён, temp_error
//     возвращает домен в повтор (T-031);
//   - отмена/дедлайн ctx — ошибка вызывающему, а не итог домена: эпизод
//     не завершён, задача вернётся доставкой (lease/XAUTOCLAIM, ТЗ 3.4);
//   - AAAA апекса не запрашивается: ТЗ 5.4 п. 2 относит AAAA к субдоменам
//     (там же апекс — метка "", T-037); адреса этого шага — IPv4 из
//     A-записей апекса.
//
// NS «при необходимости» (§5.4 п. 1): пока A отвечает адресами, NS не
// нужен; NS запрашивается только при пустом A (NODATA) и подтверждает
// существование зоны — ветка «есть NS/ответ, но нет A/AAAA» §5.4 п. 5.

package dnsworker

import (
	"context"
	"errors"
	"fmt"
	"net/netip"
	"strings"

	"github.com/miekg/dns"

	"crawler/internal/dnsclient"
	"crawler/internal/model"
)

// ErrEmptyDomain — пустое/пробельное имя домена в пачке q:dns (мусорная
// или порченая строка domains). Ошибка детерминирована: повторная
// доставка не изменит исход, поэтому цикл воркера фиксирует её Warn-ом
// по домену, не роняя пачку (DNS-03).
var ErrEmptyDomain = errors.New("dnsworker: пустое имя домена — апекс обязателен")

// ApexStatus — ветка классификации ответа апекса (ТЗ 5.4 п. 1, §6.2, §7).
// Имена веток, совпадающих со статусами домена ТЗ 6.2, — имена статусов;
// ApexDelegated — промежуточный итог (финальный статус ставит HTTP-этап).
type ApexStatus int8

const (
	// ApexDelegated — NOERROR, в ответе есть A-записи: апекс делегирован,
	// адреса есть («есть адреса → q:http», §7). Конвейер продолжается:
	// субдомены (T-037), wildcard (T-038), запись результата (T-039);
	// адреса — вход HTTP-этапа (§5.4 п. 4, §5.5 п. 2).
	ApexDelegated ApexStatus = iota
	// ApexDelegatedNoAddress — NOERROR без A-записей (NODATA): имя
	// существует, адресов у апекса нет; зону подтверждает NS-запрос
	// («есть NS/ответ, но нет A/AAAA», §5.4 п. 5). Отдельная ветка по §7
	// («нет адресов → delegated_no_address»), но итоговый статус решает
	// T-039: адреса могут найтись у субдоменов (T-037), конвейер
	// продолжается.
	ApexDelegatedNoAddress
	// ApexNotDelegated — NXDOMAIN апекса: статус not_delegated (§6.2),
	// субдомены и wildcard не проверяются — RFC 8020 (NXDOMAIN доказывает
	// отсутствие всего поддерева имён). См. Stop.
	ApexNotDelegated
	// ApexTempError — SERVFAIL, REFUSED, иной код ответа либо сеть после
	// ретраев dnsclient: статус temp_error (§6.2, «требуется повтор»),
	// эпизод повторится целиком (T-031). Субдомены не проверяются.
	ApexTempError
)

// String возвращает имя ветки классификации ("delegated",
// "delegated_no_address", "not_delegated", "temp_error"); для кода вне
// перечня — "ApexStatus(<код>)".
func (s ApexStatus) String() string {
	switch s {
	case ApexDelegated:
		return "delegated"
	case ApexDelegatedNoAddress:
		return "delegated_no_address"
	case ApexNotDelegated:
		return "not_delegated"
	case ApexTempError:
		return "temp_error"
	default:
		return fmt.Sprintf("ApexStatus(%d)", int8(s))
	}
}

// Stop — явный признак «дальше не идти» (ТЗ 5.4 п. 1): NXDOMAIN завершает
// DNS-этап домена — субдомены (T-037) и wildcard (T-038) не проверяются
// (RFC 8020: отрицательный ответ резолвера распространяется на всё
// поддерево); temp_error тоже останавливает эпизод — при неотвечающем
// резолвере проверка субдоменов бессмысленна, повтор назначается целиком
// (T-031). Ветки делегирования продолжают конвейер (false).
func (s ApexStatus) Stop() bool {
	return s == ApexNotDelegated || s == ApexTempError
}

// DomainStatus возвращает код domains.status (ТЗ 6.2) для терминальных
// веток апекса: ApexNotDelegated → StatusNotDelegated (NXDOMAIN, RFC
// 8020), ApexTempError → StatusTempError. Для веток делегирования итог
// на этом шаге не решен — false: адреса субдоменов (T-037) дают либо
// задачу q:http, либо статус delegated_no_address (§5.4 п. 5, §7) —
// запись выполняет T-039.
func (s ApexStatus) DomainStatus() (model.Status, bool) {
	switch s {
	case ApexNotDelegated:
		return model.StatusNotDelegated, true
	case ApexTempError:
		return model.StatusTempError, true
	default:
		return model.StatusNew, false
	}
}

// ApexResult — структурированный итог проверки апекса: классификация,
// адреса апекса и признак подтверждающего NS-запроса. Запись в БД и
// публикация q:http — T-039; здесь только результат запросов.
type ApexResult struct {
	// Status — ветка классификации (см. ApexStatus).
	Status ApexStatus
	// Addresses — IPv4-адреса A-записей апекса в порядке ответа; вход
	// HTTP-этапа (§5.4 п. 4, §5.5 п. 2). Непуста только для ApexDelegated
	// (AAAA апекса — T-037, NS-записи адресами не являются).
	Addresses []netip.Addr
	// NSQueried — выполнялся ли подтверждающий NS-запрос (только ветка
	// NODATA, §5.4 п. 1) — диагностика и обоснование
	// ApexDelegatedNoAddress для логов T-039.
	NSQueried bool
}

// CheckApex проверяет апекс домена (ТЗ 5.4 п. 1): запрос A, при пустом
// ответе — подтверждающий NS, и классификация итога по §6.2/§7.
//
// domain — полное имя домена в punycode, как в сообщении q:dns (ТЗ 5.2);
// завершающая точка не обязательна (приводится к FQDN). Ветвление и
// признаки — в комментарии пакета и ApexStatus; результат не пишется
// в БД — запись статуса, dns_mask и публикация q:http — T-039.
//
// Ошибка возвращается только для детерминированных отказов: пустое имя
// домена и отмена/дедлайн ctx. Ошибки сети после ретраев dnsclient
// (контракт T-034: ошибка = попытки исчерпаны) и любые коды ответа —
// ветки классификации (ApexTempError), а не ошибка.
func CheckApex(ctx context.Context, r dnsclient.Resolver, domain string) (*ApexResult, error) {
	fqdn, err := apexFQDN(domain)
	if err != nil {
		return nil, err
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	// Шаг 1 — A апекса (ТЗ 5.4 п. 1).
	res, err := r.Exchange(ctx, dnsclient.Question{Name: fqdn, Type: dns.TypeA})
	if err != nil {
		if ctxErr := ctx.Err(); ctxErr != nil {
			return nil, ctxErr
		}
		return &ApexResult{Status: ApexTempError}, nil
	}
	switch res.RCode {
	case dnsclient.RCodeNOERROR:
		if addrs := apexAAddresses(res); len(addrs) > 0 {
			return &ApexResult{Status: ApexDelegated, Addresses: addrs}, nil
		}
		// NODATA: имя существует (NOERROR — RFC 2308), адресов нет;
		// делегирование подтверждает NS-запрос (шаг 2).
	case dnsclient.RCodeNXDOMAIN:
		// Домен не делегирован; поддерево не существует (RFC 8020) —
		// субдомены и wildcard не проверяются, NS не нужен.
		return &ApexResult{Status: ApexNotDelegated}, nil
	default:
		// SERVFAIL, REFUSED, FORMERR, NOTIMP и прочие — резолвер не
		// ответил о зоне: повтор эпизода (T-031).
		return &ApexResult{Status: ApexTempError}, nil
	}

	// Шаг 2 — NS «при необходимости» (ТЗ 5.4 п. 1): только когда A пуст.
	ns, err := r.Exchange(ctx, dnsclient.Question{Name: fqdn, Type: dns.TypeNS})
	if err != nil {
		if ctxErr := ctx.Err(); ctxErr != nil {
			return nil, ctxErr
		}
		return &ApexResult{Status: ApexTempError, NSQueried: true}, nil
	}
	out := &ApexResult{Status: ApexDelegatedNoAddress, NSQueried: true}
	switch ns.RCode {
	case dnsclient.RCodeNOERROR:
		// Зона (или имя) существует: «есть NS/ответ, но нет A/AAAA»
		// (§5.4 п. 5) — адресов нет и у апекса (шаг 1); NS-записи сами
		// по себе адресами не являются. Финальный статус — T-039 по
		// итогам субдоменов (§7): даже без адресов у апекса субдомены
		// (например www) могут дать адреса и задачу q:http.
	case dnsclient.RCodeNXDOMAIN:
		// Более свежий отрицательный ответ противоречит NOERROR шага 1 —
		// регистрация исчезла между запросами; последнее слово резолвера:
		// имени нет (RFC 8020), субдомены не проверяются.
		out.Status = ApexNotDelegated
	default:
		// SERVFAIL/REFUSED/… и на NS — резолвер не ответил, temp_error.
		out.Status = ApexTempError
	}
	return out, nil
}

// apexFQDN приводит имя домена к FQDN вопроса miekg/dns: завершающая
// точка (dns.Fqdn идемпотентна — уже приведённое имя не меняется).
// dnsclient.Exchange добавляет точку сам; здесь контракт фиксируется
// явно — вопрос всегда уходит в канонической форме. Проверка минимальна:
// имя обязано быть непустым и не корневой зоной; глубокая валидация —
// территория internal/idn на входе в систему (ТЗ 5.2), из q:dns имя
// приходит уже нормализованным (T-024).
func apexFQDN(domain string) (string, error) {
	name := dns.Fqdn(strings.TrimSpace(domain))
	if name == "." {
		return "", ErrEmptyDomain
	}
	return name, nil
}

// apexAAddresses собирает IPv4-адреса A-записей ответа в порядке
// следования. RecordsOf фильтрует по типу; цепочку CNAME разворачивает
// рекурсор (Unbound) и приносит целевые A-записи в ту же секцию answer —
// разворачивание вручную — территория субдоменов (T-037). Записи без
// валидного IPv4 пропускаются: в ответе исправного резолвера их не
// бывает, страховка от мусора не роняет классификацию (ветка NODATA).
func apexAAddresses(res *dnsclient.Result) []netip.Addr {
	rrs := res.RecordsOf(dns.TypeA)
	if len(rrs) == 0 {
		return nil
	}
	addrs := make([]netip.Addr, 0, len(rrs))
	for _, rr := range rrs {
		a, ok := rr.(*dns.A)
		if !ok {
			continue
		}
		addr, ok := netip.AddrFromSlice(a.A.To4())
		if !ok {
			continue
		}
		addrs = append(addrs, addr)
	}
	return addrs
}
