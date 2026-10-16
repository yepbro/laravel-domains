// Package dnsworker — DNS-этап проверки домена (ТЗ 5.4): апекс (T-036),
// субдомены (T-037, этот файл) и wildcard-детект (T-038) поверх клиента
// internal/dnsclient (T-034).
//
// Файл subdom — проверка субдоменов домена (T-037; ТЗ 5.4 п. 2): по
// каждому включённому субдомену из справочника subdomains — запросы
// A/AAAA с разворачиванием цепочки CNAME; итог по каждому субдомену и
// агрегат DNSMask (бит = subdomains.id у субдоменов с адресами, ТЗ 5.4
// п. 4). Запись результата, статус домена и публикация q:http — T-039:
// здесь только сырые данные, включая признаки temp_error (RCode/Err).
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

// subdomMaxChain — предел длины цепочки CNAME: число переходов по записям
// CNAME от исходного имени субдомена (10-й переход выполняется, 11-й —
// обрыв с ErrSubdomCNAMEDepth).
//
// Обоснование предела. Легальные цепочки 1–3 звена (CDN, паркинги,
// переезды); клиентские резолверы придерживаются того же порядка: Go net
// ограничивает погоню за CNAME десятью переходами, рекурсор unbound —
// собственным пределом погони (злонамеренные зоны он завершает SERVFAIL
// ещё до нас). Предел гарантирует завершение против зон, отвечающих
// бесконечной лестницей уникальных имён: каждый переход — обмен с
// резолвером, без предела один субдомен расходовал бы бюджет времени и
// QPS неограниченно. Циклы (A→B→A) ловятся и раньше предела — множеством
// посещённых имён (subdomCheckOne).
const subdomMaxChain = 10

// Ошибки цепочки CNAME — sentinel-значения для errors.Is у потребителя
// (T-039): итог субдомена «адресов нет», причина фиксируется в
// SubdomResult.Err.
var (
	// ErrSubdomCNAMELoop — цепочка CNAME зациклилась (A→B→A): зона
	// сломана, терминального имени не существует.
	ErrSubdomCNAMELoop = errors.New("dnsworker: цикл в цепочке CNAME")
	// ErrSubdomCNAMEDepth — цепочка CNAME длиннее subdomMaxChain
	// (лестница уникальных имён); продолжение бессмысленно и дорого.
	ErrSubdomCNAMEDepth = errors.New("dnsworker: цепочка CNAME длиннее предела")
)

// SubdomResult — итог проверки одного субдомена. «Адреса есть/нет» —
// HasAddrs (запись бита dns_mask, ТЗ 5.4 п. 4); ошибки субдомена не
// прерывают остальные (ТЗ 5.4: итог по каждому субдомену отдельно).
type SubdomResult struct {
	// ID — subdomains.id, номер бита в DNSMask (0..62, ТЗ 5.10).
	ID int
	// Label — метка субдомена, "" — апекс домена.
	Label string
	// A — адреса IPv4 терминального имени цепочки (без повторов,
	// в порядке записей ответа).
	A []netip.Addr
	// AAAA — адреса IPv6 терминального имени; заполняется только при
	// askAAAA (ТЗ 5.4: AAAA отключаются конфигом без IPv6).
	AAAA []netip.Addr
	// Chain — развёрнутая цепочка CNAME: канонические (строчные FQDN
	// с завершающей точкой) цели по порядку переходов; Chain[0] —
	// цель CNAME исходного имени субдомена, последний элемент —
	// конечная цель. Пустая — субдомен не псевдоним.
	Chain []string
	// RCode — код последнего выполненного обмена (dnsclient: RCODE —
	// не ошибка). Сырой признак для T-039: SERVFAIL у субдомена —
	// кандидат temp_error домена, NXDOMAIN — имени нет (RFC 8020).
	// Нулевое значение NOERROR при Err транспортного уровня — обменов
	// с ответом не было, смотрите Err.
	RCode dnsclient.RCode
	// Err — ошибка субдомена: транспорт (сеть после ретраев dnsclient),
	// цикл или превышение глубины CNAME. nil у легальных ответов,
	// включая NXDOMAIN/NODATA/SERVFAIL (те — в RCode).
	Err error
}

// HasAddrs сообщает, есть ли у субдомена A- или AAAA-адреса — критерий
// бита dns_mask «резолвящийся субдомен» (ТЗ 5.4 п. 4). Адреса и Err не
// взаимоисключающи: AAAA-обмен мог упасть транспортом после найденных A.
func (r SubdomResult) HasAddrs() bool { return len(r.A) > 0 || len(r.AAAA) > 0 }

// CNAME возвращает цель первой записи CNAME исходного имени субдомена
// (прямой псевдоним) либо "" — поле cname задачи q:http (ТЗ 5.4 п. 4).
func (r SubdomResult) CNAME() string {
	if len(r.Chain) == 0 {
		return ""
	}
	return r.Chain[0]
}

// Target возвращает конечную цель цепочки CNAME (последнее имя, у
// которого искались адреса) либо "" для субдомена без псевдонима.
func (r SubdomResult) Target() string {
	if len(r.Chain) == 0 {
		return ""
	}
	return r.Chain[len(r.Chain)-1]
}

// SubdomReport — итог проверки субдоменов одного домена.
type SubdomReport struct {
	// Domain — имя домена, каким пришло на вход (punycode, ТЗ 5.2).
	Domain string
	// Mask — DNSMask с битами субдоменов, у которых есть адреса
	// (ТЗ 5.4 п. 4: «бит = subdomains.id для каждого резолвящегося
	// субдомена»). Пустая маска — ни одного адреса, вклад в
	// delegated_no_address (решение о статусе — T-039, ТЗ 5.4 п. 5).
	Mask model.DNSMask
	// Results — итоги по включённым субдоменам во входном порядке.
	Results []SubdomResult
}

// CheckSubdomains проверяет субдомены домена (T-037; ТЗ 5.4 п. 2): по
// каждому включённому субдомену — запрос A с разворачиванием цепочки
// CNAME и, при askAAAA, запрос AAAA терминального имени. Ответ — отчёт
// с итогом по каждому субдомену и агрегатом DNSMask.
//
// Источник списка субдоменов — таблица subdomains (T-039 читает и
// передаёт; здесь вход — срез строк model.Subdomain). Записи с
// Enabled = false пропускаются: входом может быть и полный справочник,
// и заранее отфильтрованный — результат один.
//
// Обход субдоменов последовательный, без собственного параллелизма и
// семафоров — осознанно: лимиты параллелизма/QPS и темп — декоратор
// Resolver задачи T-035 (dns.concurrency/max_qps, ТЗ 5.4), собственный
// семафор дублировал бы его и конфликтовал бы с настроенным; errgroup
// без лимита давал бы всплеск вопросов, который всё равно ждёт чужой
// лимитёр, и лишал бы порядок вопросов детерминизма в тестах. Пул
// сокетов dnsclient ограничивает одновременные обмены снизу.
//
// Ошибка отдельного субдомена не прерывает остальные (ТЗ 5.4: субдомены
// независимы): транспортный сбой, SERVFAIL, NXDOMAIN фиксируются в
// SubdomResult (Err/RCode), пачка идёт дальше; итоговое решение
// temp_error по совокупности признаков принимает T-039. Прерывает вызов
// целиком только отмена/дедлайн ctx — возврат (nil, ctx.Err()): отчёт
// частичной проверки не выдаётся, задача вернётся доставкой
// at-least-once (ТЗ 3.4).
func CheckSubdomains(
	ctx context.Context,
	resolve dnsclient.Resolver,
	domain string,
	subs []model.Subdomain,
	askAAAA bool,
) (*SubdomReport, error) {
	if strings.TrimSpace(domain) == "" {
		return nil, errors.New("dnsworker: пустое имя домена")
	}

	// Валидация входа до первого обмена: ошибки справочника — ошибка
	// вызова, а не отдельных субдоменов (id вне 0..62 ломают DNSMask,
	// кривые метки — вопрос с мусорным именем на каждый субдомен).
	enabled := make([]model.Subdomain, 0, len(subs))
	seenIDs := make(map[int]struct{}, len(subs))
	seenLabels := make(map[string]struct{}, len(subs))
	for _, s := range subs {
		if !s.Enabled {
			continue
		}
		if err := model.ValidateSubdomainID(int(s.ID)); err != nil {
			return nil, fmt.Errorf("dnsworker: субдомен id %d: %w", s.ID, err)
		}
		// Метка — одна DNS-метка (как в конфиге T-006 и задаче q:http);
		// punycode-метки (xn--…) проходят как обычные октеты.
		if len(s.Label) > 63 || strings.ContainsAny(s.Label, ". ") {
			return nil, fmt.Errorf("dnsworker: субдомен id %d: метка %q — одна DNS-метка до 63 октетов без точек и пробелов (пустая строка — апекс)", s.ID, s.Label)
		}
		if _, dup := seenIDs[int(s.ID)]; dup {
			return nil, fmt.Errorf("dnsworker: субдомен id %d повторяется во входе", s.ID)
		}
		if _, dup := seenLabels[s.Label]; dup {
			return nil, fmt.Errorf("dnsworker: метка субдомена %q повторяется во входе", s.Label)
		}
		seenIDs[int(s.ID)] = struct{}{}
		seenLabels[s.Label] = struct{}{}
		enabled = append(enabled, s)
	}
	if len(enabled) == 0 {
		return nil, errors.New("dnsworker: нет включённых субдоменов — проверять нечего")
	}

	report := &SubdomReport{Domain: domain, Results: make([]SubdomResult, 0, len(enabled))}
	withAddrs := make([]int, 0, len(enabled))
	for _, s := range enabled {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		res, err := subdomCheckOne(ctx, resolve, subdomFullName(domain, s.Label), askAAAA)
		if err != nil {
			return nil, err // только отмена/дедлайн ctx (см. subdomCheckOne)
		}
		res.ID = int(s.ID)
		res.Label = s.Label
		if res.HasAddrs() {
			withAddrs = append(withAddrs, int(s.ID))
		}
		report.Results = append(report.Results, res)
	}
	// id проверены ValidateSubdomainID выше — ошибка недостижима,
	// но игнорировать её нельзя.
	if err := report.Mask.Set(withAddrs...); err != nil {
		return nil, err
	}
	return report, nil
}

// subdomCheckOne проверяет одно имя (полное имя субдомена): фаза A с
// погоней по CNAME, затем фаза AAAA терминального имени. Ошибка
// возврата — только отмена/дедлайн ctx (прервать весь вызов); любые
// сбои DNS остаются в SubdomResult (Err/RCode).
//
// Отдельный вопрос типа CNAME не выдаётся: по RFC 1035 §3.3.1 ответ на
// вопрос к имени-псевдониму обязан содержать саму запись CNAME — она
// приходит в ответах A/AAAA, отдельный обмен дублировал бы её. Это и
// есть «запросы A, AAAA, CNAME» ТЗ 5.4 п. 2: записи всех трёх типов
// собираются из ответов, CNAME-цепочка разворачивается переходами.
func subdomCheckOne(ctx context.Context, resolve dnsclient.Resolver, name string, askAAAA bool) (SubdomResult, error) {
	var out SubdomResult
	cur := subdomCanon(name)
	visited := map[string]bool{cur: true}
	var res *dnsclient.Result

	for {
		// Ответ для cur: рекурсивный резолвер гонит цепочку сам и
		// прикладывает её к ответу целиком (RFC 1035 §3.3.1, §6/2.3
		// практики рекурсоров) — сначала ищем записи cur в уже
		// полученной секции answer, обмен только если ответ cur не
		// покрывает (цепочка оборвана NODATA/NXDOMAIN у цели).
		if res == nil || !subdomAnswerMentions(res.Answer, cur) {
			r, err := resolve.Exchange(ctx, dnsclient.Question{Name: cur, Type: dns.TypeA})
			if err != nil {
				if ctxErr := ctx.Err(); ctxErr != nil {
					return out, ctxErr
				}
				out.Err = fmt.Errorf("запрос A %s: %w", cur, err)
				return out, nil
			}
			res = r
			out.RCode = r.RCode
			if r.RCode != dnsclient.RCodeNOERROR {
				// NXDOMAIN/SERVFAIL/… — легальный ответ без адресов:
				// признак в RCode, адресов нет, ошибки Err нет.
				// Обоснование CNAME→NXDOMAIN (цель не существует):
				// субдомен-псевдоним существует (CNAME на месте),
				// адресов нет — бит dns_mask не ставится; для
				// статуса домена это вклад в «нет адресов» (ТЗ 5.4
				// п. 5), не во временная ошибка. NXDOMAIN цели не
				// опровергает существования самого субдомена
				// (RFC 8020 режет только subtree цели).
				return out, nil
			}
		}

		target, addrs, _ := subdomScanAnswer(res.Answer, cur)
		if target == "" {
			// Терминальное имя: A-записи собраны либо NODATA (имя
			// есть, записей этого типа нет).
			for _, a := range addrs {
				out.A = subdomAppendUnique(out.A, a)
			}
			break
		}
		if len(out.Chain) >= subdomMaxChain {
			out.Err = fmt.Errorf("%w: %d переходов от %s", ErrSubdomCNAMEDepth, subdomMaxChain, subdomCanon(name))
			return out, nil
		}
		if visited[target] {
			out.Err = fmt.Errorf("%w: %s → %s", ErrSubdomCNAMELoop, cur, target)
			return out, nil
		}
		out.Chain = append(out.Chain, target)
		visited[target] = true
		cur = target
	}

	// AAAA — один вопрос по терминальному имени: CNAME — свойство имени,
	// не типа запроса (RFC 2181 §10.1: на имени с CNAME других записей
	// не бывает), цепочка уже развёрнута фазой A — повторная погоня
	// дублировала бы обмены. Не спрашивается после сбоя (Err) и после
	// отрицательного ответа (не-NOERROR): NXDOMAIN безусловен для всех
	// типов (RFC 8020), SERVFAIL/отказ резолвера повторять вопросом
	// другого типа бессмысленно. Редкий CNAME, возникший у терминала
	// между фазами, не разворачивается — фиксируется отсутствие AAAA
	// у самого имени (состояние зоны изменилось, следующий эпизод
	// перепроверки увидит новую цепочку).
	if askAAAA && out.Err == nil && out.RCode == dnsclient.RCodeNOERROR {
		r, err := resolve.Exchange(ctx, dnsclient.Question{Name: cur, Type: dns.TypeAAAA})
		if err != nil {
			if ctxErr := ctx.Err(); ctxErr != nil {
				return out, ctxErr
			}
			out.Err = fmt.Errorf("запрос AAAA %s: %w", cur, err)
			return out, nil
		}
		out.RCode = r.RCode
		if r.RCode == dnsclient.RCodeNOERROR {
			_, _, addrs6 := subdomScanAnswer(r.Answer, cur)
			for _, a := range addrs6 {
				out.AAAA = subdomAppendUnique(out.AAAA, a)
			}
		}
	}
	return out, nil
}

// subdomFullName собирает полное имя субдомена: пустая метка — апекс
// (само имя домена), иначе метка.имя.
func subdomFullName(domain, label string) string {
	if label == "" {
		return domain
	}
	return label + "." + domain
}

// subdomCanon приводит DNS-имя к канонической форме сравнения: нижний
// регистр (DNS нечувствителен к регистру, RFC 1035 §2.3.3) и FQDN с
// завершающей точкой.
func subdomCanon(name string) string {
	return strings.ToLower(dns.Fqdn(name))
}

// subdomAnswerMentions сообщает, есть ли в секции answer записи,
// принадлежащие имени owner: решает, покрывает ли уже полученный ответ
// очередной шаг цепочки или нужен новый обмен.
func subdomAnswerMentions(answer []dns.RR, owner string) bool {
	for _, rr := range answer {
		if subdomCanon(rr.Header().Name) == owner {
			return true
		}
	}
	return false
}

// subdomScanAnswer выбирает из секции answer записи имени owner: цель
// CNAME (первая; у корректной зоны одна — RFC 2181 §10.1 запрещает
// сосуществование CNAME с другими записями имени) и адреса A/AAAA.
// CNAME приоритетнее адресов того же имени: резолвер, вернувший
// псевдоним, гонит цепочку дальше, октеты A рядом с CNAME — мусор
// сломанной зоны.
func subdomScanAnswer(answer []dns.RR, owner string) (target string, a, aaaa []netip.Addr) {
	for _, rr := range answer {
		if subdomCanon(rr.Header().Name) != owner {
			continue
		}
		switch v := rr.(type) {
		case *dns.CNAME:
			if target == "" {
				target = subdomCanon(v.Target)
			}
		case *dns.A:
			// A по определению 4 октета (RFC 1035 §3.4.1); To4 отсекает
			// мусорные неканонические RDATA.
			if addr, ok := netip.AddrFromSlice(v.A.To4()); ok {
				a = append(a, addr)
			}
		case *dns.AAAA:
			if addr, ok := netip.AddrFromSlice(v.AAAA); ok {
				aaaa = append(aaaa, addr)
			}
		}
	}
	return target, a, aaaa
}

// subdomAppendUnique добавляет адрес без повторов: DNS-ответы
// маленькие (десятки записей), линейная проверка дешевле map на каждый
// ответ, порядок записей сохраняется.
func subdomAppendUnique(dst []netip.Addr, addr netip.Addr) []netip.Addr {
	for _, x := range dst {
		if x == addr {
			return dst
		}
	}
	return append(dst, addr)
}
