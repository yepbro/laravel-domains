// Переключение DNS-обмена на резервные резолверы (T-107, остаток находки
// DNS-12 массового ревью; ТЗ 5.4: «Резолвер — локальный Unbound … Публичные
// резолверы — только как fallback (список в конфиге)») — декоратор
// FallbackResolver над интерфейсом Resolver, в том же слое политики, что
// и лимитёр T-035: транспорт T-034 остаётся «один клиент — один резолвер»,
// цепочка — забота декоратора выше него.
//
// Условие переключения — транспортный отказ: нижний резолвер вернул error
// после исчерпания собственных ретраев (в контракте T-034 error — только
// сеть: таймаут, отказ, обрыв; это и есть «исчерпание попыток локального»
// и «недостижимость» — с точки зрения обмена это одно и то же). Любой
// RCODE — ответ живого сервера, а не отказ: семантика ответов не меняется,
// NXDOMAIN → not_delegated и SERVFAIL → temp_error остаются решением
// потребителя (T-036/T-039), повторять вопрос резерву из-за SERVFAIL
// декоратор не делает. Отмена ctx — не авария резолвера: возвращается
// ошибка отмены без опроса резервов.
//
// Переключение — на каждый обмен, без запоминания состояния («мёртвый»
// резолвер не закорачивается): возобновление локального unbound не требует
// репробы/остывания, а гонка состояний между тысячами параллельных обменов
// не возникает вовсе. Цена — пока локальный мёртв, каждый обмен оплачивает
// его цикл ретраев (dns.timeout × (dns.retries+1)) до ухода на резерв:
// это видно метрикой переключений и ростом латентности, но конвейер
// продолжает работать и не раскладывает домены в temp_error.
//
// Место в стеке декораторов: под LimitResolver (cmd/dns-worker собирает
// LimitResolver(FallbackResolver(Client…))). Лимит параллелизма и токен QPS
// берутся один раз на весь цикл «локальный → резервы», поэтому переключение
// не тратит лимиты повторно; адаптив T-035 видит итог обмена — успех через
// резерв не снижает лимиты (конвейер работает), полное исчерпание цепи
// попадает в окно адаптива как обычная ошибка нижнего резолвера.

package dnsclient

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync/atomic"
	"time"
)

// FallbackTarget — элемент цепи резолверов: сам Resolver (обычно *Client)
// и его адрес. Адрес нужен только диагностике — сообщениям ошибок и логам
// стартовой пробы (probe.go); на работу переключения не влияет.
type FallbackTarget struct {
	// Addr — адрес резолвера host:port для сообщений ошибок и логов.
	Addr string
	// Resolver — резолвер цепи (не nil).
	Resolver Resolver
}

// FallbackStats — снимок состояния переключений для мониторинга
// (экспорт метрик — коллектор internal/dnsworker, T-107; здесь только
// данные, без зависимости от внутреннего/metrics — как LimitStats).
type FallbackStats struct {
	// Switches — обменов, отвеченных резервным резолвером (локальный
	// не ответил транспортно, резерв ответил). Растущий темп — сигнал
	// деградации локального unbound.
	Switches int64
	// Exhausted — обменов, провалившихся на всей цепи (не ответил никто);
	// каждый такой обмен — будущий temp_error потребителя (T-036/T-039).
	Exhausted int64
}

// FallbackResolver — цепь резолверов с переключением на резерв (T-107):
// первый элемент — основной (локальный unbound), остальные — резервные
// (dns.fallback_resolvers) в порядке конфига. Безопасен для одновременного
// использования из горутин (разделяемое состояние — только счётчики).
type FallbackResolver struct {
	primary   FallbackTarget
	fallbacks []FallbackTarget

	switches  atomic.Int64
	exhausted atomic.Int64
}

// compile-time check: *FallbackResolver — Resolver.
var _ Resolver = (*FallbackResolver)(nil)

// NewFallbackResolver собирает цепь: primary — основной резолвер,
// fallbacks — резервы в порядке опроса. Пустой список резервов допустим
// (изолированные окружения, dev): декоратор вырождается в основной
// резолвер, счётчики и контракт не меняются. Nil-резолвер — ошибка
// (программная, ловится на старте сервиса).
func NewFallbackResolver(primary FallbackTarget, fallbacks ...FallbackTarget) (*FallbackResolver, error) {
	if primary.Resolver == nil {
		return nil, errors.New("dnsclient: NewFallbackResolver: основной резолвер не задан")
	}
	for i, f := range fallbacks {
		if f.Resolver == nil {
			return nil, fmt.Errorf("dnsclient: NewFallbackResolver: резервный резолвер %d не задан", i+1)
		}
	}
	// Копия: слайс вызывающего не должен влиять на цепь после сборки.
	fbs := make([]FallbackTarget, len(fallbacks))
	copy(fbs, fallbacks)
	return &FallbackResolver{primary: primary, fallbacks: fbs}, nil
}

// Exchange выполняет вопрос по цепи резолверов (ТЗ 5.4): основной, при
// транспортном отказе — резервы по порядку до первого ответа. Итог
// (Result с любым RCODE) первого ответившего возвращается как есть —
// классификация ответов не меняется. Отмена ctx прерывает цепь сразу.
//
// Если не ответил никто — ошибка перечисляет все резолверы цепи и
// оборачивает причину последнего (errors.Is/As и IsTimeout работают по
// нему): классификация итоговой ошибки — по последнему опрошенному.
//
// Elapsed итога — время Exchange целиком, включая цикл ретраев отказавших
// резолверов; Attempts — обмены ответившего (Client не отдаёт число
// обменов с ошибкой, приписывать выдуманное — ложь в метрике).
func (r *FallbackResolver) Exchange(ctx context.Context, q Question) (*Result, error) {
	start := time.Now()
	res, err := r.primary.Resolver.Exchange(ctx, q)
	if err == nil {
		return res, nil
	}
	// Отмена самого вызова — не отказ резолвера: резервы не опрашиваются
	// (shutdown не должен обстреливать публичные DNS одним и тем же
	// вопросом).
	if ctxErr := ctx.Err(); ctxErr != nil {
		return nil, ctxErr
	}
	fails := []string{failDesc(r.primary, err)}
	lastErr := err
	for _, fb := range r.fallbacks {
		res, err := fb.Resolver.Exchange(ctx, q)
		if err == nil {
			r.switches.Add(1)
			res.Elapsed = time.Since(start)
			return res, nil
		}
		if ctxErr := ctx.Err(); ctxErr != nil {
			return nil, ctxErr
		}
		fails = append(fails, failDesc(fb, err))
		lastErr = err
	}
	r.exhausted.Add(1)
	return nil, fmt.Errorf("dnsclient: %s %s: не ответил ни один резолвер цепи (%d): %s; последний: %w",
		qTypeName(q.Type), q.Name, len(fails), strings.Join(fails, "; "), lastErr)
}

// Stats возвращает снимок счётчиков переключений (точки для мониторинга
// T-107; экспорт — internal/dnsworker).
func (r *FallbackResolver) Stats() FallbackStats {
	return FallbackStats{
		Switches:  r.switches.Load(),
		Exhausted: r.exhausted.Load(),
	}
}

// failDesc — диагностическое описание отказа одного резолвера цепи.
func failDesc(t FallbackTarget, err error) string {
	if t.Addr == "" {
		return err.Error()
	}
	return t.Addr + ": " + err.Error()
}
