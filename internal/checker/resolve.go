// Цепь резолверов живой проверки одного домена (T-119/T-123) — своя
// семантика отказа, отличная от массового конвейера. FallbackResolver
// (internal/dnsclient, T-107) переключается только на транспортном
// отказе: RCODE-ответ живого сервера (SERVFAIL) остаётся решением
// потребителя — при деградации unbound массовый crawl не должен
// обстреливать публичные DNS теми же вопросами. Ручная проверка —
// единичный вопрос человека: он ждёт ответ, а не классификацию отказа,
// поэтому цепь здесь эскалирует и на RCODE-отказах (SERVFAIL/REFUSED):
// dev-testbed unbound на всё вне тестовых зон отвечает SERVFAIL
// (намеренная герметичность окружения), прод-unbound так не отвечает;
// резервы — dns.fallback_resolvers (dev-форма — публичные резолверы,
// бой — по конфигурации). NXDOMAIN — законный итог «имени нет»,
// эскалации не требует; массовый конвейер (dns-worker) этой цепью
// не пользуется.
package checker

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"crawler/internal/config"
	"crawler/internal/dnsclient"
)

// escalateTarget — элемент цепи: резолвер и его адрес (только
// диагностика сообщений об ошибках).
type escalateTarget struct {
	addr string
	res  dnsclient.Resolver
}

// escalateResolver — цепь резолверов с эскалацией на отказ (см. комментарий
// пакета). Первый элемент — основной (локальный unbound, dns.resolvers[0]),
// остальные — dns.fallback_resolvers в порядке конфигурации. Безопасен для
// одновременного использования из горутин (состояния нет).
type escalateResolver struct {
	targets []escalateTarget
}

// compile-time check: *escalateResolver — dnsclient.Resolver.
var _ dnsclient.Resolver = (*escalateResolver)(nil)

// Exchange идёт по цепи до первого полезного итога. Полезный итог — ответ
// с любым RCODE, кроме SERVFAIL и REFUSED (NOERROR, NXDOMAIN, …): его
// семантику интерпретирует потребитель (T-036/T-039), цепь не решает.
// SERVFAIL/REFUSED и транспортный отказ — переход к следующему резолверу;
// отмена ctx прерывает цепь сразу (shutdown не обстреливает резервы).
//
// Если полезного итога не дал никто — возвращается последний RCODE-ответ
// (честная классификация: SERVFAIL → temp_error у потребителя), а при
// чисто транспортных отказах — ошибка, перечисляющая цепь и оборачивающая
// причину последнего (errors.Is/As работают по нему).
func (r *escalateResolver) Exchange(ctx context.Context, q dnsclient.Question) (*dnsclient.Result, error) {
	var (
		lastRes *dnsclient.Result
		lastErr error
		fails   []string
	)
	for _, t := range r.targets {
		res, err := t.res.Exchange(ctx, q)
		if err == nil && res.RCode != dnsclient.RCodeSERVFAIL && res.RCode != dnsclient.RCodeREFUSED {
			return res, nil
		}
		if ctxErr := ctx.Err(); ctxErr != nil {
			return nil, ctxErr
		}
		switch {
		case err != nil:
			lastRes, lastErr = nil, err
			fails = append(fails, t.desc(err))
		default:
			lastRes, lastErr = res, nil
			fails = append(fails, t.desc(res.RCode))
		}
	}
	if lastRes != nil {
		return lastRes, nil
	}
	return nil, fmt.Errorf("checker: %s: не ответил ни один резолвер цепи (%d): %s; последний: %w",
		q.Name, len(fails), strings.Join(fails, "; "), lastErr)
}

// desc — диагностическое описание отказа одного элемента цепи.
func (t escalateTarget) desc(v any) string {
	if t.addr == "" {
		return fmt.Sprint(v)
	}
	return t.addr + ": " + fmt.Sprint(v)
}

// newResolverChain собирает цепь для живой проверки: основной клиент
// (dns.resolvers[0] — локальный unbound) плюс по клиенту на каждый
// dns.fallback_resolvers. Пустой список резервов вырождается в единственный
// основной клиент — поведение до цепи (T-119). Возвращаемый close закрывает
// все клиенты цепи (один вызов, ошибки объединены).
func newResolverChain(ctx context.Context, cfg config.DNS) (dnsclient.Resolver, func() error, error) {
	primaryOpts := dnsclient.NewOptions(cfg)
	primary, err := dnsclient.NewClient(ctx, primaryOpts)
	if err != nil {
		return nil, nil, fmt.Errorf("checker: основной резолвер %s: %w", primaryOpts.Addr, err)
	}
	clients := []*dnsclient.Client{primary}
	closeAll := func() error {
		var errs []error
		for _, c := range clients {
			if err := c.Close(); err != nil {
				errs = append(errs, err)
			}
		}
		return errors.Join(errs...)
	}
	targets := []escalateTarget{{addr: primaryOpts.Addr, res: primary}}
	for _, addr := range cfg.FallbackResolvers {
		opts := dnsclient.NewOptions(cfg)
		opts.Addr = addr
		c, err := dnsclient.NewClient(ctx, opts)
		if err != nil {
			_ = closeAll()
			return nil, nil, fmt.Errorf("checker: резервный резолвер %s: %w", addr, err)
		}
		clients = append(clients, c)
		targets = append(targets, escalateTarget{addr: addr, res: c})
	}
	return &escalateResolver{targets: targets}, closeAll, nil
}
