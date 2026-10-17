package render

import (
	"context"
	"log/slog"
	"sync"
	"sync/atomic"
)

// Pool — параллельная обработка задач рендера (ТЗ 5.6: «один браузер,
// несколько изолированных контекстов, по умолчанию 2–4 параллельно»):
// одновременно не более Options.Contexts задач, каждая — в свежем
// изолированном профиле. Сессии не переиспользуются: контекст пересоздаётся
// на каждую задачу, состояние между страницами не сохраняется (ТЗ §9).
type Pool struct {
	b *Browser

	// slots — семафор параллелизма: ёмкость = Options.Contexts.
	slots chan struct{}
	// done закрывается при Close: ожидающие слот задачи отходят сразу.
	done chan struct{}
	// wg — задачи в полёте; Close их дорабатывает (ТЗ 3.4).
	wg sync.WaitGroup

	closed atomic.Bool

	// newSession — фабрика сессий; подменяется фейками в юнит-тестах.
	newSession func(ctx context.Context) (renderSession, error)
}

// renderSession — минимальный контракт сессии для пула (*Session ему
// удовлетворяет).
type renderSession interface {
	Run(ctx context.Context, task RenderTask) (RenderOutput, error)
	Close()
}

// NewPool — пул над запущенным браузером b; параллелизм берётся из
// b.opts.Contexts. Один пул на один браузер: перезапуск браузера (T-065) —
// новый браузер и новый пул над ним.
func NewPool(b *Browser) *Pool {
	p := &Pool{
		b:     b,
		slots: make(chan struct{}, b.opts.Contexts),
		done:  make(chan struct{}),
	}
	p.newSession = func(ctx context.Context) (renderSession, error) {
		return b.NewSession(ctx)
	}
	return p
}

// Browser возвращает браузер пула (счётчик страниц, Alive — для цикла
// воркера T-066 и перезапусков T-065).
func (p *Pool) Browser() *Browser { return p.b }

// Run выполняет задачу в свободном изолированном контексте, при нехватке —
// блокируется до освобождения слота или отмены ctx. Ошибка ClassContext
// (упавший контекст) повторяется один раз на новом контексте; ошибки
// страницы и недоступный браузер возвращаются наверх — повторная доставка
// остаётся уровню очереди (T-066), перезапуск браузера — T-065.
func (p *Pool) Run(ctx context.Context, task RenderTask) (RenderOutput, error) {
	p.wg.Add(1)
	defer p.wg.Done()
	select {
	case p.slots <- struct{}{}:
		// Close могла произойти в гонке с выбором кейса (select берёт
		// случайный из готовых): перепроверяем после захвата слота.
		select {
		case <-p.done:
			<-p.slots
			return RenderOutput{}, &Error{Class: ClassTask, URL: task.URL, Err: ErrClosed}
		default:
		}
	case <-p.done:
		return RenderOutput{}, &Error{Class: ClassTask, URL: task.URL, Err: ErrClosed}
	case <-ctx.Done():
		return RenderOutput{}, &Error{Class: ClassTask, URL: task.URL, Err: ctx.Err()}
	}
	defer func() { <-p.slots }()

	out, err := p.attempt(ctx, task)
	if err == nil {
		p.b.pages.Add(1)
		return out, nil
	}
	if ClassOf(err) == ClassContext && !p.closed.Load() {
		slog.Warn("контекст рендера умер, повтор задачи на новом контексте",
			"url", task.URL, "err", err.Error())
		if out, err = p.attempt(ctx, task); err == nil {
			p.b.pages.Add(1)
			return out, nil
		}
	}
	return out, err
}

// attempt — одна попытка: свежий изолированный контекст, задача, закрытие
// профиля независимо от результата (состояние не переживает страницу).
func (p *Pool) attempt(ctx context.Context, task RenderTask) (RenderOutput, error) {
	sess, err := p.newSession(ctx)
	if err != nil {
		return RenderOutput{}, err
	}
	defer sess.Close()
	return sess.Run(ctx, task)
}

// Close останавливает пул: новые задачи отклоняются (ErrClosed), текущие
// дорабатываются — каждая ограничена Options.Timeout (ТЗ 3.4), — затем
// останавливается браузер.
func (p *Pool) Close() {
	if !p.closed.CompareAndSwap(false, true) {
		return
	}
	close(p.done)
	p.wg.Wait()
	p.b.Close()
}
