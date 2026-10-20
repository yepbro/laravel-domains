package render

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
)

// maxRestartsPerTask — предел перезапусков браузера на одну задачу: краш-луп
// Chromium не должен множить выполнения одной страницы; после исчерпания
// ошибка уходит наверх (повторная доставка — уровень очереди, T-066/T-067).
const maxRestartsPerTask = 2

// Manager — жизненный цикл Chromium воркера (T-065, ТЗ 5.6: «перезапуск
// браузера каждые N страниц и при превышении порога памяти»): старт,
// перезапуски и graceful stop. Владеет текущим поколением «браузер + пул»;
// задачи выполняет Run — конкурентно, как Pool.Run (вызывает цикл очередей,
// T-067).
//
// Смена поколения не ждёт задач старого пула: старый пул уходит на graceful
// stop фоновой горутиной (дорабатывает свои страницы — каждая ограничена
// Options.Timeout, ТЗ 3.4, — и закрывает свой браузер), новый браузер
// стартует сразу: простой из-за перезапуска не добавляется к чужим задачам,
// иначе перезапуск по памяти ждал бы самые «тяжёлые» страницы, съевшие
// бюджет. Задача, не начавшаяся в закрываемом пуле (ErrClosed), повторяется
// на новом поколении.
type Manager struct {
	opts    Options
	lifeCtx context.Context // контекст жизненного цикла браузеров (StartBrowser)

	// newBrowser/newPool — фабрики; подменяются фейками в юнит-тестах.
	newBrowser func(ctx context.Context, opts Options) (*Browser, error)
	newPool    func(b *Browser) *Pool

	restarts atomic.Int64
	closed   atomic.Bool

	mu sync.Mutex
	b  *Browser
	p  *Pool
}

// NewManager — менеджер над контекстом жизненного цикла ctx: на нём
// стартуют все браузеры менеджера, отмена ctx останавливает их, включая
// фоновые поколения прошлых перезапусков; дедлайн на ctx не накладывать
// (контракт StartBrowser). opts проходит валидацию при первом старте.
func NewManager(ctx context.Context, opts Options) *Manager {
	m := &Manager{opts: opts.withDefaults(), lifeCtx: ctx}
	m.newBrowser = StartBrowser
	m.newPool = NewPool
	return m
}

// Run выполняет задачу рендера на управляемом браузере: перед задачей
// гарантируется живое поколение (старт или перезапуск по триггерам — пороги
// страниц и памяти, смерть браузера), затем Pool.Run. Ошибки страницы и
// контекста возвращаются как есть (свой повтор на новом контексте пул уже
// сделал); смерть браузера посреди задачи (ClassBrowser) и закрытие пула
// сменой поколения до начала задачи (ErrClosed — задача не выполнялась) —
// перезапуск и повтор, не более maxRestartsPerTask раз.
func (m *Manager) Run(ctx context.Context, task RenderTask) (RenderOutput, error) {
	for attempt := 0; ; attempt++ {
		p, err := m.acquire()
		if err != nil {
			if errors.Is(err, ErrClosed) {
				err = &Error{Class: ClassTask, URL: task.URL, Err: err}
			}
			return RenderOutput{}, err
		}
		out, err := runAcquired(ctx, p, task)
		// Эквивалентный мутант (T-M08): вырезание тела if при err == nil
		// проваливается в следующий if, где третья дизъюнкция заведомо
		// истинна (ClassOf(nil) = ClassTask != ClassBrowser, а
		// errors.Is(nil, ErrClosed) = false) — возвращаются те же
		// (out, nil) с err == nil.
		// mutator-disable-next-line branch/if
		if err == nil {
			return out, nil
		}
		if m.closed.Load() || attempt >= maxRestartsPerTask ||
			(ClassOf(err) != ClassBrowser && !errors.Is(err, ErrClosed)) {
			return out, err
		}
	}
}

// runAcquired выполняет задачу на пуле, удерживая взятый в acquire слот
// поколения (wg.Done) до конца Pool.Run — см. обоснование в acquire.
func runAcquired(ctx context.Context, p *Pool, task RenderTask) (RenderOutput, error) {
	defer p.wg.Done()
	return p.Run(ctx, task)
}

// Start запускает первое поколение немедленно, а не лениво первой задачей:
// боевая сборка (cmd/render-worker) обязана упасть на старте при
// недоступном Chromium (ошибка OnRun → код 1 → systemd Restart=on-failure),
// а не молча перебирать попытки доставок q:render. На живом поколении —
// no-op; после Close — ErrClosed.
func (m *Manager) Start() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.closed.Load() {
		return ErrClosed
	}
	if reason, swap := m.swapReasonLocked(); swap {
		return m.restartLocked(reason)
	}
	return nil
}

// Alive — зонд потребления воркера (renderworker.Options.Alive): живо ли
// текущее поколение; мёртвое или отсутствующее заменяется прямо здесь
// (restartLocked → новый StartBrowser), поэтому после потери соединения
// или рестарта Chromium (host-юнит crawler-chromium.service,
// Restart=on-failure) зонд сам восстанавливает поколение — пауза
// потребления не зависает навсегда. Ошибка старта — false: потребление
// остаётся на паузе (доставки не расходуют попытки, T-025), следующий
// вызов зонда повторит старт.
func (m *Manager) Alive() bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.closed.Load() {
		return false
	}
	if reason, swap := m.swapReasonLocked(); swap {
		return m.restartLocked(reason) == nil
	}
	return true
}

// acquire возвращает пул текущего живого поколения, при необходимости
// меняя его (триггеры — swapReason). Конкурентные вызовы сериализуются
// мьютексом: смена поколения одна; проверка порогов под ним же — RSS
// читается раз в задачу (один проход /proc), не чаще.
//
// Взятый пул помечается в полёте (wg.Add) под тем же m.mu: retire
// сменившегося поколения тоже под m.mu, значит смена поколения не может
// проскочить между acquire и Pool.Run — а фоновый Close поколения никогда
// не ждёт WaitGroup с нулевым счётчиком, пока у задачи есть acquired-слот
// (Add из нуля параллельно Wait запрещён sync.WaitGroup). Слот снимается
// в runAcquired после Pool.Run.
func (m *Manager) acquire() (*Pool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.closed.Load() {
		return nil, ErrClosed
	}
	if reason, swap := m.swapReasonLocked(); swap {
		if err := m.restartLocked(reason); err != nil {
			return nil, err
		}
	}
	m.p.wg.Add(1)
	return m.p, nil
}

// swapReasonLocked — причина смены поколения, если она нужна. Порог памяти
// проверяется по RSS дерева процессов (Browser.RSS, proc.go); неизвестный
// RSS (remote-режим dev-стека) не триггерует — там жёсткий потолок держит
// mem_limit контейнера Chromium (ТЗ 3.2/10.1). Закрытый пул — страховка от
// гонки «поколение сменилось сразу после acquire»: закрытое поколение
// заменяется, а ошибки ErrClosed не копятся до исчерпания попыток задачи.
func (m *Manager) swapReasonLocked() (string, bool) {
	// Эквивалентный мутант (T-M08): вырезание тела if с m.b == nil
	// проваливается в !m.b.Alive() — nil-guard Alive возвращает false,
	// решение о смене поколения то же (true); отличается только
	// строка-причина, а она уходит исключительно в текст slog.Info
	// перезапуска (логирование, §4 docs/mutation-testing.md).
	// mutator-disable-next-line branch/if
	if m.b == nil {
		return "первый старт", true
	}
	if !m.b.Alive() {
		return "браузер недоступен", true
	}
	if m.p == nil || m.p.closed.Load() {
		return "пул закрыт", true
	}
	if m.b.NeedsRestart() {
		return fmt.Sprintf("порог страниц %d (отрендерено %d)",
			m.b.opts.RestartEveryPages, m.b.Pages()), true
	}
	// Порог памяти: RSS читается одним проходом /proc — значение идёт и в
	// проверку, и в сообщение (R-06); контроль отключён — /proc не читается
	// вовсе. Неизвестный RSS (remote-режим) не триггерует.
	if m.b.opts.MemoryThreshold > 0 {
		// Эквивалентны только мутанты левой половины условия (T-M08; тот
		// же вывод, что у Browser.OverMemory в browser.go): при пороге
		// >= 1 (guard выше) `rss > 0` неотличимо от `>= 0`/`> -1`/`true`
		// — rss == 0 и rss == -1 отбрасываются правой половиной (0/-1 >=
		// порог неверно), rss == 1 недостижим (RSS из /proc кратен
		// странице). Строчная гранулярность аннотации гасит и двух
		// убиваемых мутантов правой половины — trade-off §4
		// docs/mutation-testing.md: вырезание `rss >= порог` (осталось
		// бы `rss > 0` — перезапуск при любом положительном RSS) и
		// границу `>= -> >` (равенство RSS == порогу через живой /proc
		// недетерминированно). Вырезание правой половины закреплено
		// TestManagerMemoryRestartBoundaries (порог выше живого RSS —
		// без перезапуска); граница `>= -> >` детерминированного
		// прикрытия не имеет вовсе (равенство RSS == порогу через живой
		// /proc недостижимо) — из тестов она закрыта только для копии
		// выражения в Browser.OverMemory (TestBrowserOverMemory,
		// SIGSTOP-фиксация RSS).
		// mutator-disable-next-line expression/comparison, expression/remove, numbers/decrementer, numbers/incrementer
		if rss := m.b.RSS(); rss > 0 && rss >= m.b.opts.MemoryThreshold {
			return fmt.Sprintf("порог памяти %d MiB (RSS %d MiB)",
				// Логирование (§4 docs/mutation-testing.md): сдвиги и ±1
				// аргументов меняют только текст сообщения о причине
				// перезапуска, не решение о нём.
				// mutator-disable-next-line arithmetic/bitwise, numbers/decrementer, numbers/incrementer
				m.b.opts.MemoryThreshold>>20, rss>>20), true
		}
	}
	return "", false
}

// restartLocked стартует новое поколение. Старый пул выводится из работы
// синхронно (retire — новые задачи мимо него), а дорабатывает свои страницы
// и закрывает браузер уже фоновой горутиной: graceful (ТЗ 3.4) не
// блокирует старт — иначе перезапуск по памяти ждал бы самые долгие
// страницы, которые и съели бюджет. Неудачный старт возвращает ошибку
// (задача уходит наверх, следующая попытка Run повторит старт), при этом
// прежний пул всё равно уходит на закрытие: его браузер мёртв или исчерпал
// порог.
//
// Старт браузера идёт под m.mu — осознанно (R-07): пока стартует новое
// поколение, живого поколения нет, конкурентным задачам нечего приобретать
// — их ожидание на мьютексе эквивалентно ожиданию поколения, и вынос старта
// за критическую секцию (single-flight) ничего бы не сэкономил, добавив
// гонки установки поколения. Верхняя граница — Options.DialTimeout
// (remote) или чтение адреса процесса (local).
func (m *Manager) restartLocked(reason string) error {
	old := m.p
	if old != nil {
		old.retire()
	}
	m.b, m.p = nil, nil
	b, err := m.newBrowser(m.lifeCtx, m.opts)
	if err != nil {
		if old != nil {
			go old.Close()
		}
		return err
	}
	m.b = b
	m.p = m.newPool(b)
	if old == nil {
		// Логирование запуска chromium (§4 docs/mutation-testing.md).
		// mutator-disable-next-line statement/remove
		slog.Info("chromium запущен", "mode", m.mode(), "pid", b.rootPID)
		return nil
	}
	n := m.restarts.Add(1)
	go old.Close()
	// Логирование перезапуска chromium (§4 docs/mutation-testing.md).
	// mutator-disable-next-line statement/remove
	slog.Info("chromium перезапущен", "reason", reason,
		"restarts", n, "pid", b.rootPID)
	return nil
}

func (m *Manager) mode() string {
	// Мутант ветки меняет только текст лога запуска (remote назвался бы
	// локальным): mode() потребляется лишь slog.Info в restartLocked —
	// логирование, §4 docs/mutation-testing.md.
	// mutator-disable-next-line branch/if
	if m.opts.RemoteURL != "" {
		return "remote " + m.opts.RemoteURL
	}
	return "локальный процесс"
}

// Close останавливает менеджер: новые задачи отклоняются (ErrClosed →
// повторная доставка), текущее поколение дорабатывается (graceful, ТЗ 3.4)
// и останавливается вместе с браузером. Под мьютексом — только перевод в
// closed и открепление поколения: долгий дрен задач поколения (до
// Options.Timeout) не блокирует acquire, зонд Alive и Browser() — новые
// задачи получают ErrClosed сразу, не ожидая дрена (R-07). Если Close
// попадает на идущий перезапуск, он ждёт мьютекс до конца старта (до
// Options.DialTimeout): старт поколения короткоживущий, а браузер этого
// ещё не созданного поколения погасит отмена контекста жизненного цикла.
// Фоновые закрытия прошлых поколений завершаются независимо. Повторный
// вызов безвреден.
func (m *Manager) Close() {
	m.mu.Lock()
	// Эквивалентный мутант (T-M08): проваливание из неудачного CAS
	// исполняет хвост Close — p := m.p всегда nil (первый Close открепил
	// поколение, новое после closed не поднимается), повторные
	// открепление и Unlock ниже дают тот же результат. statement/remove
	// той же строки не гасим: вырезание именно Unlock вешает мьютекс —
	// убито тестом (Run после повторного Close).
	// mutator-disable-next-line branch/if
	if !m.closed.CompareAndSwap(false, true) {
		m.mu.Unlock()
		return
	}
	p := m.p
	m.b, m.p = nil, nil
	m.mu.Unlock()
	if p != nil {
		p.Close() // вне m.mu: дорабатывает задачи и гасит браузер
	}
}

// Restarts — число перезапусков браузера (диагностика; метрики — T-076+).
func (m *Manager) Restarts() int64 { return m.restarts.Load() }

// Browser — браузер текущего поколения (счётчики Pages/RSS — метрики);
// nil до первого старта или после Close.
func (m *Manager) Browser() *Browser {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.b
}
