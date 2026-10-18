// Reloader — перезагрузка набора правил Laravel по SIGHUP (ТЗ 5.8):
// «правила перезагружаются без перезапуска». Сигнал ловит каркас
// internal/service (Options.OnSIGHUP, make rules-reload) и вызывает
// HandleSIGHUP; сам Reloader от сигналов не зависит — точка входа
// перезагрузки инъектируется сервисом analyzer (T-071).

package rules

import (
	"log/slog"
	"sync"
)

// subBuf — ёмкость канала подписки: подписчик получает самый свежий набор;
// если он не забрал предыдущий, тот заменяется (медленный потребитель
// всё равно читает актуальное через Set).
const subBuf = 1

// Reloader хранит актуальный набор правил из каталога и оповещает
// подписчиков о смене. Безопасен для одновременного использования:
// потребители (воркеры analyzer) читают снапшот через Set, перезагрузка
// заменяет его целиком — правила внутри Set не мутируют.
type Reloader struct {
	dir    string
	logger *slog.Logger
	// load — шов для тестов (ошибки/подмены без файловой системы);
	// продакшн — LoadDir.
	load func(dir string) (*Set, error)

	mu   sync.Mutex
	set  *Set
	subs map[chan *Set]struct{}
}

// NewReloader загружает начальный набор из dir. Ошибка начальной загрузки
// возвращается вызывающему: сервис без валидного набора правил не стартует
// (иначе analyzer молча не находил бы Laravel).
func NewReloader(dir string, logger *slog.Logger) (*Reloader, error) {
	r := &Reloader{
		dir:    dir,
		logger: logger,
		load:   LoadDir,
		subs:   make(map[chan *Set]struct{}),
	}
	if r.logger == nil {
		r.logger = slog.New(slog.DiscardHandler)
	}
	set, err := r.load(dir)
	if err != nil {
		return nil, err
	}
	r.set = set
	r.logger.Info("набор правил загружен", "rules_dir", dir, "rules", len(set.Rules), "rules_version", set.Version)
	return r, nil
}

// Set — текущий снапшот набора (неизменяем; ссылку можно держать без
// блокировок и после перезагрузки).
func (r *Reloader) Set() *Set {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.set
}

// Reload перечитывает каталог. Ошибка загрузки (нет каталога, битый YAML,
// невалидный набор) НЕ меняет текущий набор и не останавливает сервис —
// продолжают работать прежние правила (Warn в лог); ошибка возвращается
// для тестов и вызывающих, которым она нужна. Успешная перезагрузка с тем
// же rules_version — no-op: набор поведенчески не изменился.
func (r *Reloader) Reload() error {
	set, err := r.load(r.dir)
	if err != nil {
		r.logger.Warn("перезагрузка правил не удалась — работает прежний набор",
			"rules_dir", r.dir, "err", err)
		return err
	}

	r.mu.Lock()
	if set.Version == r.set.Version {
		r.mu.Unlock()
		r.logger.Info("набор правил не изменился", "rules_version", set.Version)
		return nil
	}
	old := r.set
	r.set = set
	chans := make([]chan *Set, 0, len(r.subs))
	for ch := range r.subs {
		chans = append(chans, ch)
	}
	r.mu.Unlock()

	// Кэш компиляции живёт вместе с набором: чистится по смене
	// rules_version — записи паттернов ушедших правил (и осевшие
	// ошибки компиляции битых редакций) не копятся процессом.
	resetCompileCache()

	r.logger.Info("набор правил перезагружен",
		"rules_dir", r.dir, "rules", len(set.Rules),
		"rules_version", set.Version, "prev_version", old.Version)
	for _, ch := range chans {
		notify(ch, set)
	}
	return nil
}

// HandleSIGHUP — точка входа для service.Options.OnSIGHUP: перезагрузка
// по сигналу без рестарта сервиса. Ошибка уже залогирована Reload'ом —
// сигнал не источник ошибок, каркас продолжает работу.
func (r *Reloader) HandleSIGHUP() {
	_ = r.Reload()
}

// Subscribe — подписка на смену набора (например, планировщик переанализа
// T-074 запускает обход при смене rules_version). Канал буферизован: при
// пропуске уведомления актуальный набор всё равно доступен через Set.
// cancel отписывает; после его возврата канал больше не получает сообщений.
func (r *Reloader) Subscribe() (<-chan *Set, func()) {
	ch := make(chan *Set, subBuf)
	r.mu.Lock()
	r.subs[ch] = struct{}{}
	r.mu.Unlock()
	cancel := func() {
		r.mu.Lock()
		delete(r.subs, ch)
		r.mu.Unlock()
	}
	return ch, cancel
}

// notify доставляет набор подписчику без блокировки: канал полон —
// подписчик ещё не забрал предыдущее уведомление, заменяем его свежим.
func notify(ch chan *Set, set *Set) {
	select {
	case ch <- set:
	default:
		select {
		case <-ch:
		default:
		}
		select {
		case ch <- set:
		default:
		}
	}
}
