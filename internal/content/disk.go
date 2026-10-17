// Контроль свободного места каталога контента (ТЗ 8; задачи T-046, T-080).
//
// ТЗ 8: «Переполнение диска: сервисы приостанавливают запись контента
// и сигнализируют об этом алертом; порог настраивается». Механика:
//
//   - перед записью нового блока (Put после промаха дедупликации) и
//     периодически (DiskWatchLoop) снимается statfs каталога контента;
//   - доля свободного места ниже порога content.disk_free_percent —
//     Put возвращает ErrStoreFull: воркер не сохраняет контент, но
//     результат пишет (контракт ТЗ 8 — пауза записи контента, не
//     результатов); чтение и дедуп-ссылки продолжаются — байты не
//     расходуются;
//   - сигнал алерта — типизированное событие StoreFullEvent в хук
//     PackOptions.OnStoreFull (счётчик для T-080) и Warn в логе
//     DiskWatchLoop; сам алерт (метрики, рассылка) — T-080, здесь не
//     реализуется;
//   - ENOSPC при физической записи пака остаётся последней линией
//     обороны (wrapPackWriteErr, T-042) и тоже даёт событие.
//
// Отдельного счётчика занятого места нет (как и у компактификации,
// T-045): занятое — сумма размеров пак-файлов, свободное берётся живым
// statfs, поэтому после ротации и удаления старых паков компактификацией
// порог честно перевычисляется и запись возобновляется сама.
package content

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"syscall"
	"time"
)

// DefaultDiskWatchInterval — интервал периодической проверки места
// DiskWatchLoop. ТЗ темп не задаёт: statfs — один дешёвый сисколл,
// 30 секунд дают алерту T-080 свежесть минуты без заметной нагрузки.
const DefaultDiskWatchInterval = 30 * time.Second

// Источники события StoreFullEvent (Source).
const (
	// StoreFullPut — Put упёрся в порог свободного места: новый блок
	// не дописан, данные не испорчены.
	StoreFullPut = "put"
	// StoreFullENOSPC — физическая запись пака закончилась ошибкой
	// ENOSPC (последняя линия обороны после порога).
	StoreFullENOSPC = "enospc"
	// StoreFullWatch — периодическая проверка DiskWatchLoop (вход в
	// паузу и восстановление).
	StoreFullWatch = "watch"
)

// SpaceStat — снимок места файловой системы каталога, байты.
type SpaceStat struct {
	// Avail — доступно процессам без root-резерва (statfs.Bavail):
	// сервисы краулера работают непривилегированными (ТЗ 10.1).
	Avail uint64
	// Total — ёмкость тома (statfs.Blocks).
	Total uint64
}

// SpaceChecker — источник сведений о месте файловой системы; выделен
// интерфейсом, чтобы юнит-тесты подменяли statfs без реального тома.
type SpaceChecker interface {
	// Space возвращает снимок места каталога dir.
	Space(dir string) (SpaceStat, error)
}

// OSStatfs — SpaceChecker поверх syscall.Statfs (реальная файловая
// система каталога контента).
type OSStatfs struct{}

// compile-time: OSStatfs — SpaceChecker.
var _ SpaceChecker = OSStatfs{}

// Space снимает statfs каталога dir.
func (OSStatfs) Space(dir string) (SpaceStat, error) {
	var st syscall.Statfs_t
	if err := syscall.Statfs(dir, &st); err != nil {
		return SpaceStat{}, err
	}
	bs := uint64(st.Bsize)
	return SpaceStat{
		Avail: st.Bavail * bs,
		Total: st.Blocks * bs,
	}, nil
}

// StoreFullEvent — типизированное событие «запись контента остановлена:
// свободное место каталога ниже порога» (ТЗ 8). Аргумент хука алерта
// PackOptions.OnStoreFull и логов DiskWatchLoop; счётчик/метрика — T-080.
type StoreFullEvent struct {
	// Space — снимок места на момент события (у события ENOSPC —
	// best-effort после неудачной записи).
	Space SpaceStat
	// Source — кто обнаружил: StoreFullPut / StoreFullENOSPC /
	// StoreFullWatch.
	Source string
	// Recovered — место вернулось за порог, пауза снята (только
	// StoreFullWatch: переход «полно → свободно»).
	Recovered bool
}

// OnStoreFullFunc — хук алерта T-080: вызывается на каждом Put,
// остановленном порогом (Source = put — счётчик событий), на ENOSPC
// и на переходах периодической проверки. Хук обязан быть быстрым и
// не паниковать: вызывается под Put.
type OnStoreFullFunc func(StoreFullEvent)

// spaceChecker — источник statfs хранилища (nil — реальная ОС; тесты
// пакета подменяют поле stat до первого использования).
func (s *PackStore) spaceChecker() SpaceChecker {
	if s.stat != nil {
		return s.stat
	}
	return OSStatfs{}
}

// diskFull — снимок места каталога контента и вердикт «свободного меньше
// порога». Общий источник для паузы Put (T-046) и периодической проверки
// DiskWatchLoop: одна формула — неотвергающая семантика. Total = 0 —
// экзотическая ФС без ёмкости: пауза по доле неопределима, отдаём «место
// есть» (ложная пауза остановила бы запись рабочего каталога).
func (s *PackStore) diskFull() (sp SpaceStat, full bool, err error) {
	if s.opts.DiskFreePercent <= 0 {
		return SpaceStat{}, false, nil // контроль выключен (конфиг задаёт всегда)
	}
	sp, err = s.spaceChecker().Space(s.opts.Dir)
	if err != nil {
		return SpaceStat{}, false, fmt.Errorf("content: свободное место %s: %w", s.opts.Dir, err)
	}
	if sp.Total == 0 {
		return sp, false, nil
	}
	full = 100*float64(sp.Avail)/float64(sp.Total) < s.opts.DiskFreePercent
	return sp, full, nil
}

// pauseIfDiskFull — пауза записи контента (ТЗ 8): места ниже порога —
// ErrStoreFull и событие в хук алерта. Вызывается Put-ом только перед
// дописыванием нового блока: дедуп-ссылки и чтение байтов не тратят и
// продолжают работать.
func (s *PackStore) pauseIfDiskFull() error {
	sp, full, err := s.diskFull()
	if err != nil {
		return err
	}
	if !full {
		return nil
	}
	s.fireStoreFull(StoreFullEvent{Space: sp, Source: StoreFullPut})
	return fmt.Errorf("%w: свободно %s из %s (%.1f%%) — ниже порога %.1f%% (content.disk_free_percent)",
		ErrStoreFull, humanBytes(sp.Avail), humanBytes(sp.Total),
		100*float64(sp.Avail)/float64(sp.Total), s.opts.DiskFreePercent)
}

// fireStoreFull отправляет событие в хук алерта, если тот установлен
// (nil — hook не задан, T-056 подключит на стыке сервиса).
func (s *PackStore) fireStoreFull(ev StoreFullEvent) {
	if s.opts.OnStoreFull != nil {
		s.opts.OnStoreFull(ev)
	}
}

// enospcEvent — событие после ENOSPC: снимок места best-effort (сама
// запись только что провалилась, statfs может не успеть отразить
// освобождение — событие информативно и без него).
func (s *PackStore) enospcEvent() StoreFullEvent {
	ev := StoreFullEvent{Source: StoreFullENOSPC}
	if sp, err := s.spaceChecker().Space(s.opts.Dir); err == nil {
		ev.Space = sp
	}
	return ev
}

// DiskWatchOptions — параметры DiskWatchLoop; нулевой Interval —
// DefaultDiskWatchInterval, nil Log — записи подавлены.
type DiskWatchOptions struct {
	Interval time.Duration
	Log      *slog.Logger
}

// DiskWatchLoop — периодическая проверка свободного места каталога
// контента (ТЗ 8): Warn в логе и событие StoreFullEvent при входе
// в паузу (даже если записей нет — алерт виден оператору), Info
// и Recovered-событие при восстановлении. Сигнал фронтовой (на
// переходах), не на каждом тике: Warn-шторм неделю полного диска —
// не информация. Ошибка statfs — Warn без смены состояния: следующий
// тик повторит. Возврат nil при отмене ctx — контракт OnRun каркаса
// (стиль CompactLoop).
func DiskWatchLoop(ctx context.Context, st *PackStore, opts DiskWatchOptions) error {
	if st == nil {
		return errors.New("content: контроль диска: хранилище не задано")
	}
	interval := opts.Interval
	if interval <= 0 {
		interval = DefaultDiskWatchInterval
	}
	log := opts.Log
	if log == nil {
		log = slog.New(slog.DiscardHandler)
	}

	full := false
	check := func() {
		sp, isFull, err := st.diskFull()
		switch {
		case err != nil:
			if ctx.Err() == nil {
				log.Warn("контроль свободного места каталога контента", "err", err)
			}
			return
		case isFull && !full:
			full = true
			st.fireStoreFull(StoreFullEvent{Space: sp, Source: StoreFullWatch})
			log.Warn("пауза записи контента: свободное место ниже порога",
				"avail", humanBytes(sp.Avail), "total", humanBytes(sp.Total),
				"threshold_percent", st.opts.DiskFreePercent)
		case !isFull && full:
			full = false
			st.fireStoreFull(StoreFullEvent{Space: sp, Source: StoreFullWatch, Recovered: true})
			log.Info("запись контента возобновлена: свободное место вернулось за порог",
				"avail", humanBytes(sp.Avail), "total", humanBytes(sp.Total))
		}
	}

	check() // первый проход сразу: дренаж накопленного за простой
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			check()
		}
	}
}

// humanBytes — человекочитаемый размер для сообщений об ошибках и логов
// (локальный аналог config.ByteSize.String: пакет контента не тянет
// конфигурацию ради форматирования).
func humanBytes(n uint64) string {
	switch {
	case n >= 1<<30:
		return fmt.Sprintf("%.1fGiB", float64(n)/(1<<30))
	case n >= 1<<20:
		return fmt.Sprintf("%.1fMiB", float64(n)/(1<<20))
	case n >= 1<<10:
		return fmt.Sprintf("%.1fKiB", float64(n)/(1<<10))
	default:
		return fmt.Sprintf("%dB", n)
	}
}
