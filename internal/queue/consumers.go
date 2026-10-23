// Гигиена консьюмеров групп (T-155): каждое чтение XREADGROUP закрепляет
// записи за потребителем группы (имя — процесс воркера с pid), и Redis
// не вычищает консьюмеры сам — после рестартов (деплои волн, падения,
// OOM-киллеры) они копятся в группах навсегда: 4 деплоя волны 21
// оставили 11 мёртвых pid-консьюмеров в q:render и 13 в q:http
// (docs/operations.md §5). Сами по себе они — строки метаданных
// (копейки памяти), но засоряют разбор XINFO CONSUMERS/XPENDING при
// инцидентах, а часть мёртвых консьюмеров держит висящий pending
// (430 записей на http-worker-455745): такие записи спасаются только
// клеймом живого воркера (XAUTOCLAIM, T-153) — чистка их НЕ трогает.
//
// Механизм — периодический XGROUP DELCONSUMER строго пустых консьюмеров
// (pending = 0 и неактивны ≥ redis.consumer_idle):
//
//   - pending ≠ 0 — никогда: DELCONSUMER удаляет консьюмера вместе с его
//     PEL-записями — недоставка навсегда (штучные задачи q:http/q:render/
//     q:analyze не восстанавливаются lease — только клеймом из PEL;
//     источник истины PostgreSQL спасает лишь будущую перепроверку
//     recheck, месяцы). Висящий pending мёртвого консьюмера разбору
//     клеймом не мешает (XAUTOCLAIM перезакрепляет записи на живого
//     независимо от владельца) — удалять его незачем;
//   - неактивность (Idle из XINFO CONSUMERS, время с последнего
//     взаимодействия консьюмера) — защита от удаления консьюмера живого
//     воркера под нагрузкой: активный этап трогает консьюмера пачками
//     чтений, затишье между доставками — секунды. Порог 2 ч закрывает
//     всё, кроме честного простоя этапа; живой консьюмер пустого стрима
//     может простаивать дольше (XREADGROUP без записей консьюмера не
//     трогает — idle растёт), но его удаление безвредно: имя
//     пересоздаётся первым же чтением с записями, а пустых чтений
//     консьюмер не порождает.
//
// Гонка «оживления»: между XINFO CONSUMERS и DELCONSUMER затаившийся
// воркер может получить доставку (появилась запись, XREADGROUP закрепил
// её за консьюмером-кандидатом) — DELCONSUMER унёс бы его PEL. Окно
// сужено перепроверкой: непосредственно перед удалением XPENDING EXTENDED
// по одному консьюмеру (Start «-», End «+», Count 1, фильтр Consumer)
// подтверждает пустоту PEL — окно «оживления» из всего прохода XINFO по
// группе сжимается до пары команд на кандидата. Ответ DELCONSUMER
// (число удалённых вместе с консьюмером PEL-записей) проверяется: > 0 —
// гонка всё же случилась, возвращается ошибкой для Warn-лога цикла —
// записи требуют ручного разбора (перевыпуска по идентификаторам из
// задачи нет, см. выше про недоставку).
//
// Цикл CleanupConsumersLoop — сиблинг TrimLoop (T-141): живёт в сервисе
// scheduler (одиночный хозяин гигиены стримов очередей), период — тот же
// redis.trim_interval (проход дёшев: XINFO CONSUMERS на стрим × группу,
// остальные команды — только по консьюмерам-кандидатам, поэтому
// отдельная ручка темпа не нужна), порог неактивности —
// redis.consumer_idle. Пока планировщик остановлен, мёртвые консьюмеры
// копятся безвредно (метаданные; чистка догонит).

package queue

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sort"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
)

// DefaultConsumerIdle — порог неактивности консьюмера для удаления
// (T-155): 2 ч с запасом поверх затишья между доставками активного этапа
// (секунды) и короче междеплойного интервала (мёртвый после рестарта
// консьюмер вычищается в ближайшие проходы). Настройка —
// redis.consumer_idle.
const DefaultConsumerIdle = 2 * time.Hour

// DefaultConsumerInterval — период проходов чистки (CleanupConsumersLoop).
// Минута — та же ценовая категория гигиены, что у освобождения префикса
// (DefaultTrimInterval): проход — XINFO CONSUMERS на стрим × группу плюс
// команды только по консьюмерам-кандидатам; боевая проводка передаёт
// redis.trim_interval явно (один темп дворников планировщика).
const DefaultConsumerInterval = time.Minute

// WorkingGroups — рабочие пары стрим → группа потребителей (T-155): у
// каждого этапа одна группа на стрим (ТЗ 4.2; инвариант механизма
// освобождения T-141 «одна consumer group на рабочий стрим»,
// docs/redis.md §2). Имена зеркалируют DefaultGroup пакетов воркеров
// (пакет queue не может импортировать их напрямую — воркеры строятся
// поверх queue, цикл импорта); соответствие сверяет пин-тест
// consumers_pin_test.go (внешний пакет queue_test). q:dead не входит:
// стрим без группы (ручной разбор) — консьюмеров нет.
func WorkingGroups() map[string]string {
	return map[string]string{
		StreamDNS:     "dns-worker",    // dnsworker.DefaultGroup
		StreamHTTP:    "http-worker",   // httpworker.DefaultGroup
		StreamRender:  "render-worker", // renderworker.DefaultGroup
		StreamAnalyze: "analyzer",      // analyzer.DefaultGroup
	}
}

// consumerGroups — окно go-redis для гигиены консьюмеров: XINFO CONSUMERS
// (список группы с pending и idle каждого), XPENDING EXTENDED
// (перепроверка пустоты PEL кандидата) и XGROUP DELCONSUMER. Узкое, как
// trimStreams (trim.go): *redis.Client и *redis.ClusterClient
// удовлетворяют интерфейсу, тесты подменяют фейком.
type consumerGroups interface {
	XInfoConsumers(ctx context.Context, key, group string) *redis.XInfoConsumersCmd
	XPendingExt(ctx context.Context, a *redis.XPendingExtArgs) *redis.XPendingExtCmd
	XGroupDelConsumer(ctx context.Context, stream, group, consumer string) *redis.IntCmd
}

// ConsumerOptions — параметры гигиены консьюмеров; нулевые значения
// заменяются дефолтами (withDefaults, стиль TrimOptions).
type ConsumerOptions struct {
	// Groups — чистимые пары стрим → группа; nil — рабочие пары
	// задач (WorkingGroups). Пара с неизвестным пакету стримом —
	// ошибка прохода (как у TrimOnce: имена — константы пакета).
	Groups map[string]string
	// Idle — порог неактивности консьюмера для удаления; 0 —
	// DefaultConsumerIdle. Консьюмеры с pending ≠ 0 не удаляются
	// при любом пороге.
	Idle time.Duration
	// Interval — период проходов CleanupConsumersLoop; 0 —
	// DefaultConsumerInterval. Поле управляет только циклом,
	// CleanupConsumersOnce выполняет один проход всегда.
	Interval time.Duration
	// Log — логгер цикла CleanupConsumersLoop; nil — записи подавлены
	// (slog.DiscardHandler, стиль RecoverOptions). Once не логирует.
	Log *slog.Logger
}

func (o ConsumerOptions) withDefaults() ConsumerOptions {
	if len(o.Groups) == 0 {
		o.Groups = WorkingGroups()
	}
	if o.Idle <= 0 {
		o.Idle = DefaultConsumerIdle
	}
	if o.Interval <= 0 {
		o.Interval = DefaultConsumerInterval
	}
	return o
}

// CleanupConsumersOnce — один проход гигиены по парам опций: XINFO
// CONSUMERS каждой группы, XGROUP DELCONSUMER строго пустых и
// неактивных консьюмеров (см. шапку файла). Возвращает число удалённых
// по всем группам. Ошибка одной группы не отменяет остальные (стиль
// Reliab.Claim/TrimOnce): ошибки собираются в возврат (errors.Join),
// валидная часть отрабатывает — следующий проход повторит сбойную.
//
// Стрим без ключа (ещё не создавался) и группа без ключа (ленивое
// создание при первом чтении, redisq.go) — пустой проход без ошибки:
// чистить нечего.
func CleanupConsumersOnce(ctx context.Context, rdb consumerGroups, opt ConsumerOptions) (int, error) {
	if rdb == nil {
		return 0, errors.New("queue: чистка консьюмеров: клиент Redis не задан")
	}
	o := opt.withDefaults()
	// Пары обходятся по возрастанию имён стримов — детерминированный
	// порядок ошибок и логов прохода (карта в Go итерируется случайно).
	streams := make([]string, 0, len(o.Groups))
	for s := range o.Groups {
		streams = append(streams, s)
	}
	sort.Strings(streams)
	var (
		removed int
		errs    []error
	)
	for _, stream := range streams {
		group := o.Groups[stream]
		if !knownStream(stream) {
			errs = append(errs, fmt.Errorf("queue: чистка консьюмеров: неизвестный стрим %q (стримы очереди — константы пакета: StreamDNS, StreamHTTP, StreamRender, StreamAnalyze, StreamDead)", stream))
			continue
		}
		if group == "" {
			errs = append(errs, fmt.Errorf("queue: чистка консьюмеров %s: не задана группа", stream))
			continue
		}
		// Счётчик группы точен при любом исходе: инкремент только после
		// успешного DELCONSUMER — засчитываем и при ошибке соседних
		// консьюмеров (стиль «валидная часть отрабатывает»).
		n, err := cleanupGroup(ctx, rdb, stream, group, o)
		removed += n
		if err != nil {
			errs = append(errs, err)
		}
	}
	return removed, errors.Join(errs...)
}

// cleanupGroup вычищает строго пустых консьюмеров одной группы:
// кандидат — pending = 0 и idle ≥ порога (XINFO CONSUMERS); удалению
// предшествует перепроверка PEL кандидата (гонка оживления — шапка
// файла). Ответ DELCONSUMER — число удалённых PEL-записей: ноль
// обязателен по построению, ненулевой — случившаяся гонка, ошибка
// для сигнала оператору.
func cleanupGroup(ctx context.Context, rdb consumerGroups, stream, group string, opt ConsumerOptions) (int, error) {
	consumers, err := rdb.XInfoConsumers(ctx, stream, group).Result()
	if err != nil {
		// Нет стрима (ещё не создавался) или группы (создаётся лениво
		// при первом чтении, redisq.go) — чистить нечего, не ошибка
		// (как отсутствие ключа у TrimOnce).
		if errors.Is(err, redis.Nil) || strings.Contains(err.Error(), "no such key") || isNoGroup(err) {
			return 0, nil
		}
		return 0, fmt.Errorf("queue: чистка консьюмеров %s (группа %s): %w", stream, group, err)
	}
	var (
		removed int
		errs    []error
	)
	for _, c := range consumers {
		if c.Pending != 0 || c.Idle < opt.Idle {
			continue // держит PEL или активен — не трогаем (шапка файла)
		}
		pend, err := rdb.XPendingExt(ctx, &redis.XPendingExtArgs{
			Stream:   stream,
			Group:    group,
			Start:    "-",
			End:      "+",
			Count:    1,
			Consumer: c.Name, // ровно этот консьюмер: подтверждение пустоты PEL перед удалением
		}).Result()
		if err != nil {
			// Не подтвердили пустоту — не удаляем: следующий проход
			// повторит (консьюмер остаётся в группе, ничего не теряем).
			errs = append(errs, fmt.Errorf("queue: чистка консьюмеров %s (группа %s): проверка pending %s: %w", stream, group, c.Name, err))
			continue
		}
		if len(pend) > 0 {
			// Получил доставки между XINFO и перепроверкой — теперь
			// держит PEL, не кандидат.
			continue
		}
		pel, err := rdb.XGroupDelConsumer(ctx, stream, group, c.Name).Result()
		if err != nil {
			errs = append(errs, fmt.Errorf("queue: чистка консьюмеров %s (группа %s): удаление %s: %w", stream, group, c.Name, err))
			continue
		}
		removed++
		if pel > 0 {
			// Гонка оживления состоялась: PEL-записи удалены вместе
			// с консьюмером — недоставка навсегда (шапка файла),
			// требуется разбор по журналу.
			errs = append(errs, fmt.Errorf("queue: чистка консьюмеров %s (группа %s): удаление %s унесло %d pending-записей (гонка оживления) — записи не будут доставлены, разберите вручную по идентификаторам предмета", stream, group, c.Name, pel))
		}
	}
	return removed, errors.Join(errs...)
}

// CleanupConsumersLoop — фоновый цикл гигиены консьюмеров (T-155):
// проход CleanupConsumersOnce при старте (рестарт планировщика сразу
// вычищает накопленное рестартами воркеров) и далее каждый Interval.
// Удалённые за проход считаются суммарно и логируются Info (проход
// без жертв молчит — норма); ошибка прохода не останавливает цикл:
// логируется Warn, следующий проход повторит — опоздание чистки стоит
// только строк метаданных в группах. Работает до отмены ctx и возвращает
// nil — контракт OnRun каркаса (T-012, graceful shutdown, ТЗ 3.4).
func CleanupConsumersLoop(ctx context.Context, rdb consumerGroups, opt ConsumerOptions) error {
	if rdb == nil {
		return errors.New("queue: цикл чистки консьюмеров: клиент Redis не задан")
	}
	o := opt.withDefaults()
	log := o.Log
	if log == nil {
		log = slog.New(slog.DiscardHandler)
	}
	pairs := make([]string, 0, len(o.Groups))
	for s, g := range o.Groups {
		pairs = append(pairs, s+"="+g)
	}
	sort.Strings(pairs)
	log.Info("цикл чистки мёртвых консьюмеров групп",
		"groups", strings.Join(pairs, ","), "idle", o.Idle.String(), "interval", o.Interval.String())

	cleanup := func() {
		removed, err := CleanupConsumersOnce(ctx, rdb, o)
		if err != nil && ctx.Err() == nil {
			log.Warn("чистка мёртвых консьюмеров групп", "err", err)
		}
		if removed > 0 {
			log.Info("удалены мёртвые консьюмеры групп", "removed", removed, "idle", o.Idle.String())
		}
	}

	cleanup()
	ticker := time.NewTicker(o.Interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			cleanup()
		}
	}
}
