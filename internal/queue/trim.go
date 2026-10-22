// Освобождение подтверждённого префикса стримов (T-141, ТЗ 4.2
// «Дополнение 2026-10-23»): XACK убирает запись из pending-списка группы,
// но не из стрима — подтверждённая история копилась бы до потолка MAXLEN
// (T-025) и переполняла бюджет Redis ≤ 0,5 ГБ (ТЗ 3.2): суточный прогон
// T-093 встал на «OOM command not allowed» при стоке q:dns 1735 пачек
// ≈ 410 МиБ (docs/load-measurements.md §11). Освобождение — периодический
// XTRIM MINID по старейшей записи, которая обязана остаться в стриме:
// удаляется только доставленное и подтверждённое, необработанные задачи
// и зависшие PEL-записи не трогаются.
//
// Точка обрезки — минимум по группам стрима (в бою у рабочего стрима одна
// группа — потребители этапа, docs/redis.md §2):
//
//   - группа с зависшими записями (pending без XACK): старейшая pending-
//     запись. Всё ниже её уже доставлено — XREADGROUP «>» раздаёт записи
//     по порядку id — и подтверждено (иначе была бы в pending): зависший
//     PEL не удаляется никогда;
//   - группа с пустым pending: последний доставленный id (XINFO GROUPS
//     last-delivered-id). Всё ниже доставлено и подтверждено, всё выше
//     ещё не доставлено — и остаётся.
//
// Почему XTRIM MINID, а не XDEL после каждого Ack (ТЗ 4.2 допускает оба):
//
//   - нагрузка на Redis: один проход — XINFO GROUPS + XPENDING (по группам
//     с pending) + XTRIM на стрим, ~12 команд в минуту на четыре стрима,
//     против дополнительной команды XDEL на каждую подтверждённую запись
//     (тысячи в минуту на пике HTTP-этапа, замер T-139);
//   - атомарность: XTRIM — одна команда, точка обрезки вычисляется по
//     данным, которые не могут стать хуже к моменту выполнения: новые
//     pending появляются только у записей выше last-delivered (id записей
//     монотонны), ниже вычисленной точки подтверждённый префикс только
//     растёт. Гонка «прочитали точку → обрезаем» безопасна без транзакции;
//   - consumer groups: PEL не затрагивается (записей ниже старейшей pending
//     в нём нет по построению), lag не искажается (недоставленное не
//     обрезается). Группа, созданная позже со стартом "0" (ленивое
//     создание, redisq.go), не увидит обрезанное — но это уже подтверждённая
//     работа с результатом в БД: повторная доставка не нужна (источник
//     истины PostgreSQL, ТЗ 3.4).
//
// Механизм повторяет проверенное серверное решение инцидента (юнит
// t092-trimhist: XTRIM MINID старейшего pending раз в 2 мин) — T-141
// встраивает его в код: цикл TrimLoop живёт в сервисе scheduler (одиночный
// хозяин стримов очередей, рядом с lease-проходом и гейтом backpressure;
// redis.trim_interval). Пока планировщик остановлен, подтверждённая
// история копится не выше потолков MAXLEN — сумма потолков в бюджете
// (reliab.go, T-141).
//
// Стрим без групп не освобождается и ошибкой не считается: подтверждений
// не бывает (q:dead — ручной разбор без consumer group), потолок стрима —
// MAXLEN с крошечными записями (T-105). XTRIM по такому стриму не
// выполняется вовсе — стримы между рестартами сервисов живут с группами,
// создаваемыми лениво, и обрезка от них не зависит.

package queue

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
)

// DefaultTrimInterval — период проходов освобождения (TrimLoop). Минута
// балансирует память и нагрузку: между проходами копится не больше минуты
// подтверждённых записей (при текущих темпах этапов: q:dns ≤ 60 пачек/мин
// ≈ 3,8 МиБ при постановке пачки в секунду, штучные ≈ 2 МиБ на стрим) —
// единицы процентов бюджета ТЗ 3.2, а цена прохода — три команды на стрим.
const DefaultTrimInterval = time.Minute

// workingStreams — рабочие стримы задач (ТЗ 4.2), освобождаемые
// подтверждением: allStreams без q:dead (без consumer группы
// подтверждённого префикса не бывает — потолок стрима MAXLEN с крошечными
// записями T-105). Фильтр по allStreams — новый рабочий стрим,
// добавленный в allStreams, освобождается автоматически (пин-тест
// TestTrimOptionsDefaults сверяет множества).
func workingStreams() []string {
	out := make([]string, 0, len(allStreams)-1)
	for _, s := range allStreams {
		if s != StreamDead {
			out = append(out, s)
		}
	}
	return out
}

// trimStreams — окно go-redis для освобождения подтверждённого префикса:
// XINFO GROUPS (pending и last-delivered-id групп), XPENDING (старейшая
// pending-запись), XTRIM MINID и XLEN (сигнал застрявшего префикса).
// Узкое, как redisStreams (redisq.go): *redis.Client и *redis.ClusterClient
// удовлетворяют интерфейсу, тесты подменяют фейком.
type trimStreams interface {
	XInfoGroups(ctx context.Context, key string) *redis.XInfoGroupsCmd
	XPending(ctx context.Context, stream, group string) *redis.XPendingCmd
	XTrimMinID(ctx context.Context, key, minID string) *redis.IntCmd
	XLen(ctx context.Context, key string) *redis.IntCmd
}

// TrimOptions — параметры освобождения; нулевые значения заменяются
// дефолтами (withDefaults, стиль RecoverOptions).
type TrimOptions struct {
	// Streams — освобождаемые стримы; nil — рабочие стримы задач
	// (q:dns, q:http, q:render, q:analyze). q:dead не входит: без
	// consumer group подтверждённого префикса не бывает.
	Streams []string
	// Interval — период проходов TrimLoop; 0 — DefaultTrimInterval.
	// TrimOnce проход и так делает — поле управляет только циклом.
	Interval time.Duration
	// MaxLen — предел длины стримов для сигнала застрявшего префикса
	// (TrimLoop): длина стрима выше трёх четвертей предела при живых
	// группах — точка обрезки не двигается (например, осиротевшая группа
	// с замороженным last-delivered-id держит минимум по группам).
	// Зеркалит redis.maxlen; 0 — пределы по стримам политики T-025/T-141
	// (DefaultStreamMaxLen).
	MaxLen int64
	// Log — логгер цикла TrimLoop; nil — записи подавлены
	// (slog.DiscardHandler, стиль RecoverOptions). TrimOnce не логирует.
	Log *slog.Logger
}

func (o TrimOptions) withDefaults() TrimOptions {
	if len(o.Streams) == 0 {
		o.Streams = workingStreams()
	}
	if o.Interval <= 0 {
		o.Interval = DefaultTrimInterval
	}
	return o
}

// streamMaxLen — предел длины стрима для сигнала застрявшего префикса:
// явный из опций или политика по стримам (DefaultStreamMaxLen).
func (o TrimOptions) streamMaxLen(stream string) int64 {
	if o.MaxLen > 0 {
		return o.MaxLen
	}
	return DefaultStreamMaxLen(stream)
}

// TrimOnce — один проход освобождения по стримам опций: для каждого
// вычисляет безопасную точку обрезки (минимум по группам — см. шапку
// файла) и выполняет XTRIM MINID. Возвращает число удалённых записей по
// всем стримам и список застрявших стримов — длина (XLEN) выше трёх
// четвертей предела при живых группах: подтверждённый префикс растёт,
// а точка не двигается (сигнал оператору — TrimLoop логирует Warn;
// глубина lag+pending такое не видит: backlog групп у нуля).
// Ошибка одного стрима не отменяет остальные (стиль Reliab.Claim):
// ошибки собираются в возврат (errors.Join), валидная часть отрабатывает —
// следующий проход повторит сбойный.
//
// Стрим без ключа (ещё не создавался) и стрим без групп — пустой проход
// без ошибки: освобождать нечего.
func TrimOnce(ctx context.Context, rdb trimStreams, opt TrimOptions) (int64, []string, error) {
	if rdb == nil {
		return 0, nil, errors.New("queue: освобождение стримов: клиент Redis не задан")
	}
	o := opt.withDefaults()
	var (
		trimmed int64
		stuck   []string
		errs    []error
	)
	for _, stream := range o.Streams {
		n, isStuck, err := trimStream(ctx, rdb, stream, o)
		if err != nil {
			errs = append(errs, err)
			continue
		}
		trimmed += n
		if isStuck {
			stuck = append(stuck, stream)
		}
	}
	return trimmed, stuck, errors.Join(errs...)
}

// trimStream освобождает подтверждённый префикс одного стрима: точка
// обрезки — старейшая запись, которая обязана остаться (минимум по
// группам), XTRIM MINID удаляет строго ниже неё. pending-записи и
// недоставленный хвост не удаляются никогда (at-least-once, ТЗ 3.4).
// isStuck — длина стрима выше трёх четвертей предела opt.streamMaxLen
// при живых группах: точка обрезки не двигается и префикс копится —
// обычная обрезка такое не лечит, нужен разбор групп стрима.
func trimStream(ctx context.Context, rdb trimStreams, stream string, opt TrimOptions) (int64, bool, error) {
	if !knownStream(stream) {
		return 0, false, fmt.Errorf("queue: освобождение: неизвестный стрим %q (стримы очереди — константы пакета: StreamDNS, StreamHTTP, StreamRender, StreamAnalyze, StreamDead)", stream)
	}
	groups, err := rdb.XInfoGroups(ctx, stream).Result()
	if err != nil {
		// Стрима ещё нет — Redis отвечает ошибкой no such key, а не пустым
		// списком (как у глубины гейта, backpressure.go): освобождать
		// нечего, не ошибка.
		if errors.Is(err, redis.Nil) || strings.Contains(err.Error(), "no such key") {
			return 0, false, nil
		}
		return 0, false, fmt.Errorf("queue: освобождение %s: группы: %w", stream, err)
	}
	if len(groups) == 0 {
		// Групп нет (как у q:dead): подтверждённого префикса не бывает,
		// потолок стрима — MAXLEN. Пустой проход, не ошибка.
		return 0, false, nil
	}
	safe := ""
	for _, g := range groups {
		cand := g.LastDeliveredID
		if g.Pending > 0 {
			p, err := rdb.XPending(ctx, stream, g.Name).Result()
			if err != nil {
				// Ошибка чтения PEL — обрезка по last-delivered-id
				// небезопасна: pending-записи могут лежать ниже его.
				// Стрим пропускается целиком: следующий проход повторит.
				return 0, false, fmt.Errorf("queue: освобождение %s (группа %s): pending: %w", stream, g.Name, err)
			}
			if p.Count > 0 {
				// Гонка: между XINFO и XPENDING всё подтвердили — точка
				// остаётся last-delivered-id (доставленное подтверждено).
				cand = p.Lower
			}
		}
		if cand == "" {
			continue // группа ничего не доставляла — ограничения нет
		}
		if safe == "" || streamIDGreater(safe, cand) {
			// Численно меньший id: лексикографический порядок "999-0" >
			// "1000-0" обманчив (trackProgress, redisq.go).
			safe = cand
		}
	}
	var (
		n       int64
		isStuck bool
	)
	if safe != "" {
		n, err = rdb.XTrimMinID(ctx, stream, safe).Result()
		if err != nil {
			return 0, false, fmt.Errorf("queue: освобождение %s (XTRIM MINID %s): %w", stream, safe, err)
		}
	}
	// Застрявший префикс: группы живы (есть), а длина у потолка — глубина
	// backlog при этом может быть нулевой, и гейт с алертами молчат:
	// точка обрезки заморожена (минимум по группам держит осиротевшая
	// группа или зависшая last-delivered-id).
	length, err := rdb.XLen(ctx, stream).Result()
	if err != nil {
		// Длина — сигнал, не инвариант: ошибка XLEN не отменяет обрезку.
		return n, false, fmt.Errorf("queue: освобождение %s: длина: %w", stream, err)
	}
	if limit := opt.streamMaxLen(stream); length*4 >= limit*3 {
		isStuck = true
	}
	return n, isStuck, nil
}

// TrimLoop — фоновый цикл освобождения подтверждённого префикса (T-141):
// проход TrimOnce при старте (рестарт сервиса сразу освобождает
// накопленную историю) и далее каждый Interval. Ошибка прохода не
// останавливает цикл: логируется Warn, следующий проход повторит —
// неподтверждённое не удаляется, опоздание обрезки стоит только памяти
// до потолков MAXLEN. Застрявший стрим (длина у потолка при живых
// группах — точка обрезки заморожена, см. TrimOnce) сигнализируется
// Warn один раз за эпизод: подтверждённый префикс не виден глубине
// lag+pending, и без сигнала молчаливый рост повторил бы T-093.
// Работает до отмены ctx и возвращает nil — контракт OnRun каркаса
// (T-012, graceful shutdown, ТЗ 3.4).
func TrimLoop(ctx context.Context, rdb trimStreams, opt TrimOptions) error {
	if rdb == nil {
		return errors.New("queue: цикл освобождения: клиент Redis не задан")
	}
	o := opt.withDefaults()
	log := o.Log
	if log == nil {
		log = slog.New(slog.DiscardHandler)
	}
	log.Info("цикл освобождения подтверждённого префикса стримов",
		"streams", strings.Join(o.Streams, ","), "interval", o.Interval.String())

	warned := make(map[string]bool, len(o.Streams)) // Warn один раз за эпизод застревания
	trim := func() {
		_, stuck, err := TrimOnce(ctx, rdb, o)
		if err != nil && ctx.Err() == nil {
			log.Warn("освобождение подтверждённого префикса", "err", err)
		}
		stuckSet := make(map[string]bool, len(stuck))
		for _, s := range stuck {
			stuckSet[s] = true
			if !warned[s] {
				warned[s] = true
				log.Warn("подтверждённый префикс стрима у потолка, точка обрезки не двигается — проверьте группы стрима (осиротевшая группа с замороженным last-delivered-id держит обрезку)",
					"stream", s, "maxlen", o.streamMaxLen(s))
			}
		}
		for s := range warned { // эпизод закончился — снова сможем предупредить
			if !stuckSet[s] {
				delete(warned, s)
			}
		}
	}

	trim()
	ticker := time.NewTicker(o.Interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			trim()
		}
	}
}
