// Scheduler: постановка выбранных доменов в очередь DNS-проверки (T-028;
// ТЗ 5.3, 4.2, 3.4).
//
// EnqueueBatch переводит пачку выборки (DispatchBatch, T-027) в
// check_state = queued с queued_at = now() — с этого момента действует
// lease домена (ТЗ 5.3: «переводит выбранные домены в queued с отметкой
// времени (lease)») — и публикует переведённые домены одним сообщением
// DNSBatch в q:dns (ТЗ 4.2: единица задачи q:dns — пачка доменов
// id + имя). EnqueueLoop — рабочий цикл постановки для OnRun (каркас
// T-012): выборка → постановка, пауза между пачками, ожидание пополнения
// при пустой пачке; сборка сервиса — cmd/scheduler.
//
// Порядок операций — сначала перевод, затем публикация (ТЗ 3.4: «источник
// истины — PostgreSQL. Потеря Redis не должна приводить к потере данных
// (задачи восстанавливаются планировщиком)»):
//
//   - перевод фиксирует в БД, что домен взят в работу. Сбой после перевода,
//     но до публикации оставляет строки queued без задачи в Redis — их
//     вернёт в pending возврат истёкших lease (T-029, scheduler.lease,
//     по умолчанию 30 мин): ограниченная задержка без повторной обработки;
//
//   - обратный порядок (публикация → перевод) при сбое перед UPDATE оставил
//     бы задачу в q:dns для домена, оставшегося pending: следующий цикл
//     снова выбрал бы и снова опубликовал его — неограниченные дубли работы
//     при краш-лупе, и результат писался бы домену без активного lease;
//
//   - сбой публикации не откатывает перевод: XADD мог удаться, пока клиент
//     видел ошибку (потерянный ответ) — откат в pending дал бы вторую
//     задачу на домен, уже взятый в работу. Восстановление одно и то же —
//     lease (T-029); повторная доставка допустима и ограничена (at-least-once,
//     ТЗ 3.4), записи результатов идемпотентны.
//
// Охрана перевода — check_state = pending на момент UPDATE: между выборкой
// (T-027, состояние не меняет) и переводом строка могла уйти из pending
// (конкурентный возврат lease T-029, воскрешение add-файлом T-018, второй
// экземпляр планировщика). RETURNING отдаёт ключи только фактически
// переведённых строк — публикация строится по ним; непереведённые (гонка)
// исключаются из публикации: их подберёт следующий цикл (доставка
// at-least-once, ТЗ 3.4).

package scheduler

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"crawler/internal/db"
	"crawler/internal/model"
	"crawler/internal/queue"
)

// enqueueMaxNameLen — лимит полного имени домена в ASCII-форме (ТЗ 5.2
// п. 4). Дублирует queue.maxNameLen (неэкспортируем): проверка до перевода
// не заводит в queued строки, которые очередь отвергнет при публикации.
const enqueueMaxNameLen = 253

// enqueueUpdate — пакетный перевод пачки в queued: один оператор
// UPDATE … FROM unnest (конструктор internal/db, ТЗ 6: «запись пачками …
// UPDATE … FROM unnest(...)»; каждый массив-ключ — один параметр, размер
// пачки не упирается в лимит протокола). Ключи — (zone_id, id), PK domains
// (ТЗ 6.1). Присваивания — общие выражения пачки: check_state литералом
// кода модели (коды схемы, а не данные — стиль dispatchZoneSQL),
// queued_at = now() — время сервера БД (без расхождения часов клиентов)
// и единый момент начала lease всей пачки; flags = (t.flags & ~бита
// http_published)::int2 — staging-пометка публикации q:http (T-108)
// снимается в точке входа в новый эпизод: постановка — единственный
// переход pending → queued, через неё проходит любой возврат строки
// в работу (lease T-029, воскрешение T-018, перепроверка T-030/T-031,
// crawlerctl recheck) — свежий эпизод всегда начинается с чистого бита,
// иначе пометка прошлого эпизода подавила бы его публикацию (потеря
// задачи q:http). Знаковая маска ~бита и ::int2 — smallint-арифметика PG
// (стиль renderq.go T-066). Охрана t.check_state = pending перепроверяет
// состояние на момент блокировки строки; RETURNING — ключи фактически
// переведённых строк: публикация строится ровно по ним.
var enqueueUpdate = db.Update("domains").
	Key(
		db.Column{Name: "zone_id", Type: "int2"},
		db.Column{Name: "id", Type: "int8"},
	).
	SetExpr(
		fmt.Sprintf("check_state = %d", int16(model.CheckStateQueued)),
		"queued_at = now()",
		fmt.Sprintf("flags = (t.flags & %d)::int2", int16(^int16(model.FlagHTTPPublished))),
	).
	Where(fmt.Sprintf("t.check_state = %d", int16(model.CheckStatePending))).
	Returning("t.zone_id", "t.id")

// enqueueKey — ключ строки domains: пачка адресуется парой (zone_id, id).
type enqueueKey struct {
	zone int16
	id   int64
}

// EnqueueResult — итог постановки пачки.
type EnqueueResult struct {
	// Queued — строки, переведённые в queued (по RETURNING перевода).
	Queued int
	// Published — домены в опубликованном сообщении q:dns: равно Queued
	// при успешной публикации. При ошибке публикации 0, а Queued
	// показывает, сколько строк ушло в queued без задачи (их вернёт
	// lease, T-029).
	Published int
	// Skipped — строки пачки, не переведённые из-за гонки (уже не
	// pending на момент UPDATE), включая дубликаты ключей в пачке.
	Skipped int
}

// EnqueueBatch переводит пачку выбранных доменов в check_state = queued с
// queued_at = now() (lease начинается, ТЗ 5.3) одним пакетным UPDATE с
// охраной pending и публикует переведённые домены одним сообщением DNSBatch
// в q:dns (ТЗ 4.2). Гарантии и порядок операций — в комментарии файла;
// пустая пачка — no-op без обращения к БД и очереди.
func EnqueueBatch(ctx context.Context, pool db.DB, q queue.Queue, batch []DispatchRow) (EnqueueResult, error) {
	if pool == nil {
		return EnqueueResult{}, errors.New("scheduler: постановка пачки: пул не задан")
	}
	if q == nil {
		return EnqueueResult{}, errors.New("scheduler: постановка пачки: очередь не задана")
	}
	if len(batch) == 0 {
		// Переводить и публиковать нечего; пустая DNSBatch вдобавок
		// запрещена контрактом очереди (queue.DNSBatch.validate).
		return EnqueueResult{}, nil
	}
	if err := enqueueValidate(batch); err != nil {
		return EnqueueResult{}, err
	}

	// Ключи пачки — параллельные массивы (колонки конструктора db.Update —
	// []any с элементами int2/int8; каждый массив — один параметр).
	zoneIDs := make([]any, len(batch))
	domainIDs := make([]any, len(batch))
	for i, r := range batch {
		zoneIDs[i] = r.ZoneID
		domainIDs[i] = r.DomainID
	}

	// Перевод. RETURNING — ключи фактически переведённых строк.
	rows, err := enqueueUpdate.Query(ctx, pool, zoneIDs, domainIDs)
	if err != nil {
		return EnqueueResult{}, fmt.Errorf("scheduler: постановка пачки (%d доменов): %w", len(batch), err)
	}
	defer rows.Close()

	translated := make(map[enqueueKey]struct{}, len(batch))
	for rows.Next() {
		var k enqueueKey
		if err := rows.Scan(&k.zone, &k.id); err != nil {
			return EnqueueResult{}, fmt.Errorf("scheduler: постановка пачки: чтение переведённых: %w", err)
		}
		translated[k] = struct{}{}
	}
	if err := rows.Err(); err != nil {
		return EnqueueResult{}, fmt.Errorf("scheduler: постановка пачки: %w", err)
	}

	res := EnqueueResult{Queued: len(translated), Skipped: len(batch) - len(translated)}

	// Публикация: одно сообщение q:dns из переведённых строк в порядке
	// выборки (чередование зон T-027 сохраняется для потребителя). Ключ
	// удаляется после использования — дубликат ключа в пачке публикуется
	// один раз.
	msg := queue.DNSBatch{Domains: make([]queue.DNSDomain, 0, res.Queued)}
	for _, r := range batch {
		k := enqueueKey{zone: r.ZoneID, id: r.DomainID}
		if _, ok := translated[k]; !ok {
			continue
		}
		delete(translated, k)
		msg.Domains = append(msg.Domains, queue.DNSDomain{
			ZoneID:   r.ZoneID,
			DomainID: r.DomainID,
			Name:     r.Name, // полное имя, punycode (dispatch.go, ТЗ 5.2)
		})
	}
	if len(msg.Domains) == 0 {
		// Все строки проиграли гонку — публиковать нечего; их подберёт
		// следующий цикл (at-least-once, ТЗ 3.4).
		return res, nil
	}
	if _, err := q.Publish(ctx, queue.StreamDNS, msg); err != nil {
		// Перевод зафиксирован и не откатывается (комментарий файла):
		// res.Queued строк ждут возврата по lease (T-029).
		return res, fmt.Errorf("scheduler: постановка пачки: публикация %d доменов в q:dns: %w", len(msg.Domains), err)
	}
	res.Published = len(msg.Domains)
	return res, nil
}

// enqueueValidate проверяет строки пачки до обращения к БД: публикация q:dns
// отвергает недопустимые значения (queue.DNSBatch.validate), а перевод к
// этому моменту уже зафиксирован — проверка заранее не заводит строки
// в queued без сообщения. Условия совпадают с контрактом очереди
// (зона/домен > 0, имя непустое в пределах лимита ТЗ 5.2).
func enqueueValidate(batch []DispatchRow) error {
	for i, r := range batch {
		if r.ZoneID <= 0 {
			return fmt.Errorf("scheduler: постановка пачки: домен №%d: zone_id %d ≤ 0", i+1, r.ZoneID)
		}
		if r.DomainID <= 0 {
			return fmt.Errorf("scheduler: постановка пачки: домен №%d: domain_id %d ≤ 0", i+1, r.DomainID)
		}
		if r.Name == "" {
			return fmt.Errorf("scheduler: постановка пачки: домен №%d: пустое имя", i+1)
		}
		if len(r.Name) > enqueueMaxNameLen {
			return fmt.Errorf("scheduler: постановка пачки: домен №%d: имя длиннее %d октетов", i+1, enqueueMaxNameLen)
		}
	}
	return nil
}

// Дефолты цикла постановки.
const (
	// DefaultEnqueueBatch — размер пачки выборки: зеркалит дефолт
	// scheduler.batch_size (ТЗ 5.10); рабочее значение цикл получает из
	// конфига.
	DefaultEnqueueBatch = 1000

	// DefaultEnqueueInterval — пауза между пачками. Единственный на
	// данный момент ограничитель темпа наполнения q:dns: гейт
	// backpressure по длине очереди — T-026 (параллельная задача),
	// TODO ниже в EnqueueLoop.
	DefaultEnqueueInterval = time.Second

	// DefaultEnqueueIdleWait — ожидание пополнения pending при пустой
	// пачке и пауза после ошибок цикла (БД или Redis недоступны):
	// холостые круги не греют ни БД, ни логи, а неопубликованные строки
	// ждут возврата по lease (T-029) — спешить некуда.
	DefaultEnqueueIdleWait = 30 * time.Second
)

// EnqueueOptions — параметры цикла постановки; нулевые значения
// заменяются дефолтами (withDefaults, стиль RecheckOptions).
type EnqueueOptions struct {
	// BatchSize — размер пачки выборки (scheduler.batch_size, ТЗ 5.3).
	BatchSize int
	// Interval — пауза между пачками (DefaultEnqueueInterval).
	Interval time.Duration
	// IdleWait — ожидание пополнения при пустой пачке и пауза после
	// ошибок (DefaultEnqueueIdleWait).
	IdleWait time.Duration
	// ZoneWeights — веса зон выборки (DispatchQuery.ZoneWeights, T-027):
	// источник — scheduler.zone_weights, имена разрешены в zone_id к
	// моменту сборки (ResolveZoneWeights, T-106); nil — равные веса.
	ZoneWeights map[int16]int
	// Log — логгер цикла; nil — записи подавлены (slog.DiscardHandler).
	Log *slog.Logger
}

func (o EnqueueOptions) withDefaults() EnqueueOptions {
	if o.BatchSize <= 0 {
		o.BatchSize = DefaultEnqueueBatch
	}
	if o.Interval <= 0 {
		o.Interval = DefaultEnqueueInterval
	}
	if o.IdleWait <= 0 {
		o.IdleWait = DefaultEnqueueIdleWait
	}
	return o
}

// EnqueueLoop — рабочий цикл постановки (ТЗ 5.3): DispatchBatch (выборка
// пачки pending, T-027) → EnqueueBatch (перевод в queued с lease +
// публикация q:dns) → пауза; пустая пачка или ошибка шага — ожидание
// пополнения/пауза и новый круг. Работает до отмены ctx и возвращает nil —
// контракт OnRun каркаса (T-012: graceful shutdown дорабатывает текущую
// пачку; ошибка цикла не останавливает сервис — логируется Warn, очередная
// итерация повторит, состояние в БД консистентно: перевод и публикация
// идемпотентны с точки зрения следующего круга, зависшие строки возвращает
// lease T-029). Гейт backpressure подключается обёрткой очереди в сборке
// сервиса (queue.WatermarkPublisher, T-026) — цикл публикует через
// интерфейс Queue и гейта не знает.
//
// Мутанты логирования (T-M05): Warn шагов цикла и Debug итога пачки не
// меняют наблюдаемого поведения (ошибки уже учтены wait/continue) —
// гасятся по docs/mutation-testing.md §4 (логирование не тестируется).
// mutator-disable-regexp log\.Warn\("цикл statement/remove
// mutator-disable-regexp log\.Debug\("пачка statement/remove
func EnqueueLoop(ctx context.Context, pool db.DB, q queue.Queue, opts EnqueueOptions) error {
	if pool == nil {
		return errors.New("scheduler: цикл постановки: пул не задан")
	}
	if q == nil {
		return errors.New("scheduler: цикл постановки: очередь не задана")
	}
	o := opts.withDefaults()
	log := o.Log
	if log == nil {
		log = slog.New(slog.DiscardHandler)
	}
	log.Info("цикл постановки пачек в q:dns",
		"batch", o.BatchSize, "interval", o.Interval.String(), "idle_wait", o.IdleWait.String(),
		"zone_weights", len(o.ZoneWeights))

	// wait прерывается отменой ctx: выход из ожидания — часть остановки.
	wait := func(d time.Duration) {
		t := time.NewTimer(d)
		defer t.Stop()
		select {
		case <-ctx.Done():
		case <-t.C:
		}
	}

	for {
		batch, err := DispatchBatch(ctx, pool, DispatchQuery{
			Limit:       o.BatchSize,
			ZoneWeights: o.ZoneWeights,
		})
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			log.Warn("цикл постановки: выборка пачки", "err", err)
			wait(o.IdleWait)
			continue
		}
		if len(batch) == 0 {
			// pending пуст — ждать пополнения (вернут recheck T-030,
			// retry T-031, lease T-029, ingest T-018).
			wait(o.IdleWait)
			continue
		}

		res, err := EnqueueBatch(ctx, pool, q, batch)
		if err != nil {
			// Эквивалентный мутант (T-M05): без этого return цикл
			// завершается следующим витком — wait прерывается ctx.Done,
			// DispatchBatch по отменённому ctx вернёт ошибку, выход там
			// тоже nil (контракт OnRun); отличие — лишний Warn.
			// mutator-disable-next-line branch/if
			if ctx.Err() != nil {
				return nil
			}
			// Строки из res.Queued, оставшиеся без публикации, вернёт
			// lease (T-029): состояние БД консистентно; пауза не
			// разгоняет новые пачки в queued, пока очередь недоступна.
			log.Warn("цикл постановки: постановка пачки",
				"err", err, "batch", len(batch), "queued", res.Queued)
			wait(o.IdleWait)
			continue
		}
		log.Debug("пачка поставлена в q:dns",
			"published", res.Published, "skipped", res.Skipped)
		wait(o.Interval)
	}
}
