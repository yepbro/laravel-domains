// Возврат зависших доставок в работу (T-025, ТЗ 4.2: «зависшие сообщения
// забираются через XAUTOCLAIM»): фоновый цикл RecoverLoop рядом с
// потреблением воркера. Reliab.Claim возвращает записи, зависшие в
// pending группы (прочитаны и не подтверждены — воркер пал или завис),
// RecoverLoop обрабатывает их тем же паттерном Processing — обработка и
// запись результата в БД → XACK, — каким воркер берёт свежие доставки:
// идемпотентность записей (ТЗ 3.4) позволяет смешивать повторы со
// свежими задачами без отдельного обработчика.
//
// Записи, исчерпавшие лимит попыток (redis.max_attempts), Claim переводит
// в q:dead сам (T-025) — RecoverLoop их не видит. Восстановитель
// запускается в каждом экземпляре воркера от имени его же потребителя:
// клейм конкурентен и безопасен (XAUTOCLAIM атомарно перезакрепляет
// запись), единственный владелец не нужен.
//
// До включения восстановителя запись, не подтверждённая крашем воркера,
// оставалась в PEL навсегда: повторной доставки не было, q:dead был
// недостижим, а lease (T-029) возвращает только задачи q:dns — домены
// остальных этапов уже в check_state = done (ТЗ 7) и восстановления не
// имеют.

package queue

import (
	"context"
	"errors"
	"log/slog"
	"time"
)

// DefaultRecoverInterval — период опроса зависших записей. Один
// XAUTOCLAIM на воркер за период — дёшев; латентность возврата —
// MinIdle плюс до одного интервала (запись становится доступной клейму
// только после MinIdle простоя, чаще опрашивать незачем). Настройка —
// redis.recover_interval (T-153).
const DefaultRecoverInterval = 30 * time.Second

// DefaultRecoverBatch — пачка клейма за один проход. T-025 брал окно
// потребления воркеров (16 записей) — восстановитель жил в темпе
// основного цикла; T-153 поднимает пачку до 64 и выносит темп обхода
// (batch/interval) в redis.recover_batch/recover_interval: при 16/30 с
// обход PEL ~5К записей занимает ≈2,6 ч — в инциденте 22.10 (OOM Redis)
// дожигание попыток одной когорты растянулось на 12 ч (этап 23,
// tasks.md), здоровые задачи ждали возврата часами. Стоимость
// повторной обработки клеймнутых ограничена идемпотентностью (ТЗ 3.4:
// повтор Processing — то же «результат в БД → XACK») — подъём пачки
// стоит только времени этапа, не корректности.
//
// Инвариант выбора: RecoverLoop обходит пачку последовательно и держит
// её записи в PEL всё время обхода, поэтому время обработки пачки
// (худшая запись × batch) должно оставаться меньше claim_min_idle
// (10 мин), иначе хвост пачки перехватывается RecoverLoop соседнего
// воркера (+1 попытка за круг — тот же механизм дожигания). Типовая
// запись рендера ~5 с (замер T-092 §4: 4 контекста ≈ 0,78 стр/с):
// 64 × 5 с ≈ 5,3 мин < 10 мин — запас ×1,9. Предельная запись
// (тарпит, render.timeout 25 с): 64 × 25 с ≈ 26,7 мин — пачка целиком
// из тарпитов перклеймится в хвосте, осознанный предел дефолта (не 128+:
// 128 × 5 с ≈ 10,7 мин уже не укладывается и типовым случаем); при
// подъёме batch пропорционально поднимают claim_min_idle — расчёт
// в docs/configuration.md, раздел redis.
const DefaultRecoverBatch = 64

// RecoverOptions — параметры цикла RecoverLoop; нулевые значения
// заменяются дефолтами (withDefaults, стиль ReliabOptions).
type RecoverOptions struct {
	// Stream — рабочий стрим потребления воркера (константы пакета).
	Stream string
	// Group — группа потребителей стрима (та же, что у Consume воркера).
	Group string
	// Consumer — имя потребителя: то же, что у Consume воркера, — клейм
	// закрепляет записи за ним же, PEL не расползается по именам.
	Consumer string
	// Batch — размер пачки клейма; 0 — DefaultRecoverBatch. Настройка
	// боевых сборок — redis.recover_batch (T-153); инвариант выбора
	// дефолта — DefaultRecoverBatch выше.
	Batch int
	// Interval — период опроса при пустом клейме; 0 —
	// DefaultRecoverInterval. Настройка — redis.recover_interval (T-153).
	Interval time.Duration
	// Log — логгер цикла; nil — записи подавлены (slog.DiscardHandler).
	Log *slog.Logger
}

func (o RecoverOptions) withDefaults() RecoverOptions {
	if o.Batch <= 0 {
		o.Batch = DefaultRecoverBatch
	}
	if o.Interval <= 0 {
		o.Interval = DefaultRecoverInterval
	}
	return o
}

// RecoverLoop — фоновый цикл возврата зависших доставок (T-025): тик —
// Claim пачки → Processing каждой записи с обработчиком process (тем же,
// что у основного цикла воркера). Полная пачка — немедленный повтор
// (дренаж зависшего за простой), неполная/пустая — ожидание тика: живой
// PEL невелик. Ошибка клейма или обработки не останавливает цикл:
// логируется Warn, очередной тик повторит (контракт OnRun каркаса,
// T-012; не подтверждённая обработкой запись вернётся следующим клеймом,
// счётчик попыток ускорит перевод в q:dead). Работает до отмены ctx и
// возвращает nil — graceful shutdown (ТЗ 3.4): начатая обработка
// дорабатывает или остаётся неподтверждённой.
func RecoverLoop(ctx context.Context, r *Reliab, process func(context.Context, Delivery) error, opt RecoverOptions) error {
	if process == nil {
		return errors.New("queue: цикл возврата зависших: обработчик не задан")
	}
	return RecoverLoopAttempt(ctx, r, func(c context.Context, d Delivery, _ int) error {
		return process(c, d)
	}, opt)
}

// RecoverLoopAttempt — RecoverLoop для обработчика с номером попытки
// доставки (T-148): каждой клеймнутой записи передаётся Claimed.Attempts
// (счётчик доставок PEL: первое чтение группы + каждый клейм; свежей
// доставке соответствовал бы 1). Обработчик видит, какая это по счёту
// доставка, и применяет раздельную политику ошибок (таймаут-тарпит против
// повторимой, T-148), не храня состояние между доставками: счётчик живёт
// в Redis и переживает рестарты воркеров.
//
// Гейт записи (T-152): перед каждым проходом клейма проверяется
// доступность Redis на запись (Reliab.Writable — SET короткоживущего
// ключа). Недоступный для записи Redis (OOM noeviction, обрыв) делает
// клейм бессмысленным: обработка зависшей записи вернётся той же ошибкой
// публикации результата, а счётчик доставок вырастет — клейм без шанса
// записать результат только жжёт попытки к q:dead (корень инцидента
// 22–23.10: 5507 смертей при исправной обработке, §11.1). Проход
// пропускается до следующего тика; запись восстановилась — клейм и
// обработка продолжаются, попытки не потеряны (at-least-once, ТЗ 3.4).
func RecoverLoopAttempt(ctx context.Context, r *Reliab, process func(context.Context, Delivery, int) error, opt RecoverOptions) error {
	if r == nil {
		return errors.New("queue: цикл возврата зависших: слой надёжности не задан (NewReliab)")
	}
	if process == nil {
		return errors.New("queue: цикл возврата зависших: обработчик не задан")
	}
	o := opt.withDefaults()
	log := o.Log
	if log == nil {
		log = slog.New(slog.DiscardHandler)
	}
	log.Info("цикл возврата зависших доставок",
		"stream", o.Stream, "group", o.Group, "consumer", o.Consumer,
		"batch", o.Batch, "interval", o.Interval.String())

	// claim — один проход: клейм пачки и обработка каждой записи
	// паттерном Processing (результат в БД → XACK). Возвращает размер
	// пачки клейма для решения «дренаж или тик». Частичный сбой Claim
	// (счётчик одной записи) не отменяет валидную часть — она
	// обрабатывается, ошибка логируется. Курсор обхода PEL переносится
	// между проходами: большой PEL перечитывается страницами, а не с
	// начала на каждом тике (Q-07).
	cursor := "0-0"
	claim := func() int {
		// Эквивалентный мутант (T-M04): minIdle 0 → −1 неотличим — Claim
		// заменяет любое неположительное значение опцией слоя. Единицу
		// (numbers/incrementer) отличим тестом по MinIdle XAUTOCLAIM.
		// mutator-disable-next-line numbers/decrementer
		claimed, next, err := r.Claim(ctx, o.Stream, o.Group, o.Consumer, 0, o.Batch, cursor)
		if err != nil && ctx.Err() == nil {
			log.Warn("возврат зависших доставок: клейм", "err", err)
		}
		cursor = next
		for _, c := range claimed {
			if ctx.Err() != nil {
				return len(claimed)
			}
			if err := r.ProcessAttempt(ctx, o.Group, c.Delivery, c.Attempts, process); err != nil && ctx.Err() == nil {
				log.Warn("зависшая доставка не подтверждена — вернётся клеймом",
					"entry", c.ID, "attempts", c.Attempts, "err", err)
			}
		}
		return len(claimed)
	}

	ticker := time.NewTicker(o.Interval)
	defer ticker.Stop()
	for {
		// Гейт записи (T-152): пока Redis отвергает записи, клейм
		// пропускается — попытки доставок не расходуются. Проба —
		// один SET (Reliab.Writable); при неподдерживаемой пробе
		// гейт прозрачен (true — поведение до T-152).
		if !r.Writable(ctx) {
			log.Warn("возврат зависших доставок: Redis не доступен на запись (OOM/соединение) — клейм пропущен, попытки доставок не расходуются (T-152)")
			select {
			case <-ctx.Done():
				return nil
			case <-ticker.C:
			}
			continue
		}
		if n := claim(); n < o.Batch {
			// Нечего дренировать — ждать тика (полная пачка — сразу
			// следующий клейм: зависший за простой PEL разбирается без
			// пауз).
			select {
			case <-ctx.Done():
				return nil
			case <-ticker.C:
			}
		}
	}
}
