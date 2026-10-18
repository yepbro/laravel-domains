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
// только после MinIdle простоя, чаще опрашивать незачем).
const DefaultRecoverInterval = 30 * time.Second

// DefaultRecoverBatch — пачка клейма за один проход: окно потребления
// воркеров этапов (16 записей) — восстановитель работает с тем же темпом,
// что и основной цикл.
const DefaultRecoverBatch = 16

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
	// Batch — размер пачки клейма; 0 — DefaultRecoverBatch. Осмысленно —
	// окно Consume воркера.
	Batch int
	// Interval — период опроса при пустом клейме; 0 —
	// DefaultRecoverInterval.
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
	// пачки клейма для решения «дренаж или тик».
	claim := func() int {
		claimed, err := r.Claim(ctx, o.Stream, o.Group, o.Consumer, 0, o.Batch)
		if err != nil {
			// Отмена во время клейма — часть остановки, не тревога.
			if ctx.Err() == nil {
				log.Warn("возврат зависших доставок: клейм", "err", err)
			}
			return 0
		}
		for _, c := range claimed {
			if ctx.Err() != nil {
				return len(claimed)
			}
			if err := r.Process(ctx, o.Group, c.Delivery, process); err != nil && ctx.Err() == nil {
				log.Warn("зависшая доставка не подтверждена — вернётся клеймом",
					"entry", c.ID, "attempts", c.Attempts, "err", err)
			}
		}
		return len(claimed)
	}

	ticker := time.NewTicker(o.Interval)
	defer ticker.Stop()
	for {
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
