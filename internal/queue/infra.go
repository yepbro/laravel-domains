// Классификация инфраструктурных ошибок очереди (T-152, пост-мортем
// docs/load-measurements.md §11.1): сбой самого Redis — OOM при
// noeviction («OOM command not allowed …», инцидент 22–23.10.2026),
// обрыв соединения, таймаут транспорта — в отличие от ошибок обработки
// (бизнес-логика задачи). До T-152 ошибка публикации результата (XADD)
// обрабатывалась как ошибка самой задачи: доставка оставалась без XACK,
// RecoverLoop клеймила её по кругу, попытки сгорали — одно OOM-окно
// 2,25 ч дало 5507 мёртвых задач при полностью исправной обработке.
//
// Маркер InfraError навешивается на ошибки транспорта в Publish/Ack и
// прозрачно проходит через %w-обёртки воркеров (IsInfra): потребитель
// ошибки отличает «Redis не смог записать» от «задача не обработалась» —
// первое лечится паузой потребления и повтором записи (результат уже
// вычислен), а не расходом попыток доставки.

package queue

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"strings"

	"github.com/redis/go-redis/v9"
)

// InfraError — маркер инфраструктурного сбоя очереди: Redis не смог
// выполнить команду по собственной причине (OOM noeviction, обрыв
// соединения, таймаут транспорта, LOADING после рестарта), задача
// непричастна. Определяет поведение потребителей ошибки (T-152):
//
//   - воркеры по маркеру приостанавливают чтение новых доставок
//     (fuse-паттерн T-139/T-142): Redis всё равно отвергает записи,
//     черпать задачи бессмысленно;
//   - семантика at-least-once не меняется: InfraError ≠ успех, XACK
//     после успешной публикации — но и попытки доставок не расходуются
//     клеймами без шанса записать результат (гейт RecoverLoop).
//
// Ошибки обработки (бизнес-логика: декодирование, БД результата, рендер)
// маркером не оборачиваются — их повторная доставка осмысленна и жжёт
// попытки по общей политике redis.max_attempts → q:dead (T-025).
type InfraError struct {
	// Op — операция очереди, которая не смогла («publish в q:http»,
	// «ack q:render (группа render-worker)») — контекст в сообщении.
	Op string
	// Err — исходная ошибка транспорта/сервера.
	Err error
}

func (e *InfraError) Error() string {
	return fmt.Sprintf("queue: %s: инфраструктурный сбой Redis: %v", e.Op, e.Err)
}

func (e *InfraError) Unwrap() error { return e.Err }

// IsInfra сообщает, что в цепочке ошибки (включая %w-обёртки воркеров —
// от OnResult до Processing) есть инфраструктурный сбой очереди.
func IsInfra(err error) bool {
	var ie *InfraError
	return errors.As(err, &ie)
}

// infraCause классифицирует ошибку go-redis как сбой инфраструктуры:
// ответы сервера о собственной недоступности (OOM — предмет инцидента
// T-152; LOADING — старт после рестарта; READONLY/MASTERDOWN/CLUSTERDOWN/
// TRYAGAIN — топология и реплики) и транспорт (обрыв соединения, таймаут
// чтения/записи, исчерпание пула). Классификация типизированная: реальных
// нетипизированных ошибок у go-redis нет, а текстовые маркеры ошибочно
// ловили бы бизнес-тексты. Отмена/дедлайн контекста вызывающего — не сбой
// Redis (context.DeadlineExceeded реализует net.Error и обязан
// отсекаться раньше сетевой проверки).
func infraCause(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return false
	}
	// Ответы сервера (Is*-хелперы go-redis понимают обёртки, включая
	// строковый fallback тестовых ошибок «OOM …»).
	if redis.IsOOMError(err) || redis.IsLoadingError(err) || redis.IsReadOnlyError(err) ||
		redis.IsMasterDownError(err) || redis.IsClusterDownError(err) || redis.IsTryAgainError(err) {
		return true
	}
	// Ответ в обёртке стороннего типа с сохранением интерфейса
	// redis.Error (Is*-хелперы ищут конкретный proto.RedisError и голую
	// строку — интерфейсный обход закрывает %w-цепочки).
	var rErr redis.Error
	if errors.As(err, &rErr) {
		m := rErr.Error()
		for _, prefix := range []string{"OOM ", "LOADING ", "READONLY ", "MASTERDOWN ", "CLUSTERDOWN ", "TRYAGAIN "} {
			if strings.HasPrefix(m, prefix) {
				return true
			}
		}
	}
	// Транспорт: закрытое соединение и пул.
	if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) ||
		errors.Is(err, redis.ErrPoolTimeout) || errors.Is(err, redis.ErrClosed) {
		return true
	}
	// *net.OpError (отказ/обрыв/сброс соединения) и таймауты сокета
	// реализуют net.Error.
	var netErr net.Error
	return errors.As(err, &netErr)
}
