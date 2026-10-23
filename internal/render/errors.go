package render

import (
	"errors"
	"fmt"

	"github.com/chromedp/chromedp"
)

// Class — класс ошибки рендера; определяет реакцию на сбой (ТЗ 5.6, §8):
// воркер не должен диагностировать Chromium по текстам ошибок библиотеки.
type Class int

const (
	// ClassTask — ошибка страницы: превышен Options.Timeout, отменён
	// вызывающий контекст, страница не загрузилась (net::ERR_*).
	// Повтор на новом контексте бессмысленен; повторная доставка —
	// уровень очереди (T-066).
	ClassTask Class = iota
	// ClassContext — умер изолированный контекст (вкладка или рендер
	// упали, профиль не отвечает): Pool повторяет задачу один раз на
	// новом контексте.
	ClassContext
	// ClassBrowser — Chromium недоступен или соединение потеряно: нужен
	// перезапуск браузера (T-065); Pool не повторяет.
	ClassBrowser
)

// String — имя класса для логов и сообщений об ошибках.
func (c Class) String() string {
	switch c {
	case ClassTask:
		return "task"
	case ClassContext:
		return "context"
	case ClassBrowser:
		return "browser"
	default:
		return fmt.Sprintf("class(%d)", int(c))
	}
}

// Error — ошибка рендера с классификацией. errors.As(*Error) даёт класс,
// Unwrap — исходную причину (включая context.Canceled/DeadlineExceeded).
type Error struct {
	// Class — реакция на ошибку, см. константы выше.
	Class Class
	// URL — страница задачи (диагностика).
	URL string
	// Err — исходная ошибка.
	Err error
}

// Error реализует error.
func (e *Error) Error() string {
	if e.URL != "" {
		return fmt.Sprintf("render %s: %v (url %q)", e.Class, e.Err, e.URL)
	}
	return fmt.Sprintf("render %s: %v", e.Class, e.Err)
}

// Unwrap даёт доступ к исходной ошибке (errors.Is/As проходят насквозь).
func (e *Error) Unwrap() error { return e.Err }

// ClassOf возвращает класс ошибки. Все ошибки этого пакета — *Error;
// посторонние ошибки трактуются как ошибки страницы (без ретраев).
func ClassOf(err error) Class {
	var e *Error
	if errors.As(err, &e) {
		return e.Class
	}
	return ClassTask
}

// LoadErrorCode — код ошибки загрузки страницы из цепочки ошибки: текст
// браузера из net/base/net_error_list.h (например net::ERR_NAME_NOT_RESOLVED
// или net::ERR_CERT_AUTHORITY_INVALID), которым chromedp маркирует сбой
// навигации (тип LoadError, errors.Is с ErrPageLoad; classifyCtx относит
// его к классу task). Пустая строка — ошибки загрузки страницы в цепочке
// нет. Код — поле LoadError.ErrorText, а не подстрока текста ошибки:
// точное сравнение стабильно против формата сообщений библиотеки.
// Потребитель — политика постоянных ошибок сертификата хоста
// (renderworker, T-159): сам код решения не принимает.
func LoadErrorCode(err error) string {
	var le *chromedp.LoadError
	if errors.As(err, &le) {
		return le.ErrorText
	}
	return ""
}

// ErrClosed — пул или браузер остановлены, задача не выполнялась.
// Гарантирует перевод задачи в очередь на повторную доставку (T-066).
var ErrClosed = errors.New("рендер остановлен")

// ErrBrowserDead — браузерная сессия не инициализирована или потеряна
// (проверка до создания сессии, чтобы не гонять заведомо падающие команды).
var ErrBrowserDead = errors.New("браузерная сессия Chromium недоступна")
