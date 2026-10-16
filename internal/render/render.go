// Package render — каркас рендера страниц в headless-Chromium (ТЗ 5.6).
//
// Реализация (один браузер с 2–4 изолированными контекстами, блокировка
// загрузки изображений/шрифтов/медиа, ожидание «затишья» сети, снятие
// итогового DOM и cookies через CDP) — задачи T-061…T-067. Сейчас пакет
// фиксирует способ подключения к внешнему Chromium: в dev-окружении
// Chromium поднят отдельным контейнером, его CDP-адрес передаётся
// переменной окружения CHROMIUM_WS_URL.
package render

import (
	"context"

	"github.com/chromedp/chromedp"
	"github.com/chromedp/chromedp/remote"
)

// NewRemoteContext создаёт контекст chromedp, подключённый к внешнему
// headless-Chromium по CDP (wsURL вида ws://127.0.0.1:9222; если в адресе
// нет /devtools/browser/, адрес websocket-эндпоинта браузера уточняется
// запросом /json/version). Соединение устанавливается лениво — при первом
// действии (chromedp.Run), поэтому функция не выполняет I/O. Второе
// возвращаемое значение отменяет контекст и закрывает соединение.
func NewRemoteContext(ctx context.Context, wsURL string) (context.Context, context.CancelFunc) {
	allocCtx, cancelAlloc := remote.NewAllocator(ctx, wsURL)
	browserCtx, cancelBrowser := chromedp.NewContext(allocCtx)
	return browserCtx, func() {
		cancelBrowser()
		cancelAlloc()
	}
}
