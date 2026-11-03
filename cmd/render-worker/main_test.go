// Структурный инвариант остановки render-worker (T-232): фоновые циклы
// run (DiskWatchLoop — store, коллектор длины q:render — rdb через гейт)
// останавливаются производной отменой и джойнятся WaitGroup'ом до
// defer-закрытий rdb/store/менеджера Chromium на любом выходе run —
// штатной отмене ctx, раннем возврате из renderworker.New, панике. До
// T-232 оба цикла жили fire-and-forget: defer-закрытия могли настигнуть
// середину прохода DiskWatchLoop или запроса глубины очереди. Разбор
// main.go — общий хелпер internal/bgjoin (образец и прародитель —
// TestRunBackgroundJoin http-worker, T-223).
package main

import (
	"testing"

	"crawler/internal/bgjoin"
)

func TestRunBackgroundJoin(t *testing.T) {
	bgjoin.Check(t, "main.go")
}
