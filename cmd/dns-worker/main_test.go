// Структурный инвариант остановки dns-worker (T-232): фоновые коллекторы
// run (длина q:dns — rdb через гейт, состояние лимитёра T-035,
// переключения на резерв T-107 — метрики) останавливаются производной
// отменой и джойнятся WaitGroup'ом до defer-закрытий rdb и клиентов цепи
// резолверов на любом выходе run — штатной отмене ctx, панике. До T-232
// все три жили fire-and-forget: defer-закрытия могли настигнуть запрос
// глубины очереди. Разбор main.go — общий хелпер internal/bgjoin
// (образец и прародитель — TestRunBackgroundJoin http-worker, T-223).
package main

import (
	"testing"

	"crawler/internal/bgjoin"
)

func TestRunBackgroundJoin(t *testing.T) {
	bgjoin.Check(t, "main.go")
}
