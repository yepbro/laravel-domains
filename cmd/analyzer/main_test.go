// Структурный инвариант остановки analyzer (T-232): фоновые циклы run
// (ReanalyzeLoop — пул БД и публикация q:analyze через rdb, коллектор
// длины q:analyze — rdb через гейт) останавливаются производной отменой
// и джойнятся WaitGroup'ом до defer-закрытий store/rdb на любом выходе
// run — штатной отмене ctx, раннем возврате из analyzer.New, панике. До
// T-232 оба цикла жили fire-and-forget: defer-закрытия могли настигнуть
// середину прохода переанализа. Разбор main.go — общий хелпер
// internal/bgjoin (образец и прародитель — TestRunBackgroundJoin
// http-worker, T-223).
package main

import (
	"testing"

	"crawler/internal/bgjoin"
)

func TestRunBackgroundJoin(t *testing.T) {
	bgjoin.Check(t, "main.go")
}
