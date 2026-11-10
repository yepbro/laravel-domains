// Тесты триажа выживших мутантов CI-прогона 10.11.2026 (часть B,
// score 0.857): добивки покрытия status — часы кэша по умолчанию (nil
// Now — time.Now, вызов не паникует), защитная копия кэшированной карты
// полная (обрыв копии портил бы кэш первым же вызовом), единичный TTL —
// законное явное значение; имя статуса вне легенды не прерывает обход
// ответа (более поздние легендные строки считаются); окно темпа в
// одну секунду — реальное окно, а не дефолт.
package status

import (
	"context"
	"testing"
	"time"

	"crawler/internal/model"
)

// TestCacheTriage — механика TTL-кэша: часы по умолчанию (nil Now —
// time.Now), полная защитная копия карты, единичный TTL.
func TestCacheTriage(t *testing.T) {
	// TTL 1нс — положительное явное значение, дефолт не заменяет.
	if o := (CacheOptions{TTL: time.Nanosecond}).withDefaults(); o.TTL != time.Nanosecond {
		t.Errorf("TTL 1ns заменён дефолтом: %+v (дефолт — только для ≤ 0)", o)
	}

	// NewCache без часов: nil-Now обязан подмениться time.Now — первый
	// же вызов не паникует (mutant оставлял nil-функцию).
	c := NewCache(CacheOptions{})
	got, err := cached(c, &c.statuses, func() (map[string]int64, error) {
		return map[string]int64{"new": 1, "alive": 2}, nil
	})
	if err != nil {
		t.Fatalf("cached без явных часов: %v", err)
	}
	// Защитная копия ПОЛНАЯ: обрыв/пропуск записи в cloneOf возвращал бы
	// урезанную карту (мутация результата callers'ом рвала бы кэш).
	if len(got) != 2 || got["new"] != 1 || got["alive"] != 2 {
		t.Errorf("копия кэш-значения = %v, хочу обе записи (полная копия)", got)
	}
}

// TestFetchDomainStatusesTriageUnknownFirst — имя вне легенды
// model.Statuses НЕ прерывает обход ответа (continue, не break):
// легендные строки после него считаются.
func TestFetchDomainStatusesTriageUnknownFirst(t *testing.T) {
	pool := newStatusPool()
	pool.domStatuses = []statusStatusSeed{
		{"unknown-code", 7}, // вне легенды — первой строкой ответа
		{model.StatusAlive.String(), 40},
		{model.StatusNew.String(), 1000},
	}
	got, err := FetchDomainStatuses(context.Background(), pool)
	if err != nil {
		t.Fatalf("FetchDomainStatuses: %v", err)
	}
	if got["alive"] != 40 || got["new"] != 1000 {
		t.Errorf("статусы после неизвестного имени = %v: живые строки обязаны считаться", got)
	}
}

// TestComputeRatesTriageWindowOne — окно корзин в одну секунду —
// реальное окно (деление на 1), а не дефолт speedWindowSec: ручной
// сбор значений с узким окном не занижает темп.
func TestComputeRatesTriageWindowOne(t *testing.T) {
	r := ComputeRates(Throughput{HourWindowSec: 1, ResultsHour: 3600, RenderedHour: 60}, 0, 0)
	if r.ResultsPerSec != 3600 || r.RenderPerSec != 60 {
		t.Errorf("темп окна 1с = %v/%v, хочу 3600/60 (деление на фактическую длину окна)",
			r.ResultsPerSec, r.RenderPerSec)
	}
}
