// Источник алерта мусорных content_hash (T-099): счётчик
// crawler_ingest_bad_hashes_total читается из HTTP-экспозиции /metrics
// сервиса ingest — общий реестр на сервис (ТЗ 5.9, metrics.New), у monitor
// своя копия счётчика быть не может (процессы разные). Единственный
// межсервисный зонд пакета: те же адреса, что скрейпит Prometheus
// (deploy/docker/prometheus), без промежуточной БД — счётчик живёт ровно
// в одном месте, у владельца данных. Разбор — вручную по строкам
// текстового формата: нужна одна метрика-счётчик, полноценный парсер
// экспозиции (expfmt) не окупает зависимость.
//
// Деградация: ingest остановлен или метрики в экспозиции нет (старая
// версия) — ошибка источника, алерт «не измеряется» (Warn), не ложное
// срабатывание; смерть самого ingest видна по crawler_service_up каркаса.
package monitor

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"
)

const (
	// badHashesMetricName — имя метрики в экспозиции ingest (T-099).
	badHashesMetricName = "crawler_ingest_bad_hashes_total"

	// badHashesBodyLimit — потолок чтения тела /metrics: реестр ingest
	// несёт коллекторы Go/runtime и процесса (десятки КиБ), потолок —
	// защита от неадекватного ответа, не оценка размера.
	badHashesBodyLimit = 4 << 20
)

// IngestBadHashes — BadHashesSource поверх GET /metrics сервиса ingest:
// адрес — monitor.ingest_metrics_url (dev: http://ingest:9101/metrics,
// бой: loopback ingest по METRICS_ADDR).
type IngestBadHashes struct {
	// URL — адрес экспозиции /metrics сервиса ingest.
	URL string
	// Client — HTTP-клиент; nil — собственный с таймаутом ниже.
	Client *http.Client
}

// compile-time: IngestBadHashes — BadHashesSource.
var _ BadHashesSource = IngestBadHashes{}

// ingestProbeTimeout — бюджет одного чтения: цикл алертов живёт своим
// периодом (60 с), зонд не должен его задерживать отвалом сети.
const ingestProbeTimeout = 5 * time.Second

// BadHashes читает текущее значение счётчика из экспозиции ingest.
func (s IngestBadHashes) BadHashes(ctx context.Context) (int64, error) {
	cl := s.Client
	if cl == nil {
		cl = &http.Client{Timeout: ingestProbeTimeout}
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, s.URL, nil)
	if err != nil {
		return 0, fmt.Errorf("monitor: зонд %s: %w", s.URL, err)
	}
	resp, err := cl.Do(req)
	if err != nil {
		return 0, fmt.Errorf("monitor: зонд %s: %w", s.URL, err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		return 0, fmt.Errorf("monitor: зонд %s: статус %s", s.URL, resp.Status)
	}
	return parseBadHashesExposition(resp.Body)
}

// parseBadHashesExposition достаёт значение счётчика из текстовой
// экспозиции Prometheus: строки «имя{метки} значение [время]», '#'-строки
// (HELP/TYPE) пропускаются. Выделен чистой функцией для табличного теста
// без HTTP.
func parseBadHashesExposition(r io.Reader) (int64, error) {
	sc := bufio.NewScanner(io.LimitReader(r, badHashesBodyLimit))
	for sc.Scan() {
		fields := strings.Fields(sc.Text())
		if len(fields) < 2 || strings.HasPrefix(fields[0], "#") {
			continue
		}
		name, _, _ := strings.Cut(fields[0], "{")
		if name != badHashesMetricName {
			continue
		}
		v, err := strconv.ParseFloat(fields[1], 64)
		if err != nil || v < 0 {
			return 0, fmt.Errorf("monitor: %s: значение %q не счётчик", badHashesMetricName, fields[1])
		}
		return int64(v), nil
	}
	if err := sc.Err(); err != nil {
		return 0, fmt.Errorf("monitor: чтение экспозиции: %w", err)
	}
	return 0, fmt.Errorf("monitor: метрика %s отсутствует в экспозиции ingest", badHashesMetricName)
}
