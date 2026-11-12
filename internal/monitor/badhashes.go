// The source of the junk content_hash alert (T-099): the crawler_ingest_bad_hashes_total
// counter is read from the HTTP /metrics exposition of the ingest
// service — a shared registry per service (spec 5.9, metrics.New); monitor
// cannot have its own copy of the counter (different processes). The only
// inter-service probe of the package: the same addresses Prometheus scrapes
// (deploy/docker/prometheus), without an intermediate DB — the counter lives in exactly
// one place, at the data owner. Parsing — manually, by the lines
// of the text format: a single counter metric is needed; a full exposition
// parser (expfmt) does not pay for the dependency.
//
// Degradation: ingest stopped or no metric in the exposition (an old
// version) — a source error, the alert is "not measured" (a Warn), not a
// false firing; the death of ingest itself is seen via the framework's crawler_service_up.
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
	// badHashesMetricName — the metric name in the ingest exposition (T-099).
	badHashesMetricName = "crawler_ingest_bad_hashes_total"

	// badHashesBodyLimit — the cap on reading the /metrics body: the ingest registry
	// carries the Go/runtime and process collectors (tens of KiB); the cap is
	// protection against an inadequate answer, not a size estimate.
	badHashesBodyLimit = 4 << 20
)

// IngestBadHashes — a BadHashesSource on top of GET /metrics of the ingest service:
// the address is monitor.ingest_metrics_url (dev: http://ingest:9101/metrics,
// production: the ingest loopback by METRICS_ADDR).
type IngestBadHashes struct {
	// URL — the address of the /metrics exposition of the ingest service.
	URL string
	// Client — the HTTP client; nil — its own with the timeout below.
	Client *http.Client
}

// compile-time: IngestBadHashes is a BadHashesSource.
var _ BadHashesSource = IngestBadHashes{}

// ingestProbeTimeout — the budget of one read: the alert loop lives by its
// own period (60 s); the probe must not delay it by a network hang.
//
// An exact budget value is only distinguishable by a pause of seconds against
// a hung server (4/5/6 s; 5/time.Second gives Timeout 0 — "no
// timeout", which differs from 5 s in the same place) — it contradicts
// the hermeticity of the package's unit tests (docs/mutation-testing.md §4):
// the value is a tuning budget, not logic.
// mutator-disable-next-line arithmetic/base, numbers/decrementer, numbers/incrementer
const ingestProbeTimeout = 5 * time.Second

// BadHashes reads the current counter value from the ingest exposition.
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

// parseBadHashesExposition extracts the counter value from the text
// Prometheus exposition: the lines "name{labels} value [time]"; '#'-lines
// (HELP/TYPE) are skipped. Factored out as a pure function for a table test
// without HTTP.
func parseBadHashesExposition(r io.Reader) (int64, error) {
	sc := bufio.NewScanner(io.LimitReader(r, badHashesBodyLimit))
	for sc.Scan() {
		fields := strings.Fields(sc.Text())
		// Skipping #-lines is equivalent to the subsequent name comparison: Cut
		// of the field with '#' gives a name with '#', unequal to the metric — there
		// are no inputs distinguishing the `|| false` mutant. The twin `false ||`
		// (removing the length check) is covered by the table ("a name without a value" —
		// an index panic), but the annotation applies to the whole line.
		// mutator-disable-next-line expression/remove
		if len(fields) < 2 || strings.HasPrefix(fields[0], "#") {
			continue
		}
		name, _, _ := strings.Cut(fields[0], "{")
		if name != badHashesMetricName {
			continue
		}
		// ParseFloat only distinguishes bitSize=32 (rounding to float32);
		// 63 and 65 behave like 64 on any input (strconv/atof.go) —
		// equivalent mutants.
		// mutator-disable-next-line numbers/decrementer, numbers/incrementer
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
