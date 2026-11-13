// Instruments of the public domain check form (T-221): the outcomes of
// the POST /api/check-domain requests, the failures by class, and the
// processing duration — the form's healthy operation is visible from
// Prometheus (:9107), not only from journald. The label semantics —
// like the stage instruments (T-077): service is set by the registry
// factory, result/type are local to the form.
package web

import (
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"crawler/internal/metrics"
)

// The values of the result label of the crawler_web_checks_total
// counter — the form request outcome, exactly one per request.
const (
	// checkResultOK — 200: the live check passed.
	checkResultOK = "ok"
	// checkResultStoredOnly — 200: the live check failed, the response
	// is assembled from the stored pipeline data.
	checkResultStoredOnly = "stored_only"
	// checkResultFailed — 502: the live check failed, no stored data.
	checkResultFailed = "failed"
	// checkResultBusy — 503: the semaphore of concurrent live checks is
	// full (T-220), a fast rejection with Retry-After.
	checkResultBusy = "busy"
	// checkResultRateLimited — 429: the form rate limit is exhausted.
	checkResultRateLimited = "rate_limited"
	// checkResultInvalid — 422: junk input (the landing client format).
	checkResultInvalid = "invalid"
)

// The values of the type label of the crawler_web_check_errors_total
// counter — the form failure class (the stages label LabelStage is
// taken by the dns/http/render/analyze pipeline and is not used for the
// local series).
const (
	// checkErrLive — the live check pipeline failed (not an overload;
	// an overload is the normal busy outcome, not an error).
	checkErrLive = "live"
	// checkErrJournal — the INSERT into web_checks (T-120) failed: the
	// manual checks log loses a row (before T-221 it was lost silently).
	checkErrJournal = "journal"
	// checkErrNonIP — the client address is not an IP (a unix-socket
	// peer without a valid proxy header): the web_checks record is
	// skipped — the ip_address inet NOT NULL column does not accept an
	// arbitrary string.
	checkErrNonIP = "non_ip"
)

// webCheckSecondsBuckets — the form request duration buckets: the
// web.timeout cap of 60s (DNS + HTTP of both schemes + the SPA render),
// light domains — a few seconds.
var webCheckSecondsBuckets = []float64{0.25, 0.5, 1, 2.5, 5, 10, 20, 30, 45, 60, 120}

// Metrics — the form instruments; built by NewMetrics from the service
// registry; nil (no registry — isolated tests) — all methods are no-op.
// Thread-safe: the prometheus instruments are synchronized.
type Metrics struct {
	checks  *prometheus.CounterVec // crawler_web_checks_total{result}
	errors  *prometheus.CounterVec // crawler_web_check_errors_total{stage}
	seconds prometheus.Observer    // crawler_web_check_seconds
}

// NewMetrics registers the form instruments in the reg registry; nil —
// nil. Idempotent (T-076): a repeated call shares the same collectors.
func NewMetrics(reg *metrics.Registry) *Metrics {
	if reg == nil {
		return nil
	}
	return &Metrics{
		checks: reg.MustCounter(prometheus.CounterOpts{
			Name: "crawler_web_checks_total",
			Help: "Domain check form requests by outcome: ok — the live check passed (200), stored_only — the live check failed, the answer is assembled from the stored data (200), failed — the live check failed with no stored data (502), busy — the live check semaphore is overloaded (503), rate_limited — the form rate limit (429), invalid — junk input (422).",
		}, "result"),
		errors: reg.MustCounter(prometheus.CounterOpts{
			Name: "crawler_web_check_errors_total",
			Help: "Domain check form failures by class: live — the live check pipeline failed (an overload is the busy outcome, not an error), journal — the INSERT into web_checks failed (a journal row is lost), non_ip — the client address is not an IP (a unix socket without a valid proxy header), the journal entry is skipped.",
		}, metrics.LabelType),
		seconds: reg.MustHistogram(prometheus.HistogramOpts{
			Name:    "crawler_web_check_seconds",
			Help:    "The full processing time of a form request that passed the rate limit and validation (the stored read, the live check, the journal write; 429/422 and the 503 overload are not observed — no work was done).",
			Buckets: webCheckSecondsBuckets,
		}).WithLabelValues(),
	}
}

// Outcome — the form request outcome: +1 into the counter by result.
func (m *Metrics) Outcome(result string) {
	if m == nil {
		return
	}
	m.checks.WithLabelValues(result).Inc()
}

// Failure — a form failure by class: +1 into the counter by type.
func (m *Metrics) Failure(kind string) {
	if m == nil {
		return
	}
	m.errors.WithLabelValues(kind).Inc()
}

// Duration — the processing duration of a request that reached the
// live check.
func (m *Metrics) Duration(d time.Duration) {
	if m == nil {
		return
	}
	m.seconds.Observe(d.Seconds())
}
