// Package metrics — the shared registration point of the Prometheus
// metrics of all the Domain Crawler services (spec 5.9): a Registry
// factory over prometheus with a unified service label.
//
// Each service creates its own registry New(name) — the internal/service
// framework raises it together with /livez + /healthz and /metrics: the
// monitor address is monitor.listen from the configuration (spec 5.10),
// the addresses of the other services are METRICS_ADDR; Prometheus
// scrapes each service separately, there is no aggregation through
// monitor. The registry already carries the framework metrics
// (crawler_service_up, crawler_build_info) and the Go/runtime and process
// collectors: the uptime is derived from process_start_time_seconds in
// PromQL, a separate metric is not needed. The working logic adds
// instruments with the factory methods Counter/Gauge/Histogram (T-076) —
// the service label is substituted automatically, the unified label
// names are fixed by the Label* constants. The stage instruments
// (T-077, spec 5.9) are registered by the services themselves through
// the NewMetrics constructors of their worker packages; the queue length
// collector is collect.go of this package.
package metrics

import (
	"errors"
	"fmt"
	"net/http"
	"slices"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// The unified label names of the project metrics (spec 5.9): the stage
// instruments are T-077; the names are fixed here so that the metrics of
// different services have the same labels.
const (
	// LabelService — the service name. Substituted by the factory
	// automatically, not passed by hand.
	LabelService = "service"
	// LabelStage — the pipeline stage (dns, http, render, analyze) for
	// the per-stage throughput and response time.
	LabelStage = "stage"
	// LabelQueue — the queue name (q:dns, q:http, q:render, q:analyze,
	// q:dead — internal/queue) for the queue lengths and the backlog
	// depth.
	LabelQueue = "queue"
	// LabelType — the error type (timeout, tls, network, body, …) for
	// the per-type errors; the same label is the domain status name in
	// the final status counter (not_delegated, temp_error, no_http, …).
	LabelType = "type"
	// LabelState — the domain check state (pending, queued) for the
	// backlog depth from the DB: the label name is fixed by the
	// documentation (docs/services.md, docs/operations.md, spec 5.9) —
	// separate from LabelType: "type" is already taken by the error
	// type / domain status.
	LabelState = "state"
	// LabelKind — the kind of a row within one metric (for example,
	// effective/configured of the DNS limiter caps): the values are
	// local to the metric, unlike the end-to-end LabelStage/LabelQueue
	// values.
	LabelKind = "kind"
	// LabelUnit — the systemd unit name (crawler-http-worker.service)
	// for the monitor unit event metrics (T-145): the system contour,
	// not a pipeline stage — a separate name from LabelService.
	LabelUnit = "unit"
	// LabelEngine — the render engine of the render-worker build
	// (chromium, moli, lightpanda — the internal/config dictionary) for
	// the render stage metrics (T-235, the engines render spec §4.6
	// item 1, FR-15): the series distinguishes the engines; the
	// aggregations without the label (sum by without engine) are
	// unchanged.
	LabelEngine = "engine"
)

// buildMode — the value of the mode label of the crawler_build_info
// metric. Production builds inject it with the linker:
//
//	go build -ldflags "-X crawler/internal/metrics.buildMode=prod"
//
// (deploy/docker/Dockerfile.go, Dockerfile.render, docs/deploy.md §4.2).
// Without the injection — a dev build (go build / air / go test): the
// development framework; the default value is kept so as not to break
// dev.
var buildMode = "dev-stub"

// buildVersion/buildRevision — the values of the version/revision labels
// of the crawler_build_info metric (T-111). Injected by the linker the
// same way as buildMode:
//
//	go build -ldflags "-X crawler/internal/metrics.buildVersion=v1.2.0 \
//	   -X crawler/internal/metrics.buildRevision=abc1234"
//
// The source is the git of the build: buildVersion = git describe --tags
// --always --dirty (a tag or a hash, with a mark of unsaved edits),
// buildRevision = git rev-parse --short HEAD. The Docker build context
// does not contain .git (.dockerignore), so the calling side computes the
// values and passes them as build args (make build-runtime,
// ARG BUILD_VERSION/BUILD_REVISION in the Dockerfile); with git
// unavailable — unknown.
//
// Without the injection — a dev build: version=dev, an empty revision.
// The label name set of the metric is the same for dev and prod (the
// dashboards do not depend on the build method); an empty label value in
// PromQL is equivalent to its absence, so dev builds do not spawn an
// extra value in the groupings by revision.
var (
	buildVersion  = "dev"
	buildRevision = ""
)

// The LabelStage values — the pipeline stages (spec 4.2, §7). The unified
// values are fixed here so that the metrics of all the services
// aggregate in PromQL without normalization.
const (
	// StageDNS — the DNS stage (the dns-worker).
	StageDNS = "dns"
	// StageHTTP — the HTTP stage (the http-worker).
	StageHTTP = "http"
	// StageRender — the render stage (the render-worker).
	StageRender = "render"
	// StageAnalyze — the analysis stage (the analyzer).
	StageAnalyze = "analyze"
)

// Registry — the metrics registry of one service: its own
// *prometheus.Registry (embedded — the Register/Gather methods are
// available for non-standard collectors) and the service name for the
// automatic service label. The service registries are independent: the
// same-named metrics of different services do not conflict — each has
// its own /metrics (spec 5.9).
type Registry struct {
	*prometheus.Registry
	service string
}

// New creates a Prometheus registry for the name service, registers the
// Go/runtime and process collectors and the basic framework metrics. For
// the working-logic metrics the registry is used as a factory:
// Counter/Gauge/Histogram.
func New(name string) *Registry {
	reg := &Registry{
		Registry: prometheus.NewRegistry(),
		service:  name,
	}
	reg.MustRegister(
		collectors.NewGoCollector(),
		collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}),
	)

	up := reg.MustGauge(prometheus.GaugeOpts{
		Name: "crawler_service_up",
		Help: "The service is up and answering.",
	})
	up.WithLabelValues().Set(1)

	build := reg.MustGauge(prometheus.GaugeOpts{
		Name: "crawler_build_info",
		Help: "Build info: mode — the mode (dev-stub — dev/tests, prod — production builds via -ldflags -X), version — the version from git describe (dev — no injection), revision — the short commit hash (empty — no injection, the label is absent in PromQL).",
	}, "mode", "version", "revision")
	build.WithLabelValues(buildMode, buildVersion, buildRevision).Set(1)

	return reg
}

// Counter creates and registers a counter with the labels [LabelService]
// + labelNames and returns the vector curried by service: the caller
// passes the values of the other labels only —
//
//	tasks := reg.MustCounter(prometheus.CounterOpts{
//		Name: "crawler_stage_tasks_total",
//		Help: "Pipeline domains processed, by stage.",
//	}, LabelStage)
//	tasks.WithLabelValues("dns").Inc() // the service is substituted automatically
//
// The HELP/TYPE are exported by Prometheus from Name/Help: the metric
// documentation is written where the instrument is created.
//
// The reserved LabelService label and duplicate labels are rejected with
// an error (not a prometheus.New*Vec panic). A repeated registration of
// an identical metric (the same name, help and labels) is not an error:
// the already registered vector is returned, so the instrument
// constructors may be called repeatedly (the typical
// AlreadyRegisteredError trap does not panic in tests). A descriptor
// conflict is an error.
func (r *Registry) Counter(opts prometheus.CounterOpts, labelNames ...string) (*prometheus.CounterVec, error) {
	name := prometheus.BuildFQName(opts.Namespace, opts.Subsystem, opts.Name)
	labels, err := withServiceLabel(labelNames)
	if err != nil {
		return nil, err
	}
	c, err := r.register(name, prometheus.NewCounterVec(opts, labels))
	if err != nil {
		return nil, err
	}
	vec, ok := c.(*prometheus.CounterVec)
	if !ok {
		return nil, fmt.Errorf("metrics: metric %q is already registered by a collector of another type", name)
	}
	return vec.CurryWith(r.serviceLabels())
}

// Gauge — the Counter counterpart for gauge metrics (the queue lengths,
// the backlog depth, the worker memory): the labels [LabelService] +
// labelNames, the service label is substituted automatically, a duplicate
// of an identical metric is not an error.
func (r *Registry) Gauge(opts prometheus.GaugeOpts, labelNames ...string) (*prometheus.GaugeVec, error) {
	name := prometheus.BuildFQName(opts.Namespace, opts.Subsystem, opts.Name)
	labels, err := withServiceLabel(labelNames)
	if err != nil {
		return nil, err
	}
	c, err := r.register(name, prometheus.NewGaugeVec(opts, labels))
	if err != nil {
		return nil, err
	}
	vec, ok := c.(*prometheus.GaugeVec)
	if !ok {
		return nil, fmt.Errorf("metrics: metric %q is already registered by a collector of another type", name)
	}
	return vec.CurryWith(r.serviceLabels())
}

// Histogram — the Counter counterpart for histograms (the response
// time): the labels [LabelService] + labelNames, the service label is
// substituted automatically, a duplicate of an identical metric is not an
// error. The ObserverVec interface is returned (CurryWith of HistogramVec):
// the observations are added with WithLabelValues(...).Observe(...); the
// caller does not need the HistogramVec specifics. The buckets (Buckets)
// are not part of the prometheus descriptor: a repeated registration with
// other buckets returns the existing vector — the buckets of the first
// call win.
func (r *Registry) Histogram(opts prometheus.HistogramOpts, labelNames ...string) (prometheus.ObserverVec, error) {
	name := prometheus.BuildFQName(opts.Namespace, opts.Subsystem, opts.Name)
	labels, err := withServiceLabel(labelNames)
	if err != nil {
		return nil, err
	}
	c, err := r.register(name, prometheus.NewHistogramVec(opts, labels))
	if err != nil {
		return nil, err
	}
	vec, ok := c.(*prometheus.HistogramVec)
	if !ok {
		return nil, fmt.Errorf("metrics: metric %q is already registered by a collector of another type", name)
	}
	return vec.CurryWith(r.serviceLabels())
}

// MustCounter — Counter with a panic instead of an error. The panic —
// only on a real descriptor conflict (a duplicate of an identical metric
// is not a conflict); it suits the instruments created at the service
// startup, where an error is a program bug.
func (r *Registry) MustCounter(opts prometheus.CounterOpts, labelNames ...string) *prometheus.CounterVec {
	vec, err := r.Counter(opts, labelNames...)
	if err != nil {
		panic(err)
	}
	return vec
}

// MustGauge — Gauge with a panic instead of an error (see MustCounter).
func (r *Registry) MustGauge(opts prometheus.GaugeOpts, labelNames ...string) *prometheus.GaugeVec {
	vec, err := r.Gauge(opts, labelNames...)
	if err != nil {
		panic(err)
	}
	return vec
}

// MustHistogram — Histogram with a panic instead of an error (see
// MustCounter).
func (r *Registry) MustHistogram(opts prometheus.HistogramOpts, labelNames ...string) prometheus.ObserverVec {
	vec, err := r.Histogram(opts, labelNames...)
	if err != nil {
		panic(err)
	}
	return vec
}

// Handler returns an HTTP handler handing out the registry metrics in the
// Prometheus format (the /metrics endpoint of each service, spec 5.9).
// g is any Gatherer: a *Registry of the factory or a *prometheus.Registry
// directly.
func Handler(g prometheus.Gatherer) http.Handler {
	return promhttp.HandlerFor(g, promhttp.HandlerOpts{})
}

// register registers the collector in the registry. An identical
// already-registered metric (an AlreadyRegisteredError with the same
// descriptor — the same name, help and labels) is not an error: the
// existing collector is returned, the duplicate registration is
// idempotent. The other collisions are rejected by prometheus itself:
// the same name with another help or label set is a dimension error.
func (r *Registry) register(name string, c prometheus.Collector) (prometheus.Collector, error) {
	err := r.Register(c)
	if err == nil {
		return c, nil
	}
	var are prometheus.AlreadyRegisteredError
	if !errors.As(err, &are) {
		return nil, err
	}
	if !descEqual(are.ExistingCollector, c) {
		return nil, fmt.Errorf("metrics: metric %q is already registered with another description: %w", name, err)
	}
	return are.ExistingCollector, nil
}

// withServiceLabel — the label names of the factory metrics:
// LabelService is always the first; the caller passes the values of the
// others. LabelService and duplicate labels are rejected with an error —
// prometheus.New*Vec panics on repeated label names.
func withServiceLabel(labelNames []string) ([]string, error) {
	labels := make([]string, 0, len(labelNames)+1)
	labels = append(labels, LabelService)
	for _, l := range labelNames {
		if slices.Contains(labels, l) {
			return nil, fmt.Errorf("metrics: label %q is already taken (LabelService is added automatically)", l)
		}
		labels = append(labels, l)
	}
	return labels, nil
}

// serviceLabels — the currying set of the service label.
func (r *Registry) serviceLabels() prometheus.Labels {
	return prometheus.Labels{LabelService: r.service}
}

// descEqual — whether the descriptors of two collectors match (the name,
// the help, the labels). The histogram buckets (Buckets) are not part of
// the descriptor — they cannot be compared for a registered vector; a
// repeated registration of a histogram with other buckets hands out the
// existing vector (see Histogram).
func descEqual(a, b prometheus.Collector) bool {
	da, oka := descOf(a)
	db, okb := descOf(b)
	return oka && okb && da.String() == db.String()
}

// descOf — the first descriptor of the collector. Describe runs in a
// separate goroutine and its remainder is drained: a non-standard
// collector may send more than one descriptor or none (then ok=false) —
// neither should hang the call. The Counter/Gauge/Histogram vectors give
// exactly one descriptor.
func descOf(c prometheus.Collector) (d *prometheus.Desc, ok bool) {
	ch := make(chan *prometheus.Desc)
	go func() {
		defer close(ch)
		c.Describe(ch)
	}()
	d, ok = <-ch
	if !ok {
		return nil, false
	}
	for range ch { // drain the descriptor remainder
	}
	return d, true
}
