// CPU pressure metrics via PSI (T-162): gauge
// crawler_psi_cpu_some_percent{kind=avg10|avg60|avg300} — the some share
// of CPU stall time from /proc/pressure/cpu, the same pass that feeds
// the psi_cpu alert. A PSISource wrapper, not a separate loop:
// reading /proc is cheap, but the single point keeps the metric values
// and the alert threshold from diverging (UnitEventsMetrics style, T-145).
package monitor

import (
	"context"

	"github.com/prometheus/client_golang/prometheus"

	"crawler/internal/metrics"
)

// Values of the kind label of the crawler_psi_cpu_some_percent metric.
const (
	psiKindAvg10  = "avg10"
	psiKindAvg60  = "avg60"
	psiKindAvg300 = "avg300"
)

// PSIMetrics — PSI gauges on top of another PSISource (pass-through plus
// mirroring into metrics).
type PSIMetrics struct {
	// gauge — crawler_psi_cpu_some_percent{kind}.
	gauge *prometheus.GaugeVec
	// src — the underlying source (required).
	src PSISource
}

// compile-time: PSIMetrics — PSISource.
var _ PSISource = (*PSIMetrics)(nil)

// NewPSIMetrics wraps src in a metrics-reporting source in reg; nil reg —
// pure pass-through (NewUnitEventsMetrics style). Idempotent (T-076).
func NewPSIMetrics(reg *metrics.Registry, src PSISource) *PSIMetrics {
	m := &PSIMetrics{src: src}
	if reg == nil {
		return m
	}
	m.gauge = reg.MustGauge(prometheus.GaugeOpts{
		Name: "crawler_psi_cpu_some_percent",
		Help: "The CPU stall time share per PSI some (%) from /proc/pressure/cpu (T-162): kind=avg10/avg60/avg300 — the window averages. A sustained avg300 above ~90% — scheduler starvation, the monitor psi_cpu alert (all stages drown; measurement §12: some 91–95%).",
	}, metrics.LabelKind)
	return m
}

// PSI reads the underlying source and mirrors the result into metrics. A
// source error leaves the metrics untouched — they keep the last value
// (hold-last, UnitEventsMetrics style): a read failure does not break the graph.
func (m *PSIMetrics) PSI(ctx context.Context) (PSIStat, error) {
	st, err := m.src.PSI(ctx)
	if err != nil {
		return PSIStat{}, err
	}
	if m.gauge != nil {
		m.gauge.WithLabelValues(psiKindAvg10).Set(st.Avg10)
		m.gauge.WithLabelValues(psiKindAvg60).Set(st.Avg60)
		m.gauge.WithLabelValues(psiKindAvg300).Set(st.Avg300)
	}
	return st, nil
}
