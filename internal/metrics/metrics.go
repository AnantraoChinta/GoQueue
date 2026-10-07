// Package metrics declares the Prometheus collectors exported by GoQueue.
//
// Both binaries import this package so that the scheduler and the worker
// emit metrics with identical names and labels. Prometheus aggregates across
// instances by label, so a name that drifts between the two is a metric you
// cannot sum.
package metrics

import "github.com/prometheus/client_golang/prometheus"

// Namespace prefixes every metric name, e.g. goqueue_jobs_submitted_total.
const Namespace = "goqueue"

// TODO: construct these with promauto and register them on a dedicated
// *prometheus.Registry rather than the default one, so tests can assert on a
// clean registry and so the Go runtime collectors are opt-in.
//
// Keep label cardinality low: label by job *type* and outcome, never by job
// ID. One time series per job ID will exhaust Prometheus' memory.
var (
	// JobsSubmitted counts jobs accepted by the scheduler, by job type.
	// TODO: prometheus.NewCounterVec, labels: {type}.
	JobsSubmitted *prometheus.CounterVec

	// JobsProcessed counts jobs finished by workers, by type and outcome
	// (success, failure, timeout, panic).
	// TODO: prometheus.NewCounterVec, labels: {type, outcome}.
	JobsProcessed *prometheus.CounterVec

	// JobDuration observes how long jobs take to execute, by type.
	// TODO: prometheus.NewHistogramVec, labels: {type}. Pick buckets that
	// match the expected job latency; the default buckets top out at 10s.
	JobDuration *prometheus.HistogramVec

	// QueueDepth reports how many jobs are waiting, by queue name. This is
	// the key scaling signal: sustained growth means workers are not
	// keeping up.
	// TODO: prometheus.NewGaugeVec, labels: {queue}.
	QueueDepth *prometheus.GaugeVec

	// WorkersActive reports how many pool goroutines are currently running
	// a job, versus idle.
	// TODO: prometheus.NewGaugeVec, labels: {state}.
	WorkersActive *prometheus.GaugeVec
)

// Register adds every collector above to the given registry.
//
// TODO: implement, and return an error rather than panicking on duplicate
// registration so the caller controls startup failure.
func Register(r prometheus.Registerer) error {
	// TODO: implement.
	return nil
}
