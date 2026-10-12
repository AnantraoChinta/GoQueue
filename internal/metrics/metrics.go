// Package metrics declares the Prometheus collectors exported by GoQueue.
//
// Both binaries use this package so that the scheduler and the worker emit
// metrics with identical names and labels. Prometheus aggregates across
// instances by name and label, so a name that drifts between the two is a
// metric you cannot sum.
package metrics

import (
	"net/http"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// Namespace prefixes every metric name, e.g. goqueue_jobs_processed_total.
const Namespace = "goqueue"

// Metrics owns a private registry and the collectors registered on it.
//
// A dedicated registry rather than prometheus.DefaultRegisterer: the default
// is global mutable state, so tests pollute each other and a double
// registration panics at init. Here the lifetime is explicit and a test can
// build a clean instance.
type Metrics struct {
	registry *prometheus.Registry

	// JobsProcessed counts finished jobs by type and outcome. This is the
	// error-rate numerator and denominator in one metric.
	JobsProcessed *prometheus.CounterVec

	// JobDuration observes execution time by type.
	JobDuration *prometheus.HistogramVec

	// JobsInFlight is how many pool slots are currently running a job.
	// Compared against PoolSize it shows saturation: equal values mean the
	// pool is the bottleneck.
	JobsInFlight prometheus.Gauge

	// PoolSize is the configured concurrency of this process.
	PoolSize prometheus.Gauge

	// QueueDepth is how many jobs are waiting, by state. The key scaling
	// signal: sustained growth means workers are not keeping up. This is
	// what the HorizontalPodAutoscaler should key on, not CPU.
	//
	// AGGREGATION WARNING. This is a property of the queue, not of the
	// process reporting it: every worker samples the same Redis and
	// publishes the same number. Querying sum(goqueue_queue_depth)
	// therefore multiplies the real depth by the replica count - with six
	// workers a backlog of 12 reads as 72, and an autoscaler keyed on that
	// would scale on a number that grows just because you scaled.
	//
	// Always aggregate with max() or avg(), never sum(). The recording
	// rule goqueue:queue_depth:max in deploy/k8s/monitoring.yaml exists so
	// dashboards and the HPA cannot get this wrong.
	QueueDepth *prometheus.GaugeVec
}

// New builds the collectors and registers them on a fresh registry.
func New() *Metrics {
	m := &Metrics{
		registry: prometheus.NewRegistry(),

		JobsProcessed: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: Namespace,
			Name:      "jobs_processed_total",
			Help:      "Jobs finished, by type and outcome.",
			// Labelled by job TYPE, never job ID. One time series per job
			// ID would grow without bound and exhaust Prometheus.
		}, []string{"type", "outcome"}),

		JobDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Namespace: Namespace,
			Name:      "job_duration_seconds",
			Help:      "Job execution time in seconds, by type.",
			// Prometheus' default buckets stop at 10s, which is too short
			// for background jobs: everything slower lands in +Inf and the
			// histogram cannot answer "what is p99". These span 10ms to
			// roughly 3 minutes.
			Buckets: prometheus.ExponentialBuckets(0.01, 3, 10),
		}, []string{"type"}),

		JobsInFlight: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: Namespace,
			Name:      "jobs_in_flight",
			Help:      "Jobs currently executing in this process.",
		}),

		PoolSize: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: Namespace,
			Name:      "pool_size",
			Help:      "Configured number of worker goroutines in this process.",
		}),

		QueueDepth: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: Namespace,
			Name:      "queue_depth",
			Help:      "Jobs in the queue, by state. Global value: aggregate with max(), never sum().",
		}, []string{"state"}),
	}

	m.registry.MustRegister(
		m.JobsProcessed,
		m.JobDuration,
		m.JobsInFlight,
		m.PoolSize,
		m.QueueDepth,
		// Go runtime and process metrics: goroutine count, GC pauses, heap,
		// open file descriptors, CPU. The default registry includes these
		// automatically; a custom one does not, and losing them makes a
		// goroutine leak invisible.
		collectors.NewGoCollector(),
		collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}),
	)

	return m
}

// Registry exposes the underlying registry, for tests and for callers that
// need to register their own collectors alongside these.
func (m *Metrics) Registry() *prometheus.Registry { return m.registry }

// Handler serves the Prometheus exposition format for this registry.
func (m *Metrics) Handler() http.Handler {
	return promhttp.HandlerFor(m.registry, promhttp.HandlerOpts{
		// Report scrape-time collection errors as 500s rather than serving
		// a partially-broken response that looks healthy.
		ErrorHandling: promhttp.HTTPErrorOnError,
	})
}

// JobStarted implements the observer the worker pool calls. It takes no job
// type because in-flight count is not labelled: a per-type gauge would need
// decrementing on exactly the right series, and a missed decrement leaks a
// permanently wrong value.
func (m *Metrics) JobStarted(_ string) {
	m.JobsInFlight.Inc()
}

// JobFinished implements the observer the worker pool calls.
//
// outcome must come from a bounded set; the worker package defines it
// (worker.OutcomeSuccess and friends). Deriving a label from an error
// string would create unbounded cardinality.
func (m *Metrics) JobFinished(jobType, outcome string, d time.Duration) {
	m.JobsInFlight.Dec()
	m.JobsProcessed.WithLabelValues(jobType, outcome).Inc()
	m.JobDuration.WithLabelValues(jobType).Observe(d.Seconds())
}
