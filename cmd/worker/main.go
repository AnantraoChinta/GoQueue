// Command worker is the GoQueue worker process entry point.
//
// It claims jobs from the queue and executes them on a pool of goroutines,
// exposing Prometheus metrics on /metrics. Many worker processes run
// concurrently; each one is stateless and may be killed or rescheduled at
// any time.
//
// Pass -redis-addr to share a queue with the scheduler and other workers.
// Without it the queue is in-process only (queue.MemoryQueue), which is
// useful for a quick local run with -demo-jobs but invisible to any other
// process.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/anantraochinta/goqueue/internal/metrics"
	"github.com/anantraochinta/goqueue/internal/queue"
	"github.com/anantraochinta/goqueue/internal/worker"
)

type config struct {
	redisAddr      string
	redisPrefix    string
	connectTimeout time.Duration
	claimTTL       time.Duration
	reapInterval   time.Duration
	metricsAddr    string
	poolSize       int
	jobTimeout     time.Duration
	claimBlock     time.Duration
	shutdownGrace  time.Duration
	depthInterval  time.Duration
	logLevel       string
	workerID       string
	demoJobs       int
}

func main() {
	if err := run(); err != nil {
		slog.Error("worker exited with error", "error", err)
		os.Exit(1)
	}
}

func run() error {
	cfg := parseConfig()

	logger := newLogger(cfg.logLevel)
	slog.SetDefault(logger)

	// NotifyContext cancels ctx on SIGINT/SIGTERM. Kubernetes sends SIGTERM
	// first and only SIGKILLs after terminationGracePeriodSeconds, which is
	// the window the pool uses to drain.
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	m := metrics.New()
	m.PoolSize.Set(float64(cfg.poolSize))

	// The HTTP server starts FIRST, before connecting to Redis.
	//
	// /healthz answers "is this process wedged", which is true the moment
	// the process is up - it must not depend on Redis. Starting the
	// listener after a connection attempt that retries for up to a minute
	// means the liveness probe gets connection-refused and Kubernetes
	// kills the container mid-retry, which is a crash loop caused entirely
	// by our own startup ordering.
	srv := newMetricsServer(cfg.metricsAddr, m)
	serveErr := make(chan error, 1)
	go func() {
		logger.Info("metrics listening", "addr", cfg.metricsAddr)
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			serveErr <- err
			return
		}
		serveErr <- nil
	}()

	q, closeQueue, err := buildQueue(ctx, cfg, logger)
	if err != nil {
		shutdownCtx, cancel := context.WithTimeout(context.Background(), cfg.shutdownGrace)
		defer cancel()
		_ = srv.Shutdown(shutdownCtx)
		return err
	}
	defer closeQueue()

	if cfg.demoJobs > 0 {
		if err := seedDemoJobs(ctx, q, cfg.demoJobs); err != nil {
			return fmt.Errorf("seeding demo jobs: %w", err)
		}
		logger.Info("seeded demo jobs", "count", cfg.demoJobs)
	}

	go sampleQueueDepth(ctx, q, m, cfg.depthInterval, logger)
	go reclaimExpired(ctx, q, cfg.reapInterval, logger)

	pool := &worker.Pool{
		Queue:      q,
		Handlers:   demoHandlers(),
		Size:       cfg.poolSize,
		JobTimeout: cfg.jobTimeout,
		ClaimBlock: cfg.claimBlock,
		WorkerID:   cfg.workerID,
		Logger:     logger,
		Observer:   m,
	}

	// Blocks until ctx is cancelled and every in-flight job has finished.
	runErr := pool.Run(ctx)

	// The metrics server is shut down only AFTER the pool has drained, so
	// Prometheus can still scrape final counter values during shutdown.
	shutdownCtx, cancel := context.WithTimeout(context.Background(), cfg.shutdownGrace)
	defer cancel()
	if err := srv.Shutdown(shutdownCtx); err != nil {
		logger.Warn("metrics server shutdown", "error", err)
	}

	if runErr != nil {
		return runErr
	}
	return <-serveErr
}

// buildQueue returns the configured queue and a cleanup func.
func buildQueue(ctx context.Context, cfg config, logger *slog.Logger) (queue.Queue, func(), error) {
	if cfg.redisAddr == "" {
		logger.Warn("no -redis-addr: using an in-process queue that no other process can see")
		return queue.NewMemoryQueue(), func() {}, nil
	}

	rdb := redis.NewClient(&redis.Options{Addr: cfg.redisAddr})
	q, err := queue.NewWithRetry(ctx, rdb, cfg.redisPrefix, cfg.connectTimeout, queue.WithClaimTTL(cfg.claimTTL))
	if err != nil {
		_ = rdb.Close()
		// Fail at startup rather than on the first job: a worker that
		// cannot reach Redis should crash-loop visibly, not sit idle
		// looking healthy.
		return nil, nil, fmt.Errorf("connecting to redis at %s: %w", cfg.redisAddr, err)
	}
	logger.Info("connected to redis", "addr", cfg.redisAddr, "prefix", cfg.redisPrefix,
		"claim_ttl", cfg.claimTTL)
	return q, func() { _ = rdb.Close() }, nil
}

// reclaimExpired periodically returns jobs whose claims expired to the
// pending set. This is what makes the durability guarantee true: without
// it, a job held by a worker that was OOM-killed is stranded forever.
//
// Every worker runs this. The underlying script is atomic, so concurrent
// reapers are safe and the work is naturally shared.
func reclaimExpired(ctx context.Context, q queue.Queue, every time.Duration, logger *slog.Logger) {
	type reclaimer interface {
		ReclaimExpired(context.Context, int) (int, error)
	}
	r, ok := q.(reclaimer)
	if !ok {
		return // MemoryQueue is single-process; there is nothing to repair.
	}

	ticker := time.NewTicker(every)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			n, err := r.ReclaimExpired(ctx, 100)
			if err != nil {
				logger.Warn("reclaim failed", "error", err)
				continue
			}
			if n > 0 {
				// Worth a log line: steady reclaims mean workers are dying
				// or ClaimTTL is shorter than real job durations.
				logger.Info("reclaimed expired claims", "count", n)
			}
		}
	}
}

func newMetricsServer(addr string, m *metrics.Metrics) *http.Server {
	mux := http.NewServeMux()
	mux.Handle("GET /metrics", m.Handler())

	// Liveness only: is this process wedged? It deliberately does not touch
	// the queue. A liveness probe that checks a dependency turns a brief
	// outage of that dependency into a restart storm of your own pods.
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok\n"))
	})

	return &http.Server{
		Addr:    addr,
		Handler: mux,
		// Without these a slow or malicious client can hold a connection
		// open indefinitely; Go's defaults are no timeout at all.
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       10 * time.Second,
		WriteTimeout:      30 * time.Second,
		IdleTimeout:       60 * time.Second,
	}
}

// sampleQueueDepth polls the queue and publishes the depth gauge.
//
// It is a poller rather than a push from the queue because the gauge must
// reflect reality even when nothing is happening - a queue that stopped
// being drained is exactly the case you need to see.
func sampleQueueDepth(ctx context.Context, q queue.Queue, m *metrics.Metrics, every time.Duration, logger *slog.Logger) {
	// Optional capability, asserted rather than required on the Queue
	// interface.
	type depther interface {
		Depth(context.Context) (map[queue.State]int, error)
	}
	d, ok := q.(depther)
	if !ok {
		logger.Warn("queue does not report depth; goqueue_queue_depth will stay at zero")
		return
	}

	ticker := time.NewTicker(every)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			counts, err := d.Depth(ctx)
			if err != nil {
				logger.Warn("queue depth sample failed", "error", err)
				continue
			}
			// Every state is set explicitly, including zeros. Omitting a
			// state would leave its last non-zero value published forever.
			for _, state := range []queue.State{
				queue.StatePending, queue.StateClaimed, queue.StateSucceeded,
				queue.StateFailed, queue.StateDead,
			} {
				m.QueueDepth.WithLabelValues(string(state)).Set(float64(counts[state]))
			}
		}
	}
}

// demoHandlers are placeholders so the worker does something runnable.
// Replace them with real job types.
func demoHandlers() map[string]worker.Handler {
	return map[string]worker.Handler{
		"noop": worker.HandlerFunc(func(_ context.Context, _ *queue.Job) error {
			return nil
		}),

		// Honours cancellation, so it is also a demonstration of a
		// well-behaved handler under JobTimeout and shutdown.
		"sleep": worker.HandlerFunc(func(ctx context.Context, job *queue.Job) error {
			var args struct {
				Duration string `json:"duration"`
			}
			if len(job.Payload) > 0 {
				if err := json.Unmarshal(job.Payload, &args); err != nil {
					return fmt.Errorf("bad payload: %w", err)
				}
			}
			d := 200 * time.Millisecond
			if args.Duration != "" {
				parsed, err := time.ParseDuration(args.Duration)
				if err != nil {
					return fmt.Errorf("bad duration %q: %w", args.Duration, err)
				}
				d = parsed
			}
			select {
			case <-time.After(d):
				return nil
			case <-ctx.Done():
				return ctx.Err()
			}
		}),

		// Always fails, to exercise the retry and dead-letter paths.
		"flaky": worker.HandlerFunc(func(_ context.Context, job *queue.Job) error {
			return fmt.Errorf("demo failure on attempt %d", job.Attempts)
		}),
	}
}

func seedDemoJobs(ctx context.Context, q queue.Queue, n int) error {
	types := []string{"noop", "sleep", "flaky"}
	for i := range n {
		job := &queue.Job{
			ID:          fmt.Sprintf("demo-%d", i),
			Type:        types[i%len(types)],
			Payload:     json.RawMessage(`{"duration":"150ms"}`),
			MaxAttempts: 3,
		}
		if err := q.Enqueue(ctx, job); err != nil {
			return err
		}
	}
	return nil
}

func parseConfig() config {
	var c config
	flag.DurationVar(&c.connectTimeout, "redis-connect-timeout", envDur("GOQUEUE_REDIS_CONNECT_TIMEOUT", 60*time.Second),
		"how long to keep retrying the initial Redis connection before giving up")
	flag.StringVar(&c.redisAddr, "redis-addr", envStr("GOQUEUE_REDIS_ADDR", ""),
		"Redis address, e.g. localhost:6379; empty uses an in-process queue")
	flag.StringVar(&c.redisPrefix, "redis-prefix", envStr("GOQUEUE_REDIS_PREFIX", "goqueue"),
		"key prefix, so several environments can share one Redis")
	flag.DurationVar(&c.claimTTL, "claim-ttl", envDur("GOQUEUE_CLAIM_TTL", queue.DefaultClaimTTL),
		"how long a claim is honoured before another worker may take the job; must exceed -job-timeout")
	flag.DurationVar(&c.reapInterval, "reap-interval", envDur("GOQUEUE_REAP_INTERVAL", 15*time.Second),
		"how often to return expired claims to the queue")
	flag.StringVar(&c.metricsAddr, "metrics-addr", envStr("GOQUEUE_METRICS_ADDR", ":8081"),
		"address for the /metrics and /healthz endpoints")
	flag.IntVar(&c.poolSize, "pool-size", envInt("GOQUEUE_POOL_SIZE", worker.DefaultSize),
		"number of concurrent worker goroutines")
	flag.DurationVar(&c.jobTimeout, "job-timeout", envDur("GOQUEUE_JOB_TIMEOUT", worker.DefaultJobTimeout),
		"maximum run time for a single job")
	flag.DurationVar(&c.claimBlock, "claim-block", envDur("GOQUEUE_CLAIM_BLOCK", worker.DefaultClaimBlock),
		"how long a worker waits for a job before looping")
	flag.DurationVar(&c.shutdownGrace, "shutdown-grace", envDur("GOQUEUE_SHUTDOWN_GRACE", 5*time.Second),
		"time allowed for the metrics server to shut down after the pool drains")
	flag.DurationVar(&c.depthInterval, "depth-interval", envDur("GOQUEUE_DEPTH_INTERVAL", 5*time.Second),
		"how often to sample queue depth")
	flag.StringVar(&c.logLevel, "log-level", envStr("GOQUEUE_LOG_LEVEL", "info"),
		"debug, info, warn or error")
	flag.StringVar(&c.workerID, "worker-id", envStr("GOQUEUE_WORKER_ID", defaultWorkerID()),
		"identifier recorded on claims; use the pod name in Kubernetes")
	flag.IntVar(&c.demoJobs, "demo-jobs", envInt("GOQUEUE_DEMO_JOBS", 0),
		"seed this many demo jobs at startup (local development only)")
	flag.Parse()

	// A claim that expires while the job is still legitimately running
	// means two workers execute it at once. Warn rather than fail: the
	// operator may know something we do not.
	if c.claimTTL <= c.jobTimeout {
		fmt.Fprintf(os.Stderr,
			"warning: -claim-ttl (%v) <= -job-timeout (%v); long jobs may be redelivered while still running\n",
			c.claimTTL, c.jobTimeout)
	}
	return c
}

func defaultWorkerID() string {
	// In Kubernetes the downward API sets HOSTNAME to the pod name, which
	// is exactly the identifier you want recorded on a claim.
	if h, err := os.Hostname(); err == nil && h != "" {
		return h
	}
	return "worker"
}

func newLogger(level string) *slog.Logger {
	var l slog.Level
	if err := l.UnmarshalText([]byte(level)); err != nil {
		l = slog.LevelInfo
	}
	// JSON because these lines are destined for a log aggregator, not a
	// human terminal, once this runs in Kubernetes.
	return slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: l}))
}

func envStr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func envInt(key string, def int) int {
	if v, err := strconv.Atoi(os.Getenv(key)); err == nil {
		return v
	}
	return def
}

func envDur(key string, def time.Duration) time.Duration {
	if v, err := time.ParseDuration(os.Getenv(key)); err == nil {
		return v
	}
	return def
}
