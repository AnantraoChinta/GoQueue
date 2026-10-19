// Command scheduler is the GoQueue HTTP API entry point.
//
// It accepts job submissions from clients over HTTP and pushes them onto
// the queue for workers to claim. It never executes jobs itself.
//
// Pass -redis-addr to share a queue with the workers. Without it the queue
// is in-process only, which means submitted jobs are invisible to every
// worker - useful only for exercising the HTTP surface.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/anantraochinta/goqueue/internal/api"
	"github.com/anantraochinta/goqueue/internal/metrics"
	"github.com/anantraochinta/goqueue/internal/queue"
)

type config struct {
	addr           string
	redisAddr      string
	redisPrefix    string
	connectTimeout time.Duration
	shutdownGrace  time.Duration
	readyTimeout   time.Duration
	depthInterval  time.Duration
	logLevel       string
}

func main() {
	if err := run(); err != nil {
		slog.Error("scheduler exited with error", "error", err)
		os.Exit(1)
	}
}

func run() error {
	cfg := parseConfig()
	logger := newLogger(cfg.logLevel)
	slog.SetDefault(logger)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	q, ready, closeQueue, err := buildQueue(ctx, cfg, logger)
	if err != nil {
		return err
	}
	defer closeQueue()

	m := metrics.New()

	// The scheduler samples queue depth as well as the workers.
	//
	// Found the hard way: the sampler originally lived only in the worker,
	// so when every worker died the queue-depth series went stale and
	// GoQueueBacklogGrowing could not fire - precisely the outage where a
	// growing backlog matters most. Redis showed 80 pending jobs while
	// Prometheus showed no data at all.
	//
	// Schedulers stay up independently of workers, so reporting from here
	// keeps the backlog visible through a total worker outage. Both
	// publish the same queue-wide value, which is why every query must
	// aggregate with max() rather than sum().
	go sampleQueueDepth(ctx, q, m, cfg.depthInterval, logger)

	srv := &http.Server{
		Addr:    cfg.addr,
		Handler: newHandler(q, m, ready, cfg, logger),
		// Go's defaults are no timeout at all, which lets one slow client
		// hold a connection open indefinitely.
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       15 * time.Second,
		WriteTimeout:      30 * time.Second,
		IdleTimeout:       60 * time.Second,
	}

	serveErr := make(chan error, 1)
	go func() {
		logger.Info("scheduler listening", "addr", cfg.addr)
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			serveErr <- err
			return
		}
		serveErr <- nil
	}()

	select {
	case err := <-serveErr:
		return err
	case <-ctx.Done():
		logger.Info("shutdown signal received; draining connections")
	}

	// Graceful: stop accepting new connections, let in-flight requests
	// finish. Kubernetes keeps routing briefly after SIGTERM, so cutting
	// connections immediately would return 502s to live traffic.
	shutdownCtx, cancel := context.WithTimeout(context.Background(), cfg.shutdownGrace)
	defer cancel()
	if err := srv.Shutdown(shutdownCtx); err != nil {
		return fmt.Errorf("graceful shutdown: %w", err)
	}
	logger.Info("scheduler stopped")
	return nil
}

// sampleQueueDepth polls the queue and publishes the depth gauge.
func sampleQueueDepth(ctx context.Context, q queue.Queue, m *metrics.Metrics,
	every time.Duration, logger *slog.Logger) {

	// Optional capability, asserted rather than required on the Queue
	// interface - the same pattern net/http uses for http.Flusher.
	type depther interface {
		Depth(context.Context) (map[queue.State]int, error)
	}
	d, ok := q.(depther)
	if !ok {
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
			// Every state is set explicitly, including zeros: omitting one
			// would leave its last non-zero value published forever.
			for _, state := range []queue.State{
				queue.StatePending, queue.StateClaimed, queue.StateSucceeded,
				queue.StateFailed, queue.StateDead,
			} {
				m.QueueDepth.WithLabelValues(string(state)).Set(float64(counts[state]))
			}
		}
	}
}

func newHandler(q queue.Queue, m *metrics.Metrics, ready func(*http.Request) error,
	cfg config, logger *slog.Logger) http.Handler {

	apiSrv := &api.Server{Queue: q, Logger: logger, Ready: ready}

	mux := http.NewServeMux()
	// The API owns everything except /metrics, which belongs to the
	// process rather than to the job resource.
	mux.Handle("/", apiSrv.Routes())
	mux.Handle("GET /metrics", m.Handler())
	return mux
}

func buildQueue(ctx context.Context, cfg config, logger *slog.Logger) (
	queue.Queue, func(*http.Request) error, func(), error) {

	if cfg.redisAddr == "" {
		logger.Warn("no -redis-addr: using an in-process queue; no worker will see these jobs")
		// Nothing external to check, so readiness is simply "up".
		return queue.NewMemoryQueue(), nil, func() {}, nil
	}

	rdb := redis.NewClient(&redis.Options{Addr: cfg.redisAddr})
	q, err := queue.NewWithRetry(ctx, rdb, cfg.redisPrefix, cfg.connectTimeout)
	if err != nil {
		_ = rdb.Close()
		return nil, nil, nil, fmt.Errorf("connecting to redis at %s: %w", cfg.redisAddr, err)
	}
	logger.Info("connected to redis", "addr", cfg.redisAddr, "prefix", cfg.redisPrefix)

	// Readiness pings Redis: a scheduler that cannot enqueue should be
	// taken out of the Service rather than accepting jobs it will drop.
	// Note this is wired to /readyz only, never /healthz.
	ready := func(r *http.Request) error {
		pingCtx, cancel := context.WithTimeout(r.Context(), cfg.readyTimeout)
		defer cancel()
		return rdb.Ping(pingCtx).Err()
	}
	return q, ready, func() { _ = rdb.Close() }, nil
}

func parseConfig() config {
	var c config
	flag.StringVar(&c.addr, "addr", envStr("GOQUEUE_ADDR", ":8080"),
		"address to serve the HTTP API on")
	flag.DurationVar(&c.connectTimeout, "redis-connect-timeout", envDur("GOQUEUE_REDIS_CONNECT_TIMEOUT", 60*time.Second),
		"how long to keep retrying the initial Redis connection before giving up")
	flag.StringVar(&c.redisAddr, "redis-addr", envStr("GOQUEUE_REDIS_ADDR", ""),
		"Redis address, e.g. localhost:6379; empty uses an in-process queue")
	flag.StringVar(&c.redisPrefix, "redis-prefix", envStr("GOQUEUE_REDIS_PREFIX", "goqueue"),
		"key prefix, so several environments can share one Redis")
	flag.DurationVar(&c.shutdownGrace, "shutdown-grace", envDur("GOQUEUE_SHUTDOWN_GRACE", 15*time.Second),
		"time allowed for in-flight requests to finish on shutdown")
	flag.DurationVar(&c.depthInterval, "depth-interval", envDur("GOQUEUE_DEPTH_INTERVAL", 5*time.Second),
		"how often to sample queue depth")
	flag.DurationVar(&c.readyTimeout, "ready-timeout", envDur("GOQUEUE_READY_TIMEOUT", 2*time.Second),
		"timeout for the readiness check's Redis ping")
	flag.StringVar(&c.logLevel, "log-level", envStr("GOQUEUE_LOG_LEVEL", "info"),
		"debug, info, warn or error")
	flag.Parse()
	return c
}

func newLogger(level string) *slog.Logger {
	var l slog.Level
	if err := l.UnmarshalText([]byte(level)); err != nil {
		l = slog.LevelInfo
	}
	return slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: l}))
}

func envStr(key, def string) string {
	if v := os.Getenv(key); v != "" {
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
