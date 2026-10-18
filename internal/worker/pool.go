// Package worker is the goroutine worker pool that executes jobs.
//
// The pool is the concurrency boundary of GoQueue: it decides how many jobs
// a single process runs at once, how long a job may take, and what happens
// when one fails.
package worker

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"maps"
	"math"
	"math/rand/v2"
	"runtime/debug"
	"sync"
	"time"

	"github.com/anantraochinta/goqueue/internal/queue"
)

// Defaults applied by Run when a field is left zero.
const (
	// DefaultSize is deliberately larger than GOMAXPROCS. Jobs are assumed
	// to be I/O-bound - HTTP calls, database writes - where goroutines
	// spend most of their life blocked and core count is the wrong bound.
	// Override it for CPU-bound work.
	DefaultSize = 10

	DefaultJobTimeout = 30 * time.Second
	DefaultClaimBlock = time.Second
	DefaultAckTimeout = 5 * time.Second
)

// Outcome labels reported to an Observer. A closed set: these become
// Prometheus label values, and an unbounded set of them would blow up
// cardinality.
const (
	OutcomeSuccess   = "success"
	OutcomeFailure   = "failure"
	OutcomeTimeout   = "timeout"
	OutcomePanic     = "panic"
	OutcomeNoHandler = "no_handler"
)

var (
	// ErrNoHandler means a job arrived whose Type nothing is registered for.
	// Treated as a job failure, not a pool failure: one bad job type must
	// not stop the worker.
	ErrNoHandler = errors.New("no handler registered for job type")

	// ErrPanic wraps a value recovered from a panicking handler.
	ErrPanic = errors.New("handler panicked")
)

// Observer receives job lifecycle events. internal/metrics implements it;
// the interface lives here so the pool has no dependency on Prometheus and
// stays testable without a registry.
//
// Implementations are called from every worker goroutine and must be safe
// for concurrent use. They must not block - a slow Observer slows the pool.
type Observer interface {
	JobStarted(jobType string)
	JobFinished(jobType, outcome string, d time.Duration)
}

// nopObserver is the default, so the pool never nil-checks.
type nopObserver struct{}

func (nopObserver) JobStarted(string)                         {}
func (nopObserver) JobFinished(string, string, time.Duration) {}

// Classify maps a job error to one of the Outcome constants.
func Classify(err error) string {
	switch {
	case err == nil:
		return OutcomeSuccess
	case errors.Is(err, ErrPanic):
		return OutcomePanic
	case errors.Is(err, ErrNoHandler):
		return OutcomeNoHandler
	case errors.Is(err, context.DeadlineExceeded):
		return OutcomeTimeout
	default:
		return OutcomeFailure
	}
}

// Handler executes one job type. Implementations must respect ctx
// cancellation, otherwise a hung job will occupy a pool slot until its
// timeout expires.
type Handler interface {
	Handle(ctx context.Context, job *queue.Job) error
}

// HandlerFunc adapts a plain function to Handler, so callers and tests can
// pass a closure instead of declaring a type.
type HandlerFunc func(ctx context.Context, job *queue.Job) error

// Handle implements Handler.
func (f HandlerFunc) Handle(ctx context.Context, job *queue.Job) error {
	return f(ctx, job)
}

// Pool runs a fixed number of goroutines that claim and execute jobs.
//
// Configure it, then call Run. Fields must not be modified after Run starts.
type Pool struct {
	// Queue is where jobs are claimed from. Required.
	Queue queue.Queue

	// Handlers maps a job Type to the code that runs it. Required, and must
	// be non-empty: a pool with no handlers would drain the queue into
	// failures.
	Handlers map[string]Handler

	// Size is the number of concurrent worker goroutines. It bounds
	// concurrency; it does not guarantee parallelism.
	Size int

	// JobTimeout bounds how long a single job may run.
	JobTimeout time.Duration

	// ClaimBlock is how long a worker waits for a job before looping. It
	// also bounds shutdown latency, because a worker only notices
	// cancellation between claims.
	ClaimBlock time.Duration

	// AckTimeout bounds the Ack/Nack that records a job's outcome.
	AckTimeout time.Duration

	// Backoff maps a job's attempt count to how long before it may be
	// retried. Defaults to ExponentialBackoff(100ms, 30s).
	Backoff func(attempts int) time.Duration

	// WorkerID prefixes the per-goroutine identifiers recorded on claims.
	// In Kubernetes this should be the pod name.
	WorkerID string

	// Logger defaults to slog.Default().
	Logger *slog.Logger

	// Observer receives job lifecycle events. Defaults to a no-op.
	Observer Observer
}

// Run starts Size goroutines and blocks until ctx is cancelled and every
// in-flight job has finished.
//
// It returns an error only for a misconfigured pool. A cancelled context is
// a normal shutdown and returns nil.
func (p *Pool) Run(ctx context.Context) error {
	cfg, err := p.resolve()
	if err != nil {
		return err
	}

	cfg.Logger.Info("worker pool starting",
		"size", cfg.Size,
		"job_timeout", cfg.JobTimeout,
		"types", handlerTypes(cfg.Handlers))

	var wg sync.WaitGroup
	for i := range cfg.Size {
		wg.Add(1)
		// Since Go 1.22 the loop variable is per-iteration, so capturing i
		// directly is correct. The `i := i` shadow older code needs is
		// obsolete.
		go func() {
			defer wg.Done()
			cfg.workerLoop(ctx, fmt.Sprintf("%s-%d", cfg.WorkerID, i))
		}()
	}

	// Pass 5, phase two: every goroutine has stopped claiming and finished
	// the job it held, so the pool has drained.
	wg.Wait()
	cfg.Logger.Info("worker pool stopped")
	return nil
}

// workerLoop is one pool slot: claim a job, run it, repeat until shutdown.
func (p *Pool) workerLoop(ctx context.Context, workerID string) {
	for {
		// Pass 5, phase one: stop claiming. Checking here rather than
		// mid-job is what makes shutdown graceful - a job already running
		// is allowed to finish.
		if ctx.Err() != nil {
			return
		}

		job, err := p.Queue.Claim(ctx, workerID, p.ClaimBlock)
		switch {
		case err == nil:
			p.runJob(ctx, workerID, job)

		case errors.Is(err, queue.ErrNoJobAvailable):
			// An idle queue is not an error.
			continue

		default:
			if ctx.Err() != nil {
				return
			}
			// A broken queue (Redis down) must not become a hot loop.
			p.Logger.Error("claim failed", "worker", workerID, "error", err)
			select {
			case <-ctx.Done():
				return
			case <-time.After(p.ClaimBlock):
			}
		}
	}
}

// runJob executes one claimed job, reports it to the Observer, and records
// its outcome in the queue.
func (p *Pool) runJob(ctx context.Context, workerID string, job *queue.Job) {
	start := time.Now()
	p.Observer.JobStarted(job.Type)

	err := p.dispatch(ctx, job)

	elapsed := time.Since(start)
	// Paired with JobStarted on every path, including the no-handler case,
	// so the in-flight gauge cannot drift upward over time.
	p.Observer.JobFinished(job.Type, Classify(err), elapsed)

	p.Logger.Debug("job finished",
		"worker", workerID,
		"job", job.ID,
		"type", job.Type,
		"attempt", job.Attempts,
		"duration", elapsed,
		"outcome", Classify(err),
		"error", err)

	p.complete(workerID, job, err)
}

// dispatch looks up the handler and runs it under a bounded context.
func (p *Pool) dispatch(ctx context.Context, job *queue.Job) error {
	handler, ok := p.Handlers[job.Type]
	if !ok {
		return fmt.Errorf("%w: %q", ErrNoHandler, job.Type)
	}

	// Pass 2 + pass 5. Two things are happening here.
	//
	// WithoutCancel detaches the job from the pool's lifecycle context, so
	// shutdown does NOT cancel work that is already running - that is what
	// makes the drain graceful rather than abrupt. Deriving jobCtx from ctx
	// directly would kill every in-flight job the instant SIGTERM arrived.
	//
	// WithTimeout then re-bounds it, so a detached job still cannot run
	// forever. JobTimeout is therefore the worst-case drain time, and the
	// Kubernetes terminationGracePeriodSeconds must exceed it.
	jobCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), p.JobTimeout)
	defer cancel()

	return invoke(jobCtx, handler, job)
}

// invoke calls the handler, converting a panic into an error.
//
// Pass 3: without this, one bad handler takes down the process and kills
// every other in-flight job in it. The stack is captured here because the
// recover site is the last place it still exists.
func invoke(ctx context.Context, h Handler, job *queue.Job) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("%w: %v\n%s", ErrPanic, r, debug.Stack())
		}
	}()
	return h.Handle(ctx, job)
}

// complete reports the outcome of a job back to the queue.
func (p *Pool) complete(workerID string, job *queue.Job, jobErr error) {
	// Deliberately NOT derived from the pool's context. During shutdown
	// that context is already cancelled, and acking through it would fail -
	// redelivering a job that actually succeeded. The outcome of finished
	// work must be recorded even as the process is going away.
	ctx, cancel := context.WithTimeout(context.Background(), p.AckTimeout)
	defer cancel()

	if jobErr == nil {
		if err := p.Queue.Ack(ctx, job.ID); err != nil {
			// ErrNotClaimed here means the claim expired and another worker
			// took over: the job ran twice. Expected under at-least-once
			// delivery, but a rising rate means ClaimTTL is too short.
			p.Logger.Warn("ack failed",
				"worker", workerID, "job", job.ID, "error", err)
		}
		return
	}

	// Pass 4. Note the pool does not decide dead-lettering: Nack compares
	// Attempts against MaxAttempts and moves the job to StateDead itself.
	// Duplicating that rule here would let the two drift apart.
	retryAfter := p.Backoff(job.Attempts)
	if err := p.Queue.Nack(ctx, job.ID, jobErr, retryAfter); err != nil {
		p.Logger.Warn("nack failed",
			"worker", workerID, "job", job.ID, "error", err)
	}
}

// resolve validates the pool and returns a copy with defaults applied, so
// Run never mutates the caller's struct and never observes a Handlers map
// that changed underneath it.
func (p *Pool) resolve() (*Pool, error) {
	if p.Queue == nil {
		return nil, errors.New("worker: Pool.Queue is required")
	}
	if len(p.Handlers) == 0 {
		return nil, errors.New("worker: Pool.Handlers must not be empty")
	}

	c := *p
	c.Handlers = maps.Clone(p.Handlers)
	if c.Size <= 0 {
		c.Size = DefaultSize
	}
	if c.JobTimeout <= 0 {
		c.JobTimeout = DefaultJobTimeout
	}
	if c.ClaimBlock <= 0 {
		c.ClaimBlock = DefaultClaimBlock
	}
	if c.AckTimeout <= 0 {
		c.AckTimeout = DefaultAckTimeout
	}
	if c.Backoff == nil {
		c.Backoff = ExponentialBackoff(100*time.Millisecond, 30*time.Second)
	}
	if c.WorkerID == "" {
		c.WorkerID = "worker"
	}
	if c.Logger == nil {
		c.Logger = slog.Default()
	}
	if c.Observer == nil {
		c.Observer = nopObserver{}
	}
	return &c, nil
}

// ExponentialBackoff doubles the delay each attempt up to limit, then picks
// a random duration in (0, delay] - "full jitter".
//
// The jitter is the point. Without it, a hundred jobs that fail together
// because a downstream service blipped all retry at the same instant, and
// keep colliding on every subsequent attempt. Spreading them out is what
// lets the downstream recover.
func ExponentialBackoff(base, limit time.Duration) func(attempts int) time.Duration {
	return func(attempts int) time.Duration {
		if attempts < 1 {
			attempts = 1
		}
		if attempts > 30 {
			attempts = 30 // guard against shifting into overflow
		}
		d := time.Duration(float64(base) * math.Pow(2, float64(attempts-1)))
		if d <= 0 || d > limit {
			d = limit
		}
		return time.Duration(rand.Int64N(int64(d)) + 1)
	}
}

// handlerTypes returns the registered job types, for the startup log line.
func handlerTypes(h map[string]Handler) []string {
	out := make([]string, 0, len(h))
	for k := range h {
		out = append(out, k)
	}
	return out
}
