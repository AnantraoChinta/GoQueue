package worker_test

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/anantraochinta/goqueue/internal/queue"
	"github.com/anantraochinta/goqueue/internal/worker"
)

func quietLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

// start runs the pool in the background and returns a stop func that
// cancels it and waits for a clean return.
func start(t *testing.T, p *worker.Pool) (context.Context, func()) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	errCh := make(chan error, 1)
	go func() { errCh <- p.Run(ctx) }()

	return ctx, func() {
		cancel()
		select {
		case err := <-errCh:
			if err != nil {
				t.Errorf("Run returned error: %v", err)
			}
		case <-time.After(10 * time.Second):
			t.Error("Run did not return within 10s of cancellation")
		}
	}
}

func waitFor(t *testing.T, c <-chan struct{}, d time.Duration, what string) {
	t.Helper()
	select {
	case <-c:
	case <-time.After(d):
		t.Fatalf("timed out waiting for %s", what)
	}
}

// Every job is executed exactly once, with many goroutines competing.
func TestPoolProcessesEveryJobExactlyOnce(t *testing.T) {
	const n = 100
	q := queue.NewMemoryQueue()
	for i := range n {
		job := &queue.Job{ID: fmt.Sprintf("job-%d", i), Type: "noop", MaxAttempts: 1}
		if err := q.Enqueue(context.Background(), job); err != nil {
			t.Fatalf("enqueue: %v", err)
		}
	}

	var mu sync.Mutex
	seen := make(map[string]int)
	var handled atomic.Int64
	done := make(chan struct{})

	p := &worker.Pool{
		Queue:      q,
		Size:       8,
		ClaimBlock: 20 * time.Millisecond,
		Logger:     quietLogger(),
		Handlers: map[string]worker.Handler{
			"noop": worker.HandlerFunc(func(_ context.Context, j *queue.Job) error {
				mu.Lock()
				seen[j.ID]++
				mu.Unlock()
				if handled.Add(1) == n {
					close(done)
				}
				return nil
			}),
		},
	}

	_, stop := start(t, p)
	waitFor(t, done, 10*time.Second, "all jobs to be handled")
	stop()

	mu.Lock()
	defer mu.Unlock()
	if len(seen) != n {
		t.Fatalf("handled %d distinct jobs, want %d", len(seen), n)
	}
	for id, count := range seen {
		if count != 1 {
			t.Fatalf("job %s handled %d times, want exactly 1", id, count)
		}
	}
	for i := range n {
		id := fmt.Sprintf("job-%d", i)
		state, err := q.Status(context.Background(), id)
		if err != nil {
			t.Fatalf("status %s: %v", id, err)
		}
		if state != queue.StateSucceeded {
			t.Fatalf("job %s in state %s, want succeeded", id, state)
		}
	}
}

// A handler that ignores cancellation must not hold its slot past
// JobTimeout, and the freed slot must go on to serve other jobs.
func TestPoolJobTimeoutFreesTheSlot(t *testing.T) {
	q := queue.NewMemoryQueue()
	mustEnqueue(t, q, &queue.Job{ID: "slow", Type: "slow", MaxAttempts: 1})
	mustEnqueue(t, q, &queue.Job{ID: "fast", Type: "fast", MaxAttempts: 1})

	slowStarted := make(chan struct{})
	fastDone := make(chan struct{})

	p := &worker.Pool{
		Queue:      q,
		Size:       1, // a single slot, so "fast" can only run once "slow" releases it
		JobTimeout: 100 * time.Millisecond,
		ClaimBlock: 20 * time.Millisecond,
		Backoff:    func(int) time.Duration { return 0 },
		Logger:     quietLogger(),
		Handlers: map[string]worker.Handler{
			"slow": worker.HandlerFunc(func(ctx context.Context, _ *queue.Job) error {
				close(slowStarted)
				<-ctx.Done() // respects cancellation, but only via the timeout
				return ctx.Err()
			}),
			"fast": worker.HandlerFunc(func(_ context.Context, _ *queue.Job) error {
				close(fastDone)
				return nil
			}),
		},
	}

	_, stop := start(t, p)
	waitFor(t, slowStarted, 5*time.Second, "slow job to start")
	waitFor(t, fastDone, 5*time.Second, "fast job to run after the slot was freed")
	stop()

	if state, _ := q.Status(context.Background(), "slow"); state != queue.StateDead {
		t.Fatalf("timed-out job in state %s, want dead", state)
	}
	if state, _ := q.Status(context.Background(), "fast"); state != queue.StateSucceeded {
		t.Fatalf("fast job in state %s, want succeeded", state)
	}
}

// A panicking handler must be contained: the process survives, the job is
// recorded as failed, and the pool keeps working.
func TestPoolSurvivesPanickingHandler(t *testing.T) {
	q := queue.NewMemoryQueue()
	mustEnqueue(t, q, &queue.Job{ID: "boom", Type: "boom", MaxAttempts: 1})
	mustEnqueue(t, q, &queue.Job{ID: "ok", Type: "ok", MaxAttempts: 1})

	okDone := make(chan struct{})
	p := &worker.Pool{
		Queue:      q,
		Size:       1,
		ClaimBlock: 20 * time.Millisecond,
		Backoff:    func(int) time.Duration { return 0 },
		Logger:     quietLogger(),
		Handlers: map[string]worker.Handler{
			"boom": worker.HandlerFunc(func(_ context.Context, _ *queue.Job) error {
				panic("handler exploded")
			}),
			"ok": worker.HandlerFunc(func(_ context.Context, _ *queue.Job) error {
				close(okDone)
				return nil
			}),
		},
	}

	_, stop := start(t, p)
	waitFor(t, okDone, 5*time.Second, "pool to keep working after a panic")
	stop()

	if state, _ := q.Status(context.Background(), "boom"); state != queue.StateDead {
		t.Fatalf("panicked job in state %s, want dead", state)
	}
	last := lastError(t, q, "boom")
	if !strings.Contains(last, "handler panicked") || !strings.Contains(last, "handler exploded") {
		t.Fatalf("LastError did not capture the panic: %q", last)
	}
}

// Failures retry up to MaxAttempts, then dead-letter rather than vanish.
func TestPoolRetriesThenDeadLetters(t *testing.T) {
	q := queue.NewMemoryQueue()
	mustEnqueue(t, q, &queue.Job{ID: "flaky", Type: "flaky", MaxAttempts: 3})

	var attempts atomic.Int64
	thirdDone := make(chan struct{})
	var backoffCalls []int
	var mu sync.Mutex

	p := &worker.Pool{
		Queue:      q,
		Size:       1,
		ClaimBlock: 10 * time.Millisecond,
		Logger:     quietLogger(),
		Backoff: func(n int) time.Duration {
			mu.Lock()
			backoffCalls = append(backoffCalls, n)
			mu.Unlock()
			return 0 // retry immediately, so the test is fast
		},
		Handlers: map[string]worker.Handler{
			"flaky": worker.HandlerFunc(func(_ context.Context, _ *queue.Job) error {
				if attempts.Add(1) == 3 {
					close(thirdDone)
				}
				return errors.New("always fails")
			}),
		},
	}

	_, stop := start(t, p)
	waitFor(t, thirdDone, 5*time.Second, "three attempts")
	// Give the final Nack time to land before asserting state.
	waitForState(t, q, "flaky", queue.StateDead, 5*time.Second)
	stop()

	if got := attempts.Load(); got != 3 {
		t.Fatalf("handler ran %d times, want exactly 3 (MaxAttempts)", got)
	}
	mu.Lock()
	defer mu.Unlock()
	if len(backoffCalls) != 3 || backoffCalls[0] != 1 || backoffCalls[2] != 3 {
		t.Fatalf("backoff called with %v, want attempt numbers 1,2,3", backoffCalls)
	}
}

// Shutdown is graceful: a job already running is allowed to finish.
func TestPoolDrainsInFlightJobOnShutdown(t *testing.T) {
	q := queue.NewMemoryQueue()
	mustEnqueue(t, q, &queue.Job{ID: "long", Type: "long", MaxAttempts: 1})

	started := make(chan struct{})
	var finished atomic.Bool

	p := &worker.Pool{
		Queue:      q,
		Size:       1,
		JobTimeout: 5 * time.Second,
		ClaimBlock: 10 * time.Millisecond,
		Logger:     quietLogger(),
		Handlers: map[string]worker.Handler{
			"long": worker.HandlerFunc(func(ctx context.Context, _ *queue.Job) error {
				close(started)
				select {
				case <-time.After(250 * time.Millisecond):
					finished.Store(true)
					return nil
				case <-ctx.Done():
					// Must not happen: shutdown must not cancel a running job.
					return ctx.Err()
				}
			}),
		},
	}

	ctx, cancel := context.WithCancel(context.Background())
	errCh := make(chan error, 1)
	go func() { errCh <- p.Run(ctx) }()

	waitFor(t, started, 5*time.Second, "job to start")
	cancel() // shut down while the job is mid-flight

	select {
	case err := <-errCh:
		if err != nil {
			t.Fatalf("Run returned error: %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("Run did not return")
	}

	if !finished.Load() {
		t.Fatal("in-flight job was cancelled by shutdown instead of being allowed to finish")
	}
	if state, _ := q.Status(context.Background(), "long"); state != queue.StateSucceeded {
		t.Fatalf("drained job in state %s, want succeeded", state)
	}
}

// An unregistered job type fails that job without stopping the worker.
func TestPoolUnknownJobTypeFailsOnlyThatJob(t *testing.T) {
	q := queue.NewMemoryQueue()
	mustEnqueue(t, q, &queue.Job{ID: "orphan", Type: "nobody-handles-this", MaxAttempts: 1})
	mustEnqueue(t, q, &queue.Job{ID: "ok", Type: "ok", MaxAttempts: 1})

	okDone := make(chan struct{})
	p := &worker.Pool{
		Queue:      q,
		Size:       1,
		ClaimBlock: 10 * time.Millisecond,
		Backoff:    func(int) time.Duration { return 0 },
		Logger:     quietLogger(),
		Handlers: map[string]worker.Handler{
			"ok": worker.HandlerFunc(func(_ context.Context, _ *queue.Job) error {
				close(okDone)
				return nil
			}),
		},
	}

	_, stop := start(t, p)
	waitFor(t, okDone, 5*time.Second, "pool to continue past the unhandled job")
	stop()

	if state, _ := q.Status(context.Background(), "orphan"); state != queue.StateDead {
		t.Fatalf("unhandled job in state %s, want dead", state)
	}
	if last := lastError(t, q, "orphan"); !strings.Contains(last, "no handler registered") {
		t.Fatalf("LastError = %q, want it to mention the missing handler", last)
	}
}

func TestPoolRejectsInvalidConfig(t *testing.T) {
	t.Run("no queue", func(t *testing.T) {
		p := &worker.Pool{Handlers: map[string]worker.Handler{
			"x": worker.HandlerFunc(func(context.Context, *queue.Job) error { return nil }),
		}}
		if err := p.Run(context.Background()); err == nil {
			t.Fatal("expected an error when Queue is nil")
		}
	})
	t.Run("no handlers", func(t *testing.T) {
		p := &worker.Pool{Queue: queue.NewMemoryQueue()}
		if err := p.Run(context.Background()); err == nil {
			t.Fatal("expected an error when Handlers is empty")
		}
	})
}

func TestExponentialBackoffStaysWithinBounds(t *testing.T) {
	const limit = time.Second
	b := worker.ExponentialBackoff(10*time.Millisecond, limit)
	for attempt := range 40 {
		d := b(attempt)
		if d <= 0 {
			t.Fatalf("attempt %d produced non-positive delay %v", attempt, d)
		}
		if d > limit {
			t.Fatalf("attempt %d produced %v, above the %v limit", attempt, d, limit)
		}
	}
	// Full jitter means the first attempt can never exceed the base delay.
	for range 100 {
		if d := b(1); d > 10*time.Millisecond {
			t.Fatalf("first attempt produced %v, above the 10ms base", d)
		}
	}
}

func mustEnqueue(t *testing.T, q queue.Queue, j *queue.Job) {
	t.Helper()
	if err := q.Enqueue(context.Background(), j); err != nil {
		t.Fatalf("enqueue %s: %v", j.ID, err)
	}
}

func waitForState(t *testing.T, q queue.Queue, id string, want queue.State, d time.Duration) {
	t.Helper()
	deadline := time.Now().Add(d)
	for time.Now().Before(deadline) {
		if state, err := q.Status(context.Background(), id); err == nil && state == want {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	state, _ := q.Status(context.Background(), id)
	t.Fatalf("job %s never reached %s (stuck in %s)", id, want, state)
}

func lastError(t *testing.T, q *queue.MemoryQueue, id string) string {
	t.Helper()
	job, err := q.Get(context.Background(), id)
	if err != nil {
		t.Fatalf("get %s: %v", id, err)
	}
	return job.LastError
}
