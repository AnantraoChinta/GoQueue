package queue_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/anantraochinta/goqueue/internal/queue"
)

// Every test below runs against each implementation. The point is that the
// in-memory fake used to develop the worker pool and the Redis queue used
// in production cannot disagree: a behavioural difference is a failure
// here, not a surprise in staging.
type factory struct {
	name string
	make func(t *testing.T) queue.Queue
}

func implementations() []factory {
	return []factory{
		{"memory", func(*testing.T) queue.Queue { return queue.NewMemoryQueue() }},
		{"redis", newTestRedisQueue},
	}
}

// newTestRedisQueue connects to a real Redis, or skips. Integration tests
// that silently pass without the dependency are worse than no tests.
func newTestRedisQueue(t *testing.T) queue.Queue {
	t.Helper()

	addr := os.Getenv("GOQUEUE_TEST_REDIS")
	if addr == "" {
		addr = "localhost:6379"
	}
	rdb := redis.NewClient(&redis.Options{
		Addr: addr,
		// No retries and a short dial timeout: when Redis is absent we want
		// a fast skip, not five seconds of reconnect logging per test.
		MaxRetries:  -1,
		DialTimeout: 500 * time.Millisecond,
	})

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := rdb.Ping(ctx).Err(); err != nil {
		_ = rdb.Close()
		t.Skipf("no Redis at %s (run `make compose-up`): %v", addr, err)
	}

	// A unique prefix per test, so tests cannot see each other's jobs and
	// can run in parallel against one Redis.
	prefix := fmt.Sprintf("goqtest:%s:%d", t.Name(), time.Now().UnixNano())
	q, err := queue.New(context.Background(), rdb, prefix,
		queue.WithPollInterval(10*time.Millisecond),
		queue.WithClaimTTL(2*time.Second),
	)
	if err != nil {
		t.Fatalf("construct RedisQueue: %v", err)
	}

	t.Cleanup(func() {
		keys, _ := rdb.Keys(context.Background(), prefix+"*").Result()
		if len(keys) > 0 {
			_ = rdb.Del(context.Background(), keys...).Err()
		}
		_ = rdb.Close()
	})
	return q
}

func forEachImpl(t *testing.T, fn func(t *testing.T, q queue.Queue)) {
	t.Helper()
	for _, impl := range implementations() {
		t.Run(impl.name, func(t *testing.T) { fn(t, impl.make(t)) })
	}
}

func TestContractLifecycle(t *testing.T) {
	forEachImpl(t, func(t *testing.T, q queue.Queue) {
		ctx := context.Background()
		job := &queue.Job{ID: "j1", Type: "email", MaxAttempts: 3}
		if err := q.Enqueue(ctx, job); err != nil {
			t.Fatalf("enqueue: %v", err)
		}

		if s, err := q.Status(ctx, "j1"); err != nil || s != queue.StatePending {
			t.Fatalf("status = %s, %v; want pending", s, err)
		}

		claimed, err := q.Claim(ctx, "w1", time.Second)
		if err != nil {
			t.Fatalf("claim: %v", err)
		}
		if claimed.State != queue.StateClaimed {
			t.Fatalf("claimed.State = %s, want claimed", claimed.State)
		}
		if claimed.Attempts != 1 {
			t.Fatalf("claimed.Attempts = %d, want 1", claimed.Attempts)
		}
		if claimed.ClaimedBy != "w1" {
			t.Fatalf("claimed.ClaimedBy = %q, want w1", claimed.ClaimedBy)
		}
		if claimed.MaxAttempts != 3 {
			t.Fatalf("claimed.MaxAttempts = %d, want 3", claimed.MaxAttempts)
		}

		if err := q.Ack(ctx, "j1"); err != nil {
			t.Fatalf("ack: %v", err)
		}
		if s, _ := q.Status(ctx, "j1"); s != queue.StateSucceeded {
			t.Fatalf("status after ack = %s, want succeeded", s)
		}
	})
}

// The payload is opaque and must survive the round trip byte for byte.
// A Lua implementation that decoded the job with cjson would fail this:
// the big integer loses precision as a double and key order changes.
func TestContractPayloadRoundTripsExactly(t *testing.T) {
	forEachImpl(t, func(t *testing.T, q queue.Queue) {
		ctx := context.Background()
		payload := json.RawMessage(`{"n":9007199254740993,"z":1,"a":2,"s":"hi"}`)

		if err := q.Enqueue(ctx, &queue.Job{
			ID: "p1", Type: "t", MaxAttempts: 1, Payload: payload,
		}); err != nil {
			t.Fatalf("enqueue: %v", err)
		}

		claimed, err := q.Claim(ctx, "w1", time.Second)
		if err != nil {
			t.Fatalf("claim: %v", err)
		}
		if string(claimed.Payload) != string(payload) {
			t.Fatalf("payload corrupted:\n got %s\nwant %s", claimed.Payload, payload)
		}
	})
}

func TestContractNackRetriesThenDeadLetters(t *testing.T) {
	forEachImpl(t, func(t *testing.T, q queue.Queue) {
		ctx := context.Background()
		if err := q.Enqueue(ctx, &queue.Job{ID: "r1", Type: "t", MaxAttempts: 2}); err != nil {
			t.Fatalf("enqueue: %v", err)
		}

		// Attempt 1 -> retryable.
		c1, err := q.Claim(ctx, "w1", time.Second)
		if err != nil {
			t.Fatalf("claim 1: %v", err)
		}
		if err := q.Nack(ctx, c1.ID, errors.New("boom"), 0); err != nil {
			t.Fatalf("nack 1: %v", err)
		}
		if s, _ := q.Status(ctx, "r1"); s != queue.StatePending {
			t.Fatalf("after retryable nack state = %s, want pending", s)
		}

		// Attempt 2 -> exhausted.
		c2, err := q.Claim(ctx, "w2", time.Second)
		if err != nil {
			t.Fatalf("claim 2: %v", err)
		}
		if c2.Attempts != 2 {
			t.Fatalf("second claim Attempts = %d, want 2", c2.Attempts)
		}
		if err := q.Nack(ctx, c2.ID, errors.New("final"), 0); err != nil {
			t.Fatalf("nack 2: %v", err)
		}
		if s, _ := q.Status(ctx, "r1"); s != queue.StateDead {
			t.Fatalf("after exhausting attempts state = %s, want dead", s)
		}

		// And it must not come back.
		if _, err := q.Claim(ctx, "w3", 50*time.Millisecond); !errors.Is(err, queue.ErrNoJobAvailable) {
			t.Fatalf("dead job was reclaimable: err = %v", err)
		}
	})
}

// retryAfter must actually hold the job back, not just be recorded.
func TestContractNackHonoursRetryAfter(t *testing.T) {
	forEachImpl(t, func(t *testing.T, q queue.Queue) {
		ctx := context.Background()
		if err := q.Enqueue(ctx, &queue.Job{ID: "b1", Type: "t", MaxAttempts: 5}); err != nil {
			t.Fatalf("enqueue: %v", err)
		}
		c, err := q.Claim(ctx, "w1", time.Second)
		if err != nil {
			t.Fatalf("claim: %v", err)
		}
		if err := q.Nack(ctx, c.ID, errors.New("retry me"), 400*time.Millisecond); err != nil {
			t.Fatalf("nack: %v", err)
		}

		// Too early.
		if _, err := q.Claim(ctx, "w2", 50*time.Millisecond); !errors.Is(err, queue.ErrNoJobAvailable) {
			t.Fatalf("job was claimable before its backoff elapsed: err = %v", err)
		}
		// Late enough.
		if _, err := q.Claim(ctx, "w2", 2*time.Second); err != nil {
			t.Fatalf("job never became claimable after backoff: %v", err)
		}
	})
}

func TestContractErrors(t *testing.T) {
	forEachImpl(t, func(t *testing.T, q queue.Queue) {
		ctx := context.Background()

		t.Run("claim on empty queue", func(t *testing.T) {
			if _, err := q.Claim(ctx, "w", 50*time.Millisecond); !errors.Is(err, queue.ErrNoJobAvailable) {
				t.Fatalf("err = %v, want ErrNoJobAvailable", err)
			}
		})
		t.Run("status of unknown job", func(t *testing.T) {
			if _, err := q.Status(ctx, "nope"); !errors.Is(err, queue.ErrJobNotFound) {
				t.Fatalf("err = %v, want ErrJobNotFound", err)
			}
		})
		t.Run("ack unknown job", func(t *testing.T) {
			if err := q.Ack(ctx, "nope"); !errors.Is(err, queue.ErrJobNotFound) {
				t.Fatalf("err = %v, want ErrJobNotFound", err)
			}
		})
		t.Run("ack job that is not claimed", func(t *testing.T) {
			if err := q.Enqueue(ctx, &queue.Job{ID: "unclaimed", Type: "t"}); err != nil {
				t.Fatalf("enqueue: %v", err)
			}
			if err := q.Ack(ctx, "unclaimed"); !errors.Is(err, queue.ErrNotClaimed) {
				t.Fatalf("err = %v, want ErrNotClaimed", err)
			}
		})
		t.Run("nack job that is not claimed", func(t *testing.T) {
			if err := q.Nack(ctx, "unclaimed", nil, 0); !errors.Is(err, queue.ErrNotClaimed) {
				t.Fatalf("err = %v, want ErrNotClaimed", err)
			}
		})
		t.Run("duplicate id", func(t *testing.T) {
			if err := q.Enqueue(ctx, &queue.Job{ID: "dup", Type: "t"}); err != nil {
				t.Fatalf("first enqueue: %v", err)
			}
			if err := q.Enqueue(ctx, &queue.Job{ID: "dup", Type: "t"}); err == nil {
				t.Fatal("duplicate ID was accepted")
			}
		})
		t.Run("missing type", func(t *testing.T) {
			if err := q.Enqueue(ctx, &queue.Job{ID: "notype"}); err == nil {
				t.Fatal("job without a Type was accepted")
			}
		})
	})
}

// Defaults must match across implementations, or the fake lies about
// production behaviour.
func TestContractEnqueueDefaults(t *testing.T) {
	forEachImpl(t, func(t *testing.T, q queue.Queue) {
		ctx := context.Background()
		job := &queue.Job{Type: "t"} // no ID, no MaxAttempts, no CreatedAt
		if err := q.Enqueue(ctx, job); err != nil {
			t.Fatalf("enqueue: %v", err)
		}
		if job.ID == "" {
			t.Fatal("Enqueue did not generate an ID")
		}
		claimed, err := q.Claim(ctx, "w1", time.Second)
		if err != nil {
			t.Fatalf("claim: %v", err)
		}
		if claimed.MaxAttempts != 1 {
			t.Fatalf("MaxAttempts defaulted to %d, want 1", claimed.MaxAttempts)
		}
		if claimed.CreatedAt.IsZero() {
			t.Fatal("CreatedAt was not defaulted")
		}
	})
}

// No job may be handed to two workers at once.
func TestContractConcurrentClaimsAreUnique(t *testing.T) {
	forEachImpl(t, func(t *testing.T, q queue.Queue) {
		ctx := context.Background()
		const jobs, workers = 40, 8
		for i := range jobs {
			if err := q.Enqueue(ctx, &queue.Job{
				ID: fmt.Sprintf("c-%d", i), Type: "t", MaxAttempts: 1,
			}); err != nil {
				t.Fatalf("enqueue: %v", err)
			}
		}

		var wg sync.WaitGroup
		got := make(chan string, jobs*2)
		for w := range workers {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for {
					job, err := q.Claim(ctx, fmt.Sprintf("w%d", w), 200*time.Millisecond)
					if err != nil {
						return // ErrNoJobAvailable: drained
					}
					got <- job.ID
					_ = q.Ack(ctx, job.ID)
				}
			}()
		}
		wg.Wait()
		close(got)

		seen := make(map[string]int)
		for id := range got {
			seen[id]++
			if seen[id] > 1 {
				t.Fatalf("job %s was claimed %d times", id, seen[id])
			}
		}
		if len(seen) != jobs {
			t.Fatalf("claimed %d distinct jobs, want %d", len(seen), jobs)
		}
	})
}
