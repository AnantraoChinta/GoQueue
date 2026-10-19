package worker_test

import (
	"context"
	"fmt"
	"os"
	"sync/atomic"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/anantraochinta/goqueue/internal/queue"
	"github.com/anantraochinta/goqueue/internal/worker"
)

// Throughput benchmarks for the worker pool.
//
// Run against the in-memory queue to measure the pool itself, and against
// Redis to measure the realistic end-to-end path (two Lua round trips per
// job: claim and ack).
//
//	go test -bench=. -benchtime=10s ./internal/worker/
//	GOQUEUE_TEST_REDIS=localhost:6379 go test -bench=Redis -benchtime=10s ./internal/worker/
//
// The reported ns/op is per job. jobs/sec is reported as a custom metric so
// you do not have to do the arithmetic.

func benchQueue(b *testing.B, kind string) queue.Queue {
	b.Helper()
	if kind == "memory" {
		return queue.NewMemoryQueue()
	}

	addr := os.Getenv("GOQUEUE_TEST_REDIS")
	if addr == "" {
		addr = "localhost:6379"
	}
	rdb := redis.NewClient(&redis.Options{
		Addr: addr, MaxRetries: -1, DialTimeout: 500 * time.Millisecond,
		// The pool must not be narrower than the worker pool, or
		// goroutines queue on connections and you benchmark the
		// connection pool instead of the queue.
		PoolSize: 128,
	})
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := rdb.Ping(ctx).Err(); err != nil {
		_ = rdb.Close()
		b.Skipf("no Redis at %s (run `make compose-up`): %v", addr, err)
	}

	prefix := fmt.Sprintf("goqbench:%d", time.Now().UnixNano())
	q, err := queue.New(context.Background(), rdb, prefix,
		queue.WithPollInterval(time.Millisecond))
	if err != nil {
		b.Fatalf("construct RedisQueue: %v", err)
	}
	b.Cleanup(func() {
		keys, _ := rdb.Keys(context.Background(), prefix+"*").Result()
		if len(keys) > 0 {
			_ = rdb.Del(context.Background(), keys...).Err()
		}
		_ = rdb.Close()
	})
	return q
}

// runThroughput enqueues b.N jobs, drains them, and reports jobs/sec.
func runThroughput(b *testing.B, q queue.Queue, poolSize int) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	for i := range b.N {
		job := &queue.Job{
			ID: fmt.Sprintf("b-%d", i), Type: "noop", MaxAttempts: 1,
		}
		if err := q.Enqueue(ctx, job); err != nil {
			b.Fatalf("enqueue: %v", err)
		}
	}

	var done atomic.Int64
	finished := make(chan struct{})
	var once bool

	p := &worker.Pool{
		Queue:      q,
		Size:       poolSize,
		ClaimBlock: 50 * time.Millisecond,
		JobTimeout: 30 * time.Second,
		Logger:     quietLogger(),
		Handlers: map[string]worker.Handler{
			// A no-op handler: this measures the queue and pool, not the
			// work. Real throughput is bounded by whatever the handler does.
			"noop": worker.HandlerFunc(func(context.Context, *queue.Job) error {
				if done.Add(1) == int64(b.N) && !once {
					once = true
					close(finished)
				}
				return nil
			}),
		},
	}

	runDone := make(chan struct{})
	b.ResetTimer()
	start := time.Now()

	go func() { _ = p.Run(ctx); close(runDone) }()

	select {
	case <-finished:
	case <-time.After(5 * time.Minute):
		b.Fatalf("timed out: only %d/%d jobs ran", done.Load(), b.N)
	}
	elapsed := time.Since(start)
	b.StopTimer()

	cancel()
	<-runDone

	b.ReportMetric(float64(b.N)/elapsed.Seconds(), "jobs/sec")
	b.ReportMetric(float64(poolSize), "goroutines")
}

func BenchmarkPoolMemory(b *testing.B) {
	for _, size := range []int{1, 10, 50} {
		b.Run(fmt.Sprintf("pool-%d", size), func(b *testing.B) {
			runThroughput(b, benchQueue(b, "memory"), size)
		})
	}
}

func BenchmarkPoolRedis(b *testing.B) {
	for _, size := range []int{1, 10, 50} {
		b.Run(fmt.Sprintf("pool-%d", size), func(b *testing.B) {
			runThroughput(b, benchQueue(b, "redis"), size)
		})
	}
}

// BenchmarkRedisClaimAck isolates the queue operations from the pool, so
// you can tell whether a throughput ceiling is Redis or scheduling.
func BenchmarkRedisClaimAck(b *testing.B) {
	q := benchQueue(b, "redis")
	ctx := context.Background()

	for i := range b.N {
		if err := q.Enqueue(ctx, &queue.Job{
			ID: fmt.Sprintf("ca-%d", i), Type: "noop", MaxAttempts: 1,
		}); err != nil {
			b.Fatalf("enqueue: %v", err)
		}
	}

	b.ResetTimer()
	start := time.Now()
	for range b.N {
		job, err := q.Claim(ctx, "bench", time.Second)
		if err != nil {
			b.Fatalf("claim: %v", err)
		}
		if err := q.Ack(ctx, job.ID); err != nil {
			b.Fatalf("ack: %v", err)
		}
	}
	elapsed := time.Since(start)
	b.StopTimer()
	b.ReportMetric(float64(b.N)/elapsed.Seconds(), "jobs/sec")
}
