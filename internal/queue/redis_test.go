package queue_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/anantraochinta/goqueue/internal/queue"
)

func redisQueue(t *testing.T) *queue.RedisQueue {
	t.Helper()
	q, ok := newTestRedisQueue(t).(*queue.RedisQueue)
	if !ok {
		t.Fatal("expected a *queue.RedisQueue")
	}
	return q
}

// The durability guarantee in one test: a worker claims a job and dies
// without acking. Nothing else can recover it until the reaper runs.
func TestRedisReclaimExpiredClaims(t *testing.T) {
	q := redisQueue(t)
	ctx := context.Background()

	if err := q.Enqueue(ctx, &queue.Job{ID: "orphan", Type: "t", MaxAttempts: 5}); err != nil {
		t.Fatalf("enqueue: %v", err)
	}
	claimed, err := q.Claim(ctx, "doomed-worker", time.Second)
	if err != nil {
		t.Fatalf("claim: %v", err)
	}
	if claimed.Attempts != 1 {
		t.Fatalf("Attempts = %d, want 1", claimed.Attempts)
	}

	// The worker is now "dead". While the claim is still valid nobody else
	// may take the job.
	if _, err := q.Claim(ctx, "other", 50*time.Millisecond); !errors.Is(err, queue.ErrNoJobAvailable) {
		t.Fatalf("job was stolen while its claim was still valid: %v", err)
	}
	if n, err := q.ReclaimExpired(ctx, 10); err != nil || n != 0 {
		t.Fatalf("ReclaimExpired = %d, %v; want 0, nil while the claim is live", n, err)
	}

	// newTestRedisQueue sets ClaimTTL to 2s.
	time.Sleep(2100 * time.Millisecond)

	n, err := q.ReclaimExpired(ctx, 10)
	if err != nil {
		t.Fatalf("ReclaimExpired: %v", err)
	}
	if n != 1 {
		t.Fatalf("ReclaimExpired = %d, want 1", n)
	}

	if s, _ := q.Status(ctx, "orphan"); s != queue.StatePending {
		t.Fatalf("reclaimed job state = %s, want pending", s)
	}
	again, err := q.Claim(ctx, "survivor", time.Second)
	if err != nil {
		t.Fatalf("reclaimed job was not claimable: %v", err)
	}
	if again.Attempts != 2 {
		t.Fatalf("Attempts after redelivery = %d, want 2", again.Attempts)
	}
	if again.ClaimedBy != "survivor" {
		t.Fatalf("ClaimedBy = %q, want survivor", again.ClaimedBy)
	}
}

// A late ack from a worker whose claim was reaped must be reported, not
// silently applied over the new owner's work.
func TestRedisLateAckAfterReclaimIsRejected(t *testing.T) {
	q := redisQueue(t)
	ctx := context.Background()

	if err := q.Enqueue(ctx, &queue.Job{ID: "slowpoke", Type: "t", MaxAttempts: 5}); err != nil {
		t.Fatalf("enqueue: %v", err)
	}
	if _, err := q.Claim(ctx, "slow-worker", time.Second); err != nil {
		t.Fatalf("claim: %v", err)
	}

	time.Sleep(2100 * time.Millisecond)
	if _, err := q.ReclaimExpired(ctx, 10); err != nil {
		t.Fatalf("reclaim: %v", err)
	}

	// The job is pending again, so the original worker's ack must fail.
	if err := q.Ack(ctx, "slowpoke"); !errors.Is(err, queue.ErrNotClaimed) {
		t.Fatalf("late ack returned %v, want ErrNotClaimed", err)
	}
}

func TestRedisDepth(t *testing.T) {
	q := redisQueue(t)
	ctx := context.Background()

	for _, id := range []string{"d1", "d2", "d3"} {
		if err := q.Enqueue(ctx, &queue.Job{ID: id, Type: "t", MaxAttempts: 1}); err != nil {
			t.Fatalf("enqueue %s: %v", id, err)
		}
	}
	assertDepth(t, q, 3, 0, 0)

	c, err := q.Claim(ctx, "w1", time.Second)
	if err != nil {
		t.Fatalf("claim: %v", err)
	}
	assertDepth(t, q, 2, 1, 0)

	// MaxAttempts is 1, so one nack dead-letters it.
	if err := q.Nack(ctx, c.ID, errors.New("nope"), 0); err != nil {
		t.Fatalf("nack: %v", err)
	}
	assertDepth(t, q, 2, 0, 1)
}

func assertDepth(t *testing.T, q *queue.RedisQueue, pending, claimed, dead int) {
	t.Helper()
	got, err := q.Depth(context.Background())
	if err != nil {
		t.Fatalf("depth: %v", err)
	}
	if got[queue.StatePending] != pending ||
		got[queue.StateClaimed] != claimed ||
		got[queue.StateDead] != dead {
		t.Fatalf("depth = pending:%d claimed:%d dead:%d; want %d/%d/%d",
			got[queue.StatePending], got[queue.StateClaimed], got[queue.StateDead],
			pending, claimed, dead)
	}
}

// Succeeded jobs must expire, or Redis grows without bound.
func TestRedisAckSetsExpiry(t *testing.T) {
	q := redisQueue(t)
	ctx := context.Background()

	if err := q.Enqueue(ctx, &queue.Job{ID: "ttl1", Type: "t", MaxAttempts: 1}); err != nil {
		t.Fatalf("enqueue: %v", err)
	}
	if _, err := q.Claim(ctx, "w1", time.Second); err != nil {
		t.Fatalf("claim: %v", err)
	}
	if err := q.Ack(ctx, "ttl1"); err != nil {
		t.Fatalf("ack: %v", err)
	}

	ttl, err := q.KeyTTL(ctx, "ttl1")
	if err != nil {
		t.Fatalf("ttl: %v", err)
	}
	if ttl <= 0 {
		t.Fatalf("succeeded job has TTL %v; it would live in Redis forever", ttl)
	}
}
