// Package queue is the Redis-backed job queue.
//
// It is the only package that knows how jobs are stored in Redis. The
// scheduler and the worker pool both talk to this interface, which keeps the
// storage layout in one place and makes the queue swappable in tests.
package queue

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
)

// Queue is the behaviour the rest of GoQueue depends on. Depending on this
// interface rather than on *RedisQueue lets tests substitute an in-memory
// fake without a running Redis.
//
// The doc comments below are the contract, not a description of one
// implementation. Every implementation must honour them, and callers may
// rely on nothing else. Conditions the contract forbids are not checked;
// violating them is a bug in the caller and may panic.
type Queue interface {
	// Enqueue stores job and makes it claimable.
	//
	// The caller must pass a non-nil job with a non-empty Type. ID, State,
	// CreatedAt and MaxAttempts are filled in when left zero. job is copied,
	// so the caller may reuse or mutate it afterwards.
	//
	// Returns an error if Type is empty or if job.ID already exists.
	Enqueue(ctx context.Context, job *Job) error

	// Claim reserves one claimable job for workerID, moves it to
	// StateClaimed and increments its Attempts. It blocks for up to block
	// waiting for a job to become claimable; block <= 0 checks once and
	// returns immediately. The returned *Job is a copy.
	//
	// A job is claimable when it is in StatePending and its NotBefore has
	// passed.
	//
	// Returns ErrNoJobAvailable if nothing became claimable in time. On a
	// nil error the returned job is always non-nil.
	Claim(ctx context.Context, workerID string, block time.Duration) (*Job, error)

	// Ack marks a claimed job as succeeded. Only the worker that claimed the
	// job should call it.
	//
	// Returns ErrJobNotFound if jobID is unknown, or ErrNotClaimed if the
	// job is no longer in StateClaimed - which happens when a claim expired
	// and another worker took over. Callers should treat ErrNotClaimed as a
	// signal worth counting, not as a fatal error.
	Ack(ctx context.Context, jobID string) error

	// Nack reports that an attempt failed. reason may be nil if no error is
	// available; it is recorded in Job.LastError. If attempts remain the job
	// returns to StatePending with NotBefore set to now+retryAfter, so it is
	// not reclaimed before then; otherwise it moves to StateDead.
	//
	// Returns ErrJobNotFound or ErrNotClaimed under the same conditions as
	// Ack.
	Nack(ctx context.Context, jobID string, reason error, retryAfter time.Duration) error

	// Status reports the current state of a job. jobID is untrusted input;
	// an unknown ID returns ErrJobNotFound rather than an empty State.
	Status(ctx context.Context, jobID string) (State, error)
}

var (
	// ErrJobNotFound means no job exists with the given ID.
	ErrJobNotFound = errors.New("job not found")

	// ErrNoJobAvailable means Claim found nothing claimable before its
	// deadline. It is an ordinary outcome of an idle queue, not a failure.
	ErrNoJobAvailable = errors.New("no job available")

	// ErrNotClaimed means Ack or Nack was called for a job that is not in
	// StateClaimed. With a claim TTL this is expected rather than
	// exceptional: a slow worker's claim expires, another worker takes the
	// job, and the first worker's Ack arrives late. Count it - a rising
	// rate means claimTTL is shorter than real job durations.
	ErrNotClaimed = errors.New("job is not claimed")
)

// Compile-time proof that the in-memory implementation still matches the
// interface. Without this, MemoryQueue could silently drift out of sync.
var _ Queue = (*MemoryQueue)(nil)

// Job is one unit of work travelling through the system.
//
// The payload is intentionally stored as raw JSON so the queue can carry a
// structured body without hard-coding one serialization format in the queue
// package itself.
type Job struct {
	ID          string          `json:"id"`
	Type        string          `json:"type"`
	Payload     json.RawMessage `json:"payload,omitempty"`
	State       State           `json:"state"`
	Attempts    int             `json:"attempts"`
	MaxAttempts int             `json:"max_attempts"`
	CreatedAt   time.Time       `json:"created_at"`
	ClaimedAt   *time.Time      `json:"claimed_at,omitempty"`
	ClaimedBy   string          `json:"claimed_by,omitempty"`
	LastError   string          `json:"last_error,omitempty"`

	// NotBefore holds a job back until the given time. Set by Nack to
	// implement retry backoff; nil means immediately claimable.
	NotBefore *time.Time `json:"not_before,omitempty"`
}

// State is the lifecycle position of a job.
type State string

const (
	// StatePending means the job is claimable. A job that is being retried
	// is also pending; Attempts > 0 distinguishes a retry from a first run.
	StatePending State = "pending"

	// StateClaimed means a worker holds the job and is executing it.
	StateClaimed State = "claimed"

	// StateSucceeded is terminal: the handler returned nil.
	StateSucceeded State = "succeeded"

	// StateFailed is terminal: the job failed in a way that must not be
	// retried, regardless of remaining attempts.
	//
	// RESERVED - no code path sets this yet. It becomes reachable once
	// handlers can signal a permanent error. See the note in the package
	// README section of CLAUDE.md.
	StateFailed State = "failed"

	// StateDead is terminal: the job exhausted MaxAttempts.
	StateDead State = "dead"
)

// MemoryQueue is an in-memory implementation used for tests and local
// development before a Redis-backed implementation is added.
type MemoryQueue struct {
	mu      sync.Mutex
	jobs    map[string]*Job
	pending []string
}

func NewMemoryQueue() *MemoryQueue {
	return &MemoryQueue{
		jobs: make(map[string]*Job),
	}
}

// Enqueue implements Queue. It panics if job is nil, which the contract
// forbids.
func (q *MemoryQueue) Enqueue(_ context.Context, job *Job) error {
	// Type is the one field the caller must supply; everything else is
	// defaulted. This is untrusted input from the HTTP layer, so it is
	// validated rather than assumed.
	if job.ID == "" {
		job.ID = fmt.Sprintf("job-%d", time.Now().UnixNano())
	}
	if job.Type == "" {
		return errors.New("job type is required")
	}
	if job.MaxAttempts <= 0 {
		job.MaxAttempts = 1
	}
	if job.CreatedAt.IsZero() {
		job.CreatedAt = time.Now().UTC()
	}
	if job.State == "" {
		job.State = StatePending
	}

	q.mu.Lock()
	defer q.mu.Unlock()

	if _, exists := q.jobs[job.ID]; exists {
		return fmt.Errorf("job %s already exists", job.ID)
	}
	// Stores a copy of the job in the queue to avoid external mutation after enqueue.
	stored := *job
	q.jobs[job.ID] = &stored
	q.pending = append(q.pending, job.ID)
	return nil
}

// Claim implements Queue.
func (q *MemoryQueue) Claim(_ context.Context, workerID string, block time.Duration) (*Job, error) {
	deadline := time.Now().Add(block)
	for {
		q.mu.Lock()
		now := time.Now().UTC()
		for i, id := range q.pending {
			// q.jobs[id] is always present: IDs are only appended to
			// q.pending in the same critical section that inserts into
			// q.jobs, and jobs are never deleted. A state check is still
			// needed because Nack re-queues an ID that may since have
			// changed state.
			job := q.jobs[id]
			if job.State != StatePending {
				continue
			}
			// Backoff: a nacked job is held until its retry time.
			if job.NotBefore != nil && now.Before(*job.NotBefore) {
				continue
			}
			q.pending = append(q.pending[:i], q.pending[i+1:]...)
			job.Attempts++
			job.State = StateClaimed
			now := time.Now().UTC()
			job.ClaimedAt = &now
			job.ClaimedBy = workerID
			out := *job
			q.mu.Unlock()
			return &out, nil
		}
		q.mu.Unlock()

		if block <= 0 {
			return nil, ErrNoJobAvailable
		}
		if time.Now().After(deadline) {
			return nil, ErrNoJobAvailable
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// Ack implements Queue.
func (q *MemoryQueue) Ack(_ context.Context, jobID string) error {
	q.mu.Lock()
	defer q.mu.Unlock()

	job, ok := q.jobs[jobID]
	if !ok {
		return ErrJobNotFound
	}
	// Refusing a non-claimed job also keeps q.pending honest: acking a job
	// that was never claimed would otherwise leave its ID in the slice for
	// Claim to skip over forever.
	if job.State != StateClaimed {
		return ErrNotClaimed
	}

	job.State = StateSucceeded
	job.ClaimedBy = ""
	job.ClaimedAt = nil
	return nil
}

// Nack implements Queue.
func (q *MemoryQueue) Nack(_ context.Context, jobID string, reason error, retryAfter time.Duration) error {
	q.mu.Lock()
	defer q.mu.Unlock()

	job, ok := q.jobs[jobID]
	if !ok {
		return ErrJobNotFound
	}
	// Previously this returned nil, silently swallowing a late Nack from a
	// worker whose claim had expired. Surfacing it lets the pool count it.
	if job.State != StateClaimed {
		return ErrNotClaimed
	}

	job.ClaimedBy = ""
	job.ClaimedAt = nil
	if reason != nil {
		job.LastError = reason.Error()
	} else {
		job.LastError = ""
	}

	if job.Attempts >= job.MaxAttempts {
		job.State = StateDead
		return nil
	}

	// Back to StatePending, not StateFailed: StatePending is the single
	// definition of "claimable", and Claim skips anything else. Attempts > 0
	// is what marks this as a retry.
	job.State = StatePending
	if retryAfter > 0 {
		next := time.Now().UTC().Add(retryAfter)
		job.NotBefore = &next
	} else {
		job.NotBefore = nil
	}
	q.pending = append(q.pending, jobID)
	return nil
}

// Get returns a copy of a job, including fields Status does not expose such
// as Attempts and LastError.
//
// Not part of the Queue interface yet. It will likely need to be once
// internal/api serves GET /jobs/{id}, which needs more than a bare state.
func (q *MemoryQueue) Get(_ context.Context, jobID string) (*Job, error) {
	q.mu.Lock()
	defer q.mu.Unlock()

	job, ok := q.jobs[jobID]
	if !ok {
		return nil, ErrJobNotFound
	}
	out := *job
	return &out, nil
}

// Status implements Queue.
func (q *MemoryQueue) Status(_ context.Context, jobID string) (State, error) {
	q.mu.Lock()
	defer q.mu.Unlock()

	job, ok := q.jobs[jobID]
	if !ok {
		return "", ErrJobNotFound
	}
	return job.State, nil
}

// RedisQueue is the Redis implementation of Queue.
//
// TODO: implement. Design notes to resolve:
//
//   - Use a Redis list (LPUSH + BRPOPLPUSH) or a Redis Stream with consumer
//     groups. Streams give consumer-group bookkeeping and redelivery for
//     free; lists are simpler but need a hand-rolled in-flight set.
//   - Whichever is chosen, a claimed job must be recoverable if the worker
//     holding it dies, otherwise the "no job is lost" guarantee is false.
//   - Multi-key updates need a Lua script to stay atomic; Redis pipelines
//     are not transactional on their own.
type RedisQueue struct {
	rdb *redis.Client

	// namespace prefix for keys, so several environments can share a
	// Redis instance.
	prefix string

	// how long a claim is valid before another worker may steal it.
	claimTTL time.Duration
}

// New returns a RedisQueue backed by the given client. rdb must be non-nil.
//
// It pings Redis so that a misconfigured deployment fails at startup rather
// than on the first job.
func New(ctx context.Context, rdb *redis.Client, prefix string) (*RedisQueue, error) {
	if prefix == "" {
		prefix = "goqueue"
	}
	if err := rdb.Ping(ctx).Err(); err != nil {
		return nil, fmt.Errorf("redis ping failed: %w", err)
	}

	return &RedisQueue{
		rdb:      rdb,
		prefix:   prefix,
		claimTTL: 30 * time.Second,
	}, nil
}
