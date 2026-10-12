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
	"time"
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

	// Get returns a copy of a job, including fields Status does not expose
	// such as Attempts and LastError. Returns ErrJobNotFound for an
	// unknown ID.
	Get(ctx context.Context, jobID string) (*Job, error)
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

// applyEnqueueDefaults fills in the fields Enqueue is allowed to default.
//
// Shared by every implementation on purpose: if MemoryQueue defaulted
// MaxAttempts to 1 and RedisQueue to 3, the contract test suite would pass
// against both while the two behaved differently in production.
func applyEnqueueDefaults(job *Job) {
	if job.ID == "" {
		job.ID = fmt.Sprintf("job-%d", time.Now().UnixNano())
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
}
