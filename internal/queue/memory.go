// Package queue - in-memory implementation.
//
// MemoryQueue exists so the worker pool and the API can be developed and
// tested without a running Redis. It is also the reference against which
// RedisQueue is checked: both run the same contract test suite, and any
// behavioural difference is a bug in one of them.
package queue

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"
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
	applyEnqueueDefaults(job)
	// Type is the one field the caller must supply. This is untrusted input
	// from the HTTP layer, so it is validated rather than assumed.
	if job.Type == "" {
		return errors.New("job type is required")
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

// Get implements Queue.
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

// Depth counts jobs by state. This is the signal worker autoscaling keys
// on: a growing StatePending count means workers are not keeping up.
//
// Not on the Queue interface. Callers that want it should type-assert, the
// way net/http callers assert for http.Flusher - that keeps the core
// interface small and lets an implementation opt out if counting is
// expensive.
func (q *MemoryQueue) Depth(_ context.Context) (map[State]int, error) {
	q.mu.Lock()
	defer q.mu.Unlock()

	out := make(map[State]int, 5)
	for _, job := range q.jobs {
		out[job.State]++
	}
	return out, nil
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
