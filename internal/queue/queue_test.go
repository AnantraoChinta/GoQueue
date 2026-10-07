package queue

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"
)

func TestMemoryQueueLifecycle(t *testing.T) {
	q := NewMemoryQueue()

	job := &Job{
		ID:          "job-1",
		Type:        "email",
		Payload:     json.RawMessage(`{"to":"a@example.com"}`),
		MaxAttempts: 3,
	}

	if err := q.Enqueue(context.Background(), job); err != nil {
		t.Fatalf("enqueue returned error: %v", err)
	}

	state, err := q.Status(context.Background(), job.ID)
	if err != nil {
		t.Fatalf("status returned error: %v", err)
	}
	if state != StatePending {
		t.Fatalf("expected pending state, got %s", state)
	}

	claimed, err := q.Claim(context.Background(), "worker-1", time.Second)
	if err != nil {
		t.Fatalf("claim returned error: %v", err)
	}
	if claimed == nil {
		t.Fatal("expected a claimed job, got nil")
	}
	if claimed.State != StateClaimed {
		t.Fatalf("expected claimed state, got %s", claimed.State)
	}
	if claimed.Attempts != 1 {
		t.Fatalf("expected attempts to be 1, got %d", claimed.Attempts)
	}

	if err := q.Ack(context.Background(), claimed.ID); err != nil {
		t.Fatalf("ack returned error: %v", err)
	}

	state, err = q.Status(context.Background(), claimed.ID)
	if err != nil {
		t.Fatalf("status returned error: %v", err)
	}
	if state != StateSucceeded {
		t.Fatalf("expected succeeded state, got %s", state)
	}
}

func TestMemoryQueueNackRequeuesUntilMaxAttempts(t *testing.T) {
	q := NewMemoryQueue()

	job := &Job{ID: "job-2", Type: "email", MaxAttempts: 2}
	if err := q.Enqueue(context.Background(), job); err != nil {
		t.Fatalf("enqueue returned error: %v", err)
	}

	claimed, err := q.Claim(context.Background(), "worker-1", time.Second)
	if err != nil {
		t.Fatalf("claim returned error: %v", err)
	}
	if claimed == nil {
		t.Fatal("expected a claimed job, got nil")
	}

	reason := errors.New("temporary failure")
	if err := q.Nack(context.Background(), claimed.ID, reason, time.Second); err != nil {
		t.Fatalf("nack returned error: %v", err)
	}

	state, err := q.Status(context.Background(), claimed.ID)
	if err != nil {
		t.Fatalf("status returned error: %v", err)
	}
	if state != StatePending {
		t.Fatalf("expected pending (claimable again) after retryable nack, got %s", state)
	}
	if q.jobs[claimed.ID].LastError != reason.Error() {
		t.Fatalf("expected last error %q, got %q", reason.Error(), q.jobs[claimed.ID].LastError)
	}

	claimedAgain, err := q.Claim(context.Background(), "worker-2", time.Second)
	if err != nil {
		t.Fatalf("second claim returned error: %v", err)
	}
	if claimedAgain == nil {
		t.Fatal("expected a second claim, got nil")
	}

	finalReason := errors.New("permanent failure")
	if err := q.Nack(context.Background(), claimedAgain.ID, finalReason, time.Second); err != nil {
		t.Fatalf("second nack returned error: %v", err)
	}

	state, err = q.Status(context.Background(), claimedAgain.ID)
	if err != nil {
		t.Fatalf("status returned error: %v", err)
	}
	if state != StateDead {
		t.Fatalf("expected dead after max attempts, got %s", state)
	}
	if q.jobs[claimedAgain.ID].LastError != finalReason.Error() {
		t.Fatalf("expected last error %q, got %q", finalReason.Error(), q.jobs[claimedAgain.ID].LastError)
	}
}

func TestMemoryQueueMultipleWorkersClaimUniqueJobs(t *testing.T) {
	q := NewMemoryQueue()

	for i := 0; i < 20; i++ {
		job := &Job{
			ID:          fmt.Sprintf("job-%d", i),
			Type:        "email",
			Payload:     json.RawMessage(`{"to":"a@example.com"}`),
			MaxAttempts: 3,
		}
		if err := q.Enqueue(context.Background(), job); err != nil {
			t.Fatalf("enqueue %d returned error: %v", i, err)
		}
	}

	const workers = 5
	var wg sync.WaitGroup
	claimedCh := make(chan string, 20)
	claimErrCh := make(chan error, 1)

	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()

			for {
				job, err := q.Claim(context.Background(), fmt.Sprintf("worker-%d", workerID), 50*time.Millisecond)
				if err != nil {
					if errors.Is(err, ErrNoJobAvailable) {
						return
					}
					select {
					case claimErrCh <- err:
					default:
					}
					return
				}
				if job == nil {
					continue
				}
				claimedCh <- job.ID
				if err := q.Ack(context.Background(), job.ID); err != nil {
					select {
					case claimErrCh <- err:
					default:
					}
					return
				}
			}
		}(i)
	}

	wg.Wait()
	close(claimedCh)
	close(claimErrCh)

	for err := range claimErrCh {
		t.Fatalf("worker error: %v", err)
	}

	seen := make(map[string]int)
	for id := range claimedCh {
		seen[id]++
		if seen[id] > 1 {
			t.Fatalf("job %s claimed more than once", id)
		}
	}

	if len(seen) != 20 {
		t.Fatalf("expected 20 unique claimed jobs, got %d", len(seen))
	}
}
