package api_test

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/anantraochinta/goqueue/internal/api"
	"github.com/anantraochinta/goqueue/internal/queue"
)

func quietLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

func newServer(t *testing.T, q queue.Queue, ready func(*http.Request) error) http.Handler {
	t.Helper()
	s := &api.Server{Queue: q, Logger: quietLogger(), Ready: ready}
	return s.Routes()
}

func do(t *testing.T, h http.Handler, method, path, body string) *httptest.ResponseRecorder {
	t.Helper()
	var r *http.Request
	if body == "" {
		r = httptest.NewRequest(method, path, nil)
	} else {
		r = httptest.NewRequest(method, path, strings.NewReader(body))
		r.Header.Set("Content-Type", "application/json")
	}
	w := httptest.NewRecorder()
	h.ServeHTTP(w, r)
	return w
}

// brokenQueue fails every operation, to exercise the degraded paths.
type brokenQueue struct{ err error }

func (b brokenQueue) Enqueue(context.Context, *queue.Job) error { return b.err }
func (b brokenQueue) Claim(context.Context, string, time.Duration) (*queue.Job, error) {
	return nil, b.err
}
func (b brokenQueue) Ack(context.Context, string) error                        { return b.err }
func (b brokenQueue) Nack(context.Context, string, error, time.Duration) error { return b.err }
func (b brokenQueue) Status(context.Context, string) (queue.State, error)      { return "", b.err }
func (b brokenQueue) Get(context.Context, string) (*queue.Job, error)          { return nil, b.err }

func TestSubmitAcceptsAJob(t *testing.T) {
	q := queue.NewMemoryQueue()
	h := newServer(t, q, nil)

	w := do(t, h, http.MethodPost, "/api/v1/jobs",
		`{"type":"email","payload":{"to":"a@example.com"},"max_attempts":3}`)

	// 202, not 201: the job is accepted for later execution, nothing ran.
	if w.Code != http.StatusAccepted {
		t.Fatalf("status = %d, want 202; body = %s", w.Code, w.Body)
	}

	var got api.JobResponse
	if err := json.Unmarshal(w.Body.Bytes(), &got); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if got.ID == "" {
		t.Fatal("response has no generated ID")
	}
	if got.State != string(queue.StatePending) {
		t.Fatalf("state = %s, want pending", got.State)
	}
	if got.MaxAttempts != 3 {
		t.Fatalf("max_attempts = %d, want 3", got.MaxAttempts)
	}
	if loc := w.Header().Get("Location"); loc != "/api/v1/jobs/"+got.ID {
		t.Fatalf("Location = %q, want /api/v1/jobs/%s", loc, got.ID)
	}

	// The job really is in the queue, not just echoed back.
	if state, err := q.Status(context.Background(), got.ID); err != nil || state != queue.StatePending {
		t.Fatalf("queue state = %s, %v; want pending", state, err)
	}
}

func TestSubmitRejectsBadInput(t *testing.T) {
	cases := []struct {
		name string
		body string
		want int
	}{
		{"missing type", `{"payload":{}}`, http.StatusBadRequest},
		{"empty type", `{"type":""}`, http.StatusBadRequest},
		{"malformed json", `{nope`, http.StatusBadRequest},
		{"empty body", ``, http.StatusBadRequest},
		// A typo'd field must fail loudly rather than being silently
		// dropped, which is why the decoder disallows unknown fields.
		{"unknown field", `{"type":"email","maxattempts":3}`, http.StatusBadRequest},
		{"negative max_attempts", `{"type":"email","max_attempts":-1}`, http.StatusBadRequest},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			h := newServer(t, queue.NewMemoryQueue(), nil)
			// httptest.NewRequest needs a non-nil body for the empty case.
			r := httptest.NewRequest(http.MethodPost, "/api/v1/jobs", strings.NewReader(tc.body))
			w := httptest.NewRecorder()
			h.ServeHTTP(w, r)

			if w.Code != tc.want {
				t.Fatalf("status = %d, want %d; body = %s", w.Code, tc.want, w.Body)
			}
			var body map[string]string
			if err := json.Unmarshal(w.Body.Bytes(), &body); err != nil || body["error"] == "" {
				t.Fatalf("expected a JSON error body, got %s", w.Body)
			}
		})
	}
}

func TestSubmitDuplicateIDConflicts(t *testing.T) {
	h := newServer(t, queue.NewMemoryQueue(), nil)

	if w := do(t, h, http.MethodPost, "/api/v1/jobs", `{"type":"email","id":"fixed"}`); w.Code != http.StatusAccepted {
		t.Fatalf("first submit status = %d, want 202", w.Code)
	}
	// A client-supplied ID makes submission idempotent: the repeat is the
	// client's problem (409), not a server fault.
	if w := do(t, h, http.MethodPost, "/api/v1/jobs", `{"type":"email","id":"fixed"}`); w.Code != http.StatusConflict {
		t.Fatalf("duplicate submit status = %d, want 409", w.Code)
	}
}

func TestSubmitRejectsOversizedBody(t *testing.T) {
	h := newServer(t, queue.NewMemoryQueue(), nil)
	huge := `{"type":"email","payload":{"blob":"` + strings.Repeat("x", api.MaxBodyBytes+1024) + `"}}`

	w := do(t, h, http.MethodPost, "/api/v1/jobs", huge)
	if w.Code != http.StatusBadRequest && w.Code != http.StatusRequestEntityTooLarge {
		t.Fatalf("status = %d, want the oversized body to be rejected", w.Code)
	}
}

// A queue outage is a 503, not a 500: it is transient and the client should
// retry.
func TestSubmitReportsQueueFailureAsUnavailable(t *testing.T) {
	h := newServer(t, brokenQueue{err: errors.New("redis is down")}, nil)

	w := do(t, h, http.MethodPost, "/api/v1/jobs", `{"type":"email"}`)
	if w.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want 503", w.Code)
	}
	// The internal error text must not leak to the client.
	if strings.Contains(w.Body.String(), "redis is down") {
		t.Fatalf("internal error leaked to the client: %s", w.Body)
	}
}

func TestGetJob(t *testing.T) {
	q := queue.NewMemoryQueue()
	h := newServer(t, q, nil)

	if err := q.Enqueue(context.Background(), &queue.Job{
		ID: "known", Type: "email", MaxAttempts: 2,
		Payload: json.RawMessage(`{"to":"a@example.com"}`),
	}); err != nil {
		t.Fatalf("enqueue: %v", err)
	}

	t.Run("existing", func(t *testing.T) {
		w := do(t, h, http.MethodGet, "/api/v1/jobs/known", "")
		if w.Code != http.StatusOK {
			t.Fatalf("status = %d, want 200", w.Code)
		}
		var got api.JobResponse
		if err := json.Unmarshal(w.Body.Bytes(), &got); err != nil {
			t.Fatalf("decode: %v", err)
		}
		if got.ID != "known" || got.Type != "email" || got.MaxAttempts != 2 {
			t.Fatalf("unexpected body: %+v", got)
		}
		if string(got.Payload) != `{"to":"a@example.com"}` {
			t.Fatalf("payload = %s, want it round-tripped exactly", got.Payload)
		}
	})

	t.Run("unknown", func(t *testing.T) {
		if w := do(t, h, http.MethodGet, "/api/v1/jobs/nope", ""); w.Code != http.StatusNotFound {
			t.Fatalf("status = %d, want 404", w.Code)
		}
	})
}

func TestMethodNotAllowed(t *testing.T) {
	h := newServer(t, queue.NewMemoryQueue(), nil)
	// Method-qualified routes mean the router returns 405 without every
	// handler needing a check.
	if w := do(t, h, http.MethodDelete, "/api/v1/jobs/x", ""); w.Code != http.StatusMethodNotAllowed {
		t.Fatalf("DELETE status = %d, want 405", w.Code)
	}
	if w := do(t, h, http.MethodGet, "/api/v1/jobs", ""); w.Code != http.StatusMethodNotAllowed {
		t.Fatalf("GET on the collection status = %d, want 405", w.Code)
	}
}

// The central probe invariant: liveness must stay green while a dependency
// is down. If it did not, a brief Redis outage would restart every
// scheduler pod and turn a dependency blip into an outage of our own.
func TestHealthzIgnoresDependencies(t *testing.T) {
	h := newServer(t, brokenQueue{err: errors.New("redis is down")},
		func(*http.Request) error { return errors.New("redis is down") })

	if w := do(t, h, http.MethodGet, "/healthz", ""); w.Code != http.StatusOK {
		t.Fatalf("/healthz = %d while the dependency was down; want 200", w.Code)
	}
	if w := do(t, h, http.MethodGet, "/readyz", ""); w.Code != http.StatusServiceUnavailable {
		t.Fatalf("/readyz = %d while the dependency was down; want 503", w.Code)
	}
}

func TestReadyzPassesWhenDependenciesAreUp(t *testing.T) {
	h := newServer(t, queue.NewMemoryQueue(), func(*http.Request) error { return nil })
	if w := do(t, h, http.MethodGet, "/readyz", ""); w.Code != http.StatusOK {
		t.Fatalf("/readyz = %d, want 200", w.Code)
	}
}

// With no Ready func configured there is nothing external to check, so the
// scheduler is ready as soon as it is up.
func TestReadyzDefaultsToReady(t *testing.T) {
	h := newServer(t, queue.NewMemoryQueue(), nil)
	if w := do(t, h, http.MethodGet, "/readyz", ""); w.Code != http.StatusOK {
		t.Fatalf("/readyz = %d, want 200", w.Code)
	}
}
