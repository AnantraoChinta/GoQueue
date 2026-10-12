// Package api holds the HTTP handlers for the scheduler.
//
// It is deliberately thin: handlers decode and validate requests, call into
// the queue, and encode responses. Business rules belong in the queue and
// worker packages so they stay testable without HTTP.
package api

import (
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"time"

	"github.com/anantraochinta/goqueue/internal/queue"
)

// MaxBodyBytes caps a job submission. Without a limit, one client can
// exhaust the scheduler's memory with a single request.
const MaxBodyBytes = 1 << 20 // 1 MiB

// Server wires the HTTP handlers to their dependencies.
type Server struct {
	Queue  queue.Queue
	Logger *slog.Logger

	// Ready reports whether dependencies are reachable. Used by /readyz
	// only - never by /healthz.
	Ready func(r *http.Request) error
}

// SubmitRequest is the body of POST /api/v1/jobs.
type SubmitRequest struct {
	Type        string          `json:"type"`
	Payload     json.RawMessage `json:"payload,omitempty"`
	MaxAttempts int             `json:"max_attempts,omitempty"`

	// ID is optional. Supplying one makes submission idempotent: a repeat
	// with the same ID is rejected rather than duplicating the work.
	ID string `json:"id,omitempty"`
}

// JobResponse is the public view of a job. It is a separate type from
// queue.Job on purpose: the storage representation should be free to change
// without altering the API.
type JobResponse struct {
	ID          string          `json:"id"`
	Type        string          `json:"type"`
	State       string          `json:"state"`
	Attempts    int             `json:"attempts"`
	MaxAttempts int             `json:"max_attempts"`
	CreatedAt   time.Time       `json:"created_at"`
	ClaimedBy   string          `json:"claimed_by,omitempty"`
	LastError   string          `json:"last_error,omitempty"`
	Payload     json.RawMessage `json:"payload,omitempty"`
}

// Routes returns the HTTP handler for the scheduler API.
func (s *Server) Routes() http.Handler {
	mux := http.NewServeMux()

	// Method-qualified patterns (Go 1.22+): a GET to a POST-only route gets
	// 405 from the router rather than needing a check in every handler.
	mux.HandleFunc("POST /api/v1/jobs", s.handleSubmit)
	mux.HandleFunc("GET /api/v1/jobs/{id}", s.handleGet)

	// Liveness: is this process wedged? It must NOT check Redis. A liveness
	// probe that depends on Redis turns a brief Redis outage into every
	// scheduler pod being restarted, which is an outage you caused.
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok\n"))
	})

	// Readiness: should traffic come here? This one may check Redis.
	// Failing it removes the pod from the Service without restarting it.
	mux.HandleFunc("GET /readyz", s.handleReady)

	return s.withLogging(mux)
}

func (s *Server) handleSubmit(w http.ResponseWriter, r *http.Request) {
	r.Body = http.MaxBytesReader(w, r.Body, MaxBodyBytes)

	var req SubmitRequest
	dec := json.NewDecoder(r.Body)
	dec.DisallowUnknownFields() // a typo'd field should fail loudly, not be ignored
	if err := dec.Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, fmt.Sprintf("invalid JSON body: %v", err))
		return
	}
	if req.Type == "" {
		writeError(w, http.StatusBadRequest, "field \"type\" is required")
		return
	}
	if req.MaxAttempts < 0 {
		writeError(w, http.StatusBadRequest, "max_attempts must not be negative")
		return
	}

	job := &queue.Job{
		ID:          req.ID,
		Type:        req.Type,
		Payload:     req.Payload,
		MaxAttempts: req.MaxAttempts,
	}
	if err := s.Queue.Enqueue(r.Context(), job); err != nil {
		// A duplicate ID is the client's fault, not ours.
		if req.ID != "" {
			writeError(w, http.StatusConflict, err.Error())
			return
		}
		s.Logger.Error("enqueue failed", "type", req.Type, "error", err)
		writeError(w, http.StatusServiceUnavailable, "could not enqueue job")
		return
	}

	// 202, not 201: the job is accepted for later execution, and nothing
	// has run yet. 201 Created would imply the work is done.
	w.Header().Set("Location", "/api/v1/jobs/"+job.ID)
	writeJSON(w, http.StatusAccepted, toResponse(job))
}

func (s *Server) handleGet(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")

	job, err := s.Queue.Get(r.Context(), id)
	if err != nil {
		if errors.Is(err, queue.ErrJobNotFound) {
			writeError(w, http.StatusNotFound, "job not found")
			return
		}
		s.Logger.Error("job lookup failed", "job", id, "error", err)
		writeError(w, http.StatusServiceUnavailable, "could not read job")
		return
	}
	writeJSON(w, http.StatusOK, toResponse(job))
}

func (s *Server) handleReady(w http.ResponseWriter, r *http.Request) {
	if s.Ready != nil {
		if err := s.Ready(r); err != nil {
			writeError(w, http.StatusServiceUnavailable, "not ready: "+err.Error())
			return
		}
	}
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte("ready\n"))
}

// withLogging records one structured line per request.
func (s *Server) withLogging(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		rec := &statusRecorder{ResponseWriter: w, status: http.StatusOK}
		next.ServeHTTP(rec, r)

		// Health checks fire every few seconds; logging them at info level
		// would bury everything else.
		level := slog.LevelInfo
		if r.URL.Path == "/healthz" || r.URL.Path == "/readyz" {
			level = slog.LevelDebug
		}
		s.Logger.Log(r.Context(), level, "http request",
			"method", r.Method, "path", r.URL.Path,
			"status", rec.status, "duration", time.Since(start))
	})
}

type statusRecorder struct {
	http.ResponseWriter
	status int
}

func (r *statusRecorder) WriteHeader(code int) {
	r.status = code
	r.ResponseWriter.WriteHeader(code)
}

func toResponse(j *queue.Job) JobResponse {
	return JobResponse{
		ID:          j.ID,
		Type:        j.Type,
		State:       string(j.State),
		Attempts:    j.Attempts,
		MaxAttempts: j.MaxAttempts,
		CreatedAt:   j.CreatedAt,
		ClaimedBy:   j.ClaimedBy,
		LastError:   j.LastError,
		Payload:     j.Payload,
	}
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func writeError(w http.ResponseWriter, status int, msg string) {
	writeJSON(w, status, map[string]string{"error": msg})
}
