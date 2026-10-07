// Package api holds the HTTP handlers for the scheduler.
//
// It is deliberately thin: handlers decode and validate requests, call into
// the queue, and encode responses. Business rules belong in the queue and
// worker packages so they stay testable without HTTP.
package api

import "net/http"

// Server wires the HTTP handlers to their dependencies.
//
// TODO: hold a queue.Queue and a logger.
type Server struct {
	// TODO: define.
}

// Routes returns the HTTP handler for the scheduler API.
//
// TODO: implement. Planned endpoints:
//
//	POST   /api/v1/jobs       submit a job, return its ID (202 Accepted)
//	GET    /api/v1/jobs/{id}  fetch job status
//	DELETE /api/v1/jobs/{id}  cancel a pending job
//	GET    /healthz           liveness: is the process up
//	GET    /readyz            readiness: can it reach Redis
//	GET    /metrics           Prometheus exposition (from internal/metrics)
//
// Note that /healthz and /readyz must be genuinely different: a liveness
// probe that checks Redis will cause Kubernetes to restart every scheduler
// pod during a Redis blip, turning a dependency outage into an outage of
// your own.
func (s *Server) Routes() http.Handler {
	// TODO: implement.
	return nil
}
