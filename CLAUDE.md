# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project state

**GoQueue is scaffolding only. No scheduler logic exists yet.** Every file under
`cmd/` and `internal/` is a placeholder: package docs, type declarations, and
TODO comments describing intended behaviour. `go build ./...` succeeds because
the declarations compile, not because anything works.

Treat the TODO comments in those files as design decisions already made and
recorded. They are not filler — they capture tradeoffs (Redis Streams vs. lists,
why the HPA must scale on queue depth, why liveness must not probe Redis) that
are expensive to rediscover. Read them before implementing a package.

The infrastructure around the code *is* real and verified: Docker Compose, the
kind cluster config, and the Redis Kubernetes manifest all work.

## Commands

```bash
make help              # all targets
make build             # both binaries into ./bin
make test              # go test -race -count=1 ./...
make lint              # golangci-lint using .golangci.yml
make tidy              # go mod tidy
```

Single test / single package:

```bash
go test -race -run TestPoolDrains ./internal/worker
go test -race -count=20 ./internal/worker   # repeat to surface flaky concurrency
```

`-race` is on by default in `make test` because the worker pool is the only
genuinely concurrent part of the system. Do not remove it. `-count=1` disables
Go's test cache so a run always actually runs.

Local dependencies (Redis, Prometheus, Grafana — not the app):

```bash
make compose-up / compose-down / compose-ps / compose-logs
```

The scheduler and worker are meant to run on the **host** (`make run`,
`make run-worker`) against that stack, so edits are a `go run` away rather than
an image rebuild. This is why `deploy/prometheus.yml` scrapes
`host.docker.internal:8080` and `:8081`.

Kubernetes (kind):

```bash
make kind-up           # 3-node cluster from deploy/kind-config.yaml
make k8s-apply         # namespace first, then deploy/k8s/
make k8s-status        # pods + services
make redis-ping        # throwaway pod -> redis-cli ping -> PONG
make kind-down
```

## Architecture

### Two binaries, one module

`cmd/scheduler` (HTTP API accepting job submissions) and `cmd/worker` (claims
and executes jobs) are separate `package main` directories, so one module
produces two executables. They have different scaling curves — a couple of API
replicas, many workers — and different shutdown semantics. Do not merge them
behind a `--mode` flag.

They share `internal/queue` deliberately: producer and consumer must agree
exactly on job encoding, and a shared type makes that a compile-time fact.

### Dependency direction

```
cmd/scheduler ──► internal/api ──┐
                                 ├──► internal/queue ──► Redis
cmd/worker ─────► internal/worker┘
       both ────► internal/metrics
```

`internal/queue` is the only package that may know the Redis key layout. If
Redis commands or key names appear anywhere else, that is a bug.

### Package responsibilities

- **`internal/queue`** — `Job`, `State`, the `Queue` interface, and
  `RedisQueue`. The durability guarantee ("no job is lost if a worker dies")
  is an invariant across Enqueue/Claim/Ack/Nack and only checkable because all
  four live here. Consumers depend on the `Queue` *interface*, never on
  `*RedisQueue`, so tests can use an in-memory fake instead of a live Redis.
- **`internal/worker`** — the goroutine pool. Concurrency boundary: pool size,
  per-job timeout, panic recovery, retry/backoff, two-phase graceful drain.
  This is where the hard bugs will be.
- **`internal/api`** — thin HTTP handlers. Decode, validate, call the queue,
  encode. No business rules; they belong in `queue` and `worker` where they are
  testable without HTTP.
- **`internal/metrics`** — Prometheus collector declarations, imported by both
  binaries so metric names cannot drift between them. Prometheus aggregates by
  name and labels; divergent names are unsummable.

### Unresolved design decision

`internal/queue` has not chosen between a Redis **list** (`LPUSH` +
`BRPOPLPUSH` with a hand-rolled in-flight set) and a Redis **Stream** with
consumer groups (redelivery and bookkeeping built in). Whichever is chosen, a
claim held by a dead worker must be recoverable or the durability guarantee is
false. Multi-key updates need a Lua script — Redis pipelines are not atomic.

### Implementation order

The pool cannot be written before the `Queue` interface, `Job`, and `Handler`
signatures exist — but it does **not** need Redis. Define the types, write an
in-memory fake `Queue`, build and test the pool against it, implement
`RedisQueue` last.

## Conventions and constraints

- **Go floor is 1.26.0**, set by `go-redis/v9`'s own `go.mod`, not by choice.
  Raising or lowering it has consequences for who can build the project.
- **Loop variables are per-iteration** (Go 1.22+). Do not write `i := i`.
- **`internal/` is compiler-enforced**, not convention. Nothing outside this
  module can import those packages, so the Redis layout stays changeable.
- **`.golangci.yml` carries a temporary exclusion** silencing `unused` on
  `internal/queue/queue.go`. Those placeholder struct fields are what keep
  go-redis in `go.mod` through `go mod tidy` — deleting them to satisfy the
  linter would drop the dependency. Remove the exclusion once the queue is
  implemented.
- **`revive`'s `unused-parameter` is disabled permanently**: Go routinely
  forces unused parameters via interface satisfaction.
- **Liveness probes must not check Redis.** `/healthz` = is the process wedged.
  `/readyz` = should traffic arrive. A liveness probe touching Redis turns a
  dependency blip into a restart storm.
- **The Redis manifest uses a `Deployment` + `emptyDir`**, so the queue is
  wiped on reschedule. Correct for local dev, wrong anywhere else; production
  needs a `StatefulSet` with a `volumeClaimTemplate`.
- **Worker autoscaling must key on queue depth, not CPU.** A worker blocked on
  I/O burns no CPU while the backlog grows.

## Environment gotchas

- **Keep the repo under `$HOME`.** Docker Desktop on macOS only bind-mounts
  paths on its file-sharing allowlist (`/Users`, `/Volumes`, `/private`,
  `/tmp`, `/var/folders`). The project previously lived in `/Applications` and
  Prometheus failed with `mounts denied`. Redis was unaffected because it used
  a named volume rather than a bind mount.
- **`make` is GNU Make 3.81** (Apple's bundled version). The Makefile avoids
  `.ONESHELL`, `$(file ...)`, and `.RECIPEPREFIX`. Keep it that way.
- **`make kind-load` is mandatory** before deploying local images. kind nodes
  have their own image store and cannot see the host Docker daemon's images;
  skipping it yields `ErrImagePull` even when `docker images` shows the image.
- **`make k8s-apply` applies `namespace.yaml` first**, separately.
  `kubectl apply -f <dir>/` has no cross-file ordering guarantee.
- **kind host ports are 8080 / 19090 / 13000**; Compose uses 6379 / 9090 /
  3000. Deliberately non-overlapping so both can run simultaneously.
- **Module path casing differs from the repo**: module is
  `github.com/anantraochinta/goqueue`, GitHub repo is `AnantraoChinta/GoQueue`.
  Harmless locally; matters if this is ever published as a library.

## Git

The user commits and pushes themselves. Do not run `git commit`, `git push`,
`gh repo create`, or add remotes unless explicitly asked.
