# GoQueue

A distributed job scheduler written in Go. GoQueue accepts units of work ("jobs")
submitted over HTTP, persists them to a Redis-backed queue so that no job is lost
if a worker dies, and hands them out to a pool of goroutine workers that execute
them concurrently. Everything runs as containers on a local Kubernetes cluster
(kind), exports Prometheus metrics, and autoscales the worker pool on queue depth.
The goal is a small but complete example of the patterns a production job system
needs: durable queueing, bounded concurrency, at-least-once delivery with
redelivery after worker death, horizontal scale-out, and operational visibility.

- [Architecture](#architecture)
- [Quick start](#quick-start)
- [Project structure](#project-structure)
- [Prerequisites](#prerequisites)
- [Setup](#setup)
- [HTTP API](#http-api)
- [Configuration](#configuration)
- [Metrics](#metrics)
- [Local development](#local-development)
- [Kubernetes](#kubernetes)
- [Testing](#testing)
- [Troubleshooting](#troubleshooting)
- [Project status](#project-status)

## Architecture

```
          POST /api/v1/jobs
                 │
                 ▼
        ┌──────────────────┐
        │    scheduler     │  HTTP API, 2+ replicas, stateless
        │   (cmd/scheduler)│
        └────────┬─────────┘
                 │ Enqueue
                 ▼
        ┌──────────────────┐
        │      Redis       │  ZSETs + hashes, Lua scripts
        │  (StatefulSet)   │  pending / inflight / dead
        └────────┬─────────┘
                 │ Claim ── Ack / Nack
                 ▼
        ┌──────────────────┐
        │     workers      │  goroutine pool, N replicas
        │   (cmd/worker)   │  autoscaled on queue depth
        └────────┬─────────┘
                 │ /metrics
                 ▼
        ┌──────────────────┐     ┌──────────┐
        │    Prometheus    │────▶│ Grafana  │
        └────────┬─────────┘     └──────────┘
                 │ goqueue:queue_depth:max
                 ▼
        ┌──────────────────┐
        │ prometheus-adapter│──▶ HorizontalPodAutoscaler ──▶ scales workers
        └──────────────────┘
```

### Job lifecycle

```
          Enqueue                Claim                 Ack
  (none) ─────────▶ pending ───────────▶ claimed ─────────────▶ succeeded
                       ▲                    │
                       │                    │ Nack, attempts remain
                       └────────────────────┤  (held until NotBefore)
                       │                    │
                       │                    │ Nack, attempts exhausted
                       │                    └────────────────────▶ dead
                       │
                       └── reaper: claim expired, worker presumed dead
```

A job is claimable when it is in `pending` **and** its `NotBefore` has passed.
`Attempts > 0` on a pending job marks it as a retry.

### Redis data model

| Key | Type | Contents |
| --- | --- | --- |
| `{prefix}:job:{id}` | HASH | every field of a job; `payload` is opaque bytes |
| `{prefix}:pending` | ZSET | claimable job IDs, score = unix ms it becomes ready |
| `{prefix}:inflight` | ZSET | claimed job IDs, score = unix ms the claim expires |
| `{prefix}:dead` | ZSET | dead-lettered IDs, score = unix ms it died |

**Why sorted sets rather than Streams.** Redis Streams with consumer groups are
the more obvious choice — the pending-entries list and `XAUTOCLAIM` give
redelivery almost for free. Two requirements tip the balance:

- *Retry backoff.* A nacked job must not be reclaimable before `NotBefore`. A
  ZSET scored by ready-time expresses that directly; `ZRANGEBYSCORE` with a
  ceiling of "now" returns exactly the due jobs. Streams have no delayed entry,
  and `XAUTOCLAIM`'s min-idle-time conflates "the worker died" with "this is
  scheduled for later".
- *Opaque payloads.* Mutating a job inside a Lua script means parsing it with
  `cjson`, which reorders keys and truncates int64 to a double — corrupting
  caller payloads. A HASH keeps `payload` a field Lua never touches.

The cost is that `Claim` polls, because no Redis command blocks on "lowest
score, but only if it is due". `-claim-block` and the poll interval trade claim
latency against Redis load.

**Atomicity.** Every operation touches at least two keys (an index ZSET and the
job HASH). Pipelines batch round trips but are not transactional, so a crash
between two pipelined commands would leave a job indexed as pending with a state
of `claimed`. Each operation is therefore a Lua script, which Redis runs to
completion without interleaving.

**Durability.** A worker that is OOM-killed mid-job never acks. Its claim ages
past the deadline in `:inflight`, and the reaper (`ReclaimExpired`, run by every
worker on a ticker) returns the job to `:pending`. Without that, the job is
stranded forever and the "no job is lost" guarantee is false.

### Delivery semantics

**At-least-once, not exactly-once.** A worker can finish a job and die before
acking; the reaper will redeliver it. **Handlers must be idempotent.** A late
ack from a worker whose claim was reaped returns `ErrNotClaimed` rather than
being silently applied — a rising rate of those is the signal that `-claim-ttl`
is shorter than real job durations.

## Quick start

```bash
make compose-up                               # Redis, Prometheus, Grafana
make build

./bin/scheduler -redis-addr localhost:6379 &  # terminal 1
./bin/worker    -redis-addr localhost:6379 &  # terminal 2

curl -X POST http://localhost:8080/api/v1/jobs \
  -H 'Content-Type: application/json' \
  -d '{"type":"sleep","payload":{"duration":"500ms"},"max_attempts":3}'

# returns 202 with a job id; fetch it back:
curl http://localhost:8080/api/v1/jobs/<id>

make compose-down
```

The worker ships three demo handlers — `noop`, `sleep` (honours a
`{"duration":"..."}` payload), and `flaky` (always fails, to exercise retry and
dead-lettering). Replace them with real job types in `cmd/worker/main.go`.

## Project structure

```
goqueue/
├── cmd/                      Executables. One subdirectory per binary.
│   ├── scheduler/            HTTP API that accepts job submissions
│   └── worker/               Process that claims and executes jobs
├── internal/                 Private packages (see note below)
│   ├── api/                  HTTP handlers and routing
│   ├── metrics/              Prometheus collectors, on a private registry
│   ├── queue/
│   │   ├── queue.go          Job, State, the Queue interface and its contract
│   │   ├── memory.go         MemoryQueue, the in-process implementation
│   │   ├── redis.go          RedisQueue, Lua scripts, the reaper
│   │   └── contract_test.go  one suite, run against BOTH implementations
│   └── worker/               goroutine worker pool
├── deploy/
│   ├── docker-compose.yml    Redis + Prometheus + Grafana for local dev
│   ├── prometheus.yml        Compose scrape config (static targets)
│   ├── kind-config.yaml      3-node kind cluster definition
│   └── k8s/
│       ├── namespace.yaml    the `goqueue` namespace
│       ├── redis.yaml        StatefulSet + headless Service + PVC
│       ├── scheduler.yaml    Deployment + NodePort Service
│       ├── worker.yaml       Deployment + metrics Service
│       ├── monitoring.yaml   Prometheus (service discovery) + Grafana + rules
│       └── autoscaling.yaml  prometheus-adapter + HorizontalPodAutoscaler
├── Dockerfile                Multi-stage build for both binaries
├── Makefile                  Developer tasks; `make help` lists them
└── .golangci.yml             Lint configuration
```

### Why this layout

**`cmd/scheduler` and `cmd/worker` are separate binaries.** Each subdirectory of
`cmd/` is its own `package main`, so one module produces two executables. They
have different scaling curves — a couple of API replicas, many workers — and
different shutdown semantics. A single binary with a `--mode` flag would force
them to scale together and ship an unused HTTP server into every worker pod.

They share `internal/queue` deliberately: producer and consumer must agree
exactly on job encoding, and a shared type makes that a compile-time fact.

**`internal/` is enforced by the compiler, not convention.** Packages under
`internal/` can only be imported by code rooted at its parent directory. Nobody
who runs `go get github.com/anantraochinta/goqueue` can import `internal/queue`,
so the Redis key layout stays a private implementation detail.

**`internal/queue` is the only package that knows the Redis key layout.** The
durability guarantee is an invariant across Enqueue/Claim/Ack/Nack and is only
checkable because all four live in one place. Consumers depend on the `Queue`
*interface*, never on `*RedisQueue`, which is what lets tests use `MemoryQueue`.

**One contract test suite runs against both implementations.** `MemoryQueue` is
not just a convenience — it is the reference `RedisQueue` is checked against.
Any behavioural difference is a test failure rather than a production surprise.

**`internal/metrics` is shared by both binaries.** Prometheus aggregates by name
and labels; if the scheduler emitted `jobs_total` and the worker `job_count`,
you could never sum across them.

## Prerequisites

Verified on **macOS 26.6.2 (build 25G83), Apple Silicon (arm64)**, shell `zsh`.
Nothing is macOS-specific apart from the install commands; see
[other platforms](#installing-on-other-platforms).

| Tool | Required | Verified version | Check with |
| --- | --- | --- | --- |
| Go | **1.26.0+** | 1.27.1 darwin/arm64 | `go version` |
| Docker | 20+ | 29.8.2 (Desktop 4.94.0) | `docker run hello-world` |
| Docker Compose | v2 | v5.5.1 (bundled plugin) | `docker compose version` |
| kind | any recent | 0.33.0 | `kind version` |
| kubectl | any recent | 1.37.1 | `kubectl version --client` |
| make | 3.81+ | 3.81 (Apple-bundled) | `make --version` |
| git | 2.x | 2.50.1 (Apple Git-155) | `git --version` |
| golangci-lint | 2.x | 2.14.0 | `golangci-lint --version` |

Go dependencies: `github.com/redis/go-redis/v9 v9.23.0`,
`github.com/prometheus/client_golang v1.24.1`.

### Notes on specific tools

- **Go 1.26.0 is the real floor, not 1.22.** `go-redis/v9 v9.23.0` declares
  `go 1.26.0` in its own manifest, and Go requires a module's `go` directive to
  be at least the maximum of its dependencies'. To lower it, pin an older client
  (`go get github.com/redis/go-redis/v9@v9.7.0 && go mod tidy`).
- **`make` is GNU Make 3.81**, the version Apple ships. The Makefile avoids
  `.ONESHELL`, `$(file ...)` and `.RECIPEPREFIX` to stay compatible. If you need
  those, `brew install make` provides GNU Make 4.x as `gmake`.
- **Keep the repository under your home directory.** Docker Desktop on macOS
  only bind-mounts paths on its file-sharing allowlist (`/Users`, `/Volumes`,
  `/private`, `/tmp`, `/var/folders`). The project once lived in
  `/Applications` and Prometheus failed to start with `mounts denied`.
- **Module path casing.** The module is `github.com/anantraochinta/goqueue`
  (lowercase) while the repository is `github.com/AnantraoChinta/GoQueue`.
  Harmless locally; Go module paths are case-sensitive, so align them before
  publishing this as a library.

## Setup

### 1. Install the toolchain

```bash
/bin/bash -c "$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/HEAD/install.sh)"   # if needed
brew update
brew install go kind kubectl golangci-lint
brew install --cask docker-desktop   # run interactively; prompts for your password
```

`make` and `git` ship with the Xcode Command Line Tools
(`xcode-select --install` if missing).

### 2. Start the Docker daemon

`open -a Docker`, then wait for the whale icon to stop animating.

### 3. Verify

```bash
go version && docker run hello-world && docker compose version
kind version && kubectl version --client && golangci-lint --version
```

### 4. Clone and build

Clone into your **home directory**, not `/Applications` — see the file-sharing
note above.

```bash
mkdir -p ~/Developer && cd ~/Developer
git clone https://github.com/AnantraoChinta/GoQueue.git goqueue
cd goqueue
make build
```

### Installing on other platforms

- **Linux:** Go from <https://go.dev/dl/>, Docker Engine from your package
  manager, and kind/kubectl/golangci-lint from their official release binaries.
  In `deploy/docker-compose.yml`, add
  `extra_hosts: ["host.docker.internal:host-gateway"]` to the `prometheus`
  service — that hostname is a Docker Desktop feature.
- **Windows:** WSL2 with Docker Desktop's WSL2 backend, then follow the Linux
  instructions inside your distribution. Keep the repo on the WSL filesystem
  (`~/...`), not `/mnt/c`, or file I/O will be very slow.

## HTTP API

Served by `cmd/scheduler` on `-addr` (default `:8080`).

### `POST /api/v1/jobs`

```bash
curl -X POST http://localhost:8080/api/v1/jobs \
  -H 'Content-Type: application/json' \
  -d '{"type":"sleep","payload":{"duration":"500ms"},"max_attempts":3,"id":"optional"}'
```

| Field | Required | Notes |
| --- | --- | --- |
| `type` | yes | routes the job to a registered handler |
| `payload` | no | opaque JSON, passed through byte-for-byte |
| `max_attempts` | no | defaults to 1 |
| `id` | no | supplying one makes submission idempotent |

Returns **202 Accepted** with a `Location` header — the job is accepted for
later execution, and nothing has run yet. 201 Created would imply the work is
done.

| Status | When |
| --- | --- |
| 202 | accepted |
| 400 | missing/empty `type`, malformed JSON, unknown field, negative `max_attempts` |
| 409 | a job with the supplied `id` already exists |
| 413/400 | body over 1 MiB |
| 503 | the queue is unreachable |

Unknown fields are rejected rather than ignored, so a typo fails loudly.

### `GET /api/v1/jobs/{id}`

Returns the job, including `state`, `attempts` and `last_error`. 404 if unknown.

### `GET /healthz` and `GET /readyz`

These are deliberately different. **`/healthz` never touches Redis** — it
answers "is this process wedged", and a liveness probe that checked a dependency
would turn a brief Redis outage into every scheduler pod restarting, an outage
of your own making. **`/readyz` does** ping Redis; failing it only removes the
pod from the Service.

### `GET /metrics`

Prometheus exposition. Also served by the worker on `-metrics-addr`
(default `:8081`).

## Configuration

Every flag has a `GOQUEUE_`-prefixed environment variable equivalent
(`-pool-size` → `GOQUEUE_POOL_SIZE`). Flags win.

### Shared

| Flag | Default | Purpose |
| --- | --- | --- |
| `-redis-addr` | *(empty)* | Redis address. **Empty means an in-process queue no other process can see.** |
| `-redis-prefix` | `goqueue` | key prefix, so environments can share one Redis |
| `-redis-connect-timeout` | `1m` | how long to retry the initial connection before giving up |
| `-log-level` | `info` | `debug`, `info`, `warn`, `error` |

### Scheduler

| Flag | Default | Purpose |
| --- | --- | --- |
| `-addr` | `:8080` | HTTP API listen address |
| `-shutdown-grace` | `15s` | time for in-flight requests to finish |
| `-ready-timeout` | `2s` | timeout for the readiness Redis ping |

### Worker

| Flag | Default | Purpose |
| --- | --- | --- |
| `-pool-size` | `10` | concurrent goroutines. Larger than GOMAXPROCS on purpose: jobs are assumed I/O-bound, where core count is the wrong bound. |
| `-job-timeout` | `30s` | maximum run time for one job |
| `-claim-ttl` | `1m` | how long a claim is honoured. **Must exceed `-job-timeout`**, or healthy long jobs get reclaimed and run twice. The binary warns if it does not. |
| `-claim-block` | `1s` | how long a worker waits for a job before looping; also bounds shutdown latency |
| `-reap-interval` | `15s` | how often expired claims are returned to the queue |
| `-depth-interval` | `5s` | how often queue depth is sampled |
| `-metrics-addr` | `:8081` | `/metrics` and `/healthz` |
| `-worker-id` | hostname | recorded on claims; the pod name in Kubernetes |
| `-demo-jobs` | `0` | seed N demo jobs at startup (local development only) |

## Metrics

All prefixed `goqueue_`, plus the standard Go runtime and process collectors.

| Metric | Type | Labels |
| --- | --- | --- |
| `jobs_processed_total` | counter | `type`, `outcome` |
| `job_duration_seconds` | histogram | `type` |
| `jobs_in_flight` | gauge | — |
| `pool_size` | gauge | — |
| `queue_depth` | gauge | `state` |

`outcome` is one of `success`, `failure`, `timeout`, `panic`, `no_handler`.
Metrics are labelled by job **type**, never job ID — one time series per ID
would grow without bound and exhaust Prometheus.

The duration histogram spans 10ms–197s (`ExponentialBuckets(0.01, 3, 10)`)
rather than Prometheus' default, which stops at 10s and would dump every
background job slower than that into `+Inf`.

> **`queue_depth` must be aggregated with `max()`, never `sum()`.** It is a
> property of the queue, but every worker samples the same Redis and reports the
> same number, so `sum()` multiplies the real backlog by the replica count —
> with six workers a backlog of 12 reads as 72. The recording rules in
> `deploy/k8s/monitoring.yaml` exist so dashboards and the autoscaler cannot get
> this wrong:
>
> - `goqueue:queue_depth:max`
> - `goqueue:jobs_processed:rate5m`
> - `goqueue:job_duration_seconds:p95`

## Local development

`make compose-up` starts only the *backing services*. The scheduler and worker
run on the host (`make run`, `make run-worker`) so a change is a `go run` away
rather than an image rebuild.

| Target | Purpose |
| --- | --- |
| `make build` | both binaries into `./bin` |
| `make test` | `go test -race -count=1 ./...` |
| `make lint` | golangci-lint |
| `make fmt` / `make vet` / `make tidy` | formatting, vet, module tidy |
| `make run` / `make run-worker` | run on the host against Compose |
| `make compose-up` / `compose-down` | Redis `:6379`, Prometheus `:9090`, Grafana `:3000` (admin/admin) |
| `make compose-ps` / `compose-logs` | status and logs |
| `make clean` | remove `./bin` and clear the test cache |

```bash
docker exec goqueue-redis redis-cli ping     # PONG
curl -s http://localhost:9090/-/healthy      # Prometheus Server is Healthy.
curl -s http://localhost:3000/api/health     # {"database":"ok", ...}
```

`compose-down` deletes the volumes, so queued jobs do not survive it.

## Kubernetes

kind runs Kubernetes nodes as Docker containers, so Docker must be running.

```bash
make kind-up        # 3-node cluster (1 control-plane, 2 workers)
make docker-build   # build goqueue-scheduler:dev and goqueue-worker:dev
make kind-load      # MANDATORY - see below
make k8s-apply      # namespace first, then everything in deploy/k8s/
make k8s-status
make kind-down
```

**`make kind-load` is not optional.** kind nodes have their own image store and
cannot see your host Docker daemon's images. Skip it and pods fail with
`ErrImagePull` even though `docker images` lists the image. This is also why
both Deployments set `imagePullPolicy: Never` — the images exist in no registry.

**Three nodes, not kind's single-node default**, so pod scheduling across nodes
is actually exercised; on one node everything is co-located by accident, which
hides affinity and topology mistakes.

### Host ports

`deploy/kind-config.yaml` forwards these, deliberately offset from the Compose
stack so both can run at once:

| Service | NodePort | Host |
| --- | --- | --- |
| scheduler API | 30080 | <http://localhost:8080> |
| Prometheus | 30090 | <http://localhost:19090> |
| Grafana | 30030 | <http://localhost:13000> |

Redis is a headless `ClusterIP`, deliberately not exposed. Reach it with
`make redis-cli` or `kubectl -n goqueue port-forward svc/redis 6379:6379`.

### What gets deployed

| Manifest | Objects |
| --- | --- |
| `redis.yaml` | StatefulSet (1 replica) + headless Service + 1Gi PVC |
| `scheduler.yaml` | Deployment (2 replicas) + NodePort Service |
| `worker.yaml` | Deployment (3 replicas) + metrics Service |
| `monitoring.yaml` | Prometheus + Grafana + RBAC + recording rules |
| `autoscaling.yaml` | prometheus-adapter + APIService + HPA |

**Redis is a StatefulSet, not a Deployment.** Only a StatefulSet gives it a
stable identity and a PVC that survives rescheduling. With a Deployment and an
`emptyDir`, a node drain silently discards every queued job.

**Redis runs with `--maxmemory-policy noeviction`.** The default
(`allkeys-lru`) silently deletes keys under memory pressure — correct for a
cache, catastrophic for a queue. `noeviction` makes Redis reject writes instead,
so the scheduler gets a visible error it can retry.

**Workers get `terminationGracePeriodSeconds: 60`.** The pool detaches a running
job from the shutdown signal and lets it finish, bounded by `-job-timeout`. A
shorter grace period would SIGKILL mid-job on every rolling update.

**Both app Deployments use a `startupProbe`.** Liveness and readiness are
suspended until it first succeeds, so a pod still waiting for Redis DNS on a
cold start is not mistaken for a wedged process.

### Monitoring in-cluster

`monitoring.yaml` uses `kubernetes_sd_configs`, not the static targets in
`deploy/prometheus.yml`. Worker pods come and go as the Deployment scales, and
a static target list would scrape dead IPs and miss new pods. Discovery is
annotation-driven — a pod opts in with `prometheus.io/scrape: "true"` — so a new
component needs annotations on its pod template and no change here.

Grafana comes up with the Prometheus datasource already provisioned.

### Autoscaling

Workers scale on **queue depth, not CPU**. A worker blocked on network I/O burns
almost no CPU while the backlog grows, so a CPU-based autoscaler sits at
`minReplicas` exactly when you need to scale out.

The HPA cannot read Prometheus directly. `prometheus-adapter` registers an
APIService for `external.metrics.k8s.io` and answers HPA queries by running
PromQL — against the `goqueue:queue_depth:max` recording rule, not the raw
gauge.

Target is `AverageValue: 5`, i.e. five pending jobs per worker, between 2 and 12
replicas. Scale-up is fast (15s stabilization); scale-down is slow (300s),
because retreating aggressively kills workers mid-job and thrashes on bursty
load.

```bash
kubectl -n goqueue get hpa worker
kubectl get --raw /apis/external.metrics.k8s.io/v1beta1 | python3 -m json.tool
```

Observed behaviour with 300 jobs submitted at once:

```
t=20s  pending=269  replicas=6   running=6
t=40s  pending=149  replicas=6   running=12   <- scaled out
t=60s  pending=0    replicas=12  running=12   <- drained
```

## Testing

```bash
make test                                     # -race, no cache
go test -race -run TestPoolDrains ./internal/worker
go test -race -count=20 ./internal/worker     # repeat to surface flaky concurrency
```

`-race` is on by default because the worker pool is the only genuinely
concurrent part of the system.

**The queue contract suite runs against both implementations.** `MemoryQueue`
and `RedisQueue` go through the same tests, so a behavioural difference is a
failure here rather than a surprise in production. The Redis half **skips** if
no Redis is reachable:

```bash
make compose-up                               # then the redis subtests run
GOQUEUE_TEST_REDIS=localhost:6379 go test -race ./internal/queue/
```

Running `make test` without Redis is not a full run — check for `SKIP` lines.

## Troubleshooting

**`ERROR: Docker is not running.`** `make kind-up` checks before doing anything.
Start Docker Desktop; kind nodes *are* Docker containers.

**`node(s) already exist for a cluster with the name "goqueue"`**
`kind get clusters`, then `make kind-down` and retry.

**`bind: address already in use` on cluster create.** kind forwards host ports
8080, 19090, 13000. Find the holder with `lsof -nP -iTCP:8080 -sTCP:LISTEN`.

**`mounts denied: ... is not shared from the host`** (Compose). The repository
is outside Docker Desktop's file-sharing allowlist. Move it under your home
directory.

**Pods in `ErrImagePull` for a `goqueue-*` image.** You forgot `make kind-load`.

**Pod stuck in `Pending`.** `kubectl -n goqueue describe pod <name>` and read
the Events. Usually the cluster cannot satisfy resource `requests` — Docker
Desktop's VM has a memory cap under Settings → Resources.

**Pod in `CrashLoopBackOff`.** `kubectl -n goqueue logs <name> --previous`. The
`--previous` matters: it shows the crashed container, not the one starting now.

**HPA shows `<unknown>` for the metric.** Check the APIService is healthy
(`kubectl get apiservice v1beta1.external.metrics.k8s.io`), that
prometheus-adapter is running, and that the recording rule has data
(`curl 'http://localhost:19090/api/v1/query?query=goqueue:queue_depth:max'`).

**Queue depth looks far too high.** You are using `sum()` on
`goqueue_queue_depth`. Use `max()` or the `goqueue:queue_depth:max` rule.

**Jobs run twice.** Expected under at-least-once delivery, but a *rising* rate
means `-claim-ttl` is shorter than real job durations and the reaper is stealing
live work. Handlers must be idempotent regardless.

**Everything is broken.** `make kind-down && make kind-up && make k8s-apply` —
under a minute once the node image is cached.

## Project status

Working end to end: jobs submitted over HTTP are executed by separate worker
processes through Redis, in a local Kubernetes cluster, with metrics and
queue-depth autoscaling.

### Done

- [x] Toolchain verified; module `github.com/anantraochinta/goqueue`
- [x] `Queue` interface with a documented contract, `MemoryQueue`, and
      `RedisQueue` (Lua scripts, reaper, retention TTL), checked by one shared
      contract suite
- [x] Worker pool: per-job timeout, panic recovery, exponential backoff with
      full jitter, dead-lettering, graceful drain
- [x] HTTP API with health/readiness split; 17 handler tests
- [x] Prometheus metrics on a private registry
- [x] Multi-stage Dockerfile; ~30MB distroless images
- [x] Kubernetes: StatefulSet Redis, scheduler and worker Deployments, probes
- [x] In-cluster Prometheus with service discovery, Grafana, recording rules
- [x] HorizontalPodAutoscaler on queue depth via prometheus-adapter

### Outstanding

- [ ] CI running `make test lint`, with a Redis service container so the
      contract suite's Redis half actually runs
- [ ] Grafana dashboards (the datasource is provisioned; no dashboards yet)
- [ ] Tests for `internal/metrics` and the `cmd/` packages
- [ ] `StateFailed` is reserved and currently set by nothing — it is intended
      for permanent, non-retryable handler failures
- [ ] `RedisQueue` targets a single Redis instance; Redis Cluster would need
      hash-tagged keys so Lua scripts stay within one slot
- [ ] Job cancellation (`DELETE /api/v1/jobs/{id}`) and a dead-letter
      inspection/requeue endpoint
- [ ] Prometheus in-cluster uses an `emptyDir`; a real deployment needs a PVC
