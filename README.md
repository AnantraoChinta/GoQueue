# GoQueue

A distributed job scheduler written in Go. GoQueue accepts units of work ("jobs")
submitted by clients, persists them to a Redis-backed queue so that no job is lost
if a worker dies, and hands them out to a pool of goroutine workers that execute
them concurrently. Workers run as containers, scheduled onto a local Kubernetes
cluster (kind) so that the system can be scaled horizontally and exercised in a
realistic multi-node environment, and every component exports Prometheus metrics
that are visualised in Grafana. The goal is a small but complete example of the
patterns a production job system needs: durable queueing, bounded concurrency,
horizontal scale-out, and operational visibility.

## Architecture

_Placeholder — to be written once the components exist._

Planned shape, at a glance:

| Component | Role |
| --- | --- |
| Producer / API | Accepts job submissions from clients |
| Redis | Durable queue and job state store |
| Worker pool | Goroutine pool that claims and executes jobs |
| Prometheus | Scrapes metrics from producers and workers |
| Grafana | Dashboards over the Prometheus data |
| kind | Local Kubernetes cluster hosting the above |

## Project Structure

```
goqueue/
├── cmd/                      Executables. One subdirectory per binary.
│   ├── scheduler/            HTTP API that accepts job submissions
│   └── worker/               Process that claims and executes jobs
├── internal/                 Private packages (see note below)
│   ├── api/                  HTTP handlers and routing
│   ├── metrics/              Prometheus collector declarations
│   ├── queue/                Redis-backed queue: the storage layer
│   └── worker/               Goroutine worker pool
├── deploy/
│   ├── docker-compose.yml    Redis + Prometheus + Grafana for local dev
│   ├── prometheus.yml        Scrape config, mounted into Prometheus
│   ├── kind-config.yaml      kind cluster definition (3 nodes)
│   └── k8s/                  Kubernetes manifests
│       ├── namespace.yaml    The `goqueue` namespace
│       ├── redis.yaml        Redis Deployment + Service (implemented)
│       ├── scheduler.yaml    placeholder
│       ├── worker.yaml       placeholder
│       └── monitoring.yaml   placeholder
├── Dockerfile                Multi-stage build for both binaries
├── Makefile                  Developer tasks; `make help` lists them
├── .golangci.yml             Lint configuration
└── .dockerignore             Keeps the build context small
```

### Why this layout

**`cmd/scheduler` and `cmd/worker` are separate binaries.** Each subdirectory of
`cmd/` is its own `package main`, so one module produces two executables. This is
what lets the scheduler and the worker scale independently in Kubernetes — you
typically want a couple of API replicas and many workers — while still sharing
the queue types, which matters because producer and consumer must agree exactly
on how a job is encoded. A single binary with a `--mode` flag would force them to
scale together and would ship the HTTP server into every worker pod.

**`internal/` is enforced by the Go compiler, not by convention.** Packages under
`internal/` can only be imported by code rooted at `internal/`'s parent
directory. Nobody who runs `go get github.com/anantraochinta/goqueue` can import
`internal/queue`, which means the queue's storage layout stays a private
implementation detail and can be changed without breaking anyone.

**`internal/queue` is the only package that knows the Redis key layout.** Both
the scheduler and the worker pool depend on the `Queue` interface rather than on
Redis directly. That keeps the storage decisions in one file and lets tests
substitute an in-memory fake instead of requiring a live Redis.

**`internal/metrics` is shared by both binaries deliberately.** Prometheus
aggregates time series by metric name and labels; if the scheduler called a
counter `jobs_total` and the worker called it `job_count`, you could never sum
across them. One package of declarations makes that drift impossible.

**`make k8s-apply` applies `namespace.yaml` explicitly before the directory.**
`kubectl apply -f <dir>/` gives no ordering guarantee across files, and every
other object declares `namespace: goqueue`, so the namespace has to exist first
or the apply fails. Doing it in two commands is clearer than encoding the
dependency in filenames.

## Prerequisites

Host this was set up and verified on: **macOS 26.6.2 (build 25G83), Apple Silicon
(arm64)**, shell `zsh`. Nothing here is macOS-specific apart from the install
commands; see "Installing on other platforms" below.

| Tool | Required | Installed version | Verified with |
| --- | --- | --- | --- |
| Go | **1.26.0+** | **1.27.1** (darwin/arm64) | `go version` |
| Docker | 20+ | **29.8.2** (Desktop 4.94.0) | `docker run hello-world` |
| Docker Compose | v2 | **v5.5.1** (bundled plugin) | `docker compose version` |
| kind | any recent | **0.33.0** (built with go1.27.0) | `kind version` |
| kubectl | any recent | **1.37.1** | `kubectl version --client` |
| make | 3.81+ | **3.81** (Apple-bundled GNU Make) | `make --version` |
| git | 2.x | **2.50.1** (Apple Git-155) | `git --version` |
| golangci-lint | 2.x | **2.14.0** (built with go1.27.1) | `golangci-lint --version` |

Supporting tooling on this host: Homebrew 7.0.6 (`/opt/homebrew`), Xcode Command
Line Tools at `/Library/Developer/CommandLineTools`.

### Notes on specific tools

- **Go 1.26.0 is the real floor, not 1.22.** The project started with
  `go 1.22` in `go.mod`, but `go-redis/v9 v9.23.0` declares `go 1.26.0` in its own
  manifest, and Go requires a module's `go` directive to be at least the maximum
  of all its dependencies'. Adding the dependency raised the floor automatically.
  To get back to 1.22, pin an older client
  (`go get github.com/redis/go-redis/v9@v9.7.0 && go mod tidy`); the tradeoff is
  an unmaintained dependency in exchange for supporting older toolchains.
- **`make` is GNU Make 3.81**, the version Apple ships with macOS. The Makefile is
  written to stay compatible with it: no `.ONESHELL`, no `$(file ...)`, no
  `.RECIPEPREFIX`. If you need those, `brew install make` provides GNU Make 4.x as
  `gmake` without touching `/usr/bin/make`.
- **Project location matters on macOS.** Docker Desktop only bind-mounts host
  paths on its file-sharing allowlist, which by default covers `/Users`,
  `/Volumes`, `/private`, `/tmp`, and `/var/folders` — but **not**
  `/Applications`. `deploy/docker-compose.yml` bind-mounts `prometheus.yml`, so
  keep the repository somewhere under your home directory. This one is from
  experience: the project originally lived in `/Applications/goQueue` and
  Prometheus failed to start with `mounts denied: ... is not shared from the
  host`. Redis was unaffected because it uses a named volume, not a bind mount.
- **Module path casing.** The module is `github.com/anantraochinta/goqueue`
  (lowercase), while the GitHub repository is `github.com/AnantraoChinta/GoQueue`.
  Harmless for local development and for `go build`, because the module path is
  only resolved over the network when someone imports this package. GitHub
  redirects case-insensitively, so `go get` generally still works; but Go module
  paths *are* case-sensitive, so if this is ever published as a library, rename
  the repository to lowercase or change the module path to match it exactly.

## Setup

### 1. Install the toolchain

Install Homebrew if you do not have it:

```bash
/bin/bash -c "$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/HEAD/install.sh)"
```

Then:

```bash
brew update
brew install go kind kubectl golangci-lint
brew install --cask docker-desktop   # prompts for your password; run interactively
```

`make` and `git` ship with the Xcode Command Line Tools. If `git --version`
fails, run `xcode-select --install`.

### 2. Start the Docker daemon

Launch Docker Desktop once (`open -a Docker`) and wait for the whale icon in the
menu bar to stop animating.

### 3. Verify every tool

```bash
go version                  # expect go1.26.0 or newer
docker run hello-world      # expect "Hello from Docker!"
docker compose version
kind version
kubectl version --client
make --version
git --version
golangci-lint --version
```

### 4. Clone and build

Clone into your **home directory**, not `/Applications` — see the file-sharing
note under Prerequisites.

```bash
mkdir -p ~/Developer && cd ~/Developer
git clone https://github.com/AnantraoChinta/GoQueue.git goqueue
cd goqueue
go build ./...
```

### 5. Makefile targets

`make help` prints this list at any time.

#### Go development

| Target | What it does |
| --- | --- |
| `make build` | Compiles both binaries into `./bin/scheduler` and `./bin/worker`. |
| `make test` | `go test -race -count=1 ./...`. The race detector is on by default because a worker pool is exactly the kind of code that hides data races. `-count=1` disables Go's test result cache so a run always actually runs. |
| `make lint` | Runs `golangci-lint` using `.golangci.yml`. |
| `make fmt` | `gofmt -s -w .` over the module. |
| `make vet` | `go vet ./...`. |
| `make tidy` | `go mod tidy`, syncing `go.mod`/`go.sum` with the actual imports. |
| `make clean` | Deletes `./bin` and clears the test cache. |

#### Running locally

| Target | What it does |
| --- | --- |
| `make run` | `go run ./cmd/scheduler`. Expects the Compose stack to be up. |
| `make run-worker` | `go run ./cmd/worker`. Run in a second terminal. |

#### Local dependencies (Docker Compose)

The Compose stack runs only the *backing services*. The scheduler and worker are
meant to run on the host so you get fast rebuilds without a container round-trip.

| Target | What it does |
| --- | --- |
| `make compose-up` | Starts Redis (`:6379`), Prometheus (`:9090`), Grafana (`:3000`, admin/admin) in the background. |
| `make compose-down` | Stops and removes the containers **and their volumes**. Queued jobs do not survive this. |
| `make compose-ps` | Shows container status. |
| `make compose-logs` | Tails logs from all three services. |

Check the stack is healthy:

```bash
docker exec goqueue-redis redis-cli ping     # PONG
curl -s http://localhost:9090/-/healthy      # Prometheus Server is Healthy.
curl -s http://localhost:3000/api/health     # {"database":"ok", ...}
```

The `goqueue-scheduler` and `goqueue-worker` targets at
<http://localhost:9090/targets> will show as **DOWN** until those binaries exist
and expose `/metrics`. That is expected at this stage.

#### Containers

| Target | What it does |
| --- | --- |
| `make docker-build` | Builds `goqueue-scheduler:dev` and `goqueue-worker:dev` from the multi-stage `Dockerfile`. Override the tag with `make docker-build IMAGE_TAG=v0.1.0`. |

#### Kubernetes (kind)

See the [Kubernetes](#kubernetes) section below.

### Installing on other platforms

- **Linux:** install Go from <https://go.dev/dl/>, Docker Engine via your
  distribution's package manager, and `kind`, `kubectl`, and `golangci-lint` from
  their official release binaries. `make` and `git` come from your package
  manager. In `deploy/docker-compose.yml`, add
  `extra_hosts: ["host.docker.internal:host-gateway"]` to the `prometheus`
  service — that hostname is a Docker Desktop feature and does not resolve on
  Linux by default.
- **Windows:** use WSL2 with Docker Desktop's WSL2 backend, then follow the Linux
  instructions inside your WSL distribution. Keep the repository on the WSL
  filesystem (`~/...`), not under `/mnt/c`, or file I/O will be very slow.

## Kubernetes

GoQueue runs on a local [kind](https://kind.sigs.k8s.io/) cluster — Kubernetes
nodes running as Docker containers on your machine. Docker must be running
before any of this works.

Currently only Redis is deployed. The scheduler and worker manifests are
commented placeholders, because there are no binaries to run yet.

### Create the cluster

```bash
make kind-up
```

This reads `deploy/kind-config.yaml` and creates a **3-node** cluster named
`goqueue`: one control-plane node and two workers. Three nodes rather than
kind's single-node default, so that pod scheduling across nodes is actually
exercised — on one node every pod is co-located by accident, which hides
affinity and topology mistakes until you deploy somewhere real.

`kind-up` also points your kubectl context at the new cluster
(`kind-goqueue`) and prints the nodes. Takes roughly 45 seconds, plus a
one-time ~1 GB pull of the node image on first run.

Nodes report `NotReady` for the first few seconds while the CNI network plugin
starts. That is normal. To block until they are usable:

```bash
kubectl wait --for=condition=Ready nodes --all --timeout=180s
```

### Deploy

```bash
make k8s-apply
```

This applies `deploy/k8s/namespace.yaml` first, then the whole `deploy/k8s/`
directory. The namespace must exist before anything inside it, and
`kubectl apply -f <dir>/` gives no ordering guarantee across files.

The scheduler, worker, and monitoring manifests contain only comments, so
kubectl skips them silently — no error.

### Check status

```bash
kubectl -n goqueue get pods          # the direct check
make k8s-status                      # pods + services, with node and IP
make k8s-logs                        # tail the Redis logs
```

A healthy result looks like:

```
NAME                     READY   STATUS    RESTARTS   AGE   IP           NODE
redis-6556c894cd-f48gv   1/1     Running   0          20s   10.244.2.2   goqueue-worker
```

`READY 1/1` means the readiness probe is passing, not merely that the
container started. `NODE` showing a worker rather than `goqueue-control-plane`
confirms the multi-node scheduling is real.

### Confirm Redis responds

```bash
make redis-ping
```

That launches a throwaway pod and runs `redis-cli -h redis ping` against the
Service. It should print `PONG`. It is a genuine end-to-end test: cluster DNS
resolved the Service name, the Service found the pod through its label
selector, and Redis answered.

For an interactive session against the running instance:

```bash
make redis-cli                       # exec into the pod
kubectl -n goqueue port-forward svc/redis 6379:6379   # or reach it from the host
```

Redis is a `ClusterIP` Service, deliberately not exposed outside the cluster.
In-cluster clients address it as `redis:6379` from the same namespace, or
`redis.goqueue.svc.cluster.local:6379` from anywhere.

### Tear down

```bash
make k8s-delete    # remove the objects, keep the cluster
make kind-down     # delete the whole cluster
```

`kind-down` removes all three node containers and frees the forwarded host
ports. Nothing persists: the Redis pod uses an `emptyDir` volume, so every
queued job is discarded.

### Once there are binaries to deploy

```bash
make docker-build && make kind-load && make k8s-apply
```

`make kind-load` is not optional. kind nodes have their own container image
store and **cannot see images in your host's Docker daemon**. Skip it and pods
fail with `ErrImagePull` even though `docker images` lists the image on your
machine.

### Troubleshooting

**`ERROR: Docker is not running.`**
`make kind-up` checks for this before doing anything. Start Docker Desktop
(`open -a Docker`) and wait for the whale icon to stop animating. kind nodes
*are* Docker containers, so no daemon means no cluster.

**`failed to create cluster: node(s) already exist for a cluster with the name "goqueue"`**
A previous cluster was never deleted. `kind get clusters` to confirm, then
`make kind-down` and retry.

**Port conflict on create: `bind: address already in use`**
`deploy/kind-config.yaml` forwards host ports **8080**, **19090**, and
**13000**. Find the holder with `lsof -nP -iTCP:8080 -sTCP:LISTEN`, then either
stop it or change `hostPort` in the config. Port 8080 is the usual culprit.
The Compose stack uses 6379/9090/3000 and deliberately does not overlap, so
both can run simultaneously.

**Pod stuck in `Pending`**
`kubectl -n goqueue describe pod <name>` and read the Events at the bottom.
Usually the cluster cannot satisfy the resource `requests` — Docker Desktop's
VM has a memory cap (Settings → Resources) and three nodes plus your workload
have to fit inside it.

**Pod stuck in `ContainerCreating`**
Usually still pulling the image. `kubectl -n goqueue describe pod <name>` will
say. If it mentions `ErrImagePull` for a `goqueue-*` image, you forgot
`make kind-load`.

**Pod in `CrashLoopBackOff`**
`kubectl -n goqueue logs <name> --previous` — the `--previous` flag is the
important part, since it shows the crashed container's output rather than the
one currently starting.

**`connection refused` from kubectl**
Your context is pointing at a cluster that no longer exists.
`kubectl config current-context` should print `kind-goqueue`; fix with
`kubectl config use-context kind-goqueue`, or recreate with `make kind-up`.

**`namespaces "goqueue" not found`**
The namespace was deleted, or you ran `kubectl apply -f deploy/k8s/` by hand
instead of `make k8s-apply`. Run `make k8s-apply`, which creates it first.

**Everything is broken and you want a clean slate**
```bash
make kind-down && make kind-up && make k8s-apply
```
Cheap — under a minute once the node image is cached.

## Project Status

**Current phase: environment setup and scaffolding complete. No scheduler logic
has been implemented** — every Go file is a placeholder containing package
documentation, type declarations, and TODO comments.

### Done

- [x] Toolchain installed and verified: Go 1.27.1, Docker 29.8.2, kind 0.33.0,
      kubectl 1.37.1, make 3.81, git 2.50.1, golangci-lint 2.14.0
- [x] Go module `github.com/anantraochinta/goqueue`, dependencies added
      (`go-redis/v9 v9.23.0`, `client_golang v1.24.1`) and tidied
- [x] Package skeleton under `cmd/` and `internal/`; `go build ./...`,
      `go vet ./...`, and `make lint` all pass clean
- [x] Multi-stage `Dockerfile` (distroless runtime, static binary) and
      `.dockerignore`
- [x] `Makefile` with build/test/lint/run, Compose, and kind targets
- [x] `deploy/docker-compose.yml` — Redis, Prometheus, Grafana; verified end to
      end: `redis-cli ping` → `PONG`, Prometheus healthy on `:9090` serving our
      scrape config, Grafana healthy on `:3000`
- [x] `deploy/prometheus.yml` scrape config
- [x] `deploy/kind-config.yaml` — 3-node cluster (1 control-plane, 2 workers);
      `make kind-up` / `make kind-down` verified
- [x] `deploy/k8s/redis.yaml` — Redis Deployment + Service, verified on kind:
      pod `Running 1/1` on a worker node, `make redis-ping` → `PONG`,
      LPUSH/RPOP roundtrip over the Service FQDN
- [x] `.golangci.yml` (golangci-lint 2.x schema), passing with 0 issues
- [x] Git repository with a Go-appropriate `.gitignore`

### Outstanding

- [ ] Fill in the Architecture section
- [ ] `internal/queue` — choose Redis Streams vs. list + in-flight set, then
      implement `Enqueue`, `Claim`, `Ack`, `Nack`
- [ ] `internal/worker` — goroutine pool, per-job timeout, panic recovery,
      retry with backoff, dead-letter queue, graceful drain
- [ ] `internal/api` — job submission, status, health and readiness endpoints
- [ ] `internal/metrics` — construct and register the collectors
- [ ] Flesh out the `scheduler.yaml`, `worker.yaml`, and `monitoring.yaml`
      placeholders into real manifests
- [ ] Replace the Redis `Deployment` + `emptyDir` with a `StatefulSet` +
      `volumeClaimTemplate`. As it stands the queue is wiped whenever the pod
      is rescheduled, which contradicts the durability guarantee — fine for
      local development, wrong anywhere else
- [ ] Prometheus `kubernetes_sd_configs` for in-cluster service discovery
- [ ] Grafana dashboard provisioning
- [ ] Horizontal Pod Autoscaler driven by queue depth
- [ ] Tests, and CI to run `make test lint`
- [ ] Remove the temporary `unused` exclusion from `.golangci.yml` once
      `internal/queue` is implemented
