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

## Prerequisites

Host this was set up and verified on: **macOS 26.6.2 (build 25G83), Apple Silicon
(arm64)**, shell `zsh`. Nothing here is macOS-specific apart from the install
commands; see "Installing on other platforms" below.

| Tool | Required | Installed version | Verified with |
| --- | --- | --- | --- |
| Go | 1.22+ | **1.27.1** (darwin/arm64) | `go version` |
| Docker | any recent | **not yet installed** — see note below | `docker run hello-world` |
| kind | any recent | **0.33.0** (built with go1.27.0) | `kind version` |
| kubectl | any recent | **1.37.1** | `kubectl version --client` |
| make | 3.81+ | **3.81** (Apple-bundled GNU Make) | `make --version` |
| git | 2.x | **2.50.1** (Apple Git-155) | `git --version` |
| golangci-lint | 2.x | **2.14.0** (built with go1.27.1) | `golangci-lint --version` |

Supporting tooling on this host: Homebrew 7.0.6 (`/opt/homebrew`), Xcode Command
Line Tools at `/Library/Developer/CommandLineTools`.

> **Docker is not installed yet.** `brew install --cask docker-desktop` needs an
> administrator password to create its symlinks under `/usr/local/bin`, so it has
> to be run from an interactive terminal. See "Setup" step 2.

### Notes on specific tools

- **`make` is GNU Make 3.81**, the version Apple ships with macOS. It is fine for
  an ordinary Makefile but predates `.ONESHELL`, `$(file ...)`, and other GNU Make
  4.x features. If a future Makefile needs them, install a modern one with
  `brew install make` and invoke it as `gmake`; this leaves `/usr/bin/make` alone.
- **Go toolchain version.** `go.mod` declares `go 1.22`, the project's supported
  floor, rather than the 1.27.1 that happens to be installed here. Anyone on Go
  1.22 or newer can build the module.
- **Module path casing.** The module is `github.com/anantraochinta/goqueue`
  (lowercase), while the GitHub repository is `github.com/AnantraoChinta/GoQueue`.
  This is harmless for local development and for `go build`, because the module
  path is only resolved over the network when someone imports this package. GitHub
  redirects case-insensitively, so `go get` generally still works; but Go module
  paths *are* case-sensitive, so if this is ever published as a library, rename the
  repository to lowercase or change the module path to match the repository exactly.

## Setup

Reproducing this environment from scratch on macOS (Apple Silicon or Intel):

**1. Install Homebrew**, if you do not have it:

```bash
/bin/bash -c "$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/HEAD/install.sh)"
```

**2. Install the toolchain:**

```bash
brew update
brew install go kind kubectl golangci-lint
brew install --cask docker-desktop   # run from an interactive terminal; prompts for your password
```

`make` and `git` ship with the Xcode Command Line Tools. If `git --version`
fails, run `xcode-select --install`.

**3. Start the Docker daemon.** Launch Docker Desktop once from Applications (or
`open -a Docker`) and wait for the whale icon in the menu bar to stop animating.

**4. Verify every tool:**

```bash
go version                  # expect go1.22 or newer
docker run hello-world      # expect "Hello from Docker!"
kind version
kubectl version --client
make --version
git --version
golangci-lint --version
```

**5. Clone and build:**

```bash
git clone https://github.com/AnantraoChinta/GoQueue.git
cd GoQueue
go build ./...
```

### Installing on other platforms

- **Linux:** install Go from <https://go.dev/dl/>, Docker Engine via your
  distribution's package manager, and `kind`, `kubectl`, and `golangci-lint` from
  their official release binaries. `make` and `git` come from your package manager.
- **Windows:** use WSL2 with Docker Desktop's WSL2 backend, then follow the Linux
  instructions inside your WSL distribution.

## Project Status

**Current phase: environment setup and scaffolding.** No scheduler logic has been
written yet.

Done:

- [x] Toolchain installed and verified: Go, kind, kubectl, golangci-lint, make, git
- [x] Go module initialised as `github.com/anantraochinta/goqueue`
- [x] Go-appropriate `.gitignore`
- [x] Local git repository with an initial commit

Outstanding:

- [ ] Install Docker Desktop and verify with `docker run hello-world`
- [ ] Fill in the Architecture section
- [ ] Redis-backed queue
- [ ] Goroutine worker pool
- [ ] Dockerfile and container build
- [ ] kind cluster config and Kubernetes manifests
- [ ] Prometheus metrics and Grafana dashboards
- [ ] Makefile
- [ ] golangci-lint configuration
- [ ] Tests and CI
