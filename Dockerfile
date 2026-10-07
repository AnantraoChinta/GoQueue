# Multi-stage build for the GoQueue binaries.
#
# One Dockerfile builds both commands; pick which with the CMD build arg:
#   docker build --build-arg CMD=scheduler -t goqueue-scheduler .
#   docker build --build-arg CMD=worker    -t goqueue-worker    .

# ---- build stage -----------------------------------------------------------
ARG GO_VERSION=1.27

FROM golang:${GO_VERSION}-alpine AS build

WORKDIR /src

# Copy the manifests first and download modules as their own layer. Docker
# caches this layer and only invalidates it when go.mod/go.sum change, so
# ordinary source edits do not re-download the dependency graph.
COPY go.mod go.sum ./
RUN go mod download

COPY . .

ARG CMD=scheduler
# CGO_ENABLED=0 produces a static binary with no libc dependency, which is
# what lets the runtime stage be a scratch-like image.
# -trimpath strips local filesystem paths; -s -w drop the symbol table and
# DWARF debug info, cutting binary size substantially.
RUN CGO_ENABLED=0 GOOS=linux go build \
      -trimpath \
      -ldflags="-s -w" \
      -o /out/goqueue \
      ./cmd/${CMD}

# ---- runtime stage ---------------------------------------------------------
# distroless/static has no shell and no package manager: a much smaller
# attack surface than alpine, and nothing for an attacker to pivot with.
# Trade-off: you cannot `docker exec` into it to debug. Use the :debug tag
# temporarily if you need a shell.
FROM gcr.io/distroless/static-debian12:nonroot AS runtime

COPY --from=build /out/goqueue /usr/local/bin/goqueue

# The nonroot tag runs as uid 65532. Kubernetes can additionally enforce
# this with runAsNonRoot: true in the pod security context.
USER nonroot:nonroot

# Scheduler HTTP API / metrics. The worker overrides this in its manifest.
EXPOSE 8080

ENTRYPOINT ["/usr/local/bin/goqueue"]
