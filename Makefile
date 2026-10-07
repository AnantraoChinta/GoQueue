# GoQueue developer tasks.
#
# Written for GNU Make 3.81 (the version macOS ships), so no .ONESHELL and
# no $(file ...). Run `make help` for the target list.

SHELL := /bin/bash

MODULE      := github.com/anantraochinta/goqueue
BIN_DIR     := bin
COMPOSE     := docker compose -f deploy/docker-compose.yml
KIND_CLUSTER:= goqueue
KIND_CONFIG := deploy/kind-config.yaml

# Image coordinates. Override on the command line:
#   make docker-build IMAGE_TAG=v0.1.0
IMAGE_PREFIX ?= goqueue
IMAGE_TAG    ?= dev

.DEFAULT_GOAL := help

## help: list the available targets
help:
	@echo "GoQueue targets:"
	@grep -E '^## ' $(MAKEFILE_LIST) | sed -e 's/^## /  /' | awk -F': ' '{printf "  %-16s %s\n", $$1, $$2}'

## build: compile both binaries into ./bin
build:
	@mkdir -p $(BIN_DIR)
	go build -trimpath -o $(BIN_DIR)/scheduler ./cmd/scheduler
	go build -trimpath -o $(BIN_DIR)/worker    ./cmd/worker
	@echo "built: $(BIN_DIR)/scheduler $(BIN_DIR)/worker"

## test: run the unit tests with the race detector
test:
	go test -race -count=1 ./...

## lint: run golangci-lint over the whole module
lint:
	golangci-lint run ./...

## fmt: format all Go source
fmt:
	gofmt -s -w .

## vet: run go vet
vet:
	go vet ./...

## tidy: sync go.mod/go.sum with the imports in the source
tidy:
	go mod tidy

## run: run the scheduler against the Compose stack
run:
	go run ./cmd/scheduler

## run-worker: run a single worker against the Compose stack
run-worker:
	go run ./cmd/worker

## compose-up: start Redis, Prometheus and Grafana for local development
compose-up:
	$(COMPOSE) up -d
	@echo ""
	@echo "  Redis       localhost:6379"
	@echo "  Prometheus  http://localhost:9090"
	@echo "  Grafana     http://localhost:3000  (admin/admin)"

## compose-down: stop the local stack and delete its volumes
compose-down:
	$(COMPOSE) down --volumes --remove-orphans

## compose-logs: tail logs from the local stack
compose-logs:
	$(COMPOSE) logs -f

## compose-ps: show the status of the local stack
compose-ps:
	$(COMPOSE) ps

## docker-build: build the scheduler and worker container images
docker-build:
	docker build --build-arg CMD=scheduler -t $(IMAGE_PREFIX)-scheduler:$(IMAGE_TAG) .
	docker build --build-arg CMD=worker    -t $(IMAGE_PREFIX)-worker:$(IMAGE_TAG)    .

## kind-up: create the local Kubernetes cluster
kind-up:
	@docker info >/dev/null 2>&1 || { echo "ERROR: Docker is not running. Start Docker Desktop first."; exit 1; }
	kind create cluster --name $(KIND_CLUSTER) --config $(KIND_CONFIG)
	kubectl cluster-info --context kind-$(KIND_CLUSTER)
	kubectl get nodes

## kind-down: delete the local Kubernetes cluster
kind-down:
	kind delete cluster --name $(KIND_CLUSTER)

## kind-load: push the locally built images into the kind cluster
kind-load:
	kind load docker-image $(IMAGE_PREFIX)-scheduler:$(IMAGE_TAG) --name $(KIND_CLUSTER)
	kind load docker-image $(IMAGE_PREFIX)-worker:$(IMAGE_TAG)    --name $(KIND_CLUSTER)

## k8s-apply: apply the Kubernetes manifests
k8s-apply:
	# The namespace must exist before anything is created inside it, and
	# `kubectl apply -f <dir>` gives no ordering guarantee across files.
	kubectl apply -f deploy/k8s/namespace.yaml
	kubectl apply -f deploy/k8s/
	kubectl -n goqueue get all

## k8s-status: show what is running in the goqueue namespace
k8s-status:
	kubectl -n goqueue get pods,svc -o wide

## k8s-logs: tail the Redis logs
k8s-logs:
	kubectl -n goqueue logs -l app.kubernetes.io/name=redis -f

## redis-ping: ping Redis from inside the cluster
redis-ping:
	kubectl -n goqueue run redis-ping-$$$$ --rm -i --restart=Never \
	  --image=redis:8-alpine --command -- redis-cli -h redis ping

## redis-cli: open an interactive redis-cli inside the cluster
redis-cli:
	kubectl -n goqueue exec -it deploy/redis -- redis-cli

## k8s-delete: remove the Kubernetes manifests
k8s-delete:
	kubectl delete -f deploy/k8s/ --ignore-not-found

## clean: remove build artifacts
clean:
	rm -rf $(BIN_DIR)
	go clean -testcache

.PHONY: help build test lint fmt vet tidy run run-worker \
        compose-up compose-down compose-logs compose-ps \
        docker-build kind-up kind-down kind-load \
        k8s-apply k8s-delete k8s-status k8s-logs redis-ping redis-cli clean
