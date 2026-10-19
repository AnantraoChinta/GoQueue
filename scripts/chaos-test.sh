#!/usr/bin/env bash
#
# Fault injection: kill worker pods mid-run and prove no job is lost.
#
# This is the test behind GoQueue's durability claim. A worker that is
# killed while holding a job never acks it. The job sits in the inflight
# set until its claim expires, at which point the reaper returns it to
# pending and another worker picks it up.
#
# Without the reaper, killed-mid-job work is stranded forever.
#
# Usage:
#   make kind-up && make docker-build && make kind-load && make k8s-apply
#   ./scripts/chaos-test.sh [JOB_COUNT] [KILL_ROUNDS]

set -euo pipefail

NS=goqueue
API=${API:-http://localhost:8080}
JOBS=${1:-60}
ROUNDS=${2:-3}
# Jobs must be LONGER than the worker can drain in, otherwise a killed pod
# finishes its in-flight work gracefully and the reaper is never exercised.
# With --grace-period=0 the container is SIGKILLed, so a job this long is
# guaranteed to be interrupted mid-execution.
DURATION=${DURATION:-25s}

red()   { printf "\033[31m%s\033[0m\n" "$*"; }
green() { printf "\033[32m%s\033[0m\n" "$*"; }
info()  { printf "  %s\n" "$*"; }

command -v kubectl >/dev/null || { red "kubectl not found"; exit 1; }
kubectl -n "$NS" get deployment/worker >/dev/null 2>&1 || {
  red "worker deployment not found; run 'make k8s-apply' first"; exit 1; }
curl -fsS -o /dev/null "$API/healthz" || {
  red "scheduler not reachable at $API"; exit 1; }

echo "=== Fault injection: $JOBS jobs, $ROUNDS kill rounds ==="

# --- submit ---------------------------------------------------------------
info "submitting $JOBS jobs of ${DURATION} each..."
ids=()
for i in $(seq 1 "$JOBS"); do
  id="chaos-$(date +%s)-$i"
  curl -fsS -o /dev/null -X POST "$API/api/v1/jobs" \
    -H 'Content-Type: application/json' \
    -d "{\"type\":\"sleep\",\"id\":\"$id\",\"max_attempts\":5,\"payload\":{\"duration\":\"$DURATION\"}}"
  ids+=("$id")
done
green "  submitted ${#ids[@]} jobs"

# --- kill -----------------------------------------------------------------
# max_attempts is 5 so a job can survive several redeliveries. Each round
# deletes one pod WHILE it is running jobs.
killed=0
for r in $(seq 1 "$ROUNDS"); do
  sleep 4
  pod=$(kubectl -n "$NS" get pods -l app.kubernetes.io/name=worker \
          --field-selector=status.phase=Running \
          -o jsonpath='{.items[0].metadata.name}' 2>/dev/null || true)
  [ -z "$pod" ] && { info "round $r: no running worker to kill"; continue; }

  inflight=$(kubectl -n "$NS" exec statefulset/redis -- \
               redis-cli ZCARD goqueue:inflight 2>/dev/null | tr -d '\r')
  info "round $r: killing $pod (inflight=$inflight)"
  # --grace-period=0 --force is SIGKILL: no graceful drain, no chance to
  # ack. This is the failure the reaper exists for.
  kubectl -n "$NS" delete pod "$pod" --grace-period=0 --force >/dev/null 2>&1
  killed=$((killed+1))
done
green "  hard-killed $killed worker pods mid-run"

# --- wait for the queue to settle ----------------------------------------
info "waiting for the queue to drain (reaper interval is 15s)..."
deadline=$(( $(date +%s) + 300 ))
while [ "$(date +%s)" -lt "$deadline" ]; do
  pending=$(kubectl -n "$NS" exec statefulset/redis -- redis-cli ZCARD goqueue:pending 2>/dev/null | tr -d '\r')
  inflight=$(kubectl -n "$NS" exec statefulset/redis -- redis-cli ZCARD goqueue:inflight 2>/dev/null | tr -d '\r')
  [ "${pending:-1}" = "0" ] && [ "${inflight:-1}" = "0" ] && break
  sleep 5
done

# --- verify ---------------------------------------------------------------
echo
echo "=== Result ==="
lost=0; succeeded=0; dead=0; other=0
for id in "${ids[@]}"; do
  state=$(curl -fsS "$API/api/v1/jobs/$id" 2>/dev/null \
          | python3 -c 'import sys,json;print(json.load(sys.stdin)["state"])' 2>/dev/null || echo "MISSING")
  case "$state" in
    succeeded) succeeded=$((succeeded+1)) ;;
    dead)      dead=$((dead+1)) ;;
    MISSING)   lost=$((lost+1)); red "  LOST: $id" ;;
    *)         other=$((other+1)); red "  STUCK in $state: $id" ;;
  esac
done

redeliveries=$(kubectl -n "$NS" exec statefulset/redis -- sh -c '
  n=0
  for k in $(redis-cli --scan --pattern "goqueue:job:chaos-*"); do
    a=$(redis-cli HGET "$k" attempts)
    [ "${a:-1}" -gt 1 ] && n=$((n+1))
  done
  echo $n' 2>/dev/null | tr -d '\r')

info "submitted:    ${#ids[@]}"
info "succeeded:    $succeeded"
info "dead:         $dead"
info "lost:         $lost"
info "stuck:        $other"
info "redelivered:  ${redeliveries:-?}  (attempts > 1, i.e. survived a pod kill)"
echo

if [ "$lost" -ne 0 ] || [ "$other" -ne 0 ]; then
  red "FAIL: $lost job(s) lost, $other stuck after $killed pod kills"
  exit 1
fi
if [ "$succeeded" -ne "${#ids[@]}" ]; then
  red "FAIL: only $succeeded/${#ids[@]} jobs succeeded"
  exit 1
fi
if [ "${redeliveries:-0}" -eq 0 ]; then
  red "INCONCLUSIVE: all jobs completed, but none were redelivered."
  red "  No job was interrupted mid-execution, so the reaper was never exercised."
  red "  Increase DURATION so jobs outlast the drain window, then re-run."
  exit 1
fi
green "PASS: all ${#ids[@]} jobs completed despite $killed hard pod kills"
green "      $redeliveries job(s) were orphaned by a kill and recovered by the reaper"
