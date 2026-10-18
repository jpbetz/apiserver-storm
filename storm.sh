#!/usr/bin/env bash
# storm.sh <run-name> [feature-gates]
#
# Relist-storm experiment against a local kube-apiserver:
#   boot etcd+apiserver -> seed configmaps -> start watch herd -> baseline
#   -> STORM 1: kill -9 apiserver, restart (watch re-establishment + cache re-warm)
#   -> STORM 2: concurrent LIST burst, rv=0 (cache-served)
#   -> STORM 3: concurrent LIST burst, rv="" (consistent read)
#   -> STORM 4: concurrent LIST burst, label selector (full filter, tiny response)
# Sampling: 1s /metrics + /proc (RSS/PSS/Referenced-WSS, CPU); pprof heap grabs.
#
# Env knobs: N_OBJECTS(15000) OBJ_SIZE(4096) WATCHERS(100) BURST_CONC(40) BURST_COUNT(80)
set -euo pipefail

NAME=${1:?run name}
GATES=${2:-}

HARNESS_DIR="$(cd "$(dirname "$0")" && pwd)"
KUBE_ROOT=${KUBE_ROOT:-/home/jpbetz/projects/kubernetes}
# kube-apiserver's certs and kubeconfig come from qas (~/projects/bin): run
# `qas up` once (and `qas down`) so they exist.
QA_DIR=$(qas env QAS_DIR)
SERVER=https://127.0.0.1:6443
N_OBJECTS=${N_OBJECTS:-15000}
OBJ_SIZE=${OBJ_SIZE:-4096}
WATCHERS=${WATCHERS:-100}
BURST_CONC=${BURST_CONC:-40}
BURST_COUNT=${BURST_COUNT:-80}
RESOURCE=${RESOURCE:-configmaps}
BURST_ACCEPTS=${BURST_ACCEPTS:-json}
WATCHLIST=${WATCHLIST:-}
# The token qas (the default, and arms.tsv's) means the kube-apiserver qas built.
APISERVER_BIN=${APISERVER_BIN:-qas}
if [[ $APISERVER_BIN == qas ]]; then APISERVER_BIN=$(qas env QAS_APISERVER_BIN); fi
SEED=${SEED:-42}
CHURN_QPS=${CHURN_QPS:-}

TS=$(date +%Y%m%d-%H%M%S)
RUNDIR="$HARNESS_DIR/runs/$NAME-$TS"
mkdir -p "$RUNDIR/pprof" "$RUNDIR/metrics"

# Snapshot harness scripts into the rundir (never run scripts that might be edited mid-run).
cp "$HARNESS_DIR/storm.sh" "$HARNESS_DIR/sampler.sh" "$HARNESS_DIR/analyze.py" "$HARNESS_DIR/stormtool.go" "$RUNDIR/"
STORMTOOL="$HARNESS_DIR/stormtool"

# Client cert: qas's admin user (system:masters), the same one its kubeconfig embeds.
CERT="$RUNDIR/client.crt"; KEY="$RUNDIR/client.key"
cp "$QA_DIR/certs/admin.crt" "$CERT"
cp "$QA_DIR/certs/admin.key" "$KEY"

CERTS="$QA_DIR/certs"
APISERVER_ARGS=(
  --v=2
  --runtime-config=api/all=true
  --etcd-servers=http://localhost:2379
  --service-account-key-file="$CERTS/service-account-key.pem"
  --service-account-signing-key-file="$CERTS/service-account-key.pem"
  --service-account-issuer=api
  --tls-cert-file="$CERTS/server.crt"
  --tls-private-key-file="$CERTS/server.key"
  --client-ca-file="$CERTS/ca.crt"
)
if [[ -n "$GATES" ]]; then
  APISERVER_ARGS+=(--feature-gates="$GATES")
fi

phase() {
  echo "$(date +%s%3N),$1" >> "$RUNDIR/phases.csv"
  echo ">>> phase: $1"
  local pid
  pid=$(cat "$RUNDIR/apiserver.pid" 2>/dev/null || true)
  if [[ -n "$pid" && -e "/proc/$pid" ]]; then
    echo 4 > "/proc/$pid/clear_refs" 2>/dev/null || true   # reset Referenced => per-phase WSS
  fi
}

grab_heap() {
  curl -sk --max-time 10 --cert "$CERT" --key "$KEY" \
    "$SERVER/debug/pprof/heap" -o "$RUNDIR/pprof/heap-$1-$(date +%s).pb.gz" || true
}

wait_ready() {
  for _ in $(seq 1 120); do
    if [[ "$(curl -sk --max-time 2 --cert "$CERT" --key "$KEY" "$SERVER/readyz" || true)" == "ok" ]]; then
      return 0
    fi
    sleep 1
  done
  echo "apiserver never became ready" >&2
  return 1
}

start_apiserver() {
  "$APISERVER_BIN" "${APISERVER_ARGS[@]}" >> "$RUNDIR/kube-apiserver.log" 2>&1 &
  APIPID=$!
  echo "$APIPID" > "$RUNDIR/apiserver.pid"
  echo "apiserver pid=$APIPID"
}

cleanup() {
  set +e
  # Only signal processes we started (tracked pids) — NEVER pkill by name:
  # other kube-apiserver/etcd processes (kind, multi-tenant experiments) may
  # be running on this host.
  [[ -n "${HERD_PID:-}" ]] && kill "$HERD_PID" 2>/dev/null
  [[ -n "${CHURN_PID:-}" ]] && kill "$CHURN_PID" 2>/dev/null
  [[ -n "${SAMPLER_PID:-}" ]] && kill "$SAMPLER_PID" 2>/dev/null
  local apid
  apid=$(cat "$RUNDIR/apiserver.pid" 2>/dev/null || true)
  [[ -n "$apid" ]] && kill "$apid" 2>/dev/null
  [[ -n "${ETCD_PID:-}" ]] && kill "$ETCD_PID" 2>/dev/null
  sleep 1
  [[ -n "$apid" ]] && kill -9 "$apid" 2>/dev/null
  [[ -n "${ETCD_PID:-}" ]] && kill -9 "$ETCD_PID" 2>/dev/null
}
trap cleanup EXIT

{
  echo "name=$NAME"
  echo "gates=$GATES"
  echo "resource=$RESOURCE accepts=$BURST_ACCEPTS watchlist=${WATCHLIST:-no} seed=$SEED churn_qps=${CHURN_QPS:-0}"
  echo "apiserver_bin=$APISERVER_BIN version=$("$APISERVER_BIN" --version 2>/dev/null || true)"
  echo "n_objects=$N_OBJECTS obj_size=$OBJ_SIZE watchers=$WATCHERS burst=$BURST_CONC/$BURST_COUNT"
  echo "kube_git=$(git -C "$KUBE_ROOT" rev-parse --short HEAD) branch=$(git -C "$KUBE_ROOT" rev-parse --abbrev-ref HEAD)"
  echo "host=$(uname -a)"
} > "$RUNDIR/metadata.txt"

# --- boot ---
"$KUBE_ROOT/third_party/etcd/etcd" --data-dir "$RUNDIR/etcd" --quota-backend-bytes=8589934592 \
  > "$RUNDIR/etcd.log" 2>&1 &
ETCD_PID=$!
# Wait for etcd to actually serve (a prior run's socket in teardown can delay
# the bind; era apiserver binaries give up on etcd quickly at boot).
for _ in $(seq 1 30); do
  if curl -s --max-time 1 http://127.0.0.1:2379/health 2>/dev/null | command grep -q '"health"'; then
    break
  fi
  sleep 1
done

phase boot
start_apiserver
wait_ready

"$HARNESS_DIR/sampler.sh" "$RUNDIR" "$SERVER" "$CERT" "$KEY" &
SAMPLER_PID=$!

# --- seed ---
phase seed
"$STORMTOOL" seed -server "$SERVER" -cert "$CERT" -key "$KEY" -resource "$RESOURCE" \
  -n "$N_OBJECTS" -size "$OBJ_SIZE" -c 64 -ns storm -seed "$SEED" >> "$RUNDIR/stormtool.log" 2>&1
sleep 5

# --- herd + baseline ---
phase herd_start
"$STORMTOOL" herd -server "$SERVER" -cert "$CERT" -key "$KEY" -resource "$RESOURCE" \
  ${WATCHLIST:+-watchlist} -w "$WATCHERS" -duration 30m >> "$RUNDIR/herd.log" 2>&1 &
HERD_PID=$!
sleep 20   # all watchers listed+watching
if [[ -n "$CHURN_QPS" ]]; then
  "$STORMTOOL" churn -server "$SERVER" -cert "$CERT" -key "$KEY" -resource "$RESOURCE" \
    -n "$N_OBJECTS" -qps "$CHURN_QPS" -duration 30m >> "$RUNDIR/churn.log" 2>&1 &
  CHURN_PID=$!
  sleep 5
fi
grab_heap baseline
phase baseline
sleep 30

# --- STORM 1: apiserver crash-restart => watch re-establishment + cache re-warm ---
phase storm_restart
kill -9 "$APIPID" 2>/dev/null || true
sleep 2
start_apiserver
( sleep 8 && grab_heap restart-early ) &
wait_ready
grab_heap restart-ready
sleep 45   # let the herd fully relist; observe recovery
grab_heap restart-late

# --- STORM 2: LIST bursts rv=0 (watch-cache served), one per accept encoding ---
for acc in $BURST_ACCEPTS; do
  phase "storm_burst_rv0_$acc"
  "$STORMTOOL" listburst -server "$SERVER" -cert "$CERT" -key "$KEY" -resource "$RESOURCE" \
    -b "$BURST_CONC" -count "$BURST_COUNT" -rv 0 -accept "$acc" >> "$RUNDIR/stormtool.log" 2>&1
  grab_heap "burst-rv0-$acc"
  sleep 10
done

# --- STORM 3: LIST burst rv="" (consistent read) ---
phase storm_burst_rvempty
"$STORMTOOL" listburst -server "$SERVER" -cert "$CERT" -key "$KEY" -resource "$RESOURCE" \
  -b "$BURST_CONC" -count "$BURST_COUNT" -rv "" >> "$RUNDIR/stormtool.log" 2>&1
grab_heap burst-rvempty
sleep 10

# --- STORM 4: LIST burst with selector (full scan+filter, tiny response) ---
phase storm_burst_selector
"$STORMTOOL" listburst -server "$SERVER" -cert "$CERT" -key "$KEY" -resource "$RESOURCE" \
  -b "$BURST_CONC" -count "$BURST_COUNT" -rv "" -selector "app=nonexistent" >> "$RUNDIR/stormtool.log" 2>&1
grab_heap burst-selector
sleep 10

phase cooldown
sleep 15
phase done

kill "$HERD_PID" 2>/dev/null || true
[[ -n "${CHURN_PID:-}" ]] && kill "$CHURN_PID" 2>/dev/null || true
kill "$SAMPLER_PID" 2>/dev/null || true
sleep 1

python3 "$HARNESS_DIR/analyze.py" "$RUNDIR" | tee "$RUNDIR/SUMMARY.md"
rm -rf "$RUNDIR/etcd"   # scratch state only; measurement data is kept
echo "rundir: $RUNDIR"
