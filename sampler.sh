#!/usr/bin/env bash
# sampler.sh <rundir> <server> <cert> <key>
# 1s-resolution sampler for kube-apiserver peak-load runs.
# Writes:
#   <rundir>/proc.csv            ts_ms,pid,rss_kb,pss_kb,referenced_kb,utime_ticks,stime_ticks
#   <rundir>/metrics/<ts>.prom   filtered /metrics snapshot
set -u
RUNDIR=$1; SERVER=$2; CERT=$3; KEY=$4
mkdir -p "$RUNDIR/metrics"
echo "ts_ms,pid,rss_kb,pss_kb,referenced_kb,utime_ticks,stime_ticks" > "$RUNDIR/proc.csv"

GAUGE_RE='^(go_memstats_(heap_inuse_bytes|heap_alloc_bytes|alloc_bytes_total)|go_gc_duration_seconds_(sum|count)|process_(cpu_seconds_total|resident_memory_bytes)|apiserver_current_inflight_requests|apiserver_flowcontrol_(current_executing_seats|current_inqueue_requests)|apiserver_watch_cache_initializations_total)'

while true; do
  ts=$(date +%s%3N)
  pid=$(cat "$RUNDIR/apiserver.pid" 2>/dev/null || true)
  [[ -n "${pid:-}" && ! -e "/proc/$pid" ]] && pid=""
  if [[ -n "${pid:-}" && -r /proc/$pid/smaps_rollup ]]; then
    rss=$(awk '/^Rss:/{print $2}' /proc/$pid/smaps_rollup 2>/dev/null)
    pss=$(awk '/^Pss:/{print $2}' /proc/$pid/smaps_rollup 2>/dev/null)
    ref=$(awk '/^Referenced:/{print $2}' /proc/$pid/smaps_rollup 2>/dev/null)
    cpu=$(awk '{print $14","$15}' /proc/$pid/stat 2>/dev/null)
    echo "$ts,$pid,${rss:-},${pss:-},${ref:-},${cpu:-,}" >> "$RUNDIR/proc.csv"
  else
    echo "$ts,,,,,," >> "$RUNDIR/proc.csv"
  fi
  tmp=$(mktemp)
  if curl -sk --max-time 2 --cert "$CERT" --key "$KEY" "$SERVER/metrics" -o "$tmp" 2>/dev/null && [[ -s "$tmp" ]]; then
    out="$RUNDIR/metrics/$ts.prom"
    grep -E "$GAUGE_RE" "$tmp" > "$out" || true
    grep -E '^apiserver_request_total' "$tmp" | grep -E 'verb="(LIST|WATCH|GET|POST|PUT)"' >> "$out" || true
    grep -E '^apiserver_longrunning_requests' "$tmp" | grep 'verb="WATCH"' >> "$out" || true
    grep -E '^apiserver_request_duration_seconds_(bucket|sum|count)' "$tmp" | grep 'verb="LIST"' >> "$out" || true
    grep -E '^apiserver_flowcontrol_rejected_requests_total' "$tmp" >> "$out" || true
    grep -E '^etcd_requests_total|^etcd_request_duration_seconds_(sum|count)' "$tmp" >> "$out" || true
    grep -E '^apiserver_resource_size_estimate_bytes|^apiserver_storage_objects' "$tmp" >> "$out" || true
    grep -E '^apiserver_flowcontrol_work_estimated_seats_(sum|count)' "$tmp" >> "$out" || true
    grep -E '^apiserver_flowcontrol_request_wait_duration_seconds_(sum|count)' "$tmp" | grep 'execute="true"' >> "$out" || true
  fi
  rm -f "$tmp"
  sleep 0.6
done
