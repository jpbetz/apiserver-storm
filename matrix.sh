#!/usr/bin/env bash
# matrix.sh <arms.tsv> <reps>
#
# Apples-to-apples attribution matrix: runs every arm through the IDENTICAL
# storm scenario, interleaved rep-major (r1 of all arms, then r2, ...) so
# thermal/background drift spreads evenly across arms.
#
# arms.tsv columns (tab-separated, # comments allowed):
#   name  binary  gates  extra_env
# extra_env is a comma-separated list like WATCHLIST=1,CHURN_QPS=300 (or -)
set -euo pipefail
ARMS_FILE=${1:?arms.tsv}
REPS=${2:-2}
REP_START=${REP_START:-1}
HARNESS_DIR="$(cd "$(dirname "$0")" && pwd)"

# Fixed scenario for every arm.
export RESOURCE=${RESOURCE:-pods}
export BURST_ACCEPTS=${BURST_ACCEPTS:-json proto table}
export N_OBJECTS=${N_OBJECTS:-15000}
export WATCHERS=${WATCHERS:-100}
export SEED=${SEED:-42}

for rep in $(seq "$REP_START" "$REPS"); do
  while IFS=$'\t' read -r name binary gates extra; do
    [[ -z "$name" || "$name" == \#* ]] && continue
    echo "=== rep $rep arm $name"
    (
      export APISERVER_BIN="$binary"
      if [[ -n "${extra:-}" && "$extra" != "-" ]]; then
        IFS=',' read -ra kvs <<< "$extra"
        for kv in "${kvs[@]}"; do export "${kv?}"; done
      fi
      "$HARNESS_DIR/storm.sh" "mx-$name-r$rep" "${gates/#-/}" > /dev/null 2>&1
    ) || echo "ARM FAILED: $name r$rep"
  done < "$ARMS_FILE"
done
echo "MATRIX COMPLETE"
python3 "$HARNESS_DIR/compare.py" "$HARNESS_DIR/runs" mx-
