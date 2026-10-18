# apiserver-storm

Reproducible kube-apiserver **peak-load** experiments: trigger relist herds and
LIST storms against a local apiserver, sample the peak envelope at 1-second
resolution, and run apples-to-apples **attribution matrices** across binaries
and feature gates.

Steady-state benchmarks structurally miss peak behavior (allocation integrals
hide transient peaks; server-side metrics can invert verdicts during recovery
windows). This harness measures what those miss: peak heap, allocation rate,
working-set, client-observed recovery latency, and 429s by source — per storm
phase, per arm.

Example results: [docs/peak-load-report.html](./docs/peak-load-report.html)
([pdf](./docs/peak-load-report.pdf)) — triggers taxonomy, per-change attribution
across nine recent efficiency improvements, and the cacher-not-ready-429
methodology finding.

## Requirements

- A built `kube-apiserver` binary (any version; the harness is version-agnostic)
  and its serving certs/kubeconfig — [`qas`](https://github.com/jpbetz/bin)
  produces both (`qas up` once; the binary name `qas` in arms.tsv means
  `$(qas env QAS_APISERVER_BIN)`), or point at your own via env.
- `etcd` on `$PATH` or via `$KUBE_ROOT/third_party/etcd`.
- Go (to build `stormtool`), Python 3, `curl`.

## Tools

- **`stormtool.go`** — raw-HTTP load generator, no client-go dependency
  (reflector semantics implemented explicitly, so herd behavior is exact and
  tunable):
  - `seed` — N configmaps or pods (`-resource`), deterministic payloads
    (`-seed`) for byte-identical runs. Creates the namespace + `default`
    ServiceAccount (no controller-manager runs in this setup).
  - `herd` — W concurrent list+watch loops: LIST rv=0 → WATCH → immediate
    re-LIST on disconnect. `-watchlist` switches to KEP-3157 streaming
    (sendInitialEvents, no LIST). Logs per-relist bytes/latency and every
    client-visible 429.
  - `listburst` — B concurrent LISTs: `-rv 0|""`, `-selector`,
    `-accept json|proto|table`.
  - `churn` — steady annotation-PATCH load at target QPS.
- **`storm.sh <name> [feature-gates]`** — one full scenario: boot etcd+apiserver
  → seed → herd → baseline → kill-9 **restart storm** → bursts per encoding →
  consistent-read burst → selector burst. 1s sampler (filtered /metrics +
  RSS/PSS/Referenced-WSS with per-phase `clear_refs` reset + CPU), pprof heap
  per phase. Scripts snapshot themselves into each run dir. Knobs via env:
  `APISERVER_BIN, RESOURCE, N_OBJECTS, OBJ_SIZE, WATCHERS, BURST_CONC,
  BURST_ACCEPTS, WATCHLIST, CHURN_QPS, SEED, KUBE_ROOT`.
- **`analyze.py <rundir>`** — per-phase peak table, reset-aware across apiserver
  restarts: LIST p50/p99, peak heap, `heap_delta` (peak minus phase entry —
  raw peaks carry un-GC'd residue from the previous phase), alloc rate + total,
  RSS/WSS, CPU, APF queue/rejects, etcd `list` vs `listStream` ops.
- **`matrix.sh <arms.tsv> <reps>`** — the attribution matrix: every arm through
  the identical scenario, interleaved rep-major; ends with `compare.py`
  (per-phase medians ±spread). `REP_START=n` resumes/extends.
- **`arms.tsv`** — arm definitions (name, binary, gates, extra env). Three
  attribution techniques: gate flips (same binary), leave-one-out reverts,
  boundary pairs (`commit^` vs `commit`, same era/toolchain). `build-arms.sh`
  builds the revert/boundary binaries from a kubernetes worktree.

## Quickstart

```bash
go build -o stormtool stormtool.go
# one storm against your apiserver build (defaults: 15k configmaps, 100 watchers)
APISERVER_BIN=/path/to/kube-apiserver ./storm.sh my-first-storm ""
# pods + all encodings, EtcdRangeStream off:
RESOURCE=pods BURST_ACCEPTS="json proto table" \
  ./storm.sh rs-off "EtcdRangeStream=false"
# full attribution matrix, 3 reps
./matrix.sh arms.tsv 3
```

## Measurement rules learned the hard way

- **Client truth is mandatory.** Server-side phase metrics alone inverted a
  verdict in our runs: a slower-recovering configuration looked calmer because
  clients were bouncing on cacher-not-ready 429s that never touch
  `apiserver_flowcontrol_rejected_requests_total`. The herd logs every 429 it
  receives; use both views.
- **Peaks, not integrals.** `B/op` and alloc totals understate streaming-style
  improvements; sample live heap at high frequency (see the companion
  `BenchmarkCacherInit` peak-heap patch, branch `bench/cacherinit-peak-heap`).
- **Phase self-attribution.** Use `heap_delta`, not raw phase peak — heap decays
  lazily across phase boundaries.
- **Never kill by process name.** Other kube-apiserver/etcd processes may run on
  the host; all cleanup here signals tracked PIDs only. Check `ss -tlnp` for
  2379/6443 before running.

## License

Apache-2.0.
