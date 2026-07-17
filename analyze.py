#!/usr/bin/env python3
"""analyze.py <rundir> — per-phase peak-load summary for a storm run.

Reads phases.csv, proc.csv, metrics/*.prom and stormtool logs; prints a
markdown summary and writes summary.json.
"""
import json
import os
import re
import sys
from collections import defaultdict

CLK_TCK = os.sysconf("SC_CLK_TCK")


def parse_prom_line(line):
    m = re.match(r"^([a-zA-Z0-9_:]+)(\{[^}]*\})?\s+([0-9eE+.\-]+|NaN)$", line.strip())
    if not m:
        return None
    name, labels, val = m.group(1), m.group(2) or "", m.group(3)
    try:
        return name, labels, float(val)
    except ValueError:
        return None


def load_snapshot(path):
    """Aggregate one filtered /metrics snapshot into scalars."""
    agg = defaultdict(float)
    list_buckets = defaultdict(float)
    for line in open(path):
        p = parse_prom_line(line)
        if not p:
            continue
        name, labels, val = p
        if name == "apiserver_request_duration_seconds_bucket" and 'verb="LIST"' in labels:
            le = re.search(r'le="([^"]+)"', labels)
            if le:
                list_buckets[le.group(1)] += val
        elif name == "apiserver_request_duration_seconds_count" and 'verb="LIST"' in labels:
            agg["list_dur_count"] += val
        elif name == "apiserver_request_duration_seconds_sum" and 'verb="LIST"' in labels:
            agg["list_dur_sum"] += val
        elif name == "apiserver_request_total":
            verb = re.search(r'verb="([^"]+)"', labels)
            if verb:
                agg["req_" + verb.group(1)] += val
        elif name == "apiserver_longrunning_requests":
            agg["watch_gauge"] += val
        elif name == "etcd_requests_total":
            op = re.search(r'operation="([^"]+)"', labels)
            if op:
                agg["etcd_" + op.group(1)] += val
        elif name == "apiserver_flowcontrol_rejected_requests_total":
            agg["apf_rejected"] += val
        elif name == "apiserver_flowcontrol_current_executing_seats":
            agg["apf_exec_seats"] += val
        elif name == "apiserver_flowcontrol_current_inqueue_requests":
            agg["apf_inqueue"] += val
        elif name == "apiserver_current_inflight_requests":
            agg["inflight"] += val
        else:
            agg[name] = agg.get(name, 0.0) + val
    return agg, list_buckets


def hist_quantile(delta_buckets, q):
    """q-quantile from a delta histogram (le -> count)."""
    items = sorted(
        ((float("inf") if le == "+Inf" else float(le)), c) for le, c in delta_buckets.items()
    )
    if not items:
        return None
    total = items[-1][1]
    if total <= 0:
        return None
    target = q * total
    prev_le, prev_c = 0.0, 0.0
    for le, c in items:
        if c >= target:
            if le == float("inf"):
                return prev_le
            if c == prev_c:
                return le
            return prev_le + (le - prev_le) * (target - prev_c) / (c - prev_c)
        prev_le, prev_c = le, c
    return items[-1][0]


def main(rundir):
    phases = []  # (ts_ms, name)
    for line in open(os.path.join(rundir, "phases.csv")):
        ts, name = line.strip().split(",", 1)
        phases.append((int(ts), name))
    phases.sort()

    proc = []  # dicts
    with open(os.path.join(rundir, "proc.csv")) as f:
        header = f.readline()
        for line in f:
            parts = line.strip().split(",")
            if len(parts) < 7 or not parts[2]:
                continue
            try:
                proc.append(
                    dict(
                        ts=int(parts[0]),
                        pid=parts[1],
                        rss=int(parts[2]),
                        pss=int(parts[3]),
                        ref=int(parts[4]),
                        utime=int(parts[5]) if parts[5] else 0,
                        stime=int(parts[6]) if parts[6] else 0,
                    )
                )
            except ValueError:  # torn read while the process was dying
                continue

    mdir = os.path.join(rundir, "metrics")
    snaps = []
    for fn in sorted(os.listdir(mdir)):
        if fn.endswith(".prom"):
            agg, buckets = load_snapshot(os.path.join(mdir, fn))
            snaps.append((int(fn[:-5]), agg, buckets))
    snaps.sort()

    def phase_of(ts):
        cur = "pre"
        for pts, name in phases:
            if ts >= pts:
                cur = name
            else:
                break
        return cur

    # Per-phase aggregation
    result = {}
    order = []
    for _, name in phases:
        if name not in result:
            result[name] = {}
            order.append(name)

    # proc.csv: peaks and CPU% (delta of utime+stime across consecutive samples of same pid)
    by_phase_proc = defaultdict(list)
    prev = None
    for row in proc:
        cpu_pct = None
        if prev and prev["pid"] == row["pid"] and row["ts"] > prev["ts"]:
            dt = (row["ts"] - prev["ts"]) / 1000.0
            dticks = (row["utime"] + row["stime"]) - (prev["utime"] + prev["stime"])
            cpu_pct = 100.0 * (dticks / CLK_TCK) / dt
        row["cpu_pct"] = cpu_pct
        by_phase_proc[phase_of(row["ts"])].append(row)
        prev = row

    # snapshots: gauges take per-phase max; counters accumulate reset-aware
    # per-interval deltas (delta < 0 => process restart => counter restarted
    # from 0, so the interval contributes the new value itself).
    COUNTERS = [
        "req_LIST", "req_WATCH", "apf_rejected",
        "etcd_list", "etcd_listStream", "etcd_get",
    ]
    by_phase_snap = defaultdict(list)
    phase_counter = defaultdict(lambda: defaultdict(float))
    phase_buckets = defaultdict(lambda: defaultdict(float))
    prev_snap = None
    for ts, agg, buckets in snaps:
        alloc_rate = None
        if prev_snap is not None and ts > prev_snap[0]:
            dt = (ts - prev_snap[0]) / 1000.0
            ph = phase_of(ts)
            da = agg.get("go_memstats_alloc_bytes_total", 0) - prev_snap[1].get(
                "go_memstats_alloc_bytes_total", 0
            )
            if da < 0:  # restart
                da = agg.get("go_memstats_alloc_bytes_total", 0)
            alloc_rate = da / dt
            phase_counter[ph]["alloc_total_bytes"] += da
            for key in COUNTERS:
                d = agg.get(key, 0) - prev_snap[1].get(key, 0)
                if d < 0:
                    d = agg.get(key, 0)
                phase_counter[ph][key] += d
            for le, c in buckets.items():
                d = c - prev_snap[2].get(le, 0)
                if d < 0:
                    d = c
                phase_buckets[ph][le] += d
        by_phase_snap[phase_of(ts)].append((ts, agg, buckets, alloc_rate))
        prev_snap = (ts, agg, buckets)

    for name in order:
        r = result[name]
        rows = by_phase_proc.get(name, [])
        if rows:
            r["peak_rss_mb"] = round(max(x["rss"] for x in rows) / 1024, 1)
            r["peak_referenced_mb"] = round(max(x["ref"] for x in rows) / 1024, 1)
            cpus = [x["cpu_pct"] for x in rows if x["cpu_pct"] is not None]
            if cpus:
                r["peak_cpu_pct"] = round(max(cpus), 1)
        ss = by_phase_snap.get(name, [])
        if ss:
            r["peak_heap_inuse_mb"] = round(
                max(a.get("go_memstats_heap_inuse_bytes", 0) for _, a, _, _ in ss) / 1048576, 1
            )
            # Peak minus phase-entry heap: the phase's OWN contribution.
            # (Raw peak can carry un-GC'd residue from the previous phase.)
            entry = ss[0][1].get("go_memstats_heap_inuse_bytes", 0)
            r["heap_delta_mb"] = round(
                (max(a.get("go_memstats_heap_inuse_bytes", 0) for _, a, _, _ in ss) - entry) / 1048576, 1
            )
            rates = [ar for _, _, _, ar in ss if ar is not None]
            if rates:
                r["peak_alloc_rate_mb_s"] = round(max(rates) / 1048576, 1)
            r["alloc_total_mb"] = round(phase_counter[name].get("alloc_total_bytes", 0) / 1048576, 1)
            r["watch_gauge_max"] = int(max(a.get("watch_gauge", 0) for _, a, _, _ in ss))
            r["apf_inqueue_max"] = int(max(a.get("apf_inqueue", 0) for _, a, _, _ in ss))
            r["apf_exec_seats_max"] = int(max(a.get("apf_exec_seats", 0) for _, a, _, _ in ss))
            first, last = ss[0], ss[-1]
            dur_s = max((last[0] - first[0]) / 1000.0, 0.001)
            r["duration_s"] = round(dur_s, 1)
            for key, label in [
                ("req_LIST", "lists"),
                ("req_WATCH", "watches"),
                ("apf_rejected", "apf_rejected"),
                ("etcd_list", "etcd_lists"),
                ("etcd_listStream", "etcd_liststreams"),
                ("etcd_get", "etcd_gets"),
            ]:
                d = phase_counter[name].get(key, 0)
                if d > 0:
                    r[label] = int(d)
            for q, label in [(0.5, "list_p50_s"), (0.99, "list_p99_s")]:
                v = hist_quantile(phase_buckets[name], q)
                if v is not None:
                    r[label] = round(v, 3)

    with open(os.path.join(rundir, "summary.json"), "w") as f:
        json.dump({"phases": order, "results": result}, f, indent=2)

    cols = [
        "duration_s", "lists", "list_p50_s", "list_p99_s", "peak_heap_inuse_mb",
        "peak_alloc_rate_mb_s", "alloc_total_mb", "peak_rss_mb", "peak_referenced_mb", "peak_cpu_pct",
        "watch_gauge_max", "etcd_lists", "etcd_liststreams", "apf_inqueue_max",
        "apf_exec_seats_max", "apf_rejected",
    ]
    print("| phase | " + " | ".join(cols) + " |")
    print("|---" * (len(cols) + 1) + "|")
    for name in order:
        r = result[name]
        print(f"| {name} | " + " | ".join(str(r.get(c, "")) for c in cols) + " |")


if __name__ == "__main__":
    main(sys.argv[1])
