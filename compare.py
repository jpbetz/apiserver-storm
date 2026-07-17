#!/usr/bin/env python3
"""compare.py <runsdir> [prefix] — aggregate matrix runs into a ranked table.

Groups run dirs named <prefix><arm>-r<rep>-<timestamp>, takes the median of
each metric across reps, and prints one markdown table per phase of interest.
"""
import json
import os
import re
import statistics
import sys

PHASES = [
    "herd_start", "storm_restart",
    "storm_burst_rv0_json", "storm_burst_rv0_proto", "storm_burst_rv0_table",
    "storm_burst_rvempty",
]
METRICS = ["list_p50_s", "list_p99_s", "peak_heap_inuse_mb", "alloc_total_mb",
           "peak_rss_mb", "peak_cpu_pct", "apf_rejected"]


def main(runsdir, prefix="mx-"):
    arms = {}  # arm -> list of summary dicts
    pat = re.compile(rf"^{re.escape(prefix)}(.+)-r(\d+)-\d{{8}}-\d{{6}}$")
    for entry in sorted(os.listdir(runsdir)):
        m = pat.match(entry)
        if not m:
            continue
        path = os.path.join(runsdir, entry, "summary.json")
        if not os.path.exists(path):
            continue
        arms.setdefault(m.group(1), []).append(json.load(open(path))["results"])

    if not arms:
        print(f"no runs matching {prefix}*")
        return

    for phase in PHASES:
        rows = []
        for arm, summaries in sorted(arms.items()):
            vals = {}
            for metric in METRICS:
                xs = [s[phase][metric] for s in summaries
                      if phase in s and metric in s[phase]]
                if xs:
                    med = statistics.median(xs)
                    spread = (max(xs) - min(xs)) / med * 100 if med and len(xs) > 1 else 0
                    vals[metric] = (med, spread, len(xs))
            if vals:
                rows.append((arm, vals))
        if not rows:
            continue
        print(f"\n## {phase}")
        print("| arm | " + " | ".join(METRICS) + " |")
        print("|---" * (len(METRICS) + 1) + "|")
        for arm, vals in rows:
            cells = []
            for metric in METRICS:
                if metric in vals:
                    med, spread, n = vals[metric]
                    cells.append(f"{med:g}" + (f" ±{spread:.0f}%" if spread >= 5 else ""))
                else:
                    cells.append("")
            print(f"| {arm} | " + " | ".join(cells) + " |")


if __name__ == "__main__":
    main(sys.argv[1], sys.argv[2] if len(sys.argv) > 2 else "mx-")
