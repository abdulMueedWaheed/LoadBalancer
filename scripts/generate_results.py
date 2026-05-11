#!/usr/bin/env python3
"""Generate results.md from matrix_summary.csv benchmark data.

Usage:
    python scripts/generate_results.py
    python scripts/generate_results.py --input logs/matrix_summary.csv --output results.md
"""

import argparse
import csv
from collections import defaultdict
from datetime import date
from statistics import mean
from typing import Any


def load_summary(path: str) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    with open(path, "r", encoding="utf-8") as f:
        lines = f.readlines()
        if not lines:
            return []
        
        # Skip "section,per_run" metadata row if present
        start_idx = 0
        if lines[0].strip() == 'section,per_run':
            start_idx = 1
            
        import io
        reader = csv.DictReader(io.StringIO("".join(lines[start_idx:])))
        for row in reader:
            # Stop if we hit a new section marker or empty row
            if not row.get("algorithm") or row["algorithm"] == "section" or row["algorithm"] == "algorithm":
                break
            
            def safe_float(val, default=0.0):
                try:
                    return float(val) if val is not None else default
                except (ValueError, TypeError):
                    return default

            def safe_int(val, default=0):
                try:
                    return int(val) if val is not None else default
                except (ValueError, TypeError):
                    return default

            row["avg_ms"] = safe_float(row.get("avg_ms"))
            row["p95_ms"] = safe_float(row.get("p95_ms"))
            row["p99_ms"] = safe_float(row.get("p99_ms"))
            row["max_ms"] = safe_float(row.get("max_ms"))
            row["success_rate"] = safe_float(row.get("success_rate"))
            row["total"] = safe_int(row.get("total"))
            row["success"] = safe_int(row.get("success"))
            row["fail"] = safe_int(row.get("fail"))
            # Handle both naming conventions
            row["repeat_id"] = safe_int(row.get("repeat_id") or row.get("run_id"))
            row["requests"] = safe_int(row.get("requests"), 500)
            row["concurrency"] = safe_int(row.get("concurrency"), 30)
            rows.append(row)
    return rows


def aggregate(rows: list[dict[str, Any]]) -> dict[tuple[str, str, str], dict[str, float]]:
    """Group by (algorithm, profile, pattern) and compute means."""
    groups: dict[tuple[str, str, str], list[dict[str, Any]]] = defaultdict(list)
    for r in rows:
        key = (r["algorithm"], r["profile"], r["pattern"])
        groups[key].append(r)

    result: dict[tuple[str, str, str], dict[str, float]] = {}
    for key, items in groups.items():
        result[key] = {
            "runs": len(items),
            "success_rate": mean(i["success_rate"] for i in items),
            "avg_ms": mean(i["avg_ms"] for i in items),
            "p95_ms": mean(i["p95_ms"] for i in items),
            "p99_ms": mean(i["p99_ms"] for i in items),
            "max_ms": mean(i["max_ms"] for i in items),
        }
    return result


ALGO_LABELS = {
    "round_robin": "Round Robin",
    "least_connections": "Least Connections",
    "ucb": "UCB",
    "metric_aware": "Metric-Aware",
}

PROFILE_LABELS = {
    "db_point_light": "Point Query (Light)",
    "db_range_heavy": "Range Query (Heavy)",
    "db_mixed_50_50": "Mixed 50/50",
}


def pct_delta(val: float, baseline: float) -> str:
    if baseline == 0:
        return "N/A"
    delta = (val - baseline) / baseline * 100
    sign = "+" if delta > 0 else ""
    return f"{sign}{delta:.2f}%"


def generate_markdown(rows: list[dict[str, Any]]) -> str:
    agg = aggregate(rows)
    patterns = sorted(set(r["pattern"] for r in rows))
    algorithms = sorted(set(r["algorithm"] for r in rows))
    profiles = sorted(set(r["profile"] for r in rows))
    requests_per_run = rows[0]["requests"]
    concurrency = rows[0]["concurrency"]
    total_runs = len(rows)

    lines: list[str] = []
    w = lines.append

    w("# Benchmark Results — Final Aggregated Summary\n")
    w(f"Date: {date.today().strftime('%B %d, %Y')}  ")
    w(f"Methodology: 3 repeated runs per (algorithm, workload, pattern)  ")
    w(f"Algorithms: `{'`, `'.join(algorithms)}`  ")
    w(f"Workloads: `{'`, `'.join(profiles)}`  ")
    w(f"Patterns: `{'`, `'.join(patterns)}`  ")
    w(f"Run settings: `{requests_per_run} requests`, `{concurrency} concurrency`, clean Docker start  ")
    w(f"Total runs: {total_runs}  ")
    w("")
    w("---\n")

    # ── Aggregated Results Table ──
    w("## Aggregated Results (Mean over 3 Runs)\n")

    for pattern in patterns:
        w(f"### Traffic Pattern: `{pattern}`\n")
        w("| Algorithm | Workload | Runs | Success % | Avg (ms) | P95 (ms) | P99 (ms) | Max (ms) |")
        w("|---|---|---:|---:|---:|---:|---:|---:|")

        for algo in algorithms:
            for profile in profiles:
                key = (algo, profile, pattern)
                if key not in agg:
                    continue
                d = agg[key]
                w(f"| {ALGO_LABELS.get(algo, algo)} | {PROFILE_LABELS.get(profile, profile)} "
                  f"| {d['runs']:.0f} | {d['success_rate']:.1f} "
                  f"| **{d['avg_ms']:.2f}** | {d['p95_ms']:.2f} "
                  f"| {d['p99_ms']:.2f} | {d['max_ms']:.2f} |")
        w("")

    w("---\n")

    # ── Algorithm Comparison (steady traffic, which is the fair baseline) ──
    w("## Algorithm Comparison (Steady Traffic)\n")

    baseline_algo = "round_robin"

    for profile in profiles:
        w(f"### {PROFILE_LABELS.get(profile, profile)}\n")
        w("| Algorithm | Avg (ms) | vs RR | P95 (ms) | vs RR | P99 (ms) | vs RR |")
        w("|---|---:|---|---:|---|---:|---|")

        baseline_key = (baseline_algo, profile, "steady")
        if baseline_key not in agg:
            w("*No baseline data available.*\n")
            continue
        bl = agg[baseline_key]

        for algo in algorithms:
            key = (algo, profile, "steady")
            if key not in agg:
                continue
            d = agg[key]
            w(f"| {ALGO_LABELS.get(algo, algo)} "
              f"| {d['avg_ms']:.2f} | {pct_delta(d['avg_ms'], bl['avg_ms'])} "
              f"| {d['p95_ms']:.2f} | {pct_delta(d['p95_ms'], bl['p95_ms'])} "
              f"| {d['p99_ms']:.2f} | {pct_delta(d['p99_ms'], bl['p99_ms'])} |")
        w("")

    w("---\n")

    # ── Steady vs Burst comparison ──
    if "burst" in patterns and "steady" in patterns:
        w("## Traffic Pattern Impact (Steady vs Burst)\n")
        w("| Algorithm | Workload | Steady Avg (ms) | Burst Avg (ms) | Delta |")
        w("|---|---|---:|---:|---|")

        for algo in algorithms:
            for profile in profiles:
                sk = (algo, profile, "steady")
                bk = (algo, profile, "burst")
                if sk in agg and bk in agg:
                    s = agg[sk]
                    b = agg[bk]
                    w(f"| {ALGO_LABELS.get(algo, algo)} | {PROFILE_LABELS.get(profile, profile)} "
                      f"| {s['avg_ms']:.2f} | {b['avg_ms']:.2f} | {pct_delta(b['avg_ms'], s['avg_ms'])} |")
        w("")
        w("---\n")

    # ── Key Findings ──
    w("## Key Findings\n")

    # Find best algo per profile under steady
    for profile in profiles:
        avgs = {}
        for algo in algorithms:
            key = (algo, profile, "steady")
            if key in agg:
                avgs[algo] = agg[key]["avg_ms"]

        if avgs:
            best = min(avgs, key=avgs.get)  # type: ignore
            worst = max(avgs, key=avgs.get)  # type: ignore
            w(f"### {PROFILE_LABELS.get(profile, profile)}")
            w(f"- **Best**: {ALGO_LABELS.get(best, best)} ({avgs[best]:.2f} ms)")
            w(f"- **Worst**: {ALGO_LABELS.get(worst, worst)} ({avgs[worst]:.2f} ms)")
            w(f"- Improvement: {pct_delta(avgs[best], avgs[worst])} vs worst")
            w("")

    w("---\n")

    # ── Algorithm Summary ──
    w("## Algorithm Summary\n")

    w("### Round Robin (Static Baseline)")
    w("- **Strategy**: Cyclic assignment, no feedback")
    w("- **Strengths**: Lowest overhead, predictable")
    w("- **Weakness**: Ignores node latency and load\n")

    w("### Least Connections (Heuristic)")
    w("- **Strategy**: Route to node with fewest active connections")
    w("- **Strengths**: Adapts to connection-level load")
    w("- **Weakness**: No latency awareness\n")

    w("### UCB (Multi-Armed Bandit)")
    w("- **Strategy**: Balances exploitation (fast nodes) with exploration (underused nodes)")
    w("- **Reward**: `1 / (1 + latency_ms / 1000)` — lower latency = higher reward")
    w("- **Strengths**: Learns node performance over time; adapts to changing conditions")
    w("- **Weakness**: Initial exploration overhead; slower convergence with many nodes\n")

    w("### Metric-Aware (Adaptive)")
    w("- **Strategy**: Weighted scoring using push-based metrics (latency, queue depth, connections, failure rate)")
    w("- **Strengths**: Richest signal set; incorporates multiple operational dimensions")
    w("- **Weakness**: Higher computational overhead; sensitive to weight tuning and metric staleness\n")

    w("---\n")

    w("## Methodology Notes\n")
    w(f"- All experiments run on containerized infrastructure (Docker Compose)")
    w(f"- 6 heterogeneous nodes with varying delay profiles (50ms–1500ms)")
    w(f"- Nodes 2 and 5 have crash simulation (3–5%); Node 3 has timeout simulation (5%)")
    w(f"- Controller timeout: 1 second per node attempt; retries all nodes before failing")
    w(f"- Each (algorithm, workload, pattern) combination repeated 3 times")
    w(f"- Clean Docker restart between algorithm changes")
    w(f"- All results generated by `scripts/benchmark_matrix.py`")
    w("")

    return "\n".join(lines)


def main() -> None:
    parser = argparse.ArgumentParser(description="Generate results.md from benchmark data.")
    parser.add_argument("--input", default="logs/matrix_summary.csv", help="Path to matrix_summary.csv")
    parser.add_argument("--output", default="results.md", help="Output markdown file")
    args = parser.parse_args()

    rows = load_summary(args.input)
    md = generate_markdown(rows)

    with open(args.output, "w", encoding="utf-8") as f:
        f.write(md)

    print(f"Generated {args.output} from {len(rows)} runs")


if __name__ == "__main__":
    main()
