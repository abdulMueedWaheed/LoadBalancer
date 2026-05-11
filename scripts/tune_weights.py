#!/usr/bin/env python3
"""Grid search for optimal Metric-Aware weights.

Runs quick benchmark tests with different weight combinations and reports
the best configuration.

Usage:
    python scripts/tune_weights.py
    python scripts/tune_weights.py --requests 200 --concurrency 30
"""

import argparse
import csv
import itertools
import os
import re
import subprocess
import time
from statistics import mean
from typing import Any


def set_ma_weights_in_compose(compose_path: str, weights: dict[str, float]) -> None:
    """Inject MA_W_* environment variables into the controller service."""
    with open(compose_path, "r") as f:
        content = f.read()

    # Ensure ALGORITHM is metric_aware
    content = re.sub(
        r'(ALGORITHM:\s*)"[^"]*"',
        r'\1"metric_aware"',
        content,
    )

    # Remove any existing MA_ env vars
    content = re.sub(r'\s+MA_W_\w+:.*\n', '\n', content)
    content = re.sub(r'\s+MA_STALE_\w+:.*\n', '\n', content)

    # Build the env var block
    env_lines = ""
    for key, val in weights.items():
        env_lines += f'      {key}: "{val}"\n'

    # Insert after ALGORITHM line in controller
    content = re.sub(
        r'(ALGORITHM:\s*"metric_aware"\n)',
        r'\1' + env_lines,
        content,
    )

    with open(compose_path, "w") as f:
        f.write(content)


def run_quick_benchmark(
    root: str,
    profile: str,
    requests: int,
    concurrency: int,
    label: str,
) -> dict[str, float]:
    """Run a single quick benchmark and return summary stats."""
    subprocess.run(
        [
            "python", "client/main.py",
            "--workload-profile", profile,
            "--requests", str(requests),
            "--concurrency", str(concurrency),
            "--pattern", "steady",
            "--repeat", "1",
            "--label", label,
            "--algo-label", "metric_aware",
        ],
        cwd=root,
        check=True,
        capture_output=True,
    )

    # Find the CSV
    log_dir = os.path.join(root, "logs")
    expected = f"client_metric_aware_{profile}_{label}_run1.csv"
    csv_path = os.path.join(log_dir, expected)

    if not os.path.exists(csv_path):
        return {"avg_ms": 9999.0, "p95_ms": 9999.0, "p99_ms": 9999.0, "success_rate": 0.0}

    # Parse
    latencies: list[float] = []
    total = 0
    success = 0
    with open(csv_path, "r") as f:
        for row in csv.DictReader(f):
            total += 1
            if str(row.get("success", "")).lower() == "true":
                success += 1
                latencies.append(float(row.get("latency_ms", "0") or 0))

    latencies.sort()
    avg = mean(latencies) if latencies else 9999.0
    p95 = latencies[int(len(latencies) * 0.95)] if latencies else 9999.0
    p99 = latencies[int(len(latencies) * 0.99)] if latencies else 9999.0
    rate = (success / total * 100) if total else 0.0

    # Clean up the CSV
    os.remove(csv_path)

    return {"avg_ms": avg, "p95_ms": p95, "p99_ms": p99, "success_rate": rate}


def main() -> None:
    parser = argparse.ArgumentParser(description="Grid search for MA weights")
    parser.add_argument("--project-root", default=".")
    parser.add_argument("--requests", type=int, default=200, help="Requests per test (lower = faster)")
    parser.add_argument("--concurrency", type=int, default=30)
    parser.add_argument("--profile", default="db_point_light", help="Workload profile to tune for")
    parser.add_argument("--output", default="logs/weight_tuning.csv")
    args = parser.parse_args()

    root = os.path.abspath(args.project_root)
    compose_path = os.path.join(root, "docker-compose.yml")

    # ── Weight grid ──
    # The original weights were: active=1.0, latency=0.02, queue=1.5, failure=2.0, stale=3.0
    # Problem: latency weight far too low, stale penalty too high
    grid = {
        "MA_W_ACTIVE":       [0.5, 1.0],
        "MA_W_LATENCY":      [0.02, 0.2, 0.5, 1.0],   # key variable to explore
        "MA_W_QUEUE":        [0.5, 1.5],
        "MA_W_FAILURE":      [1.0, 2.0],
        "MA_STALE_PENALTY":  [0.5, 1.5, 3.0],
        "MA_STALE_SECONDS":  [5, 15],
    }

    keys = list(grid.keys())
    combos = list(itertools.product(*[grid[k] for k in keys]))
    total = len(combos)

    print(f"Grid search: {total} weight combinations × {args.requests} requests each")
    print(f"Profile: {args.profile}")
    print(f"Estimated time: ~{total * 15 // 60} minutes\n")

    results: list[dict[str, Any]] = []

    for i, combo in enumerate(combos):
        weights = dict(zip(keys, combo))
        label = f"tune_{i}"

        print(f"[{i+1}/{total}] {weights}")

        set_ma_weights_in_compose(compose_path, weights)
        subprocess.run(["docker", "compose", "up", "--build", "-d"], cwd=root,
                       check=True, capture_output=True)
        time.sleep(6)

        stats = run_quick_benchmark(root, args.profile, args.requests, args.concurrency, label)
        print(f"  → avg={stats['avg_ms']:.1f}ms  p95={stats['p95_ms']:.1f}ms  "
              f"p99={stats['p99_ms']:.1f}ms  success={stats['success_rate']:.0f}%")

        results.append({**weights, **stats})
        time.sleep(2)

    # Sort by avg latency
    results.sort(key=lambda x: x["avg_ms"])

    # Save
    os.makedirs(os.path.dirname(os.path.join(root, args.output)), exist_ok=True)
    out_path = os.path.join(root, args.output)
    with open(out_path, "w", newline="") as f:
        fieldnames = keys + ["avg_ms", "p95_ms", "p99_ms", "success_rate"]
        wr = csv.DictWriter(f, fieldnames=fieldnames)
        wr.writeheader()
        wr.writerows(results)

    # Print top 5
    print(f"\n{'=' * 80}")
    print(f"TOP 5 WEIGHT CONFIGURATIONS (by avg latency)")
    print(f"{'=' * 80}")
    for rank, r in enumerate(results[:5], 1):
        print(f"\n  #{rank}  avg={r['avg_ms']:.2f}ms  p95={r['p95_ms']:.2f}ms  p99={r['p99_ms']:.2f}ms")
        for k in keys:
            print(f"       {k}={r[k]}")

    print(f"\n{'=' * 80}")
    print(f"WORST CONFIGURATION")
    print(f"{'=' * 80}")
    worst = results[-1]
    print(f"  avg={worst['avg_ms']:.2f}ms  p95={worst['p95_ms']:.2f}ms")
    for k in keys:
        print(f"       {k}={worst[k]}")

    print(f"\nFull results: {out_path}")
    print(f"Total configs tested: {total}")


if __name__ == "__main__":
    main()
