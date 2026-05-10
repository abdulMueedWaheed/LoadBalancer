#!/usr/bin/env python3
import argparse
import csv
import glob
import math
import os
import subprocess
import time
from typing import Any


def run(cmd: list[str], cwd: str) -> None:
    subprocess.run(cmd, cwd=cwd, check=True)


def set_algorithm_in_compose(compose_path: str, algorithm: str) -> None:
    with open(compose_path, "r", encoding="utf-8") as f:
        content = f.read()

    lines = content.splitlines()
    updated: list[str] = []
    replaced = False
    for line in lines:
        if "ALGORITHM:" in line:
            indent = line[: len(line) - len(line.lstrip())]
            updated.append(f'{indent}ALGORITHM: "{algorithm}"')
            replaced = True
        else:
            updated.append(line)

    if not replaced:
        raise RuntimeError("Could not find ALGORITHM in docker-compose.yml")

    with open(compose_path, "w", encoding="utf-8") as f:
        f.write("\n".join(updated) + "\n")


def percentile(sorted_values: list[float], pct: float) -> float:
    if not sorted_values:
        return 0.0
    idx = int(math.ceil(pct / 100.0 * len(sorted_values))) - 1
    return sorted_values[max(0, idx)]


def summarize_client_csv(csv_path: str) -> dict[str, Any]:
    total = 0
    success = 0
    fail = 0
    lats_success: list[float] = []
    with open(csv_path, "r", encoding="utf-8") as f:
        rd = csv.DictReader(f)
        for row in rd:
            total += 1
            ok = str(row.get("success", "")).lower() == "true"
            lat = float(row.get("latency_ms", "0") or 0.0)
            if ok:
                success += 1
                lats_success.append(lat)
            else:
                fail += 1

    lats_success.sort()
    avg = sum(lats_success) / len(lats_success) if lats_success else 0.0
    return {
        "total": total,
        "success": success,
        "fail": fail,
        "success_rate": (success / total * 100.0) if total else 0.0,
        "avg_ms": avg,
        "p95_ms": percentile(lats_success, 95),
        "p99_ms": percentile(lats_success, 99),
        "max_ms": (lats_success[-1] if lats_success else 0.0),
    }


def find_latest_csv(log_dir: str, algo_prefix: str, profile: str, label: str) -> str:
    pattern = os.path.join(log_dir, f"client_{algo_prefix}_{profile}_{label}_run1.csv")
    matches = glob.glob(pattern)
    if not matches:
        raise FileNotFoundError(f"Expected client CSV not found: {pattern}")
    return matches[0]


def main() -> None:
    parser = argparse.ArgumentParser(description="Run algorithm x workload benchmark matrix.")
    parser.add_argument("--project-root", default=".", help="Project root containing docker-compose.yml")
    parser.add_argument("--requests", type=int, default=90)
    parser.add_argument("--concurrency", type=int, default=15)
    parser.add_argument("--pattern", default="steady", choices=["steady", "burst", "spike"])
    parser.add_argument("--repeat", type=int, default=3, help="Repeats per (algorithm, profile)")
    parser.add_argument("--label", default="matrix")
    parser.add_argument("--workload-file", default="workloads.json")
    parser.add_argument(
        "--algorithms",
        nargs="+",
        default=["least_connections", "ucb", "metric_aware"],
    )
    parser.add_argument(
        "--profiles",
        nargs="+",
        default=["db_point_light", "db_range_heavy", "db_aggregate_mid"],
    )
    parser.add_argument("--output", default="logs/matrix_summary.csv")
    args = parser.parse_args()

    root = os.path.abspath(args.project_root)
    compose_path = os.path.join(root, "docker-compose.yml")
    log_dir = os.path.join(root, "logs")

    algo_prefix = {
        "round_robin": "rr",
        "least_connections": "lc",
        "ucb": "ucb",
        "metric_aware": "metric_aware",
    }

    rows: list[dict[str, Any]] = []

    for algo in args.algorithms:
        if algo not in algo_prefix:
            raise ValueError(f"Unknown algorithm: {algo}")

        set_algorithm_in_compose(compose_path, algo)
        run(["docker", "compose", "up", "-d", "controller"], cwd=root)
        time.sleep(2.0)

        for profile in args.profiles:
            for run_num in range(1, args.repeat + 1):
                repeat_label = f"{args.label}rep{run_num}"
                run(
                    [
                        "python",
                        "client/main.py",
                        "--workload-profile",
                        profile,
                        "--workload-file",
                        args.workload_file,
                        "--requests",
                        str(args.requests),
                        "--concurrency",
                        str(args.concurrency),
                        "--pattern",
                        args.pattern,
                        "--repeat",
                        "1",
                        "--label",
                        repeat_label,
                    ],
                    cwd=root,
                )

                csv_path = find_latest_csv(
                    log_dir=log_dir,
                    algo_prefix=algo_prefix[algo],
                    profile=profile,
                    label=repeat_label,
                )
                s = summarize_client_csv(csv_path)
                rows.append(
                    {
                        "algorithm": algo,
                        "profile": profile,
                        "repeat_id": run_num,
                        "requests": args.requests,
                        "concurrency": args.concurrency,
                        "pattern": args.pattern,
                        **s,
                        "client_csv": os.path.basename(csv_path),
                    }
                )

    os.makedirs(os.path.dirname(os.path.join(root, args.output)), exist_ok=True)
    out_path = os.path.join(root, args.output)
    with open(out_path, "w", newline="", encoding="utf-8") as f:
        fieldnames = [
            "algorithm",
            "profile",
            "repeat_id",
            "requests",
            "concurrency",
            "pattern",
            "total",
            "success",
            "fail",
            "success_rate",
            "avg_ms",
            "p95_ms",
            "p99_ms",
            "max_ms",
            "client_csv",
        ]
        wr = csv.DictWriter(f, fieldnames=fieldnames)
        wr.writeheader()
        wr.writerows(rows)

    print(f"Matrix complete. Summary written to: {out_path}")


if __name__ == "__main__":
    main()
