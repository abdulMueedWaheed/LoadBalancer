#!/usr/bin/env python3
import argparse
import csv
import glob
import math
import os
from collections import defaultdict
from statistics import mean
from typing import Any


def percentile(sorted_values: list[float], pct: float) -> float:
    if not sorted_values:
        return 0.0
    idx = int(math.ceil((pct / 100.0) * len(sorted_values))) - 1
    return sorted_values[max(0, idx)]


def parse_filename(path: str) -> dict[str, Any] | None:
    """
    Expected examples:
      client_lc_db_point_light_matrix_run1.csv
      client_metric_aware_db_range_heavy_bench_run1.csv
      client_ucb_db_aggregate_mid_run1.csv
    """
    base = os.path.basename(path)
    if not (base.startswith("client_") and base.endswith(".csv")):
        return None

    core = base[len("client_") : -len(".csv")]
    if "_run" not in core:
        return None

    left, run_part = core.rsplit("_run", 1)
    if not run_part.isdigit():
        return None
    run_id = int(run_part)

    parts = left.split("_")
    if not parts:
        return None

    # algorithm extraction
    if parts[0] == "metric" and len(parts) > 1 and parts[1] == "aware":
        algorithm = "metric_aware"
        tail = parts[2:]
    elif parts[0] == "lc":
        algorithm = "least_connections"
        tail = parts[1:]
    elif parts[0] == "ucb":
        algorithm = "ucb"
        tail = parts[1:]
    elif parts[0] == "rr":
        algorithm = "round_robin"
        tail = parts[1:]
    else:
        # unknown prefix; skip
        return None

    if not tail:
        profile = "unknown"
        label = "none"
    elif len(tail) == 1:
        profile = tail[0]
        label = "none"
    else:
        # assume last segment is label, rest is profile
        profile = "_".join(tail[:-1])
        label = tail[-1]

    return {
        "algorithm": algorithm,
        "profile": profile,
        "label": label,
        "run_id": run_id,
    }


def summarize_client_csv(path: str) -> dict[str, float | int]:
    total = 0
    success = 0
    fail = 0
    lat_success: list[float] = []

    with open(path, "r", encoding="utf-8") as f:
        rd = csv.DictReader(f)
        for row in rd:
            total += 1
            ok = str(row.get("success", "")).lower() == "true"
            lat = float(row.get("latency_ms", "0") or 0)
            if ok:
                success += 1
                lat_success.append(lat)
            else:
                fail += 1

    lat_success.sort()
    avg_ms = mean(lat_success) if lat_success else 0.0
    p95_ms = percentile(lat_success, 95)
    p99_ms = percentile(lat_success, 99)
    max_ms = lat_success[-1] if lat_success else 0.0
    success_rate = (success / total * 100.0) if total else 0.0

    return {
        "total": total,
        "success": success,
        "fail": fail,
        "success_rate": success_rate,
        "avg_ms": avg_ms,
        "p95_ms": p95_ms,
        "p99_ms": p99_ms,
        "max_ms": max_ms,
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Build aggregate matrix CSV from client log files.")
    parser.add_argument("--logs-dir", default="logs", help="Directory containing client_*.csv files")
    parser.add_argument("--label", default="", help="Optional label filter (e.g. matrix, bench)")
    parser.add_argument(
        "--output",
        default="scripts/results_matrix.csv",
        help="Output CSV path for aggregated matrix",
    )
    args = parser.parse_args()

    pattern = os.path.join(args.logs_dir, "client_*.csv")
    files = sorted(glob.glob(pattern))
    if not files:
        raise FileNotFoundError(f"No client CSV files found with: {pattern}")

    per_run_rows: list[dict[str, Any]] = []
    aggregate_map: dict[tuple[str, str, str], list[dict[str, Any]]] = defaultdict(list)

    for path in files:
        parsed = parse_filename(path)
        if parsed is None:
            continue
        if args.label and not parsed["label"].startswith(args.label):
            continue

        stats = summarize_client_csv(path)
        row = {
            "algorithm": parsed["algorithm"],
            "profile": parsed["profile"],
            "label": parsed["label"],
            "run_id": parsed["run_id"],
            "source_file": os.path.basename(path),
            **stats,
        }
        per_run_rows.append(row)
        key = (parsed["algorithm"], parsed["profile"], parsed["label"])
        aggregate_map[key].append(row)

    if not per_run_rows:
        raise RuntimeError("No matching logs found after parsing/filtering.")

    # Write a single file with two sections: per-run rows then aggregate rows.
    os.makedirs(os.path.dirname(args.output) or ".", exist_ok=True)
    with open(args.output, "w", newline="", encoding="utf-8") as f:
        wr = csv.writer(f)

        wr.writerow(["section", "per_run"])
        wr.writerow(
            [
                "algorithm",
                "profile",
                "label",
                "run_id",
                "total",
                "success",
                "fail",
                "success_rate",
                "avg_ms",
                "p95_ms",
                "p99_ms",
                "max_ms",
                "source_file",
            ]
        )
        for r in sorted(per_run_rows, key=lambda x: (x["algorithm"], x["profile"], x["run_id"])):
            wr.writerow(
                [
                    r["algorithm"],
                    r["profile"],
                    r["label"],
                    r["run_id"],
                    r["total"],
                    r["success"],
                    r["fail"],
                    f'{r["success_rate"]:.3f}',
                    f'{r["avg_ms"]:.3f}',
                    f'{r["p95_ms"]:.3f}',
                    f'{r["p99_ms"]:.3f}',
                    f'{r["max_ms"]:.3f}',
                    r["source_file"],
                ]
            )

        wr.writerow([])
        wr.writerow(["section", "aggregate_mean_over_runs"])
        wr.writerow(
            [
                "algorithm",
                "profile",
                "label",
                "runs",
                "total_mean",
                "success_mean",
                "fail_mean",
                "success_rate_mean",
                "avg_ms_mean",
                "p95_ms_mean",
                "p99_ms_mean",
                "max_ms_mean",
            ]
        )
        for key in sorted(aggregate_map.keys()):
            algo, profile, label = key
            rows = aggregate_map[key]
            wr.writerow(
                [
                    algo,
                    profile,
                    label,
                    len(rows),
                    f'{mean([float(r["total"]) for r in rows]):.3f}',
                    f'{mean([float(r["success"]) for r in rows]):.3f}',
                    f'{mean([float(r["fail"]) for r in rows]):.3f}',
                    f'{mean([float(r["success_rate"]) for r in rows]):.3f}',
                    f'{mean([float(r["avg_ms"]) for r in rows]):.3f}',
                    f'{mean([float(r["p95_ms"]) for r in rows]):.3f}',
                    f'{mean([float(r["p99_ms"]) for r in rows]):.3f}',
                    f'{mean([float(r["max_ms"]) for r in rows]):.3f}',
                ]
            )

    print(f"Wrote matrix file: {args.output}")


if __name__ == "__main__":
    main()
