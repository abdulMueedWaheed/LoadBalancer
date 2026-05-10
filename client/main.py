"""
Load Balancer Stress-Test Client
================================
Configurable experiment runner with traffic patterns, CSV logging,
and post-run summary statistics.

Usage:
    python main.py                                          # defaults
    python main.py --requests 100 --concurrency 20 --label rr
    python main.py --pattern burst --requests 50 --label lc
    python main.py --repeat 3 --pattern steady --requests 60 --label ucb
"""

import argparse
import csv
import math
import os
import random
import time
from collections import Counter
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any, Callable, TypedDict, cast

import requests

# ── Defaults ──────────────────────────────────────────────────────────────────
DEFAULT_URL         = "http://localhost:8000"
DEFAULT_REQUESTS    = 20
DEFAULT_CONCURRENCY = 10
DEFAULT_INTERVAL    = 0.0      # seconds between request submissions
DEFAULT_TIMEOUT     = 10.0     # per-request timeout (should be > controller's 2s node timeout)
DEFAULT_PATTERN     = "steady" # steady | burst | spike
DEFAULT_LABEL       = "run"
DEFAULT_REPEAT      = 1
LOG_DIR             = "logs"
ALGO_SHORT = {
    "round_robin": "rr",
    "least_connections": "lc",
    "ucb": "ucb",
}


class RequestResult(TypedDict):
    req_id: int
    timestamp: str
    status_code: int
    latency_ms: float
    node: str
    success: bool
    error: str


SchedulerFn = Callable[[int, int, float, str, float], list[RequestResult]]

# ── Request worker ────────────────────────────────────────────────────────────
def _send_request(req_id: int, url: str, timeout: float) -> RequestResult:
    """Send a single request and return a result dict."""
    start = time.time()
    result: RequestResult = {
        "req_id":       req_id,
        "timestamp":    time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "status_code":  0,
        "latency_ms":   0.0,
        "node":         "",
        "success":      False,
        "error":        "",
    }
    try:
        r = requests.get(url, timeout=timeout)
        elapsed = (time.time() - start) * 1000
        result["status_code"] = r.status_code
        result["latency_ms"]  = round(elapsed, 3)

        if r.status_code == 200:
            body = r.json()
            if isinstance(body, dict):
                body_dict = cast(dict[str, Any], body)
                result["node"] = str(body_dict.get("node", ""))
            else:
                result["node"] = ""
            result["success"] = True
        else:
            result["error"] = f"HTTP {r.status_code}"

    except requests.Timeout:
        result["latency_ms"] = round((time.time() - start) * 1000, 3)
        result["error"]      = "timeout"
    except requests.ConnectionError:
        result["latency_ms"] = round((time.time() - start) * 1000, 3)
        result["error"]      = "connection_refused"
    except Exception as e:
        result["latency_ms"] = round((time.time() - start) * 1000, 3)
        result["error"]      = str(e)

    return result


# Traffic-pattern schedulers 
def _schedule_steady(n_requests: int, concurrency: int, interval: float,
                     url: str, timeout: float) -> list[RequestResult]:
    """Even, steady stream of requests."""
    results: list[RequestResult] = []
    with ThreadPoolExecutor(max_workers=concurrency) as pool:
        futures: dict[Any, int] = {}
        for i in range(1, n_requests + 1):
            f = pool.submit(_send_request, i, url, timeout)
            futures[f] = i
            if interval > 0:
                time.sleep(interval)

        for f in as_completed(futures):
            results.append(f.result())
    return results


def _schedule_burst(n_requests: int, concurrency: int, _interval: float,
                    url: str, timeout: float) -> list[RequestResult]:
    """All requests fired at once with maximum concurrency."""
    results: list[RequestResult] = []
    with ThreadPoolExecutor(max_workers=n_requests) as pool:
        futures = {pool.submit(_send_request, i, url, timeout): i
                   for i in range(1, n_requests + 1)}
        for f in as_completed(futures):
            results.append(f.result())
    return results


def _schedule_spike(n_requests: int, concurrency: int, _interval: float,
                    url: str, timeout: float) -> list[RequestResult]:
    """Alternates between calm periods (low rate) and random spikes
    (high concurrency bursts)."""
    results: list[RequestResult] = []
    sent = 0
    req_id = 1

    while sent < n_requests:
        # Calm phase: small batch with delay
        calm_size = min(random.randint(2, max(3, concurrency // 3)), n_requests - sent)
        with ThreadPoolExecutor(max_workers=calm_size) as pool:
            futures = {pool.submit(_send_request, req_id + j, url, timeout): j
                       for j in range(calm_size)}
            for f in as_completed(futures):
                results.append(f.result())
        sent   += calm_size
        req_id += calm_size
        if sent >= n_requests:
            break
        time.sleep(random.uniform(0.3, 0.8))  # calm pause

        # Spike phase: big burst
        spike_size = min(random.randint(concurrency, concurrency * 2), n_requests - sent)
        with ThreadPoolExecutor(max_workers=spike_size) as pool:
            futures = {pool.submit(_send_request, req_id + j, url, timeout): j
                       for j in range(spike_size)}
            for f in as_completed(futures):
                results.append(f.result())
        sent   += spike_size
        req_id += spike_size
        if sent >= n_requests:
            break
        time.sleep(random.uniform(0.05, 0.2))  # short pause before next cycle

    return results


PATTERNS: dict[str, SchedulerFn] = {
    "steady": _schedule_steady,
    "burst":  _schedule_burst,
    "spike":  _schedule_spike,
}


def _fetch_algorithm_code(base_url: str, timeout: float = 2.0) -> str:
    """Fetch active algorithm from controller /stats; fallback to unknown."""
    stats_url = f"{base_url.rstrip('/')}/stats"
    try:
        res = requests.get(stats_url, timeout=timeout)
        res.raise_for_status()
        payload = res.json()
        if isinstance(payload, dict):
            payload_dict = cast(dict[str, Any], payload)
            algo = str(payload_dict.get("algorithm", "")).strip().lower()
            if algo:
                return ALGO_SHORT.get(algo, algo)
    except Exception:
        pass
    return "unknown"


# ── CSV logging ───────────────────────────────────────────────────────────────
def _save_csv(results: list[RequestResult], label: str, run_num: int) -> str:
    os.makedirs(LOG_DIR, exist_ok=True)
    filename = os.path.join(LOG_DIR, f"client_{label}_run{run_num}.csv")
    with open(filename, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow([
            "timestamp", "req_id", "status_code",
            "latency_ms", "node", "success", "error",
        ])
        for r in sorted(results, key=lambda x: x["req_id"]):
            writer.writerow([
                r["timestamp"], r["req_id"], r["status_code"],
                r["latency_ms"], r["node"], r["success"], r["error"],
            ])
    return filename


# ── Summary statistics ────────────────────────────────────────────────────────
def _percentile(sorted_values: list[float], pct: float) -> float:
    if not sorted_values:
        return 0.0
    idx = int(math.ceil(pct / 100.0 * len(sorted_values))) - 1
    return sorted_values[max(0, idx)]


def _print_summary(results: list[RequestResult], wall_time: float, label: str, run_num: int) -> None:
    total     = len(results)
    successes = [r for r in results if r["success"]]
    failures  = [r for r in results if not r["success"]]
    success_rate = len(successes) / total * 100 if total else 0

    lats_success = sorted(r["latency_ms"] for r in successes)

    avg_lat = sum(lats_success) / len(lats_success) if lats_success else 0
    max_lat = lats_success[-1] if lats_success else 0
    p95_lat = _percentile(lats_success, 95)
    p99_lat = _percentile(lats_success, 99)

    throughput = total / wall_time if wall_time > 0 else 0

    node_counts = Counter(r["node"] for r in successes)
    error_counts = Counter(r["error"] for r in failures)

    w = 60
    print("\n" + "=" * w)
    print(f"  EXPERIMENT SUMMARY — {label.upper()} run #{run_num}")
    print("=" * w)
    print(f"  Total requests   : {total}")
    print(f"  Successful       : {len(successes)}")
    print(f"  Failed           : {len(failures)}")
    print(f"  Success rate     : {success_rate:.1f}%")
    print(f"  Wall-clock time  : {wall_time:.3f}s")
    print(f"  Throughput       : {throughput:.2f} req/s")
    print("-" * w)
    print(f"  Avg latency      : {avg_lat:.1f} ms")
    print(f"  Max latency      : {max_lat:.1f} ms")
    print(f"  p95 latency      : {p95_lat:.1f} ms")
    print(f"  p99 latency      : {p99_lat:.1f} ms")
    print("-" * w)
    print("  Node distribution:")
    for node, cnt in sorted(node_counts.items()):
        pct = cnt / len(successes) * 100 if successes else 0
        bar = "█" * int(pct / 2)
        print(f"    node {node:>3}: {cnt:>5} ({pct:5.1f}%) {bar}")
    if error_counts:
        print("-" * w)
        print("  Errors:")
        for err, cnt in error_counts.most_common():
            print(f"    {err}: {cnt}")
    print("=" * w + "\n")


# ── Main ──────────────────────────────────────────────────────────────────────
def run_experiment(args: argparse.Namespace) -> None:
    scheduler: SchedulerFn = PATTERNS.get(args.pattern, _schedule_steady)
    url = str(args.url)
    requests_count = int(args.requests)
    concurrency = int(args.concurrency)
    interval = float(args.interval)
    timeout = float(args.timeout)
    pattern = str(args.pattern)
    label = str(args.label)
    repeat = int(args.repeat)

    algo_code = _fetch_algorithm_code(url)
    effective_label = algo_code if label == DEFAULT_LABEL else f"{algo_code}_{label}"

    for run_num in range(1, repeat + 1):
        if repeat > 1:
            print(f"\n{'─' * 50}")
            print(f"  Starting run {run_num}/{repeat}")
            print(f"{'─' * 50}")

        print(f"[{effective_label}] Sending {requests_count} requests "
              f"(concurrency={concurrency}, pattern={pattern})...")

        wall_start = time.time()
        results: list[RequestResult] = scheduler(
            requests_count, concurrency, interval, url, timeout
        )
        wall_time = time.time() - wall_start

        # Live per-request output
        for r in sorted(results, key=lambda x: x["req_id"]):
            status = "OK" if r["success"] else f"FAIL({r['error']})"
            print(f"  [Req {r['req_id']:>4}] {status:>20} | "
                  f"node={r['node'] or '-':>3} | "
                  f"{r['latency_ms']:>8.1f} ms")

        csv_path = _save_csv(results, effective_label, run_num)
        print(f"  Results saved to {csv_path}")

        _print_summary(results, wall_time, effective_label, run_num)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Load Balancer Stress-Test Client",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python main.py --requests 100 --concurrency 20
  python main.py --pattern burst --requests 50 --label fault_test
  python main.py --repeat 3 --pattern spike --requests 80 --label high_load
        """,
    )
    _: Any = parser.add_argument("--url",         default=DEFAULT_URL,         help="Load balancer URL")
    _: Any = parser.add_argument("--requests",    type=int, default=DEFAULT_REQUESTS,    help="Total number of requests")
    _: Any = parser.add_argument("--concurrency", type=int, default=DEFAULT_CONCURRENCY, help="Max concurrent threads")
    _: Any = parser.add_argument("--interval",    type=float, default=DEFAULT_INTERVAL,  help="Seconds between request submissions (steady pattern)")
    _: Any = parser.add_argument("--timeout",     type=float, default=DEFAULT_TIMEOUT,   help="Per-request timeout in seconds")
    _: Any = parser.add_argument("--pattern",     choices=list(PATTERNS.keys()), default=DEFAULT_PATTERN, help="Traffic pattern")
    _: Any = parser.add_argument("--label",       default=DEFAULT_LABEL,       help="Optional custom suffix for experiment label")
    _: Any = parser.add_argument("--repeat",      type=int, default=DEFAULT_REPEAT,      help="Number of times to repeat the experiment")
    args = parser.parse_args()

    run_experiment(args)


if __name__ == "__main__":
    main()
