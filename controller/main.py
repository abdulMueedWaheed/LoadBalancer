from fastapi import FastAPI, HTTPException
import requests
import time
import calendar
import csv
import uuid
import os
import math
import threading
from abc import ABC, abstractmethod
from typing import Any, Optional, cast
from contextlib import asynccontextmanager
from collections.abc import AsyncIterator
from collections import deque

# ── Algorithm selection ────────────────────────────────────────────────────────
DEFAULT_ALGORITHM = "least_connections"
ALGORITHM = os.getenv("ALGORITHM", DEFAULT_ALGORITHM).strip().lower()

ALGO_PREFIX: dict[str, str] = {
    "round_robin":       "rr",
    "least_connections": "lc",
    "ucb":               "ucb",
    "metric_aware":      "ma",
}

# ── Log file paths (resolved at startup) ──────────────────────────────────────
log_file_path: Optional[str] = None   # e.g. logs/lc_logs.csv
failure_file_path: Optional[str] = None   # e.g. logs/lc_failures.csv

# ── Node registry ──────────────────────────────────────────────────────────────
nodes: list[str] = [
    "http://node1:8000",
    "http://node2:8000",
    "http://node3:8000",
    "http://node4:8000",
    "http://node5:8000",
    "http://node6:8000",
]

active_connections: dict[str, int] = {node: 0 for node in nodes}
node_request_count: dict[str, int] = {node: 0 for node in nodes}
connections_lock   = threading.Lock()
FAILURE_WINDOW = 50
node_failure_window: dict[str, deque[int]] = {
    node: deque(maxlen=FAILURE_WINDOW) for node in nodes
}

# ── In-memory metrics tracker ──────────────────────────────────────────────────
class MetricsTracker:
    def __init__(self):
        self._lock           = threading.Lock()
        self.total_requests: int  = 0
        self.failed_requests: int = 0
        self.latencies: list[float] = []        # ms, successful only
        self.start_time: float = time.time()

    def record_success(self, latency_ms: float) -> None:
        with self._lock:
            self.total_requests += 1
            self.latencies.append(latency_ms)

    def record_failure(self) -> None:
        with self._lock:
            self.total_requests  += 1
            self.failed_requests += 1

    def snapshot(self) -> dict[str, float | int]:
        with self._lock:
            elapsed = max(time.time() - self.start_time, 1)
            total   = self.total_requests
            failed  = self.failed_requests
            tp      = total / elapsed
            lats    = sorted(self.latencies)
            avg_ms  = sum(lats) / len(lats) if lats else 0.0
            max_ms  = lats[-1]              if lats else 0.0
            p95_ms  = lats[int(len(lats) * 0.95)] if lats else 0.0
        return dict(total=total, failed=failed, throughput=tp,
                    avg_ms=avg_ms, max_ms=max_ms, p95_ms=p95_ms)

metrics = MetricsTracker()

# ── Strategy pattern ───────────────────────────────────────────────────────────
class LoadBalancerStrategy(ABC):
    @abstractmethod
    def select_node(self, active_nodes: list[str], connections: dict[str, int]) -> Optional[str]:
        pass

    def record_result(self, node: str, latency_ms: float, success: bool) -> None:
        """Called after each request so strategies can learn. Override if needed."""
        pass

class RoundRobinStrategy(LoadBalancerStrategy):
    def __init__(self):
        self._index = 0
        self._lock  = threading.Lock()

    def select_node(self, active_nodes: list[str], connections: dict[str, int]) -> Optional[str]:
        with self._lock:
            if not active_nodes:
                return None
            node = active_nodes[self._index % len(active_nodes)]
            self._index = (self._index + 1) % len(active_nodes)
            return node

class LeastConnectionsStrategy(LoadBalancerStrategy):
    def __init__(self):
        self._rr    = 0
        self._lock  = threading.Lock()

    def select_node(self, active_nodes: list[str], connections: dict[str, int]) -> Optional[str]:
        with self._lock:
            if not active_nodes:
                return None
            min_conn   = min(connections[n] for n in active_nodes)
            candidates = [n for n in active_nodes if connections[n] == min_conn]
            node = candidates[self._rr % len(candidates)]
            self._rr = (self._rr + 1) % len(candidates)
            return node

class UCBStrategy(LoadBalancerStrategy):
    """Upper Confidence Bound (UCB1) strategy.

    Reward definition:  reward = 1 / (1 + latency_ms / 1000)
        → lower latency  ⇒  higher reward  (close to 1.0)
        → higher latency  ⇒  lower reward   (approaches 0.0)
        → failure/timeout  ⇒  reward = 0.0

    Node score:  avg_reward + c * sqrt(ln(total_pulls) / node_pulls)
        The first term exploits fast nodes; the second explores underused ones.
    """

    def __init__(self, c: float = 1.414):
        self._lock        = threading.Lock()
        self._c           = c               # exploration constant (sqrt(2) by default)
        self._total_pulls: int = 0
        self._node_pulls: dict[str, int]  = {}              # node -> int
        self._node_reward: dict[str, float]  = {}           # node -> cumulative reward sum

    def _ensure_node(self, node: str) -> None:
        if node not in self._node_pulls:
            self._node_pulls[node]  = 0
            self._node_reward[node] = 0.0

    def select_node(self, active_nodes: list[str], connections: dict[str, int]) -> Optional[str]:
        with self._lock:
            if not active_nodes:
                return None

            for n in active_nodes:
                self._ensure_node(n)

            # Phase 1: try each node at least once (exploration bootstrap)
            for n in active_nodes:
                if self._node_pulls[n] == 0:
                    return n

            # Phase 2: UCB1 score
            best_node  = None
            best_score = -1.0

            for n in active_nodes:
                avg_reward = self._node_reward[n] / self._node_pulls[n]
                explore    = self._c * math.sqrt(
                    math.log(self._total_pulls) / self._node_pulls[n]
                )
                score = avg_reward + explore

                if score > best_score:
                    best_score = score
                    best_node  = n

            return best_node

    def record_result(self, node: str, latency_ms: float, success: bool) -> None:
        with self._lock:
            self._ensure_node(node)
            self._total_pulls += 1
            self._node_pulls[node] += 1

            if success:
                # reward ∈ (0, 1] — lower latency gives higher reward
                reward = 1.0 / (1.0 + latency_ms / 1000.0)
            else:
                reward = 0.0

            self._node_reward[node] += reward


class MetricAwareStrategy(LoadBalancerStrategy):
    """Weighted score strategy with stale-metric fallback behavior.

    Lower score is better.
    score = w1*active_connections + w2*(latency_ms/100) + w3*queue_depth + w4*failure_count_recent

    All weights configurable via env vars: MA_W_ACTIVE, MA_W_LATENCY, MA_W_QUEUE, MA_W_FAILURE,
    MA_STALE_PENALTY, MA_STALE_SECONDS.
    """

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._rr = 0
        self._w_active  = float(os.getenv("MA_W_ACTIVE",  "1.0"))
        self._w_latency = float(os.getenv("MA_W_LATENCY", "0.02"))
        self._w_queue   = float(os.getenv("MA_W_QUEUE",   "1.5"))
        self._w_failure = float(os.getenv("MA_W_FAILURE",  "2.0"))
        self._stale_penalty = float(os.getenv("MA_STALE_PENALTY", "3.0"))
        self._stale_after_seconds = float(os.getenv("MA_STALE_SECONDS", "15"))

    def _node_id_from_url(self, node_url: str) -> str:
        # http://node2:8000 -> "2"
        return node_url.split("//")[1].replace(":8000", "").replace("node", "")

    def _parse_ts(self, ts: Any) -> Optional[float]:
        if not isinstance(ts, str) or not ts:
            return None
        try:
            return float(calendar.timegm(time.strptime(ts, "%Y-%m-%dT%H:%M:%SZ")))
        except ValueError:
            return None

    def select_node(self, active_nodes: list[str], connections: dict[str, int]) -> Optional[str]:
        with self._lock:
            if not active_nodes:
                return None

            best_score = float("inf")
            candidates: list[str] = []
            now_epoch = time.time()

            for node in active_nodes:
                node_id = self._node_id_from_url(node)
                latest = node_metrics.get(node_id, {})
                raw_latency = latest.get("latency", metrics.snapshot().get("avg_ms", 0.0))
                raw_queue = latest.get("queue_depth", 0)
                raw_ts = latest.get("timestamp")

                latency_ms = float(raw_latency) if isinstance(raw_latency, (int, float)) else 0.0
                queue_depth = int(raw_queue) if isinstance(raw_queue, (int, float)) else 0
                failure_recent = sum(node_failure_window[node])
                active_conn = connections.get(node, 0)

                score = (
                    self._w_active * active_conn
                    + self._w_latency * (latency_ms / 100.0)
                    + self._w_queue * queue_depth
                    + self._w_failure * failure_recent
                )

                ts_epoch = self._parse_ts(raw_ts)
                if ts_epoch is None or (now_epoch - ts_epoch) > self._stale_after_seconds:
                    # stale feedback penalty; encourages fresher feedback nodes
                    score += self._stale_penalty

                if score < best_score:
                    best_score = score
                    candidates = [node]
                elif score == best_score:
                    candidates.append(node)

            # round-robin tie-break among equally scored nodes
            node = candidates[self._rr % len(candidates)]
            self._rr = (self._rr + 1) % len(candidates)
            return node


algorithms: dict[str, LoadBalancerStrategy] = {
    "round_robin":       RoundRobinStrategy(),
    "least_connections": LeastConnectionsStrategy(),
    "ucb":               UCBStrategy(),
    "metric_aware":      MetricAwareStrategy(),
}

# ── CSV helpers ────────────────────────────────────────────────────────────────
def _append_main(row: list[Any]) -> None:
    if log_file_path is None:
        return
    with open(log_file_path, "a", newline="") as f:
        csv.writer(f).writerow(row)

def _append_failure(row: list[Any]) -> None:
    if failure_file_path is None:
        return
    with open(failure_file_path, "a", newline="") as f:
        csv.writer(f).writerow(row)

# ── Background metrics printer ─────────────────────────────────────────────────
METRICS_INTERVAL = 5  # seconds

def _live_metrics() -> None:
    while True:
        time.sleep(METRICS_INTERVAL)
        s = metrics.snapshot()
        with connections_lock:
            conns  = dict(active_connections)
            counts = dict(node_request_count)

        print("\n" + "=" * 62)
        print(f"  LIVE METRICS  [{time.strftime('%H:%M:%S')}]  algo={ALGORITHM}")
        print("=" * 62)
        print(f"  Total requests : {s['total']}")
        print(f"  Failed requests: {s['failed']}")
        print(f"  Throughput     : {s['throughput']:.2f} req/s")
        print(f"  Avg latency    : {s['avg_ms']:.1f} ms")
        print(f"  Max latency    : {s['max_ms']:.1f} ms")
        print(f"  p95 latency    : {s['p95_ms']:.1f} ms")
        print("  ── Per-node ────────────────────────────────────────")
        for node in nodes:
            short = node.split("//")[1]
            print(f"  {short:<22} active={conns[node]}  handled={counts[node]}")
        print("=" * 62 + "\n")

# ── Startup ────────────────────────────────────────────────────────────────────
def initialize_runtime() -> None:
    global log_file_path, failure_file_path

    if ALGORITHM not in algorithms:
        valid = ", ".join(sorted(algorithms.keys()))
        raise RuntimeError(
            f"Invalid ALGORITHM='{ALGORITHM}'. Valid options: {valid}"
        )

    os.makedirs("logs", exist_ok=True)
    prefix       = ALGO_PREFIX.get(ALGORITHM, ALGORITHM)
    log_file_path     = f"logs/{prefix}_logs.csv"
    failure_file_path = f"logs/{prefix}_failures.csv"

    try:
        with open(log_file_path, "w", newline="") as f:
            csv.writer(f).writerow([
                "timestamp", "request_id", "node_selected",
                "latency_ms", "node_delay_ms", "algorithm",
                "success", "active_connections_at_dispatch",
                "node_request_count",
            ])
    except Exception as e:
        print("Main log init failed:", e)

    try:
        with open(failure_file_path, "w", newline="") as f:
            csv.writer(f).writerow([
                "timestamp", "request_id", "node",
                "error_type", "error_message",
            ])
    except Exception as e:
        print("Failure log init failed:", e)

    threading.Thread(target=_live_metrics, daemon=True).start()
    print(f"[controller] algo={ALGORITHM}  log={log_file_path}  failures={failure_file_path}")


@asynccontextmanager
async def lifespan(_: FastAPI) -> AsyncIterator[None]:
    initialize_runtime()
    yield


app = FastAPI(lifespan=lifespan)

# ── Route ──────────────────────────────────────────────────────────────────────
@app.get("/")
def route(
    task: str = "simulate",
    query_type: str = "point",
    key: int = 1,
    start_key: int = 1,
    end_key: int = 1000,
    limit: int = 100,
) -> dict[str, Any]:
    request_id = str(uuid.uuid4())
    ts_start   = time.time()
    strategy   = algorithms.get(ALGORITHM, algorithms["round_robin"])

    tried_nodes: set[str] = set()
    last_error: Optional[str] = None

    while len(tried_nodes) < len(nodes):
        remaining = [n for n in nodes if n not in tried_nodes]
        if not remaining:
            break

        with connections_lock:
            safe_conns = active_connections.copy()

        node: Optional[str] = strategy.select_node(remaining, safe_conns)
        if not node:
            break

        with connections_lock:
            conns_at_dispatch = active_connections[node]
            active_connections[node] += 1

        try:
            res = requests.get(
                node,
                timeout=1.0,
                params={
                    "task": task,
                    "query_type": query_type,
                    "key": key,
                    "start_key": start_key,
                    "end_key": end_key,
                    "limit": limit,
                },
            )
            res.raise_for_status()
            data = res.json()
            if not isinstance(data, dict):
                raise requests.RequestException("Unexpected non-object JSON from node")
            payload = cast(dict[str, Any], data)

            latency_ms    = (time.time() - ts_start) * 1000
            node_delay_ms: Any = payload.get("latency", "")

            with connections_lock:
                node_request_count[node] += 1
                n_count = node_request_count[node]

            metrics.record_success(latency_ms)
            strategy.record_result(node, latency_ms, success=True)
            node_failure_window[node].append(0)

            _append_main([
                time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
                request_id,
                node,
                round(latency_ms, 3),
                round(node_delay_ms, 3) if isinstance(node_delay_ms, (int, float)) else "",
                ALGORITHM,
                "success",
                conns_at_dispatch,
                n_count,
            ])

            return payload

        except requests.Timeout as e:
            last_error = str(e)
            _append_failure([
                time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
                request_id, node, "timeout", last_error,
            ])
            print(f"[TIMEOUT] {node}")
            strategy.record_result(node, 6000.0, success=False)
            node_failure_window[node].append(1)
            tried_nodes.add(node)

        except requests.RequestException as e:
            last_error = str(e)
            _append_failure([
                time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
                request_id, node, type(e).__name__, last_error,
            ])
            print(f"[FAIL] {node}: {e}")
            strategy.record_result(node, 6000.0, success=False)
            node_failure_window[node].append(1)
            tried_nodes.add(node)

        finally:
            with connections_lock:
                if active_connections[node] > 0:
                    active_connections[node] -= 1

    # All nodes exhausted
    metrics.record_failure()
    _append_main([
        time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        request_id, "none", "", "", ALGORITHM, "failure", "", "",
    ])
    raise HTTPException(status_code=503, detail=f"All nodes failed: {last_error}")

# ── Node metrics push endpoint ─────────────────────────────────────────────────
node_metrics: dict[str, dict[str, Any]] = {}

@app.post("/metrics")
def receive_metrics(data: dict[str, Any]) -> dict[str, str]:
    node_key = str(data.get("node_id", "?"))
    node_metrics[node_key] = data
    return {"status": "ok"}

# ── Live stats endpoint ────────────────────────────────────────────────────────
@app.get("/stats")
def get_stats() -> dict[str, Any]:
    s = metrics.snapshot()
    with connections_lock:
        per_node = {
            n: {
                "active": active_connections[n],
                "handled": node_request_count[n],
                "failure_count_recent": sum(node_failure_window[n]),
                "latest_metrics": node_metrics.get(n.split("//")[1].replace(":8000", "").replace("node", ""), {}),
            }
            for n in nodes
        }
    return {**s, "nodes": per_node, "algorithm": ALGORITHM}
