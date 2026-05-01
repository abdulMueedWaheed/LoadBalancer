from fastapi import FastAPI, HTTPException
import requests
import time
import csv
import uuid
import os
import threading
from abc import ABC, abstractmethod

app = FastAPI()

# ── Algorithm selection ────────────────────────────────────────────────────────
ALGORITHM = "least_connections"   # "round_robin" | "least_connections" | "ucb"

ALGO_PREFIX = {
    "round_robin":       "rr",
    "least_connections": "lc",
    "ucb":               "ucb",
}

# ── Log file paths (resolved at startup) ──────────────────────────────────────
LOG_FILE     = None   # e.g. logs/lc_logs.csv
FAILURE_FILE = None   # e.g. logs/lc_failures.csv

# ── Node registry ──────────────────────────────────────────────────────────────
nodes = [
    "http://node1:8000",
    "http://node2:8000",
    "http://node3:8000",
]

active_connections = {node: 0 for node in nodes}
node_request_count = {node: 0 for node in nodes}
connections_lock   = threading.Lock()

# ── In-memory metrics tracker ──────────────────────────────────────────────────
class MetricsTracker:
    def __init__(self):
        self._lock           = threading.Lock()
        self.total_requests  = 0
        self.failed_requests = 0
        self.latencies       = []        # ms, successful only
        self.start_time      = time.time()

    def record_success(self, latency_ms: float):
        with self._lock:
            self.total_requests += 1
            self.latencies.append(latency_ms)

    def record_failure(self):
        with self._lock:
            self.total_requests  += 1
            self.failed_requests += 1

    def snapshot(self):
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
    def select_node(self, active_nodes: list, connections: dict) -> str:
        pass

class RoundRobinStrategy(LoadBalancerStrategy):
    def __init__(self):
        self._index = 0
        self._lock  = threading.Lock()

    def select_node(self, active_nodes: list, connections: dict) -> str:
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

    def select_node(self, active_nodes: list, connections: dict) -> str:
        with self._lock:
            if not active_nodes:
                return None
            min_conn   = min(connections[n] for n in active_nodes)
            candidates = [n for n in active_nodes if connections[n] == min_conn]
            node = candidates[self._rr % len(candidates)]
            self._rr = (self._rr + 1) % len(candidates)
            return node

algorithms = {
    "round_robin":       RoundRobinStrategy(),
    "least_connections": LeastConnectionsStrategy(),
}

# ── CSV helpers ────────────────────────────────────────────────────────────────
def _append_main(row: list):
    with open(LOG_FILE, "a", newline="") as f:
        csv.writer(f).writerow(row)

def _append_failure(row: list):
    with open(FAILURE_FILE, "a", newline="") as f:
        csv.writer(f).writerow(row)

# ── Background metrics printer ─────────────────────────────────────────────────
METRICS_INTERVAL = 5  # seconds

def _live_metrics():
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
@app.on_event("startup")
def startup():
    global LOG_FILE, FAILURE_FILE

    os.makedirs("logs", exist_ok=True)
    prefix       = ALGO_PREFIX.get(ALGORITHM, ALGORITHM)
    LOG_FILE     = f"logs/{prefix}_logs.csv"
    FAILURE_FILE = f"logs/{prefix}_failures.csv"

    try:
        with open(LOG_FILE, "w", newline="") as f:
            csv.writer(f).writerow([
                "timestamp", "request_id", "node_selected",
                "latency_ms", "node_delay_ms", "algorithm",
                "success", "active_connections_at_dispatch",
                "node_request_count",
            ])
    except Exception as e:
        print("Main log init failed:", e)

    try:
        with open(FAILURE_FILE, "w", newline="") as f:
            csv.writer(f).writerow([
                "timestamp", "request_id", "node",
                "error_type", "error_message",
            ])
    except Exception as e:
        print("Failure log init failed:", e)

    threading.Thread(target=_live_metrics, daemon=True).start()
    print(f"[controller] algo={ALGORITHM}  log={LOG_FILE}  failures={FAILURE_FILE}")

# ── Route ──────────────────────────────────────────────────────────────────────
@app.get("/")
def route():
    request_id = str(uuid.uuid4())
    ts_start   = time.time()
    strategy   = algorithms.get(ALGORITHM, algorithms["round_robin"])

    tried_nodes = set()
    last_error  = None

    while len(tried_nodes) < len(nodes):
        remaining = [n for n in nodes if n not in tried_nodes]
        if not remaining:
            break

        with connections_lock:
            safe_conns = active_connections.copy()

        node = strategy.select_node(remaining, safe_conns)
        if not node:
            break

        with connections_lock:
            conns_at_dispatch = active_connections[node]
            active_connections[node] += 1

        try:
            res = requests.get(node, timeout=2.0)
            res.raise_for_status()
            data = res.json()

            latency_ms    = (time.time() - ts_start) * 1000
            node_delay_ms = data.get("latency", "")

            with connections_lock:
                node_request_count[node] += 1
                n_count = node_request_count[node]

            metrics.record_success(latency_ms)

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

            return data

        except requests.Timeout as e:
            last_error = str(e)
            _append_failure([
                time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
                request_id, node, "timeout", last_error,
            ])
            print(f"[TIMEOUT] {node}")
            tried_nodes.add(node)

        except requests.RequestException as e:
            last_error = str(e)
            _append_failure([
                time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
                request_id, node, type(e).__name__, last_error,
            ])
            print(f"[FAIL] {node}: {e}")
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
node_metrics: dict = {}

@app.post("/metrics")
def receive_metrics(data: dict):
    node_metrics[data.get("node_id", "?")] = data
    return {"status": "ok"}

# ── Live stats endpoint ────────────────────────────────────────────────────────
@app.get("/stats")
def get_stats():
    s = metrics.snapshot()
    with connections_lock:
        per_node = {
            n: {"active": active_connections[n], "handled": node_request_count[n]}
            for n in nodes
        }
    return {**s, "nodes": per_node, "algorithm": ALGORITHM}