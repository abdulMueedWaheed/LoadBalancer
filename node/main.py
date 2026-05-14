from fastapi import FastAPI
from fastapi.responses import JSONResponse
import time
import random
import requests
import os
import threading
import sqlite3
from typing import Any

app = FastAPI()

NODE_ID        = os.environ.get("NODE_ID", "0")
CONTROLLER_URL = os.environ.get("CONTROLLER_URL", "http://controller:8000")
MIN_DELAY      = float(os.getenv("MIN_DELAY", "0.1"))
MAX_DELAY      = float(os.getenv("MAX_DELAY", "0.5"))

# ── Failure simulation config ─────────────────────────────────────────────────
CRASH_RATE   = float(os.getenv("CRASH_RATE", "0.0"))
TIMEOUT_RATE = float(os.getenv("TIMEOUT_RATE", "0.0"))
TIMEOUT_HANG = float(os.getenv("TIMEOUT_HANG", "5.0"))
CRASH_AFTER  = int(os.getenv("CRASH_AFTER", "0"))

# ── State ─────────────────────────────────────────────────────────────────────
request_counter = 0
counter_lock    = threading.Lock()
permanently_crashed = False
in_flight_requests = 0

# ── SQLite workload config ────────────────────────────────────────────────────
DB_PATH = os.getenv("DB_PATH", f"/tmp/node_{NODE_ID}.db")
SEED_ROWS = int(os.getenv("DB_SEED_ROWS", "50000"))
DB_LOCK = threading.Lock()


def _init_db() -> None:
    """Initialize database with synthetic items."""
    with DB_LOCK:
        conn = sqlite3.connect(DB_PATH)
        cur = conn.cursor()
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS items (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                shard_key INTEGER NOT NULL,
                value TEXT NOT NULL,
                score REAL NOT NULL,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            )
            """
        )
        cur.execute("CREATE INDEX IF NOT EXISTS idx_items_shard_key ON items(shard_key)")

        cur.execute("SELECT COUNT(*) FROM items")
        row_count = int(cur.fetchone()[0])
        
        if row_count < SEED_ROWS:
            batch = []
            start = row_count + 1
            for i in range(start, SEED_ROWS + 1):
                batch.append((
                    i,
                    f"value_node{NODE_ID}_{i}",
                    float((i % 1000) / 10.0),
                    time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
                    time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
                ))
                if len(batch) == 1000:
                    cur.executemany(
                        "INSERT INTO items(shard_key, value, score, created_at, updated_at) VALUES (?, ?, ?, ?, ?)",
                        batch,
                    )
                    batch = []
            if batch:
                cur.executemany(
                    "INSERT INTO items(shard_key, value, score, created_at, updated_at) VALUES (?, ?, ?, ?, ?)",
                    batch,
                )
            conn.commit()
        conn.close()

def _run_db_read(query_type: str, key: int, start_key: int, end_key: int, limit: int) -> tuple[int, dict[str, Any]]:
    """Execute read query. Returns (status_code, response_dict)."""
    qtype = query_type.strip().lower()
    
    with DB_LOCK:
        conn = sqlite3.connect(DB_PATH)
        cur = conn.cursor()
        t0 = time.time()

        try:
            if qtype == "point":
                cur.execute(
                    "SELECT id, shard_key, value, score FROM items WHERE shard_key = ? LIMIT 1",
                    (key,),
                )
                rows = cur.fetchall()
                elapsed_ms = (time.time() - t0) * 1000
                return (200, {
                    "query_type": "point",
                    "rows_returned": len(rows),
                    "sample": rows[0] if rows else None,
                    "db_query_ms": round(elapsed_ms, 3),
                })

            if qtype == "range":
                cur.execute(
                    "SELECT id, shard_key, value, score FROM items WHERE shard_key BETWEEN ? AND ? LIMIT ?",
                    (start_key, end_key, limit),
                )
                rows = cur.fetchall()
                elapsed_ms = (time.time() - t0) * 1000
                return (200, {
                    "query_type": "range",
                    "rows_returned": len(rows),
                    "sample": rows[:3],
                    "db_query_ms": round(elapsed_ms, 3),
                })

            # aggregate
            cur.execute(
                "SELECT COUNT(*), AVG(score), MIN(score), MAX(score) FROM items WHERE shard_key BETWEEN ? AND ?",
                (start_key, end_key),
            )
            count, avg_score, min_score, max_score = cur.fetchone()
            elapsed_ms = (time.time() - t0) * 1000
            return (200, {
                "query_type": "aggregate",
                "rows_scanned_estimate": int(count or 0),
                "avg_score": float(avg_score or 0.0),
                "min_score": float(min_score or 0.0),
                "max_score": float(max_score or 0.0),
                "db_query_ms": round(elapsed_ms, 3),
            })
        finally:
            conn.close()


@app.on_event("startup")
def startup() -> None:
    _init_db()
    print(f"[node {NODE_ID}] seeded_rows={SEED_ROWS}")

@app.get("/")
def handle(
    task: str = "simulate",
    query_type: str = "point",
    key: int = 1,
    start_key: int = 1,
    end_key: int = 1000,
    limit: int = 100,
):
    global request_counter, permanently_crashed, in_flight_requests

    with counter_lock:
        request_counter += 1
        current_count = request_counter

    if permanently_crashed:
        print(f"[node {NODE_ID}] PERMANENTLY CRASHED — rejecting request #{current_count}")
        return JSONResponse(
            status_code=500,
            content={"node": NODE_ID, "error": "node_crashed"}
        )

    if CRASH_AFTER > 0 and current_count > CRASH_AFTER:
        permanently_crashed = True
        print(f"[node {NODE_ID}] CRASH_AFTER={CRASH_AFTER} reached — crashing permanently")
        return JSONResponse(
            status_code=500,
            content={"node": NODE_ID, "error": "node_crashed"}
        )

    with counter_lock:
        in_flight_requests += 1

    try:
        if TIMEOUT_RATE > 0 and random.random() < TIMEOUT_RATE:
            print(f"[node {NODE_ID}] Simulating TIMEOUT")
            time.sleep(TIMEOUT_HANG)
            return JSONResponse(status_code=504, content={"node": NODE_ID, "error": "timeout"})

        if CRASH_RATE > 0 and random.random() < CRASH_RATE:
            print(f"[node {NODE_ID}] Simulating CRASH")
            return JSONResponse(status_code=500, content={"node": NODE_ID, "error": "crash_simulated"})

        start = time.time()

        task_lower = task.strip().lower()
        if task_lower == "db_query":
            status_code, response_data = _run_db_read(query_type, key, start_key, end_key, limit)
            if status_code != 200:
                return JSONResponse(status_code=status_code, content={"node": NODE_ID, **response_data})
        else:
            # Simulate delay
            delay = random.uniform(MIN_DELAY, MAX_DELAY)
            time.sleep(delay)
            response_data = {"simulated_delay_ms": round(delay * 1000, 3)}

        latency = (time.time() - start) * 1000
        with counter_lock:
            current_queue_depth = in_flight_requests

        # Push metrics to controller
        try:
            requests.post(f"{CONTROLLER_URL}/metrics", json={
                "node_id": NODE_ID,
                "latency": latency,
                "task_type": task_lower,
                "queue_depth": current_queue_depth,
                "timestamp": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
            }, timeout=2)
        except Exception:
            pass

        return {
            "node": NODE_ID,
            "latency": latency,
            "task": task_lower,
            **response_data,
        }
    finally:
        with counter_lock:
            if in_flight_requests > 0:
                in_flight_requests -= 1


@app.get("/health")
def health():
    if permanently_crashed:
        return JSONResponse(status_code=503, content={"status": "crashed"})
    return {
        "status": "healthy",
        "node": NODE_ID,
        "requests_handled": request_counter,
    }
