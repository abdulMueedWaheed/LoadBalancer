from fastapi import FastAPI
from fastapi.responses import JSONResponse
import time
import random
import requests
import os
import threading

app = FastAPI()

NODE_ID        = os.environ.get("NODE_ID", "0")
CONTROLLER_URL = os.environ.get("CONTROLLER_URL", "http://controller:8000")
MIN_DELAY      = float(os.getenv("MIN_DELAY", "0.1"))
MAX_DELAY      = float(os.getenv("MAX_DELAY", "0.5"))

# ── Failure simulation config (all optional, default = no failures) ───────────
# Probability (0.0–1.0) that a request returns HTTP 500 (simulates a crash)
CRASH_RATE   = float(os.getenv("CRASH_RATE", "0.0"))
# Probability (0.0–1.0) that a request hangs for a long time (simulates timeout)
TIMEOUT_RATE = float(os.getenv("TIMEOUT_RATE", "0.0"))
# How long a "timed-out" request hangs (seconds) — should exceed controller timeout
TIMEOUT_HANG = float(os.getenv("TIMEOUT_HANG", "30.0"))
# After this many requests, the node "crashes" permanently (0 = never)
CRASH_AFTER  = int(os.getenv("CRASH_AFTER", "0"))

# ── State ─────────────────────────────────────────────────────────────────────
request_counter = 0
counter_lock    = threading.Lock()
permanently_crashed = False

@app.get("/")
def handle():
    global request_counter, permanently_crashed

    # Track request count
    with counter_lock:
        request_counter += 1
        current_count = request_counter

    # ── Permanent crash simulation ────────────────────────────────────────────
    if permanently_crashed:
        print(f"[node {NODE_ID}] PERMANENTLY CRASHED — rejecting request #{current_count}")
        return JSONResponse(
            status_code=500,
            content={"node": NODE_ID, "error": "node_crashed", "detail": "Node has permanently crashed"}
        )

    if CRASH_AFTER > 0 and current_count > CRASH_AFTER:
        permanently_crashed = True
        print(f"[node {NODE_ID}] CRASH_AFTER={CRASH_AFTER} reached — crashing permanently")
        return JSONResponse(
            status_code=500,
            content={"node": NODE_ID, "error": "node_crashed", "detail": f"Crashed after {CRASH_AFTER} requests"}
        )

    # ── Random timeout simulation ─────────────────────────────────────────────
    if TIMEOUT_RATE > 0 and random.random() < TIMEOUT_RATE:
        print(f"[node {NODE_ID}] Simulating TIMEOUT (hanging for {TIMEOUT_HANG}s)")
        time.sleep(TIMEOUT_HANG)
        return JSONResponse(
            status_code=504,
            content={"node": NODE_ID, "error": "timeout_simulated"}
        )

    # ── Random crash simulation ───────────────────────────────────────────────
    if CRASH_RATE > 0 and random.random() < CRASH_RATE:
        print(f"[node {NODE_ID}] Simulating CRASH (returning 500)")
        return JSONResponse(
            status_code=500,
            content={"node": NODE_ID, "error": "crash_simulated"}
        )

    # ── Normal processing ─────────────────────────────────────────────────────
    start = time.time()

    delay = random.uniform(MIN_DELAY, MAX_DELAY)
    time.sleep(delay)

    latency = (time.time() - start) * 1000

    # push metrics
    try:
        requests.post(f"{CONTROLLER_URL}/metrics", json={
            "node_id": NODE_ID,
            "latency": latency
        })
    except:
        pass

    return {"node": NODE_ID, "latency": latency}

@app.get("/health")
def health():
    """Health check endpoint."""
    if permanently_crashed:
        return JSONResponse(status_code=503, content={"status": "crashed"})
    return {"status": "healthy", "node": NODE_ID, "requests_handled": request_counter}