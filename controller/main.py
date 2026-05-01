from fastapi import FastAPI
import requests
import time
import csv
import uuid
import os
import random

ALGORITHM = "round_robin"
app = FastAPI()

LOG_FILE = "logs/logs.csv"

@app.on_event("startup")
def init_log():
    os.makedirs("logs", exist_ok=True)

    try:
        with open(LOG_FILE, "w", newline="") as f:
            import csv
            writer = csv.writer(f)
            writer.writerow([
                "timestamp",
                "request_id",
                "node_id",
                "latency_ms",
                "algorithm"
            ])
    except Exception as e:
        print("Logging init failed:", e)


active_connections = {
    "http://node1:8000": 0,
    "http://node2:8000": 0,
    "http://node3:8000": 0
}

nodes = [
    "http://node1:8000",
    "http://node2:8000",
    "http://node3:8000"
]

index = 0

def select_node():
    global index

    if ALGORITHM == "round_robin":
        node = nodes[index]
        index = (index + 1) % len(nodes)
        return node

    elif ALGORITHM == "least_connections":

        min_conn = min(active_connections.values())

        candidates = [
            node for node, count in active_connections.items()
            if count == min_conn
        ]

        return random.choice(candidates)

# store metrics
node_metrics = {}

@app.get("/")
def route():

    request_id = str(uuid.uuid4())
    start = time.time()

    node = select_node()

    try:
        active_connections[node] += 1

        res = requests.get(node)
        data = res.json()

        latency = (time.time() - start) * 1000

        with open(LOG_FILE, "a", newline="") as f:
            writer = csv.writer(f)
            writer.writerow([
                time.time(),
                request_id,
                node,
                latency,
                ALGORITHM
            ])

        return data

    except Exception as e:
        print("ERROR:", e)
        return {"error": str(e)}

    finally:
        if active_connections[node] > 0:
            active_connections[node] -= 1

@app.post("/metrics")
def receive_metrics(data: dict):
    node_id = data["node_id"]
    node_metrics[node_id] = data
    return {"status": "ok"}