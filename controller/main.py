from fastapi import FastAPI
import requests
import time
import csv
import uuid
import os


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

nodes = [
    "http://node1:8000",
    "http://node2:8000",
    "http://node3:8000"
]

index = 0

# store metrics
node_metrics = {}

@app.get("/")
def route():
    global index

    request_id = str(uuid.uuid4())
    start = time.time()

    node = nodes[index]
    node_id = index + 1
    index = (index + 1) % len(nodes)

    try:
        res = requests.get(node)
        data = res.json()
        latency = (time.time() - start) * 1000

        # log it
        with open(LOG_FILE, "a", newline="") as f:
            writer = csv.writer(f)
            writer.writerow([
                time.time(),
                request_id,
                node_id,
                latency,
                "round_robin"
            ])

        return data

    except:
        return {"error": "node failed"}

@app.post("/metrics")
def receive_metrics(data: dict):
    node_id = data["node_id"]
    node_metrics[node_id] = data
    return {"status": "ok"}