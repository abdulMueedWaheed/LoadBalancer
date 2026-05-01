from fastapi import FastAPI, HTTPException
import requests
import time
import csv
import uuid
import os
import random
import threading
from abc import ABC, abstractmethod

app = FastAPI()

LOG_FILE = "logs/logs.csv"

@app.on_event("startup")
def init_log():
    os.makedirs("logs", exist_ok=True)
    try:
        with open(LOG_FILE, "w", newline="") as f:
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

active_connections = {node: 0 for node in nodes}
connections_lock = threading.Lock()

class LoadBalancerStrategy(ABC):
    @abstractmethod
    def select_node(self, active_nodes: list, connections: dict) -> str:
        pass

class RoundRobinStrategy(LoadBalancerStrategy):
    def __init__(self):
        self.index = 0
        self.lock = threading.Lock()

    def select_node(self, active_nodes: list, connections: dict) -> str:
        with self.lock:
            if not active_nodes:
                return None
            node = active_nodes[self.index % len(active_nodes)]
            self.index = (self.index + 1) % len(active_nodes)
            return node

class LeastConnectionsStrategy(LoadBalancerStrategy):
    def __init__(self):
        self.rr_index = 0
        self.lock = threading.Lock()

    def select_node(self, active_nodes: list, connections: dict) -> str:
        with self.lock:
            if not active_nodes:
                return None
            
            min_conn = min(connections[node] for node in active_nodes)
            candidates = [node for node in active_nodes if connections[node] == min_conn]
            
            node = candidates[self.rr_index % len(candidates)]
            self.rr_index = (self.rr_index + 1) % len(candidates)
            return node

algorithms = {
    "round_robin": RoundRobinStrategy(),
    "least_connections": LeastConnectionsStrategy()
}

ALGORITHM = "least_connections"  # Can be easily changed here

# store metrics
node_metrics = {}

@app.get("/")
def route():
    request_id = str(uuid.uuid4())
    start = time.time()
    
    current_algorithm = algorithms.get(ALGORITHM, algorithms["least_connections"])
    
    tried_nodes = set()
    last_error = None
    
    while len(tried_nodes) < len(nodes):
        active_nodes = [n for n in nodes if n not in tried_nodes]
        if not active_nodes:
            break
            
        with connections_lock:
            safe_connections = active_connections.copy()
            
        node = current_algorithm.select_node(active_nodes, safe_connections)
        if not node:
            break
            
        try:
            with connections_lock:
                active_connections[node] += 1
                
            res = requests.get(node, timeout=2.0)
            res.raise_for_status()
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
            
        except requests.RequestException as e:
            print(f"Node {node} failed: {e}")
            tried_nodes.add(node)
            last_error = str(e)
        finally:
            with connections_lock:
                if active_connections[node] > 0:
                    active_connections[node] -= 1
                    
    raise HTTPException(status_code=503, detail=f"All nodes failed. Last error: {last_error}")

@app.post("/metrics")
def receive_metrics(data: dict):
    node_id = data["node_id"]
    node_metrics[node_id] = data
    return {"status": "ok"}