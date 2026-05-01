from fastapi import FastAPI
import time
import random
import requests
import os

app = FastAPI()

NODE_ID = os.environ.get("NODE_ID", "0")
CONTROLLER_URL = os.environ.get("CONTROLLER_URL", "http://controller:8000")
MIN_DELAY = float(os.getenv("MIN_DELAY"))
MAX_DELAY = float(os.getenv("MAX_DELAY"))

@app.get("/")
def handle():
    start = time.time()

    # simulate variable load
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