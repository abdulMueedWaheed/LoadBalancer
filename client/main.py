import requests
import threading
import time

def send(req_id):
    start_time = time.time()
    try:
        r = requests.get("http://localhost:8000")
        elapsed = time.time() - start_time
        print(f"[Req {req_id}] STATUS: {r.status_code} | TIME: {elapsed:.3f}s | BODY: {r.text.strip()}")
    except Exception as e:
        elapsed = time.time() - start_time
        print(f"[Req {req_id}] FAILED: {e} | TIME: {elapsed:.3f}s")

threads = []

print("Sending 20 concurrent requests to the Load Balancer...")
for i in range(20):
    t = threading.Thread(target=send, args=(i+1,))
    t.start()
    threads.append(t)

for t in threads:
    t.join()
print("Test completed.")