import requests
import threading

def send():
    r = requests.get("http://localhost:8000")

    print("STATUS:", r.status_code)
    print("BODY:", r.text)

threads = []

for _ in range(20):
    t = threading.Thread(target=send)
    t.start()
    threads.append(t)

for t in threads:
    t.join()