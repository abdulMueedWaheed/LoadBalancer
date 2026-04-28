import requests

for i in range(20):
    r = requests.get("http://localhost:8000")
    print(r.json())