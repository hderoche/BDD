import json
import time
import requests
with open('./jeuDeDonnees_1.log', 'r') as f:
    data = f.readlines()
    print(data)
    print(data[0])
for row in data: 
    row = json.loads(row)
    requests.post('http://localhost:5001/api/insert', json=row)
    time.sleep(0.1)