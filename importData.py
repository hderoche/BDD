# -*- coding: utf-8 -*-
"""
Created on Mon Mar 15 14:52:30 2021

@author: malco
"""

import time
import requests
import json

with open('./jeuDeDonnees_1.log', 'r') as f:
    data = f.readlines()
for row in data:
    row = json.loads(row)
    requests.post('http://localhost:5001/api/insert', json=row)
    time.sleep(0.1)
    