# -*- coding: utf-8 -*-
"""
Created on Mon Mar 15 15:35:31 2021

@author: malco
"""

import pymongo
import json
import mysql.connector
import collections
import io
import dns
from datetime import datetime

with open("secret.json", "r") as f:
    secret = json.load(f)
    print(secret)

mydb = mysql.connector.connect(
    host=secret['host'],
    user=secret['user'],
    port=secret['port'],
    password=secret['password'],
    database=secret['database']
)

fake_file = io.StringIO()

def sqlToJson(data):
    rowarray_list = []
    for row in data:
        # print(type(row))
        date = datetime.timestamp(row[2])
        # print(date)
        t = (row[0], row[1], date, row[3], row[4], row[5], row[6])
        rowarray_list.append(t)
        
    j = json.dumps(rowarray_list)
    with open("getrequest.json", "w") as f:
        f.write(j)
        
    # Convert query to objects of key-value pairs
    objects_list = []
    for row in data:
        d = collections.OrderedDict()
        d["id"] = row[0]
        d["event-type"] = row[1]
        d["occuredOn"] = date
        d["version"] = row[3]
        d["graph-id"] = row[4]
        d["nature"] = row[5]
        d["object-name"] = row[6]
        d["path"] = row[7]
        objects_list.append(d)
        
    j = json.dumps(objects_list)
    
    with open("getrequest.json", "w") as f:
        f.write(j)
        
    return j

def getAll():
    cursor = mydb.cursor()
    req = 'SELECT * FROM `' + secret['database'] + '`.`' + secret['table'] + '` GROUP BY `object-name`'
    cursor.execute(req)
    myresult = cursor.fetchall()
    l = sqlToJson(myresult)
    print(l, file=fake_file)
    return l

client = pymongo.MongoClient('mongodb+srv://admin:admin@cluster0.ocwzp.mongodb.net/NoSqlProject_db?retryWrites=true&w=majority')
db = client.get_database('NoSqlProject_db')

records = db.objects

getAll()

sample_data = json.loads(fake_file.getvalue())

records.insert_many(sample_data)


