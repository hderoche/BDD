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
    req = 'SELECT * FROM `' + secret['database'] + '`.`' + secret['table'] + '`'
    cursor.execute(req)
    myresult = cursor.fetchall()
    l = sqlToJson(myresult)
    print(l, file=fake_file)
    return l

def updateMongo():
    files = json.loads(fake_file.getvalue())
    
    for file in files:
        addOrUpdate(file)

client = pymongo.MongoClient('mongodb+srv://admin:admin@cluster0.ocwzp.mongodb.net/NoSqlProject_db?retryWrites=true&w=majority')
db = client.get_database('NoSqlProject_db')

# File -> object name & id
def addOrUpdate(file):
    col = db.get_collection('objects')
    cur = col.find({file.object_name})
    if cur:
        for doc in cur:
            newPath = set(doc.path)
            newPath.add(file.path)
            doc.path = list(newPath)
            doc.ids = [doc.id, file.id]
            col.find_one_and_update({"object_name":file.object_name}, {"$set": {"path": doc.path, "ids": doc.ids}})
    # if no document with this object_name
    col.insert(file)
        
class Stats:
    def __init__(self):
        self.status = dict({
            "received": 0, 
            "verified": 0,
            "processed": 0,
            "remedied": 0,
            "consumed": 0,
            "rejected":0,
            "to_be_purged": 0,
            "purged": 0
        })
        self.integrity = 0
        self.last_doc_id = 0
    
    def count_by_status(self, doc):
        if self.last_doc_id == doc.id:
            return
        self.last_doc_id = doc.id
        path = doc.path
        if "RECEIVED" in path:
            if "VERIFIED" in path:
                if "PROCESSED" in path:
                    if "CONSUMED" in path:
                        if "TO_BE_PURGED" in path:
                            if "PURGED" in path:
                                self.integrity += 1
                                addAll()
                                self.rejected -= 1
                    elif "REJECTED" in path:
                        if "TO_BE_PURGED" in path:
                            if "PURGED" in path:
                                self.integrity += 1
                                addAll()
                                self.consumed -= 1
                        elif "REMEDIED" in path:
                            # loop back on consumed
                    else:
                        self.status.consumed += 1 
                else:
                    self.status.processed += 1
            else:
                self.status.verified += 1
        else:
            self.status.received += 1                       
        self.save()
        return
        
    def addAll(self):
        self.status.received += 1 
        self.status.verified += 1
        self.status.processed += 1
        self.status.consumed += 1
        self.status.rejected += 1
        self.status.to_be_purged += 1
        self.status.purged += 1

    