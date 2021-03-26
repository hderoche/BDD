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

def updateMongoObjects():
    # We retrieve that data from the database
    getAll()
    
    files = json.loads(fake_file.getvalue())
    
    for file in files:
        # print(file)
        addOrUpdate(file)

client = pymongo.MongoClient('mongodb+srv://admin:admin@cluster0.ocwzp.mongodb.net/NoSqlProject_db?retryWrites=true&w=majority')
db = client.get_database('NoSqlProject_db')

# File -> object name & id
def addOrUpdate(file):
    col = db.get_collection('objects')
    cur = col.find({'object-name':file['object-name']})
    if cur:
        for doc in cur:
            # We complete the path to make it complete
            newPath = doc['path'][1:-1].replace(' ', '').split(',')
            filePath = file['path'][1:-1].replace(' ', '').split(',')
            for elem in filePath:
                if (elem not in newPath):
                    newPath.append(elem)
            
            # We convert the set into a string to put it back in the database
            updatePath = "["
            
            for i in range(len(newPath)):
                if (i < len(newPath)-1):
                    updatePath += newPath[i] + ', '
                else:
                    updatePath += newPath[i] + "]"
                    
            doc['path'] = updatePath
            doc['id'] = doc['id'] + "," + file['id']
            col.find_one_and_update({"object-name":file['object-name']}, {"$set": {"path": doc['path'], "id": doc['id']}})
            
    # if no document with this object_name
    else:
        col.insert_one(file)
        
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
            "purged": 0,
            "integrity" : 0
        })
        # self.last_doc_id = 0
    
    def count_by_status(self, doc):
        # if self.last_doc_id == doc.id:
        #     return
        # self.last_doc_id = doc.id
        path = doc['path']
        
        rec = "RECEIVED" in path
        ver = "VERIFIED" in path
        pro = "PROCESSED" in path
        rem = "REMEDIED" in path
        con = "CONSUMED" in path
        rej = "REJECTED" in path
        tbp = "TO_BE_PURGED" in path
        pur = "PURGED" in path
        
        integ = ((rec and ver and pro and con and tbp and pur) or 
                 (rec and ver and pro and rej and tbp and pur) or
                 (rec and ver and pro and rej and rem and tbp and pur) or
                 (rec and ver and pro and rej and rem and con and tbp and pur))
        
        if (rec):
             self.status['received'] += 1
        if (ver):
            self.status['verified'] += 1
        if (pro):
            self.status['processed'] += 1
        if (rem):
            self.status['remedied'] += 1
        if (con):
            self.status['consumed'] += 1
        if (rej):
            self.status['rejected'] += 1
        if (tbp):
            self.status['to_be_purged'] += 1
        if (pur):
            self.status['purged'] += 1
        if (integ):
            self.status['integrity'] += 1


def updateStats():
    files = list(db.get_collection('objects').find())
    statis = db.get_collection('stats')
    
    stats = Stats()
    
    for file in files:
        stats.count_by_status(file)
        
    # print(stats.status)
    
    newStats = stats.status
    
    if(len(list(statis.find())) == 0):
        statis.insert_one(newStats)
        
    else:
        id = statis.find_one()['_id']
        statis.find_one_and_update({"_id":id}, {"$set": {'received': newStats['received'], 'verified': newStats['verified'], 
                                                         'processed': newStats['processed'], 'remedied': newStats['remedied'], 
                                                         'consumed': newStats['consumed'], 'rejected': newStats['rejected'], 
                                                         'to_be_purged': newStats['to_be_purged'], 'purged': newStats['purged'], 
                                                         'integrity': newStats['integrity']}})
    
    print("Stats Updated in Mongo return!")
    

def updateStatsHeure():
    return
    
    
updateStats()

# updateMongoObjects()
# print("Mongo upadated !")


    