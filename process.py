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
from datetime import datetime, timedelta
import redis


with open("secret.json", "r") as f:
    secret = json.load(f)

mydb = mysql.connector.connect(
    host=secret['host'],
    user=secret['user'],
    port=secret['port'],
    password=secret['password'],
    database=secret['database']
)
# class RedisFunctions():
#     def __init__(self, host, port, pas):
#         self.r = redis.Redis(host,port, pas)
    
#     def client(self):
#         return self.r

#     def find_by_namespace(self, namespace):
#         docByNamespace = self.r.keys(namespace)
#         return docByNamespace
    
#     def insert(self, name, doc):
#         r.hmset(name, doc)
    
#     # match is a regex param
#     def find_one(self, match):
#         return r.scan_iter(match)
    
#     def find_and_update(self, name, field, value):
#         r.hset(name, field, value)

#     # match is a regex param
#     def count_collection(self, match):
#         arr = r.scan_iter(match)
#         return len(arr)

# Connection to the databases Mongodb and redis
client = pymongo.MongoClient('mongodb+srv://admin:admin@cluster0.ocwzp.mongodb.net/NoSqlProject_db?retryWrites=true&w=majority')
db = client.get_database('NoSqlProject_db')
rclient = redis.Redis(secret['host'], secret['portRedis'], 0)

# File to read tthe data received from the database in MySQL
fake_file = io.StringIO()




# Turns the data retrieved from the MySQL db and turns it into JSON
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


# Retrieve all the lines of the db MySQL by using the previous function
def getAll():
    cursor = mydb.cursor()
    req = 'SELECT * FROM `' + secret['database'] + '`.`' + secret['table'] + '`'
    cursor.execute(req)
    myresult = cursor.fetchall()
    l = sqlToJson(myresult)
    # print(l, file=fake_file)
    return l



# Called in the previous function, its managing to update the path of a given file
# if it's already in the database. It also adds files that aren't in the db.
# File -> object name & id
def addOrUpdate(file):
    print("here file :", file['object-name'])
    col = db.get_collection('objects')
    # We needed two cursors because they become empty after we used them once
    cur = col.find({'object-name': file['object-name']})
    cur2 = col.find({'object-name': file['object-name']})
    if (len(list(cur)) > 0) :
        for doc in cur2:
            # We update the path of a given file
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
            rclient.hmset(file['object-name'], {"path": doc['path'], "id": doc['id']})
    # if no document with this object_name
    # we just add it to both databases
    else:
        col.insert_one(file)
        file['_id'] = str(file['_id'])
        rclient.hmset(file['object-name'], file)


# Used to generate Stats objects that will store the information of
# how many files there are for a given status and if the file
# respects the integrity or not
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
    
    def count_by_status(self, doc):

        path = doc['path']
        
        rec = "RECEIVED" in path
        ver = "VERIFIED" in path
        pro = "PROCESSED" in path
        rem = "REMEDIED" in path
        con = "CONSUMED" in path
        rej = "REJECTED" in path
        tbp = "TO_BE_PURGED" in path
        pur = "PURGED" in path
        
        # Testing the integrity of a file
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


# Updates the data in mongo an redis in order to get the stats
def updateStats():
    files = list(db.get_collection('objects').find())
    statis = db.get_collection('stats')
    stats = Stats()
    
    for file in files:
        stats.count_by_status(file)
        
    # print(stats.status)
    
    newStats = stats.status
    print('stats',newStats)
    if(len(list(statis.find())) == 0):
        statis.insert_one(newStats)
        newStats['_id'] = str(newStats['_id'])
        rclient.hmset('stats', newStats)
    else:
        id = statis.find_one()['_id']
        statis.find_one_and_update({"_id":id}, {"$set": {'received': newStats['received'], 'verified': newStats['verified'], 
                                                         'processed': newStats['processed'], 'remedied': newStats['remedied'], 
                                                         'consumed': newStats['consumed'], 'rejected': newStats['rejected'], 
                                                         'to_be_purged': newStats['to_be_purged'], 'purged': newStats['purged'], 
                                                         'integrity': newStats['integrity']}})

        rclient.hmset('stats', {'received': newStats['received'], 'verified': newStats['verified'], 'processed': newStats['processed'], 'remedied': newStats['remedied'], 'consumed': newStats['consumed'], 'rejected': newStats['rejected'], 'to_be_purged': newStats['to_be_purged'], 'purged': newStats['purged'], 'integrity': newStats['integrity']})
    
    print("Stats Updated in Mongo return!")
    

# Updates the data in mongo an redis in order to get the stats by hour
def updateStatsHeure():

    # We get the current hour and the last hour
    hour = datetime.now() - timedelta(hours=1)
    tsHour = int(hour.timestamp())
    now = int(datetime.now().timestamp())
    
    files = list(db.get_collection('objects').find({'$and': [{'occuredOn': {'$gte': tsHour}}, {'occuredOn': {'$lte': now}}]}))
    
    statis = db.get_collection('statsHeure')
    
    stats = Stats()
    
    for file in files:
        stats.count_by_status(file)
        
    # print(stats.status)
    
    newStats = stats.status
    
    if(len(list(statis.find())) == 0):
        statis.insert_one(newStats)

        rclient.hmset('stats', newStats)
        
    else:
        id = statis.find_one()['_id']
        statis.find_one_and_update({"_id":id}, {"$set": {'received': newStats['received'], 'verified': newStats['verified'], 
                                                         'processed': newStats['processed'], 'remedied': newStats['remedied'], 
                                                         'consumed': newStats['consumed'], 'rejected': newStats['rejected'], 
                                                         'to_be_purged': newStats['to_be_purged'], 'purged': newStats['purged'], 
                                                         'integrity': newStats['integrity']}})

        rclient.hmset('stats', {'received': newStats['received'], 'verified': newStats['verified'], 
                                                         'processed': newStats['processed'], 'remedied': newStats['remedied'], 
                                                         'consumed': newStats['consumed'], 'rejected': newStats['rejected'], 
                                                         'to_be_purged': newStats['to_be_purged'], 'purged': newStats['purged'], 
                                                         'integrity': newStats['integrity']})
    


# updateMongoObjects()
# print("Mongo upadated !")


# Fills the objects table in the mongo database
def updateMongoObjects():
    # We retrieve that data from the database
    getAll()
    with open('getrequest.json', 'r') as f:
        files = json.loads(f.read())
    for file in files:
        addOrUpdate(file)

