# -*- coding: utf-8 -*-
"""
Created on Mon Mar 15 13:12:46 2021

@author: malco
"""
import json
from flask import Flask, request, jsonify
import mysql.connector
import collections

bind, port = 'localhost', 5001
app = Flask(__name__)
app.config["DEBUG"] = True

mydb = mysql.connector.connect(
    host="localhost",
    user="root",
    port=3306,
    password="nexuswefax@arsenal.me123",
    database="nosqlproject"
)
print(mydb)
print(mydb.cursor())

@app.route('/', methods=['GET'])
def home():
    return "<h1>Server for inserting data</h1><p>API for NoSQL databases</p>"

@app.route('/api/insert', methods=['POST'])
def sendInsert():
    content = json.loads(request.data)
    print(content)
    id = content['id']
    event_type = content['event-type']
    nature = content['nature']
    version = content['version']
    object_name = content['object-name']
    graph_id = content['graph-id']
    occurredOn = content['occurredOn']
    path = content['path']
    cursor = mydb.cursor()
    req = "INSERT INTO `nosqlproject`.`data` (`id`, `event-type`, `occuredOn`, `version`, `graph-id`, `nature`, `object-name`, `path`) VALUES (%s, %s, %s, %s, %s, %s, %s, %s);"
    val = (id, event_type, occurredOn, version, graph_id, nature, object_name, path)
    cursor.execute(req, val)
    mydb.commit()
    return jsonify(content), 201


def sqlToJson(data):
    rowarray_list = []
    for row in data:
        print(type(row))
        date = str(row[2].strftime("%m/%d/%Y, %H:%M:%S"))
        print(date)
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
        d["object-nature"] = row[6]
        d["path"] = row[7]
        objects_list.append(d)
        
    j = json.dumps(objects_list)
    
    with open("getrequest.json", "w") as f:
        f.write(j)
        
    return j

@app.route('/api/getall', methods=['GET'])
def getall():
    cursor = mydb.cursor()
    cursor.execute('SELECT * FROM `nosqlproject`.`data`')
    myresult = cursor.fetchall()
    l = sqlToJson(myresult)
    print(l)
    return l

app.run(bind, port)