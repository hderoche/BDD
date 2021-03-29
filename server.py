import json
import collections
from flask import Flask, request, jsonify
import mysql.connector
from datetime import datetime
import pymongo
import periodicFunction
import process 

with open("secret.json", "r") as f:
    secret = json.load(f)

bind, port = 'localhost', 5001
app = Flask(__name__)
app.config["DEBUG"] = True

mydb = mysql.connector.connect(
    host=secret['host'],
    user=secret['user'],
    port=secret['port'],
    password=secret['password'],
    database=secret['database']
)
print(mydb)
print(mydb.cursor())

# Function that returns HTML 
@app.route('/', methods=['GET'])
def home():
    return "<h1>Server to interract with SQL, MongoDB and Redis databases</h1><p>API for NoSQL databases</p>"

# Function that inserts a raw document in SQL database
@app.route('/api/insert', methods=['POST'])
def sendInsert():
    content = json.loads(request.data)
    print(type(content))
    id = content['id']
    event_type = content['event-type']
    nature = content['nature']
    version = content['version']
    object_name = content['object-name']
    graph_id = content['graph-id']
    occurredOn = content['occurredOn']
    path = content['path']
    cursor = mydb.cursor()
    req = 'INSERT INTO `' + secret['database'] + '`.`' + secret['table'] + '` ' + '(`id`, `event-type`, `occuredOn`, `version`, `graph-id`, `nature`, `object-name`, `path`) VALUES (%s, %s, %s, %s, %s, %s, %s, %s);'
    val = (id, event_type, occurredOn, version, graph_id, nature, object_name, path)
    cursor.execute(req, val)
    mydb.commit()
    return jsonify(content), 201

# Function that convert SQL output to JSON
def sqlToJson(data):
    rowarray_list = []
    for row in data:
        print(type(row))
        date = datetime.timestamp(row[2])
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
        d["object-name"] = row[6]
        d["path"] = row[7]
        objects_list.append(d)
    j = json.dumps(objects_list)
    with open("getrequest.json", "w") as f:
        f.write(j)
    return j

# Function that returns all the documents from the SQL database
@app.route('/api/all', methods=['GET'])
def getall():
    cursor = mydb.cursor()
    cursor.execute('SELECT * FROM `' + secret['database'] + '`.`' + secret['table'] + '`')
    myresult = cursor.fetchall()
    l = sqlToJson(myresult)
    print(l)
    return l

# Starts a function on a given interval

periodicFunction.set_interval(process.updateMongoObjects,30)

# Function that gets the path of an document given its object-name
@app.route('/api/mongo/path', methods=['GET'])
def path():
    clientMongo = pymongo.MongoClient('mongodb+srv://admin:admin@cluster0.ocwzp.mongodb.net/NoSqlProject_db?retryWrites=true&w=majority')
    objectName = request.args.get('object-name')
    db = clientMongo.get_database('NoSqlProject_db')
    col = db.get_collection('objects')
    doc = col.find({'object-name':objectName})
    print('doc :', doc)
    for record in doc:
        path = record['path']
        return jsonify(path), 200
    return(jsonify("{'error in find function'}"), 500)


# Function that returns all documents that have been modified in the last hour
@app.route('/api/mongo/lasthour', methods=['GET'])
def statusLastHour():
    timestamp = request.args.get('timestamp')
    clientMongo = pymongo.MongoClient('mongodb+srv://admin:admin@cluster0.ocwzp.mongodb.net/NoSqlProject_db?retryWrites=true&w=majority')
    db = clientMongo.get_database('NoSqlProject_db')
    col = db.get_collection('statsHeure')
    docs = col.find({}, {"_id": 0})
    for record in docs:
        return jsonify(record), 200
    return(jsonify("{'error in find function'}"), 500)


@app.route('/api/stats', methods=['GET'])
def stats():
    process.updateStats()
    clientMongo = pymongo.MongoClient('mongodb+srv://admin:admin@cluster0.ocwzp.mongodb.net/NoSqlProject_db?retryWrites=true&w=majority')
    db = clientMongo.get_database('NoSqlProject_db')
    col = db.get_collection('stats')
    docs = col.find({}, {"_id": 0})
    for record in docs:
        return jsonify(record, 200)
    return(jsonify("{'error in find function'}"), 500)


app.run(bind, port)