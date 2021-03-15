import json
from flask import Flask, request, jsonify
import mysql.connector

with open("secret.json", "r") as f:
    secret = json.load(f)
    print(secret)

bind, port = 'localhost', 5001
app = Flask(__name__)
app.config["DEBUG"] = True

mydb = mysql.connector.connect(
    host=secret['route'],
    user=secret['user'],
    port=secret['port'],
    password=secret['password'],
    database=secret['database']
)
print(mydb)
print(mydb.cursor())

@app.route('/', methods=['GET'])
def home():
    return "<h1>Server for inserting data</h1><p>API for NoSQL databases</p>"

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
    req = "INSERT INTO `A4BDD`.`app` (`id`, `event-type`, `occuredOn`, `version`, `graph-id`, `nature`, `object-name`, `path`) VALUES (%s, %s, %s, %s, %s, %s, %s, %s);"
    val = (id, event_type, occurredOn, version, graph_id, nature, object_name, path)
    cursor.execute(req, val)
    mydb.commit()
    return jsonify(content), 201



@app.route('/api/getall', methods=['GET'])
def getall():
    cursor = mydb.cursor()
    cursor.execute('SELECT * FROM `A4BDD`.`app` FOR JSON AUTO')
    myresult = cursor.fetchall()
    return jsonify(myresult)

app.run(bind, port)