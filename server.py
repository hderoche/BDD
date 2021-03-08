import json
from flask import Flask, request, jsonify


bind, port = 'localhost', 5001
app = Flask(__name__)
app.config["DEBUG"] = True


@app.route('/', methods=['GET'])
def home():
    return "<h1>Server for inserting data</h1><p>API for NoSQL databases</p>"

@app.route('/api/insert', methods=['POST'])
def sendInsert():
    content = json.loads(request.data)
    print(content)
    return jsonify(content), 201




app.run(bind, port)