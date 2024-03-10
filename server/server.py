from flask import Flask, jsonify, request
import os

app = Flask(__name__)

@app.route('/config', methods=['POST'])
def config():
    pass

@app.route('/heartbeat', methods=['GET'])
def heartbeat():
    response={
        "message": ""
    }
    return jsonify(response), 200

@app.route('/copy', methods=['GET'])
def copy():
    pass

@app.route('/read', methods=['POST'])
def read():
    pass

@app.route('/write', methods=['POST'])
def write():
    pass

@app.route('/update', methods=['PUT'])
def update():
    pass

@app.route('/del', methods=['DELETE'])
def delete():
    pass

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)
