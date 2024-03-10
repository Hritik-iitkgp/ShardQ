from flask import Flask, jsonify
import os

app = Flask(__name__)

@app.route('/home', methods=['GET'])
def home():
    server_id = os.getenv("SERVER_ID")
    response = {
        "message": f"Hello from ssServer: {server_id}",
        "status": "successful"
    }
    return jsonify(response), 200

@app.route('/heartbeat', methods=['GET'])
def heartbeat():
    response={
        "message": ""
        
        
    }
    return jsonify(response), 200

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)
