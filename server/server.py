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
    try:
        payload = request.get_json()
        shard_id = payload['shard']
        stud_id_range = payload['Stud_id']

        # Extract low and high values from the range
        low = stud_id_range['low']
        high = stud_id_range['high']

        # Retrieve the requested data 
        

        response = {
            "data": data,
            "status": "success"
        }
        return jsonify(response), 200

    except Exception as e:
        response = {
            "message": str(e),
            "status": "error"
        }
        return jsonify(response), 500

@app.route('/write', methods=['POST'])
def write():
    try:
        payload = request.get_json()
        shard_id = payload['shard']
        curr_idx = payload['curr_idx']
        data_entries = payload['data']

        # wrtite 

        response = {
            "message": "Data entries added",
            "current_idx": ,
            "status": "success"
        }
        return jsonify(response), 200

    except Exception as e:
        response = {
            "message": str(e),
            "status": "error"
        }
        return jsonify(response), 500

@app.route('/update', methods=['PUT'])
def update():
    try:
        payload = request.get_json()
        shard_id = payload['shard']
        stud_id = payload['Stud_id']
        updated_data = payload['data']

      # perform updates

        response = {
            "message": f"Data entry for Stud_id:{stud_id} updated",
            "status": "success"
        }
        return jsonify(response), 200

    except Exception as e:
        response = {
            "message": str(e),
            "status": "error"
        }
        return jsonify(response), 500

@app.route('/del', methods=['DELETE'])
def delete():
    try:
        payload = request.get_json()
        shard_id = payload['shard']
        stud_id = payload['Stud_id']

        # Remove the data entry 

        response = {
            "message": f"Data entry with Stud_id:{stud_id} removed",
            "status": "success"
        }
        return jsonify(response), 200

    except Exception as e:
        response = {
            "message": str(e),
            "status": "error"
        }
        return jsonify(response), 500

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)
