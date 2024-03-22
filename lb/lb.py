from flask import Flask, jsonify,request
import os,subprocess,random,requests,time
from consistent_hashing import ConsistentHashMap 
import threading
import random
import sqlite3,os
import mysql.connector 
N=0
schema={}
shards=[]
servers={}
hashmaps={}

# my_unq_cnt=1
# req_id=0
# replicas=[]
while True:
     try:
        _conn = mysql.connector.connect(
            host="metadb",
            user="root",
            password="giri123456",
            database="metadb"
        )
        print("connected")
        break
     except  Exception as e:
         time.sleep(2)
_conn.close()

app = Flask(__name__)

DB_FILE = "metadata.db"

if os.path.exists(DB_FILE):
    os.remove(DB_FILE)

#TODO : need to add hashing 
@app.route('/init', methods=['POST'])
def init():
    global N
    global schema
    global shards
    global servers
    global hashmaps

    
    
    try:
        conn = mysql.connector.connect(
    host="metadb",
    user="root",
    password="giri123456",
    database="metadb"
)
        cursor = conn.cursor()
        payload = request.get_json()

        # Extract schema and shards information from the payload
        N = payload['N']
        schema = payload['schema']
        shards = payload['shards']
        servers = payload['servers']

        

        cursor.execute("CREATE TABLE IF NOT EXISTS ShardT ( Stud_id_low INT PRIMARY KEY, Shard_id VARCHAR(100), Shard_size INT, valid_idx INT)")
        cursor.execute("CREATE TABLE IF NOT EXISTS MapT ( Shard_id VARCHAR(100), Server_id VARCHAR(100))")
        
        # Insert shard information into ShardT table
        for shard in shards:
            shard_id= shard["Shard_id"]
            hashmaps[shard_id] = ConsistentHashMap(512,0,9)
            cursor.execute('''INSERT INTO ShardT (Stud_id_low, Shard_id, Shard_size, valid_idx) 
                            VALUES (%s, %s, %s, %s)''', (shard['Stud_id_low'], shard['Shard_id'], shard['Shard_size'], 1))

        # Insert server-shard mappings into MapT table
        for server, shard_list in servers.items():
            for shard_id in shard_list:
                hashmaps[shard_id].add_server_instance(server)
                cursor.execute('''INSERT INTO MapT (Shard_id, Server_id) 
                                VALUES (%s, %s)''', (shard_id, server))

        conn.commit()
        
        
        for server, shard_list in servers.items():
            command =  f"docker run --name {server} --network net1 -d server"
            result = subprocess.run(command,shell=True,text=True)
            
            if result.returncode == 0:
                pass
                ### --something hashmaps...--                                             CONSISTENT HASHING

            else:
                response = {
                    "message": "Failed to create server",
                    "status": "failure"
                }
                return jsonify(response), 500

        try:
            for server_id in servers:
                print(server_id)
                
                while True:
                    try:
                        response = requests.post(f"http://{server_id}:5000/config",json={
                            "schema":schema,
                            "shards":servers[server_id]
                        },timeout=2000)
                        print(response.json())
                        break
                    except Exception as e:
                        print("retrying")
                        continue
                
        except requests.RequestException as e:
            print("Request exception: ",str(e))
            return "request error"
        except Exception as e:
            print(e)
            return "some error"   

        response = {
            "message": "Configured Database",
            "status": "success"
        }
        return jsonify(response), 200

    except Exception as e:
        response = {
            "message": str(e),
            "status": "error"
        }
        return jsonify(response), 500
    
    finally:
        cursor.close()
        conn.close()
    



@app.route('/status', methods=['GET'])
def status():
    global N
    global schema
    global shards
    global servers
    
    try:
        
        response = {
            "N": N,
            "schema": schema,
            "shards": shards,
            "servers": servers,
            "status": "success"
        }
        return jsonify(response), 200

    except Exception as e:
        response = {
            "message": str(e),
            "status": "error"
        }
        return jsonify(response), 500
    



@app.route('/add', methods=['POST'])
def add():
    global N
    global schema
    global shards
    global servers
    global hashmaps

    

    try:

        conn = mysql.connector.connect(
            host="metadb",
            user="root",
            password="giri123456",
            database="metadb"
        )
        cursor = conn.cursor()

        payload = request.get_json()
        n = payload['n']
        new_shards = payload['new_shards']
        new_servers = payload['servers']
        print(n)
        print(new_shards)
        print(new_servers)
        msg_string = "Added "

        if len(new_servers) < n:
            response = {
                "message": f"<Error> Number of new servers {n} is greater than newly added instances",
                "status": "failure"
            }
            return jsonify(response), 400
        
        # Insert shard information into ShardT table
        for shard in new_shards:
            shard_id = shard["Shard_id"]
            hashmaps[shard_id]= ConsistentHashMap(512,0,9)
            shards.append(shard)
            cursor.execute('''INSERT INTO ShardT (Stud_id_low, Shard_id, Shard_size, valid_idx) 
                            VALUES (%s, %s, %s, %s)''', (shard['Stud_id_low'], shard['Shard_id'], shard['Shard_size'], 1))

        # Insert server-shard mappings into MapT table
        for server, shard_list in new_servers.items():
            for shard_id in shard_list:
                cursor.execute('''INSERT INTO MapT (Shard_id, Server_id) 
                                VALUES (%s, %s)''', (shard_id, server))

        conn.commit()
        
        for server, shard_list in new_servers.items():
            command =  f"docker run --name {server} --network net1 -d server"
            result = subprocess.run(command,shell=True,text=True)
            
            if result.returncode == 0:
                msg_string += f"{server}, "
                while True:
                    try:
                        print({
                            "schema":schema,
                            "shards":new_servers[server]
                        },flush=True)
                        response = requests.post(f"http://{server}:5000/config",json={
                            "schema":schema,
                            "shards":new_servers[server]
                        },timeout=2000)
                        print(response.json())
                        break
                    except Exception as e:
                        # print("retrying")
                        continue
                servers[server] = shard_list
                N+=1
                newShardList = [ x["Shard_id"] for x in new_shards if x  in  shard_list]
                print(newShardList,flush=True)
                for sh in newShardList:
                    hashmaps[sh].add_server_instance(server)
                    cursor.execute("SELECT Server_id FROM MapT WHERE Shard_id=%s",(sh,))
                    row = cursor.fetchone()
                    sh_server = row[0]
                    resp = requests.get(f"http://{sh_server}:5000/copy",json={
                        "shards":[sh]
                    },timeout=20)
                    if resp.status_code == 200:
                        data = resp.json()[sh]
                        resp1 = requests.post(f"http://{server}:5000/write",json={
                            "shard":sh,
                            "curr_idx": 507,
                            "data": data
                        })
                        print(resp1.json())
                        if not resp1.ok:
                            return "some error"
                        print(f"Successfully copied {sh} from {sh_server} to {server}")

                ### --something hashmaps...--                                                CONSISTENT HASHING

            else:
                response = {
                    "message": "Failed to create server",
                    "status": "failure"
                }
                return jsonify(response), 500



        response = {
            
            "N": N,
            "messsage": msg_string,
            "status": "successful"
        }
        return jsonify(response), 200
    
    except Exception as e:
        response = {
            "message": str(e),
            "status": "error"
        }
        return jsonify(response), 500
    
    finally:
        cursor.close()
        conn.close()




@app.route('/rm', methods=['DELETE'])
def remove():
    global N
    global schema
    global shards
    global servers
    global hashmaps

    try:
        conn = mysql.connector.connect(
    host="metadb",
    user="root",
    password="giri123456",
    database="metadb"
)
        cursor = conn.cursor()
        
        payload = request.get_json()
        n = payload['n']
        tbr_servers = payload['servers']
        print(n)
        print(tbr_servers)

        if len(tbr_servers) > n:
            response = {
                "message": "<Error> Length of server list is more than removable instances",
                "status": "failure"
            }
            return jsonify(response), 400
        
        extra = n-len(tbr_servers)
        for i in range(extra):
            for server in servers:
                if server not in tbr_servers:
                    tbr_servers.append(server)
                    break


        servers_removed = []
        for i in range(n):


            command = f"docker rm -f {tbr_servers[i]}"
            result = subprocess.run(command,shell=True,text=True)
            
            if result.returncode == 0:

                servers_removed.append(tbr_servers[i])
                servers.pop(tbr_servers[i])
                N-=1

                #remove server_id from MapT
                cursor.execute("DELETE FROM MapT WHERE Server_id=%s",(tbr_servers[i],))
                ###
                ### TODO : add code for updating hashmap                                                  CONSISTENT HASHING
                ###
            else:
                return jsonify({"message":{"error":f"failed to remove {tbr_servers[i]}","replicas":list(servers.keys())},"status":"failure"}),400
        

        shards_tbr=[]
        for sh in shards:
            shard_id = sh["Shard_id"]
            hashmaps[shard_id].remove_server_instance(tbr_servers[i])
            cursor.execute("SELECT * FROM MapT WHERE Shard_id=%s",(shard_id,))
            rows = cursor.fetchall()
            if len(rows) == 0:
                del hashmaps[shard_id]
                cursor.execute("DELETE FROM ShardT WHERE Shard_id=%s",(shard_id,))
                shards_tbr.append(sh)
                ###
                ### TODO : add code for updating hashmap                                                  CONSISTENT HASHING
                ###
        
        
        # updating shards 
        shards = [ x for x in shards if x not in shards_tbr]
        print(shards)    


        response = {
            "message": {
                "N": N,
                "servers": servers_removed   
            },
            "status": "successful"
        }
        return jsonify(response), 200
    
    except Exception as e:
        response = {
            "message": str(e),
            "status": "error"
        }
        return jsonify(response), 500
    
    finally:
        conn.commit()
        cursor.close()
        conn.close()








@app.route('/read', methods=['POST'])
def read():
    global N
    global schema
    global shards
    global servers
    global hashmaps

    try:

        conn = mysql.connector.connect(
    host="metadb",
    user="root",
    password="giri123456",
    database="metadb"
)
        cursor = conn.cursor()

        payload = request.get_json()
        Stud_id_range = payload['Stud_id']
        low = payload['Stud_id']['low']
        high = payload['Stud_id']['high']
        print(low)
        print(high)

        
        shards_queried = []
        data=[]

        cursor.execute("SELECT * FROM ShardT")
        rows = cursor.fetchall()

        for row in rows:
            if(low<row[0]+row[2] and row[0]<=high):
                shards_queried.append(row[1])
        
    
        for shard in shards_queried:
            print(shard+"is the shard")
            server=""                                                               ###
            server = hashmaps[shard].map_request_to_server(random.randint(0, 999999))
            print("server: "+server)

            while True:
                try:
                    response = requests.post(f"http://{server}:5000/read",json={
                        "shard":shard,
                        "Stud_id":Stud_id_range
                    },timeout=2000)
                    print(response.json())
                    studs = response.json()["data"]
                    data.extend(studs)
                    break

                except Exception as e:
                    print("retrying")
                    continue
        

        response = {
            "shards_queried": shards_queried,
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
    
    finally:
        cursor.close()
        conn.close()




@app.route('/write', methods=['POST'])
def write():
    global N
    global schema
    global shards
    global servers
    global hashmaps
    

    try:
        conn = mysql.connector.connect(
    host="metadb",
    user="root",
    password="giri123456",
    database="metadb"
)
        cursor = conn.cursor()

        payload = request.get_json()
        data = payload['data']
        students = data
        print(students)

        cursor.execute("SELECT * FROM ShardT")
        rows = cursor.fetchall()
        shds = {row[1]: {"students":[],"attr":list(row),"server":[]} for row in rows}
       
        cursor.execute("SELECT * FROM MapT")
        MapT_rows =cursor.fetchall()
       
        for MapT_row in MapT_rows:
            shard_id = MapT_row[0]
            server = MapT_row[1]
            shds[shard_id]["server"].append(server)

        for student in students:
            Stud_id =student["Stud_id"]
            Stud_name=student["Stud_name"]
            Stud_marks=student["Stud_marks"]
           
            for shard_id in shds:
                if shds[shard_id]["attr"][0] <= Stud_id and Stud_id <= shds[shard_id]["attr"][0]+shds[shard_id]["attr"][2]:
                    shds[shard_id]["students"].append((Stud_id,Stud_name,Stud_marks))
       
        data_written = []
        for shard_id in shds:
            # acquire the lock for this shard

            queries = [{"Stud_id":stud[0],"Stud_name":stud[1],"Stud_marks":stud[2]} for stud in shds[shard_id]["students"]]
            if len(queries) == 0:
                continue
            data= { "shard":shard_id,"curr_idx":shds[shard_id]["attr"][3] ,"data":queries}
            curr_idx = None
            print( shds[shard_id]["server"])
            for server in shds[shard_id]["server"]:
                print(f"Sending request to {server} :{shard_id}",flush=True)
                result = requests.post(f"http://{server}:5000/write",json=data,timeout=15)
                if result.status_code != 200:
                    return jsonify({
                        "message":f"writes to shard {shard_id} failed",
                        "data entries written successfully":data_written,
                        "status":"failure"
                    }),400
                print(result.json())
                curr_idx = result.json()["current_idx"]
            shds[shard_id]["attr"][3]=curr_idx
            cursor.execute("UPDATE ShardT SET valid_idx= %s WHERE Stud_id_low = %s AND Shard_id = %s",(curr_idx,shds[shard_id]["attr"][0],shard_id))
            conn.commit()
            data_written.extend(queries)
       
        return jsonify({"message":f"{len(students)} Data entries added","status":"success"})
    
    except Exception as e:
        response = {
            "message": str(e),
            "status": "error"
        }
        return jsonify(response), 500
    
    finally:
        cursor.close()
        conn.close()






@app.route('/update', methods=['PUT'])
def update():
    global N
    global schema
    global shards
    global servers
    global hashmaps

    try:
        conn = mysql.connector.connect(
    host="metadb",
    user="root",
    password="giri123456",
    database="metadb"
)
        cursor = conn.cursor()
        print("hi")
        payload = request.get_json()
        stud_id = payload['Stud_id']
        data = payload['data']
        print(stud_id,flush=True)

        cursor.execute("SELECT DISTINCT Shard_id FROM ShardT WHERE Stud_id_low <= %s AND Stud_id_low + Shard_size > %s", (stud_id, stud_id))
        row = cursor.fetchone()
        print(row,flush=True)

        shard_id = row[0]

        cursor.execute("SELECT DISTINCT Server_id FROM MapT WHERE Shard_id = %s", (shard_id,))
        rows = cursor.fetchall()
        print(rows)
        if rows:
            for row in rows:
                
                server = row[0]

                while True:
                    try:
                        response = requests.put(f"http://{server}:5000/update",json={
                            "shard":shard_id,
                            "Stud_id":stud_id,
                            "data": data
                        },timeout=2000)
                        print(response.json())
                        
                        break

                    except Exception as e:
                        print("retrying")
                        continue
                    
            response = {
                "message": f"Data entry fo Stud_id: {stud_id} updated",
                "status": "success"
            }
            return jsonify(response), 200
        
        else:
            response = {
                "message": "No server found",
                "status": "failure"
            }
            return jsonify(response), 400


    except Exception as e:
        response = {
            "message": str(e),
            "status": "error"
        }
        return jsonify(response), 500
    
    finally:
        cursor.close()
        conn.close()




@app.route('/del', methods=['DELETE'])
def delete():
    global N
    global schema
    global shards
    global servers
    global hashmaps

    try:
        conn = mysql.connector.connect(
    host="metadb",
    user="root",
    password="giri123456",
    database="metadb"
)
        cursor = conn.cursor()

        payload = request.get_json()
        stud_id = payload['Stud_id']
        print(stud_id)

        cursor.execute("SELECT DISTINCT Shard_id FROM ShardT WHERE Stud_id_low <= %s AND Stud_id_low + Shard_size > %s", (stud_id, stud_id))
        row = cursor.fetchone()

        shard_id = row[0]

        cursor.execute("SELECT DISTINCT Server_id FROM MapT WHERE Shard_id = %s", (shard_id,))
        rows = cursor.fetchall()

        if rows:
            for row in rows:
                
                server = row[0]

                while True:
                    try:
                        response = requests.delete(f"http://{server}:5000/del",json={
                            "shard":shard_id,
                            "Stud_id":stud_id,
                        },timeout=2000)
                        print(response.json())
                        
                        break

                    except Exception as e:
                        print("retrying")
                        continue
                    
            response = {
                "message": f"Data entry with Stud_id:{stud_id}removed from all replicas",
                "status": "success"
            }
            return jsonify(response), 200
        
        else:
            response = {
                "message": "No server found",
                "status": "failure"
            }
            return jsonify(response), 400


    except Exception as e:
        response = {
            "message": str(e),
            "status": "error"
        }
        return jsonify(response), 500
    
    finally:
        cursor.close()
        conn.close()
    

def is_dead(server_id)->bool:
    print(f"checking the heartbeat of {server_id}...",flush=True)
    for i in range(3):
       try:
         resp = requests.get(f"http://{server_id}:5000/heartbeat",timeout=15)
         if resp.ok:
             return False
       except requests.RequestException as e:
           time.sleep(0.01)
           print("Trying again....",flush=True)
    return True

def respawn(server_id):
    global servers
    dead_server_shards = servers.pop(server_id)
    conn  = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    cursor.execute("DELETE FROM MapT WHERE Server_id=%s",(server_id,))
    new_server = "rspn"+server_id
    
    command =  f"docker run --name {new_server} --network net1 -d server"
    result = subprocess.run(command,shell=True,text=True)
            
    if result.returncode == 0:
        while True:
                    try:
                        print({
                            "schema":schema,
                            "shards":dead_server_shards
                        },flush=True)
                        response = requests.post(f"http://{new_server}:5000/config",json={
                            "schema":schema,
                            "shards":dead_server_shards
                        },timeout=2000)
                        print(response.json())
                        break
                    except Exception as e:
                        # print("retrying")
                        continue
        for sh in dead_server_shards:
            cursor.execute("SELECT Server_id FROM MapT WHERE Shard_id=%s",(sh,))
            row = cursor.fetchone()
            sh_server = row[0]
            resp = requests.get(f"http://{sh_server}:5000/copy",json={
                "shards":[sh]
            },timeout=20)
            if resp.status_code == 200:
                data = resp.json()[sh]
                if len(data) == 0:
                    continue
                resp1 = requests.post(f"http://{new_server}:5000/write",json={
                    "shard":sh,
                    "curr_idx": 507,
                    "data": data
                })
                print(resp1.json())
                if not resp1.ok:
                    return "some error"
                cursor.execute("INSERT INTO MapT (Shard_id, Server_id) VALUES (%s, %s)",(new_server,sh))
                print(f"Successfully copied {sh} from {sh_server} to {new_server}")
    servers[new_server] = dead_server_shards
    print(f"Successfully respawned {server_id}:{new_server}")
    

def checking_health():
    while True:
        # get locks over shared resource 
        time.sleep(30)
        __servers = list(servers)
        for server_id in __servers:
            if is_dead(server_id):
                respawn(server_id)

if __name__ == "__main__":
    # t1 = threading.Thread(target=lambda: app.run(host='0.0.0.0', port=5000,debug=True))
    t2 = threading.Thread(target=checking_health)

    t2.start()
    app.run(host='0.0.0.0', port=5000,debug=True)
    # t1.start()
    # t1.join()
    t2.join()
