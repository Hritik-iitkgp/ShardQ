import requests
import time

def add_shards_and_test_speed():
    # New shards to add
    new_shards = [
        {"Stud_id_low": 12288, "Shard_id": "sh5", "Shard_size": 4096},
        {"Stud_id_low": 16384, "Shard_id": "sh6", "Shard_size": 4096},
        {"Stud_id_low": 20480, "Shard_id": "sh7", "Shard_size": 4096},
        {"Stud_id_low": 24576, "Shard_id": "sh8", "Shard_size": 4096}
    ]
    
    # Updated servers with new shard mappings
    updated_servers = {
        "Server4": ["sh3", "sh5"],
        "Server5": ["sh2", "sh6"],
        "Server6": ["sh1", "sh7"]
    }
    
    # Send request to add new shards and measure time taken
    add_shards_url = "http://localhost:5000/add"
    payload = {
        "n": 3,  # Number of new servers (should be equal to the number of servers in updated_servers)
        "new_shards": new_shards,
        "servers": updated_servers
    }
    start_time_add_shards = time.time()
    response = requests.post(add_shards_url, json=payload)
    end_time_add_shards = time.time()
    print("Time taken for adding shards:", end_time_add_shards - start_time_add_shards)
    print("Response from adding shards:", response.json())
    
    # Test speed by performing 1000 read and 1000 write requests
    # You can implement this part based on your existing client code for read and write requests
    # Make sure to measure the time taken for these operations

# Main function to add shards and test speed
def main():
    add_shards_and_test_speed()

if __name__ == "__main__":
    main()
