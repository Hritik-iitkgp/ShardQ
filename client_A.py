import requests
import time
import random

# Replace the following variables with your actual server information
SERVER_URL = "http://localhost:5000"
WRITE_DATA = [{"Stud_id": i, "Stud_name": f"Student_{i}", "Stud_marks": random.randint(0, 30)} for i in range(10000)]


print("OUTPUT FOR A-3:- ")
print()

# Function to perform 10000 write requests
def test_write_requests():
    start_time = time.time()
    
    for data in WRITE_DATA:
        response = requests.post(f"{SERVER_URL}/write", json={"data": [data]})
        if response.status_code != 200:
            print(f"Error occurred while writing data: {response.json()}")
    
    end_time = time.time()
    print(f"Time taken for 10000 write requests: {end_time - start_time} seconds")

# Function to perform 10000 read requests
def test_read_requests():
    start_time = time.time()
    
    for i in range(10000):
        READ_STUD_ID_LOW = random.randint(0, 9000)
        READ_STUD_ID_HIGH = READ_STUD_ID_LOW + random.randint(0, 1000)
        
        response = requests.post(f"{SERVER_URL}/read", json={"Stud_id": {"low": READ_STUD_ID_LOW, "high": READ_STUD_ID_HIGH}})
        if response.status_code != 200:
            print(f"Error occurred while reading data: {response.json()}")
    
    end_time = time.time()
    print(f"Time taken for 10000 read requests: {end_time - start_time} seconds")
    

if __name__ == "__main__":
    print("Testing write requests...")
    test_write_requests()
    print("\nTesting read requests...")
    test_read_requests()

 