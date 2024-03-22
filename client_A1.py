import requests
import time
import random

# Replace the following variables with your actual server information
SERVER_URL = "http://localhost:5000"
WRITE_DATA = [{"Stud_id": i, "Stud_name": f"Student_{i}", "Stud_marks": random.randint(0, 100)} for i in range(1000)]
READ_STUD_ID_LOW = random.randint(0, 900)
READ_STUD_ID_HIGH = READ_STUD_ID_LOW + 1000

# Function to perform 1000 write requests
def test_write_requests():
    start_time = time.time()
    for data in WRITE_DATA:
        response = requests.post(f"{SERVER_URL}/write", json={"data": [data]})
        if response.status_code != 200:
            print(f"Error occurred while writing data: {response.json()}")
    end_time = time.time()
    print(f"Time taken for 1000 write requests: {end_time - start_time} seconds")

# Function to perform 1000 read requests
def test_read_requests():
    start_time = time.time()
    response = requests.post(f"{SERVER_URL}/read", json={"Stud_id": {"low": READ_STUD_ID_LOW, "high": READ_STUD_ID_HIGH}})
    if response.status_code == 200:
        #data = response.json()
        end_time = time.time()
        print(f"Time taken for 1000 read requests: {end_time - start_time} seconds")
        #print(f"Data retrieved: {data}")
    else:
        print(f"Error occurred while reading data: {response.json()}")

if __name__ == "__main__":
    print("Testing write requests...")
    test_write_requests()
    print("\nTesting read requests...")
    test_read_requests()
# import asyncio
# import aiohttp
# import time
# import random

# # Endpoint URLs
# READ_URL = "http://localhost:5000/read"
# WRITE_URL = "http://localhost:5000/write"

# # Generate random student data for write requests
# WRITE_DATA = [{"Stud_id": i, "Stud_name": f"Student_{i}", "Stud_marks": random.randint(0, 100)} for i in range(10)]

# # Function to perform 1000 write requests asynchronously
# async def test_write_requests():
#     async with aiohttp.ClientSession() as session:
#         tasks = []
#         for data in WRITE_DATA:
#             tasks.append(asyncio.ensure_future(write_request(session, data)))
#         await asyncio.gather(*tasks)

# # Function to make a single write request
# async def write_request(session, data):
#     async with session.post(WRITE_URL, json={"data": [data]}) as response:
#         if response.status != 200:
#             print(f"Error occurred while writing data: {await response.json()}")

# # Function to perform 1000 read requests asynchronously
# async def test_read_requests():
#     async with aiohttp.ClientSession() as session:
#         tasks = []
#         for _ in range(1000):
#             # Generate random student ID range for read requests
#             Stud_id_low = random.randint(0, 900)
#             Stud_id_high = Stud_id_low + 1000
#             tasks.append(asyncio.ensure_future(read_request(session, Stud_id_low, Stud_id_high)))
#         await asyncio.gather(*tasks)

# # Function to make a single read request
# async def read_request(session, Stud_id_low, Stud_id_high):
#     async with session.post(READ_URL, json={"Stud_id": {"low": Stud_id_low, "high": Stud_id_high}}) as response:
#         if response.status == 200:
#             data = await response.json()
#             # Uncomment the line below if you want to print the retrieved data
#             # print(f"Data retrieved: {data}")
#         else:
#             print(f"Error occurred while reading data: {await response.json()}")

# if __name__ == "__main__":
#     print("Testing write requests...")
#     start_time = time.time()
#     asyncio.run(test_write_requests())
#     end_time = time.time()
#     print(f"Time taken for 1000 write requests: {end_time - start_time} seconds")

#     print("\nTesting read requests...")
#     start_time = time.time()
#     asyncio.run(test_read_requests())
#     end_time = time.time()
#     print(f"Time taken for 1000 read requests: {end_time - start_time} seconds")
