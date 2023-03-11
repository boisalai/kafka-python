# Import packages
import pandas as pd
import json
import datetime as dt
from time import sleep
from kafka import KafkaProducer

# Initialize the Kafka Producer Client
producer = KafkaProducer(bootstrap_servers=['localhost:9092'])
print(f'Initialized Kafka producer at {dt.datetime.utcnow()}')

# Set a basic message counter and define the file path
counter = 0
filename = "green_tripdata_2019-01.csv.gz"
filename = "fhv_tripdata_2019-01.csv.gz"
filepath = f"/Users/boisalai/GitHub/kafka-python/data/{filename}"

for chunk in pd.read_csv(filepath, compression="gzip", chunksize=10000):
    # For each chunk, convert the invoice date into the correct time format
    if filename.startswith("fhv"):
        chunk["pickup_datetime"] = pd.to_datetime(chunk["pickup_datetime"])
        chunk["dropoff_datetime"] = pd.to_datetime(chunk["dropOff_datetime"])
    else:
        chunk["pickup_datetime"] = pd.to_datetime(chunk["lpep_pickup_datetime"])
        chunk["dropoff_datetime"] = pd.to_datetime(chunk["lpep_dropoff_datetime"])

    chunk = chunk[['PULocationID', 'pickup_datetime', 'dropoff_datetime']]

    # Set the counter as the message key
    key = str(counter).encode()

    # Convert the data frame chunk into a dictionary
    chunkd = chunk.to_dict()

    # Encode the dictionary into a JSON Byte Array
    data = json.dumps(chunkd, default=str).encode('utf-8')

    # Send the data to Kafka
    producer.send(topic="trips", key=key, value=data)

    # Sleep to simulate a real-world interval
    sleep(0.5)

    # Increment the message counter for the message key
    counter = counter + 1

    print(f'Sent record to topic at time {dt.datetime.utcnow()}')