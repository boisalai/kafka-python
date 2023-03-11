# kafka-python

Here, my code which implements a streaming application
for finding out popularity of `PUlocationID` across green and fhv trip datasets.

I use the datasets [fhv_tripdata_2019-01.csv.gz](https://github.com/DataTalksClub/nyc-tlc-data/releases/tag/fhv) and 
[green_tripdata_2019-01.csv.gz](https://github.com/DataTalksClub/nyc-tlc-data/releases/tag/green).

I'm using a macOS computer.

To develop this application, I followed this article:<br>
[How to send tabular time series data to Apache Kafka with Python and Pandas](https://towardsdatascience.com/how-to-send-tabular-time-series-data-to-apache-kafka-with-python-and-pandas-39e2055373c3) from Tomáš Neubauer.

## Step 0: Create conda environment

```bash
conda create -n kafka-env python=3.9
conda activate kafka-env
conda install pip
pip install pandas
pip install kafka-python
brew install java
```

## Step 1: Setting up Kafka

Kafka from the [Apache Kafka Download page](https://kafka.apache.org/downloads) 
(for example, [kafka_2.12–3.3.1.tgz](https://archive.apache.org/dist/kafka/3.3.1/kafka_2.12-3.3.1.tgz)) and 
extract the contents of the file to a convenient location.

In the Kafka directory, open a terminal window and start the zookeeper service with the following command:

```bash
bin/zookeeper-server-start.sh config/zookeeper.properties
```

Open a second terminal window and start the Kafka server with the following command:

```bash
bin/kafka-server-start.sh config/server.properties
```

Next, you’ll need to create a topic called **trips** to store the data.

Open a third terminal window and enter the following command:

```bash
bin/kafka-topics.sh --create --topic trips --bootstrap-server localhost:9092
```

## Step 2: Analyzing the Data

Download datasets [fhv_tripdata_2019-01.csv.gz](https://github.com/DataTalksClub/nyc-tlc-data/releases/tag/fhv) and 
[green_tripdata_2019-01.csv.gz](https://github.com/DataTalksClub/nyc-tlc-data/releases/tag/green).

Read the CSV files into a DataFrame by entering the following commands:

```python
>>> import pandas as pd
>>> df1 = pd.read_csv("/Users/boisalai/GitHub/kafka-python/data/fhv_tripdata_2019-01.csv.gz", compression="gzip")
>>> print(df1.info())
<class 'pandas.core.frame.DataFrame'>
RangeIndex: 23143222 entries, 0 to 23143221
Data columns (total 7 columns):
 #   Column                  Dtype  
---  ------                  -----  
 0   dispatching_base_num    object 
 1   pickup_datetime         object 
 2   dropOff_datetime        object 
 3   PUlocationID            float64
 4   DOlocationID            float64
 5   SR_Flag                 float64
 6   Affiliated_base_number  object 
dtypes: float64(3), object(4)
memory usage: 1.2+ GB
None
>>> df2 = pd.read_csv("/Users/boisalai/GitHub/kafka-python/data/green_tripdata_2019-01.csv.gz", compression="gzip")
>>> print(df2.info())
<class 'pandas.core.frame.DataFrame'>
RangeIndex: 630918 entries, 0 to 630917
Data columns (total 20 columns):
 #   Column                 Non-Null Count   Dtype  
---  ------                 --------------   -----  
 0   VendorID               630918 non-null  int64  
 1   lpep_pickup_datetime   630918 non-null  object 
 2   lpep_dropoff_datetime  630918 non-null  object 
 3   store_and_fwd_flag     630918 non-null  object 
 4   RatecodeID             630918 non-null  int64  
 5   PULocationID           630918 non-null  int64  
 6   DOLocationID           630918 non-null  int64  
 7   passenger_count        630918 non-null  int64  
 8   trip_distance          630918 non-null  float64
 9   fare_amount            630918 non-null  float64
 10  extra                  630918 non-null  float64
 11  mta_tax                630918 non-null  float64
 12  tip_amount             630918 non-null  float64
 13  tolls_amount           630918 non-null  float64
 14  ehail_fee              0 non-null       float64
 15  improvement_surcharge  630918 non-null  float64
 16  total_amount           630918 non-null  float64
 17  payment_type           630918 non-null  int64  
 18  trip_type              630918 non-null  int64  
 19  congestion_surcharge   84538 non-null   float64
dtypes: float64(10), int64(7), object(3)
memory usage: 96.3+ MB
None
```

## Step 3: Sending Data to a Kafka with a Producer

Create a file called `producer.py` and insert the following code block:

```python
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
```

Save your file and run your code with the following command:

```bash
python producer.py
```

## Step 4: Read Data from Kafka with a Consumer

Create a file called `consumer.py` and insert the following code block:

```python
from kafka import KafkaConsumer
import json
import pandas as pd

# Consume all the messages from the topic but do not mark them as 'read' (enable_auto_commit=False)
# so that we can re-read them as often as we like.
consumer = KafkaConsumer('trips',
                         group_id='test-consumer-group',
                         bootstrap_servers=['localhost:9092'],
                         value_deserializer=lambda m: json.loads(m.decode('utf-8')),
                         auto_offset_reset='earliest',
                         enable_auto_commit=False)

for message in consumer:
    mframe = pd.DataFrame(message.value)

    # Count frequency of PUlocationID.
    summary = mframe['PULocationID'].value_counts()

    print(summary)
```

As you can see in the code comments, we are performing a simple calculation 
that outputs a summary of the `PULocationID` frequency for each message batch.

Save the file and run your code with the following command:

```bash
python consumer.py
```

We should see something like this in the terminal:

```txt
Name: PULocationID, Length: 192, dtype: int64
74     715
75     615
41     471
7      377
42     334
      ... 
46       1
186      1
100      1
1        1
206      1
Name: PULocationID, Length: 187, dtype: int64
75     875
74     622
166    483
41     461
7      344
      ... 
253      1
202      1
64       1
120      1
140      1
Name: PULocationID, Length: 193, dtype: int64
74     668
75     655
41     494
7      429
82     414
      ... 
46       1
100      1
170      1
101      1
54       1
```

I see that the most popular `PULocationID` vary from batch to batch, but are always either `74` (*East Harlem North of Manhattan*) or `75` (*East Harlem South of Manhattan*).

Note that the "taxi zones" are presented in the file [Taxi Zone Lookup Table](https://d37ci6vzurychx.cloudfront.net/misc/taxi+_zone_lookup.csv).

## Step 5: Terminating the Kafka Environment

You can terminate the Kafka CLI operations in a sequence.

1. Stop the producer and consumer clients by pressing the `CTRL+C` key combination.
2. Stop running Kafka broker with `CTRL+C`.
3. Then, stop the ZooKeeper service with `CTRL+C`.

Last updated: March 11, 2023
