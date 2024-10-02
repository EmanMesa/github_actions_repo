import pandas as pd
from datetime import datetime, timedelta
import os
import warnings
import time
import json
from kafka import KafkaProducer

warnings.filterwarnings("ignore", category=FutureWarning)
print("Random Test workflow")
# Initialize index and file path
index = 0
file_name = f"/home/fort-vtc3/Desktop/gpsd_sim_{index}.csv"  # Use escaped backslashes

# Define the data columns
columns = ['ModelSN', 'Time', 'Latitude', 'Longitude', 'Elevation', 'GPS_Speed']

# Data to append
data = ["PC850-8R1-11236", datetime.now().isoformat(), -3.51428, 33.6094, 1077.6, 0]


def produce(content):
    producer = KafkaProducer(
        bootstrap_servers = "192.168.15.166:9092",
        value_serializer = lambda v: json.dumps(v).encode('utf-8')
    )
    
    map_data = {
        "ModelSN":"PC850-8R1-11236",
        "Time":datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
        "Latitude":-3.51428,
        "Longitude":33.6094,
        "Elevation":1077.6,
        "GPS_Speed":0
    }
    data = ["PC850-8R1-11236", datetime.now().strftime('%Y-%m-%d %H:%M:%S'), -3.51428, 33.6094, 1077.6, 0]
    
    
    producer.send('gpsd_truck', data)
    producer.flush()
    producer.close()
    
    print("SENT")
    
    time.sleep(5)
    


while True:
    produce(data)
    # Check if the file exists and read the first entry
    if os.path.exists(file_name) and os.path.getsize(file_name) > 0:
        # Read the first row to get the timestamp
        first_entry = pd.read_csv(file_name, nrows=1)
        first_time = pd.to_datetime(first_entry['Time'][0])
        
        # Get the current time
        current_time = datetime.now()
        
        # Check the difference in hours
        if (current_time - first_time) > timedelta(hours=1):
            # Increment index and change file name
            index += 1
            file_name = f"/home/fort-vtc3/Desktop/gpsd_sim_{index}.csv"
            print(f"File name changed to: {file_name}")
            with open(file_name, 'w', newline='') as f:
                pd.DataFrame(columns=columns).to_csv(f, header=True, index=False)

    else:
        # If the file doesn't exist or is empty, write the headers
        with open(file_name, 'w', newline='') as f:
            pd.DataFrame(columns=columns).to_csv(f, header=True, index=False)

    # Append data to the file
    with open(file_name, 'a', newline='') as f:
        # Update the current time in the data
        data[1] = datetime.now()
        pd.DataFrame([data], columns=columns).to_csv(f, header=False, index=False)
        
    print("Written to file")
        
    time.sleep(10)
