from google.cloud import pubsub_v1
import requests
import  os
import json
import time
from google.auth import default

creds, project = default()
print(f"Python client is using project: {project}")
print(f"Credentials: {creds.__class__.__name__}")

# TODO(developer)
project_id = "data-transport-lab"
topic_id = "my-topic"

publisher = pubsub_v1.PublisherClient()
# The `topic_path` method creates a fully qualified identifier
# in the form `projects/{project_id}/topics/{topic_id}`
topic_path = publisher.topic_path(project_id, topic_id)

file_dir = os.path.dirname(os.path.abspath(__file__))
#file_path = os.path.join(file_dir, "bcsample.json") #For the 2 vehichle record
file_path = os.path.join(file_dir, "all_vehicles_2025-04-10.json") #For the 200 vehicle record

with open(file_path, 'r') as f:
    try:
        records = json.load(f)
    except json.JSONDecodeError as e:
        print("Failed to parse JSON:", e)
        records = []

start_time = time.time()
print(f"Publishing {len(records)} records to {topic_path}...\n")

futures = []
for i, record in enumerate(records, 1):
    try:

        data_str = json.dumps(record) # Convert dict to JSON string
        data = data_str.encode("utf-8") #Convert to bytes
        future = publisher.publish(topic_path, data)
        print(f"Publishing record {i}...")
     #   message_id = future.result()
        futures.append(future)
     #   print(f"Published record {i+1}: {future.result()}")

    except Exception as e:
        print(f"Failed to publish record {i+1}: {e}")
for i, future in enumerate(futures, 1):
    try:
        message_id = future.result(timeout=10)

    except Exception as e:
        print(f"Record {i} failed to publish: {e}")
        

end_time = time.time()
elapsed = end_time - start_time
print(f"Published {len(records)} messages to {topic_path}")
print(f"Script finished in {elapsed:.2f} seconds.")