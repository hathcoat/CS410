# Cody Hathcoat     CS410

import requests
import os
import datetime
import csv
import json
from google.cloud import pubsub_v1

GCP_PROJECT_ID = "data-engineering-2025-456119"
TOPIC_ID = "trimet-breadcrumbs"

#Initialize Pub/Sub
publisher = pubsub_v1.PublisherClient()
topic_path = publisher.topic_path(GCP_PROJECT_ID, TOPIC_ID)

#File and path setup
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
VEHICLE_ID_FILE = os.path.join(BASE_DIR, "vehicle_ids.csv")
today = datetime.datetime.now().strftime("%Y-%m-%d")

#Load vehicle IDs from CSV
def load_vehicle_ids(csv_path):
    with open(csv_path, newline='') as csvfile:
        reader = csv.reader(csvfile)
        next(reader)
        return [row[0] for row in reader if row]

vehicle_ids = load_vehicle_ids(VEHICLE_ID_FILE) 
BASE_URL = "https://busdata.cs.pdx.edu/api/getBreadCrumbs?vehicle_id="

futures = []
for vehicle_id in vehicle_ids:
    url = f"{BASE_URL}{vehicle_id}"
    try:
        response = requests.get(url)

        if(response.status_code != 200):
           continue

        response.raise_for_status() #Raise err for bad status codes.
        vehicle_data = response.json()

        if isinstance(vehicle_data, list):
            for record in vehicle_data:
                record_json = json.dumps(record)
                future = publisher.publish(topic_path, record_json.encode("utf-8"))
                futures.append(future)

            for future in futures:
                future.result()
        else:
            print(f"Unexpected format for vehicle {vehicle_id}, skipping")

    except Exception as e:
        print(f"Issue publishing record: {e}")
