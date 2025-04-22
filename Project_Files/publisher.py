# Cody Hathcoat     CS410

import requests
import os
import datetime
import csv
import json
from google.cloud import pubsub_v1

print(f"[{datetime.datetime.now()}] Publisher started.")
print(f"User: {os.getlogin()}")
#print(f"GOOGLE_APPLICATION_CREDENTIALS: {os.environ.get('GOOGLE_APPLICATION_CREDENTIALS')}")
# GCP and topic
GCP_PROJECT_ID = "data-engineering-2025-456119"
TOPIC_ID = "trimet-breadcrumbs"

#Initialize Pub/Sub
publisher = pubsub_v1.PublisherClient()
topic_path = publisher.topic_path(GCP_PROJECT_ID, TOPIC_ID)

#File and path setup
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
VEHICLE_ID_FILE = os.path.join(BASE_DIR, "vehicle_ids.csv")
today = datetime.datetime.now().strftime("%Y-%m-%d")
output_dir = os.path.join(BASE_DIR, "errors", today)
os.makedirs(output_dir, exist_ok=True)
error_log_path = os.path.join(output_dir, f"errors_{today}.log")

#Load vehicle IDs from CSV
def load_vehicle_ids(csv_path):
   # BASE_DIR = os.path.dirname(os.path.abspath(__file__))
    #csv_path = os.path.join(BASE_DIR, 'vehicle_ids.csv')
    with open(csv_path, newline='') as csvfile:
        reader = csv.reader(csvfile)
        next(reader)
        return [row[0] for row in reader if row]

#VEHICLE_ID_FILE = "vehicle_ids.csv"
vehicle_ids = load_vehicle_ids(VEHICLE_ID_FILE) 
BASE_URL = "https://busdata.cs.pdx.edu/api/getBreadCrumbs?vehicle_id="

#output_file_path = os.path.join(output_dir, f"all_vehicles_{today}.json")
#error_log_path = f"errors_{today}.log"
all_vehicle_data = []

futures = []
with open(error_log_path, 'w') as error_log:
    for vehicle_id in vehicle_ids:
        url = f"{BASE_URL}{vehicle_id}"
        try:
            response = requests.get(url)

            if(response.status_code == 404):
               error_log.write(f"{vehicle_id} - 404 Not Found\n")
               print(f"No data for vehicle {vehicle_id} (404 Not Found)")
               continue

            response.raise_for_status() #Raise err for bad status codes.
            vehicle_data = response.json()

            if isinstance(vehicle_data, list):
                for record in vehicle_data:
                    record_json = json.dumps(record)
                    future = publisher.publish(topic_path, record_json.encode("utf-8"))
                    futures.append(future)
                    #future.result()

                for future in futures:
                    future.result()
                print(f"Published {len(vehicle_data)} records for vehicle {vehicle_id}")
            else:
                print(f"Unexpected format for vehicle {vehicle_id}, skipping")

        except requests.exceptions.RequestException as e:
            error_log.write(f"{vehicle_id} - Request failed: {e}\n")
            print(f"Failed to fetch for vehicle {vehicle_id}: {e}")

print(f"All data published to Pub/Sub.")
print(f"Errors (if any) logged to: {error_log_path}")
os.chmod(error_log_path, 0o777)
