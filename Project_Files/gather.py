# Cody Hathcoat     CS410
import requests
import os
import datetime
import csv
import json

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
VEHICLE_ID_FILE = os.path.join(BASE_DIR, "vehicle_ids.csv")

def load_vehicle_ids(csv_path):
   # BASE_DIR = os.path.dirname(os.path.abspath(__file__))
    csv_path = os.path.join(BASE_DIR, 'vehicle_ids.csv')
    with open(csv_path, newline='') as csvfile:
        reader = csv.reader(csvfile)
        next(reader)
        return [row[0] for row in reader if row]


#VEHICLE_ID_FILE = "vehicle_ids.csv"
vehicle_ids = load_vehicle_ids(VEHICLE_ID_FILE) 


BASE_URL = "https://busdata.cs.pdx.edu/api/getBreadCrumbs?vehicle_id="

today = datetime.datetime.now().strftime("%Y-%m-%d")
#output_dir = f"data/{today}"
output_dir = os.path.join(BASE_DIR, "data", today)
os.makedirs(output_dir, exist_ok=True)

output_file_path = os.path.join(output_dir, f"all_vehicles_{today}.json")
#error_log_path = f"errors_{today}.log"
error_log_path = os.path.join(output_dir, f"errors_{today}.log")

all_vehicle_data = []
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
                all_vehicle_data.extend(vehicle_data)
                print(f"Added {len(vehicle_data)} records for vehicle {vehicle_id}")
            else:
                print(f"Unexpected format for vehicle {vehicle_id}, skipping")

            #Save to the file
#            output_path = os.path.join(output_dir, f"vehicle_{vehicle_id}.json")
#            with open(output_path, 'w') as f:
#                f.write(response.text)

#            print(f"Saved for vehicle {vehicle_id} to {output_path}")

        except requests.exceptions.RequestException as e:
            error_log.write(f"{vehicle_id} - Request failed: {e}\n")
            print(f"Failed to fetch for vehicle {vehicle_id}: {e}")

with open(output_file_path, 'w') as f:
    json.dump(all_vehicle_data, f, indent=2)

print(f"All vehicle data saved to: {output_file_path}")
print(f"Errors (if any) logged to: {error_log_path}")
