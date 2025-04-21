import requests
import  os
import datetime
import json

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
today = datetime.datetime.now().strftime("%Y-%m-%d")
URL1 = "https://busdata.cs.pdx.edu/api/getBreadCrumbs?vehicle_id=2908"
URL2 = "https://busdata.cs.pdx.edu/api/getBreadCrumbs?vehicle_id=2910"

#output_dir = os.path.join(BASE_DIR, "data", today)
output_dir = BASE_DIR
os.makedirs(output_dir, exist_ok=True)

response = requests.get(URL1)
output_path = os.path.join(output_dir, "bcsample.json")

with open(output_path, 'w') as f:
    f.write(response.text)
print("Saved 2908")

response2 = requests.get(URL2)
output_path = os.path.join(output_dir, "bcsample.json")
with open(output_path, 'w') as f:
    f.write(response2.text)
print("Saved 2910")