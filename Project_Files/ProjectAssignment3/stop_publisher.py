import os
import json
from concurrent  import futures
from google.oauth2 import service_account
from google.cloud import pubsub_v1
from publish_stop_module import gather

def main():
    SERVICE_ACCOUNT_FILE = os.path.join("/", "home", "hathcoat", "stop_service_key.json")
    GCP_PROJECT_ID = "data-engineering-2025-456119"
    TOPIC_ID = "trimet-stop"


    collect = gather(GCP_PROJECT_ID, TOPIC_ID)
    vehicle_ids = collect.get_vehicle_ids()
    collect.load_vehicles(vehicle_ids)
    collect.publish_records(SERVICE_ACCOUNT_FILE)

if __name__ == "__main__":
    main()
