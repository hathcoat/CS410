# Cody Hathcoat     CS410
import requests
import os
import datetime
import csv
import json
import pandas as pd
import re
from concurrent  import futures
from google.oauth2 import service_account
from google.cloud import pubsub_v1
from urllib.request import urlopen
from bs4 import BeautifulSoup

class gather:
    def __init__(self, project_id=None, topic_id=None):
        self.vehicle_data = []
        self.project_id = project_id
        self.topic_id = topic_id

    def get_vehicle_ids(self):
        base_dir = os.path.dirname(os.path.abspath(__file__))
        csv_path = os.path.join(base_dir, 'vehicle_ids.csv')
        vehicle_ids = []
        with open(csv_path, newline='') as csvfile:
            reader = csv.reader(csvfile)
            next(reader)
            for row in reader:
                vehicle_ids.append(row[0])
        return vehicle_ids

    def load_vehicles(self, vehicle_ids) :
        base_url = "https://busdata.cs.pdx.edu/api/getStopEvents?vehicle_num="

        for vehicle_id in vehicle_ids:
            try:
                url = f"{base_url}{vehicle_id}"
                html = urlopen(url) #Get html out of page
                soup = BeautifulSoup(html, 'lxml')

                main_header = soup.find(text=re.compile(r"Trimet CAD/AVL stop data for"))
                date_match = re.search(r"\d{4}-\d{2}-\d{2}", main_header)
                date_str = date_match.group(0) if date_match else "unknown_date"

                trip_headers = soup.find_all(string=re.compile(r"Stop events for PDX_TRIP \d+"))
    
                for trip_header in trip_headers:
                    trip_number = re.search(r"PDX_TRIP (\d+)", trip_header).group(1)
                    next_sibling = trip_header.find_next("table")

                    if next_sibling:
                        headers = [th.text.strip() for th in next_sibling.find_all('th')]

                        for row in next_sibling.find_all('tr')[1:]: #Skip header
                            columns = row.find_all('td')
                            if len(columns) == len(headers):
                                row_data = {headers[i]: columns[i].text.strip() for i in range(len(headers))}
                                row_data["trip_number"] = trip_number
                                row_data["date"] = date_str
                                self.vehicle_data.append(row_data)

            except Exception as e:
                print(f"No data: {e}")


    def save_to_json(self, file_name_prefix='stop_data'):
        today = datetime.datetime.now().strftime("%Y-%m-%d")
        base_dir = os.path.dirname(os.path.abspath(__file__))
        output_dir = os.path.join(base_dir, "stops", today)
        os.makedirs(output_dir, exist_ok=True)
        output_file_path = os.path.join(output_dir, f"{file_name_prefix}_{today}.json")

        if os.path.exists(output_file_path) and os.path.getsize(output_file_path) > 0:
            with open(output_file_path, "r+") as file:
                data = json.load(file)
                data.append(self.vehicle_data)
                file.seek(0)
                json.dump(data, file, indent=2)
        else:
            with open(output_file_path, 'w') as outfile:
                json.dump(self.vehicle_data, outfile, indent = 2)


    def publish_records(self, service_account_file):
        pubsub_creds = (
                service_account.Credentials.from_service_account_file(service_account_file)
            )
        publisher = pubsub_v1.PublisherClient(
                credentials=pubsub_creds
            )

        topic_path = publisher.topic_path(self.project_id, self.topic_id)
        future_list = []
        count = 0

        for rec in self.vehicle_data:
            data_str = json.dumps(rec)

            #Data must be bytestring
            data = data_str.encode()

            #When publish, client returns a future.
            future = publisher.publish(topic_path, data)

            future.add_done_callback(self.future_callback)

            future_list.append(future)
            count += 1

            if count % 10000 == 0:
                print(count)
        for future in futures.as_completed(future_list):
            continue
        print(f"Published {count} records")

    def future_callback(self, future):
        try:
            future.result()
        except Exception as e:
            print(f"An error occured: {e}")

    def get_data(self):
        return self.vehicle_data

    def update_data(self, data):
        self.vehicle_data = data
