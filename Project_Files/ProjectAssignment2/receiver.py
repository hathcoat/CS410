# Cody Hathcoat     CS410

from google.cloud import pubsub_v1 # Google cloud client library for Pub/Sub
from collections import defaultdict
import os
import json
import datetime
import signal
import sys
import io
import psycopg2
from validate_transform import validate_transform
from dotenv import load_dotenv, dotenv_values

GCP_PROJECT_ID = "data-engineering-2025-456119"
SUBSCRIPTION_ID = "trimet-sub"

subscriber = pubsub_v1.SubscriberClient() #Create a Subscriber client
subscription_path = subscriber.subscription_path(GCP_PROJECT_ID, SUBSCRIPTION_ID) #subscription path.

# File configuration
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
today = datetime.datetime.now().strftime("%Y-%m-%d")
output_dir = os.path.join(BASE_DIR, "received", today)
os.makedirs(output_dir, exist_ok=True)
output_file_path = os.path.join(output_dir, f"received_{today}.json")

record_buffer = []
record_to_write = []
is_first_record = None

if not os.path.exists(output_file_path) or os.stat(output_file_path).st_size == 0:
    with open(output_file_path, 'w') as f:
        f.write("[\n")
    os.chmod(output_file_path, 0o666)

is_first_record = True

def dbconnect():
    load_dotenv()
    return psycopg2.connect(
        dbname="postgres",
        user="postgres",
        password=os.getenv('PASSWORD'),
        host="localhost"
    )

def shutdown_handler(signum, frame):
    print(f"Received signal {signum}. Finalizing JSON file.")
    if record_buffer:
        process_batch()
    try:
        with open(output_file_path, 'rb') as f:
            f.seek(-2, os.SEEK_END)
            while f.read(1) != b'\n':
                f.seek(-2, os.SEEK_CUR)
            last_line = f.readline().strip().decode()

        if last_line != ']':
            with open(output_file_path, 'a') as f:
                f.write("\n]\n")
        else:
            print("File already ends with closing bracket.")

    except Exception as e:
        print(f"Failed to finalize file: {e}")
    finally:
        streaming_pull_future.cancel()
        sys.exit(0)

#Run necessary assertions between records.
def process_batch():
    global record_buffer, record_to_write, is_first_record

    BASE_DIR = os.path.dirname(os.path.abspath(__file__))
    today = datetime.datetime.now().strftime("%Y-%m-%d")
    output_dir = os.path.join(BASE_DIR, "received", today)
    os.makedirs(output_dir, exist_ok=True)
    output_file_path = os.path.join(output_dir, f"received_{today}.json")
    with open(output_file_path, 'a') as f:
        for record in record_to_write:
            if not is_first_record:
                f.write(",\n")
            json.dump(record, f, indent=2)
            is_first_record = False

    record_to_write = []
    validator = validate_transform(record_buffer)
    validator.first_validate()
    validator.transform_data()
    validator.second_validate()
    valid_records = validator.get_data()

    if valid_records:
        conn = dbconnect()
        conn.autocommit=True
        seen_trips = set()
        with conn.cursor() as cursor:
            #Load Trip table.
            seen_trips = set()
            output = io.StringIO()
            output_trip = io.StringIO()

            for r in valid_records:
                trip_id = r["EVENT_NO_TRIP"]
                vehicle_id = r["VEHICLE_ID"]

                if trip_id not in seen_trips:
                    output_trip.write(f"{trip_id},{vehicle_id}\n") #LINE ADDED
                    seen_trips.add(trip_id)

                #Prepare breadcrumb data for COPY
                if all(field in r for field in ("TIMESTAMP", "GPS_LATITUDE", "GPS_LONGITUDE", "SPEED", "EVENT_NO_TRIP")):
                    output.write(f"{r['TIMESTAMP']},{r['GPS_LATITUDE']},{r['GPS_LONGITUDE']},{r['SPEED']},{r['EVENT_NO_TRIP']}\n")

            #COPY to database
            try:
                #This should add to trip table (This won't be here for part 3)
                try:
                    output_trip.seek(0)
                    cursor.copy_from(output_trip, 'trip', sep=',',
                                     columns=('trip_id', 'vehicle_id')) 
                except Exception as e:
                    print("Already added trip.")

                output.seek(0)
                cursor.copy_from(output, 'breadcrumb', sep=',',
                                 columns=('tstamp', 'latitude', 'longitude', 'speed', 'trip_id'))


            except Exception as e:
                print(f"Error using copy_from: {e}")
        conn.close()

    #Keep records that haven't found their speed yet.
    valid_ids = {id(r) for r in valid_records}
    record_buffer = [r for r in record_buffer if id(r) not in valid_ids]
                
# Funcation that is called every time a message is received from Pub/Sub.
# callback decods, parses, appends, and acks the message
def callback(message):
    global record_buffer
    try:    
        data = json.loads(message.data.decode("utf-8")) # Convert from bytes to str to dictionary
        record_buffer.append(data)
        record_to_write.append(data)
        message.ack()
    except Exception as e:
        print(f"Error while processing: {e}")

signal.signal(signal.SIGTERM, shutdown_handler)
signal.signal(signal.SIGINT, shutdown_handler)
# Listen to the subscription (Designed to run in the background)
streaming_pull_future = subscriber.subscribe(subscription_path, callback=callback)
print(f"Listening on {subscription_path}...")

while(1):
    try:
        #Keep program running and listening.
        print("Reading data.")
        streaming_pull_future.result(timeout=600) 

    except Exception as e:
        print("Processing.")
        process_batch()
