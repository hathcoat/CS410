from google.cloud import pubsub_v1 # Google cloud client library for Pub/Sub
import os
import signal
import json
import datetime
import signal
import sys
import psycopg2
import io
import traceback
from dotenv import load_dotenv, dotenv_values
from validate_stop import validate_stop

class receiver:
    def __init__(self, project_id, sub_id):
        self.record_buffer = []
        self.project_id = project_id
        sub_id = sub_id
        self.streaming_pull_future = None

        subscriber = pubsub_v1.SubscriberClient() #Create a Subscriber client
        subscription_path = subscriber.subscription_path(project_id, sub_id) #subscription path.

        # Listen to the subscription (Designed to run in the background)
        self.streaming_pull_future = subscriber.subscribe(subscription_path, callback=self.callback)

        signal.signal(signal.SIGINT, self._signal_handler)

    def start_receiving(self):
        while(1):
            try:
                print("Reading Data")
                #Keep program running and listening.
                self.streaming_pull_future.result(timeout=300) 

            except Exception as e:
                if len(self.record_buffer) > 0:
                    print("Processing batch now")
                    self.process_batch()
                else:
                    print("No records in buffer")

    def dbconnect(self):
        load_dotenv()
        return psycopg2.connect(
            dbname="postgres",
            user="postgres",
            password=os.getenv('PASSWORD'),
            host="localhost"
        )

    def callback(self, message):
        try:    
            data = json.loads(message.data.decode("utf-8")) # Convert from bytes to str to dictionary
            self.record_buffer.append(data)
            message.ack()

        except Exception as e:
            print(f"Error while processing: {e}")

    def process_batch(self):
        self.save_to_json()
        validator = validate_stop(self.record_buffer)
        validator.check_data()
        data = validator.get_data()
        unique_records = validator.get_unique_trip_recs()
        conn = self.dbconnect()
        try:
            if unique_records:
                with conn.cursor() as cursor:
                    cursor.execute("BEGIN;")
                    cursor.execute("""
                        CREATE TEMP TABLE IF NOT EXISTS trip_staging(
                            trip_id INTEGER,
                            route_id INTEGER,
                            vehicle_id INTEGER,
                            service_key service_type,
                            direction  tripdir_type
                        ) ON COMMIT DROP;
                    """)
                    #Load Trip table.
                    output = io.StringIO()
                    for r in unique_records:
                        if all(field in r for field in ("vehicle_number", "route_number", "trip_number", "service_key", "direction")):
                            output.write(f"{r['trip_number']},{r['route_number']},{r['vehicle_number']},{r['service_key']},{r['direction']}\n")

                    output.seek(0)
                    try:
                        cursor.copy_from(output, 'trip_staging', sep=',',
                                         columns=('trip_id', 'route_id', 'vehicle_id', 'service_key', 'direction'))
                    except Exception as e:
                        print(f"Error: {e}")

                    #Transfer data to the main table.
                    cursor.execute("""
                        WITH ins AS(
                        INSERT INTO trip (trip_id, route_id, vehicle_id, service_key, direction)
                        SELECT trip_id, route_id, vehicle_id, service_key, direction
                        FROM trip_staging
                        ON CONFLICT (trip_id) DO NOTHING
                        RETURNING *
                        )
                        SELECT COUNT(*) FROM ins;
                        
                    """)
                    inserted_count = cursor.fetchone()[0]
                    print(f"{inserted_count} new trip records inserted.")
                    cursor.execute("COMMIT;")
        except Exception as e:
            print(f"Error loading trip data: {e}")
            conn.rollback()

        finally:
            conn.close()
            self.record_buffer = []

    def save_to_json(self, file_name_prefix='stop_data'):
        today = datetime.datetime.now().strftime("%Y-%m-%d")
        base_dir = os.path.dirname(os.path.abspath(__file__))
        output_dir = os.path.join(base_dir, "stops", today)
        os.makedirs(output_dir, exist_ok=True)
        output_file_path = os.path.join(output_dir, f"{file_name_prefix}_{today}.json")

        if os.path.exists(output_file_path) and os.path.getsize(output_file_path) > 0:
            with open(output_file_path, "r+") as file:
                data = json.load(file)
                data.append(self.record_buffer)
                file.seek(0)
                json.dump(data, file, indent=2)
        else:
            with open(output_file_path, 'w') as outfile:
                json.dump(self.record_buffer, outfile, indent = 2)

    def _signal_handler(self, signum, frame):
        if self.record_buffer:
            self.process_batch()
        self.streaming_pull_future.cancel()
        sys.exit(0)

    def set_data(self, data):
        self.record_buffer = data
