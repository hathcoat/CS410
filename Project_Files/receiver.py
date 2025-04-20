# Cody Hathcoat     CS410

#Before running make sure to export your GCP credentials:
# export GOOGLE_APPLICATION_CREDENTIALS="/path/to/key.json"

from google.cloud import pubsub_v1 # Google cloud client library for Pub/Sub
import os
import json
import datetime
import signal
import sys

GCP_PROJECT_ID = "data-engineering-2025-456119"
SUBSCRIPTION_ID = "trimet-sub"

subscriber = pubsub_v1.SubscriberClient() #Create a Subscriber client
subscription_path = subscriber.subscription_path(GCP_PROJECT_ID, SUBSCRIPTION_ID) #subscription path.

# File configuration
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
today = datetime.datetime.now().strftime("%Y-%m-%d")
output_dir = os.path.join(BASE_DIR, "received", today)
#output_dir = f"/data/received/{today}"
os.makedirs(output_dir, exist_ok=True)
output_file_path = os.path.join(output_dir, f"received_{today}.json")

if not os.path.exists(output_file_path):
    with open(output_file_path, 'w') as f:
        f.write("[\n")

is_first_record = True

def shutdown_handler(signum, frame):
    print(f"Received signal {signum}. Finalizing JSON file.")
    try:
        with open(output_file_path, 'a') as f:
            f.write("\n]\n")
    except Exception as e:
        print(f"Failed to finalize file: {e}")
    finally:
        streaming_pull_future.cancel()
        sys.exit(0)

# Funcation that is called every time a message is received from Pub/Sub.
# callback decods, parses, appends, and acks the message
def callback(message):
    global is_first_record
    data = json.loads(message.data.decode("utf-8")) # Convert from bytes to str to dictionary

    with open(output_file_path, 'a') as f:
        if not is_first_record:
            f.write(",\n")
        json.dump(data, f, indent=2)
        is_first_record = False
    message.ack()

signal.signal(signal.SIGTERM, shutdown_handler)
signal.signal(signal.SIGINT, shutdown_handler)
# Listen to teh subscription (Designed to run in the background)
streaming_pull_future = subscriber.subscribe(subscription_path, callback=callback)
print(f"Listening on {subscription_path}...")

try:
    #Keep program running and listening.
    streaming_pull_future.result() 
except KeyboardInterrupt:
    #Save messages to a file after Ctrl+C
    with open(output_file_path, 'a') as f:
        f.write("\n]\n")
    print(f"\nFinished. Output saved to: {output_file_path}")

except Exception as  e:
    print(f"Error: {e}")
    streaming_pull_future.cancel()
