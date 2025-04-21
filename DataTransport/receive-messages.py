import json
import threading
import time
from concurrent.futures import TimeoutError
from google.cloud import pubsub_v1
from google.api_core.exceptions import Cancelled


# TODO(developer)
project_id = "data-transport-lab"
subscription_id = "my-sub"
# Number of seconds the subscriber should listen for messages
timeout = 30.0

subscriber = pubsub_v1.SubscriberClient()
# The `subscription_path` method creates a fully qualified identifier
# in the form `projects/{project_id}/subscriptions/{subscription_id}`
subscription_path = subscriber.subscription_path(project_id, subscription_id)

message_count = 0
lock = threading.Lock()

def callback(message: pubsub_v1.subscriber.message.Message) -> None:
   # Decode the message
    global message_count

    try:
        data_str = message.data.decode("utf-8")
        json_obj = json.loads(data_str)

        with lock:
            message_count += 1

     #   print("Received message:")
     #   print(json.dumps(json_obj, indent=2))
        message.ack() #Acknowledge the message

    except Exception as e:
        print(f"Problem processing message: {e}")
        message.nack()


streaming_pull_future = subscriber.subscribe(subscription_path, callback=callback)
print(f"Listening for messages on {subscription_path}...\n")

# Wrap subscriber in a 'with' block to automatically call close() when done.
with subscriber:
    try:
        # When `timeout` is not set, result() will block indefinitely,
        # unless an exception is encountered first.
        streaming_pull_future.result(timeout=timeout)
    except TimeoutError:
        streaming_pull_future.cancel() #Trigger the shutdown
        streaming_pull_future.result() #Block until the shut down is complete
print(f"Total messages: {message_count}")