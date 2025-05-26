from google.cloud import pubsub_v1 # Google cloud client library for Pub/Sub
import os
import json
import datetime
import signal
import sys
import psycopg2
import io
from validate_stop import validate_stop
from stop_subscriber_module import receiver

def main():
    GCP_PROJECT_ID = "data-engineering-2025-456119"
    SUBSCRIPTION_ID = "trimet-stop-sub"
    
    receive = receiver(GCP_PROJECT_ID, SUBSCRIPTION_ID)
    receive.start_receiving()


if __name__ == "__main__":
    main()
