import json
import csv
import os
from google.cloud import pubsub_v1
import glob     

# Set the environment variable for the service account key
files=glob.glob("*.json")
os.environ["GOOGLE_APPLICATION_CREDENTIALS"]=files[0];

# Define GCP project and Pub/Sub topic
project_id = "sincere-actor-448722-g4";
topic_id = "LabelsTopic"

# Initialize the Pub/Sub publisher
publisher = pubsub_v1.PublisherClient()
topic_path = publisher.topic_path(project_id, topic_id)

def read_csv_and_publish(csv_file):
    with open(csv_file, mode="r") as file:
        reader = csv.DictReader(file)
        for row in reader:
            # Convert row to dictionary and serialize it
            message = json.dumps(row).encode("utf-8")
            # Publish the message to the topic
            future = publisher.publish(topic_path, message)
            print(f"Published message ID: {future.result()}")

if __name__ == "__main__":
    csv_file = "Labels.csv"
    read_csv_and_publish(csv_file)
