import json
import os
from google.cloud import pubsub_v1
import glob     


files=glob.glob("*.json")
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = files[0];

# Define GCP project and Pub/Sub subscription
project_id = "sincere-actor-448722-g4";
subscription_id = "LabelsTopic-sub"

# Initialize the Pub/Sub subscriber
subscriber = pubsub_v1.SubscriberClient()
subscription_path = subscriber.subscription_path(project_id, subscription_id)

def callback(message):
    # Deserialize the message and print the dictionary values
    data = json.loads(message.data.decode("utf-8"))
    print(f"Received message: {data}")
    message.ack()

if __name__ == "__main__":
    print("Listening for messages...")
    future = subscriber.subscribe(subscription_path, callback=callback)
    try:
        future.result()
    except KeyboardInterrupt:
        future.cancel()
