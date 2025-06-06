import praw
from kafka import KafkaProducer, KafkaConsumer
import json
import time
from datetime import datetime
import os
import boto3
import threading

# Toggle to save files locally as well
SAVE_LOCALLY = False

# AWS S3 setup
s3 = boto3.client('s3')
bucket_name = 'bdm.project.input'  
s3_prefix = 'data/json/reddit/'

# Local folder for saving (optional)
local_folder = 'data/json'
if SAVE_LOCALLY:
    os.makedirs(local_folder, exist_ok=True)

# Reddit API setup
reddit = praw.Reddit(
    client_id="Jk5dQq2zU2Z87e3iWJVOGQ",
    client_secret="2NW8kCLNd3KuhhAFrdhEcEsu8WFwvw",
    password="LCarrera",
    user_agent="mental_health by u/Maleficent-Layer-112",
    username="Maleficent-Layer-112",
)
reddit.read_only = True
print("Reddit read-only:", reddit.read_only)

# Kafka producer setup
producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

# Kafka consumer setup
consumer = KafkaConsumer(
    'reddit-stream',
    bootstrap_servers='localhost:9092',
    value_deserializer=lambda m: json.loads(m.decode('utf-8')),
    auto_offset_reset='earliest',
    group_id='reddit-group'
)

# Function to stream Reddit posts (Producer)
def stream_reddit(subreddit_name):
    subreddit = reddit.subreddit(subreddit_name)
    for submission in subreddit.stream.submissions():
        post = {
            "id": submission.id,
            "title": submission.title,
            "selftext": submission.selftext,
            "created_utc": submission.created_utc,
            "author": str(submission.author),
            "url": submission.url
        }
        producer.send("reddit-stream", value=post)
        timestamp = datetime.utcfromtimestamp(post["created_utc"]).strftime('%Y-%m-%d %H:%M:%S')
        print(f"[{timestamp}] Sent post: {post['title']}")
        time.sleep(1)

# Function to consume and upload posts to S3 (Consumer)
def consume_and_upload():
    for msg in consumer:
        post = msg.value
        timestamp = datetime.utcnow().strftime('%Y%m%d_%H%M%S')
        filename = f"reddit_{post['id']}_{timestamp}.json"
        s3_key = s3_prefix + filename

        # Optional local save
        if SAVE_LOCALLY:
            local_path = os.path.join(local_folder, filename)
            with open(local_path, 'w', encoding='utf-8') as f:
                json.dump(post, f, ensure_ascii=False, indent=2)
            print(f"Saved locally: {local_path}")

        # Upload to S3
        s3.put_object(
            Bucket=bucket_name,
            Key=s3_key,
            Body=json.dumps(post, ensure_ascii=False, indent=2)
        )
        print(f"Uploaded to S3: s3://{bucket_name}/{s3_key}")

# Run both producer + consumer using threading
if __name__ == "__main__":
    threading.Thread(target=consume_and_upload, daemon=True).start()
    stream_reddit("mentalhealth")



