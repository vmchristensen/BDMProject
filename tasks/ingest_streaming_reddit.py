import praw
from kafka import KafkaProducer, KafkaConsumer
import json
import time
from datetime import datetime
import os
import boto3
import threading

# --- Securely load credentials from environment variables ---
REDDIT_CLIENT_ID = os.getenv('REDDIT_CLIENT_ID')
REDDIT_CLIENT_SECRET = os.getenv('REDDIT_CLIENT_SECRET')
REDDIT_PASSWORD = os.getenv('REDDIT_PASSWORD')
REDDIT_USER_AGENT = os.getenv('REDDIT_USER_AGENT')
REDDIT_USERNAME = os.getenv('REDDIT_USERNAME')

# AWS S3 setup (boto3 will automatically use AWS env vars if set)
s3 = boto3.client('s3')
bucket_name = 'bdm.project.input'  
s3_prefix = 'data/json/reddit/'

# Reddit API setup
reddit = praw.Reddit(
    client_id=REDDIT_CLIENT_ID,
    client_secret=REDDIT_CLIENT_SECRET,
    password=REDDIT_PASSWORD,
    user_agent=REDDIT_USER_AGENT,
    username=REDDIT_USERNAME,
)
reddit.read_only = True
print("Ingest Service: Reddit read-only:", reddit.read_only)

# Kafka producer setup with retry logic
producer = None
while producer is None:
    try:
        producer = KafkaProducer(
            bootstrap_servers="kafka:9093",
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            retries=5,
            # A short timeout is good for the retry loop
            api_version_auto_timeout_ms=10000, 
            request_timeout_ms=10000
        )
        print("Ingest Service: Successfully connected to Kafka.")
    except Exception as e:
        print(f"Ingest Service: Waiting for Kafka to be available... ({e})")
        time.sleep(5)

# Kafka consumer setup for S3 upload (Cold Path)
consumer = KafkaConsumer(
    'reddit-stream',
    bootstrap_servers='kafka:9093',
    value_deserializer=lambda m: json.loads(m.decode('utf-8')),
    auto_offset_reset='earliest',
    group_id='s3-uploader-group'
)

def stream_reddit(subreddit_name):
    subreddit = reddit.subreddit(subreddit_name)
    print(f"Ingest Service: Starting to stream from r/{subreddit_name}...")
    for submission in subreddit.stream.submissions(skip_existing=True):
        post = {
            "id": submission.id,
            "title": submission.title,
            "selftext": submission.selftext,
            "created_utc": submission.created_utc,
            "author": str(submission.author),
            "url": submission.url
        }
        producer.send("reddit-stream", value=post)
        print(f"Ingest Service: Sent post to Kafka -> {post['title'][:30]}...")
        time.sleep(0.5)

def consume_and_upload():
    print("S3 Uploader: Waiting for messages...")
    for msg in consumer:
        post = msg.value
        timestamp = datetime.utcnow().strftime('%Y%m%d_%H%M%S_%f')
        filename = f"reddit_{post['id']}_{timestamp}.json"
        s3_key = s3_prefix + filename
        try:
            s3.put_object(
                Bucket=bucket_name,
                Key=s3_key,
                Body=json.dumps(post, ensure_ascii=False, indent=2)
            )
            print(f"S3 Uploader: Uploaded to S3 -> s3://{bucket_name}/{s3_key}")
        except Exception as e:
            print(f"S3 Uploader: Error uploading to S3: {e}")

if __name__ == "__main__":
    s3_thread = threading.Thread(target=consume_and_upload, daemon=True)
    s3_thread.start()
    stream_reddit("mentalhealth")