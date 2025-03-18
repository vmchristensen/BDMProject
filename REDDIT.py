import praw
from kafka import KafkaProducer
#import requests
#import requests.auth
import json
import time

#client_auth = requests.auth.HTTPBasicAuth('Jk5dQq2zU2Z87e3iWJVOGQ', 'gko_LXELoV07ZBNUXrvWZfzE3aI')

# Reddit API credentials
reddit = praw.Reddit(
    client_id="Jk5dQq2zU2Z87e3iWJVOGQ",
    client_secret="2NW8kCLNd3KuhhAFrdhEcEsu8WFwvw",
    password = "LCarrera",
    user_agent="mental_health by u/Maleficent-Layer-112",
    username = "Maleficent-Layer-112",
)

print(reddit.read_only)

reddit.read_only = True

# Kafka producer setup
# producer = KafkaProducer(
#     bootstrap_servers="localhost:9092",
#     value_serializer=lambda v: json.dumps(v).encode("utf-8")
# )

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
        #producer.send("reddit_posts", value=post)
        print(post)
        print(f"Sent post: {post['title']}")
        time.sleep(1)  # Prevents overwhelming the API

if __name__ == "__main__":
    stream_reddit("mentalhealth")
