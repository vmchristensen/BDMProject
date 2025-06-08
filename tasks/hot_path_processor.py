import json
import os
import redis
from kafka import KafkaConsumer
import time

# === NEW: Use the comprehensive dictionary for keyword matching ===
# This is the same dictionary from your dashboard, ensuring consistency.
DISORDER_CANONICAL_NAMES = {
    "bipolar disorder": ["bipolardisorder", "bipolar disorder", "bipolar%2520disorder"],
    "depression": ["depressivedisorders", "depression"],
    "anxiety": ["anxietydisorders", "anxiety", "panic disorder", "phobia", "ptsd"],
    "schizophrenia": ["schizophrenia"],
    "eating disorder": ["eatingdisorders", "eating disorder", "eating%2520disorder"],
    "substance abuse": ["alcoholusedisorders", "drugusedisorders", "addiction", "substance abuse"],
    "ocd": ["ocd"],
    "autism": ["autism"],
    "insomnia": ["insomnia"],
    "adhd": ["adhd"],
    "suicide": ["suicide", "self-harm"],
    "psychosis": ["psychosis"],
    "mental health": ["mental health"]
}

# We will create one flat list of all possible search terms.
# This makes the search loop very efficient.
ALL_VARIANTS_TO_SEARCH = [variant for sublist in DISORDER_CANONICAL_NAMES.values() for variant in sublist]

# We also need a way to map a found variant back to its official name.
# Example: If we find "bipolardisorder", we need to know that the official name is "bipolar disorder".
VARIANT_TO_CANONICAL_MAP = {
    variant: canonical_name 
    for canonical_name, variants in DISORDER_CANONICAL_NAMES.items() 
    for variant in variants
}
# ===================================================================

# --- Kafka and Redis setup (unchanged) ---
def get_kafka_consumer():
    while True:
        try:
            consumer = KafkaConsumer(
                'reddit-stream',
                bootstrap_servers='kafka:9093',
                value_deserializer=lambda m: json.loads(m.decode('utf-8')),
                auto_offset_reset='latest',
                group_id='hot-path-counter-group'
            )
            print("Hot Path: Successfully connected to Kafka.")
            return consumer
        except Exception as e:
            print(f"Hot Path: Waiting for Kafka... ({e})")
            time.sleep(5)

redis_client = redis.Redis(host='redis', port=6379, db=0, decode_responses=True)

def process_and_count():
    consumer = get_kafka_consumer()
    print("Hot Path: Processor started. Waiting for posts...")
    for msg in consumer:
        post = msg.value
        text_to_search = (post.get('title', '') + ' ' + post.get('selftext', '')).lower()

        # === MODIFIED: Use the new mapping logic for counting ===
        found_canonical_disorders = set()
        
        # Search for all possible variants
        for variant in ALL_VARIANTS_TO_SEARCH:
            if variant in text_to_search:
                # If a variant is found, add its OFFICIAL (canonical) name to the set.
                # This prevents overcounting (e.g., "anxiety" and "ptsd" in one post
                # will both correctly increment the "anxiety" counter just once).
                canonical_name = VARIANT_TO_CANONICAL_MAP[variant]
                found_canonical_disorders.add(canonical_name)

        # Increment the counter in Redis for each unique canonical disorder found.
        if found_canonical_disorders:
            for canonical_name in found_canonical_disorders:
                new_count = redis_client.hincrby('disorder_counts', canonical_name, 1)
                print(f"Hot Path: Updated '{canonical_name}'. New count: {new_count}")
        # ==========================================================

if __name__ == "__main__":
    process_and_count()