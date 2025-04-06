# BDM Project 1: Mental Health Data Ingestion Pipeline

## Overview

This project automates the ingestion of mental health-related data from multiple sources, including structured surveys, semi-structured Reddit posts, and unstructured neuroimaging data. It leverages AWS S3 for storage and Kafka for real-time streaming.

## How to Run

### 1. Set up Environment

Install dependencies:

```bash
pip install boto3 requests praw kafka-python
```


Set AWS credentials for boto3.


### 2. Launch Reddit Streaming Pipeline

Start Kafka and Docker:

```bash
docker-compose up -d
docker exec -it kafka bash
```
Create Kafka topic:

```bash
kafka-topics.sh --create --topic reddit-stream --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1
exit
```

Confirm it exists:
```bash
kafka-topics.sh --list --bootstrap-server localhost:9092
```

Run the Reddit streaming ingestion script:
```bash
python ingestion_streaming_reddit.py
```


### 3. Run Batch Ingestion Scripts

You can run the ingestion scripts either individually or by using the main script that triggers a schedule for specific tasks.

#### Option 1: Run Individual Scripts

Run the following scripts directly to ingest data from each source:

```bash
python ingest_semantic_scholar.py
python ingest_pmc.py
python ingest_neuro_data.py
python ingest_mental_health.py
python ingest_unemployment_and_suicides.py
```

#### Option 2: Run Main Script

Alternatively, you can use the main script main.py, which triggers the batch ingestion for a scheduled set of scripts, specifically:
```bash
python main.py
This will automatically execute the batch ingestion for the following data sources:
ingest_semantic_scholar.py
ingest_pmc.py
ingest_neuro_data.py
```


#### Supporting Functions

Please note that the ingestion and main scripts depend on additional utility scripts containing necessary functions. These scripts are the following:
```bash
ingest_data.py
automatic_schedule.py
```
