# app.py

import streamlit as st
import pandas as pd
import boto3
import os
import io
import json
from collections import Counter
from urllib.parse import unquote
import plotly.express as px
import redis
import time

# === ADDED HOT PATH CONFIG & FUNCTION ===
REDIS_HOST = os.getenv('REDIS_HOST', 'redis') 

try:
    redis_client = redis.Redis(host=REDIS_HOST, port=6379, db=0, decode_responses=True)
    redis_client.ping()
    REDIS_AVAILABLE = True
except redis.exceptions.ConnectionError:
    REDIS_AVAILABLE = False

# This function only gets data for the hot path.
@st.cache_data(ttl=5)
def get_live_reddit_counts():
    if not REDIS_AVAILABLE:
        return {}
    counts = redis_client.hgetall('disorder_counts')
    return {disorder: int(count) for disorder, count in counts.items()}
# ========================================================

# =========================================================
# === S3 Config ===
BUCKET = "bdm.exploitation.zone"
CSV_PREFIX = "by_disease/csv/"
JSON_PREFIX = "by_disease/json/"
NII_PREFIX = "by_disease/nii/json/"
EEG_PREFIX = "by_disease/eeg/json/"

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
    "mental health": ["mental health"],
    "parkinson":["parkinson"],
    "alzheimer": ["alzheimer"]
}

# --- Helper Functions ---------
# All your S3 data loading functions are completely unchanged.
def list_disorders(prefix):
    s3 = boto3.client("s3",
        aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
        aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY")
    )
    result = s3.list_objects_v2(Bucket=BUCKET, Prefix=prefix)
    disorders = set()
    for obj in result.get("Contents", []):
        parts = obj["Key"].split("/")
        for part in parts:
            if part.startswith("disorder="):
                val = part.split("=", 1)[1]
                if "__HIVE_DEFAULT_PARTITION__" not in val:
                    disorders.add(unquote(val))
    return list(disorders)


from urllib.parse import unquote

def list_disorders_from_prefix(prefix, partition_key="disorder="):
    s3 = boto3.client(
        "s3",
        aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
        aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY")
    )

    result = s3.list_objects_v2(Bucket=BUCKET, Prefix=prefix)
    disorders = set()

    for obj in result.get("Contents", []):
        key = obj["Key"]
        for part in key.split("/"):
            if part.startswith(partition_key):
                val = part.split("=", 1)[1]
                if "__HIVE_DEFAULT_PARTITION__" not in val:
                    disorders.add(unquote(val))
    return list(disorders)




def normalize_disorders(disorder_list):
    normalized = set()
    for canon, variants in DISORDER_CANONICAL_NAMES.items():
        for d in disorder_list:
            if d.lower() in [v.lower() for v in variants]:
                normalized.add(canon)
    return sorted(normalized)

def get_raw_disorder_names(canonical_disorder):
    return DISORDER_CANONICAL_NAMES.get(canonical_disorder, [])

def load_csv_data(disorder_raw_names):
    s3 = boto3.client("s3",
        aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
        aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY")
    )
    dfs = []
    for raw in disorder_raw_names:
        prefix = f"{CSV_PREFIX}disorder={raw}/"
        result = s3.list_objects_v2(Bucket=BUCKET, Prefix=prefix)
        for obj in result.get("Contents", []):
            key = obj["Key"]
            if key.endswith(".csv"):
                response = s3.get_object(Bucket=BUCKET, Key=key)
                df = pd.read_csv(response['Body'])
                dfs.append(df)
    return pd.concat(dfs, ignore_index=True) if dfs else pd.DataFrame()

def load_json_data(disorder_raw_names):
    s3 = boto3.client("s3",
        aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
        aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY")
    )

    articles = []

    for raw in disorder_raw_names:
        prefix = f"{JSON_PREFIX}disorder={raw}/"
        result = s3.list_objects_v2(Bucket=BUCKET, Prefix=prefix)

        if "Contents" not in result:
            continue

        for obj in result["Contents"]:
            key = obj["Key"]
            if not key.endswith(".json"):
                continue

            response = s3.get_object(Bucket=BUCKET, Key=key)
            content = response["Body"].read().decode("utf-8")

            for line in content.strip().split("\n"):
                try:
                    article = json.loads(line)
                    articles.append(article)
                except json.JSONDecodeError:
                    continue

    return articles

import boto3
import os
import json

def load_unstr_metadata(disorder_raw_names):
    s3 = boto3.client(
        "s3",
        aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
        aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY")
    )

    datasets = []

    def load_metadata_from_prefix(prefix):
        result = s3.list_objects_v2(Bucket=BUCKET, Prefix=prefix)
        if "Contents" not in result:
            return []

        records = []
        for obj in result["Contents"]:
            key = obj["Key"]
            if not key.endswith(".json"):
                continue

            response = s3.get_object(Bucket=BUCKET, Key=key)
            content = response["Body"].read().decode("utf-8")

            try:
                for line in content.strip().split("\n"):
                    record = json.loads(line)
                    records.append(record)
            except json.JSONDecodeError:
                continue
        return records

    for raw in disorder_raw_names:
        nii_prefix = f"{NII_PREFIX}disorder={raw}/"
        eeg_prefix = f"{EEG_PREFIX}disorder={raw}/"

        nii_records = load_metadata_from_prefix(nii_prefix)
        eeg_records = load_metadata_from_prefix(eeg_prefix)

        datasets.extend(nii_records)
        datasets.extend(eeg_records)

    return datasets



# --- Streamlit App ---
st.set_page_config(page_title="Mental Health Dashboard", layout="wide")
st.title("Mental Health Dashboard: Trends & Literature")


# === NEW CODE BLOCK 3: LIVE MONITOR UI ===
# This is added before your original UI logic to display the live chart at the top.
# It uses a placeholder so it can be updated without affecting the rest of the page.
st.header("Live Reddit Discussion Monitor")

if not REDIS_AVAILABLE:
    st.error("🔴 Live monitor is offline: Could not connect to the real-time data store.")
else:
    live_data_placeholder = st.empty()

st.markdown("---") # Visual separator
# =======================================================


# =========================================================
# ORIGINAL UI CODE
# =========================================================
# Fetch disorders from both folders
csv_disorders_raw = list_disorders(CSV_PREFIX)
json_disorders_raw = list_disorders(JSON_PREFIX)
nii_disorders_raw = list_disorders_from_prefix(NII_PREFIX)
eeg_disorders_raw = list_disorders_from_prefix(EEG_PREFIX)

# Combine all disorders and normalize
all_disorders = normalize_disorders(csv_disorders_raw + json_disorders_raw + nii_disorders_raw + eeg_disorders_raw)

selected_disorder = st.selectbox("Select a Disorder", all_disorders)

if selected_disorder:
    raw_names = get_raw_disorder_names(selected_disorder)

    # Real-World Data
    csv_df = load_csv_data(raw_names)
    if not csv_df.empty:
        st.subheader("🧠 Real World Statistics")
        countries = sorted(csv_df["Entity"].unique().tolist())
        selected_country = st.selectbox("Select a Country", countries, index=0)

        filtered_df = csv_df[csv_df["Entity"] == selected_country]

        fig = px.line(
            filtered_df,
            x="Year",
            y="percentage",
            color="Entity",
            markers=True,
            title=f"{selected_disorder.title()} in {selected_country} Over Time",
            labels={"PercentageAffected": "% of Population"}
        )
        st.plotly_chart(fig, use_container_width=True, key="historical_country_chart")
       
    else:
        st.info("No real-world statistics available for this disorder.")

    # Literature Data
    json_articles = load_json_data(raw_names)
    years = [article.get("year") for article in json_articles if "year" in article and article["year"] is not None]
    if json_articles:
        st.subheader("📚 Article Mentions")
        st.write(f"Found {len(json_articles)} articles mentioning *{selected_disorder}*.")
        st.write("### Latest Articles:")

        for article in json_articles[:5]:  # Show only the latest 5 articles
            title = article.get("title", "No Title").title()
            year = article.get("year", "n.d.")  # n.d. = no date

            st.markdown(f"- **{title}**  \t ({year})")

        # Create and display a bar chart of article counts per year
        df_years = pd.DataFrame(years, columns=["year"])
        year_counts = df_years.value_counts().reset_index(name="count").sort_values(by="year")
        year_counts.columns = ["year", "count"]

        fig = px.bar(year_counts, x="year", y="count", title="Number of Articles per Year")
        st.plotly_chart(fig, key="historical_literature_chart")
    else:
        st.info("No article data available for this disorder.")
    
    # NIfTI Dataset Metadata Section
    studies_datasets = load_unstr_metadata(raw_names)

    if studies_datasets:
        st.subheader("📦 Dataset Metadata")
        st.write(f"Found **{len(studies_datasets)} datasets** tagged with *{selected_disorder}*.")

        metadata_df = pd.DataFrame([
            {
                "Dataset ID": ds.get("dataset_id"),
                "Name": ds.get("name"),
                "Species": ds.get("species"),
                "Modality": ", ".join(ds.get("modalities", [])),
                "Study Design": ds.get("studyDesign"),
                "Study Domain": ds.get("studyDomain"),
                "Date": ds.get("date", "")[:10]
            }
            for ds in studies_datasets
        ])

        # Let user select a dataset row by index
        dataset_names = ["None selected"] + metadata_df["Name"].tolist()

        selected_name = st.radio("🔍 Select a dataset to view MRI image", dataset_names)

        # Show table always
        st.dataframe(metadata_df, use_container_width=True)

        # Only show image if an actual dataset is selected
        if selected_name != "None selected":
            st.image(
                "https://upload.wikimedia.org/wikipedia/commons/5/58/Head_mri_animation.gif",
                caption=f"Example MRI Image for: {selected_name}",
                use_container_width=True
            )
    else:
        st.info("No NIfTI dataset metadata available for this disorder.")



# === NEW CODE BLOCK 4: THE LIVE REFRESH LOOP ===
# This is added to the very end of the script. It powers the live monitor
# without affecting the historical section above.
if REDIS_AVAILABLE:
    while True:
        live_counts = get_live_reddit_counts()
        with live_data_placeholder.container():
            if not live_counts:
                st.info("Listening to the Reddit stream... Waiting for the first mentions.")
            else:
                df_live = pd.DataFrame(list(live_counts.items()), columns=['Disorder', 'Count'])
                df_live = df_live.sort_values('Count', ascending=False)
                
                fig_live = px.bar(
                    df_live,
                    x='Disorder',
                    y='Count',
                    title="Current Mention Counts",
                    color='Disorder'
                )
                st.plotly_chart(fig_live, use_container_width=True, key="live_chart")
        time.sleep(5) # The refresh interval
# =======================================================