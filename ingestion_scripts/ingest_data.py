import requests
import pandas as pd
import json
from io import StringIO
from lxml import etree
import boto3
from botocore.exceptions import BotoCoreError, ClientError
from datetime import datetime
import logging
import os

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler("ingestion.log"),  # Log to a file
        logging.StreamHandler()  # Also log to the console
    ]
)

# Get the logger
logger = logging.getLogger(__name__)

# AWS S3 Configuration
S3_BUCKET_NAME = "bdm.project.input"  # S3 bucket name

# Detecting data types
def detect_data_source(url):
    if url.endswith('.nii.gz'):
        response = requests.get(url)
        return "NII", response.content
    
    eeg_extensions = (".edf", ".bdf", ".set", ".vhdr", ".vmrk", ".eeg", ".mat")
    if url.endswith(eeg_extensions):
        response = requests.get(url)
        return "EEG", response.content 

    # Content-Type detection follows
    response = requests.get(url)
    content_type = response.headers.get('Content-Type', '')

    if 'application/json' in content_type or 'text/json' in content_type:
        return "JSON", response.text
    elif 'application/csv' in content_type or 'text/csv' in content_type:
        return "CSV", response.text
    elif 'application/octet-stream' in content_type:
        if b'EDF' in response.content[:100]: 
            return "EEG", response.content
        elif b'NRRD' in response.content[:100] or b'NiFTI' in response.content[:100]:
            return "NII", response.content
        else:
            return "Unknown Binary", response.content
    elif 'text/plain' in content_type:
        try:
            pd.read_csv(StringIO(response.text))
            return "CSV", response.text
        except Exception:
            return "Plain Text", response.text
    else:
        return "Unknown", response.text
    
# Checking wether dataset already exists in the Cloud
def dataset_exists_in_s3(dataset_id, modality_folder):
    s3_client = boto3.client("s3")
    prefix = f"data/{modality_folder}/{dataset_id}/"

    try:
        response = s3_client.list_objects_v2(Bucket=S3_BUCKET_NAME, Prefix=prefix)
        if "Contents" in response:
            logger.info(f"Dataset {dataset_id} already exists in S3 ({modality_folder}), skipping...")
            return True
        return False
    except (BotoCoreError, ClientError) as e:
        logger.error(f"Error checking dataset {dataset_id} in S3: {e}")
        return False


# Upload data to Cloud
def upload_to_s3(data, source_type, source_name, filename, dataset_id=None):
    """Uploads data to AWS S3 with appropriate content type and structured path."""
    s3_client = boto3.client("s3")

    # Define file extension based on source type
    
    original_extension = os.path.splitext(filename)[1]
    file_extension = {
        "JSON": ".json",
        "CSV": ".csv",
        "XML": ".xml",
        "NII": ".nii.gz",
        "Plain Text": ".txt",
        "Unknown Binary": ".bin",
        "Unknown": ".dat"
    }.get(source_type, original_extension) 

    # Add timestamp to filename (e.g., `csv_mental_health_20250325_153045.csv`)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    full_filename = f"{source_name}_{filename}_{timestamp}{file_extension}"

    # Define subfolder inside the data type directory
    subfolder = f"{dataset_id}/" if dataset_id else ""

    # Define S3 key (structured path)
    s3_key = f"data/{source_type.lower()}/{subfolder}{full_filename}"
    
    try:
        s3_client.put_object(
            Bucket=S3_BUCKET_NAME,
            Key=s3_key,
            Body=data.encode('utf-8') if isinstance(data, str) else data,
            ContentType={
                "NII": "application/gzip",
                "EEG": "application/octet-stream",  
                "CSV": "text/csv",
                "JSON": "application/json",
                "XML": "application/xml",
                "Plain Text": "text/plain"
            }.get(source_type, "application/octet-stream")
        )
        logger.info(f"File uploaded successfully to s3://{S3_BUCKET_NAME}/{s3_key}")
    except (BotoCoreError, ClientError) as e:
        logger.info(f"Error uploading to S3: {e}")

# Ingest files from GitHub
def ingest_files_from_github(repo_owner, repo_name, branch, sourcename, dataset_id=None):
    """
    Fetches a list of all files from a GitHub repository and automatically detects their type
    using `detect_data_source`, then ingests them.
    """
    # Fetch file list from GitHub API
    url = f"https://api.github.com/repos/{repo_owner}/{repo_name}/git/trees/{branch}?recursive=1"
    response = requests.get(url)

    if response.status_code == 200:

        files = response.json()["tree"]

        # Separate the files based on their extensions
        # NII files to include: include only structural data from the subjects
        nii_files = []
        subjects_with_nii = set()

        for file in files:
            if "/anat/" in file["path"] and not file["path"].startswith("derivatives/") and (file["path"].endswith("T1w.nii.gz") or file["path"].endswith("T2w.nii.gz")):
                subject_id = file["path"].split("/")[0]

                if subject_id not in subjects_with_nii:
                    nii_files.append(file["path"])
                    subjects_with_nii.add(subject_id)

        # EEG files to include: consider multiple eeg formats, only imports one file per subject
        eeg_extensions = (".edf", ".bdf", ".set", ".vhdr", ".vmrk", ".eeg", ".mat")
        subjects_with_eeg = set() # inclde only one eeg file per subject

        eeg_files = []
        for file in files:
            if file["path"].lower().endswith(eeg_extensions) and not file["path"].startswith("derivatives/"):
                subject_id = file["path"].split("/")[0]

                if subject_id not in subjects_with_eeg:
                    eeg_files.append(file["path"])
                    subjects_with_eeg.add(subject_id)

        # Base URL for downloading actual file contents
        base_url = f"https://raw.githubusercontent.com/{repo_owner}/{repo_name}/{branch}/"

        # Ingest each .nii.gz (MRI scan) file
        for file_path in nii_files:
            url = base_url + file_path
            filename = file_path.split("/")[-1]  # Extract filename
            try:
                logger.info(f"Ingesting MRI file: {filename}")
                ingest_data(url, sourcename, filename, dataset_id=dataset_id)
            except Exception as e:
                logger.info(f"Error processing MRI file {filename}: {e}")

        # Ingest each .edf (EEG) file
        for file_path in eeg_files:
            url = base_url + file_path
            filename = file_path.split("/")[-1]  # Extract filename
            try:
                logger.info(f"Ingesting EEG file: {filename}")
                ingest_data(url, sourcename, filename, dataset_id=dataset_id)
            except Exception as e:
                logger.info(f"Error processing EEG file {filename}: {e}")

    else:
        logger.info("Failed to fetch file list from GitHub.")


# Upload Metadata to Cloud as JSON files
def upload_metadata_json(metadata_dict, dataset_id, modality_folder):
    """Uploads metadata as a JSON file to the dataset's S3 folder."""
    s3_client = boto3.client("s3")

    json_data = json.dumps(metadata_dict, indent=2)
    s3_key = f"data/{modality_folder}/{dataset_id}/{dataset_id}_metadata.json"

    try:
        s3_client.put_object(
            Bucket=S3_BUCKET_NAME,
            Key=s3_key,
            Body=json_data.encode('utf-8'),
            ContentType="application/json"
        )
        logger.info(f"Metadata uploaded to s3://{S3_BUCKET_NAME}/{s3_key}")
    except (BotoCoreError, ClientError) as e:
        logger.info(f"Error uploading metadata for {dataset_id}: {e}")


# Ingest Data
def ingest_data(url, sourcename, filename, dataset_id=None):
    source_type, data = detect_data_source(url)
    
    if source_type == "Unknown":
        logger.info("Unknown data source type, cannot ingest.")
        return

    upload_to_s3(data, source_type, sourcename, filename, dataset_id=dataset_id)

