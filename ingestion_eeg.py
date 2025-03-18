import requests
import boto3
from io import BytesIO

# AWS S3 Configuration
S3_BUCKET_NAME = "bdm.project.input"  # Change to your actual S3 bucket name
S3_FOLDER = "data/eeg/"  # Folder in S3 where EEG files will be stored

# GitHub Repository Details
repo_owner = "OpenNeuroDatasets"
repo_name = "ds004840"
branch = "main"

# GitHub API URL to fetch all files in the repository
api_url = f"https://api.github.com/repos/{repo_owner}/{repo_name}/git/trees/{branch}?recursive=1"

# Initialize S3 client
s3_client = boto3.client("s3")

def fetch_eeg_file_list():
    """
    Fetches a list of .edf EEG files from the GitHub repository.
    """
    response = requests.get(api_url)

    if response.status_code == 200:
        files = response.json()["tree"]
        eeg_files = [file["path"] for file in files if file["path"].endswith(".edf")]
        print(f"Found {len(eeg_files)} EEG (.edf) files.")
        return eeg_files
    else:
        print(f"Failed to fetch EEG file list. Status Code: {response.status_code}")
        return []

def upload_to_s3(file_url, file_name):
    """
    Streams an EEG .edf file from GitHub and uploads it directly to S3.
    """
    response = requests.get(file_url, stream=True)

    if response.status_code == 200:
        file_data = BytesIO(response.content)  # Convert response content into a seekable bytes object
        s3_key = f"{S3_FOLDER}{file_name}"

        s3_client.put_object(
            Bucket=S3_BUCKET_NAME,
            Key=s3_key,
            Body=file_data.getvalue(),
            ContentType="application/octet-stream"
        )
        print(f"Uploaded: s3://{S3_BUCKET_NAME}/{s3_key}")
    else:
        print(f"Failed to download: {file_url}")

# Fetch list of EEG files
eeg_files = fetch_eeg_file_list()

if eeg_files:
    base_url = f"https://raw.githubusercontent.com/{repo_owner}/{repo_name}/{branch}/"

    # Upload each EEG file directly to S3
    for file_path in eeg_files:
        file_url = base_url + file_path
        file_name = file_path.split("/")[-1]  # Extract filename
        upload_to_s3(file_url, file_name)
else:
    print("No EEG files found to upload.")
