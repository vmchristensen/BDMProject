import requests
import boto3
from io import BytesIO

# AWS S3 Configuration
S3_BUCKET_NAME = "bdm.project.input"  # Change to your actual S3 bucket name
S3_FOLDER = "data/mri/"  # Folder in S3 where scans will be stored

# GitHub repo details
repo_owner = "OpenNeuroDatasets"
repo_name = "ds004937"
branch = "main"

# GitHub API URL to get all files
api_url = f"https://api.github.com/repos/{repo_owner}/{repo_name}/git/trees/{branch}?recursive=1"

# Initialize S3 client
s3_client = boto3.client("s3")

def upload_to_s3(file_url, file_name):
    """
    Streams the MRI scan from the URL, converts it to bytes, and uploads it directly to S3.
    """
    response = requests.get(file_url, stream=True)

    if response.status_code == 200:
        file_data = BytesIO(response.content)  # Convert response content into a seekable bytes object
        s3_key = f"{S3_FOLDER}{file_name}"

        s3_client.put_object(Bucket=S3_BUCKET_NAME, Key=s3_key, Body=file_data.getvalue(), ContentType="application/gzip")
        print(f"✅ Uploaded: s3://{S3_BUCKET_NAME}/{s3_key}")
    else:
        print(f"❌ Failed to download: {file_url}")

# Fetch dataset file list
response = requests.get(api_url)

if response.status_code == 200:
    files = response.json()["tree"]
    nii_files = [file["path"] for file in files if file["path"].endswith(".nii.gz")]

    print(f"Found {len(nii_files)} .nii.gz MRI scans.")

    # Base URL for raw GitHub files
    base_url = f"https://raw.githubusercontent.com/{repo_owner}/{repo_name}/{branch}/"

    # Upload each MRI scan directly to S3
    for file_path in nii_files:
        file_url = base_url + file_path
        file_name = file_path.split("/")[-1]  # Extract filename
        upload_to_s3(file_url, file_name)

else:
    print("❌ Failed to fetch file list from GitHub.")
