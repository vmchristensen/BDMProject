import subprocess
import zipfile
import tempfile
import os
import boto3

# Initialize S3 client
s3_client = boto3.client("s3")

# Specify your S3 bucket and path
S3_BUCKET_NAME = "bdm.project.input"
S3_KEY = "data/csv/osmi_2023_survey_results.csv"

# Create a temporary directory to hold the ZIP file
with tempfile.TemporaryDirectory() as tmp_dir:
    # Run Kaggle CLI to download the dataset into the temporary folder
    subprocess.run([
        "kaggle", "datasets", "download",
        "-d", "osmihelp/osmi-mental-health-in-tech-survey-2023",
        "-p", tmp_dir
    ], check=True)

    # Find the ZIP file in the temporary folder
    zip_file_path = os.path.join(tmp_dir, "osmi-mental-health-in-tech-survey-2023.zip")

    # Open the ZIP file and extract CSV data
    with zipfile.ZipFile(zip_file_path, "r") as zip_ref:
        print("Files in ZIP:", zip_ref.namelist())  # Confirmation step

        # Assuming there's only one CSV file in the ZIP
        extracted_csv = [f for f in zip_ref.namelist() if f.endswith(".csv")][0]

        # Read CSV from ZIP archive and stream it directly to S3
        with zip_ref.open(extracted_csv) as file_data:
            s3_client.put_object(
                Bucket=S3_BUCKET_NAME,
                Key=S3_KEY,
                Body=file_data.read(),
                ContentType="text/csv"
            )

print(f"File uploaded successfully to s3://{S3_BUCKET_NAME}/{S3_KEY}")
