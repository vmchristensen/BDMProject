'''
# save locally
import requests

# Define the raw file URL
url = "https://gist.github.com/mosesvemana/846f084f28e759bc6b522bdd269a74a8/raw/Mental_Health.csv"

# Fetch data from the URL
response = requests.get(url)

# Save to a local CSV file
with open("mental_health.csv", "wb") as file:
    file.write(response.content)

print("Data successfully downloaded and saved as 'mental_health.csv'")
'''



# save directly to aws s3
import requests
import boto3

# AWS S3 Configuration
S3_BUCKET_NAME = "bdm.project.input"  # Change this to your actual S3 bucket name
S3_KEY = "data/mental_health.csv"  # Path inside S3 (e.g., "data/mental_health.csv")

# Define the raw file URL
url = "https://gist.github.com/mosesvemana/846f084f28e759bc6b522bdd269a74a8/raw/Mental_Health.csv"

# Fetch data from the URL (CSV file)
response = requests.get(url)

# Check if request was successful
if response.status_code == 200:
    # Initialize S3 client
    s3_client = boto3.client("s3")

    # Upload directly from memory to S3
    s3_client.put_object(Bucket=S3_BUCKET_NAME, Key=S3_KEY, Body=response.content, ContentType="text/csv")

    print(f"File uploaded successfully to s3://{S3_BUCKET_NAME}/{S3_KEY}")
else:
    print(f"Failed to download CSV file. Status code: {response.status_code}")

