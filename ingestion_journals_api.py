import requests
import json
import logging
import tarfile
import os
import xmltodict
from collections import Counter
import yake
import boto3
import pandas as pd
from io import BytesIO, StringIO
from ftplib import FTP

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# AWS S3 Configuration
S3_BUCKET_NAME = "bdm.project.input"  # Change this
S3_FOLDER = "data/"  # S3 folder path

# Initialize S3 client
s3_client = boto3.client("s3")

# Define relevant mental health disorder keywords
MENTAL_HEALTH_DISORDERS = {
    "anxiety", "depression", "bipolar disorder", "schizophrenia",
    "PTSD", "OCD", "insomnia", "substance abuse", "eating disorder",
    "ADHD", "autism", "phobia", "panic disorder", "psychosis",
    "self-harm", "suicide", "addiction"
}

def fetch_pmc_articles(keyword, max_results=5):
    """
    Searches for articles mentioning the keyword in PMC and fetches their full-text package links.
    """
    search_url = f"https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esearch.fcgi?db=pmc&term={keyword.replace(' ', '+')}&retmax={max_results}&retmode=json"
    search_response = requests.get(search_url).json()
    pmc_ids = search_response.get("esearchresult", {}).get("idlist", [])

    if not pmc_ids:
        logging.warning("❌ No articles found for the given keyword.")
        return []

    logging.info(f"✅ Found {len(pmc_ids)} articles for keyword: {keyword}")
    
    article_links = []
    for pmc_id in pmc_ids:
        oa_url = f"https://www.ncbi.nlm.nih.gov/pmc/utils/oa/oa.fcgi?id=PMC{pmc_id}"
        oa_response = requests.get(oa_url).text

        # Extract the FTP tar.gz download link
        start_idx = oa_response.find('href="ftp://')
        if start_idx != -1:
            start_idx += 6  # Skip 'href="'
            end_idx = oa_response.find('"', start_idx)
            tar_url = oa_response[start_idx:end_idx]
            article_links.append({"PMC_ID": pmc_id, "Full-Text URL": tar_url})

    return article_links

def download_ftp_file(ftp_url):
    """
    Downloads a file from an FTP server and returns it as bytes.
    """
    # Extract FTP server and file path
    ftp_server = "ftp.ncbi.nlm.nih.gov"
    file_path = ftp_url.replace("ftp://ftp.ncbi.nlm.nih.gov/", "")

    logging.info(f"📥 Connecting to FTP: {ftp_server}, File: {file_path}")

    try:
        ftp = FTP(ftp_server)
        ftp.login()
        ftp.set_pasv(True)  # Enable passive mode (Fix for some servers)

        # Check if the file exists
        file_list = ftp.nlst(os.path.dirname(file_path))  # List files in the directory
        if file_path not in file_list:
            logging.warning(f"❌ File not found: {file_path}")
            ftp.quit()
            return None

        # Download the file
        file_bytes = BytesIO()
        ftp.retrbinary(f"RETR {file_path}", file_bytes.write)
        ftp.quit()
        
        file_bytes.seek(0)  # Reset pointer for reading
        logging.info(f"✅ Successfully downloaded {file_path}")
        return file_bytes

    except Exception as e:
        logging.error(f"❌ FTP download failed: {e}")
        return None


def extract_nxml_from_tar(tar_bytes):
    """
    Extracts the first `.nxml` file from a `.tar.gz` archive.
    """
    with tarfile.open(fileobj=tar_bytes, mode="r:gz") as tar:
        for member in tar.getmembers():
            if member.name.endswith(".nxml"):
                extracted_file = tar.extractfile(member)
                if extracted_file:
                    logging.info(f"✅ Extracted .nxml file: {member.name}")
                    return extracted_file.read().decode("utf-8")
    return None

def parse_nxml(nxml_content):
    """
    Parses an NXML file to extract the article's title, abstract, and body text.
    """
    try:
        article_dict = xmltodict.parse(nxml_content)
        article_meta = article_dict.get("article", {}).get("front", {}).get("article-meta", {})
        title = article_meta.get("title-group", {}).get("article-title", "No Title")
        abstract = article_meta.get("abstract", {}).get("p", "No Abstract")
        body = article_dict.get("article", {}).get("body", {}).get("sec", "No Body")

        if isinstance(abstract, dict):  # If abstract has nested structure
            abstract = abstract.get("#text", "No Abstract")

        return {
            "Title": title,
            "Abstract": abstract,
            "Body": str(body)  # Convert XML tree to string
        }
    except Exception as e:
        logging.error(f"❌ Error parsing NXML file: {e}")
        return None

def extract_disorder_mentions(text):
    """
    Extracts mental health disorder mentions from text using YAKE.
    """
    disorder_count = Counter()
    
    # Extract keywords
    keyword_extractor = yake.KeywordExtractor(n=3, top=10)
    keywords = keyword_extractor.extract_keywords(text)

    # Check for disorder mentions
    for disorder in MENTAL_HEALTH_DISORDERS:
        if disorder in text.lower() or any(disorder in kw[0].lower() for kw in keywords):
            disorder_count[disorder] += 1
    
    return disorder_count

def save_to_s3(data, filename, content_type="application/json"):
    """
    Uploads data to S3 as a JSON or CSV file, storing them in separate folders inside 'data/'.
    """
    # Define separate folders for JSON and CSV
    if content_type == "application/json":
        s3_key = f"{S3_FOLDER}json/{filename}"  # JSON files go to 'data/json/'
    else:  # CSV
        s3_key = f"{S3_FOLDER}csv/{filename}"  # CSV files go to 'data/csv/'

    # Convert the data to the correct format
    if content_type == "application/json":
        file_content = json.dumps(data, indent=4)
    else:  # CSV
        file_content = data.to_csv(index=False)

    # Upload to S3
    s3_client.put_object(
        Bucket=S3_BUCKET_NAME,
        Key=s3_key,
        Body=file_content,
        ContentType=content_type
    )
    print(f"✅ Uploaded to S3: s3://{S3_BUCKET_NAME}/{s3_key}")


if __name__ == "__main__":
    keyword = "psychology"
    logging.info(f"🔍 Searching for articles on: {keyword}")
    
    articles = fetch_pmc_articles(keyword)

    if not articles:
        logging.warning("❌ No articles found.")
    else:
        disorder_counts = Counter()
        paper_data = []

        for article in articles:
            # Download from FTP
            tar_bytes = download_ftp_file(article["Full-Text URL"])
            if tar_bytes:
                nxml_content = extract_nxml_from_tar(tar_bytes)
                if nxml_content:
                    parsed_article = parse_nxml(nxml_content)
                    if parsed_article:
                        title, abstract, body = parsed_article["Title"], parsed_article["Abstract"], parsed_article["Body"]
                        
                        disorder_mentions = extract_disorder_mentions(title + " " + abstract + " " + body)
                        disorder_counts.update(disorder_mentions)

                        paper_data.append({"Title": title, "Abstract": abstract, "Body": body, "Disorders Found": list(disorder_mentions.keys())})

        # Convert disorder counts to JSON
        disorder_counts_json = dict(disorder_counts)

        # Convert paper data to DataFrame for CSV
        paper_df = pd.DataFrame(paper_data)

        # Save disorder count JSON to S3
        save_to_s3(disorder_counts_json, "disorder_counts.json", "application/json")

        # Save paper data CSV to S3
        save_to_s3(paper_df, "mental_health_papers.csv", "text/csv")

        # Print disorder mentions
        logging.info("\n🔹 Most Common Mental Health Disorders Mentioned:")
        for disorder, count in disorder_counts.most_common(10):
            logging.info(f"{disorder}: {count} occurrences")
