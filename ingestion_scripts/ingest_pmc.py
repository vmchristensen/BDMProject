import requests
import tarfile
import xmltodict
from io import BytesIO
from ftplib import FTP
import json
from datetime import datetime
import boto3
import schedule
import time

# ---- Config ----
MENTAL_HEALTH_DISORDERS = {
    "anxiety"}
MAX_ARTICLES_PER_KEYWORD = 1
UPLOAD_TO_S3 = True

S3_BUCKET = "bdm.project.input"
S3_PREFIX = "data/json/pmc/"
S3_SEEN_FILE = S3_PREFIX + "seen_pmc_ids.json"
s3 = boto3.client("s3")

def fetch_articles(keyword, max_results=MAX_ARTICLES_PER_KEYWORD):
    url = f"https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esearch.fcgi?db=pmc&term={keyword.replace(' ', '+')}&retmax={max_results}&retmode=json"
    ids = requests.get(url).json().get("esearchresult", {}).get("idlist", [])
    links = []
    for pmc_id in ids:
        pmc_id = str(pmc_id)
        if pmc_id in seen_ids or pmc_id in fetched_ids:
            print(f"Duplicate PMC_ID {pmc_id} skipped")
            continue
        oa_url = f"https://www.ncbi.nlm.nih.gov/pmc/utils/oa/oa.fcgi?id=PMC{pmc_id}"
        text = requests.get(oa_url).text
        if 'ftp://' in text:
            start = text.find('ftp://')
            end = text.find('.tar.gz', start) + 7
            links.append({"PMC_ID": pmc_id, "URL": text[start:end]})
            fetched_ids.add(pmc_id)
    return links

def download_tarfile(url):
    try:
        path = url.replace("ftp://ftp.ncbi.nlm.nih.gov/", "")
        ftp = FTP("ftp.ncbi.nlm.nih.gov")
        ftp.login()
        buffer = BytesIO()
        ftp.retrbinary(f"RETR {path}", buffer.write)
        ftp.quit()
        buffer.seek(0)
        with tarfile.open(fileobj=buffer, mode="r:gz") as tar:
            tar.getmembers()
        buffer.seek(0)
        return buffer
    except Exception as e:
        print(f"[ERROR] Failed to download/open tar.gz: {e}")
        return None

def extract_nxml(tar_bytes):
    with tarfile.open(fileobj=tar_bytes, mode="r:gz") as tar:
        for member in tar.getmembers():
            if member.name.endswith(".nxml"):
                return tar.extractfile(member).read().decode("utf-8")
    return None

def parse_nxml(nxml_text):
    try:
        article = xmltodict.parse(nxml_text)
        meta = article.get("article", {}).get("front", {}).get("article-meta", {})
        title = meta.get("title-group", {}).get("article-title", "No Title")
        abstract = meta.get("abstract", {}).get("p", "No Abstract")
        body = article.get("article", {}).get("body", {})
        body_text = str(body)
        pub_date = meta.get("pub-date", {})
        if isinstance(pub_date, list):
            year = pub_date[0].get("year", "No Date")
        else:
            year = pub_date.get("year", "No Date")
        return title, abstract, body_text, year
    except:
        return None, None, None, None

def detect_disorders(text):
    return [d for d in MENTAL_HEALTH_DISORDERS if d in text.lower()]

def ingest_data_from_pmc():
    global seen_ids, fetched_ids

    # Reload seen_ids at runtime
    try:
        seen_ids_obj = s3.get_object(Bucket=S3_BUCKET, Key=S3_SEEN_FILE)
        seen_ids = set(json.loads(seen_ids_obj['Body'].read().decode('utf-8')))
    except s3.exceptions.NoSuchKey:
        seen_ids = set()

    fetched_ids = set()
    new_ids = []

    for keyword in MENTAL_HEALTH_DISORDERS:
        print(f"Searching for: {keyword}")
        articles = fetch_articles(keyword)
        for article in articles:
            tar_data = download_tarfile(article["URL"])
            if not tar_data:
                continue
            nxml = extract_nxml(tar_data)
            if not nxml:
                continue
            title, abstract, body, year = parse_nxml(nxml)
            if not title:
                continue
            full_text = f"{title} {abstract} {body} {year}"
            found_disorders = detect_disorders(full_text)

            paper_data = {
                "PMC_ID": article["PMC_ID"],
                "Title": title,
                "Abstract": abstract,
                "Disorders Found": found_disorders,
                "Year": year
            }

            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"pmc_{article['PMC_ID']}_{timestamp}.json"

            s3.put_object(
                Bucket=S3_BUCKET,
                Key=S3_PREFIX + filename,
                Body=json.dumps(paper_data, indent=2).encode("utf-8")
            )
            print(f"Uploaded to S3: s3://{S3_BUCKET}/{S3_PREFIX + filename}")
            new_ids.append(article["PMC_ID"])

    # Save updated seen PMC IDs to S3
    seen_ids.update(new_ids)
    s3.put_object(
        Bucket=S3_BUCKET,
        Key=S3_SEEN_FILE,
        Body=json.dumps(list(seen_ids)).encode("utf-8")
    )
    print(f"Uploaded seen PMC IDs to S3: s3://{S3_BUCKET}/{S3_SEEN_FILE}")

