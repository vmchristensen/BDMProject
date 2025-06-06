import requests
import logging
import time
import json
from datetime import datetime, timedelta
from ingest_data import upload_to_s3  

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# Define relevant mental health disorder keywords
MENTAL_HEALTH_DISORDERS = {
    "anxiety", "depression", "bipolar disorder", "schizophrenia",
    "PTSD", "OCD", "insomnia", "substance abuse", "eating disorder",
    "ADHD", "autism", "phobia", "panic disorder", "psychosis",
    "self-harm", "suicide", "addiction"
}

# Base URL for Semantic Scholar API
SEMANTIC_SCHOLAR_API_URL = "https://api.semanticscholar.org/graph/v1/paper/search"

# Get the current date and calculate the last week date
current_date = datetime.now()
one_week_ago = current_date - timedelta(weeks=1)

# Define query parameters to filter by publication date (last week or today)
query_params = {
    "fields": "title,publicationDate,authors,abstract,paperId",
    "publicationDateOrYear": f"{one_week_ago.strftime('%Y-%m-%d')}--{current_date.strftime('%Y-%m-%d')}"  # Last week's papers
}

def fetch_papers(query, retries=5, delay=2):
    papers_data = []  # Initialize an empty list to store paper data
    
    for attempt in range(retries):
        try:
            params = {
                "query": query,
                "limit": 10,  # Limit to 10 results per query
                **query_params
            }
            # Send a GET request to the Semantic Scholar API
            response = requests.get(SEMANTIC_SCHOLAR_API_URL, params=params)
            response.raise_for_status()  # Raise an error for bad responses

            # Parse the JSON response
            data = response.json()

            # Check if there are any papers returned
            if "data" in data and data["data"]:
                logging.info(f"Found {len(data['data'])} papers for query '{query}'")
                for paper in data["data"]:
                    title = paper.get("title", "No title available")
                    publicationDate = paper.get("publicationDate", "No publicationDate available")
                    authors = ", ".join([author["name"] for author in paper.get("authors", [])])
                    abstract = paper.get("abstract", "No abstract available")
                    paper_id = paper.get("paperId", "No ID available")

                    # Add the paper details to the papers_data list
                    papers_data.append({
                        "title": title,
                        "publicationDate": publicationDate,
                        "authors": authors,
                        "abstract": abstract,
                        "paperId": paper_id
                    })
            else:
                logging.info(f"No papers found for query '{query}'")
            break

        except requests.exceptions.RequestException as e:
            logging.error(f"Request failed for query '{query}': {e}")
            if e.response and e.response.status_code == 429:
                # If rate limited, wait before retrying
                wait_time = delay * (attempt + 1)  # Exponential backoff
                logging.info(f"Rate limit exceeded. Retrying in {wait_time} seconds...")
                time.sleep(wait_time)
            else:
                break  # If it's not a rate-limiting issue, stop retrying

    return papers_data

# Fetch papers for each mental health disorder keyword
def ingest_data_from_mental_health_queries():
    # Loop through each query and fetch the papers
    for query in MENTAL_HEALTH_DISORDERS:
        papers_data = fetch_papers(query)
        if papers_data:
            # Convert papers data to JSON and upload it to S3
            json_data = json.dumps(papers_data, indent=4)
            upload_to_s3(json_data, "JSON", "semantic_scholar", query, dataset_id="semantic_scholar")
