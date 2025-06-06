import requests
from ingest_data import ingest_files_from_github, upload_metadata_json, dataset_exists_in_s3

# OpenNeuro GraphQL endpoint
OPENNEURO_API_URL = "https://openneuro.org/crn/graphql"

# GraphQL query to fetch datasets depending on modality
GRAPHQL_MRI_QUERY = """
query Datasets {
  datasets(first: 20, modality: "MRI", orderBy:{created: descending}) {
    edges {
      node {
        id
        created
        metadata{
          modalities
          studyDesign
          studyDomain
          species
          datasetName
        }
      }
    }
  }
}
"""

GRAPHQL_EEG_QUERY = """
query Datasets {
  datasets(first: 20, modality: "EEG", orderBy:{created: descending}) {
    edges {
      node {
        id
        created
        metadata{
          modalities
          studyDesign
          studyDomain
          species
          datasetName
        }
      }
    }
  }
}
"""

def fetch_neuro_datasets(query):
    # Send the request to the OpenNeuro GraphQL API
    response = requests.post(
        OPENNEURO_API_URL, json={"query": query}
    )

    if response.status_code == 200:
        data = response.json()

        # Extract dataset information
        datasets = data["data"]["datasets"]["edges"]
        
        # Prepare a list to store MRI dataset details
        datasets_list = []

        # Iterate through the datasets and extract the necessary info
        for dataset in datasets:
            dataset_id = dataset["node"]["id"]
            dataset_date = dataset["node"]["created"]
            dataset_name = dataset["node"]["metadata"]["datasetName"]
            modalities = dataset["node"]["metadata"]["modalities"]
            studyDesign = dataset["node"]["metadata"]["studyDesign"]
            studyDomain = dataset["node"]["metadata"]["studyDomain"]
            species = dataset["node"]["metadata"]["species"]

            # Store the dataset info in a dictionary
            datasets_list.append({
                "id": dataset_id,
                "date": dataset_date,
                "name": dataset_name,
                "modalities": modalities,
                "studyDesign": studyDesign,
                "studyDomain": studyDomain,
                "species": species
            })

        return datasets_list
    else:
        print(f"Error fetching datasets: {response.status_code}")
        return []
    
# Main function
def ingest_neuro_data():
  mri_datasets = fetch_neuro_datasets(GRAPHQL_MRI_QUERY)
  eeg_datasets = fetch_neuro_datasets(GRAPHQL_EEG_QUERY)

  datasets_id = []

  for dataset in mri_datasets:
    repo_name = dataset['id']
    if dataset_exists_in_s3(repo_name, "nii"):
       continue
    upload_metadata_json(dataset, repo_name, "nii")
    datasets_id.append(repo_name)

  for dataset in eeg_datasets:
    repo_name = dataset['id']
    if dataset_exists_in_s3(repo_name, "eeg"):
       continue
    upload_metadata_json(dataset, repo_name, "eeg")
    if repo_name not in datasets_id:
        datasets_id.append(repo_name)

  # Ingestion
  repo_owner = "OpenNeuroDatasets"
  branch = "main"
  sourcename = "OpenNeuro"

  for repo_name in datasets_id:
    ingest_files_from_github(repo_owner, repo_name, branch, sourcename, dataset_id=repo_name)


ingest_neuro_data()