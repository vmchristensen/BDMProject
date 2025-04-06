from ingest_data import ingest_data

# Define the raw file URL
url = "https://gist.githubusercontent.com/fmejias/8df2a27f1285576ae3cf4d67c3368144/raw/mental_health_disorders_unemployment_and_suicides.csv"
sourcename = "gist"
filename = "mental_health_disorders_unemployment_and_suicides"

ingest_data(url, sourcename, filename)

