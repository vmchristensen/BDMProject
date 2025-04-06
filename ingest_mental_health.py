from ingest_data import ingest_data

url = "https://gist.github.com/mosesvemana/846f084f28e759bc6b522bdd269a74a8/raw/Mental_Health.csv"
sourcename = "gist"
filename = "mental_health"
ingest_data(url, sourcename, filename)
