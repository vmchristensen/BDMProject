from automatic_schedule import schedule_days
from ingest_semantic_scholar import ingest_data_from_mental_health_queries 
from ingestion_pmc import ingest_data_from_pmc
from ingest_neuro_data import ingest_neuro_data  
import time

# Schedule ingest_data_from_mental_health_queries to run every 7 days
schedule_days(ingest_data_from_mental_health_queries, 7)

# Schedule ingest_data_from_pmcs to run every 7 days
schedule_days(ingest_data_from_pmc, 7)

# Schedule ingest_neuro_data to run every 30 days
schedule_days(ingest_neuro_data, 30)


while True:
    time.sleep(3600)  # Sleep for 1 hour, adjust if needed
