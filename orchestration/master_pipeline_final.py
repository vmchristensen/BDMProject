# Save this file as: ~/airflow/dags/master_pipeline_final.py

from __future__ import annotations
import pendulum
from airflow.models.dag import DAG
from airflow.operators.bash import BashOperator
from airflow.utils.task_group import TaskGroup

# --- Define Your S3 Bucket Variables ---
LANDING_ZONE_BUCKET = "bdm.project.input"
TRUSTED_ZONE_BUCKET = "bdm.trusted.zone"
EXPLOITATION_ZONE_BUCKET = "bdm.exploitation.zone"

# --- DAG Definition ---
with DAG(
    dag_id="master_pipeline_final",
    start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
    catchup=False,
    schedule="@daily",
    tags=[ "aws", "data-platform"],
    doc_md="""
    ### Master Data Pipeline (Final Version)
    - **Hot Path**: Daily checks on streaming services.
    - **Cold Path**: Batch ETL (Landing -> Trusted -> Exploitation).
    - **Outputs**: Final convergence point for dashboards.
    """
) as dag:

    start_pipeline = BashOperator(
        task_id="start_pipeline",
        bash_command='echo "Starting daily data platform synchronization for {{ ds }}..."',
    )

    # HOT PATH
    with TaskGroup(group_id="hot_path_processing", tooltip="Daily tasks for the live streaming path") as hot_path_group:
        health_check = BashOperator(
            task_id="check_streaming_service_health",
            bash_command='echo "[HOT_PATH] Health check on Kafka/Redis... OK"',
        )
        snapshot_hot_path_counters = BashOperator(
            task_id="snapshot_redis_hot_path_counters",
            bash_command=f'echo "[HOT_PATH] Archiving Redis counts to s3://{TRUSTED_ZONE_BUCKET}/hot_path_snapshots/"',
        )
        health_check >> snapshot_hot_path_counters

    # COLD PATH
    with TaskGroup(group_id="cold_path_batch_processing", tooltip="Full daily batch data processing") as cold_path_group:

        # LANDING ZONE
        with TaskGroup(group_id="bdm_project_input", tooltip="Trigger ingestion from various batch sources") as landing_zone_group:
            ingest_mental_health_stats = BashOperator(
                task_id="ingest_mental_health",
                bash_command='echo "Ingesting mental health stats..."'
            )
            ingest_neuro_data = BashOperator(
                task_id="ingest_neuro_data",
                bash_command='echo "Ingesting neuroimaging data..."'
            )
            ingest_pmc_articles = BashOperator(
                task_id="ingest_pmc",
                bash_command='echo "Ingesting PubMed articles..."'
            )
            ingest_semantic_scholar = BashOperator(
                task_id="ingest_semantic_scholar",
                bash_command='echo "Ingesting Semantic Scholar data..."'
            )
            ingest_socioeconomic_data = BashOperator(
                task_id="ingest_unemployment_and_suicides",
                bash_command='echo  "Ingesting socioeconomic data..."'
            )

        # TRUSTED ZONE
        with TaskGroup(group_id="bdm_trusted_zone", tooltip="Trigger Spark jobs for cleaning and transformation") as trusted_zone_group:
            process_eeg_main = BashOperator(
                task_id="run_eeg_spark",
                bash_command='echo  "Running eeg_spark.py"'
            )
            process_eeg_metadata = BashOperator(
                task_id="run_eeg_metadata_spark",
                bash_command='echo  "Running eeg_metadata_spark.py"'
            )
            process_nii_main = BashOperator(
                task_id="run_nii_spark",
                bash_command='echo  "Running nii_spark.py"'
            )
            process_nii_metadata = BashOperator(
                task_id="run_nii_metadata_spark",
                bash_command='echo  "Running nii_metadata_spark.py"'
            )
            process_pmc = BashOperator(
                task_id="run_pmc_spark",
                bash_command='echo  "Running pmc_spark.py"'
            )
            process_semantic_scholar = BashOperator(
                task_id="run_semanticscholar_spark",
                bash_command='echo  "Running semanticscholar_spark.py"'
            )
            process_generic_csv = BashOperator(
                task_id="run_generic_csv_processor_nospark",
                bash_command='echo  "Running generic_csv_processor_nospark.py"'
            )

        # EXPLOITATION ZONE
        with TaskGroup(group_id="bdm_exploitation_zone", tooltip="Trigger final aggregation jobs for the dashboard") as exploitation_zone_group:
            aggregate_csv_gen = BashOperator(
                task_id="run_csv_toexpl_gen",
                bash_command='echo  "Running csv_toexpl_gen.py"'
            )
            aggregate_csv_spark = BashOperator(
                task_id="run_csv_toexpl_spark",
                bash_command='echo  "Running csv_toexpl_spark.py"'
            )
            aggregate_json = BashOperator(
                task_id="run_json_toexpl_spark",
                bash_command='echo "Running json_toexpl_spark.py"'
            )
            aggregate_nii = BashOperator(
                task_id="run_nii_toexpl_spark",
                bash_command='echo  "Running nii_toexpl_spark.py"'
            )
            aggregate_eeg = BashOperator(
                task_id="run_eeg_toexpl_spark",
                bash_command='echo  "Running eeg_toexpl_spark.py"'
            )

        # Define batch chain
        landing_zone_group >> trusted_zone_group >> exploitation_zone_group

    # OUTPUTS
    with TaskGroup(group_id="outputs", tooltip="Update and refresh final outputs (e.g., dashboards)") as outputs_group:
        trigger_dashboard_refresh = BashOperator(
            task_id="trigger_dashboard_refresh",
            bash_command='echo "[OUTPUT] Triggering dashboard refresh..."'
        )

    finish_pipeline = BashOperator(
        task_id="finish_pipeline",
        bash_command='echo "Master data pipeline finished successfully."',
    )

    # MASTER WORKFLOW
    start_pipeline >> [hot_path_group, cold_path_group]
    [hot_path_group, cold_path_group] >> outputs_group
    outputs_group >> finish_pipeline
