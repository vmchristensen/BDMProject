from pyspark.sql import SparkSession
from pyspark.sql.functions import col, udf, input_file_name
from pyspark.sql.types import StringType
import os
import unicodedata
import re

# --- UDFs ---
KNOWN_DISORDERS = [
    "adhd", "autism", "depression", "anxiety", "bipolar", "ptsd",
    "schizophrenia", "ocd", "epilepsy", "stroke", "alzheimer", "parkinson"
]

def extract_disorder(name):
    if name:
        normalized = unicodedata.normalize("NFKC", name.strip().lower())
        for disorder in KNOWN_DISORDERS:
            if re.search(rf"\b{re.escape(disorder)}\b", normalized):
                return disorder
    return None


def extract_dataset_id(path):
    match = re.search(r"/(ds\d{6})/", path)
    return match.group(1) if match else None

extract_disorder_udf = udf(extract_disorder, StringType())
extract_dataset_id_udf = udf(extract_dataset_id, StringType())

# --- Main ---
def main():
    spark = SparkSession.builder \
        .appName("Metadata_Disorder_Extraction") \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.2,com.amazonaws:aws-java-sdk-bundle:1.11.901") \
        .getOrCreate()

    hadoop_conf = spark._jsc.hadoopConfiguration()
    hadoop_conf.set("fs.s3a.path.style.access", "true")
    hadoop_conf.set("fs.s3a.access.key", os.getenv("AWS_ACCESS_KEY_ID"))
    hadoop_conf.set("fs.s3a.secret.key", os.getenv("AWS_SECRET_ACCESS_KEY"))
    hadoop_conf.set("fs.s3a.endpoint", "s3.amazonaws.com")
    hadoop_conf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")

    input_pattern = "s3a://bdm.project.input/data/eeg/*/ds*_metadata.json"
    print(f"Reading: {input_pattern}")

    df_raw = spark.read.option("multiline", "true").json(input_pattern) \
        .withColumn("path", input_file_name())

    # Extract fields
    df_enriched = (
        df_raw
        .withColumn("dataset_id", extract_dataset_id_udf(col("path")))
        .withColumn("disorder", extract_disorder_udf(col("name")))
        .dropna(subset=["dataset_id", "disorder"])
        .dropDuplicates(["dataset_id"])
        .cache()
    )

    # Write per-dataset enriched JSON
    dataset_ids = [row.dataset_id for row in df_enriched.select("dataset_id").distinct().collect()]

    for ds_id in dataset_ids:
        out_path = f"s3a://bdm.trusted.zone/clean_data/json/eeg/{ds_id}/"
        print(f"Writing enriched metadata for {ds_id} to: {out_path}")
        (
            df_enriched
            .filter(col("dataset_id") == ds_id)
            .drop("path")  # Optional: drop internal path column
            .write
            .mode("overwrite")
            .json(out_path)
        )

    print("Finished processing all metadata files.")
    spark.stop()

if __name__ == "__main__":
    main()


