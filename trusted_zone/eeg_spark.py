from pyspark.sql import SparkSession
from pyspark.sql.functions import input_file_name, regexp_extract, col, udf
from pyspark.sql.types import StructType, StructField, BooleanType, StringType, IntegerType, FloatType
import mne
import boto3
import io
import os

# === UDF: Validate EEG files using MNE ===
def validate_eeg(path):
    try:
        if not path.startswith("s3a://"):
            return (False, None, None, None, "Invalid path")
        bucket, key = path.replace("s3a://", "").split("/", 1)
        s3 = boto3.client("s3",
                          aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
                          aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"))
        obj = s3.get_object(Bucket=bucket, Key=key)
        raw_bytes = io.BytesIO(obj["Body"].read())

        raw = mne.io.read_raw(raw_bytes, preload=False, verbose=False)
        info = raw.info
        return (True, info['sfreq'], len(info['ch_names']), raw.times[-1], None)
    except Exception as e:
        return (False, None, None, None, str(e))

schema = StructType([
    StructField("is_valid", BooleanType()),
    StructField("sampling_rate", FloatType()),
    StructField("channel_count", IntegerType()),
    StructField("duration_sec", FloatType()),
    StructField("error_msg", StringType())
])

validate_eeg_udf = udf(validate_eeg, schema)

# === Spark Main ===
def main():
    spark = SparkSession.builder \
        .appName("EEG_Metadata_Integration") \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.2,com.amazonaws:aws-java-sdk-bundle:1.11.901") \
        .getOrCreate()

    # Configure S3
    hadoop_conf = spark._jsc.hadoopConfiguration()
    hadoop_conf.set("fs.s3a.path.style.access", "true")
    hadoop_conf.set("fs.s3a.access.key", os.getenv("AWS_ACCESS_KEY_ID"))
    hadoop_conf.set("fs.s3a.secret.key", os.getenv("AWS_SECRET_ACCESS_KEY"))
    hadoop_conf.set("fs.s3a.endpoint", "s3.amazonaws.com")
    hadoop_conf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")

    # 1. Load EEG binary files
    eeg_path = "s3a://bdm.project.input/data/eeg/*/*"
    eeg_df = spark.read.format("binaryFile").load(eeg_path) \
        .filter(col("path").rlike(r"\.(edf|vhdr|eeg|bdf|set|mat)$")) \
        .withColumn("file_name", regexp_extract(input_file_name(), r'([^/]+)$', 1)) \
        .withColumn("dataset", regexp_extract(input_file_name(), r'data/eeg/([^/]+)/', 1)) \
        .withColumn("subject_id", regexp_extract("file_name", r'sub-([0-9]+)', 1))

    # 2. Validate files with MNE
    validated = eeg_df.withColumn("validation", validate_eeg_udf("path")) \
        .select(
            "path", "file_name", "dataset", "subject_id",
            col("validation.is_valid"),
            col("validation.sampling_rate"),
            col("validation.channel_count"),
            col("validation.duration_sec"),
            col("validation.error_msg")
        )

    # 3. Load and join metadata
    metadata_path = "s3a://bdm.project.input/data/eeg/*/dataset_metadata.json"
    metadata_df = spark.read.json(metadata_path) \
        .withColumnRenamed("id", "dataset_id") \
        .withColumnRenamed("date", "metadata_date") \
        .withColumn("modality", col("modalities")[0]) \
        .select("dataset_id", "metadata_date", "name", "modality", "studyDesign", "studyDomain", "species")

    enriched = validated.join(
        metadata_df,
        validated["dataset"] == metadata_df["dataset_id"],
        "left"
    ).drop("dataset_id")

    # 4. Save
    output_path = "s3a://bdm.project.trusted/clean_data/eeg/full/"
    enriched.write.mode("overwrite").parquet(output_path)

    print("EEG processing completed.")
    spark.stop()

if __name__ == "__main__":
    main()
