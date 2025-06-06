from pyspark.sql import SparkSession
from pyspark.sql.functions import input_file_name, regexp_extract, col, udf, lit
from pyspark.sql.types import BooleanType, ArrayType, IntegerType, StringType, StructType, StructField, FloatType
import nibabel as nib
import boto3
import io
import os

# --- UDF for validating NIfTI files from S3 ---
def validate_nii_from_s3(path):
    try:
        if not path.startswith("s3a://"):
            return (False, None, None, "Invalid S3 path")

        s3_parts = path.replace("s3a://", "").split("/", 1)
        bucket, key = s3_parts[0], s3_parts[1]

        s3 = boto3.client("s3",
            aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
            aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY")
        )
        obj = s3.get_object(Bucket=bucket, Key=key)
        file_bytes = io.BytesIO(obj["Body"].read())

        img = nib.load(file_bytes)
        shape = img.shape
        affine = img.affine.tolist()

        return (True, shape, affine, None)

    except Exception as e:
        return (False, None, None, str(e))

schema = StructType([
    StructField("is_valid", BooleanType()),
    StructField("shape", ArrayType(IntegerType())),
    StructField("affine", ArrayType(ArrayType(FloatType()))),
    StructField("error_msg", StringType())
])

validate_nii_udf = udf(validate_nii_from_s3, schema)

# --- Main Spark Job ---
def main():
    spark = SparkSession.builder \
        .appName("MRI_Metadata_Integration") \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.2,com.amazonaws:aws-java-sdk-bundle:1.11.901") \
        .getOrCreate()

    # S3 configs
    hadoop_conf = spark._jsc.hadoopConfiguration()
    hadoop_conf.set("fs.s3a.access.key", os.getenv("AWS_ACCESS_KEY_ID"))
    hadoop_conf.set("fs.s3a.secret.key", os.getenv("AWS_SECRET_ACCESS_KEY"))
    hadoop_conf.set("fs.s3a.endpoint", "s3.amazonaws.com")
    hadoop_conf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    hadoop_conf.set("fs.s3a.path.style.access", "true")

    # === 1. Load .nii.gz files ===
    input_path = "s3a://bdm.project.input/data/nii/*/*.nii.gz"
    print("Loading MRI files...")

    nii_df = spark.read.format("binaryFile").load(input_path) \
        .withColumn("file_name", regexp_extract(input_file_name(), r'([^/]+\.nii\.gz)$', 1)) \
        .withColumn("dataset", regexp_extract(input_file_name(), r'data/nii/([^/]+)/', 1)) \
        .withColumn("subject_id", regexp_extract("file_name", r'sub-([0-9]+)', 1)) \
        .withColumn("timestamp", regexp_extract("file_name", r'_(\d{8}_\d{6})', 1))

    # === 2. Validate files using nibabel ===
    print("Validating NIfTI headers...")
    validated_df = nii_df.withColumn("validation", validate_nii_udf("path")) \
        .select(
            "path", "file_name", "dataset", "subject_id", "timestamp",
            col("validation.is_valid").alias("is_valid"),
            col("validation.shape").alias("shape"),
            col("validation.affine").alias("affine"),
            col("validation.error_msg").alias("error_msg")
        )

    # === 3. Deduplicate by subject_id (latest timestamp) ===
    from pyspark.sql.window import Window
    from pyspark.sql.functions import row_number

    window = Window.partitionBy("subject_id").orderBy(col("timestamp").desc())
    dedup_df = validated_df.withColumn("rank", row_number().over(window)) \
        .filter("rank = 1 AND is_valid = true") \
        .drop("rank")

    print(f"Valid & deduplicated MRI files: {dedup_df.count()}")

    # === 4. Load dataset_metadata.json files ===
    print("Loading dataset metadata JSONs...")
    metadata_path = "s3a://bdm.project.input/data/nii/*/*_metadata.json"
    metadata_df = spark.read.option("multiline", "true").json(metadata_path)

    metadata_flat = metadata_df.withColumnRenamed("id", "dataset_id") \
        .withColumnRenamed("date", "metadata_date") \
        .withColumn("modality", col("modalities")[0]) \
        .select("dataset_id", "metadata_date", "name", "modality", "studyDesign", "studyDomain", "species")

    # === 5. Join MRI file data with dataset metadata ===
    enriched_df = dedup_df.join(
        metadata_flat,
        dedup_df["dataset"] == metadata_flat["dataset_id"],
        how="left"
    ).drop("dataset_id")

    # === 6. Save to Trusted Zone ===
    output_path = "s3a://bdm.trusted.zone/clean_data/nii/full/"
    print("Writing enriched MRI metadata to trusted zone...")
    enriched_df.write.mode("overwrite").parquet(output_path)
    
    print("MRI metadata processing complete.")
    spark.stop()

if __name__ == "__main__":
    main()
