from pyspark.sql import SparkSession
from pyspark.sql.functions import input_file_name, regexp_extract, lit, col, lower, trim, concat_ws, udf, split, expr
from pyspark.sql.types import StringType, ArrayType, IntegerType
import os
import unicodedata

# --- UDFs ---

# Normalize text to lowercase, strip, and apply NFKC Unicode normalization
def normalize_text(text):
    if text:
        return unicodedata.normalize("NFKC", str(text).strip().lower())
    return ""

# Flatten possibly nested abstract fields
def flatten_abstract(val):
    def recurse(x):
        if isinstance(x, str):
            return [x]
        elif isinstance(x, list):
            return sum((recurse(i) for i in x), [])
        elif isinstance(x, dict):
            return sum((recurse(v) for v in x.values()), [])
        else:
            return []
    return recurse(val)

# Register UDFs
normalize_udf = udf(normalize_text, StringType())
flatten_udf = udf(flatten_abstract, ArrayType(StringType()))

def main():
    # Start Spark session
    spark = SparkSession.builder \
        .appName("SS_transform") \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.2,com.amazonaws:aws-java-sdk-bundle:1.11.901") \
        .getOrCreate()
    
    # Configure S3 access via environment variables
    hadoop_conf = spark._jsc.hadoopConfiguration()
    hadoop_conf.set("fs.s3a.path.style.access", "true")
    hadoop_conf.set("fs.s3a.access.key", os.getenv("AWS_ACCESS_KEY_ID"))
    hadoop_conf.set("fs.s3a.secret.key", os.getenv("AWS_SECRET_ACCESS_KEY"))
    hadoop_conf.set("fs.s3a.endpoint", "s3.amazonaws.com")
    hadoop_conf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")

    # S3 path pattern for all disorders
    input_path = "s3a://bdm.project.input/data/json/semantic_scholar/semantic_scholar*.json"

    # Read the JSON files
    print("Reading raw Semantic Scholar data from S3...")
    ss_raw = spark.read.option("multiline", "true").json(input_path)

    print(f"Raw record count: {ss_raw.count()}")

    # Rename and select columns to match output schema
    print("Transforming data...")
    ss_clean = (
        ss_raw
        .withColumn("paperId", trim(col("paperId")))
        .dropna(subset=["paperId", "abstract"])
        .withColumn("abstract_array", flatten_udf(col("abstract")))
        .withColumn("abstract", normalize_udf(concat_ws(" ", col("abstract_array"))))
        .withColumn("title", normalize_udf(col("title")))
        .withColumn("publication_year", expr("substring(publicationDate, 1, 4)"))
        .withColumn("year", col("publication_year").cast(IntegerType()))
        .withColumn("disorders", regexp_extract(input_file_name(), r"semantic_scholar_(.*?)_\d{8}", 1))
        .select("paperId", "title", "authors", "abstract", "year", "disorders")
        .dropDuplicates(["paperId"])
        .cache()
    )

    print(f"Cleaned record count: {ss_clean.count()}")

    # Write output as Parquet (or CSV, JSON etc.)
    output_path = "s3a://bdm.trusted.zone/clean_data/json/semantic_scholar/"
    ss_clean.write.mode("overwrite").json(output_path + "json/")
    # ss_clean.write.mode("overwrite").parquet(output_path + "parquet/")

    print("Transformation completed.")
    spark.stop()

if __name__ == "__main__":
    main()
