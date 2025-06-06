from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lower, trim, concat_ws, udf
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

# --- Main Transformation Logic ---

def main():
    # Start Spark session
    spark = SparkSession.builder \
        .appName("PMC_Transform") \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.2,com.amazonaws:aws-java-sdk-bundle:1.11.901") \
        .getOrCreate()

    # Configure S3 access via environment variables
    hadoop_conf = spark._jsc.hadoopConfiguration()
    hadoop_conf.set("fs.s3a.path.style.access", "true")
    hadoop_conf.set("fs.s3a.access.key", os.getenv("AWS_ACCESS_KEY_ID"))
    hadoop_conf.set("fs.s3a.secret.key", os.getenv("AWS_SECRET_ACCESS_KEY"))
    hadoop_conf.set("fs.s3a.endpoint", "s3.amazonaws.com")
    hadoop_conf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")

    # Load JSON files
    print("Reading raw PMC data from S3...")
    pmc_raw = spark.read.option("multiline", "true").json("s3a://bdm.project.input/data/json/pmc/pmc*.json")

    # Clean and transform
    pmc_raw = pmc_raw.withColumnRenamed("Disorders Found", "Disorders_Found")

    print(f"Raw record count: {pmc_raw.count()}")

    # Data transformations
    print("Transforming data...")
    pmc_clean = (
        pmc_raw
        .withColumn("paperId", trim(col("PMC_ID")))
        .dropna(subset=["paperId", "Abstract"])
        .withColumn("abstract_array", flatten_udf(col("Abstract")))
        .withColumn("abstract", normalize_udf(concat_ws(" ", col("abstract_array"))))
        .withColumn("title", normalize_udf(col("Title")))
        .withColumn("year", col("Year").cast(IntegerType()))
        .withColumn("disorders", concat_ws(", ", col("Disorders_Found")))
        .select("paperId", "title", "abstract", "year", "disorders")
        .dropDuplicates(["paperId"])
        .cache()
    )

    print(f"Cleaned record count: {pmc_clean.count()}")
    
    # Write to trusted zone in S3
    output_path = "s3a://bdm.trusted.zone/clean_data/json/pmc/spark/"
    print(f"Writing cleaned data to: {output_path}")
    pmc_clean.write.mode("overwrite").json(output_path + "json/")
    pmc_clean.write.mode("overwrite").parquet(output_path + "parquet/")

    print("Transformation completed.")
    spark.stop()

if __name__ == "__main__":
    main()
