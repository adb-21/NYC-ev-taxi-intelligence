# Databricks notebook source
import boto3
import requests

# COMMAND ----------

# Set up AWS credentials
ACCESS_KEY = dbutils.secrets.get(scope="my_app_creds", key="access_key_id")
SECRET_KEY = dbutils.secrets.get(scope="my_app_creds", key="secret_access_key")
REGION = 'us-east-1'

# Create a client with explicit credentials
s3_client = boto3.client(
    's3',
    aws_access_key_id=ACCESS_KEY,
    aws_secret_access_key=SECRET_KEY,
    region_name=REGION
)

# COMMAND ----------

# Download the taxi zone data from the NYC Taxi and Limousine Commission (TLC)

url = "https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv"
bucket = "nyc-lakehouse"
key = f"bronze/misc/taxi_zone_lookup.csv"

with requests.get(url) as r:
    r.raise_for_status()
    s3_client.put_object(Bucket=bucket, Key=key, Body=r.content)

# COMMAND ----------

# Read the taxi zone data into a Spark DataFrame

zone_df = spark.read.format("csv") \
    .option("header", True) \
    .load(f"s3a://{bucket}/{key}")

# COMMAND ----------

# Write the taxi zone data to Delta Lake

zone_df.write.format("delta") \
    .mode("overwrite") \
    .save("s3a://nyc-lakehouse/reference/taxi_zones")

# COMMAND ----------

#One time code to register as table

spark.sql("""
CREATE TABLE IF NOT EXISTS taxi_zones
USING DELTA
LOCATION 's3a://nyc-lakehouse/reference/taxi_zones'
""")