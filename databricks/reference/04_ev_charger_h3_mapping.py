# Databricks notebook source
#pip install "h3<4"

# COMMAND ----------

from pyspark.sql.functions import *
import h3
from pyspark.sql.types import StringType

# COMMAND ----------

# Fetch charger data

chargers = spark.read.option("header",True) \
    .csv("/Volumes/workspace/default/nyc_reference_volume/ev_chargers/NY_NJ_EV_chargers.csv")

# COMMAND ----------

# Filter for NYC and NJ

NYC_chargers = chargers.filter(
    (col("Latitude").try_cast("double")  >= 40.45) &
    (col("Latitude").try_cast("double")  <= 40.95) &
    (col("Longitude").try_cast("double") >= -74.30) &
    (col("Longitude").try_cast("double") <= -73.65)
)

# COMMAND ----------


NYC_chargers = NYC_chargers.select(
    col("Station Name").alias("station_name"),
    col("Latitude").cast('double').alias("lat"),
    col("Longitude").cast('double').alias("lon"),
    col("EV DC Fast Count").cast('integer').alias("fast_chargers"),
    col("EV Level2 EVSE Num").cast('integer').alias("level2_chargers")
)

# COMMAND ----------

NYC_chargers = NYC_chargers.withColumn(
    "charger_id",
    monotonically_increasing_id()
)

# COMMAND ----------

temp = NYC_chargers.select("charger_id","station_name","lat","lon")
temp.printSchema()

# COMMAND ----------

# Create H3 cell

def geo_to_h3(lat,lon):
    return h3.geo_to_h3(lat,lon,8)

geo_to_h3_udf = udf(geo_to_h3,StringType())

# COMMAND ----------


NYC_chargers = NYC_chargers.withColumn(
    "charger_h3",
    geo_to_h3_udf("lat","lon")
)

# COMMAND ----------

# Store as paquet
NYC_chargers.write.format("delta") \
    .mode("overwrite") \
    .save("s3a://nyc-lakehouse/reference/chargers")

# COMMAND ----------

# Register as table
spark.sql("""
CREATE TABLE IF NOT EXISTS NY_chargers
USING DELTA
LOCATION 's3a://nyc-lakehouse/reference/chargers'
""")
