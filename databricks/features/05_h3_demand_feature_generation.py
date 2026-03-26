# Databricks notebook source
#pip install "h3<4"

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import StructField, DoubleType
import h3

# COMMAND ----------

trips = spark.read.format('delta').table("nyc_silver_yellow")

# COMMAND ----------

# Read the h3 mappings
h3_mappings = spark.read.format('delta').table("h3_mappings_resolution_8")


# COMMAND ----------

# Create a mapping between taxi zones and h3 cells
trips_h3 = trips.join(
    broadcast(h3_mappings),
    trips.PULocationID == h3_mappings.LocationID,
    "left"
)

# COMMAND ----------

trips_h3 = trips_h3.filter(~col('PULocationID').isin([264,265]))

# COMMAND ----------

# Randomly select a h3 cell
trips_h3 = trips_h3.withColumn(
    "pickup_h3",
    expr("h3_cells[cast(rand() * size(h3_cells) as int)]")
)


# COMMAND ----------

# Group by pickup hour and h3 cell
demand_features = trips_h3.groupBy(
    window("tpep_pickup_datetime", "5 minutes"),
    "PULocationID",
    "pickup_h3"
).agg(
    count("*").alias("pickup_count"),
    avg("trip_distance").alias("avg_trip_distance"),
    avg("fare_amount").alias("avg_fare"),
    avg("average_speed_mph").alias("avg_speed"),
    avg("tip_percentage").alias("avg_tip_pct"),
    avg("passenger_count").alias("avg_passengers")
)

# COMMAND ----------

def h3_to_geo(h):
    lat, lon = h3.h3_to_geo(h)
    return (float(lat), float(lon))

schema = StructType([
    StructField("lat",DoubleType()),
    StructField("lon",DoubleType())
])

h3_to_geo_udf = udf(h3_to_geo, schema)

# COMMAND ----------

demand_features = demand_features.withColumn(
    "h3_coord",
    h3_to_geo_udf("pickup_h3")
)

# COMMAND ----------

demand_features = demand_features \
    .withColumn("pickup_lat", col("h3_coord.lat")) \
    .withColumn("pickup_lon", col("h3_coord.lon")) \
    .drop("h3_coord") \
    .drop("h3_cells")


# COMMAND ----------

demand_features = demand_features.select(
    col("window.start").alias("window_start"),
    col("window.end").alias("window_end"),
    col("PULocationID").alias("pickup_zone_id"),
    "pickup_lat",
    "pickup_lon",
    "pickup_h3",
    "pickup_count",
    "avg_trip_distance",
    "avg_fare",
    "avg_speed",
    "avg_tip_pct",
    "avg_passengers"
)

# COMMAND ----------

# Add a column for the start of the window
demand_features = demand_features.withColumn(
    "window_start_date",
    to_date(col("window_start"))
)


# COMMAND ----------

demand_features.write.format("delta") \
    .mode("overwrite") \
    .partitionBy("window_start_date") \
    .save("s3a://nyc-lakehouse/features/demand_h3_res8_5min")

# COMMAND ----------

# Register as table
spark.sql("""
CREATE TABLE IF NOT EXISTS demand_h3_res8_5min
USING DELTA
LOCATION 's3a://nyc-lakehouse/features/demand_h3_res8_5min'
""")