# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.window import Window
from pyspark.ml.feature import VectorAssembler
from xgboost.spark import SparkXGBRegressor
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.feature import StringIndexer
from pyspark.ml.feature import OneHotEncoder
from pyspark.ml.regression import GBTRegressor
from pyspark.ml.regression import GBTRegressionModel
import pandas as pd

# COMMAND ----------

#Read h3 cell wise demand data

demand_h3 = spark.read.format("delta").table("demand_h3_res8_5min")

# COMMAND ----------

#Read weather data

weather = spark.read.format("delta").table("weather")

# COMMAND ----------

zones = spark.read.format("delta").table("taxi_zones")

# COMMAND ----------

demand_h3_zones = demand_h3.join(broadcast(zones), demand_h3.pickup_zone_id == zones.LocationID, "left")

# COMMAND ----------

demand_h3_weather = demand_h3_zones.join(broadcast(weather), (weather.borough == demand_h3_zones.Borough) & (date_trunc("hour", demand_h3_zones.window_start) == weather.time), "left")

# COMMAND ----------

#Repartition to avoid shuffling
demand_h3_weather = demand_h3_weather.repartition('pickup_h3')

# COMMAND ----------

#Add target pickup count

w = Window.partitionBy("pickup_h3").orderBy("window_start")

demand_h3_weather = demand_h3_weather.withColumn(
    "target_pickup_count",
    lead("pickup_count", 1).over(w)
)

# COMMAND ----------

demand_h3_weather = demand_h3_weather.filter("target_pickup_count IS NOT NULL")

# COMMAND ----------

demand_h3_weather = demand_h3_weather \
    .withColumn("window_start_hour", hour("window_start")) \
    .withColumn("day_of_week", dayofweek("window_start"))

# COMMAND ----------

demand_h3_weather = demand_h3_weather.withColumn(
    "prev_demand",
    lag("pickup_count", 1).over(w)
)

# COMMAND ----------

demand_h3_weather = demand_h3_weather \
    .withColumn("lag_12",  lag("pickup_count", 12).over(w)) \
    .withColumn("lag_288", lag("pickup_count", 288).over(w))    

# COMMAND ----------

demand_h3_weather = demand_h3_weather.fillna(0)

# COMMAND ----------

# Convert categorical columns to numeric

weather_indexer = StringIndexer(
    inputCol="weather_type",
    outputCol="weather_type_index",
    handleInvalid="keep"
)

temp_indexer = StringIndexer(
    inputCol="temp_bucket",
    outputCol="temperature_bucket_index"
    ,handleInvalid="keep"
)

# COMMAND ----------

encoder = OneHotEncoder(
    inputCols=["weather_type_index", "temperature_bucket_index"],
    outputCols=["weather_type_vec", "temperature_bucket_vec"]
)

# COMMAND ----------

demand_h3_weather = weather_indexer.fit(demand_h3_weather).transform(demand_h3_weather)
demand_h3_weather = temp_indexer.fit(demand_h3_weather).transform(demand_h3_weather)

demand_h3_weather = encoder.fit(demand_h3_weather).transform(demand_h3_weather)

# COMMAND ----------

# Define feature columns and assemble

feature_cols = [
    "pickup_count",
    "avg_trip_distance",
    "avg_fare",
    "avg_speed",
    "avg_tip_pct",
    "avg_passengers",
    "temp_2m",
    "rain",
    "snow",
    "wind",
    "is_raining",
    "is_snowing",
    "weather_type_vec",
    "temperature_bucket_vec",
    "window_start_hour",
    "day_of_week",
    "prev_demand",
    "lag_12",
    "lag_288"
]

assembler = VectorAssembler(
    inputCols=feature_cols,
    outputCol="features"
)

df_assembled = assembler.transform(demand_h3_weather)

# COMMAND ----------

# Split data into train and test

cutoff = "2025-06-01"
train_df = df_assembled.filter(col("window_start") < cutoff)
test_df  = df_assembled.filter(col("window_start") >= cutoff)

# COMMAND ----------

# Train model

gbt = GBTRegressor(
    featuresCol="features",
    labelCol="target_pickup_count",
    predictionCol="prediction",
    maxIter=40,
    maxDepth=6
)

model = gbt.fit(train_df)

# COMMAND ----------

# Evaluate model

predictions = model.transform(test_df)

# COMMAND ----------

rmse = RegressionEvaluator(labelCol="target_pickup_count", predictionCol="prediction", metricName="rmse").evaluate(predictions)
mae  = RegressionEvaluator(labelCol="target_pickup_count", predictionCol="prediction", metricName="mae").evaluate(predictions)
r2   = RegressionEvaluator(labelCol="target_pickup_count", predictionCol="prediction", metricName="r2").evaluate(predictions)

# COMMAND ----------

print(f"RMSE: {rmse:.4f} | MAE: {mae:.4f} | R²: {r2:.4f}")

# COMMAND ----------

model.write().overwrite().save(
    "s3a://nyc-lakehouse/models/gbt_model"
)

# COMMAND ----------

"""
model = GBTRegressionModel.load(
    "s3a://nyc-lakehouse/models/gbt_model"
)
"""

# COMMAND ----------

predictions = model.transform(df_assembled)

# COMMAND ----------

predictions_final = predictions.select(
    "window_start",
    "pickup_h3",
    "pickup_lat",
    "pickup_lon",
    "prediction"
)

# COMMAND ----------

predictions_final = predictions_final.withColumnRenamed(
    "prediction",
    "predicted_demand"
)

# COMMAND ----------

typical_demand = predictions_final.groupBy(
    "pickup_h3",
    hour("window_start").alias("hour_of_day"),
    dayofweek("window_start").alias("day_of_week")
).agg(
    avg("predicted_demand").alias("typical_demand"),
    percentile_approx("predicted_demand", 0.75).alias("demand_p75")
)

# COMMAND ----------

predictions_final.write.format("delta") \
    .mode("overwrite") \
    .save("s3a://nyc-lakehouse/features/demand_predictions")

# COMMAND ----------

# Register as table
spark.sql("""
CREATE TABLE IF NOT EXISTS demand_predictions
USING DELTA
LOCATION 's3a://nyc-lakehouse/features/demand_predictions'
""")

# COMMAND ----------

typical_demand.write.format("delta") \
    .mode("overwrite") \
    .save("s3a://nyc-lakehouse/features/typical_demand_lookup")

# COMMAND ----------

# Register as table
spark.sql("""
CREATE TABLE IF NOT EXISTS typical_demand_lookup
USING DELTA
LOCATION 's3a://nyc-lakehouse/features/typical_demand_lookup'
""")