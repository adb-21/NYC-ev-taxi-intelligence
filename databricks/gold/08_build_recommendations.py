# Databricks notebook source
#pip install "h3<4"

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.window import Window
from pyspark.sql.types import ArrayType, StringType, IntegerType
import h3

# COMMAND ----------

typical_demand  = spark.read.format("delta").table("typical_demand_lookup")
charger_mapping = spark.read.format("delta").table("nearest_chargers")

# COMMAND ----------

# Define the UDF to generate the k-ring of a given H3 cell

candidate_udf = udf(
    lambda cell: [c for c in h3.k_ring(cell, 4) if c != cell],
    ArrayType(StringType())
)

# COMMAND ----------

# Apply the UDF to the DataFrame

demand_with_candidates = typical_demand \
    .withColumn("candidate_cells", candidate_udf("pickup_h3")) \
    .withColumn("candidate_h3", explode("candidate_cells")) \
    .withColumnRenamed("pickup_h3", "origin_h3") \
    .withColumnRenamed("typical_demand", "origin_demand")

# COMMAND ----------

candidate_demand = typical_demand \
    .withColumnRenamed("pickup_h3", "candidate_h3") \
    .withColumnRenamed("typical_demand", "candidate_demand") \
    .withColumnRenamed("demand_p75", "candidate_p75")

# COMMAND ----------

scored = demand_with_candidates.join(
    candidate_demand,
    on=["candidate_h3", "hour_of_day", "day_of_week"],
    how="left"
).filter(col("candidate_demand").isNotNull())

# COMMAND ----------

distance_udf = udf(
    lambda a, b: int(h3.h3_distance(a, b)),
    IntegerType()
)

# COMMAND ----------

scored = scored.withColumn(
    "grid_distance",
    distance_udf("origin_h3", "candidate_h3")
)

# COMMAND ----------

charger_cols = charger_mapping.select(
    "h3_cell",
    "P1_charger_fast_chargers",
    "P1_charger_level2_chargers",
    "P2_charger_fast_chargers",
    "P2_charger_level2_chargers",
    "P3_charger_fast_chargers",
    "P3_charger_level2_chargers"
)

# COMMAND ----------

scored = scored.join(
    charger_cols.withColumnRenamed("h3_cell", "candidate_h3"),
    "candidate_h3", "left"
).fillna(0)

# COMMAND ----------

# Calculate the score for each candidate cell

scored = scored.withColumn("charger_score",
    col("P1_charger_fast_chargers")   * 2.5 +        # P1 full weight
    col("P1_charger_level2_chargers") * 1.0 +
    col("P2_charger_fast_chargers")   * 2.5 * 0.6 +  # P2 discounted 40%
    col("P2_charger_level2_chargers") * 1.0 * 0.6 +
    col("P3_charger_fast_chargers")   * 2.5 * 0.3 +  # P3 discounted 70%
    col("P3_charger_level2_chargers") * 1.0 * 0.3
)

# COMMAND ----------

# Calculate the normalized scores for each candidate cell

w = Window.partitionBy("origin_h3", "hour_of_day", "day_of_week")

scored = scored \
    .withColumn("norm_demand",
        (col("candidate_demand") - min("candidate_demand").over(w)) /
        (max("candidate_demand").over(w) - min("candidate_demand").over(w) + 1e-9)
    ) \
    .withColumn("norm_distance",
        (col("grid_distance") - min("grid_distance").over(w)) /
        (max("grid_distance").over(w) - min("grid_distance").over(w) + 1e-9)
    ) \
    .withColumn("norm_charger",
        (col("charger_score") - min("charger_score").over(w)) /
        (max("charger_score").over(w) - min("charger_score").over(w) + 1e-9)
    ) \
    .withColumn("final_score",
        0.50 * col("norm_demand") -
        0.25 * col("norm_distance") +
        0.25 * col("norm_charger")
    )

# COMMAND ----------

# Keep top 5 per origin cell × time slot 
rank_w = Window.partitionBy(
    "origin_h3", "hour_of_day", "day_of_week"
).orderBy(desc("final_score"))

recommendations = scored \
    .withColumn("rank", row_number().over(rank_w)) \
    .filter(col("rank") <= 5) \
    .select(
        "origin_h3", "hour_of_day", "day_of_week",
        "candidate_h3", "grid_distance",
        "candidate_demand", "charger_score",
        "final_score", "rank"
    )

# COMMAND ----------

recommendations.write.format("delta") \
    .mode("overwrite") \
    .partitionBy("hour_of_day", "day_of_week") \
    .save("s3a://nyc-lakehouse/features/recommendations")

spark.sql("""
    CREATE TABLE IF NOT EXISTS recommendations
    USING DELTA
    LOCATION 's3a://nyc-lakehouse/features/recommendations'
""")