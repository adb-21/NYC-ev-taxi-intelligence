# Databricks notebook source
#pip install "h3<4"

# COMMAND ----------

from pyspark.sql.functions import *
import h3
from pyspark.sql.types import IntegerType
from pyspark.sql.window import Window

# COMMAND ----------

h3_mapping_res8 = spark.read.format('delta').table('h3_mappings_resolution_8')

# COMMAND ----------

chargers = spark.read.format('delta').table('ny_chargers')

# COMMAND ----------

h3_mappings_exploded = h3_mapping_res8.select('LocationID', explode('h3_cells').alias('h3_cell'))

# COMMAND ----------

zone_charger_mapping = h3_mappings_exploded.crossJoin(chargers)

# COMMAND ----------

def h3_distance(a,b):
    return h3.h3_distance(a,b)

distance_udf = udf(h3_distance,IntegerType())


# COMMAND ----------

zone_charger_mapping = zone_charger_mapping.withColumn(
    "charger_distance",
    distance_udf("h3_cell","charger_h3")
)

# COMMAND ----------

# Sort according to charger distance
partition_window = Window.partitionBy("LocationID","h3_Cell").orderBy(col("charger_distance").asc())
zone_charger_mapping = zone_charger_mapping.withColumn("priority", row_number().over(partition_window))

# COMMAND ----------

# Filter top 3 nearest charging stations
nearest_chargers = zone_charger_mapping.filter(col("priority") <= 3)

# COMMAND ----------

priorities = ["1", "2", "3"]

# Pivot using the priority column
nearest_chargers = nearest_chargers.groupBy("LocationID","h3_cell") \
    .pivot("priority", priorities) \
    .agg(first("charger_id")) 

nearest_chargers = nearest_chargers.withColumnRenamed("1","priority_1") \
    .withColumnRenamed("2","priority_2") \
    .withColumnRenamed("3","priority_3")


# COMMAND ----------

# Join with charger details to get the details of the nearest charging stations

nearest_chargers_1 = nearest_chargers.join(
    chargers, 
    nearest_chargers.priority_1 == chargers.charger_id, 
    'left'
).select(
    'LocationID',
    'h3_cell',
    'priority_1',
    'priority_2',
    'priority_3',
    col('station_name').alias('P1_charger_station_name'),
    col('lat').alias('P1_charger_lat'),
    col('lon').alias('P1_charger_lon'),
    col('fast_chargers').alias('P1_charger_fast_chargers'),
    col('level2_chargers').alias('P1_charger_level2_chargers'),
    col('charger_h3').alias('P1_charger_h3')
)


# COMMAND ----------

nearest_chargers_2 = nearest_chargers_1.join(
    chargers, 
    nearest_chargers_1.priority_2 == chargers.charger_id, 
    'left'
).select(
    'LocationID',
    'h3_cell',
    'priority_1',
    'priority_2',
    'priority_3',
    'P1_charger_station_name',
    'P1_charger_lat',
    'P1_charger_lon',
    'P1_charger_fast_chargers',
    'P1_charger_level2_chargers',
    'P1_charger_h3',
    col('station_name').alias('P2_charger_station_name'),
    col('lat').alias('P2_charger_lat'),
    col('lon').alias('P2_charger_lon'),
    col('fast_chargers').alias('P2_charger_fast_chargers'),
    col('level2_chargers').alias('P2_charger_level2_chargers'),
    col('charger_h3').alias('P2_charger_h3')    
)


# COMMAND ----------

nearest_chargers_3 = nearest_chargers_2.join(
    chargers, 
    nearest_chargers_2.priority_3 == chargers.charger_id, 
    'left'
).select(
    'LocationID',
    'h3_cell',
    'priority_1',
    'priority_2',
    'priority_3',
    'P1_charger_station_name',
    'P1_charger_lat',
    'P1_charger_lon',
    'P1_charger_fast_chargers',
    'P1_charger_level2_chargers',
    'P1_charger_h3',
    'P2_charger_station_name',
    'P2_charger_lat',
    'P2_charger_lon',
    'P2_charger_fast_chargers',
    'P2_charger_level2_chargers',
    'P2_charger_h3',
    col('station_name').alias('P3_charger_station_name'),
    col('lat').alias('P3_charger_lat'),
    col('lon').alias('P3_charger_lon'),
    col('fast_chargers').alias('P3_charger_fast_chargers'),
    col('level2_chargers').alias('P3_charger_level2_chargers'),
    col('charger_h3').alias('P3_charger_h3')    
)


# COMMAND ----------

nearest_chargers_3.write.format('delta').mode("overwrite").save('s3a://nyc-lakehouse/reference/zone_charger_mapping')

# COMMAND ----------

# Register as table
spark.sql("""
CREATE TABLE IF NOT EXISTS nearest_chargers
USING DELTA
LOCATION 's3a://nyc-lakehouse/reference/zone_charger_mapping'
""")