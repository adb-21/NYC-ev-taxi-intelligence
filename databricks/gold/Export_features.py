# Databricks notebook source
# Export tables as parquet to Databricks Volume
# Run this as a Databricks notebook cell

base_path = "/Volumes/workspace/default/nyc_reference_volume/export"

df_recommendations = spark.read.format("delta").table('recommendations')
df_typical_demand_lookup = spark.read.format("delta").table('typical_demand_lookup') 
df_nearest_chargers =  spark.read.format("delta").table('nearest_chargers')
df_chargers = spark.read.format("delta").table('ny_chargers')

df_recommendations.write.mode("overwrite") \
        .partitionBy("hour_of_day", "day_of_week") \
        .parquet(f"{base_path}/recommendations")
     
df_typical_demand_lookup.write.mode("overwrite") \
        .partitionBy("hour_of_day", "day_of_week") \
        .parquet(f"{base_path}/typical_demand_lookup")

df_chargers.write.mode("overwrite") \
        .parquet(f"{base_path}/ny_chargers")


df_nearest_chargers.write.mode("overwrite") \
        .parquet(f"{base_path}/nearest_chargers")



print("\nAll exports complete.")

