# Databricks notebook source
#pip install geopandas "h3<4" 


# COMMAND ----------

import geopandas as gpd
import h3
from shapely.geometry import Polygon, MultiPolygon, Point
import random
from pyspark.sql.functions import *
from pyspark.sql.types import StructType,StructField,DoubleType

# COMMAND ----------

#Read taxi zone shape files
zones = gpd.read_file("/Volumes/workspace/default/nyc_reference_volume/taxi_zones_dir/taxi_zones.shp") 

# Convert to lat/lon
zones = zones.to_crs(epsg=4326)

# COMMAND ----------

zones_res9 = zones[['LocationID','geometry']]
zones_res8 = zones[['LocationID','geometry']]

# COMMAND ----------

# Function to convert polygon to H3
def polygon_to_h3_res_9(geometry, resolution=9):
    
    h3_cells = set()

    if isinstance(geometry, Polygon):
        h3_cells.update(
            h3.polyfill(
                geometry.__geo_interface__,
                resolution,
                geo_json_conformant=True
            )
        )

    elif isinstance(geometry, MultiPolygon):
        for poly in geometry.geoms:
            h3_cells.update(
                h3.polyfill(
                    poly.__geo_interface__,
                    resolution,
                    geo_json_conformant=True
                )
            )
    
    # fallback if empty
    if len(h3_cells) == 0:
        centroid = geometry.centroid
        h3_cells.add(h3.geo_to_h3(centroid.y, centroid.x, resolution))

    return list(h3_cells)

# COMMAND ----------

# Function to convert polygon to H3
def polygon_to_h3_res_8(geometry, resolution=8):
    
    h3_cells = set()

    if isinstance(geometry, Polygon):
        h3_cells.update(
            h3.polyfill(
                geometry.__geo_interface__,
                resolution,
                geo_json_conformant=True
            )
        )

    elif isinstance(geometry, MultiPolygon):
        for poly in geometry.geoms:
            h3_cells.update(
                h3.polyfill(
                    poly.__geo_interface__,
                    resolution,
                    geo_json_conformant=True
                )
            )

    # fallback if empty
    if len(h3_cells) == 0:
        centroid = geometry.centroid
        h3_cells.add(h3.geo_to_h3(centroid.y, centroid.x, resolution))

    return list(h3_cells)

# COMMAND ----------

# Add H3 cells to the dataframe
zones_res8['h3_cells'] = [
    polygon_to_h3_res_8(geom, 8)
    for geom in zones.geometry
]

# COMMAND ----------

# Add H3 cells to the dataframe
zones_res9['h3_cells'] = [
    polygon_to_h3_res_9(geom, 9)
    for geom in zones.geometry
]

# COMMAND ----------

# Convert to Spark DataFrame
df_zones_8 = spark.createDataFrame(zones_res8[['LocationID','h3_cells']])
df_zones_9 = spark.createDataFrame(zones_res9[['LocationID','h3_cells']])

# COMMAND ----------

#Store the dataframe as a parquet file
df_zones_8.write.format("delta") \
    .mode("overwrite") \
    .save("s3a://nyc-lakehouse/reference/h3_mappings/resolution_8")

df_zones_9.write.format("delta") \
    .mode("overwrite") \
    .save("s3a://nyc-lakehouse/reference/h3_mappings/resolution_9")


# COMMAND ----------

spark.sql("""
CREATE TABLE IF NOT EXISTS h3_mappings_resolution_8
USING DELTA
LOCATION 's3a://nyc-lakehouse/reference/h3_mappings/resolution_8'
""")

spark.sql("""
CREATE TABLE IF NOT EXISTS h3_mappings_resolution_9
USING DELTA
LOCATION 's3a://nyc-lakehouse/reference/h3_mappings/resolution_9'
""")