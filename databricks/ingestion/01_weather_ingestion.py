# Databricks notebook source
#pip install openmeteo-requests requests-cache pandas retry-requests

# COMMAND ----------

from pyspark.sql.functions import udf, explode, col, lit, when
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, ArrayType, TimestampType
import pandas as pd
import openmeteo_requests
import requests_cache
from retry_requests import retry
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


# COMMAND ----------

# Define the location data
location_data = [
    ("Manhattan", 40.7812, -73.9665),
    ("Queens", 40.7769, -73.8740),
    ("Brooklyn", 40.6782, -73.9442),
    ("Bronx", 40.8501, -73.8662),
    ("Staten Island", 40.5795, -74.1502),
    ("EWR", 40.6895, -74.1745)
]

schema = StructType([
    StructField("borough", StringType(), False),
    StructField("lat", DoubleType(), False),
    StructField("lon", DoubleType(), False)
])

df_locations = spark.createDataFrame(location_data, schema)

# COMMAND ----------

# Puspose: Fetch weather data for each location in the 'df_locations' DataFrame using the Open-Meteo API. The function should return a list of tuples containing the time, temperature, precipitation, rain, snow, and wind speed for each hour of the year for the given location.

def fetch_weather_batch(lat, lon):
   
    # Use a standard session instead of a file-based cache
    session = requests.Session()
    retries = Retry(total=5, backoff_factor=0.2, status_forcelist=[500, 502, 503, 504])
    session.mount('https://', HTTPAdapter(max_retries=retries))
    
    # Initialize the Open-Meteo client with the standard session
    openmeteo = openmeteo_requests.Client(session=session)

    url = "https://archive-api.open-meteo.com/v1/archive"
    params = {
        "latitude": lat,
        "longitude": lon,
        "start_date": "2023-01-01",
        "end_date": "2025-12-31",
        "hourly": ["temperature_2m", "precipitation", "rain", "snowfall", "wind_speed_10m"],
        "timezone": "America/New_York"
    }
    
    try:
        responses = openmeteo.weather_api(url, params=params)
        response = responses[0]
        hourly = response.Hourly()
        
        times = pd.date_range(
            start=pd.to_datetime(hourly.Time(), unit="s", utc=True),
            end=pd.to_datetime(hourly.TimeEnd(), unit="s", utc=True),
            freq=pd.Timedelta(seconds=hourly.Interval()),
            inclusive="left"
        )
        
        return list(zip(
            times.to_pydatetime().tolist(),
            [float(x) for x in hourly.Variables(0).ValuesAsNumpy()],
            [float(x) for x in hourly.Variables(1).ValuesAsNumpy()],
            [float(x) for x in hourly.Variables(2).ValuesAsNumpy()],
            [float(x) for x in hourly.Variables(3).ValuesAsNumpy()],
            [float(x) for x in hourly.Variables(4).ValuesAsNumpy()]
        ))
    except Exception as e:
        # Return empty list if a specific borough fails
        return []

# COMMAND ----------

# Define the schema for the weather data

weather_row_schema = ArrayType(StructType([
    StructField("time", TimestampType()),
    StructField("temp_2m", DoubleType()),
    StructField("precip", DoubleType()),
    StructField("rain", DoubleType()),
    StructField("snow", DoubleType()),
    StructField("wind", DoubleType())
]))

# Register as UDF
fetch_weather_udf = udf(fetch_weather_batch, weather_row_schema)

# COMMAND ----------

# Apply the UDF to the 'df_locations' DataFrame to fetch weather data for each location

weather_df = df_locations.withColumn("weather_stats", fetch_weather_udf(col("lat"), col("lon")))

# Explode the array into individual rows
final_weather_df = weather_df.select(
    col("borough"),
    explode(col("weather_stats")).alias("data")
).select(
    "borough",
    "data.time",
    "data.temp_2m",
    "data.precip",
    "data.rain",
    "data.snow",
    "data.wind"
)

# COMMAND ----------

# Add additional columns to the 'final_weather_df' DataFrame

final_weather_df = final_weather_df \
    .withColumn("is_raining", 
        when(col("rain") > 0.1, 1).otherwise(0) # 0.1mm is a standard 'measurable' rain threshold
    ) \
    .withColumn("is_snowing", 
        when(col("snow") > 0.1, 1).otherwise(0) # 0.1cm threshold for snow
    ) \
    .withColumn("weather_type",
        when(col("snow") > 0.1, "Snow")
        .when(col("rain") > 0.1, "Rain")
        .otherwise("Clear")
    ) \
    .withColumn("temp_bucket",
        when(col("temp_2m") <= 0, "Freezing")      # Below or at 0°C
        .when(col("temp_2m") < 15, "Cold")         # 0°C to 15°C
        .when(col("temp_2m") < 25, "Moderate")     # 15°C to 25°C
        .otherwise("Hot")                          # Above 25°C
    )

# COMMAND ----------

# Write the DataFrame to a Delta table

final_weather_df.write.format('delta').mode("overwrite").save('s3a://nyc-lakehouse/reference/weather')

# COMMAND ----------

# Register as table
spark.sql("""
CREATE TABLE IF NOT EXISTS weather
USING DELTA
LOCATION 's3a://nyc-lakehouse/reference/weather'
""")