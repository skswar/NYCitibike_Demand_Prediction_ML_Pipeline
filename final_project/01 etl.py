# Databricks notebook source
# MAGIC %run ./includes/includes

# COMMAND ----------

start_date = str(dbutils.widgets.get('01.start_date'))
end_date = str(dbutils.widgets.get('02.end_date'))
hours_to_forecast = int(dbutils.widgets.get('03.hours_to_forecast'))
promote_model = bool(True if str(dbutils.widgets.get('04.promote_model')).lower() == 'yes' else False)

print(start_date,end_date,hours_to_forecast, promote_model)
print("YOUR CODE HERE...")

# COMMAND ----------

# print("hello")

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType, TimestampType, DoubleType

# COMMAND ----------

bike_schema = StructType([
  StructField("ride_id", StringType(), True),
  StructField("rideable_type", StringType(), True),
  StructField("started_at", StringType(), True),
  StructField("ended_at", StringType(), True),
  StructField("start_station_name", StringType(), True),
  StructField("start_station_id", StringType(), True),
  StructField("end_station_name", StringType(), True),
  StructField("end_station_id", StringType(), True),
  StructField("start_lat", StringType(), True),
  StructField("start_lng", StringType(), True),
  StructField("end_lat", StringType(), True),
  StructField("end_lng", StringType(), True),
  StructField("member_casual", StringType(), True)
  
# ride_id:string
# rideable_type:string
# started_at:string
# ended_at:string
# start_station_name:string
# start_station_id:string
# end_station_name:string
# end_station_id:string
# start_lat:string
# start_lng:string
# end_lat:string
# end_lng:string
# member_casual:string
  # more fields as needed
])

# COMMAND ----------

input_path = "dbfs:/FileStore/tables/raw/bike_trips/"
output_path = "dbfs:/FileStore/tables/G07/bronze/bike_trips_history"

# COMMAND ----------

query = (
    spark
    .readStream
    .format("csv")
    .schema(bike_schema)  # specify the schema for the data
    .option("header", "true")  # specify if the file has a header row
    .load(input_path)
    .writeStream
    .format("delta")
    .option("path", output_path)
    .option("checkpointLocation", output_path + "/checkpoint")
    .start()
)

# Wait for the stream to finish
query.awaitTermination()

# COMMAND ----------

weather_schema = StructType([
  StructField("dt", IntegerType(), True),
  StructField("temp", DoubleType(), True),
  StructField("feels_like", DoubleType(), True),
  StructField("pressure", IntegerType(), True),
  StructField("humidity", IntegerType(), True),
  StructField("dew_point", DoubleType(), True),
  StructField("uvi", DoubleType(), True),
  StructField("clouds", IntegerType(), True),
  StructField("visibility", IntegerType(), True),
  StructField("wind_speed", DoubleType(), True),
  StructField("wind_deg", IntegerType(), True),
  StructField("pop", DoubleType(), True),
  StructField("snow_1h", DoubleType(), True),
  StructField("id", IntegerType(), True),
  StructField("main", StringType(), True),
  StructField("description", StringType(), True),
  StructField("icon", StringType(), True),
  StructField("loc", StringType(), True),
  StructField("lat", DoubleType(), True),
  StructField("lon", DoubleType(), True),  
  StructField("timezone", StringType(), True),
  StructField("timezone_offset", IntegerType(), True),  
# dt:integer
# temp:double
# feels_like:double
# pressure:integer
# humidity:integer
# dew_point:double
# uvi:double
# clouds:integer
# visibility:integer
# wind_speed:double
# wind_deg:integer
# pop:double
# snow_1h:double
# id:integer
# main:string
# description:string
# icon:string
# loc:string
# lat:double
# lon:double
# timezone:string
# timezone_offset:integer
  # more fields as needed
])

# COMMAND ----------

input_path2 = "dbfs:/FileStore/tables/raw/weather/"
output_path2 = "dbfs:/FileStore/tables/G07/bronze/weather_history"

# COMMAND ----------

query = (
    spark
    .readStream
    .format("csv")
    .schema(weather_schema)  # specify the schema for the data
    .option("header", "true")  # specify if the file has a header row
    .load(input_path2)
    .writeStream
    .format("delta")
    .option("path", output_path2)
    .option("checkpointLocation", output_path2 + "/checkpoint")
    .start()
)

# Wait for the stream to finish
query.awaitTermination()

# COMMAND ----------

files=dbutils.fs.ls("dbfs:/FileStore/tables/G07")
# count=0
for file in files:
#     count+=1
# print(count)
    print(file.name)

# COMMAND ----------

import json

# Return Success
dbutils.notebook.exit(json.dumps({"exit_code": "OK"}))
