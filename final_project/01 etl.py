# Databricks notebook source
# MAGIC %run ./includes/includes

# COMMAND ----------

# start_date = str(dbutils.widgets.get('01.start_date'))
# end_date = str(dbutils.widgets.get('02.end_date'))
# hours_to_forecast = int(dbutils.widgets.get('03.hours_to_forecast'))
# promote_model = bool(True if str(dbutils.widgets.get('04.promote_model')).lower() == 'yes' else False)

# print(start_date,end_date,hours_to_forecast, promote_model)
# print("YOUR CODE HERE...")

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
    .trigger(availableNow=True)
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
    .trigger(availableNow=True)
    .start()
)

# Wait for the stream to finish
query.awaitTermination()

# COMMAND ----------

files=dbutils.fs.ls("dbfs:/FileStore/tables/G07")
count=0
for file in files:
    # count+=1
# print(count)
    print(file.name)

# COMMAND ----------

delta_path = "dbfs:/FileStore/tables/G07/bronze/bike_trips_history"
spark.read.format("delta").load(delta_path).createOrReplaceTempView("bike_trip_history_delta")

# # Display filtered data
# display(df_g07.head(5))  

# # Display count of dataframe
# df_g07.count()

# COMMAND ----------

df_g07 = spark.sql("""
  SELECT count(*) 
  FROM bike_trip_history_delta 
  WHERE start_station_name = 'Broadway & W 25 St'
""")
display(df_g07)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT min(started_at) as started_at,max(started_at) as ended_date
# MAGIC FROM bike_trip_history_delta 
# MAGIC WHERE start_station_name = 'Broadway & W 25 St'

# COMMAND ----------

# delta_table_name = 'historic_bike_trip_g07'
# df_g07.write.format("delta").mode("overwrite").option("overwriteSchema", "true").option("path", GROUP_DATA_PATH + "silver"+ delta_table_name).saveAsTable(delta_table_name)

# COMMAND ----------

delta_path = "dbfs:/FileStore/tables/G07/bronze/weather_history"
spark.read.format("delta").load(delta_path).createOrReplaceTempView("weather_history_delta")

# df_weather_g07 = spark.sql("""
#   SELECT *
#   FROM weather_history_delta
# """)

# display(df_weather_g07)

# COMMAND ----------

from pyspark.sql.functions import *

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Below Query checks for duplicates in weather data
# MAGIC
# MAGIC -- SELECT from_unixtime(dt) as abc,count(*)
# MAGIC -- FROM weather_history_delta 
# MAGIC -- GROUP BY from_unixtime(dt)
# MAGIC -- HAVING count(*)>1
# MAGIC
# MAGIC --below weather dates has duplicates in the data
# MAGIC -- 2022-10-30 16:00:00
# MAGIC -- 2022-10-30 17:00:00
# MAGIC -- 2022-10-30 19:00:00
# MAGIC -- 2022-10-30 18:00:00
# MAGIC
# MAGIC SELECT from_unixtime(dt) as abc,*
# MAGIC FROM weather_history_delta 
# MAGIC where DATE(from_unixtime(dt)) = '2022-11-05'

# COMMAND ----------

## Creating Duplicate Free, Clean Weather Table
clean_weather_df = spark.sql(
"""
with cte as(
SELECT from_unixtime(dt) as parsed_dt,*,
row_number() over (PARTITION BY from_unixtime(dt) ORDER BY id desc) as rnum
FROM weather_history_delta 
--where from_unixtime(dt) = '2022-10-30 19:00:00'
)
select parsed_dt, temp, feels_like, pressure, humidity, dew_point, uvi, clouds, visibility, wind_speed, wind_deg, pop, snow_1h, id, main, description, icon, loc, lat, lon, timezone, timezone_offset
from cte
where rnum=1
"""
)
clean_weather_df.createOrReplaceTempView("clean_weather_history_delta")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT min(parsed_dt) as started_at,max(parsed_dt) as ended_date, count(*)
# MAGIC FROM clean_weather_history_delta 

# COMMAND ----------

# MAGIC %sql
# MAGIC --- Checking lon, lat for weather station
# MAGIC SELECT DISTINCT lat, lon
# MAGIC FROM clean_weather_history_delta 

# COMMAND ----------

# delta_table_name = 'weather_g07'
# df_weather_g07.write.format("delta").mode("overwrite").option("overwriteSchema", "true").option("path", GROUP_DATA_PATH + "silver"+ delta_table_name).saveAsTable(delta_table_name)

# COMMAND ----------

bike_weather_df=spark.sql("""SELECT parsed_dt as weather_date,* 
FROM bike_trip_history_delta as tab1 
LEFT JOIN clean_weather_history_delta as tab2
ON DATE(tab1.started_at) = DATE(parsed_dt) 
AND HOUR(tab1.started_at) = HOUR(parsed_dt)
WHERE start_station_name = 'Broadway & W 25 St'""")

# COMMAND ----------

# MAGIC %sql
# MAGIC --checking if all weather data is available or we are getting any NULLS
# MAGIC
# MAGIC SELECT min(tab1.started_at), max(tab1.started_at) 
# MAGIC FROM bike_trip_history_delta as tab1 
# MAGIC LEFT JOIN clean_weather_history_delta as tab2
# MAGIC ON DATE(tab1.started_at) = DATE(parsed_dt) 
# MAGIC AND HOUR(tab1.started_at) = HOUR(parsed_dt)
# MAGIC WHERE start_station_name = 'Broadway & W 25 St'
# MAGIC AND tab2.parsed_dt is NULL

# COMMAND ----------

# MAGIC %sql
# MAGIC --checking if all weather data is available or we are getting any NULLS
# MAGIC
# MAGIC SELECT distinct(DATE(tab1.started_at)) --min(tab1.started_at), max(tab1.started_at), count(*) 
# MAGIC FROM bike_trip_history_delta as tab1 
# MAGIC LEFT JOIN clean_weather_history_delta as tab2
# MAGIC ON DATE(tab1.started_at) = DATE(parsed_dt) 
# MAGIC AND HOUR(tab1.started_at) = HOUR(parsed_dt)
# MAGIC WHERE start_station_name = 'Broadway & W 25 St'
# MAGIC AND tab2.parsed_dt is null
# MAGIC AND DATE(tab1.started_at) >= '2021-11-20'
# MAGIC ORDER BY DATE(started_at)
# MAGIC
# MAGIC -- select DISTINCT DATE(started_at)
# MAGIC -- FROM bike_trip_history_delta
# MAGIC -- WHERE start_station_name = 'Broadway & W 25 St'
# MAGIC -- order by DATE(started_at)

# COMMAND ----------

## Our code for calling weather API
def executeRestApi(verb, url):
  body = json.dumps({})
  print("***")
  headers = {
      'content-type': "application/json"
  }  
  res = None
  print("***")
  # Make API request, get response object back, create dataframe from above schema.
  try:
    print("wdw")
    if verb == "get":
      res = requests.get(url, data=body, headers=headers)
      print("inside get")
    else:
      res = requests.post(url, data=body, headers=headers)
      print("inside post")
    print(res)
  except Exception as e:
    print("***")
    return e  
    if res != None and res.status_code == 200:
        return json.loads(res.text)  
    return None

# COMMAND ----------

# try2=executeRestApi("get","http://api.openweathermap.org/data/2.5/onecall/timemachine?lat=33.44&lon=-94.04&exclude=hourly,daily&appid=2e8cb9a16f0c29402d9dc70ee400d311")

try2 = executeRestApi("get",'https://api.openweathermap.org/data/2.5/onecall/timemachine?lat=60&lon=30&dt=1683177254&appid=2e8cb9a16f0c29402d9dc70ee400d311&only_current={true}')

display(try2)

# COMMAND ----------

delta_table_name = 'bike_weather_g07'
bike_weather_df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").option("path", GROUP_DATA_PATH + "silver"+ delta_table_name).saveAsTable(delta_table_name)

# COMMAND ----------

import json

# Return Success
dbutils.notebook.exit(json.dumps({"exit_code": "OK"}))
