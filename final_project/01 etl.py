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

# MAGIC %md
# MAGIC #### Importing All the required Libraries

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType, TimestampType, DoubleType
from pyspark.sql.functions import *
from pyspark.sql.types import TimestampType

# COMMAND ----------

# MAGIC %md
# MAGIC #### Building schema for the columns of Bike Trips

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
  StructField("member_casual", StringType(), True)])

# COMMAND ----------

# MAGIC %md
# MAGIC #### Setting the input/output paths for the history bike trips data

# COMMAND ----------

input_path = "dbfs:/FileStore/tables/raw/bike_trips/"
output_path = "dbfs:/FileStore/tables/G07/bronze/bike_trips_history"

# COMMAND ----------

# MAGIC %md
# MAGIC #### Loading the history bike trip data and saving in a delta format

# COMMAND ----------

query = (
    spark
    .readStream
    .format("csv")
    .schema(bike_schema)  
    .option("header", "true")
    .load(input_path)
    .writeStream
    .format("delta")
    .option("path", output_path)
    .option("checkpointLocation", output_path + "/checkpoint")
    .trigger(availableNow=True)
    .start()
)
query.awaitTermination()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Building schema for the columns of weather data

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
  StructField("timezone_offset", IntegerType(), True)
])

# COMMAND ----------

# MAGIC %md
# MAGIC #### Setting the input/output paths for the weather data

# COMMAND ----------

input_path2 = "dbfs:/FileStore/tables/raw/weather/"
output_path2 = "dbfs:/FileStore/tables/G07/bronze/weather_history"

# COMMAND ----------

# MAGIC %md
# MAGIC #### Loading the weather data and saving in a delta format

# COMMAND ----------

query = (
    spark
    .readStream
    .format("csv")
    .schema(weather_schema)  
    .option("header", "true") 
    .load(input_path2)
    .writeStream
    .format("delta")
    .option("path", output_path2)
    .option("checkpointLocation", output_path2 + "/checkpoint")
    .trigger(availableNow=True)
    .start()
)
query.awaitTermination()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Updating Shuffle Paritions

# COMMAND ----------

spark.conf.set("spark.sql.shuffle.partitions", spark.sparkContext.defaultParallelism)
print(spark.conf.get("spark.sql.shuffle.partitions"))

# COMMAND ----------

# MAGIC %md
# MAGIC #### Listing the files present in our directory to check how it looks

# COMMAND ----------

files=dbutils.fs.ls("dbfs:/FileStore/tables/G07")
for file in files:
    print(file.name)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Reading bike trips data from the path where we stored the bronze files and creating a SQL Temp for reading the data

# COMMAND ----------

delta_path = "dbfs:/FileStore/tables/G07/bronze/bike_trips_history"
spark.read.format("delta").load(delta_path).createOrReplaceTempView("bike_trip_history_delta")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Analyzing the minimum and maximum available dates in our data which is from "2021-11-01 00:00:01" to "2023-03-31 23:59:57". Also confirming the count. There is 40399055 unique ride id count. This mataches with 40399055 total rides count. Thus ride_id is a unique identifier.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT min(started_at) as started_at,max(started_at) as ended_date, count(*) as totaldata
# MAGIC FROM bike_trip_history_delta

# COMMAND ----------

# MAGIC %md
# MAGIC #### Creating a temp in order to have a date column with hours data parsed only. This column will be used later for grouping bike trips data and get counts. We are pulling only our station data where our station is either the start or end.

# COMMAND ----------

spark.sql(
"""
select
date_format(date_trunc('hour',started_at),'yyyy-MM-dd HH:mm:ss') as startdate,
date_format(date_trunc('hour',ended_at),'yyyy-MM-dd HH:mm:ss') as enddate,
*
from bike_trip_history_delta
where start_station_name = "Broadway & W 25 St"
OR end_station_name = "Broadway & W 25 St"
"""
).createOrReplaceTempView("bike_trip_history_delta_2")


# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT min(started_at) as started_at,max(started_at) as ended_date, count(*) as totaldata
# MAGIC FROM bike_trip_history_delta_2

# COMMAND ----------

# MAGIC %md
# MAGIC #### Here we are calculating the number of bike trips taking place in an hour. We are creating calculated columns which holds values such as number of bikes left our station at a given hour and came back to our station at a given hour. We are also takig the net difference of the bikes. This net ifference is what we are going to predict in our forecast model.

# COMMAND ----------

bike_trips_df = spark.sql(
"""
with cte1 as(
select startdate, sum(case when start_station_name="Broadway & W 25 St" then 1 else 0 end) tripstarted_at_our_station
from bike_trip_history_delta_2
group by startdate)
,cte2 AS (
select enddate, sum(case when end_station_name="Broadway & W 25 St" then 1 else 0 end) tripend_at_our_station
from bike_trip_history_delta_2
group by enddate)
select startdate, tripstarted_at_our_station bike_leaving_our_station, tripend_at_our_station bike_starting_our_station,
(tripend_at_our_station-tripstarted_at_our_station) as net_trip_difference
from cte1 as tab1
inner join cte2 as tab2
on tab1.startdate=tab2.enddate
order by tab1.startdate
"""
)

# COMMAND ----------

bike_trips_df.createOrReplaceTempView("bike_trips_count_delta")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Here we are analyzing for different dates whether our code written above is working fine

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from
# MAGIC bike_trip_history_delta_2
# MAGIC where date(started_at) = "2021-11-17"
# MAGIC and hour(started_at) = 8
# MAGIC and start_station_name="Broadway & W 25 St" 
# MAGIC order by started_at

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from bike_trip_history_delta_2
# MAGIC order by startdate

# COMMAND ----------

# MAGIC %md
# MAGIC #### We are creating a silver table from the RAW Bronze data of bike trips. This silver biketrip count table will hold the number of trips taking place at a given hour.

# COMMAND ----------

delta_table_name = 'biketrips_count_g07'
bike_trips_df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").option("path", GROUP_DATA_PATH + "silver"+ delta_table_name).saveAsTable(delta_table_name)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Reading bike trips data from the path where we stored the bronze files and creating a SQL Temp for reading the data

# COMMAND ----------

delta_path = "dbfs:/FileStore/tables/G07/bronze/weather_history"
spark.read.format("delta").load(delta_path).createOrReplaceTempView("weather_history_delta")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Below Query checks for duplicates in weather data, and other weather data related analysis

# COMMAND ----------

# MAGIC %sql
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
# MAGIC SELECT from_unixtime(dt) as dt,*
# MAGIC FROM weather_history_delta 
# MAGIC where DATE(from_unixtime(dt)) = '2022-06-04'
# MAGIC order by dt

# COMMAND ----------

# MAGIC %md
# MAGIC #### Creating Duplicate Free, Clean Weather Table. This step also creates a SQL temp table "clean_weather_history_delta"

# COMMAND ----------

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

# MAGIC %md
# MAGIC #### Following few code chunks analyzes the weather data for making decisions on how to precprocess it

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT min(parsed_dt) as started_at,max(parsed_dt) as ended_date, count(*)
# MAGIC FROM clean_weather_history_delta 

# COMMAND ----------

# MAGIC %sql
# MAGIC --- Checking lon, lat for weather station
# MAGIC SELECT DISTINCT lat, lon, timezone, timezone_offset, loc
# MAGIC FROM clean_weather_history_delta 

# COMMAND ----------

# MAGIC %sql
# MAGIC --checking if all weather data is available or we are getting any NULLS
# MAGIC SELECT min(tab1.started_at), max(tab1.started_at) 
# MAGIC FROM bike_trip_history_delta as tab1 
# MAGIC LEFT JOIN clean_weather_history_delta as tab2
# MAGIC ON DATE(tab1.started_at) = DATE(parsed_dt) 
# MAGIC AND HOUR(tab1.started_at) = HOUR(parsed_dt)
# MAGIC WHERE tab2.parsed_dt is NULL

# COMMAND ----------

# MAGIC %sql
# MAGIC --checking if all weather data is available or we are getting any NULLS
# MAGIC SELECT distinct(DATE(tab1.started_at))
# MAGIC FROM bike_trip_history_delta as tab1 
# MAGIC LEFT JOIN clean_weather_history_delta as tab2
# MAGIC ON DATE(tab1.started_at) = DATE(parsed_dt) 
# MAGIC AND HOUR(tab1.started_at) = HOUR(parsed_dt)
# MAGIC WHERE start_station_name = 'Broadway & W 25 St'
# MAGIC AND tab2.parsed_dt is null
# MAGIC AND DATE(tab1.started_at) >= '2021-11-20'
# MAGIC ORDER BY DATE(started_at)

# COMMAND ----------

# MAGIC %md
# MAGIC #### The following code pulls the data of weathers from OpeweatherAPI. The idea is to pull the missing weather data from the API which might be useful in building the forecasting model

# COMMAND ----------

import requests, json

BASE_URL = "https://history.openweathermap.org/data/2.5/history/city?"
lat = "40.712"
lon = "-74.006"

URL = BASE_URL + "lat=" + lat + "&lon=" + lon + "&type=hour&start=1667260800&end=1667865600" + "&appid=" + "10db4449c9624126b288cedc8a5cca2d"
response = requests.get(URL).json()

URL = BASE_URL + "lat=" + lat + "&lon=" + lon + "&type=hour&start=1654362000&end=1654376400" + "&appid=" + "10db4449c9624126b288cedc8a5cca2d"
response2 = requests.get(URL).json()

# COMMAND ----------

# MAGIC %md
# MAGIC #### The following code is for Parsing the json weather request received from the API

# COMMAND ----------

loc="NYC"
lat=40.712
lon=-74.006
timezone="America/New_York"
timezone_offset=-14400

list_weather = list()

for i in range(len(response['list'])):
    dt=0
    temp=0
    feels_like=0
    pressure=0
    humidity=0
    dew_point=0
    uvi=0
    clouds=0
    visibility=0
    wind_speed=0
    wind_deg=0
    pop=0
    snow_1h=0
    id=0
    main=0
    description=0
    icon=0

    dt = response['list'][i]['dt']
    temp = response['list'][i]['main']['temp']+0.0
    feels_like = response['list'][i]['main']['feels_like']
    pressure = response['list'][i]['main']['pressure']
    humidity = response['list'][i]['main']['humidity']
    dew_point=0.0
    uvi=0.0
    clouds = response['list'][i]['clouds']['all']
    wind_speed = response['list'][i]['wind']['speed']+0.0
    wind_deg = response['list'][i]['wind']['deg']
    pop=0.0
    snow_1h=0.0
    id = response['list'][i]['weather'][0]['id']
    main = response['list'][i]['weather'][0]['main']
    description = response['list'][i]['weather'][0]['description']
    icon = response['list'][i]['weather'][0]['icon']
    visibility=0

    list_weather.append([dt,temp,feels_like,pressure,humidity,dew_point,uvi,clouds,visibility,wind_speed,wind_deg,pop,snow_1h,id,main,description,icon,
    loc,lat,lon,timezone,timezone_offset])

for i in range(len(response2['list'])):
    dt=0
    temp=0
    feels_like=0
    pressure=0
    humidity=0
    dew_point=0
    uvi=0
    clouds=0
    visibility=0
    wind_speed=0
    wind_deg=0
    pop=0
    snow_1h=0
    id=0
    main=0
    description=0
    icon=0

    dt = response2['list'][i]['dt']
    temp = response2['list'][i]['main']['temp']+0.0
    feels_like = response2['list'][i]['main']['feels_like']+0.0
    pressure = response2['list'][i]['main']['pressure']
    humidity = response2['list'][i]['main']['humidity']
    dew_point=0.0
    uvi=0.0
    clouds = response2['list'][i]['clouds']['all']
    wind_speed = response2['list'][i]['wind']['speed']+0.0
    wind_deg = response2['list'][i]['wind']['deg']
    pop=0.0
    snow_1h=0.0
    id = response2['list'][i]['weather'][0]['id']
    main = response2['list'][i]['weather'][0]['main']
    description = response2['list'][i]['weather'][0]['description']
    icon = response2['list'][i]['weather'][0]['icon']
    visibility=0

    list_weather.append([dt,temp,feels_like,pressure,humidity,dew_point,uvi,clouds,visibility,wind_speed,wind_deg,pop,snow_1h,id,main,description,icon,
    loc,lat,lon,timezone,timezone_offset])

# COMMAND ----------

# MAGIC %md
# MAGIC #### Creating a union fo newly pulled weather data and adding it to the history table.

# COMMAND ----------

weather_api_df = spark.createDataFrame(data=list_weather, schema = weather_schema)
display(weather_api_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Creating Temp view for the union data created in the previous step. Also testing all distinct data pulled from the wether api to make sure we are pulling required information

# COMMAND ----------

weather_api_df.createOrReplaceTempView("weather_api_df_delta")

# COMMAND ----------

# MAGIC %sql
# MAGIC select distinct(from_unixtime(dt)) as dt
# MAGIC FROM weather_api_df_delta
# MAGIC order by dt

# COMMAND ----------

# MAGIC %md
# MAGIC #### Here we are converting the weather date to a proper datetime format to be used further. In the following code chunk we are creating the union of history weather data and api pulled weather data

# COMMAND ----------

weather_api_df_stg1 = spark.sql(
"""
with cte as(
SELECT from_unixtime(dt) as parsed_dt,*
FROM weather_api_df_delta
)
select parsed_dt, temp, feels_like, pressure, humidity, dew_point, uvi, clouds, visibility, wind_speed, wind_deg, pop, snow_1h, id, main, description, icon, loc, lat, lon, timezone, timezone_offset
from cte
"""
)

# COMMAND ----------

weather_final_df = clean_weather_df.union(weather_api_df_stg1)
weather_final_df.createOrReplaceTempView("weather_final_df_delta")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Removing any duplicate timestamps from weather data. After that making a SQL Temp table for using further. Then we are checking again if still duplicates persists.

# COMMAND ----------

clean_weather_final_df_delta = spark.sql(
"""
with cte as(
SELECT *,
row_number() over (PARTITION BY parsed_dt ORDER BY id desc) as rnum
FROM weather_final_df_delta 
)
select parsed_dt, temp, feels_like, pressure, humidity, dew_point, uvi, clouds, visibility, wind_speed, wind_deg, pop, snow_1h, id, main, description, icon, loc, lat, lon, timezone, timezone_offset
from cte
where rnum=1
"""
)
clean_weather_final_df_delta.createOrReplaceTempView("clean_weather_final_df_delta_delta")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT parsed_dt as abc,count(*)
# MAGIC FROM clean_weather_final_df_delta_delta 
# MAGIC GROUP BY parsed_dt
# MAGIC HAVING count(*)>1

# COMMAND ----------

# MAGIC %md
# MAGIC ### Creating a merged biketrip count data with weather data created above. After that we are saving that data in the filepath as silverbike_weather_g07. We are also creating a separate file for silverweather_imputed_g07. This data is all weather data including history and api pulled weather data. The first two code chunks analyzes how the merge will work and if we are getting null weather in expected dates i.e. 2021-01-01 to 2021-11-19

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * 
# MAGIC FROM bike_trips_count_delta as tab1 
# MAGIC LEFT JOIN clean_weather_final_df_delta_delta as tab2
# MAGIC ON tab1.startdate = tab2.parsed_dt
# MAGIC order by startdate

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT min(tab1.startdate), max(tab1.startdate) 
# MAGIC FROM bike_trips_count_delta as tab1 
# MAGIC LEFT JOIN clean_weather_final_df_delta_delta as tab2
# MAGIC ON tab1.startdate = tab2.parsed_dt
# MAGIC WHERE tab2.parsed_dt is NULL

# COMMAND ----------

bike_weather_df=spark.sql("""SELECT * 
FROM bike_trips_count_delta as tab1 
LEFT JOIN clean_weather_final_df_delta_delta as tab2
ON tab1.startdate = tab2.parsed_dt
"""
)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Adding Z-Ordering on the column on which we are joining data with other tables and writing as a delta file

# COMMAND ----------

##creating a silver table which is merged of weather and bike trip
delta_table_name = 'bike_weather_g07'
bike_weather_df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").option("path", GROUP_DATA_PATH + "silver"+ delta_table_name).option("zOrderByCol", "startdate").saveAsTable(delta_table_name)

# COMMAND ----------

##creating a silver table only for weather with api imputed data to use during EDA
delta_table_name = 'weather_imputed_g07'
clean_weather_final_df_delta.write.format("delta").mode("overwrite").option("overwriteSchema", "true").option("path", GROUP_DATA_PATH + "silver"+ delta_table_name).option("zOrderByCol", "parsed_dt").saveAsTable(delta_table_name)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Getting the Realtime data from the three other Bronze Tables and preparing corresponding silver tables

# COMMAND ----------

files=dbutils.fs.ls("dbfs:/FileStore/tables/G07/bronze")
for file in files:
    print(file.name)

# COMMAND ----------

read_path_rt_stationinfo = BRONZE_STATION_INFO_PATH
read_path_rt_stationstatus = BRONZE_STATION_STATUS_PATH
read_path_rt_weather = BRONZE_NYC_WEATHER_PATH

# COMMAND ----------

# MAGIC %md
# MAGIC #### Pulling the Station Info/Status Bronze Data For our station and preparing Silver table for it

# COMMAND ----------

stationinfo_real_time = spark.read.format("delta").option("ignoreChanges", "true").load(read_path_rt_stationinfo)
stationstatus_real_time = spark.read.format("delta").option("ignoreChanges", "true").load(read_path_rt_stationstatus)
stationinfo_real_time.createOrReplaceTempView('stationinfo_delta')
stationstatus_real_time.createOrReplaceTempView('stationstatus_delta')

# COMMAND ----------

GROUP_STATION_ASSIGNMENT

# COMMAND ----------

# MAGIC %sql
# MAGIC -- select count(*) from stationinfo_delta --1907
# MAGIC select * from stationinfo_delta
# MAGIC where station_id = "daefc84c-1b16-4220-8e1f-10ea4866fdc7"

# COMMAND ----------

# MAGIC %sql
# MAGIC -- select count(*) from stationstatus_delta --4006993
# MAGIC select from_unixtime(last_reported) as date,num_bikes_available,num_bikes_disabled,* 
# MAGIC from stationstatus_delta
# MAGIC where station_id = "daefc84c-1b16-4220-8e1f-10ea4866fdc7"
# MAGIC order by from_unixtime(last_reported) desc

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Preparing the Silver Table for Realtime Station Info Data

# COMMAND ----------

realtime_bike_status = spark.sql(
"""
WITH CTE AS(
select sid.station_id,name,region_id,short_name,lat,lon,from_unixtime(last_reported) as last_reported,capacity,num_bikes_available,
num_docks_available,is_installed,num_bikes_disabled,station_status, (capacity-num_bikes_available-num_bikes_disabled) net_availability,
ROW_NUMBER() OVER (PARTITION BY DATE(from_unixtime(last_reported)), HOUR(from_unixtime(last_reported)) ORDER BY from_unixtime(last_reported) DESC) AS RNUM
from stationinfo_delta sid
right join stationstatus_delta ssd
on sid.station_id = ssd.station_id
where sid.name = "Broadway & W 25 St"
)
,cte2 as (
SELECT
date_format(date_trunc('hour',last_reported),'yyyy-MM-dd HH:mm:ss') as last_reported_hour ,capacity,
ifnull(LAG(num_bikes_available) OVER(ORDER BY date_format(date_trunc('hour',last_reported),'yyyy-MM-dd HH:mm:ss')),0) num_bikes_available,
num_docks_available,num_bikes_disabled,net_availability
FROM CTE WHERE RNUM=1
)
select *, num_bikes_available - ifnull(LAG(num_bikes_available) OVER(ORDER BY last_reported_hour),0) net_difference
FROM cte2
order by last_reported_hour
"""
)

# COMMAND ----------

realtime_bike_status.createOrReplaceTempView('realtime_bike_status_delta')

# COMMAND ----------

display(realtime_bike_status)

# COMMAND ----------

delta_table_name = "realtime_bike_status"
realtime_bike_status.write.format("delta").mode("overwrite").option("overwriteSchema", "true").option("path", GROUP_DATA_PATH + "silver"+ delta_table_name).saveAsTable(delta_table_name) 

# COMMAND ----------

# MAGIC %md
# MAGIC #### Preparing Real Time Bike-Weather Merged Silver Table Data

# COMMAND ----------

#reading realtime weather
weather_real_time = spark.read.format("delta").option("ignoreChanges", "true").load(read_path_rt_weather)
weather_real_time.createOrReplaceTempView('weather_real_time_delta')
display(weather_real_time)

# COMMAND ----------

realtime_bike_weather = spark.sql(
"""
select last_reported_hour as startdate, num_bikes_available, net_difference as net_differece,
from_unixtime(dt) as parsed_date,temp,feels_like, pressure, humidity, dew_point, uvi, clouds, visibility, wind_speed, wind_deg, pop, weather.main[0] as main,weather.description[0] as description
from realtime_bike_status_delta as stg1
LEFT JOIN weather_real_time_delta as stg2
on stg1.last_reported_hour = from_unixtime(stg2.dt)
where stg2.temp is not null
order by stg1.last_reported_hour
""")

# COMMAND ----------

display(realtime_bike_weather)

# COMMAND ----------

delta_table_name = "realtime_bike_weather_merged"
realtime_bike_weather.write.format("delta").mode("overwrite").option("overwriteSchema", "true").option("path", GROUP_DATA_PATH + "silver"+ delta_table_name).saveAsTable(delta_table_name) 

# COMMAND ----------

import json

# Return Success
dbutils.notebook.exit(json.dumps({"exit_code": "OK"}))
