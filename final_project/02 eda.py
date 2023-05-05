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

spark.sql(f"USE {GROUP_DB_NAME}")

# COMMAND ----------

files=dbutils.fs.ls("dbfs:/FileStore/tables/G07")
# count=0
for file in files:
#     count+=1
# print(count)
    print(file.name)

# COMMAND ----------

merged_df = spark.read.format('delta').option('header', True).option('inferSchema', True).load("dbfs:/FileStore/tables/G07/silverbike_weather_g07")
display(merged_df)

# COMMAND ----------

# merged_df.createOrReplaceTempView("my_bike_view")

# COMMAND ----------

# df = spark.sql("""
# select CONCAT(CAST(MONTH(started_at) AS VARCHAR(10)),"-",CAST(YEAR(started_at) AS VARCHAR(10))), count(ride_id) as cnt
# from my_bike_view
# group by MONTH(started_at), YEAR(started_at)
# order by YEAR(started_at),MONTH(started_at)""")

# display(df)

# COMMAND ----------

# %sql
# SELECT DAY(started_at), COUNT(ride_id) as cnt
# FROM my_bike_view
# GROUP BY DAY(started_at)

# COMMAND ----------

# %sql
# SELECT DATE(started_at) as start_date, COUNT(ride_id) as cnt
# FROM my_bike_view
# GROUP BY DATE(started_at)

# COMMAND ----------

# %sql
# SELECT DATE(started_at) as start_date, COUNT(ride_id) as cnt
# FROM my_bike_view
# GROUP BY DATE(started_at)

# COMMAND ----------

# MAGIC %md
# MAGIC First dip that we see is in November 25th, 2021 because of Thanksgiving.<br>
# MAGIC Secondly we see dip is in December 25th, 2021 another valid reason as it was Christmas.<br>
# MAGIC There was a big snowstorm at East Coast, USA on Jan 29th 2022 thus explains the dip at NYC.
# MAGIC Also the long weekend of President's day on Feb 21st, 2022 explained a dip.Like this there are dips holidays such as Easter (April 17th), Independence Day (July 4th) etc. which are explaing that less number of people used the bike service.

# COMMAND ----------

from pyspark.sql.functions import date_format

# COMMAND ----------

# %sql
# SELECT date_format(started_at,'EEEE') as start_date, COUNT(ride_id) as cnt
# FROM my_bike_view
# GROUP BY date_format(started_at,'EEEE')
# ORDER BY  DAYOFWEEK(start_date)

# COMMAND ----------

# %sql
# SELECT HOUR(started_at) as Hour_of_Day, COUNT(ride_id) as cnt
# FROM my_bike_view
# GROUP BY HOUR(started_at)
# ORDER BY HOUR(started_at)

# COMMAND ----------

# MAGIC %md
# MAGIC We saw that there are less number of rides during midnight and early morning (12AM to 6AM).<br>
# MAGIC There is peak during late noon to evening hours (highest between 4PM to 6PM).<br>
# MAGIC This pattern explains the normal working schedule of general population.

# COMMAND ----------

# wdf=spark.read.format('csv').option("header","True").option("inferSchema","True").load(NYC_WEATHER_FILE_PATH)
# display(wdf)

# COMMAND ----------

# from pyspark.sql.functions import date_format, to_timestamp, from_utc_timestamp,from_unixtime,lit
# wdf.createOrReplaceTempView("my_weather_view")

# COMMAND ----------

# %sql
# SELECT distinct from_unixtime(dt) as datetime, hour(from_unixtime(dt)) as hour_value, date(from_unixtime(dt)) as date_value ,*
# FROM my_weather_view
# order by datetime

# COMMAND ----------

# %sql
# SELECT tab1.ride_id, from_unixtime(tab2.dt) weather_date,* 
# FROM my_bike_view tab1
# INNER JOIN my_weather_view as tab2
# ON DATE(tab1.started_at) = DATE(from_unixtime(tab2.dt))
# AND HOUR(tab1.started_at) = HOUR(from_unixtime(tab2.dt))
# WHERE DATE(from_unixtime(tab2.dt)) BETWEEN '2022-01-28' AND '2022-01-30'

# -- SELECT min(DATE(from_unixtime(dt))) as mindate, max(DATE(from_unixtime(dt))) as maxdate
# -- FROM my_weather_view

# COMMAND ----------

# %sql
# SELECT tab2.main as weathercondtion, COUNT(tab1.ride_id) as ridecount
# FROM my_bike_view as tab1
# INNER JOIN my_weather_view as tab2
# ON DATE(tab1.started_at) = DATE(from_unixtime(tab2.dt))
# AND HOUR(tab1.started_at) = HOUR(from_unixtime(tab2.dt))
# GROUP BY tab2.main

# COMMAND ----------



# COMMAND ----------

display(merged_df)

# COMMAND ----------

from pyspark.sql.functions import month, sum

monthly_trip_trends = merged_df.groupBy(month("parsed_dt").alias("month")) \
    .agg(sum("net_trip_difference").alias("total_trips")) \
    .orderBy("month")

display(monthly_trip_trends)


# COMMAND ----------

# MAGIC %md
# MAGIC Reading in Bronze Data

# COMMAND ----------

bronze_df = spark.read.format('delta').option('header', True).option('inferSchema', True).load("dbfs:/FileStore/tables/G07/silverhistoric_bike_trip_g07")
display(bronze_df)

# COMMAND ----------

from pyspark.sql.functions import to_timestamp

bronze_df = bronze_df.withColumn("started_at", to_timestamp("started_at", "yyyy-MM-dd HH:mm:ss")) \
    .withColumn("ended_at", to_timestamp("ended_at", "yyyy-MM-dd HH:mm:ss"))


# COMMAND ----------

from pyspark.sql.functions import month, year, count

assigned_station_id = 6173.08
monthly_trips = bronze_df.filter(bronze_df.start_station_id == assigned_station_id) \
    .groupBy(year("started_at").alias("year"), month("started_at").alias("month")) \
    .agg(count("ride_id").alias("num_trips")) \
    .orderBy("year", "month")

display(monthly_trips)


# COMMAND ----------

from pyspark.sql.functions import date_trunc

daily_trips = bronze_df.filter(bronze_df.start_station_id == assigned_station_id) \
    .groupBy(date_trunc("day", "started_at").alias("day")) \
    .agg(count("ride_id").alias("num_trips")) \
    .orderBy("day")

display(daily_trips)


# COMMAND ----------

import holidays
from pandas.tseries.holiday import USFederalHolidayCalendar as calendar
from pyspark.sql.functions import avg

# COMMAND ----------

us_holidays = holidays.US()
holiday_dates=[]
for p,q in holidays.US(years = [2021,2022,2023]).items():  
    holiday_dates.append(p)

print(holiday_dates)
     

# COMMAND ----------

daily_trips = daily_trips.withColumn("is_holiday", daily_trips.day.isin(holiday_dates))

avg_trips_by_holiday_status = daily_trips.groupBy("is_holiday") \
    .agg(avg("num_trips").alias("avg_num_trips")) \
    .orderBy("is_holiday")

display(avg_trips_by_holiday_status)

# COMMAND ----------

# MAGIC %md
# MAGIC Analyzing Weather data

# COMMAND ----------

merged_df = spark.read.format('delta').option('header', True).option('inferSchema', True).load("dbfs:/FileStore/tables/G07/silverupdated_g07/")
display(merged_df)



# COMMAND ----------

print(merged_df.count())
merged_df.printSchema()

# COMMAND ----------

from pyspark.sql.functions import col,round
from pyspark.sql.functions import hour
from pyspark.sql.types import StringType, FloatType
merged_df = merged_df.withColumn("temp", col("temp").cast(FloatType()))
merged_df = merged_df.withColumn("rounded_temp", round(col("temp"), 0))
display(merged_df)

# COMMAND ----------


temperature_hourly_trend = merged_df.groupBy("rounded_temp", hour("started_at").alias("hour")) \
    .agg(count("ride_id").alias("num_trips")) \
    .orderBy("rounded_temp", "hour")

display(temperature_hourly_trend)


# COMMAND ----------



# COMMAND ----------

import json

# Return Success
dbutils.notebook.exit(json.dumps({"exit_code": "OK"}))
