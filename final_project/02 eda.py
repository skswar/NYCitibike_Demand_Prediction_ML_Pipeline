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

# Import Statements
from pyspark.sql.functions import date_format
from pyspark.sql.functions import month, sum
from pyspark.sql.functions import to_timestamp
from pyspark.sql.functions import month, year, count
from pyspark.sql.functions import date_trunc
import holidays
from pandas.tseries.holiday import USFederalHolidayCalendar as calendar
from pyspark.sql.functions import avg
from pyspark.sql.functions import col,round
from pyspark.sql.functions import hour,date_trunc
from pyspark.sql.types import StringType, FloatType
import pyspark.sql.functions as F

# COMMAND ----------

files=dbutils.fs.ls("dbfs:/FileStore/tables/G07")
# count=0
for file in files:
#     count+=1
# print(count)
    print(file.name)

# COMMAND ----------

# MAGIC %md
# MAGIC Reading in Ride Data

# COMMAND ----------

bronze_df = spark.read.format('delta').option('header', True).option('inferSchema', True).load("dbfs:/FileStore/tables/G07/silverhistoric_bike_trip_g07")
display(bronze_df)

# COMMAND ----------

bronze_df = bronze_df.withColumn("started_at", to_timestamp("started_at", "yyyy-MM-dd HH:mm:ss")) \
    .withColumn("ended_at", to_timestamp("ended_at", "yyyy-MM-dd HH:mm:ss"))


# COMMAND ----------

assigned_station_id = 6173.08
monthly_trips = bronze_df.filter(bronze_df.start_station_id == assigned_station_id) \
    .groupBy(year("started_at").alias("year"), month("started_at").alias("month")) \
    .agg(count("ride_id").alias("num_trips")) \
    .orderBy("year", "month")

display(monthly_trips)


# COMMAND ----------

# MAGIC %md
# MAGIC As the summer months approach, we observe an increase in the number of rides. However, there is a slight dip in June as temperatures become excessively hot. As fall arrives, the weather becomes more favorable for bike rides, leading to a rise in the number of rides once again. When winter and colder months set in, accompanied by snow, the number of rides experiences a sharp decline, which persists until the arrival of spring.

# COMMAND ----------

daily_trips = bronze_df.filter(bronze_df.start_station_id == assigned_station_id) \
    .groupBy(date_trunc("day", "started_at").alias("day")) \
    .agg(count("ride_id").alias("num_trips")) \
    .orderBy("day")

display(daily_trips)


# COMMAND ----------

# MAGIC %md
# MAGIC The first noticeable dip in usage occurred on November 25th, 2021, which can be attributed to the Thanksgiving holiday. The next dip took place on December 25th, 2021, a valid reason being that it was Christmas Day. On January 29th, 2022, a significant snowstorm hit the East Coast of the USA, explaining the decrease in bike usage in New York City. Additionally, the long weekend for President's Day on February 21st, 2022 also accounted for a dip in usage. Similarly, there are dips in usage during other holidays, such as Easter (April 17th) and Independence Day (July 4th), which demonstrate that fewer people utilize the bike service on these occasions.

# COMMAND ----------

bronze_df.createOrReplaceTempView("my_bike_view")
df_year_month = spark.sql("""
select CONCAT(CAST(YEAR(started_at) AS VARCHAR(10)) ,"-", LPAD(MONTH(started_at), 2, '0')) as YearMonth, count(ride_id) as cnt
from my_bike_view
group by MONTH(started_at), YEAR(started_at)
order by YEAR(started_at),MONTH(started_at)""")
display(df_year_month)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT date_format(started_at,'EEEE') as start_date, COUNT(ride_id) as cnt
# MAGIC FROM my_bike_view
# MAGIC GROUP BY date_format(started_at,'EEEE')
# MAGIC ORDER BY DAYOFWEEK(start_date)

# COMMAND ----------

# MAGIC %md
# MAGIC We can observe that there are fewer rides on weekends compared to weekdays, supporting our hypothesis that the majority of users rely on our bikes for commuting to work rather than leisure activities. While there are mild fluctuations in usage throughout the week, the data remains significantly higher than weekend figures.

# COMMAND ----------



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
# MAGIC As we suspected that on a holiday we have lesser number of people using our bikes further confirming our hypothesis that the bikes at this station is being used primarily for commuting to work rather than leisure rides

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


merged_df = merged_df.withColumn("temp", col("temp").cast(FloatType()))
merged_df = merged_df.withColumn("rounded_temp", round(col("temp"), -1))
display(merged_df.limit(5))

# COMMAND ----------


# temperature_hourly_trend = merged_df.groupBy("rounded_temp", hour("started_at").alias("hour")) \
#     .agg(count("ride_id").alias("num_trips")) \
#     .orderBy("rounded_temp", "hour")

# display(temperature_hourly_trend)


# COMMAND ----------

# MAGIC %md
# MAGIC We observed that the number of rides decreases significantly between midnight and early morning, specifically from 12 AM to 6 AM. A peak in rides can be seen during late afternoon to evening hours, with the highest volume occurring between 4 PM and 6 PM. This pattern reflects the typical working schedule of the general population.

# COMMAND ----------

hourly_trend = merged_df.groupBy(F.date_trunc("hour", F.col("started_at")).alias("hour"), F.col("temp")) \
    .agg(F.count("ride_id").alias("num_rides")) \
    .orderBy("hour", "temp")

display(hourly_trend)


# COMMAND ----------

# MAGIC %md 
# MAGIC As anticipated, there is a general trend that demonstrates an increase in bicycle ridership as temperatures rise. However, when temperatures become excessively high, there is a noticeable decline in the number of rides.

# COMMAND ----------

# daily_trend = merged_df.groupBy(F.date_trunc("day", F.col("started_at")).alias("day"), F.col("rounded_temp")) \
#     .agg(F.count("ride_id").alias("num_rides")) \
#     .orderBy("day", "rounded_temp")

# display(daily_trend)


# COMMAND ----------



# COMMAND ----------

# daily_trend_cloud = merged_df.groupBy(F.date_trunc("day", F.col("started_at")).alias("day"), F.col("clouds")) \
#     .agg(F.count("ride_id").alias("num_rides")) \
#     .orderBy("day", "clouds")

# display(daily_trend_cloud)


# COMMAND ----------

hourly_trend_dew = merged_df.groupBy(F.date_trunc("hour", F.col("started_at")).alias("hour"), F.col("dew_point")) \
    .agg(F.count("ride_id").alias("num_rides")) \
    .orderBy("hour", "dew_point")

display(hourly_trend_dew)


# COMMAND ----------

# MAGIC %md
# MAGIC We cannot discern a specific trend in relation to dew point, although there are instances where a decrease in bike rides coincides with a drop in dew point.

# COMMAND ----------

import json

# Return Success
dbutils.notebook.exit(json.dumps({"exit_code": "OK"}))
