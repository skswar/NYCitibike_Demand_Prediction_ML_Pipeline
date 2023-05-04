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

spark.sql(f"USE {GROUP_DB_NAME}")

# COMMAND ----------

events_df = spark.read.format('delta').option('header', True).option('inferSchema', True).load(BIKE_TRIP_DATA_PATH)
display(events_df)

# COMMAND ----------

events_df.createOrReplaceTempView("my_bike_view")

# COMMAND ----------

df = spark.sql("""
select CONCAT(CAST(MONTH(started_at) AS VARCHAR(10)),"-",CAST(YEAR(started_at) AS VARCHAR(10))), count(ride_id) as cnt
from my_bike_view
group by MONTH(started_at), YEAR(started_at)
order by YEAR(started_at),MONTH(started_at)""")

display(df)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT DAY(started_at), COUNT(ride_id) as cnt
# MAGIC FROM my_bike_view
# MAGIC GROUP BY DAY(started_at)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT DATE(started_at) as start_date, COUNT(ride_id) as cnt
# MAGIC FROM my_bike_view
# MAGIC GROUP BY DATE(started_at)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT DATE(started_at) as start_date, COUNT(ride_id) as cnt
# MAGIC FROM my_bike_view
# MAGIC GROUP BY DATE(started_at)

# COMMAND ----------

# MAGIC %md
# MAGIC First dip that we see is in November 25th, 2021 because of Thanksgiving.<br>
# MAGIC Secondly we see dip is in December 25th, 2021 another valid reason as it was Christmas.<br>
# MAGIC There was a big snowstorm at East Coast, USA on Jan 29th 2022 thus explains the dip at NYC.
# MAGIC Also the long weekend of President's day on Feb 21st, 2022 explained a dip.Like this there are dips holidays such as Easter (April 17th), Independence Day (July 4th) etc. which are explaing that less number of people used the bike service.

# COMMAND ----------

from pyspark.sql.functions import date_format

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT date_format(started_at,'EEEE') as start_date, COUNT(ride_id) as cnt
# MAGIC FROM my_bike_view
# MAGIC GROUP BY date_format(started_at,'EEEE')
# MAGIC ORDER BY  DAYOFWEEK(start_date)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT HOUR(started_at) as Hour_of_Day, COUNT(ride_id) as cnt
# MAGIC FROM my_bike_view
# MAGIC GROUP BY HOUR(started_at)
# MAGIC ORDER BY HOUR(started_at)

# COMMAND ----------

# MAGIC %md
# MAGIC We saw that there are less number of rides during midnight and early morning (12AM to 6AM).<br>
# MAGIC There is peak during late noon to evening hours (highest between 4PM to 6PM).<br>
# MAGIC This pattern explains the normal working schedule of general population.

# COMMAND ----------

wdf=spark.read.format('csv').option("header","True").option("inferSchema","True").load(NYC_WEATHER_FILE_PATH)
display(wdf)

# COMMAND ----------

from pyspark.sql.functions import date_format, to_timestamp, from_utc_timestamp,from_unixtime,lit
wdf.createOrReplaceTempView("my_weather_view")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT distinct from_unixtime(dt) as datetime, hour(from_unixtime(dt)) as hour_value, date(from_unixtime(dt)) as date_value ,*
# MAGIC FROM my_weather_view
# MAGIC order by datetime

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT tab1.ride_id, from_unixtime(tab2.dt) weather_date,* 
# MAGIC FROM my_bike_view tab1
# MAGIC INNER JOIN my_weather_view as tab2
# MAGIC ON DATE(tab1.started_at) = DATE(from_unixtime(tab2.dt))
# MAGIC AND HOUR(tab1.started_at) = HOUR(from_unixtime(tab2.dt))
# MAGIC WHERE DATE(from_unixtime(tab2.dt)) BETWEEN '2022-01-28' AND '2022-01-30'
# MAGIC
# MAGIC -- SELECT min(DATE(from_unixtime(dt))) as mindate, max(DATE(from_unixtime(dt))) as maxdate
# MAGIC -- FROM my_weather_view

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT tab2.main as weathercondtion, COUNT(tab1.ride_id) as ridecount
# MAGIC FROM my_bike_view as tab1
# MAGIC INNER JOIN my_weather_view as tab2
# MAGIC ON DATE(tab1.started_at) = DATE(from_unixtime(tab2.dt))
# MAGIC AND HOUR(tab1.started_at) = HOUR(from_unixtime(tab2.dt))
# MAGIC GROUP BY tab2.main

# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------

import json

# Return Success
dbutils.notebook.exit(json.dumps({"exit_code": "OK"}))
