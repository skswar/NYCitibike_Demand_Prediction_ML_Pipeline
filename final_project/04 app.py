# Databricks notebook source
# MAGIC %run ./includes/includes

# COMMAND ----------

# DBTITLE 0,YOUR APPLICATIONS CODE HERE...
start_date = str(dbutils.widgets.get('01.start_date'))
end_date = str(dbutils.widgets.get('02.end_date'))
hours_to_forecast = int(dbutils.widgets.get('03.hours_to_forecast'))
promote_model = bool(True if str(dbutils.widgets.get('04.promote_model')).lower() == 'yes' else False)

print(start_date,end_date,hours_to_forecast, promote_model)
print("YOUR CODE HERE...")

# COMMAND ----------

# MAGIC %md
# MAGIC Current timestamp when the notebook is run (now)

# COMMAND ----------

current_date = pd.Timestamp.now(tz='US/Eastern').round(freq="H")
formt = '%Y-%m-%d %H:%M:%S'
currenthour = current_date.strftime("%Y-%m-%d %H")
current_date = current_date.strftime(formt)
print("The current time:",current_date)

# COMMAND ----------

from mlflow.tracking.client import MlflowClient
import mlflow

# COMMAND ----------

# MAGIC %md
# MAGIC Production Model version
# MAGIC Staging Model version

# COMMAND ----------

client = MlflowClient()
prod_model = client.get_latest_versions("G07_model",stages=['Production'])
stage_model = client.get_latest_versions("G07_model",stages=['Staging'])
print("\nProduction Model:",prod_model)
print("\n Staging Model:",stage_model)

# COMMAND ----------

# MAGIC %md
# MAGIC Station name and a map location (marker)

# COMMAND ----------

#locating the assigned station on google maps
print("Station Name :", GROUP_STATION_ASSIGNMENT)
lat = 40.74286877312112 #defined in includes file
lon = -73.98918628692627 #defined in includes file
maps_url = f"https://www.google.com/maps/embed/v1/place?key=AIzaSyAzh2Vlgx7LKBUexJ3DEzKoSwFAvJA-_Do&q={lat},{lon}&zoom=15&maptype=satellite"
iframe = f'<iframe width="100%" height="400px" src="{maps_url}"></iframe>'
displayHTML(iframe)

# COMMAND ----------

# MAGIC %md
# MAGIC Current weather (temp and precip at a minimum)

# COMMAND ----------

weather_df= spark.read.format("delta").load('dbfs:/FileStore/tables/bronze_nyc_weather.delta').select("time","temp","visibility",'humidity',"pressure","wind_speed","clouds","pop").toPandas()
print("Weather data:")
display(weather_df[weather_df.time==current_date].reset_index(drop=True))

# COMMAND ----------

# MAGIC %md
# MAGIC Total docks at this station
# MAGIC Total bikes available at this station (list the different types of bikes and whether any bikes are disabled/broken)

# COMMAND ----------

from pyspark.sql.functions import *
print("Total docks at ",GROUP_STATION_ASSIGNMENT)
station_status_df= spark.read.format("delta").load(BRONZE_STATION_STATUS_PATH).filter(col("station_id")=="daefc84c-1b16-4220-8e1f-10ea4866fdc7").withColumn("last_reported", date_format(from_unixtime(col("last_reported").cast("long")), "yyyy-MM-dd HH:mm:ss")).sort(desc("last_reported")).select("last_reported","num_docks_available","num_bikes_disabled","num_bikes_available","num_ebikes_available","num_scooters_available")
display(station_status_df.filter(col("last_reported") <= currenthour).sort(desc("last_reported")).head(1))


# COMMAND ----------

last_report_time =station_status_df.filter(col("last_reported") <= currenthour).sort(desc("last_reported")).head(1)
last_reported=last_report_time[0]['last_reported']
num_bikes_available=last_report_time[0]['num_bikes_available']
print("Last Reported:",last_reported)
print("Number of available bikes:",num_bikes_available)

# COMMAND ----------

# MAGIC %md
# MAGIC Forecast the available bikes for the next 4 hours.

# COMMAND ----------


# Predict on the future based on the staging model
model_staging_uri = f'models:/G07_model/staging'
model_staging = mlflow.prophet.load_model(model_staging_uri)
display(model_staging)

# COMMAND ----------

model_prod_uri = f'models:/G07_model/production'
model_prod = mlflow.prophet.load_model(model_prod_uri)
display(model_prod)

# COMMAND ----------

#readiong the data used for train
data = spark.read.format('delta').option('header', True).option('inferSchema', True).load('dbfs:/FileStore/tables/G07/silverbike_weather_g07') 
data.createOrReplaceTempView("train_data")

#reading the real time weather data
weather_real_time = spark.read.format('delta').option('header', True).option('inferSchema', True).load('dbfs:/FileStore/tables/G07/silverweather_real_time_filtered')
weather_real_time.createOrReplaceTempView('silverweather_real_time_filtered')

#preparing future dataframe for prediction
forecast_df = spark.sql(
"""
with cte as(
select startdate, temp, humidity from train_data 
where startdate >'2023-01-01T00:00:00.000+0000'
union 
select startdate, temp, humidity from silverweather_real_time_filtered 
where startdate > (select max(startdate) from train_data)
)
select startdate as ds,temp,humidity
from cte order by startdate
"""
)
forecast_df_pd = forecast_df.toPandas()


# COMMAND ----------

staging_forecast = model_staging.predict(forecast_df_pd)[['ds', 'yhat']]
prod_forecast = model_prod.predict(forecast_df_pd)[['ds', 'yhat']]


# COMMAND ----------

prod_forecast['stage'] = 'prod'
staging_forecast['stage'] = 'staging'
df_forecast = pd.concat([prod_forecast, staging_forecast]).sort_values(['ds', 'stage']).reset_index(drop=True)
df_forecast

# COMMAND ----------

##the following code will create a future forecast with inventory change 
df_forecast=spark.createDataFrame(df_forecast)
df_forecast.createOrReplaceTempView('forecast_df')

realtime_bike_status = spark.read.format('delta').option('header', True).option('inferSchema', True).load('dbfs:/FileStore/tables/G07/silverrealtime_bike_status')
realtime_bike_status.createOrReplaceTempView('realtime_bike_status')

forecasted_table = spark.sql(
"""
select tab1.ds, tab2.num_bikes_available, tab2.net_difference as groundtruth, tab1.yhat, tab1.yhat+tab2.num_bikes_available as inventory, 'prod' as environment
from forecast_df tab1
left join realtime_bike_status tab2
on tab1.ds = tab2.last_reported_hour_est
where tab1.stage='prod'
union
select tab1.ds, tab2.num_bikes_available, tab2.net_difference as groundtruth, tab1.yhat, tab1.yhat+tab2.num_bikes_available as inventory, 'stage' as environment
from forecast_df tab1
left join realtime_bike_status tab2
on tab1.ds = tab2.last_reported_hour_est
where tab1.stage='staging'
order by tab1.ds
""")
forecasted_table.createOrReplaceTempView("forecasted_table")
display(forecasted_table)

# COMMAND ----------

plot_forecast_prod = spark.sql(
"""
with cte AS (
select ds, yhat, inventory, 109 as capacity_track
from forecasted_table 
where ds = (select max(ds) from forecasted_table where num_bikes_available is not null)
and environment = 'prod'
union
select ds, yhat, yhat, yhat as capacity_track
from forecasted_table 
where ds > (select max(ds) from forecasted_table where num_bikes_available is not null)
and environment = 'prod'
)
select *, round(SUM(inventory) over (order by ds)) as final_inventory,
round(SUM(capacity_track) over (order by ds)) as capacity_flow
from cte
order by ds
"""
)
display(plot_forecast_prod)
plot_forecast_prod.createOrReplaceTempView("plot_forecast_prod")
plot_forecast_prod = plot_forecast_prod.toPandas()

# COMMAND ----------

plot_forecast_stage = spark.sql(
"""
with cte AS (
select ds, yhat, inventory, 109 as capacity_track
from forecasted_table 
where ds = (select max(ds) from forecasted_table where num_bikes_available is not null)
and environment = 'stage'
union
select ds, yhat, yhat, yhat as capacity_track
from forecasted_table 
where ds > (select max(ds) from forecasted_table where num_bikes_available is not null)
and environment = 'stage'
)
select *, round(SUM(inventory) over (order by ds)) as final_inventory,
round(SUM(capacity_track) over (order by ds)) as capacity_flow
from cte
order by ds
"""
)
display(plot_forecast_stage)
plot_forecast_stage.createOrReplaceTempView("plot_forecast_stage")
plot_forecast_stage = plot_forecast_stage.toPandas()

# COMMAND ----------

#creating the gold table
gold_monitoring = spark.sql(
"""
with cte AS (
select ds, yhat, final_inventory, 'prod' as env 
from plot_forecast_prod
union
select ds, yhat, final_inventory, 'stage' as env
from plot_forecast_stage
)
select 
ds, max(case when env='prod' then yhat end) as yhat_prod, max(case when env='prod' then final_inventory end) as final_inventory_prod, 
max(case when env='stage' then yhat end) as yhat_stage, max(case when env='stage' then final_inventory end) as final_inventory_stage 
from cte
group by ds
order by ds
""")
display(gold_monitoring)

# COMMAND ----------


gold_monitoring.write.format("delta").mode("overwrite").option("overwriteSchema", "true").option("path", GROUP_DATA_PATH + "gold"+ 'netChange_inventory').saveAsTable('netChange_inventory')


# COMMAND ----------



# COMMAND ----------

import plotly.express as px
plot_forecast_prod['station_capacity'] = 109
plot_forecast_prod['lower_bound'] = 0
plot_forecast_prod['ds'] = pd.to_datetime(plot_forecast_prod['ds'], format="%Y-%m-%d %H:%M:%S")
fig = px.line(plot_forecast_prod, x='ds', y=['final_inventory','station_capacity', 'lower_bound'], color_discrete_sequence=['blue','black', 'black'])
fig.update_layout(title=f'{GROUP_STATION_ASSIGNMENT} bike forecast')
fig.update_xaxes(title_text='time')
fig.update_yaxes(title_text='bikes_available')
fig.show()

hours_to_forecast=48
displayHTML(f"<h2>Forecast the available bikes for the next {hours_to_forecast} hours.</h2>") 
displayHTML("<p>By observing the black line's lower bound of 0, we can determine whether bikes are available at a given station or not. If the blue line goes above 0, it means that bikes are available, and if it goes below 0, it means that no bikes are available. The graph shows that more bikes are needed in the morning and fewer bikes are needed during the noon. One possible reason for this pattern is that rebalancing of bikes occurs during the noon, which improve the availability for bikes at that time.</p>")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Residual Plot
# MAGIC ###### The residual plot on the real time data using the net hourly difference. The error will be little high in this case because we are not factoring the rebalacing that is taking place at the station. 

# COMMAND ----------


residuals_table = spark.sql(
"""
select tab1.ds, tab2.net_difference as groundtruth, tab1.yhat,  (tab2.net_difference - tab1.yhat) as residuals
from forecast_df tab1
left join realtime_bike_status tab2
on tab1.ds = tab2.last_reported_hour_est
where tab1.stage='prod'
and tab2.num_bikes_available is NOT NULL
order by tab1.ds
"""
)
residuals_table = residuals_table.toPandas()

#plot the residuals
import plotly.express as px
fig = px.scatter(
    residuals_table, x='yhat', y='residuals',
    marginal_y='violin',
    trendline='ols',
)
fig.show()

# COMMAND ----------

residuals_table.head()

# COMMAND ----------


residuals_table_stage = spark.sql(
"""
select tab1.ds, tab2.net_difference as groundtruth, tab1.yhat,  (tab2.net_difference - tab1.yhat) as residuals
from forecast_df tab1
left join realtime_bike_status tab2
on tab1.ds = tab2.last_reported_hour_est
where tab1.stage='staging'
and tab2.num_bikes_available is NOT NULL
order by tab1.ds
"""
)
residuals_table_stage = residuals_table_stage.toPandas()

# COMMAND ----------

residuals_table_stage.head()

# COMMAND ----------

import numpy as np
# promote_model=False 
if promote_model:
    prod_mae = np.mean(residuals_table['yhat'] - residuals_table['groundtruth'])
    staging_mae = np.mean(residuals_table_stage['yhat'] - residuals_table_stage['groundtruth'])
    
    # If staging model has lower MAE, then move staging model to 'production' and production model to 'archive'
    if staging_mae < prod_mae:
        latest_production = client.get_latest_versions(GROUP_MODEL_NAME, stages=["Production"])[0]
        client.transition_model_version_stage(name=GROUP_MODEL_NAME, version=latest_production.version, stage='Archive')
        latest_staging = client.get_latest_versions(GROUP_MODEL_NAME, stages=["Staging"])[0]
        client.transition_model_version_stage(name=GROUP_MODEL_NAME, version=latest_staging.version, stage='Production')

# COMMAND ----------

import json

# Return Success
dbutils.notebook.exit(json.dumps({"exit_code": "OK"}))
