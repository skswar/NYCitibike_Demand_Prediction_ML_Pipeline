# Databricks notebook source
# MAGIC %run ./includes/includes

# COMMAND ----------

# DBTITLE 0,YOUR APPLICATIONS CODE HERE...
# start_date = str(dbutils.widgets.get('01.start_date'))
# end_date = str(dbutils.widgets.get('02.end_date'))
# hours_to_forecast = int(dbutils.widgets.get('03.hours_to_forecast'))
# promote_model = bool(True if str(dbutils.widgets.get('04.promote_model')).lower() == 'yes' else False)

# print(start_date,end_date,hours_to_forecast, promote_model)

# print("YOUR CODE HERE...")

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

# silverrealtime_bike_weather_merged
test_data = spark.read.format('delta').option('header', True).option('inferSchema', True).load('dbfs:/FileStore/tables/G07/silverrealtime_bike_weather_merged') 
test_data = test_data.withColumnRenamed("startdate", "ds")
test_data = test_data.withColumnRenamed("net_differece", "y")
test_data_df= test_data.toPandas()
display(test_data_df)

# COMMAND ----------

staging_forecast = model_staging.predict(test_data.toPandas())[['ds', 'yhat']]
prod_forecast = model_prod.predict(test_data.toPandas())[['ds', 'yhat']]

# COMMAND ----------

prod_forecast['stage'] = 'prod'
staging_forecast['stage'] = 'staging'
df_forecast = pd.concat([prod_forecast, staging_forecast]).sort_values(['ds', 'stage']).reset_index(drop=True)
df_forecast

# COMMAND ----------


# promote_model = True # Remove this line
# promote_model = False
if promote_model:
    client.transition_model_version_stage(
    name=model_details.name,
    version=model_details.version,
    stage='Production')
else:
client.transition_model_version_stage(
name=model_details.name,
version=model_details.version,
stage='Staging')

# COMMAND ----------

gold_data = f"{GROUP_DATA_PATH}gold_data.delta"
(
spark.createDataFrame(df_forecast)
.write
.format("delta")
.mode("overwrite")
.save(gold_data)
)

# COMMAND ----------

# Calculate the recent num_bikes_available
bike_forecast = df_forecast[(df_forecast.ds > last_reported) & (df_forecast.stage == 'prod')].reset_index(drop=True)
bike_forecast['bikes_available'] = bike_forecast['yhat'].round() + num_bikes_available
bike_forecast['ds'] = bike_forecast['ds'].dt.strftime("%Y-%m-%d %H:%M:%S")

# Forecast the available bikes for the next hours_to_forecast hours
hours_to_forecast=4
displayHTML(f"<h2>Forecast the available bikes for the next {hours_to_forecast} hours:</h2>")
display(bike_forecast[['ds', 'bikes_available']].head(int(hours_to_forecast)))

# COMMAND ----------

import plotly.express as px
bike_forecast['station_capacity'] = 109
bike_forecast['lower_bound'] = 0
bike_forecast['ds'] = pd.to_datetime(bike_forecast['ds'], format="%Y-%m-%d %H:%M:%S")
fig = px.line(bike_forecast, x='ds', y=['bikes_available','station_capacity', 'lower_bound'], color_discrete_sequence=['blue','black', 'black'])
fig.update_layout(title=f'{GROUP_STATION_ASSIGNMENT} bike forecast')
fig.update_xaxes(title_text='time')
fig.update_yaxes(title_text='bikes_available')
fig.show()

# COMMAND ----------

# import plotly.express as px
# import plotly.graph_objects as go
# fig = go.Figure()
# pd_plot["zero_stock"] = 0
# fig.add_trace(go.Scatter(x=pd_plot.hour_window, y=pd_plot["new_available"], name='Forecasted available bikes',mode = 'lines+markers',
# line = dict(color='blue', width=3, dash='solid')))
# fig.add_trace(go.Scatter(x=pd_plot.hour_window[:4], y=pd_plot["new_available"][:4], mode = 'markers',name='Forecast for next 4 hours',
# marker_symbol = 'triangle-up',
# marker_size = 15,
# marker_color="green"))
# fig.add_trace(go.Scatter(x=pd_plot.hour_window, y=pd_plot["capacity"], name='Station Capacity (Overstock beyond this)',
# line = dict(color='red', width=3, dash='dot')))
# fig.add_trace(go.Scatter(x=pd_plot.hour_window, y=pd_plot["zero_stock"], name='Stock Out (Understock below this)',
# line = dict(color='red', width=3, dash='dot')))
# # Edit the layout
# fig.update_layout(title='Forecasted number of available bikes',
# xaxis_title='Forecasted Timeline',
# yaxis_title='#bikes',
# yaxis_range=[-5,100])
# fig.show()

# COMMAND ----------

# # Calcluate residuals
# test_data.createOrReplaceTempView("test_data_view")
# test_truth = spark.sql(""" select * from test_data_view """).toPandas()
# test_truth['ts'] = test_truth['ts'].apply(pd.to_datetime)
# results = df_monitor.merge(test_truth, left_on='ds', right_on='ts')
# results['residual'] = results['yhat'] - results['netChange']

# # # Plot the residuals
# fig = px.scatter(results, x='yhat', y='residual', color='stage', marginal_y='violin', trendline='ols')
# fig.update_layout(title=f'{GROUP_STATION_ASSIGNMENT} rental forecast model performance comparison')
# fig.show()

# COMMAND ----------

import json

# Return Success
dbutils.notebook.exit(json.dumps({"exit_code": "OK"}))
