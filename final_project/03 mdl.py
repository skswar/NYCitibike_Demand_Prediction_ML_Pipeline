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

pip install fbprophet -qq

# COMMAND ----------

#Variable declaration and import libraries
import mlflow
import json
import pandas as pd
import numpy as np
from prophet import Prophet
from mlflow.tracking.client import MlflowClient
import seaborn as sns
import matplotlib.pyplot as plt
sns.set(color_codes=True)
import itertools
ARTIFACT_PATH = "G07_model"
np.random.seed(42)

# COMMAND ----------

data = spark.read.format('delta').option('header', True).option('inferSchema', True).load('dbfs:/FileStore/tables/G07/silverbike_weather_g07') 
display(data)
data.createOrReplaceTempView("merged_data")

# COMMAND ----------

# Training data (till dec 31st 2022)
from pyspark.sql.functions import to_timestamp, date_format,unix_timestamp
from pyspark.sql.functions import col
data.createOrReplaceTempView("train_data")
train_df= spark.sql("""select * from train_data where startdate <='2023-01-01T00:00:00.000+0000' and startdate >='2021-11-20T00:00:00.000+0000'""")
# train_df= spark.sql("""select * from train_data where startdate >='2021-11-20T00:00:00.000+0000'""")
train_df.count()
df_train = train_df.withColumnRenamed("startdate", "ds")
df_train = df_train.withColumnRenamed("net_trip_difference", "y")
display(df_train)
df_train_pd=df_train.toPandas()
# 

# COMMAND ----------

from pyspark.sql.functions import col
data.createOrReplaceTempView("test_data")
test_df= spark.sql("""
select *
from test_data
where startdate >='2023-01-01T00:00:00.000+0000'""")
test_df.count()
test_df = test_df.withColumnRenamed("startdate", "ds")
test_df = test_df.withColumnRenamed("net_trip_difference", "y")
display(test_df)
test_df_pd=test_df.toPandas()


# COMMAND ----------

import plotly.express as px
df = data.toPandas()
df.sort_values(by="startdate", inplace=True)
fig = px.line(df, x="startdate", y="net_trip_difference", title='Net bike change')
fig.show()

# COMMAND ----------

def mean_absolute_error(y_true, y_pred):
    return np.mean(np.abs(y_true - y_pred))

def mean_squared_error(y_true, y_pred):
    return np.mean(np.square(y_true - y_pred))

def root_mean_squared_error(y_true, y_pred):
    return np.sqrt(mean_squared_error(y_true, y_pred))


# COMMAND ----------

import itertools
params = {  
    'changepoint_prior_scale': [0.001, 0.01, 0.1,0.5], 
    'seasonality_prior_scale': [0.01, 0.1, 1.0, 10.0],
    'seasonality_mode': ['additive'],
    # 'holidays_prior_scale':[0.01, 10],
    'yearly_seasonality' : [True],
    'weekly_seasonality': [True],
    'daily_seasonality': [True],

}

# Generate all combinations of parameters
model_params = [dict(zip(params.keys(), v)) for v in itertools.product(*params.values())]

# COMMAND ----------


#try
from fbprophet import Prophet
import numpy as np
import pandas as pd
from prophet.diagnostics import cross_validation, performance_metrics
mae_scores=[]
rmse_scores=[]
import mlflow
for params in model_params:
    with mlflow.start_run(): 
        model = Prophet(**params) 
        # holidays = pd.DataFrame({"ds": [], "holiday": []})
        model.add_regressor('temp')
        model.add_country_holidays(country_name='US')
        model.add_regressor('humidity')
        # model.add_regressor('visibility')
        model.fit(df_train_pd)
        res =model.predict(test_df_pd.drop('y',axis=1))

        mae = mean_absolute_error(test_df_pd['y'], res['yhat'])
        rmse = root_mean_squared_error(test_df_pd['y'], res['yhat'])
        mlflow.prophet.log_model(model, artifact_path=ARTIFACT_PATH)
        mlflow.log_params(params)
        mlflow.log_metrics({'mae': mae})
        model_uri = mlflow.get_artifact_uri(ARTIFACT_PATH)
        print(f"Aritifacts: {model_uri}")


        mae_scores.append((mae, model_uri))
        rmse_scores.append((rmse, model_uri))

# COMMAND ----------

import pandas as pd
hyper_tuning_df = pd.DataFrame(model_params)
hyper_tuning_df['mae'] = list(zip(*mae_scores))[0]
hyper_tuning_df['rmse'] = list(zip(*rmse_scores))[0]
hyper_tuning_df['model']= list(zip(*mae_scores))[1]

# COMMAND ----------

display(hyper_tuning_df)

# COMMAND ----------

# choose the best model
best_model_params = dict(hyper_tuning_df.iloc[hyper_tuning_df[['mae']].idxmin().values[0]])
display(best_model_params)


# COMMAND ----------

# test_df_pd=test_df.toPandas()
best_model = mlflow.prophet.load_model(best_model_params['model'])
results = best_model.predict(test_df_pd.drop('y',axis=1))
test_df_pd['res']=results['yhat']
display(test_df_pd)


# COMMAND ----------

prophet_plot = best_model.plot(results)

# COMMAND ----------

prophet_plot2 = best_model.plot_components(results)

# COMMAND ----------

results=results[['ds','yhat']].join(df_train.toPandas(), lsuffix='_caller', rsuffix='_other')
results['residual'] = results['yhat'] - results['y']

# COMMAND ----------

#plot the residuals
import plotly.express as px
fig = px.scatter(
    results, x='yhat', y='residual',
    marginal_y='violin',
    trendline='ols',
)
fig.show()

# COMMAND ----------

#model Registry 
model_details = mlflow.register_model(model_uri=best_model_params['model'], name=ARTIFACT_PATH)

# COMMAND ----------

from mlflow.tracking.client import MlflowClient
client = MlflowClient()

# COMMAND ----------

#making sure the first model is pushed to production
try:
    client.get_latest_versions(GROUP_MODEL_NAME, stages=["Production"])
except:
    client.transition_model_version_stage(name=model_details.name, version=model_details.version, stage="Production")

# COMMAND ----------


if promote_model:
    client.transition_model_version_stage(
    name=model_details.name,
    version=model_details.version,
    stage='Production')

client.transition_model_version_stage( name=model_details.name,version=model_details.version,stage='Staging')

# COMMAND ----------

model_version_details = client.get_model_version(
  name=model_details.name,
  version=model_details.version,
)
print("The current model stage is: '{stage}'".format(stage=model_version_details.current_stage))

# COMMAND ----------

latest_version_info = client.get_latest_versions(ARTIFACT_PATH, stages=["Staging"])

latest_staging_version = latest_version_info[0].version

print("The latest staging version of the model '%s' is '%s'." % (ARTIFACT_PATH, latest_staging_version))

# COMMAND ----------

model_staging_uri = "models:/{model_name}/staging".format(model_name=ARTIFACT_PATH)

print("Loading registered model version from URI: '{model_uri}'".format(model_uri=model_staging_uri))

model_staging = mlflow.prophet.load_model(model_staging_uri)

# COMMAND ----------

model_staging.plot(model_staging.predict(test_df_pd))

# COMMAND ----------

import json

# Return Success
dbutils.notebook.exit(json.dumps({"exit_code": "OK"}))
