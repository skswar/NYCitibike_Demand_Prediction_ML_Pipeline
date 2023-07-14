<div align="center">
<img src="https://raw.githubusercontent.com/skswar/NYCitibike_Demand_Prediction_ML_Pipeline/master/img/logo.png" alt="Databricks and Citibike Image" height=350 width=750/>
</div>

## Building an end to end machine learning pipeline that can predict the hourly demand of NY Citi-Bikes at a particular station

<hr>

## Table of contents

* [Introduction](#introduction)
* [Methodology](#methodology)
  * [Building ETL](#building-etl)
  * [Performing EDA](#performing-eda)
  * [Buidling ML Model](#buidling-ml-model)
  * [ML Model Registry and Real Time Model Updation](#ml-model-registry-and-real-time-model-updation)
  

<hr>

## Introduction
Implemented in New York City, Citi Bikes have become a popular bike-sharing system, providing convenient transportation for residents and tourists alike. With millions of daily customers using Citi Bikes for various purposes such as commuting, shopping, and leisure, the system has over 1500+ stations across New York and Jersey City. Ensuring an adequate supply of bikes at each station can be challenging, but data science plays a crucial role in addressing this issue. By employing an end-to-end machine learning pipeline, the usage of bikes can be tracked, and hourly predictions of bike rides can be made. This enables the business to better understand demand patterns, timely replenish bikes at the docks, and manage the distribution of bikes across different stations. Ultimately, this data-driven approach enhances operational efficiency, resulting in increased ride availability and satisfied customers.

## Methodology
To efficiently handle the large volume of data and deliver timely insights, a well-structured pipeline was designed. The project plan was divided into four key sections:<br>
  1. Building an ETL (Extract, Transform, Load) pipeline to handle the hourly influx of new and historical data.
  2. Conducting exploratory data analysis (EDA) to identify relevant features and patterns.
  3. Developing a machine learning model to make accurate predictions based on the identified features.
  4. Establishing a machine learning model registry for tracking and updating the models over time.

This approach ensured a streamlined process, enabling the team to effectively handle the data, gain insights, and continuously improve the machine learning models.

