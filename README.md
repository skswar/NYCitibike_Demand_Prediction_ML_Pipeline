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

### Building ETL Pipeline
Given the scale of the data involved, it was imperative to design an optimized and efficient data pipeline that could seamlessly handle both incoming and existing data. The project utilized two primary data sources: historical data files spanning a two-year period, and live data updates received every 30 minutes. The objective was to leverage the historical data for training a forecasting model, validate its performance using the live data, and utilize the model to predict demand for the next 24 hours or more.<br>
To achieve this, separate table structures were created for the historic and real-time data. The dataset consisted of two years' worth of trip history details, two years of historical weather data (with occasional gaps), and three other data sources updated every 30 minutes. The pipeline architecture followed the Medallion format, where raw data was stored in bronze tables, and data relevant for model training was cleaned, merged, and stored in silver tables. Additionally, API calls were made to address missing weather data.<br>

<div align="center">
<img src="https://raw.githubusercontent.com/skswar/NYCitibike_Demand_Prediction_ML_Pipeline/master/img/logo.png" alt="Databricks and Citibike Image" height=350 width=750/>
</div>

This carefully designed architecture ensured a robust and efficient data pipeline, facilitating the extraction, transformation, and loading of data for analysis and modeling purposes.



