<div align="center">
<img src="https://raw.githubusercontent.com/skswar/NYCitibike_Demand_Prediction_ML_Pipeline/master/img/logo_main.png" alt="Databricks and Citibike Image"/>
</div>

## Building an end to end machine learning pipeline that can predict the hourly demand of NY Citi-Bikes at a particular station

<hr>

## Table of contents

* [Introduction](#introduction)
* [Software Platform](#software-platform)
* [Methodology](#methodology)
  * [Building ETL](#building-etl)
  * [Performing EDA](#performing-eda)
  * [Buidling ML Model](#buidling-ml-model)
  * [ML Model Registry and Real Time Model Updation](#ml-model-registry-and-real-time-model-updation)
  

<hr>

## Introduction
Implemented in New York City, Citi Bikes have become a popular bike-sharing system, providing convenient transportation for residents and tourists alike. With millions of daily customers using Citi Bikes for various purposes such as commuting, shopping, and leisure, the system has over 1500+ stations across New York and Jersey City. Ensuring an adequate supply of bikes at each station can be challenging, but data science plays a crucial role in addressing this issue. By employing an end-to-end machine learning pipeline, the usage of bikes can be tracked, and hourly predictions of bike rides can be made. This enables the business to better understand demand patterns, timely replenish bikes at the docks, and manage the distribution of bikes across different stations. Ultimately, this data-driven approach enhances operational efficiency, resulting in increased ride availability and satisfied customers.

## Software Platform
To address the data-intensive nature of the problem and optimize processing speed, the entire application was developed using the **Databricks** platform. **Python** and **Pyspark SQL** were the primary programming languages utilized for coding purposes.

## Methodology
To efficiently handle the large volume of data and deliver timely insights, a well-structured pipeline was designed. The project plan was divided into four key sections:<br>
  1. Building an ETL (Extract, Transform, Load) pipeline to handle the hourly influx of new and historical data.
  2. Conducting exploratory data analysis (EDA) to identify relevant features and patterns.
  3. Developing a machine learning model to make accurate predictions based on the identified features.
  4. Establishing a machine learning model registry for tracking and updating the models over time.

This approach ensured a streamlined process, enabling the team to effectively handle the data, gain insights, and continuously improve the machine learning models.

### Building ETL
Given the scale of the data involved, it was imperative to design an optimized and efficient data pipeline that could seamlessly handle both incoming and existing data. The project utilized two primary data sources: historical data files spanning a two-year period, and live data updates received every 30 minutes. The objective was to leverage the historical data for training a forecasting model, validate its performance using the live data, and utilize the model to predict demand for the next 24 hours or more.<br>
To achieve this, separate table structures and relations were created for the historic and real-time data. The dataset consisted of two years' worth of trip history details, two years of historical weather data (with occasional gaps), and three other data sources updated every 30 minutes. The pipeline architecture followed the Medallion format, where raw data was stored in bronze tables, and data relevant for model training was cleaned, merged, and stored in silver tables. Additionally, API calls were made to address missing weather data.<br>

<p align="center">
<img src="https://raw.githubusercontent.com/skswar/NYCitibike_Demand_Prediction_ML_Pipeline/master/img/ETL_Arch.png" alt="ETL Architecture height="70%" width="70%"/></p>

This carefully designed architecture ensured a robust and efficient data pipeline, facilitating the extraction, transformation, and loading of data for analysis and modeling purposes.

### Performing EDA
Exploratory data analysis (EDA) played a pivotal role in this study as it provided valuable insights into the usage patterns and operational demand of Citi Bikes. Key findings from the analysis include:

+ Seasonal Variations: Seasonal variations played a crucial role in ride counts. During winter, ride counts decreased due to snowfall and unfavorable weather conditions, whereas ride counts increased during summer and fall.
<p align="center">
<img src="https://raw.githubusercontent.com/skswar/NYCitibike_Demand_Prediction_ML_Pipeline/master/img/yearmonth_1.png" width="450" height="200"/>
<img src="https://raw.githubusercontent.com/skswar/NYCitibike_Demand_Prediction_ML_Pipeline/master/img/yearmonth_2.png" width="450" height="200"/>
</p>
  
+ Weekend Effect: There was a noticeable decrease in ride counts during weekends, suggesting a shift in user behavior. Factors such as visibility, cloud cover, and rain emerged as significant contributors to this trend.
<p align="center">
<img src="https://raw.githubusercontent.com/skswar/NYCitibike_Demand_Prediction_ML_Pipeline/master/img/weekend-effect.png" width="500" height="200"/>
</p>

+ Hourly Patterns: Ride counts exhibited distinct patterns based on the hour of the day. Increased ride activity was observed during early morning and evening hours, corresponding to office commute times. This finding indicated a substantial user base consisting of daily office goers.
<p align="center">
<img src="https://raw.githubusercontent.com/skswar/NYCitibike_Demand_Prediction_ML_Pipeline/master/img/hourofday-effect.png" width="500" height="200"/>
</p>

+ Holiday Impact: Holidays had a notable impact on ride counts, with a decline observed during these periods. In the are plot below, significant dips in bike usage were observed on specific dates, including Thanksgiving (Nov 25, 2021), Christmas Day (Dec 25, 2021), a snowstorm (Jan 29, 2022), President's Day (Feb 21, 2022), Easter (Apr 17), and Independence Day (Jul 4). These events and holidays contributed to reduced bike activity, indicating the influence of such occasions on Citi Bikes' usage.
<p align="center">
<img src="https://raw.githubusercontent.com/skswar/NYCitibike_Demand_Prediction_ML_Pipeline/master/img/holiday-effect.png" width="450" height="200"/>
<img src="https://raw.githubusercontent.com/skswar/NYCitibike_Demand_Prediction_ML_Pipeline/master/img/monthly-effect.png" width="450" height="200"/>
</p>

+ Temperature Influence: While not a dominant factor, a significant rise in temperature was found to reduce ride counts. This observation underscores the sensitivity of ridership to changes in weather conditions.
<p align="center">
<img src="https://raw.githubusercontent.com/skswar/NYCitibike_Demand_Prediction_ML_Pipeline/master/img/temp-effect.png" width="500" height="200"/>
</p>

+ Cloud/Visibility Effect: The graph clearly illustrates that snowy and rainy weather conditions resulted in a significant decrease in the number of rides, while riders continued to use the service during cloudy and clear sky conditions.
<p align="center">
<img src="https://raw.githubusercontent.com/skswar/NYCitibike_Demand_Prediction_ML_Pipeline/master/img/cloudvisibility-effect.png" width="500" height="200"/>
</p>

These findings played a **critical role in informing the subsequent data modeling** process, enabling the development of models tailored to improve prediction accuracy.

