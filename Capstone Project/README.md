# Data Engineering Capstone Project

## Project Summary
The objective of this project was to create an ETL pipeline for the tffect of tollution on temperature change  in New York city US to form an analytic data.

## Data and Code
All the data for this project was loaded anda generated into local workspace.<br><br> 
Datasets:
* **../../data2/GlobalLandTemperaturesByCity.csv --> local dataset**
* **data/in/pollution_us_2000_2016.csv --> https://www.kaggle.com/datasets/sogun3/uspollution?resource=download&select=pollution_us_2000_2016.csv**

In addition to the datasets, the project workspace includes:
* **etl.py** - reads data, processes that data using Pandas, and writes processed data as a parquet file in /data/out/ directory in workspace.
* **config/config.cfg** - contains configuration that allows the ETL pipeline to access AWS EMR cluster. 
* **Capstone_Project.ipynb** - jupyter notebook that was used for building a correlation of pollution with temperatures in 2013 in New York US city.

## Prerequisites
* python 3
* pip install statsmodels
* pip install sklearn
* pip install plotly
* pip install seaborn
* pip install pyarrow
  
## Step 1: Scope the Project and Gather Data
### Project Scope
To create the analytics parquet file, the following steps will be carried out from ETL:
* Use Pandas to load the data into dataframes.
* Perform data cleaning functions on all the datasets.
* Merge datasets
* Create parquet file.

The technology used in this project is a modern Big Data pipeline ingestion. 
## Step 2: Explore and Assess the Data
> Refer to the jupyter notebook for exploratory data analysis

## Step 3: Define the Data Model
### 3.1 Conceptual Data Model

Parquet file is generated with schema:<br>
* CO AQI
* SO2 AQI
* AverageTemperature
* NO2 AQI
* Date Local

### 3.2 Mapping Out Data Pipelines
The pipeline steps are as follows:
* Load the datasets
* Clean Temperature dataframe
* Clean Pollution dataframe
* Merge dataframes
* Generate and load parquet data

## Step 4: Run Pipelines to Model the Data 
The ETL pipeline is defined in the etl.py script.<br><br>
**Run pipeline ETL**
> python3 etl.py<br>

and/or Verify results in Capstone_Project.ipynb
<br><br>
###By
####Fabio Haider