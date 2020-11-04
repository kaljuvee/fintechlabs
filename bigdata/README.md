# Overview

The aim of this sample job is to create a set of simple JSON extract files from various csv sources based on specific conditionis (weekly aggregation)

# Running Job

After cloning this repository and installing the standard Python and Spark (cluster) environments, run the following command:

`spark-submit run_spark_test.py sales.csv calendar.csv store.csv product.csv`

The output should be 70 specific JSON files as described below.

# Data

* Source files sales.csv, calendar.csv, store.csv and product.csv
* Destination files format consumption.json


# Description 

* Sales data contains transactional data with the following columns netSales, salesUnits, dateId, productId and storeId. These fields need to be joined with other fields from other csv’s to get the weekly aggregated value for each combination.

* Calendar data contains dateId, week number and year. (Criteria: sales.dateId should be equal to calendar.dateId to get weekNumber and year) 

* Product data contains productId vs hierarchy. (division, gender, category) (Criteria : sales.productId = product.productId)

* Store data contains store vs channel information. (Criteria:sales.storeId = store.storeId)

# Output
Output should be bunch of json files which is having the weekly aggregated values of net_sales and sales_units. The json files will have unique key. (Refer: consumption.json) 
