# COVID-19 Tracker
This capstone project is centered around tracking the impact of the COVID-19 pandemic across Canada. The architect behind the project has utilized various data analytics services offered by AWS to achieve the outcome. The main data engineering principles used were Data Ingestion, Data Processing/Transformation, Reporting and Visualization. The whole process provides us with insights which can be utilized by business analysts to aid with their research or by data scientists to make predictions.

## Week 1: Run PySpark Application locally
This module uses PySpark to ingest data from two COVID-19 datasets from Public Health Infobase Canada and John Hopkins University, apply transformations on them and create a final dataset which contains metrics like number of confirmed cases, number of recovered cases and the number of deaths from COVID-19 virus in Canada. PySpark is an interface for Apache Spark in Python which allows to write Spark Applications using Python APIs.

* Create a SparkSession object in local mode to start working on dataframes.
* Ingest data from two source csv files containing COVID-19 data each from Public Health Infobase and John Hopkins University.
* Clean the dataframes and apply filter and join transformation on the dataframes.
* Save the final dataframe containing metrics like number of confirmed cases, number of recovered cases and number of deaths from COVID-19 in Canada in an output csv file.

## Week 2: Run PySpark Application on the cloud using Amazon EMR
This module uses Amazon EMR which is a cloud big data platform offered by Amazon Web Services to provision Hadoop cluster on the cloud to run our PySpark Applcation.

* Upload the Public Health Infobase and John Hopkins University datasets to Amazon S3 along with a Bootstrap shell script which contains steps to copy the two datasets from S3 to the EMR cluster.
* Provision the EMR cluster with 1 Master and 2 Core nodes and add your Bootstrap script under Bootstrap actions.
* SSH into your EMR cluster and copy the datasets from linux file system to HDFS file system.
* Update your Python script to load data from Amazon S3 location and save output dataset in Amazon S3 bucket. Run your python job using spark-submit.

## Week 3: Create AWS Data Pipeline service to schedule creation of AWS EMR cluster running our PySpark application
This module uses AWS Data Pipeline Service to automate creation of AWS EMR Cluster and run our python script on the cluster using EMR step(s).

* In AWS Data Pipeline Service, under EMR steps, place the command-runner script and update the Spark job to accept arguments from command-runner.
* Under Pipeline Definition updates, inside your JSON configuration, insert a new key pair "applications":"spark".
* Activate the data pipeline and track the pipeline progress on AWS EMR under Steps.

## Week 4: Configure an AWS Glue Job to run your PySpark Application to analyse COVID-19 data.
AWS Glue service contains a metadata repository known as Data Catalog. Crawlers can be used to crawl source and destination data stores to create data catalogs. We can build a mapping between these catalog tables and define a Glue Job to perform data ingestion.

* Crawl John Hopkins and PHI Covid-19 data from data stores like Amazon S3 and store this data in the form of input catalog tables.
* Configure a Glue Job of type "Spark" to run a PySpark batch job. 
* In our Spark Script, create DynamicFrames from datasource catalog tables, perform transformations and write the output DynamicFrame into a datasink which is an Amazon S3 path.

## Week 5: Create a Tableau Dashboard to visualize the key insights for COVID-19 cases across Canada.
In this module we will setup Tableau Desktop Professional to read from EMR Hadoop Hive table as a data source and create a Dashboard for COVID-19 statistics for Canada.

* Provision AWS EMR Cluster using AWS CLI.
* Go to JupyterHub and create a new notebook and write script for our Spark application.
* For our final dataframe in our Spark Script register a Temp table in Hive and then create a physical table from the Temp table. 
* Launch Tableau Desktop Professional and choose data source as Amazon EMR Hadoop Hive. Under server configuration enter the EMR master node address and port number as 1000(Hive).
* Create a Dashboard for COVID-19 statistics.

## Week 6: Provision an Amazon Redshift Cluster to perform COVID-19 data analysis.
In this module, we provision an Amazon Redshift cluster, Import COVID-19 data into Redshift using AWS Glue Service and perform analysis on the data using Redshift Query Editor.

* Provision an Amazon Redshift Cluster and configure your database.
* Under Redshift service, go to Query Editor and connect to database using database credentials.
* Using AWS Glue Service import data from S3 into Redshift using AWS Glue Job.
* Perform analysis using Redshift Query Editor and export the result table into S3 using UNLOAD method.

## Week 7: Run a PySpark application on an EMR cluster to read from Kafka data source.
In this module we provision an EMR cluster for running our Kafka Streaming Application.

* Provision an Amazon EMR cluster for running our Kafka streaming application.
* Setup Kafka and PySpark on the EMR master node.
* Deploy the Spark Application as per the deployment section of "Structured Streaming + Kafka Integration Guide".
* Push records into the producer node to check if the records are processed in the micro-batch and pushed to the consumer node.

## Week 8: Launch a Microsoft SQL Server database instance on Amazon RDS service.
In this module we provision a Microsoft SQL Server database instance on Amazon RDS to run SQL operations on COVID-19 data.

* Select Engine type as Microsoft SQL Server under the service RDS.
* Under SQL Server Management Studio, connect to your database endpoint.
* Create a new database, import your tables and start working on SQL operations.
* A number of SQL operations like Selection/Projection, Filtering, Join, Aggregations and Sorting are performed.

## Week 9: Create an AWS Lambda function and deploy pandas package to AWS Lambda Layer to run our data analysis. 
In this module we create an AWS Lambda function, deploy pandas package to AWS Lambda Layer and perform COVID-19 analysis using pandas library for python.

* Launch an EC2 Instance and create our pandas package for python 3.8 runtime.
* Deploy our pandas package to AWS Lambda Layer.
* Perform data analysis for COVID-19 using this pandas library for python.
* Visualize statistics for COVID-19 using matplotlib library for python.

## Week 10: Create a notebook under Databricks Workspace to perform analytics using SparkSQL.
In this module we launch a Databricks cluster, attach notebook to the cluster and perform SparkSQL operations on COVID-19 data.

* Create a cross-account role under AWS IAM service for databricks account.
* Create a new cluster under databricks workspace and Create a new notebook.
* Create a SparkSQL table from your source CSV data and perform queries on your table using SparkSQL.
* Visualize the results table using out of the box display and displayHTML functions of Databricks to find insights regarding COVID-19.

## Week 11: Integrate Amazon API Gateway with DynamoDB to Query, Bulk Update and Insert Records in a table.
In this module we integrate Amazon API Gateway service with DynamoDB to perform operations on a table containing COVID-19 data.

* Insert records into DynamoDB using API Gateway triggers by configuring a POST method.
* Bulk update DynamoDB records using Amazon S3 file update as trigger.
* Query DynamoDB table using API Gateway by configuring a GET method.

## Week 12: Integrate Amazon API Gateway with DynamoDB to delete records from a table.
In this module we continue to integrate Amazon API Gateway service with DynamoDB table to perform delete operation on a table containing COVID-19 data.

* Under the API Endpoint Resource create a new DELETE method.
* Configure a mapping template for the DELETE request.
* To invoke the API, along with the API Gateway URL, send the record key ID as a query string parameter.
* Confirm that the record is deleted in the DynamoDB table.
