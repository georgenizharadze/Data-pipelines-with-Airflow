## Overview

This is an educational practice project to learn how to build data pipelines with Apache Airflow. It is based on a hypothetical case of a music streaming company which has decided to introduce automation and monitoring to their data warehouse ETL pipelines, utilizing Apache Airflow.

The source data resides in S3 and needs to be transferred to and processed in a data warehouse in Amazon Redshift. The source datasets consist of JSON logs that tell about user activity in the application and JSON metadata about the songs the users listen to.

The Airflow code in this repository achieves the following aims:
* Backfills the historical data from S3 into Redshift. As the log data are in files timestamped by days, the Airflow schedule is set up on a daily basis, processing new log files at each step. 
* The pipeline is built from reusable tasks and connections 
* Data quality checks are implemented at the end of each DAG run

## Requirements 

* Python 3
* Access to AWS S3 and Redshift 
* Airflow 1.10.4 or later
* Airflow cluster

Please note that this repository does not contain instructions for deploying an Airflow cluster. This will depend on our choice of independent deployment or the use a managed service. You will have to do your own research on the subject and take appropriate steps. 

## Running the code

Once you have deployed a cluster and installed Airflow, place the folders and files of this repository into your Airflow home directory, which will be created once you [install Airflow](https://airflow.apache.org/installation.html). You should then start [Airflow User Interface](https://airflow.apache.org/ui.html) and run the DAG from there. You will be able to monitor the progress, view log files, debug as required, etc. The DAG code, which is responsible for the sequencing and scheduling of all taks, resides in the `dags` folder. Operator classes and SQL statements are in the `plugins` folder.   