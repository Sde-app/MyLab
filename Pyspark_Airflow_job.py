# Databricks notebook source
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime, timedelta

# Default arguments for the DAG
default_args = {
    'owner': 'suresh',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),  # Wait 5 minutes before retrying
}

# Define the DAG
with DAG(
    dag_id='schedule_df_read_spark_job',
    default_args=default_args,
    description='DAG to schedule the df_read Spark job',
    schedule_interval='@daily',  # Adjust the schedule as per your requirements
    start_date=datetime(2024, 11, 29),
    catchup=False,
    tags=['spark', 'data-processing'],  # Tags for easier identification
) as dag:
    
    # Define the SparkSubmitOperator
    run_df_read = SparkSubmitOperator(
        task_id='run_df_read',
        application='/home/sde/MyLab/df_read.py',  # Replace with the actual path to your script
        conn_id='spark_default',  # Ensure this matches your Airflow Spark connection
        application_args=[],  # Add arguments if needed
        name='df_read_spark_job',
        verbose=True,
        driver_memory='2g',  # Adjust Spark configurations as needed
        executor_memory='4g',
        num_executors=2,
    )

    # Adding the task to the DAG
    run_df_read
