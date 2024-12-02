# Databricks notebook source
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

def print_hello():
    print("Hello from Airflow!")

# Define the DAG
with DAG(
    dag_id="data_frame_read",  
    start_date=datetime(2024, 1, 1),  
    schedule="*/30 * * * *",  # Use CRON syntax for a 30-minute interval
) as dag:
    
    # Define the task
    task1 = PythonOperator(
        task_id="daily_work",
        python_callable=print_hello,
    )