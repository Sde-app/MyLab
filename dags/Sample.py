# Databricks notebook source
from airflow import DAG
from airflow.operators.python import PythonOperator  # Correct import path
from datetime import datetime, timedelta  # Import timedelta for retry delay

# Define the Python function that will be executed
def print_date():
    print(f"Current date is: {datetime.now()}")

# Define the default arguments dictionary for the DAG
default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Instantiate the DAG
with DAG(
    'example_dag',  # DAG ID
    default_args=default_args,  # Default arguments
    description='A simple example DAG',
    schedule_interval='0 6 * * *',  # Runs every day at 6 AM
    start_date=datetime(2024, 12, 1),  # Start date
    catchup=False,  # Don't run historical tasks
) as dag:

    # Define a task in the DAG
    task = PythonOperator(
        task_id='print_current_date',  # Task ID
        python_callable=print_date,  # Function to run
    )