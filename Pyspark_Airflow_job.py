from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime

# Define default_args
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
}

# Define the DAG
with DAG(
    dag_id='spark_job_schedule',
    default_args=default_args,
    description='A simple DAG to schedule a Spark job',
    schedule_interval='@daily',  # Run daily
    start_date=datetime(2024, 11, 29),
    catchup=False,
) as dag:
    
    # Define the SparkSubmitOperator
    run_spark_job = SparkSubmitOperator(
        task_id='run_spark_job',
        application='/path/to/your/spark_script.py',  # Path to your Spark script
        conn_id='spark_default',
        application_args=['arg1', 'arg2'],  # Pass arguments to your script if needed
        name='example_spark_job',
        verbose=True,
    )

    run_spark_job
