from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import pandas as pd
import os
import requests

# Paths
url = "https://fatulla.codage.az/data/sales_transactions_2m.parquet"

# Transformation function
def transform_parquet():
    # Read the Parquet file
    df = pd.read_parquet(url)
    
    # Transform: Split the timestamp into year, month, day
    df["year"] = df["timestamp"].dt.year
    df["month"] = df["timestamp"].dt.month
    df["day"] = df["timestamp"].dt.day
    
    # Additional transformation: Calculate total price
    df["total_price"] = df["quantity"] * df["price"]
    
    print(f"Transformed Parquet file: {df.shape[0]} rows")
    print(df.head())

# Default arguments
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 1, 15),
}

# DAG definition
with DAG(
    dag_id="parquet_transform_dag_v1",
    default_args=default_args,
    schedule_interval="*/10 * * * *",  # Cron expression
    catchup=False,
) as dag:

    transform_task = PythonOperator(
        task_id="transform_parquet_v1",
        python_callable=transform_parquet,
    )

    # Set task dependencies
    transform_task
