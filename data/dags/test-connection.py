from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.http.hooks.http import HttpHook
from datetime import datetime
import pandas as pd

# Transformation function
def transform_parquet():
    # Use HttpHook to retrieve the URL
    http_hook = HttpHook(http_conn_id="fatulla-codage-az", method="GET")
    url = f"{http_hook.base_url}/data/sales_transactions_1m.parquet"
    
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
    dag_id="parquet_transform_dag_with_connection",
    default_args=default_args,
    schedule_interval="*/10 * * * *",  # Cron expression
    catchup=False,
) as dag:

    transform_task = PythonOperator(
        task_id="transform_parquet_task",
        python_callable=transform_parquet,
    )

    # Set task dependencies
    transform_task
