from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import pandas as pd
import os
import requests

# Paths
url = "https://fatulla.codage.az/data/sales_transactions.parquet"
output_file = "data/sales_transactions_transformed.parquet"



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
    
    # Save transformed data to a new Parquet file
    os.makedirs(os.path.dirname(output_file), exist_ok=True)
    df.to_parquet(output_file, index=False)

    print(f"Transformed Parquet file saved at {output_file}")
    print(df.head())

# Default arguments
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 1, 15),
}

# DAG definition
with DAG(
    dag_id="parquet_transform_dag-v1",
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
) as dag:

    transform_task = PythonOperator(
        task_id="transform_parquet-v1",
        python_callable=transform_parquet,
    )

    # Set task dependencies
    transform_task 
