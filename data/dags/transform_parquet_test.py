from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import pandas as pd
import os

# Paths
url = "https://github.com/caramale07/airflow-workspace/raw/refs/heads/master/data/sales_transactions.parquet"
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

# Validation function
def validate_transformed_file():
    # Check if the output file exists
    if not os.path.exists(output_file):
        raise FileNotFoundError(f"Transformed file {output_file} does not exist.")
    
    # Read the transformed file
    df = pd.read_parquet(output_file)
    
    # Validate required columns
    required_columns = {"year", "month", "day", "total_price"}
    if not required_columns.issubset(df.columns):
        raise ValueError(f"Missing required columns in the transformed file. Found columns: {df.columns}")
    
    print(f"Validation passed: Transformed file at {output_file} is correct.")

# Default arguments
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 1, 15),
}

# DAG definition
with DAG(
    dag_id="parquet_transform_dag",
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
) as dag:

    transform_task = PythonOperator(
        task_id="transform_parquet",
        python_callable=transform_parquet,
    )

    validate_task = PythonOperator(
        task_id="validate_transformed_file",
        python_callable=validate_transformed_file,
    )

    # Set task dependencies
    transform_task >> validate_task
