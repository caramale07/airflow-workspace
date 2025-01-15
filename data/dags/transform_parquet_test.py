from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import pandas as pd
import os

# File paths
RAW_FILE = "data/raw_sales.parquet"
TRANSFORMED_FILE = "data/transformed_sales.parquet"

# Load data
def load_data():
    url = "https://github.com/caramale07/airflow-workspace/raw/refs/heads/master/data/sales_transactions.parquet"
    os.makedirs(os.path.dirname(RAW_FILE), exist_ok=True)
    pd.read_parquet(url).to_parquet(RAW_FILE, index=False)
    print(f"Raw data saved at {RAW_FILE}")

# Transform data
def transform_data():
    df = pd.read_parquet(RAW_FILE)
    df["year"] = df["timestamp"].dt.year
    df["month"] = df["timestamp"].dt.month
    df["day"] = df["timestamp"].dt.day
    df["total_price"] = df["quantity"] * df["price"]
    os.makedirs(os.path.dirname(TRANSFORMED_FILE), exist_ok=True)
    df.to_parquet(TRANSFORMED_FILE, index=False)
    print(f"Transformed data saved at {TRANSFORMED_FILE}")

# Validate data
def validate_data():
    if not os.path.exists(TRANSFORMED_FILE):
        raise FileNotFoundError(f"Transformed file {TRANSFORMED_FILE} not found.")
    
    df = pd.read_parquet(TRANSFORMED_FILE)
    required_columns = {"year", "month", "day", "total_price"}
    if not required_columns.issubset(df.columns):
        raise ValueError(f"Missing required columns in {TRANSFORMED_FILE}. Found: {df.columns}")
    print(f"Validation passed: {TRANSFORMED_FILE}")

# Default arguments
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 1, 15),
}

# DAG definition
with DAG(
    dag_id="modular_data_pipeline",
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
) as dag:

    load_task = PythonOperator(
        task_id="load_data",
        python_callable=load_data,
    )

    transform_task = PythonOperator(
        task_id="transform_data",
        python_callable=transform_data,
    )

    validate_task = PythonOperator(
        task_id="validate_data",
        python_callable=validate_data,
    )

    # Define task dependencies
    load_task >> transform_task >> validate_task
