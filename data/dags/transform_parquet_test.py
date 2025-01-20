from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import pandas as pd
import numpy as np
import os

# Generate sample Parquet file locally
def generate_parquet_file():
    output_dir = "data"
    os.makedirs(output_dir, exist_ok=True)
    
    # Generate 3 million rows of sample sales transaction data
    rows = 3_000_000
    data = {
        "transaction_id": np.arange(1, rows + 1),
        "product_id": np.random.randint(1, 1000, size=rows),
        "quantity": np.random.randint(1, 20, size=rows),
        "price": np.random.uniform(5.0, 500.0, size=rows).round(2),
        "timestamp": pd.date_range(start="2022-01-01", periods=rows, freq="S"),
    }

    # Create DataFrame
    df = pd.DataFrame(data)

    # Save to Parquet
    output_file = os.path.join(output_dir, "sales_transactions_3m.parquet")
    df.to_parquet(output_file, index=False)
    print(f"Parquet file generated at {output_file}")

# Transformation function
def transform_parquet():
    input_file = "data/sales_transactions_3m.parquet"
    if not os.path.exists(input_file):
        raise FileNotFoundError(f"Parquet file not found at {input_file}")

    # Read the Parquet file
    df = pd.read_parquet(input_file)

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
    dag_id="local_parquet_transform_dag-v1",
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
) as dag:

    generate_task = PythonOperator(
        task_id="generate_parquet_file",
        python_callable=generate_parquet_file,
    )

    transform_task = PythonOperator(
        task_id="transform_parquet",
        python_callable=transform_parquet,
    )

    # Set task dependencies
    generate_task >> transform_task
