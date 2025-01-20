from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import pandas as pd
import numpy as np

# Function to generate the Parquet data
def generate_parquet_data(**kwargs):
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

    # Push the DataFrame to XCom
    kwargs['ti'].xcom_push(key='parquet_data', value=df)
    print("Generated Parquet data")

# Function to transform the Parquet data
def transform_parquet_data(**kwargs):
    # Pull the DataFrame from XCom
    df = kwargs['ti'].xcom_pull(key='parquet_data', task_ids='generate_parquet_data')

    # Transform the data
    df["year"] = df["timestamp"].dt.year
    df["month"] = df["timestamp"].dt.month
    df["day"] = df["timestamp"].dt.day
    df["total_price"] = df["quantity"] * df["price"]

    print(f"Transformed Parquet data: {df.shape[0]} rows")
    print(df.head())

# Default arguments
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 1, 15),
}

# DAG definition
with DAG(
    dag_id="in_memory_parquet_transform_dag",
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
) as dag:

    generate_task = PythonOperator(
        task_id="generate_parquet_data",
        python_callable=generate_parquet_data,
        provide_context=True,
    )

    transform_task = PythonOperator(
        task_id="transform_parquet_data",
        python_callable=transform_parquet_data,
        provide_context=True,
    )

    # Set task dependencies
    generate_task >> transform_task
