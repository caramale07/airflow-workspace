from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import pandas as pd
import numpy as np
import dill  # For serialization

# Path for temporary storage
TEMP_FILE = "/tmp/generated_data.pkl"

# Step 1: Generate Data
def generate_parquet_data():
    rows = 300_000  # Reduced size for scalability
    data = {
        "transaction_id": np.arange(1, rows + 1),
        "product_id": np.random.randint(1, 1000, size=rows),
        "quantity": np.random.randint(1, 20, size=rows),
        "price": np.random.uniform(5.0, 500.0, size=rows).round(2),
        "timestamp": pd.date_range(start="2022-01-01", periods=rows, freq="S"),
    }

    # Create DataFrame
    df = pd.DataFrame(data)

    # Serialize DataFrame to a temporary file
    with open(TEMP_FILE, "wb") as f:
        dill.dump(df, f)

    print(f"Data serialized and saved to {TEMP_FILE}")

# Step 2: Transform Data
def transform_parquet_data():
    # Deserialize DataFrame from the temporary file
    with open(TEMP_FILE, "rb") as f:
        df = dill.load(f)

    # Perform transformations
    df["year"] = df["timestamp"].dt.year
    df["month"] = df["timestamp"].dt.month
    df["day"] = df["timestamp"].dt.day
    df["total_price"] = df["quantity"] * df["price"]

    print(f"Transformed Data: {df.shape[0]} rows")
    print(df.head())

# DAG Definition
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 1, 15),
}

with DAG(
    dag_id="temp_file_data_transfer_dag",
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
) as dag:

    generate_task = PythonOperator(
        task_id="generate_parquet_data",
        python_callable=generate_parquet_data,
    )

    transform_task = PythonOperator(
        task_id="transform_parquet_data",
        python_callable=transform_parquet_data,
    )

    generate_task >> transform_task
