from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from datetime import datetime, timedelta

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define the DAG
with DAG(
    dag_id='test_postgres_connection',
    default_args=default_args,
    description='DAG to test PostgreSQL connection',
    schedule_interval=None,  # Run on demand
    start_date=datetime(2025, 1, 1),
    catchup=False,
) as dag:

    # Task to test PostgreSQL connection
    test_postgres_task = PostgresOperator(
        task_id='test_postgres_query',
        postgres_conn_id='postgres',  # Replace with your Connection ID
        sql='SELECT 1;',  # Simple query to test connection
    )

    # Set the task to execute
    test_postgres_task
