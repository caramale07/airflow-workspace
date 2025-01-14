from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

def print_hello():
    print("Hello, Airflow!")

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 1,
}

with DAG(
    'example_dag',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False,
) as dag:
    task = PythonOperator(
        task_id='hello_task',
        python_callable=print_hello,
    )
