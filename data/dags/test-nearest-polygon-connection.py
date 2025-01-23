from airflow import DAG
from airflow.providers.http.hooks.http import HttpHook
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import json

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Function to test the POST request
def test_post_request(**kwargs):
    # Retrieve the HTTP connection
    http_conn_id = "nearest-polygon	"  # 
    hook = HttpHook(http_conn_id=http_conn_id, method="POST")

    # Define the request body
    payload = {
        "tractors": [
            {
                "lat": 40.059299468658345,
                "lon": 47.488236399638986,
                "unit_id": 5143
            },
            {
                "lat": 39.44768715462906,
                "lon": 48.57286982635369,
                "unit_id": 5144
            },
            {
                "lat": 38.8689365683384,
                "lon": 48.801155219874104,
                "unit_id": 5145
            }
        ]
    }

    # Make the POST request
    response = hook.run(endpoint="", data=json.dumps(payload), headers={"Content-Type": "application/json"})

    # Log the response
    print("Response status code:", response.status_code)
    print("Response body:", response.text)

# Define the DAG
with DAG(
    dag_id="test_post_request_dag",
    default_args=default_args,
    description="DAG to test a POST request to the nearest_cluster endpoint",
    schedule_interval=None,  # Run on demand
    start_date=datetime(2025, 1, 1),
    catchup=False,
) as dag:

    # Task to test the POST request
    test_post_request_task = PythonOperator(
        task_id="test_post_request_task",
        python_callable=test_post_request,
        provide_context=True,
    )

    # Set the task to run
    test_post_request_task
