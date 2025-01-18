# Use the official Apache Airflow image as the base image
FROM apache/airflow:2.8.4-python3.9

# Install additional Python packages
RUN pip install --no-cache-dir datasets
