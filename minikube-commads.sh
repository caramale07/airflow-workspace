minikube start --cpus=15 --memory=7000

minikube start --memory='max' --cpus=all

minikube service airflow-flower  -n airflow —url

minikube service airflow-webserver -n airflow —url


