minikube start --cpus=10 --memory=6000 --disk-size=100g

minikube start --memory='max' --cpus=all

minikube service airflow-flower  -n airflow —url

minikube service airflow-webserver -n airflow —url


