helm install airflow apache-airflow/airflow --namespace airflow --set executor=CeleryExecutor --set workers.replicas=2  --set flower.enabled=true --set postgresql.enabled=true --set redis.enabled=true --set dags.persistence.enabled=true --set logs.persistence.enabled=true

helm install airflow apache-airflow/airflow --namespace airflow-kubernetes --create-namespace -f airflow-kubernetes.yaml

helm upgrade airflow apache-airflow/airflow --namespace airflow --create-namespace -f values.yaml

helm uninstall airflow apache-airflow/airflow --namespace airflow