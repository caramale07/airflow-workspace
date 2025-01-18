kubectl describe pod airflow-worker-0 -n airflow

kubectl get pvc -n airflow

kubectl scale statefulset airflow-worker --replicas=0 -n airflow

kubectl delete statefulset airflow-worker --cascade=orphan -n airflow

