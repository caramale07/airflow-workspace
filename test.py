import pandas as pd
url = "https://github.com/caramale07/airflow-workspace/raw/refs/heads/master/data/sales_transactions.parquet"
df = pd.read_parquet(url)
print(df.head())