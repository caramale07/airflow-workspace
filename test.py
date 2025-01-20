import pandas as pd
import requests
import io
import pyarrow.parquet as pq



# Paths
url = "https://fatulla.codage.az/data/sales_transactions_2m.parquet"
response = requests.get(url)
response.raise_for_status()  # Ensure the request was successful

# Print the first few bytes of the response to check its content
print(response.content[:100])
