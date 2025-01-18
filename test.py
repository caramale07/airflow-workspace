import pandas as pd
import os

# Paths
url = "https://fatulla.codage.az/data/sales_transactions.parquet"
output_file = "data/sales_transactions_transformed.parquet"



df = pd.read_parquet(url)

# Transform: Split the timestamp into year, month, day
df["year"] = df["timestamp"].dt.year
df["month"] = df["timestamp"].dt.month
df["day"] = df["timestamp"].dt.day

# Additional transformation: Calculate total price
df["total_price"] = df["quantity"] * df["price"]

# Save transformed data to a new Parquet file
df.to_parquet(output_file, index=False)

print(f"Transformed Parquet file saved at {output_file}")
print(df.head())

