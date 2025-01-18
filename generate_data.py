import pandas as pd
import numpy as np
import os

# Create directory if it doesn't exist
output_dir = "data"
os.makedirs(output_dir, exist_ok=True)

# Generate 1 million rows of sample sales transaction data
rows = 10_000_000
data = {
    "transaction_id": np.arange(1, rows + 1),
    "product_id": np.random.randint(1, 1000, size=rows),
    "quantity": np.random.randint(1, 20, size=rows),
    "price": np.random.uniform(5.0, 500.0, size=rows).round(2),
    "timestamp": pd.date_range(start="2022-01-01", periods=rows, freq="S"),
}

# Create DataFrame
df = pd.DataFrame(data)

# Save to Parquet
output_file = os.path.join(output_dir, "sales_transactions.parquet")
df.to_parquet(output_file, index=False)

print(f"Parquet file generated at {output_file}")
