import pandas as pd
import requests
import io
import pyarrow.parquet as pq
import pyarrow as pa


# test data
data = {
    'name': ['John', 'Anna', 'Peter', 'Linda'],
    'age': [24, 13, 53, 33],
    'city': ['New York', 'Paris', 'Berlin', 'London']
}

df = pd.DataFrame(data)

# write to parquet
table = pa.Table.from_pandas(df)
pq.write_table(table, 'data.parquet')

