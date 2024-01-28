import duckdb
# conn = duckdb.connect("bigmac_index.db")
# df = duckdb.read_csv("big-mac-full-index.csv")
# print(df)

import duckdb
import pandas as pd

# Connect to DuckDB database (create if not exists)
con = duckdb.connect(database=':memory:', read_only=False)

# Specify the path to your CSV file
csv_file_path = 'source/big-mac-full-index.csv'
table_name = 'big_mac_index'
with open(csv_file_path, 'r') as csv_file:
    # Read the header to get column names
    header = csv_file.readline().strip().split(',')
    create_table_query = f'CREATE TABLE {table_name} ({", ".join(f"{col} STRING" for col in header)})'
    con.execute(create_table_query)
    insert_data_query = f'INSERT INTO {table_name} VALUES ({", ".join("?" for _ in header)})'

    for line in csv_file:
        values = line.strip().split(',')
        con.execute(insert_data_query, values)
# Specify the DuckDB table name
result = con.execute(f'SELECT * FROM {table_name} limit 10')

# Fetch and print the result
print(result.fetchall())
con.close()