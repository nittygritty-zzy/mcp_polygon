#!/usr/bin/env python3
"""Test DuckDB's automatic schema inference."""

import duckdb

# Connect to DuckDB
con = duckdb.connect()

# Use an existing parquet file
parquet_file = 'cache/list_treasury_yields/2025/data.parquet'

# Let DuckDB infer the schema automatically
result = con.execute(f"""
    DESCRIBE SELECT * FROM read_parquet('{parquet_file}')
""").fetchall()

print(f"DuckDB's automatic schema inference for {parquet_file}:")
print("=" * 80)
for col_name, col_type, null, key, default, extra in result:
    print(f"{col_name:40s} {col_type}")

print("\n" + "=" * 80)
print("Actual data (first 3 rows):")
print("=" * 80)

result = con.execute(f"SELECT * FROM read_parquet('{parquet_file}') LIMIT 3").fetchdf()
print(result)

con.close()
