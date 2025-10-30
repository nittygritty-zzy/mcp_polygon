#!/usr/bin/env python3
"""Test DuckDB's CSV schema inference."""

import duckdb
import tempfile

# Create a sample CSV with mixed types (simulating our API responses)
sample_csv = """ticker,price,volume,change_percent,is_active,updated_at
AAPL,150.25,1000000,2.5,true,2025-01-15
MSFT,350.00,500000,-1.2,true,2025-01-15
GOOGL,140.75,750000,0.8,false,2025-01-15"""

# Write to temp file
with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as f:
    f.write(sample_csv)
    csv_file = f.name

con = duckdb.connect()

print("=" * 80)
print("DuckDB's AUTOMATIC CSV schema inference:")
print("=" * 80)

# Let DuckDB automatically infer types from CSV
result = con.execute(f"""
    DESCRIBE SELECT * FROM read_csv('{csv_file}', AUTO_DETECT=TRUE)
""").fetchall()

for col_name, col_type, null, key, default, extra in result:
    print(f"{col_name:20s} {col_type}")

print("\n" + "=" * 80)
print("Actual data:")
print("=" * 80)
result = con.execute(f"SELECT * FROM read_csv('{csv_file}', AUTO_DETECT=TRUE)").fetchdf()
print(result)
print(result.dtypes)

# Now test with our actual Parquet data converted back to check
print("\n" + "=" * 80)
print("For comparison - Parquet schema (all VARCHAR):")
print("=" * 80)
result = con.execute("""
    DESCRIBE SELECT * FROM read_parquet('cache/list_treasury_yields/2025/data.parquet')
""").fetchall()

for col_name, col_type, null, key, default, extra in result[:5]:  # Just first 5
    print(f"{col_name:20s} {col_type}")

con.close()

import os
os.unlink(csv_file)
