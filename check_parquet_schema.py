#!/usr/bin/env python3
"""Check Parquet schema to understand data types."""

import pyarrow.parquet as pq
import sys
from pathlib import Path

# Find any cached parquet file
cache_dir = Path("./cache")
parquet_files = list(cache_dir.rglob("*.parquet"))

if not parquet_files:
    print("No parquet files found in cache")
    sys.exit(1)

# Use the first one found
parquet_file = parquet_files[0]
print(f"Examining: {parquet_file}")
print()

# Read schema
table = pq.read_table(parquet_file)
schema = table.schema

print("Schema:")
print("=" * 80)
for i, field in enumerate(schema):
    print(f"{i+1:3d}. {field.name:40s} {field.type}")
print()

# Read first few rows
df = table.to_pandas()
print(f"\nFirst row data types:")
print("=" * 80)
for col in df.columns[:10]:  # First 10 columns
    print(f"{col:40s} {df[col].dtype}")
print()

print(f"\nFirst row sample:")
print("=" * 80)
print(df.iloc[0].to_dict())
