#!/usr/bin/env python3
"""Test DuckDB's implicit type conversion on VARCHAR columns."""

import duckdb

con = duckdb.connect()
parquet_file = 'cache/list_treasury_yields/2025/data.parquet'

print("=" * 80)
print("Test 1: Numeric operations WITHOUT explicit CAST")
print("=" * 80)

try:
    result = con.execute(f"""
        SELECT 
            date,
            yield_10_year,
            yield_10_year * 100 as basis_points,
            yield_10_year > 4.5 as is_high
        FROM read_parquet('{parquet_file}')
        WHERE yield_10_year != ''
        ORDER BY yield_10_year DESC
        LIMIT 5
    """).fetchdf()
    print("SUCCESS - DuckDB handles implicit conversion!")
    print(result)
except Exception as e:
    print(f"FAILED: {e}")

print("\n" + "=" * 80)
print("Test 2: Aggregations WITHOUT explicit CAST")
print("=" * 80)

try:
    result = con.execute(f"""
        SELECT 
            AVG(yield_10_year) as avg_yield,
            MAX(yield_10_year) as max_yield,
            MIN(yield_10_year) as min_yield
        FROM read_parquet('{parquet_file}')
        WHERE yield_10_year != ''
    """).fetchdf()
    print("SUCCESS - DuckDB handles implicit aggregation!")
    print(result)
except Exception as e:
    print(f"FAILED: {e}")

con.close()
