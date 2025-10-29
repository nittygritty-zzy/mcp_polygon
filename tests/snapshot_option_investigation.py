#!/usr/bin/env python3
"""Test script to understand get_snapshot_option API response structure."""

import json
import os
from polygon import RESTClient

# Get API key from environment
api_key = os.getenv("POLYGON_API_KEY")
if not api_key:
    print("Error: POLYGON_API_KEY environment variable not set")
    exit(1)

# Create client
client = RESTClient(api_key)

# Test the get_snapshot_option method
underlying_asset = "AAPL"
option_contract = "O:AAPL251219C00275000"

print(f"Testing get_snapshot_option for {option_contract}")
print("=" * 80)

# Get raw response
response = client.get_snapshot_option(
    underlying_asset=underlying_asset,
    option_contract=option_contract,
    raw=True,
)

# Decode and parse JSON
raw_data = response.data.decode("utf-8")
print("Raw JSON response:")
print(raw_data)
print("\n" + "=" * 80)

# Parse to dict
data = json.loads(raw_data)
print("\nParsed data structure:")
print(f"Top-level keys: {list(data.keys())}")

if "results" in data:
    results = data["results"]
    print(f"\nType of results: {type(results)}")
    print(f"Results value: {results}")

    if isinstance(results, dict):
        print(f"\nResults keys: {list(results.keys())}")
    elif isinstance(results, list):
        print(f"\nResults is a list with {len(results)} items")
        if results:
            print(f"First item type: {type(results[0])}")
            if isinstance(results[0], dict):
                print(f"First item keys: {list(results[0].keys())}")
