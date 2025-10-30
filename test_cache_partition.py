#!/usr/bin/env python3
"""Test cache partition key generation."""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent / "src"))

from mcp_polygon.cache_manager import CacheManager

# Create a CacheManager instance
cache_mgr = CacheManager()

# Test case 1: expiration_date is None
params1 = {
    "underlying_asset": "NVDA",
    "contract_type": "call",
    "expiration_date": None,
    "limit": 250,
    "fetch_all": True,
}

partition_key1 = cache_mgr._generate_partition_key(
    "list_snapshot_options_chain", params1
)
print(f"Test 1 (expiration_date=None):")
print(f"  Partition key: {partition_key1}")
print(f"  Expected: NVDA/call_all")
print(f"  Match: {partition_key1 == 'NVDA/call_all'}")
print()

# Test case 2: expiration_date is a date string
params2 = {
    "underlying_asset": "NVDA",
    "contract_type": "put",
    "expiration_date": "2025-12-19",
    "limit": 250,
    "fetch_all": True,
}

partition_key2 = cache_mgr._generate_partition_key(
    "list_snapshot_options_chain", params2
)
print(f"Test 2 (expiration_date='2025-12-19'):")
print(f"  Partition key: {partition_key2}")
print(f"  Expected: NVDA/put_2025-12-19")
print(f"  Match: {partition_key2 == 'NVDA/put_2025-12-19'}")
print()

# Test case 3: contract_type is None
params3 = {
    "underlying_asset": "AAPL",
    "contract_type": None,
    "expiration_date": None,
    "limit": 250,
    "fetch_all": True,
}

partition_key3 = cache_mgr._generate_partition_key(
    "list_snapshot_options_chain", params3
)
print(f"Test 3 (contract_type=None, expiration_date=None):")
print(f"  Partition key: {partition_key3}")
print(f"  Expected: AAPL/all_all")
print(f"  Match: {partition_key3 == 'AAPL/all_all'}")
