#!/usr/bin/env python3
"""Test cache idempotence with different filters."""

import asyncio
import os
from pathlib import Path
import shutil

# Add src to path
import sys
sys.path.insert(0, str(Path(__file__).parent / "src"))

from mcp_polygon.tools import news


async def test_cache_idempotence():
    """Test that caching with different filters is idempotent."""

    # Clean up any existing cache
    cache_dir = Path("./cache/list_ticker_news")
    if cache_dir.exists():
        print(f"Cleaning up existing cache at {cache_dir}")
        shutil.rmtree(cache_dir)

    print("\n=== Test 1: Cache AAPL news from 2025-01-01 ===")
    result1 = await news.list_ticker_news(
        ticker="AAPL",
        published_utc_gte="2025-01-01",
        limit=5,
        fetch_all=True,
    )
    print(f"Result 1 length: {len(result1)} bytes")

    # Check what files were created
    if cache_dir.exists():
        files1 = list(cache_dir.rglob("*.parquet"))
        print(f"Files created: {len(files1)}")
        for f in files1[:3]:  # Show first 3
            print(f"  - {f.relative_to(cache_dir)}")

    print("\n=== Test 2: Cache AAPL news from 2024-01-01 (different filter) ===")
    result2 = await news.list_ticker_news(
        ticker="AAPL",
        published_utc_gte="2024-01-01",
        limit=5,
        fetch_all=True,
    )
    print(f"Result 2 length: {len(result2)} bytes")

    # Check what files exist now
    if cache_dir.exists():
        files2 = list(cache_dir.rglob("*.parquet"))
        print(f"Files after 2nd cache: {len(files2)}")
        for f in files2[:3]:  # Show first 3
            print(f"  - {f.relative_to(cache_dir)}")

    print("\n=== Test 3: Re-cache AAPL news from 2025-01-01 (same as Test 1) ===")
    result3 = await news.list_ticker_news(
        ticker="AAPL",
        published_utc_gte="2025-01-01",
        limit=5,
        fetch_all=True,
    )
    print(f"Result 3 length: {len(result3)} bytes")

    # Check what files exist now
    if cache_dir.exists():
        files3 = list(cache_dir.rglob("*.parquet"))
        print(f"Files after 3rd cache: {len(files3)}")
        for f in files3[:5]:  # Show first 5
            print(f"  - {f.relative_to(cache_dir)}")

    print("\n=== Test 4: Cache different ticker (MSFT) ===")
    result4 = await news.list_ticker_news(
        ticker="MSFT",
        published_utc_gte="2025-01-01",
        limit=5,
        fetch_all=True,
    )
    print(f"Result 4 length: {len(result4)} bytes")

    # Check what files exist now
    if cache_dir.exists():
        files4 = list(cache_dir.rglob("*.parquet"))
        print(f"Files after 4th cache: {len(files4)}")

        # Group by ticker
        aapl_files = [f for f in files4 if "/AAPL/" in str(f)]
        msft_files = [f for f in files4 if "/MSFT/" in str(f)]
        print(f"AAPL files: {len(aapl_files)}")
        print(f"MSFT files: {len(msft_files)}")

    print("\n=== Analysis ===")
    print(f"Test 1 result == Test 3 result: {result1 == result3}")
    print(f"Test 1 result != Test 2 result: {result1 != result2}")
    print(f"Files are organized by partition keys (ticker, date, etc.)")

    # Show the cache structure
    print("\n=== Cache Directory Structure ===")
    if cache_dir.exists():
        for root, dirs, files in os.walk(cache_dir):
            level = root.replace(str(cache_dir), '').count(os.sep)
            indent = ' ' * 2 * level
            print(f'{indent}{os.path.basename(root)}/')
            subindent = ' ' * 2 * (level + 1)
            for file in files[:3]:  # Limit to 3 files per dir
                print(f'{subindent}{file}')
            if len(files) > 3:
                print(f'{subindent}... and {len(files) - 3} more files')


if __name__ == "__main__":
    # Check for API key
    if "POLYGON_API_KEY" not in os.environ:
        print("Error: POLYGON_API_KEY environment variable not set")
        sys.exit(1)

    asyncio.run(test_cache_idempotence())
