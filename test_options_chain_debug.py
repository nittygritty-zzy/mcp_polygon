#!/usr/bin/env python3
"""Debug options chain issue."""

import asyncio
import os
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).parent / "src"))

from mcp_polygon.tools import options


async def test_options_chain():
    """Test options chain with fetch_all."""

    print("Testing options chain with fetch_all=True...")

    try:
        result = await options.list_snapshot_options_chain(
            underlying_asset="NVDA",
            contract_type="put",
            limit=5,
            fetch_all=True,
        )
        print(f"Success! Result length: {len(result)} bytes")
        print(f"First 500 chars: {result[:500]}")
    except Exception as e:
        print(f"Error: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    if "POLYGON_API_KEY" not in os.environ:
        print("Error: POLYGON_API_KEY environment variable not set")
        sys.exit(1)

    asyncio.run(test_options_chain())
