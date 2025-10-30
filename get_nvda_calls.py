#!/usr/bin/env python3
"""Get NVDA call options with detailed Greeks."""

import asyncio
import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent / "src"))

from mcp_polygon.tools import options


async def main():
    """Fetch NVDA call options."""

    print("Fetching NVDA call options chain...")

    try:
        result = await options.list_snapshot_options_chain(
            underlying_asset="NVDA",
            contract_type="call",
            limit=250,  # Max per request
            fetch_all=True,  # Fetch all pages
        )

        # Save the result to a file for analysis
        output_file = Path("nvda_calls_result.json")
        with open(output_file, "w") as f:
            f.write(result)

        print(f"\nResult saved to: {output_file}")
        print(f"Result size: {len(result)} bytes")

        # Print first 1000 chars to see the structure
        print(f"\nFirst 1000 characters:")
        print(result[:1000])

    except Exception as e:
        print(f"Error: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    if "POLYGON_API_KEY" not in os.environ:
        print("Error: POLYGON_API_KEY environment variable not set")
        sys.exit(1)

    asyncio.run(main())
