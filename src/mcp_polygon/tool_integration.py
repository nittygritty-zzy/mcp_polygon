"""
Integration helper for adding caching to Polygon MCP tools.

Provides a simple wrapper to enable caching with minimal code changes.
"""

from typing import Dict, Any
import csv
import io

from .cache_manager import get_cache_manager
from .response_formatter import ResponseFormatter


async def process_tool_response(
    tool_name: str,
    params: Dict[str, Any],
    csv_data: str,
) -> str:
    """
    Process tool response with intelligent caching decision.

    This function:
    1. Analyzes the response size and parameters
    2. Decides whether to cache or return directly
    3. If caching: saves to Parquet and returns metadata with query examples
    4. If not caching: returns CSV directly

    Args:
        tool_name: Name of the MCP tool (e.g., 'get_aggs')
        params: Parameters used in the API call
        csv_data: CSV response from json_to_csv()

    Returns:
        Either:
        - CSV string (for small/uncacheable responses)
        - JSON metadata with cache location and query examples (for cached responses)
    """
    # Get cache manager
    cache_mgr = get_cache_manager()

    # Calculate response size
    response_size_bytes = len(csv_data.encode("utf-8"))

    # Check if we should cache
    if not cache_mgr.should_cache(tool_name, params, response_size_bytes):
        # Return CSV directly
        return ResponseFormatter.format_direct(csv_data)

    # Check if already cached (and not expired)
    existing = cache_mgr.get(tool_name, params)
    if existing:
        # Return existing cache metadata
        # Parse CSV to get sample rows
        sample_rows = _parse_csv_sample(csv_data)
        return ResponseFormatter.format_cached(
            cache_metadata=existing,
            tool_name=tool_name,
            params=params,
            sample_rows=sample_rows,
            csv_data=csv_data,
        )

    # Cache the data
    try:
        # Parse CSV to extract columns
        columns = _extract_columns(csv_data)

        # Save to cache
        cache_metadata = cache_mgr.save(
            tool_name=tool_name,
            params=params,
            csv_data=csv_data,
            columns=columns,
        )

        # Parse CSV for sample rows
        sample_rows = _parse_csv_sample(csv_data)

        # Return cache metadata with query examples
        return ResponseFormatter.format_cached(
            cache_metadata=cache_metadata,
            tool_name=tool_name,
            params=params,
            sample_rows=sample_rows,
            csv_data=csv_data,
        )

    except Exception as e:
        # If caching fails, fall back to direct return
        print(f"Warning: Caching failed for {tool_name}: {e}")
        return ResponseFormatter.format_direct(csv_data)


def _extract_columns(csv_data: str) -> list:
    """Extract column names from CSV string."""
    if not csv_data.strip():
        return []

    reader = csv.DictReader(io.StringIO(csv_data))
    return list(reader.fieldnames) if reader.fieldnames else []


def _parse_csv_sample(csv_data: str, n: int = 3) -> list:
    """Parse first N rows from CSV string."""
    if not csv_data.strip():
        return []

    try:
        reader = csv.DictReader(io.StringIO(csv_data))
        rows = []
        for i, row in enumerate(reader):
            if i >= n:
                break
            rows.append(row)
        return rows
    except Exception:
        return []
