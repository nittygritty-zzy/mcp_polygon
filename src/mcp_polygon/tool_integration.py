"""
Integration helper for adding caching to Polygon MCP tools.

Provides a simple wrapper to enable caching with minimal code changes.
"""

from typing import Dict, Any, Callable
import csv
import io

from .cache_manager import get_cache_manager
from .response_formatter import ResponseFormatter
from .formatters import json_to_csv


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


def create_batch_writer(
    tool_name: str,
    params: Dict[str, Any],
) -> tuple[Callable, Callable]:
    """
    Create batch writing callbacks for streaming cache writes.

    Returns a tuple of (batch_callback, finalize_callback):
    - batch_callback(batch_num, data): Writes a batch to disk
    - finalize_callback(): Finalizes the cache and returns response

    Example:
        batch_callback, finalize = create_batch_writer("get_aggs", params)

        fetcher = PolygonParallelFetcher(polygon_client, num_workers=5)
        await fetcher.fetch_all(
            method_name="get_aggs",
            batch_callback=batch_callback,
            ticker="AAPL",
            ...
        )

        return await finalize()
    """
    cache_mgr = get_cache_manager()

    # Check if we should cache
    # For batch writing, we'll always cache if fetch_all=True (large dataset expected)
    if not params.get("fetch_all", True):
        # Single page mode - return None to indicate no batch writing
        return None, None

    # Shared state for tracking batches
    state = {
        "total_rows": 0,
        "columns": None,
        "sample_rows": [],
    }

    async def batch_callback(batch_num: int, data: list):
        """Write a batch to disk immediately."""
        if not data:
            return

        # Convert batch to CSV
        csv_data = json_to_csv({"results": data})

        # Extract columns from first batch
        if state["columns"] is None:
            state["columns"] = _extract_columns(csv_data)

        # Save first few rows for sample
        if batch_num < 3:
            batch_sample = _parse_csv_sample(csv_data, n=10)
            state["sample_rows"].extend(batch_sample)

        # Write batch to disk
        cache_mgr.save_batch(
            tool_name=tool_name,
            params=params,
            csv_data=csv_data,
            batch_num=batch_num,
            columns=state["columns"],
        )

        # Update row count
        state["total_rows"] += len(data)

    async def finalize():
        """Finalize batch writing and return response."""
        if state["total_rows"] == 0:
            # No data was written
            return ResponseFormatter.format_direct("")

        # Finalize cache metadata
        cache_metadata = cache_mgr.finalize_batch_save(
            tool_name=tool_name,
            params=params,
            total_rows=state["total_rows"],
            columns=state["columns"] or [],
        )

        # Build sample CSV for response
        sample_csv = ""
        if state["sample_rows"]:
            import io
            import csv as csv_module

            output = io.StringIO()
            if state["columns"]:
                writer = csv_module.DictWriter(output, fieldnames=state["columns"])
                writer.writeheader()
                for row in state["sample_rows"][:10]:  # Limit to 10 rows
                    writer.writerow(row)
                sample_csv = output.getvalue()

        # Return cache metadata with query examples
        return ResponseFormatter.format_cached(
            cache_metadata=cache_metadata,
            tool_name=tool_name,
            params=params,
            sample_rows=state["sample_rows"][:3],
            csv_data=sample_csv,
        )

    return batch_callback, finalize
