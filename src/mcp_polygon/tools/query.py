"""
DuckDB query tool for cached Polygon data.

Provides SQL query interface to cached Parquet files.
"""

from typing import Optional, Literal
from mcp.types import ToolAnnotations

from ..clients import poly_mcp
from ..duckdb_query import get_query_tool


@poly_mcp.tool(annotations=ToolAnnotations(readOnlyHint=True))
async def duckdb_query(
    sql: str,
    format: Literal["csv", "json"] = "csv",
) -> str:
    """
    Execute SQL query on cached Polygon data using DuckDB.

    Use this tool to analyze data that was previously cached by other Polygon tools.
    The cached data is stored in Parquet format and can be queried using SQL with
    DuckDB's read_parquet() function.

    Security: Only queries on data within the ./cache/ directory are allowed.

    Args:
        sql: SQL query string. Must use read_parquet('path/to/*.parquet') to read cached data.
             Supports DuckDB SQL syntax including window functions, CTEs, and aggregations.
             Example: "SELECT * FROM read_parquet('./cache/get_aggs/AAPL/2024/10/*.parquet') ORDER BY t DESC LIMIT 10"
        format: Output format - 'csv' (default) or 'json'

    Returns:
        Query results as CSV or JSON string

    Example queries:
        # View all data from cached aggregates
        SELECT * FROM read_parquet('./cache/get_aggs/AAPL/2024/10/*.parquet') ORDER BY t

        # Calculate daily returns
        SELECT t, c as close,
               LAG(c) OVER (ORDER BY t) as prev_close,
               (c - LAG(c) OVER (ORDER BY t)) / LAG(c) OVER (ORDER BY t) * 100 as return_pct
        FROM read_parquet('./cache/get_aggs/AAPL/2024/10/*.parquet')
        ORDER BY t

        # Top 20 gainers from market snapshot
        SELECT T as ticker, c as close, todaysChangePerc
        FROM read_parquet('./cache/get_grouped_daily_aggs/2024-10-25/*.parquet')
        WHERE todaysChangePerc > 0
        ORDER BY todaysChangePerc DESC
        LIMIT 20
    """
    try:
        query_tool = get_query_tool()
        return query_tool.query(sql, format=format)
    except Exception as e:
        return f"Error: {e}"


@poly_mcp.tool(annotations=ToolAnnotations(readOnlyHint=True))
async def list_cached_data(
    tool_name: Optional[str] = None,
) -> str:
    """
    List available cached data partitions.

    Use this tool to discover what data has been cached and where it's located.
    Helpful for constructing queries with the duckdb_query tool.

    Args:
        tool_name: Optional tool name to filter by (e.g., 'get_aggs', 'list_financials_balance_sheets').
                   If not provided, lists all cached tools.

    Returns:
        JSON string with partition information including:
        - Available tool names
        - Partition keys (directory structure)
        - File counts
        - Glob patterns for querying

    Example:
        # List all cached data
        list_cached_data()

        # List cached data for specific tool
        list_cached_data(tool_name='get_aggs')
    """
    try:
        query_tool = get_query_tool()

        if tool_name:
            # Get info for specific tool
            info = query_tool.get_partition_info(tool_name)
            import json

            return json.dumps(info, indent=2)
        else:
            # List all tools with cached data
            import json
            from pathlib import Path

            cache_dir = Path("./cache")
            if not cache_dir.exists():
                return json.dumps({"cached_tools": []}, indent=2)

            # Find all tool directories
            tool_dirs = [
                d
                for d in cache_dir.iterdir()
                if d.is_dir() and not d.name.startswith("_")
            ]

            cached_tools = []
            for tool_dir in sorted(tool_dirs):
                info = query_tool.get_partition_info(tool_dir.name)
                cached_tools.append(
                    {
                        "tool_name": info["tool_name"],
                        "partitions": len(info["partitions"]),
                        "files": info["file_count"],
                        "glob_pattern": info["glob_pattern"],
                    }
                )

            return json.dumps({"cached_tools": cached_tools}, indent=2)

    except Exception as e:
        return f"Error: {e}"
