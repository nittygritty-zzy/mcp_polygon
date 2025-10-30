"""
Response formatting for Polygon MCP tools.

Handles decision between direct CSV return vs caching with DuckDB query instructions.
"""

from typing import Any, Dict, List
import json
from .cache_manager import get_cache_manager


class ResponseFormatter:
    """
    Formats tool responses based on caching decisions.

    Returns either:
    1. Direct CSV string for small/immediate results
    2. Structured cache metadata with DuckDB query examples for large/cached results
    """

    @staticmethod
    def format_direct(csv_data: str) -> str:
        """
        Format response for direct return (no caching).

        Args:
            csv_data: CSV string data

        Returns:
            CSV string as-is
        """
        return csv_data

    @staticmethod
    def format_cached(
        cache_metadata: Dict[str, Any],
        tool_name: str,
        params: Dict[str, Any],
        sample_rows: List[Dict[str, Any]],
    ) -> str:
        """
        Format response for cached data with DuckDB query instructions.

        Args:
            cache_metadata: Metadata from CacheManager.save()
            tool_name: Name of the tool
            params: Parameters used in API call
            sample_rows: First few rows of data for preview

        Returns:
            JSON string with cache location, schema, and query examples
        """
        # Get query examples from cache_manager (single source of truth)
        cache_mgr = get_cache_manager()
        query_examples = cache_mgr.generate_query_examples(
            tool_name, params, cache_metadata["cache_location"]
        )

        response = {
            "status": "cached",
            "message": "Data cached successfully. Use the duckdb_query tool to analyze the data.",
            "cache_info": {
                "location": cache_metadata["cache_location"],
                "partition_key": cache_metadata["partition_key"],
                "row_count": cache_metadata["row_count"],
                "file_size_mb": round(
                    cache_metadata["file_size_bytes"] / (1024 * 1024), 2
                ),
            },
            "schema": {
                "columns": cache_metadata["columns"],
                "sample_rows": sample_rows[:3],  # First 3 rows
            },
            "query_examples": query_examples,
            "usage": {
                "tool": "duckdb_query",
                "description": "Use the duckdb_query tool with one of the example queries above, or write your own SQL query using DuckDB syntax.",
            },
        }

        return json.dumps(response, indent=2, default=str)
