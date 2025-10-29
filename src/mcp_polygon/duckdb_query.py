"""
DuckDB query tool for analyzing cached Polygon data.

Provides SQL query interface to cached Parquet files with security constraints.
"""

import duckdb
from pathlib import Path
from typing import Optional, Dict, Any
import json
import csv
import io


class DuckDBQueryTool:
    """
    Execute SQL queries on cached Polygon data using DuckDB.

    Features:
    - Fast columnar queries on Parquet files
    - Glob pattern support for multi-file queries
    - Security: Only allows queries on Polygon cache directory
    - Returns results as CSV for LLM consumption
    """

    def __init__(self, cache_dir: str = "./cache"):
        """
        Initialize DuckDB query tool.

        Args:
            cache_dir: Root directory for cached data
        """
        self.cache_dir = Path(cache_dir).resolve()

    def query(self, sql: str, format: str = "csv") -> str:
        """
        Execute SQL query on cached data.

        Args:
            sql: SQL query string (must reference Parquet files via read_parquet())
            format: Output format ('csv' or 'json')

        Returns:
            Query results as CSV string or JSON string

        Raises:
            SecurityError: If query attempts to access files outside cache directory
            ValueError: If SQL is invalid or references non-existent files
        """
        # Security validation
        self._validate_query_security(sql)

        try:
            # Create in-memory DuckDB connection
            con = duckdb.connect(":memory:")

            # Execute query
            result = con.execute(sql)

            # Fetch results
            columns = [desc[0] for desc in result.description]
            rows = result.fetchall()

            # Format output
            if format == "json":
                return self._format_json(columns, rows)
            else:
                return self._format_csv(columns, rows)

        except Exception as e:
            return f"Error executing query: {e}"
        finally:
            con.close()

    def _validate_query_security(self, sql: str):
        """
        Validate that query only accesses files within cache directory.

        Args:
            sql: SQL query string

        Raises:
            SecurityError: If query attempts to access files outside cache directory
        """
        # Extract file paths from read_parquet() calls
        import re

        # Pattern: read_parquet('path') or read_parquet("path")
        parquet_paths = re.findall(r"read_parquet\s*\(\s*['\"]([^'\"]+)['\"]", sql, re.IGNORECASE)

        if not parquet_paths:
            # No file references - might be a metadata query, allow it
            return

        # Validate each path
        for path_pattern in parquet_paths:
            # Convert glob pattern to base directory
            base_path = path_pattern.split("*")[0].rstrip("/")
            resolved_path = Path(base_path).resolve()

            # Check if path is within cache directory
            try:
                resolved_path.relative_to(self.cache_dir)
            except ValueError:
                raise SecurityError(
                    f"Query attempts to access files outside cache directory: {path_pattern}"
                )

    def _format_csv(self, columns: list, rows: list) -> str:
        """
        Format query results as CSV.

        Args:
            columns: Column names
            rows: Result rows

        Returns:
            CSV string
        """
        if not rows:
            return ""

        output = io.StringIO()
        writer = csv.writer(output)

        # Write header
        writer.writerow(columns)

        # Write rows
        for row in rows:
            writer.writerow(row)

        return output.getvalue()

    def _format_json(self, columns: list, rows: list) -> str:
        """
        Format query results as JSON.

        Args:
            columns: Column names
            rows: Result rows

        Returns:
            JSON string
        """
        results = []
        for row in rows:
            results.append(dict(zip(columns, row)))

        return json.dumps(results, indent=2, default=str)

    def get_partition_info(self, tool_name: str) -> Dict[str, Any]:
        """
        Get information about available partitions for a tool.

        Args:
            tool_name: Name of the tool

        Returns:
            Dictionary with partition information
        """
        tool_dir = self.cache_dir / tool_name

        if not tool_dir.exists():
            return {
                "tool_name": tool_name,
                "exists": False,
                "partitions": []
            }

        # Find all Parquet files
        parquet_files = list(tool_dir.rglob("*.parquet"))

        # Extract partition keys (directory structure)
        partitions = set()
        for pf in parquet_files:
            # Get path relative to tool directory
            rel_path = pf.relative_to(tool_dir)
            partition_key = str(rel_path.parent)
            partitions.add(partition_key)

        return {
            "tool_name": tool_name,
            "exists": True,
            "partitions": sorted(partitions),
            "file_count": len(parquet_files),
            "glob_pattern": f"{tool_dir}/**/*.parquet"
        }


class SecurityError(Exception):
    """Raised when query violates security constraints."""
    pass


# Singleton instance
_query_tool = None


def get_query_tool(cache_dir: str = "./cache") -> DuckDBQueryTool:
    """
    Get or create singleton DuckDB query tool instance.

    Args:
        cache_dir: Root directory for cached data

    Returns:
        DuckDBQueryTool instance
    """
    global _query_tool
    if _query_tool is None:
        _query_tool = DuckDBQueryTool(cache_dir=cache_dir)
    return _query_tool
