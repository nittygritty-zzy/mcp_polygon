"""
Tests for DuckDB query tool.
"""

import pytest
import os
import tempfile
import shutil
from pathlib import Path
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from mcp_polygon.duckdb_query import DuckDBQueryTool, SecurityError


@pytest.fixture
def temp_cache_dir():
    """Create temporary cache directory with test data."""
    temp_dir = tempfile.mkdtemp()
    cache_dir = Path(temp_dir) / "cache"
    cache_dir.mkdir()

    # Create test data: get_aggs/AAPL/2024/10/data.parquet
    aggs_dir = cache_dir / "get_aggs" / "AAPL" / "2024" / "10"
    aggs_dir.mkdir(parents=True)

    # Sample OHLC data
    df = pd.DataFrame({
        "t": [1727755200000, 1727841600000, 1727928000000],
        "o": [229.52, 226.5, 228.0],
        "h": [229.65, 227.8, 229.5],
        "l": [223.74, 224.1, 226.2],
        "c": [226.21, 227.0, 228.5],
        "v": [63235048, 45123456, 52987654],
    })

    table = pa.Table.from_pandas(df)
    pq.write_table(table, aggs_dir / "data.parquet")

    yield cache_dir

    # Cleanup
    shutil.rmtree(temp_dir)


def test_query_basic(temp_cache_dir):
    """Test basic query functionality."""
    tool = DuckDBQueryTool(cache_dir=str(temp_cache_dir))

    sql = f"SELECT * FROM read_parquet('{temp_cache_dir}/get_aggs/AAPL/2024/10/*.parquet') ORDER BY t"
    result = tool.query(sql, format="csv")

    # Verify CSV output
    assert "t,o,h,l,c,v" in result
    assert "1727755200000" in result
    assert "229.52" in result
    assert "226.21" in result


def test_query_aggregation(temp_cache_dir):
    """Test SQL aggregation."""
    tool = DuckDBQueryTool(cache_dir=str(temp_cache_dir))

    sql = f"""
    SELECT
        COUNT(*) as row_count,
        AVG(c) as avg_close,
        MIN(l) as min_low,
        MAX(h) as max_high
    FROM read_parquet('{temp_cache_dir}/get_aggs/AAPL/2024/10/*.parquet')
    """
    result = tool.query(sql, format="csv")

    # Verify aggregation
    assert "row_count" in result
    assert "avg_close" in result
    assert "3" in result  # 3 rows


def test_query_window_function(temp_cache_dir):
    """Test window function (LAG for returns calculation)."""
    tool = DuckDBQueryTool(cache_dir=str(temp_cache_dir))

    sql = f"""
    SELECT
        t,
        c as close,
        LAG(c) OVER (ORDER BY t) as prev_close
    FROM read_parquet('{temp_cache_dir}/get_aggs/AAPL/2024/10/*.parquet')
    ORDER BY t
    """
    result = tool.query(sql, format="csv")

    lines = result.strip().split("\n")
    assert len(lines) == 4  # Header + 3 data rows
    assert "close,prev_close" in result


def test_query_json_format(temp_cache_dir):
    """Test JSON output format."""
    tool = DuckDBQueryTool(cache_dir=str(temp_cache_dir))

    sql = f"SELECT * FROM read_parquet('{temp_cache_dir}/get_aggs/AAPL/2024/10/*.parquet') LIMIT 1"
    result = tool.query(sql, format="json")

    import json
    data = json.loads(result)
    assert isinstance(data, list)
    assert len(data) == 1
    assert "t" in data[0]
    assert "c" in data[0]


def test_security_violation():
    """Test that queries outside cache directory are blocked."""
    tool = DuckDBQueryTool(cache_dir="./cache")

    # Attempt to query outside cache directory
    sql = "SELECT * FROM read_parquet('/etc/passwd')"

    with pytest.raises(SecurityError):
        tool.query(sql)


def test_security_relative_path_escape():
    """Test that relative path escapes are blocked."""
    tool = DuckDBQueryTool(cache_dir="./cache")

    # Attempt to escape using ../
    sql = "SELECT * FROM read_parquet('./cache/../../etc/passwd')"

    with pytest.raises(SecurityError):
        tool.query(sql)


def test_get_partition_info(temp_cache_dir):
    """Test partition info retrieval."""
    tool = DuckDBQueryTool(cache_dir=str(temp_cache_dir))

    info = tool.get_partition_info("get_aggs")

    assert info["exists"] is True
    assert info["tool_name"] == "get_aggs"
    assert len(info["partitions"]) > 0
    assert info["file_count"] == 1
    assert "glob_pattern" in info


def test_get_partition_info_missing_tool(temp_cache_dir):
    """Test partition info for non-existent tool."""
    tool = DuckDBQueryTool(cache_dir=str(temp_cache_dir))

    info = tool.get_partition_info("nonexistent_tool")

    assert info["exists"] is False
    assert info["tool_name"] == "nonexistent_tool"
    assert info["partitions"] == []


def test_query_error_handling():
    """Test error handling for invalid SQL."""
    tool = DuckDBQueryTool(cache_dir="./cache")

    # Invalid SQL syntax
    sql = "SELECT * FORM read_parquet('./cache/test/*.parquet')"  # FORM typo
    result = tool.query(sql)

    assert "Error" in result
