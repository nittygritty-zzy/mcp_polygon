"""
Test batch writing contract to ensure consistency.

This test suite verifies that:
1. Batch writing creates numbered Parquet files (data_001.parquet, data_002.parquet, etc.)
2. Cache metadata returns glob pattern (*.parquet) for file location
3. Response includes correct schema and query examples with glob pattern
4. DuckDB can read the glob pattern and get all data from multiple files
5. File location follows expected directory structure
"""

import pytest
import json
import tempfile
import shutil
from unittest.mock import patch

# Import DuckDB for testing glob pattern compatibility
try:
    import duckdb

    DUCKDB_AVAILABLE = True
except ImportError:
    DUCKDB_AVAILABLE = False


@pytest.fixture
def temp_cache_dir():
    """Create a temporary cache directory for testing."""
    temp_dir = tempfile.mkdtemp()
    yield temp_dir
    shutil.rmtree(temp_dir)


@pytest.mark.asyncio
async def test_batch_files_are_numbered_sequentially(temp_cache_dir):
    """Test that batch writing creates sequentially numbered Parquet files."""
    from mcp_polygon.cache_manager import CacheManager

    cache_mgr = CacheManager(cache_dir=temp_cache_dir)

    tool_name = "get_aggs"
    params = {
        "ticker": "AAPL",
        "multiplier": 1,
        "timespan": "day",
        "from_": "2024-01-01",
        "to": "2024-01-31",
        "fetch_all": True,
    }

    # Write 3 batches
    csv_batch_1 = "t,o,h,l,c,v\n1,100,105,99,103,1000"
    csv_batch_2 = "t,o,h,l,c,v\n2,103,107,102,106,1100"
    csv_batch_3 = "t,o,h,l,c,v\n3,106,110,105,109,1200"

    columns = ["t", "o", "h", "l", "c", "v"]

    cache_mgr.save_batch(tool_name, params, csv_batch_1, 0, columns)
    cache_mgr.save_batch(tool_name, params, csv_batch_2, 1, columns)
    cache_mgr.save_batch(tool_name, params, csv_batch_3, 2, columns)

    # Get partition path
    partition_path, _ = cache_mgr._get_partition_path(tool_name, params)

    # Verify numbered files exist
    assert (partition_path / "data_000.parquet").exists(), (
        "data_000.parquet should exist"
    )
    assert (partition_path / "data_001.parquet").exists(), (
        "data_001.parquet should exist"
    )
    assert (partition_path / "data_002.parquet").exists(), (
        "data_002.parquet should exist"
    )

    # Verify files are named correctly
    batch_files = sorted(partition_path.glob("data_*.parquet"))
    assert len(batch_files) == 3, "Should have exactly 3 batch files"
    assert batch_files[0].name == "data_000.parquet"
    assert batch_files[1].name == "data_001.parquet"
    assert batch_files[2].name == "data_002.parquet"


@pytest.mark.asyncio
async def test_finalize_returns_glob_pattern(temp_cache_dir):
    """Test that finalize_batch_save returns glob pattern in cache_location."""
    from mcp_polygon.cache_manager import CacheManager

    cache_mgr = CacheManager(cache_dir=temp_cache_dir)

    tool_name = "get_aggs"
    params = {
        "ticker": "AAPL",
        "multiplier": 1,
        "timespan": "day",
        "from_": "2024-01-01",
        "to": "2024-01-31",
        "fetch_all": True,
    }

    # Write a batch
    csv_data = "t,o,h,l,c,v\n1,100,105,99,103,1000"
    columns = ["t", "o", "h", "l", "c", "v"]

    cache_mgr.save_batch(tool_name, params, csv_data, 0, columns)

    # Finalize
    metadata = cache_mgr.finalize_batch_save(
        tool_name=tool_name,
        params=params,
        total_rows=1,
        columns=columns,
    )

    # Verify metadata structure
    assert metadata["cached"] is True
    assert "cache_location" in metadata

    # Verify glob pattern
    cache_location = metadata["cache_location"]
    assert cache_location.endswith("/*.parquet"), (
        "cache_location should end with /*.parquet glob pattern"
    )
    assert "get_aggs" in cache_location, "cache_location should contain tool name"
    assert "AAPL" in cache_location, "cache_location should contain ticker"

    # Verify other required fields
    assert metadata["row_count"] == 1
    assert metadata["columns"] == columns
    assert metadata["file_size_bytes"] > 0


@pytest.mark.asyncio
async def test_response_format_includes_glob_pattern(temp_cache_dir):
    """Test that ResponseFormatter includes glob pattern in cache info."""
    from mcp_polygon.cache_manager import CacheManager
    from mcp_polygon.response_formatter import ResponseFormatter

    cache_mgr = CacheManager(cache_dir=temp_cache_dir)

    tool_name = "get_aggs"
    params = {
        "ticker": "AAPL",
        "multiplier": 1,
        "timespan": "day",
        "from_": "2024-01-01",
        "to": "2024-01-31",
        "fetch_all": True,
    }

    # Write and finalize batch
    csv_data = "t,o,h,l,c,v\n1,100,105,99,103,1000\n2,103,107,102,106,1100"
    columns = ["t", "o", "h", "l", "c", "v"]

    cache_mgr.save_batch(tool_name, params, csv_data, 0, columns)
    cache_metadata = cache_mgr.finalize_batch_save(
        tool_name=tool_name,
        params=params,
        total_rows=2,
        columns=columns,
    )

    # Format response
    sample_rows = [
        {"t": "1", "o": "100", "h": "105", "l": "99", "c": "103", "v": "1000"},
        {"t": "2", "o": "103", "h": "107", "l": "102", "c": "106", "v": "1100"},
    ]

    response_str = ResponseFormatter.format_cached(
        cache_metadata=cache_metadata,
        tool_name=tool_name,
        params=params,
        sample_rows=sample_rows,
        csv_data=csv_data,
    )

    # Parse response
    response = json.loads(response_str)

    # Verify response structure
    assert response["status"] == "cached"
    assert "cache_info" in response
    assert "location" in response["cache_info"]

    # Verify glob pattern in location
    location = response["cache_info"]["location"]
    assert location.endswith("/*.parquet"), "Response location should use glob pattern"

    # Verify schema section
    assert "schema" in response
    assert "inferred_types" in response["schema"]
    assert "sample_rows" in response["schema"]

    # Verify query examples
    assert "query_examples" in response
    assert len(response["query_examples"]) > 0

    # Verify query examples use glob pattern
    for example in response["query_examples"]:
        assert "query" in example
        assert "read_parquet" in example["query"]
        assert "/*.parquet" in example["query"], (
            "Query examples should use glob pattern"
        )


@pytest.mark.asyncio
async def test_partition_path_structure(temp_cache_dir):
    """Test that partition path follows expected directory structure."""
    from mcp_polygon.cache_manager import CacheManager

    cache_mgr = CacheManager(cache_dir=temp_cache_dir)

    tool_name = "get_aggs"
    params = {
        "ticker": "AAPL",
        "multiplier": 1,
        "timespan": "day",
        "from_": "2024-01-01",
        "to": "2024-01-31",
        "fetch_all": True,
    }

    # Write a batch
    csv_data = "t,o,h,l,c,v\n1,100,105,99,103,1000"
    columns = ["t", "o", "h", "l", "c", "v"]

    file_path = cache_mgr.save_batch(tool_name, params, csv_data, 0, columns)

    # Verify path structure
    path_str = str(file_path)
    assert "get_aggs" in path_str, "Path should contain tool name"
    assert "AAPL" in path_str, "Path should contain ticker"
    assert "data_000.parquet" in path_str, "File should be named data_000.parquet"

    # Verify path components
    assert file_path.name == "data_000.parquet"
    assert file_path.parent.name != "get_aggs", "Should have partition subdirectory"


@pytest.mark.skipif(not DUCKDB_AVAILABLE, reason="DuckDB not installed")
@pytest.mark.asyncio
async def test_duckdb_can_read_glob_pattern(temp_cache_dir):
    """Test that DuckDB can actually read the glob pattern and get all data."""
    from mcp_polygon.cache_manager import CacheManager

    cache_mgr = CacheManager(cache_dir=temp_cache_dir)

    tool_name = "get_aggs"
    params = {
        "ticker": "AAPL",
        "multiplier": 1,
        "timespan": "day",
        "from_": "2024-01-01",
        "to": "2024-01-31",
        "fetch_all": True,
    }

    # Write 3 batches with different data
    columns = ["t", "o", "h", "l", "c", "v"]

    csv_batch_1 = "t,o,h,l,c,v\n1,100,105,99,103,1000\n2,103,107,102,106,1100"
    csv_batch_2 = "t,o,h,l,c,v\n3,106,110,105,109,1200\n4,109,113,108,112,1300"
    csv_batch_3 = "t,o,h,l,c,v\n5,112,116,111,115,1400"

    cache_mgr.save_batch(tool_name, params, csv_batch_1, 0, columns)
    cache_mgr.save_batch(tool_name, params, csv_batch_2, 1, columns)
    cache_mgr.save_batch(tool_name, params, csv_batch_3, 2, columns)

    # Finalize to get glob pattern
    metadata = cache_mgr.finalize_batch_save(
        tool_name=tool_name,
        params=params,
        total_rows=5,
        columns=columns,
    )

    glob_pattern = metadata["cache_location"]

    # Use DuckDB to read all files via glob pattern
    con = duckdb.connect(":memory:")
    result = con.execute(
        f"SELECT COUNT(*) as count FROM read_parquet('{glob_pattern}')"
    ).fetchone()

    # Verify we got all 5 rows from 3 files
    assert result[0] == 5, "DuckDB should read all 5 rows from 3 batch files"

    # Verify we can query specific columns
    result = con.execute(
        f"SELECT t, c FROM read_parquet('{glob_pattern}') ORDER BY t"
    ).fetchall()

    assert len(result) == 5, "Should get 5 rows"
    assert result[0][0] == "1", "First row should have t=1"
    assert result[4][0] == "5", "Last row should have t=5"


@pytest.mark.asyncio
async def test_batch_writing_maintains_metadata(temp_cache_dir):
    """Test that batch writing correctly updates cache metadata."""
    from mcp_polygon.cache_manager import CacheManager

    cache_mgr = CacheManager(cache_dir=temp_cache_dir)

    tool_name = "get_aggs"
    params = {
        "ticker": "AAPL",
        "multiplier": 1,
        "timespan": "day",
        "from_": "2024-01-01",
        "to": "2024-01-31",
        "fetch_all": True,
    }

    # Write batches
    csv_data = "t,o,h,l,c,v\n1,100,105,99,103,1000"
    columns = ["t", "o", "h", "l", "c", "v"]

    cache_mgr.save_batch(tool_name, params, csv_data, 0, columns)
    cache_mgr.save_batch(tool_name, params, csv_data, 1, columns)

    # Finalize
    cache_mgr.finalize_batch_save(
        tool_name=tool_name,
        params=params,
        total_rows=2,
        columns=columns,
    )

    # Verify metadata was saved to cache manager
    _, partition_key = cache_mgr._get_partition_path(tool_name, params)
    cache_key = f"{tool_name}/{partition_key}"

    assert cache_key in cache_mgr.metadata["entries"]
    entry = cache_mgr.metadata["entries"][cache_key]

    assert entry["tool_name"] == tool_name
    assert entry["row_count"] == 2
    assert entry["columns"] == columns
    assert entry["file_path"].endswith("data_*.parquet")
    assert entry["file_size_bytes"] > 0


@pytest.mark.asyncio
async def test_query_examples_use_glob_pattern(temp_cache_dir):
    """Test that generated query examples use glob pattern."""
    from mcp_polygon.cache_manager import CacheManager

    cache_mgr = CacheManager(cache_dir=temp_cache_dir)

    tool_name = "get_aggs"
    params = {
        "ticker": "AAPL",
        "multiplier": 1,
        "timespan": "day",
        "from_": "2024-01-01",
        "to": "2024-01-31",
        "fetch_all": True,
    }

    cache_location = "/path/to/cache/get_aggs/AAPL/*.parquet"

    # Generate query examples
    examples = cache_mgr.generate_query_examples(tool_name, params, cache_location)

    # Verify we got examples
    assert len(examples) > 0, "Should generate query examples"

    # Verify all examples use the glob pattern
    for example in examples:
        assert "query" in example
        assert "description" in example
        assert cache_location in example["query"], (
            "Query should use provided cache_location"
        )
        assert "/*.parquet" in example["query"], "Query should use glob pattern"
        assert "read_parquet" in example["query"], (
            "Query should use DuckDB read_parquet function"
        )


@pytest.mark.asyncio
async def test_end_to_end_batch_writing_flow(temp_cache_dir):
    """Test complete batch writing flow from tool to response."""
    from mcp_polygon.tool_integration import create_batch_writer
    from mcp_polygon.cache_manager import CacheManager

    # Patch cache manager to use temp directory
    with patch("mcp_polygon.tool_integration.get_cache_manager") as mock_get_cache:
        cache_mgr = CacheManager(cache_dir=temp_cache_dir)
        mock_get_cache.return_value = cache_mgr

        tool_name = "get_aggs"
        params = {
            "ticker": "AAPL",
            "multiplier": 1,
            "timespan": "day",
            "from_": "2024-01-01",
            "to": "2024-01-31",
            "fetch_all": True,
        }

        # Create batch writer
        batch_callback, finalize = create_batch_writer(tool_name, params)

        # Verify callbacks were created
        assert batch_callback is not None, "Should create batch callback"
        assert finalize is not None, "Should create finalize callback"

        # Simulate writing 2 batches
        batch_1 = [
            {"t": 1, "o": 100, "h": 105, "l": 99, "c": 103, "v": 1000},
            {"t": 2, "o": 103, "h": 107, "l": 102, "c": 106, "v": 1100},
        ]
        batch_2 = [
            {"t": 3, "o": 106, "h": 110, "l": 105, "c": 109, "v": 1200},
        ]

        await batch_callback(0, batch_1)
        await batch_callback(1, batch_2)

        # Finalize and get response
        response_str = await finalize()

        # Parse response
        response = json.loads(response_str)

        # Verify response structure
        assert response["status"] == "cached"
        assert "cache_info" in response
        assert response["cache_info"]["location"].endswith("/*.parquet")
        assert response["cache_info"]["row_count"] == 3

        # Verify batch files were created
        partition_path, _ = cache_mgr._get_partition_path(tool_name, params)
        batch_files = list(partition_path.glob("data_*.parquet"))
        assert len(batch_files) == 2, "Should have 2 batch files"


if __name__ == "__main__":
    # Run tests
    pytest.main([__file__, "-v"])
