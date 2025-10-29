"""Test Alpha Vantage caching integration."""

import pytest
from pathlib import Path
import duckdb
from src.mcp_polygon.tools.alpha_vantage import get_earnings_calendar_alpha_vantage


class TestAlphaVantageCaching:
    """Test that Alpha Vantage earnings calendar uses intelligent caching."""

    @pytest.mark.asyncio
    async def test_earnings_calendar_creates_parquet_cache(self):
        """Test that earnings calendar data is cached as Parquet."""
        # Call the tool
        result = await get_earnings_calendar_alpha_vantage(
            alpha_vantage_api_key="demo",
            horizon="3month"
        )

        # Check that we got data back (CSV or cache metadata)
        assert result is not None
        assert len(result) > 0

        # Verify Parquet cache was created
        cache_path = Path("cache/get_earnings_calendar_alpha_vantage/all/3month/data.parquet")
        assert cache_path.exists(), f"Expected Parquet cache at {cache_path}"

        # Verify file is not empty
        assert cache_path.stat().st_size > 0, "Parquet file should not be empty"

    @pytest.mark.asyncio
    async def test_earnings_calendar_parquet_queryable(self):
        """Test that cached Parquet can be queried with DuckDB."""
        # Ensure we have cached data
        await get_earnings_calendar_alpha_vantage(
            alpha_vantage_api_key="demo",
            horizon="3month"
        )

        # Query the Parquet file
        parquet_file = "cache/get_earnings_calendar_alpha_vantage/all/3month/data.parquet"

        # Test basic query
        result = duckdb.query(f"""
            SELECT COUNT(*) as count
            FROM read_parquet('{parquet_file}')
        """).fetchone()

        assert result[0] > 0, "Should have earnings records in cache"

        # Test column access
        columns = duckdb.query(f"""
            SELECT * FROM read_parquet('{parquet_file}')
            LIMIT 1
        """).to_df().columns.tolist()

        # Check expected columns exist
        expected_columns = ["symbol", "name", "reportDate", "fiscalDateEnding", "estimate", "currency"]
        for col in expected_columns:
            assert col in columns, f"Expected column '{col}' in cached data"

    @pytest.mark.asyncio
    async def test_earnings_calendar_symbol_partitioning(self):
        """Test that symbol-specific queries create separate partitions."""
        # Call with specific symbol
        result = await get_earnings_calendar_alpha_vantage(
            alpha_vantage_api_key="demo",
            symbol="AAPL",
            horizon="3month"
        )

        assert result is not None

        # Verify symbol-specific partition was created
        cache_path = Path("cache/get_earnings_calendar_alpha_vantage/AAPL/3month/data.parquet")
        assert cache_path.exists(), f"Expected symbol-specific cache at {cache_path}"

    @pytest.mark.asyncio
    async def test_cached_data_structure_valid(self):
        """Test that cached data has valid structure for querying."""
        # Ensure we have cached data
        await get_earnings_calendar_alpha_vantage(
            alpha_vantage_api_key="demo",
            horizon="3month"
        )

        parquet_file = "cache/get_earnings_calendar_alpha_vantage/all/3month/data.parquet"

        # Test various query patterns
        # Filter by symbol
        aapl_data = duckdb.query(f"""
            SELECT * FROM read_parquet('{parquet_file}')
            WHERE symbol = 'AAPL'
        """).to_df()

        if len(aapl_data) > 0:
            assert aapl_data.iloc[0]['symbol'] == 'AAPL'

        # Filter by date range
        upcoming = duckdb.query(f"""
            SELECT COUNT(*) as count
            FROM read_parquet('{parquet_file}')
            WHERE reportDate >= '2025-11-01'
        """).fetchone()[0]

        assert upcoming >= 0, "Should be able to query by date"

        # Group by aggregation (cast to DATE first since it's stored as VARCHAR)
        by_month = duckdb.query(f"""
            SELECT strftime(CAST(reportDate AS DATE), '%Y-%m') as month, COUNT(*) as count
            FROM read_parquet('{parquet_file}')
            WHERE reportDate IS NOT NULL AND reportDate != ''
            GROUP BY month
            ORDER BY month
            LIMIT 3
        """).to_df()

        assert len(by_month) > 0, "Should be able to group by month"
