"""Test fix for list_short_interest and list_short_volume with None ticker."""

from src.mcp_polygon.cache_manager import CacheManager
from src.mcp_polygon.utils import build_params


class TestShortDataNoneFix:
    """Test that short data tools handle None ticker correctly."""

    def test_build_params_filters_none_values(self):
        """Test that build_params utility filters out None values."""
        result = build_params(ticker="AAPL", limit=10, timeframe=None, fetch_all=True)

        assert result == {"ticker": "AAPL", "limit": 10, "fetch_all": True}
        assert "timeframe" not in result

    def test_build_params_with_all_none(self):
        """Test that build_params returns empty dict when all values are None."""
        result = build_params(ticker=None, limit=None, timeframe=None)

        assert result == {}

    def test_build_params_preserves_falsy_non_none_values(self):
        """Test that build_params keeps falsy values that aren't None."""
        result = build_params(ticker="", limit=0, fetch_all=False, count=None)

        # Should keep empty string, 0, and False but not None
        assert result == {"ticker": "", "limit": 0, "fetch_all": False}
        assert "count" not in result

    def test_short_interest_partition_without_ticker_key(self, tmp_path):
        """Test that list_short_interest works when ticker key is absent."""
        cache_mgr = CacheManager(cache_dir=str(tmp_path / "cache"))

        # Params built with build_params won't have ticker key if ticker=None
        params = build_params(ticker=None, fetch_all=True)
        partition_key = cache_mgr._generate_partition_key("list_short_interest", params)

        # Should use "all" as default (from params.get("ticker", "all"))
        assert partition_key == "all"

    def test_short_volume_partition_without_ticker_key(self, tmp_path):
        """Test that list_short_volume works when ticker key is absent."""
        cache_mgr = CacheManager(cache_dir=str(tmp_path / "cache"))

        # Params built with build_params won't have ticker key if ticker=None
        params = build_params(ticker=None, fetch_all=True)
        partition_key = cache_mgr._generate_partition_key("list_short_volume", params)

        # Should use "all" as default
        assert partition_key == "all"

    def test_short_interest_partition_with_ticker(self, tmp_path):
        """Test that list_short_interest generates valid partition key with ticker."""
        cache_mgr = CacheManager(cache_dir=str(tmp_path / "cache"))

        partition_key = cache_mgr._generate_partition_key(
            "list_short_interest", {"ticker": "GME", "fetch_all": True}
        )

        assert partition_key == "GME"

    def test_short_volume_partition_with_ticker_and_date(self, tmp_path):
        """Test that list_short_volume generates valid partition key with ticker and date."""
        cache_mgr = CacheManager(cache_dir=str(tmp_path / "cache"))

        partition_key = cache_mgr._generate_partition_key(
            "list_short_volume",
            {"ticker": "TSLA", "date_gte": "2025-03-01", "fetch_all": True},
        )

        assert partition_key == "TSLA/2025-03"

    def test_get_partition_path_returns_valid_path(self, tmp_path):
        """Test that _get_partition_path returns valid Path object."""
        cache_mgr = CacheManager(cache_dir=str(tmp_path / "cache"))

        # Build params without None values
        params = build_params(ticker=None, fetch_all=True)

        # This should not raise an exception
        partition_path, partition_key = cache_mgr._get_partition_path(
            "list_short_interest", params
        )

        # Should be a valid path
        assert partition_path.is_absolute() or partition_path.is_relative_to(
            tmp_path / "cache"
        )
        assert partition_key == "all"
        assert "list_short_interest" in str(partition_path)
        assert "all" in str(partition_path)

    def test_other_tools_with_build_params(self, tmp_path):
        """Test that other tools work correctly with build_params utility."""
        cache_mgr = CacheManager(cache_dir=str(tmp_path / "cache"))

        # Test various tools with None parameters filtered by build_params
        test_cases = [
            ("get_aggs", build_params(ticker=None, from_="2025-01-01")),
            ("list_financials_balance_sheets", build_params(tickers=None)),
            ("list_stock_ratios", build_params(ticker=None)),
            ("list_dividends", build_params(ticker=None)),
            ("list_splits", build_params(ticker=None)),
            ("list_ticker_news", build_params(ticker=None)),
            ("get_sma", build_params(ticker=None)),
            ("list_stock_financials", build_params(ticker=None)),
        ]

        for tool_name, params in test_cases:
            # None of these should raise an exception
            partition_key = cache_mgr._generate_partition_key(tool_name, params)
            assert partition_key is not None
            assert isinstance(partition_key, str)
            # Should not contain "None" string
            assert "None" not in partition_key
            # Params should not have any None values
            assert None not in params.values()
