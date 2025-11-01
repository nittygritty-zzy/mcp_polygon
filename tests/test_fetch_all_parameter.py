"""
Test fetch_all parameter across all paginated tools.

This test suite verifies that:
1. fetch_all=True uses parallel fetching (default)
2. fetch_all=False uses single-page fetching
3. Both code paths work correctly
"""

from unittest.mock import Mock, patch
import pytest
import json


# Test data helpers
def create_mock_response(results, next_cursor=None):
    """Create a mock API response."""
    response = Mock()
    data = {"results": results, "status": "OK"}
    if next_cursor:
        data["next_url"] = f"https://api.polygon.io/v2/aggs?cursor={next_cursor}"
    response.data = json.dumps(data).encode("utf-8")
    return response


# ============================================================================
# AGGREGATES MODULE TESTS
# ============================================================================


@pytest.mark.asyncio
async def test_get_aggs_with_fetch_all_true():
    """Test get_aggs with fetch_all=True uses parallel fetcher with batch writing."""
    from mcp_polygon.tools import aggregates
    from mcp_polygon.parallel_fetcher import PolygonParallelFetcher

    # Mock create_batch_writer in the aggregates module to return None callbacks (forces memory mode fallback)
    with (
        patch.object(aggregates, "create_batch_writer") as mock_batch_writer,
        patch.object(PolygonParallelFetcher, "fetch_all") as mock_fetch,
    ):
        mock_batch_writer.return_value = (None, None)  # Force memory mode
        mock_fetch.return_value = [
            {"t": 1, "o": 100, "h": 105, "l": 99, "c": 103, "v": 1000},
            {"t": 2, "o": 103, "h": 107, "l": 102, "c": 106, "v": 1100},
        ]

        result = await aggregates.get_aggs(
            ticker="AAPL",
            multiplier=1,
            timespan="day",
            from_="2024-01-01",
            to="2024-01-31",
            fetch_all=True,
        )

        # Verify batch writer was called to check if batch mode should be used
        mock_batch_writer.assert_called_once()
        # Verify parallel fetcher was called
        mock_fetch.assert_called_once()
        assert "t,o,h,l,c,v" in result or "t" in result  # CSV output


@pytest.mark.asyncio
async def test_get_aggs_with_fetch_all_false():
    """Test get_aggs with fetch_all=False uses single-page approach."""
    from mcp_polygon.tools import aggregates
    from mcp_polygon.clients import polygon_client

    mock_response = create_mock_response(
        [{"t": 1, "o": 100, "h": 105, "l": 99, "c": 103, "v": 1000}]
    )

    with patch.object(polygon_client, "get_aggs", return_value=mock_response):
        result = await aggregates.get_aggs(
            ticker="AAPL",
            multiplier=1,
            timespan="day",
            from_="2024-01-01",
            to="2024-01-31",
            fetch_all=False,
        )

        # Verify we got CSV output
        assert isinstance(result, str)
        assert len(result) > 0


# ============================================================================
# REFERENCE DATA MODULE TESTS
# ============================================================================


@pytest.mark.asyncio
async def test_list_tickers_with_fetch_all_true():
    """Test list_tickers with fetch_all=True uses parallel fetcher."""
    from mcp_polygon.tools import reference_data
    from mcp_polygon.parallel_fetcher import PolygonParallelFetcher

    with patch.object(PolygonParallelFetcher, "fetch_all") as mock_fetch:
        mock_fetch.return_value = [
            {"ticker": "AAPL", "name": "Apple Inc.", "market": "stocks"},
            {"ticker": "MSFT", "name": "Microsoft Corp.", "market": "stocks"},
        ]

        result = await reference_data.list_tickers(
            market="stocks", active=True, fetch_all=True
        )

        # Verify parallel fetcher was called
        mock_fetch.assert_called_once()
        assert isinstance(result, str)


@pytest.mark.asyncio
async def test_list_tickers_with_fetch_all_false():
    """Test list_tickers with fetch_all=False uses single-page approach."""
    from mcp_polygon.tools import reference_data
    from mcp_polygon.clients import polygon_client

    mock_response = create_mock_response(
        [{"ticker": "AAPL", "name": "Apple Inc.", "market": "stocks"}]
    )

    with patch.object(polygon_client, "list_tickers", return_value=mock_response):
        result = await reference_data.list_tickers(
            market="stocks", active=True, fetch_all=False
        )

        assert isinstance(result, str)
        assert len(result) > 0


# ============================================================================
# OPTIONS MODULE TESTS
# ============================================================================


@pytest.mark.asyncio
async def test_list_options_contracts_with_fetch_all_true():
    """Test list_options_contracts with fetch_all=True uses parallel fetcher."""
    from mcp_polygon.tools import options
    from mcp_polygon.parallel_fetcher import PolygonParallelFetcher

    with patch.object(PolygonParallelFetcher, "fetch_all") as mock_fetch:
        mock_fetch.return_value = [
            {
                "ticker": "O:AAPL251219C00150000",
                "strike_price": 150.0,
                "expiration_date": "2025-12-19",
            }
        ]

        result = await options.list_options_contracts(
            underlying_ticker="AAPL", contract_type="call", fetch_all=True
        )

        # Verify parallel fetcher was called
        mock_fetch.assert_called_once()
        assert isinstance(result, str)


@pytest.mark.asyncio
async def test_list_options_contracts_with_fetch_all_false():
    """Test list_options_contracts with fetch_all=False uses single-page approach."""
    from mcp_polygon.tools import options
    from mcp_polygon.clients import polygon_client

    mock_response = create_mock_response(
        [
            {
                "ticker": "O:AAPL251219C00150000",
                "strike_price": 150.0,
                "expiration_date": "2025-12-19",
            }
        ]
    )

    with patch.object(
        polygon_client, "list_options_contracts", return_value=mock_response
    ):
        result = await options.list_options_contracts(
            underlying_ticker="AAPL", contract_type="call", fetch_all=False
        )

        assert isinstance(result, str)
        assert len(result) > 0


# ============================================================================
# ECONOMICS MODULE TESTS
# ============================================================================


@pytest.mark.asyncio
async def test_list_treasury_yields_with_fetch_all_true():
    """Test list_treasury_yields with fetch_all=True uses parallel fetcher."""
    from mcp_polygon.tools import economics
    from mcp_polygon.parallel_fetcher import PolygonParallelFetcher

    with patch.object(PolygonParallelFetcher, "fetch_all") as mock_fetch:
        mock_fetch.return_value = [
            {"date": "2024-01-01", "yield_1_month": 5.0, "yield_10_year": 4.5}
        ]

        result = await economics.list_treasury_yields(
            date_gte="2024-01-01", fetch_all=True
        )

        # Verify parallel fetcher was called
        mock_fetch.assert_called_once()
        assert isinstance(result, str)


# ============================================================================
# CORPORATE ACTIONS MODULE TESTS
# ============================================================================


@pytest.mark.asyncio
async def test_list_splits_with_fetch_all_true():
    """Test list_splits with fetch_all=True uses parallel fetcher."""
    from mcp_polygon.tools import corporate_actions
    from mcp_polygon.parallel_fetcher import PolygonParallelFetcher

    with patch.object(PolygonParallelFetcher, "fetch_all") as mock_fetch:
        mock_fetch.return_value = [
            {
                "ticker": "AAPL",
                "execution_date": "2020-08-31",
                "split_from": 1.0,
                "split_to": 4.0,
            }
        ]

        result = await corporate_actions.list_splits(ticker="AAPL", fetch_all=True)

        # Verify parallel fetcher was called
        mock_fetch.assert_called_once()
        assert isinstance(result, str)


@pytest.mark.asyncio
async def test_list_ipos_with_vx_client():
    """Test list_ipos uses VX client with parallel fetcher."""
    from mcp_polygon.tools import corporate_actions
    from mcp_polygon.parallel_fetcher import PolygonParallelFetcher

    with patch.object(PolygonParallelFetcher, "fetch_all") as mock_fetch:
        mock_fetch.return_value = [
            {"ticker": "SNOW", "listing_date": "2020-09-16", "ipo_status": "new"}
        ]

        result = await corporate_actions.list_ipos(ipo_status="new", fetch_all=True)

        # Verify parallel fetcher was called with use_vx=True
        mock_fetch.assert_called_once()
        call_kwargs = mock_fetch.call_args.kwargs
        assert call_kwargs.get("use_vx") == True  # noqa: E712
        assert isinstance(result, str)


# ============================================================================
# FINANCIALS MODULE TESTS
# ============================================================================


@pytest.mark.asyncio
async def test_list_stock_financials_with_vx_client():
    """Test list_stock_financials uses VX client with parallel fetcher."""
    from mcp_polygon.tools import financials
    from mcp_polygon.parallel_fetcher import PolygonParallelFetcher

    with patch.object(PolygonParallelFetcher, "fetch_all") as mock_fetch:
        mock_fetch.return_value = [
            {"ticker": "AAPL", "fiscal_year": 2023, "timeframe": "annual"}
        ]

        result = await financials.list_stock_financials(ticker="AAPL", fetch_all=True)

        # Verify parallel fetcher was called with use_vx=True
        mock_fetch.assert_called_once()
        call_kwargs = mock_fetch.call_args.kwargs
        assert call_kwargs.get("use_vx") == True  # noqa: E712
        assert isinstance(result, str)


# ============================================================================
# TECHNICAL INDICATORS MODULE TESTS
# ============================================================================


@pytest.mark.asyncio
async def test_get_sma_with_fetch_all_true():
    """Test get_sma with fetch_all=True uses parallel fetcher."""
    from mcp_polygon.tools import technical_indicators
    from mcp_polygon.parallel_fetcher import PolygonParallelFetcher

    with patch.object(PolygonParallelFetcher, "fetch_all") as mock_fetch:
        mock_fetch.return_value = [{"timestamp": 1234567890, "value": 150.5}]

        result = await technical_indicators.get_sma(
            ticker="AAPL", window=50, timespan="day", fetch_all=True
        )

        # Verify parallel fetcher was called
        mock_fetch.assert_called_once()
        assert isinstance(result, str)


# ============================================================================
# SNAPSHOTS MODULE TESTS
# ============================================================================


@pytest.mark.asyncio
async def test_list_universal_snapshots_with_fetch_all_true():
    """Test list_universal_snapshots with fetch_all=True uses parallel fetcher."""
    from mcp_polygon.tools import snapshots
    from mcp_polygon.parallel_fetcher import PolygonParallelFetcher

    with patch.object(PolygonParallelFetcher, "fetch_all") as mock_fetch:
        mock_fetch.return_value = [
            {"ticker": "AAPL", "type": "stocks", "market_status": "open"}
        ]

        result = await snapshots.list_universal_snapshots(
            type="stocks", ticker_any_of=["AAPL", "MSFT"], fetch_all=True
        )

        # Verify parallel fetcher was called
        mock_fetch.assert_called_once()
        assert isinstance(result, str)


# ============================================================================
# INTEGRATION TEST
# ============================================================================


@pytest.mark.asyncio
async def test_parallel_fetcher_respects_num_workers():
    """Test that parallel fetcher uses configured number of workers."""
    from mcp_polygon.parallel_fetcher import PolygonParallelFetcher
    from mcp_polygon.clients import polygon_client

    # Create fetcher with specific number of workers
    fetcher = PolygonParallelFetcher(polygon_client, num_workers=5)
    assert fetcher.num_workers == 5

    # Test with different number
    fetcher = PolygonParallelFetcher(polygon_client, num_workers=10)
    assert fetcher.num_workers == 10


if __name__ == "__main__":
    # Run tests
    pytest.main([__file__, "-v"])
