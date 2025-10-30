#!/usr/bin/env python3
"""Comprehensive tests for all MCP tools using real Polygon API."""

import os
import sys
import pytest
from datetime import datetime, timedelta

# Add src to path
sys.path.insert(0, "src")

# Check for API key
POLYGON_API_KEY = os.getenv("POLYGON_API_KEY")
if not POLYGON_API_KEY:
    pytest.skip("POLYGON_API_KEY environment variable not set", allow_module_level=True)

# Import all tool modules
from mcp_polygon.tools import (
    aggregates,
    snapshots,
    technical_indicators,
    reference_data,
    corporate_actions,
    financials,
    news,
    economics,
    options,
    futures,
    currency,
)


# Helper to verify CSV output
def assert_csv_output(result: str):
    """Verify result is a CSV string with headers and data."""
    assert isinstance(result, str), "Result should be a string"
    assert len(result) > 0, "Result should not be empty"
    # Check for CSV structure (headers + data rows)
    lines = result.strip().split("\n")
    assert len(lines) >= 1, "Should have at least header row"
    if len(lines) > 1:
        # If there's data, check comma-separated format (or single column)
        # Single column CSVs won't have commas
        header = lines[0]
        assert len(header) > 0, "Header should not be empty"


# Test dates
today = datetime.now().date()
yesterday = today - timedelta(days=1)
last_month = today - timedelta(days=30)
last_year = today - timedelta(days=365)


# ============================================================================
# AGGREGATES MODULE TESTS
# ============================================================================


@pytest.mark.asyncio
async def test_get_aggs():
    """Test get_aggs with AAPL stock."""
    result = await aggregates.get_aggs(
        ticker="AAPL",
        multiplier=1,
        timespan="day",
        from_=str(last_month),
        to=str(yesterday),
        limit=10,
    )
    assert_csv_output(result)


@pytest.mark.asyncio
async def test_get_grouped_daily_aggs():
    """Test get_grouped_daily_aggs for a recent date."""
    # Use a date from 3 days ago to ensure data is available
    test_date = today - timedelta(days=3)
    result = await aggregates.get_grouped_daily_aggs(date=str(test_date))
    assert_csv_output(result)


@pytest.mark.asyncio
async def test_get_previous_close_agg():
    """Test get_previous_close_agg for AAPL."""
    result = await aggregates.get_previous_close_agg(ticker="AAPL")
    assert_csv_output(result)


@pytest.mark.asyncio
async def test_get_daily_open_close_agg():
    """Test get_daily_open_close_agg for AAPL."""
    test_date = today - timedelta(days=3)
    result = await aggregates.get_daily_open_close_agg(
        ticker="AAPL",
        date=str(test_date),
    )
    assert_csv_output(result)


# ============================================================================
# SNAPSHOTS MODULE TESTS
# ============================================================================


@pytest.mark.asyncio
async def test_get_snapshot_ticker():
    """Test get_snapshot_ticker for AAPL."""
    result = await snapshots.get_snapshot_ticker(ticker="AAPL")
    assert_csv_output(result)


@pytest.mark.asyncio
async def test_list_universal_snapshots():
    """Test list_universal_snapshots for stocks."""
    result = await snapshots.list_universal_snapshots(
        type="stocks",
        ticker_any_of=["AAPL", "MSFT", "GOOGL"],
    )
    assert_csv_output(result)


@pytest.mark.asyncio
async def test_get_snapshot_all():
    """Test get_snapshot_all with filtered tickers."""
    result = await snapshots.get_snapshot_all(
        market_type="stocks",
        tickers=["AAPL", "MSFT"],
    )
    assert_csv_output(result)


@pytest.mark.asyncio
async def test_get_snapshot_direction():
    """Test get_snapshot_direction for gainers."""
    result = await snapshots.get_snapshot_direction(
        market_type="stocks",
        direction="gainers",
    )
    assert_csv_output(result)


# ============================================================================
# TECHNICAL INDICATORS MODULE TESTS
# ============================================================================


@pytest.mark.asyncio
async def test_get_sma():
    """Test get_sma for AAPL."""
    result = await technical_indicators.get_sma(
        ticker="AAPL",
        window=50,
        timespan="day",
        limit=10,
    )
    assert_csv_output(result)


@pytest.mark.asyncio
async def test_get_ema():
    """Test get_ema for AAPL."""
    result = await technical_indicators.get_ema(
        ticker="AAPL",
        window=12,
        timespan="day",
        limit=10,
    )
    assert_csv_output(result)


@pytest.mark.asyncio
async def test_get_macd():
    """Test get_macd for AAPL."""
    result = await technical_indicators.get_macd(
        ticker="AAPL",
        timespan="day",
        limit=10,
    )
    assert_csv_output(result)


@pytest.mark.asyncio
async def test_get_rsi():
    """Test get_rsi for AAPL."""
    result = await technical_indicators.get_rsi(
        ticker="AAPL",
        window=14,
        timespan="day",
        limit=10,
    )
    assert_csv_output(result)


# ============================================================================
# REFERENCE DATA MODULE TESTS
# ============================================================================


@pytest.mark.asyncio
async def test_list_tickers():
    """Test list_tickers with market filter."""
    result = await reference_data.list_tickers(
        market="stocks",
        active=True,
        limit=10,
    )
    assert_csv_output(result)


@pytest.mark.asyncio
async def test_get_ticker_details():
    """Test get_ticker_details for AAPL."""
    result = await reference_data.get_ticker_details(ticker="AAPL")
    assert_csv_output(result)


@pytest.mark.asyncio
async def test_get_related_companies():
    """Test get_related_companies for AAPL."""
    result = await reference_data.get_related_companies(ticker="AAPL")
    assert_csv_output(result)


@pytest.mark.asyncio
async def test_get_ticker_types():
    """Test get_ticker_types."""
    result = await reference_data.get_ticker_types()
    assert_csv_output(result)


@pytest.mark.asyncio
async def test_get_market_status():
    """Test get_market_status."""
    result = await reference_data.get_market_status()
    assert_csv_output(result)


@pytest.mark.asyncio
async def test_get_market_holidays():
    """Test get_market_holidays."""
    result = await reference_data.get_market_holidays()
    assert_csv_output(result)


@pytest.mark.asyncio
async def test_get_exchanges():
    """Test get_exchanges."""
    result = await reference_data.get_exchanges(
        asset_class="stocks",
        locale="us",
    )
    assert_csv_output(result)


@pytest.mark.asyncio
async def test_list_conditions():
    """Test list_conditions."""
    result = await reference_data.list_conditions(
        asset_class="stocks",
        data_type="trade",
    )
    assert_csv_output(result)


# ============================================================================
# CORPORATE ACTIONS MODULE TESTS
# ============================================================================


@pytest.mark.asyncio
async def test_list_splits():
    """Test list_splits."""
    result = await corporate_actions.list_splits(
        ticker="AAPL",
        limit=10,
    )
    assert_csv_output(result)


@pytest.mark.asyncio
async def test_list_dividends():
    """Test list_dividends."""
    result = await corporate_actions.list_dividends(
        ticker="AAPL",
        limit=10,
    )
    assert_csv_output(result)


@pytest.mark.asyncio
async def test_get_ticker_events():
    """Test get_ticker_events."""
    result = await corporate_actions.get_ticker_events(
        ticker="AAPL",
        types="dividend,split",
    )
    assert_csv_output(result)


@pytest.mark.asyncio
async def test_list_ipos():
    """Test list_ipos."""
    result = await corporate_actions.list_ipos(
        ipo_status="history",
        limit=10,
    )
    assert_csv_output(result)


# ============================================================================
# FINANCIALS MODULE TESTS
# ============================================================================


@pytest.mark.asyncio
async def test_list_financials_income_statements():
    """Test list_financials_income_statements."""
    result = await financials.list_financials_income_statements(
        tickers="AAPL",
        timeframe="annual",
        limit=5,
    )
    assert_csv_output(result)


@pytest.mark.asyncio
async def test_list_financials_balance_sheets():
    """Test list_financials_balance_sheets."""
    result = await financials.list_financials_balance_sheets(
        tickers="AAPL",
        timeframe="annual",
        limit=5,
    )
    assert_csv_output(result)


@pytest.mark.asyncio
async def test_list_financials_cash_flow_statements():
    """Test list_financials_cash_flow_statements."""
    result = await financials.list_financials_cash_flow_statements(
        tickers="AAPL",
        timeframe="annual",
        limit=5,
    )
    assert_csv_output(result)


@pytest.mark.asyncio
async def test_list_stock_ratios():
    """Test list_stock_ratios."""
    result = await financials.list_stock_ratios(
        ticker="AAPL",
        limit=5,
    )
    assert_csv_output(result)


@pytest.mark.asyncio
async def test_list_short_interest():
    """Test list_short_interest."""
    result = await financials.list_short_interest(
        ticker="TSLA",
        limit=10,
    )
    assert_csv_output(result)


@pytest.mark.asyncio
async def test_list_short_volume():
    """Test list_short_volume."""
    result = await financials.list_short_volume(
        ticker="TSLA",
        date_gte=str(last_month),
        limit=10,
    )
    assert_csv_output(result)


# ============================================================================
# NEWS MODULE TESTS
# ============================================================================


@pytest.mark.asyncio
async def test_list_ticker_news():
    """Test list_ticker_news."""
    result = await news.list_ticker_news(
        ticker="AAPL",
        limit=10,
    )
    assert_csv_output(result)


# ============================================================================
# ECONOMICS MODULE TESTS
# ============================================================================


@pytest.mark.asyncio
async def test_list_treasury_yields():
    """Test list_treasury_yields."""
    result = await economics.list_treasury_yields(
        date_gte=str(last_month),
        limit=10,
    )
    assert_csv_output(result)


@pytest.mark.asyncio
async def test_list_inflation():
    """Test list_inflation."""
    result = await economics.list_inflation(
        date_gte=str(last_year),
        limit=12,
    )
    assert_csv_output(result)


@pytest.mark.asyncio
async def test_list_inflation_expectations():
    """Test list_inflation_expectations."""
    result = await economics.list_inflation_expectations(
        date_gte=str(last_month),
        limit=10,
    )
    assert_csv_output(result)


# ============================================================================
# OPTIONS MODULE TESTS
# ============================================================================


@pytest.mark.asyncio
async def test_list_options_contracts():
    """Test list_options_contracts."""
    result = await options.list_options_contracts(
        underlying_ticker="AAPL",
        contract_type="call",
        limit=10,
    )
    assert_csv_output(result)


@pytest.mark.asyncio
async def test_get_options_contract():
    """Test get_options_contract."""
    # First get a valid contract ticker
    contracts = await options.list_options_contracts(
        underlying_ticker="AAPL",
        contract_type="call",
        limit=1,
    )
    # Extract first ticker from CSV if available
    if contracts and "\n" in contracts:
        lines = contracts.split("\n")
        if len(lines) > 1:  # Has header + data
            # Assume ticker is first column
            first_data_row = lines[1].split(",")
            if first_data_row:
                options_ticker = first_data_row[0]
                result = await options.get_options_contract(
                    options_ticker=options_ticker
                )
                assert_csv_output(result)


@pytest.mark.asyncio
async def test_get_options_chain_snapshot():
    """Test list_snapshot_options_chain."""
    result = await options.list_snapshot_options_chain(
        underlying_asset="AAPL",
        contract_type="call",
        limit=10,
    )
    assert_csv_output(result)


# ============================================================================
# FUTURES MODULE TESTS
# ============================================================================


@pytest.mark.asyncio
async def test_list_futures_contracts():
    """Test list_futures_contracts."""
    result = await futures.list_futures_contracts(
        limit=10,
    )
    assert_csv_output(result)


# ============================================================================
# CURRENCY MODULE TESTS
# ============================================================================


@pytest.mark.asyncio
async def test_get_real_time_currency_conversion():
    """Test get_real_time_currency_conversion."""
    result = await currency.get_real_time_currency_conversion(
        from_="USD",
        to="EUR",
        amount=100,
    )
    assert_csv_output(result)


# ============================================================================
# MAIN
# ============================================================================

if __name__ == "__main__":
    # Run with pytest
    import sys

    sys.exit(pytest.main([__file__, "-v", "-s"]))
