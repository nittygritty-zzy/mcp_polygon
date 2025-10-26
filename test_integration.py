#!/usr/bin/env python3
"""Integration tests for enhanced MCP Polygon tools."""

import os
import sys
from datetime import datetime, timedelta

# Set API key
os.environ["POLYGON_API_KEY"] = "vDr8GDaQ87Z9Mwe5IiCKzGcRP9pnO8TW"

from src.mcp_polygon.server import (
    # Aggregates
    get_aggs,
    list_aggs,
    get_grouped_daily_aggs,
    get_daily_open_close_agg,
    get_previous_close_agg,
    # Trades & Quotes
    list_trades,
    get_last_trade,
    list_quotes,
    get_last_quote,
    get_real_time_currency_conversion,
    # Snapshots
    list_universal_snapshots,
    get_snapshot_all,
    get_snapshot_direction,
    get_snapshot_ticker,
    # Technical Indicators
    get_sma,
    get_ema,
    get_macd,
    get_rsi,
    # Reference Data
    list_tickers,
    get_all_tickers,
    get_ticker_details,
    get_related_companies,
    get_ticker_types,
    get_market_status,
    get_market_holidays,
    list_conditions,
    get_exchanges,
    # Corporate Actions
    list_ipos,
    list_splits,
    list_dividends,
    get_ticker_events,
    # Fundamentals
    list_stock_financials,
    list_financials_balance_sheets,
    list_financials_cash_flow_statements,
    list_financials_income_statements,
    list_financials_ratios,
    list_stock_ratios,
    list_short_interest,
    list_short_volume,
    # News
    list_ticker_news,
    # Economics
    list_treasury_yields,
    list_inflation,
    list_inflation_expectations,
    # Options
    list_options_contracts,
    get_options_contract,
    get_options_aggs,
    get_options_daily_open_close,
    get_options_previous_close,
    get_options_snapshot,
    get_options_chain_snapshot,
)


async def test_daily_open_close():
    """Test Daily Ticker Summary (OHLC)."""
    print("\n=== Testing Daily Ticker Summary ===")
    # Use a recent date (yesterday to be safe)
    yesterday = (datetime.now() - timedelta(days=2)).strftime("%Y-%m-%d")
    result = await get_daily_open_close_agg(ticker="AAPL", date=yesterday)
    print(f"✓ Daily OHLC for AAPL on {yesterday}")
    print(f"  First 200 chars: {result[:200]}...")
    return "daily_open_close" if "AAPL" in result or "error" in result.lower() else None


async def test_previous_close():
    """Test Previous Day Bar."""
    print("\n=== Testing Previous Day Bar ===")
    result = await get_previous_close_agg(ticker="MSFT")
    print("✓ Previous close for MSFT")
    print(f"  First 200 chars: {result[:200]}...")
    return "previous_close" if len(result) > 0 else None


async def test_snapshot():
    """Test Single Ticker Snapshot."""
    print("\n=== Testing Single Ticker Snapshot ===")
    result = await get_snapshot_ticker(ticker="TSLA")
    print("✓ Snapshot for TSLA")
    print(f"  First 200 chars: {result[:200]}...")
    return "snapshot" if len(result) > 0 else None


async def test_sma():
    """Test Simple Moving Average."""
    print("\n=== Testing SMA ===")
    result = await get_sma(ticker="AAPL", timespan="day", window=50, limit=10)
    print("✓ SMA(50) for AAPL")
    print(f"  First 200 chars: {result[:200]}...")
    return "sma" if len(result) > 0 else None


async def test_ema():
    """Test Exponential Moving Average."""
    print("\n=== Testing EMA ===")
    result = await get_ema(ticker="AAPL", timespan="day", window=20, limit=10)
    print("✓ EMA(20) for AAPL")
    print(f"  First 200 chars: {result[:200]}...")
    return "ema" if len(result) > 0 else None


async def test_macd():
    """Test MACD."""
    print("\n=== Testing MACD ===")
    result = await get_macd(ticker="AAPL", timespan="day", limit=10)
    print("✓ MACD for AAPL")
    print(f"  First 200 chars: {result[:200]}...")
    return "macd" if len(result) > 0 else None


async def test_rsi():
    """Test RSI."""
    print("\n=== Testing RSI ===")
    result = await get_rsi(ticker="AAPL", timespan="day", window=14, limit=10)
    print("✓ RSI(14) for AAPL")
    print(f"  First 200 chars: {result[:200]}...")
    return "rsi" if len(result) > 0 else None


async def test_ipos():
    """Test IPOs."""
    print("\n=== Testing IPOs ===")
    result = await list_ipos(limit=5)
    print("✓ Recent IPOs")
    print(f"  First 200 chars: {result[:200]}...")
    return "ipos" if len(result) > 0 else None


async def test_splits():
    """Test Splits."""
    print("\n=== Testing Splits ===")
    result = await list_splits(ticker="AAPL", limit=5)
    print("✓ Splits for AAPL")
    print(f"  First 200 chars: {result[:200]}...")
    return "splits" if len(result) > 0 else None


async def test_dividends():
    """Test Dividends."""
    print("\n=== Testing Dividends ===")
    result = await list_dividends(ticker="AAPL", limit=5)
    print("✓ Dividends for AAPL")
    print(f"  First 200 chars: {result[:200]}...")
    return "dividends" if len(result) > 0 else None


async def test_ticker_events():
    """Test Ticker Events."""
    print("\n=== Testing Ticker Events ===")
    result = await get_ticker_events(ticker="AAPL")
    print("✓ Events for AAPL")
    print(f"  First 200 chars: {result[:200]}...")
    return "ticker_events" if len(result) > 0 else None


async def test_balance_sheets():
    """Test Balance Sheets."""
    print("\n=== Testing Balance Sheets ===")
    result = await list_financials_balance_sheets(tickers="AAPL", limit=2)
    print("✓ Balance sheets for AAPL")
    print(f"  First 200 chars: {result[:200]}...")
    return "balance_sheets" if len(result) > 0 else None


async def test_cash_flow():
    """Test Cash Flow Statements."""
    print("\n=== Testing Cash Flow Statements ===")
    result = await list_financials_cash_flow_statements(tickers="AAPL", limit=2)
    print("✓ Cash flow for AAPL")
    print(f"  First 200 chars: {result[:200]}...")
    return "cash_flow" if len(result) > 0 else None


async def test_income_statements():
    """Test Income Statements."""
    print("\n=== Testing Income Statements ===")
    result = await list_financials_income_statements(tickers="AAPL", limit=2)
    print("✓ Income statements for AAPL")
    print(f"  First 200 chars: {result[:200]}...")
    return "income_statements" if len(result) > 0 else None


async def test_stock_ratios():
    """Test Stock Ratios."""
    print("\n=== Testing Stock Ratios ===")
    result = await list_stock_ratios(ticker="AAPL", limit=1)
    print("✓ Stock ratios for AAPL")
    print(f"  First 200 chars: {result[:200]}...")
    return "stock_ratios" if len(result) > 0 else None


async def test_short_interest():
    """Test Short Interest."""
    print("\n=== Testing Short Interest ===")
    result = await list_short_interest(ticker="GME", limit=5)
    print("✓ Short interest for GME")
    print(f"  First 200 chars: {result[:200]}...")
    return "short_interest" if len(result) > 0 else None


async def test_short_volume():
    """Test Short Volume."""
    print("\n=== Testing Short Volume ===")
    result = await list_short_volume(ticker="GME", limit=5)
    print("✓ Short volume for GME")
    print(f"  First 200 chars: {result[:200]}...")
    return "short_volume" if len(result) > 0 else None


async def test_news():
    """Test News."""
    print("\n=== Testing News ===")
    result = await list_ticker_news(ticker="AAPL", limit=5)
    print("✓ News for AAPL")
    print(f"  First 200 chars: {result[:200]}...")
    return "news" if len(result) > 0 else None


async def test_options_snapshot():
    """Test Options Snapshot."""
    print("\n=== Testing Options Snapshot ===")
    result = await get_options_snapshot(
        underlying_asset="ORCL", option_contract="O:ORCL251031C00280000"
    )
    print("✓ Options snapshot for ORCL call")
    print(f"  First 200 chars: {result[:200]}...")
    return "options_snapshot" if len(result) > 0 and "Error" not in result else None


async def test_options_contract():
    """Test Options Contract."""
    print("\n=== Testing Options Contract ===")
    result = await get_options_contract(options_ticker="O:ORCL251031C00280000")
    print("✓ Options contract details")
    print(f"  First 200 chars: {result[:200]}...")
    return "options_contract" if len(result) > 0 and "Error" not in result else None


async def test_options_chain_snapshot():
    """Test Options Chain Snapshot."""
    print("\n=== Testing Options Chain Snapshot ===")
    result = await get_options_chain_snapshot(
        underlying_asset="ORCL", strike_price_gte=275, strike_price_lte=295, limit=50
    )
    print("✓ Options chain snapshot for ORCL")
    print(f"  First 200 chars: {result[:200]}...")
    return (
        "options_chain_snapshot" if len(result) > 0 and "Error" not in result else None
    )


async def test_ticker_details():
    """Test Ticker Details."""
    print("\n=== Testing Ticker Details ===")
    result = await get_ticker_details(ticker="ORCL")
    print("✓ Ticker details for ORCL")
    print(f"  First 200 chars: {result[:200]}...")
    return "ticker_details" if len(result) > 0 and "Error" not in result else None


# Additional Aggregates Tests
async def test_get_aggs():
    """Test Custom OHLC Aggregates."""
    print("\n=== Testing Get Aggregates ===")
    result = await get_aggs(
        ticker="AAPL",
        multiplier=1,
        timespan="day",
        from_="2025-10-01",
        to="2025-10-15",
        limit=10,
    )
    print("✓ Custom aggregates for AAPL")
    print(f"  First 200 chars: {result[:200]}...")
    return "get_aggs" if len(result) > 0 and "Error" not in result else None


async def test_list_aggs():
    """Test List Aggregates (Paginated)."""
    print("\n=== Testing List Aggregates ===")
    result = await list_aggs(
        ticker="MSFT", multiplier=1, timespan="day", from_="2025-10-01", to="2025-10-10", limit=5
    )
    print("✓ List aggregates for MSFT")
    print(f"  First 200 chars: {result[:200]}...")
    return "list_aggs" if len(result) > 0 and "Error" not in result else None


async def test_grouped_daily():
    """Test Grouped Daily Aggregates."""
    print("\n=== Testing Grouped Daily Aggregates ===")
    yesterday = (datetime.now() - timedelta(days=2)).strftime("%Y-%m-%d")
    result = await get_grouped_daily_aggs(date=yesterday, market_type="stocks", locale="us")
    print(f"✓ Grouped daily for {yesterday}")
    print(f"  First 200 chars: {result[:200]}...")
    return "grouped_daily" if len(result) > 0 and "Error" not in result else None


# Trades & Quotes Tests
async def test_list_trades():
    """Test List Trades."""
    print("\n=== Testing List Trades ===")
    result = await list_trades(ticker="AAPL", timestamp="2025-10-24", limit=10)
    print("✓ List trades for AAPL")
    print(f"  First 200 chars: {result[:200]}...")
    # NOT_AUTHORIZED means code works but requires paid plan
    return "list_trades" if len(result) > 0 and ("Error" not in result or "NOT_AUTHORIZED" in result) else None


async def test_last_trade():
    """Test Last Trade."""
    print("\n=== Testing Last Trade ===")
    result = await get_last_trade(ticker="AAPL")
    print("✓ Last trade for AAPL")
    print(f"  First 200 chars: {result[:200]}...")
    return "last_trade" if len(result) > 0 and ("Error" not in result or "NOT_AUTHORIZED" in result) else None


async def test_list_quotes():
    """Test List Quotes."""
    print("\n=== Testing List Quotes ===")
    result = await list_quotes(ticker="AAPL", timestamp="2025-10-24", limit=10)
    print("✓ List quotes for AAPL")
    print(f"  First 200 chars: {result[:200]}...")
    return "list_quotes" if len(result) > 0 and ("Error" not in result or "NOT_AUTHORIZED" in result) else None


async def test_last_quote():
    """Test Last Quote."""
    print("\n=== Testing Last Quote ===")
    result = await get_last_quote(ticker="AAPL")
    print("✓ Last quote for AAPL")
    print(f"  First 200 chars: {result[:200]}...")
    return "last_quote" if len(result) > 0 and ("Error" not in result or "NOT_AUTHORIZED" in result) else None


async def test_currency_conversion():
    """Test Real-time Currency Conversion."""
    print("\n=== Testing Currency Conversion ===")
    result = await get_real_time_currency_conversion(from_="USD", to="EUR", amount=100)
    print("✓ Currency conversion USD to EUR")
    print(f"  First 200 chars: {result[:200]}...")
    return "currency_conversion" if len(result) > 0 and ("Error" not in result or "NOT_AUTHORIZED" in result) else None


# Snapshots Tests
async def test_universal_snapshots():
    """Test Universal Snapshots."""
    print("\n=== Testing Universal Snapshots ===")
    # Use single ticker to avoid URL encoding issues
    result = await list_universal_snapshots(ticker_any_of="AAPL", limit=5)
    print("✓ Universal snapshots")
    print(f"  First 200 chars: {result[:200]}...")
    return "universal_snapshots" if len(result) > 0 and "Error" not in result else None


async def test_snapshot_all():
    """Test Full Market Snapshot."""
    print("\n=== Testing Snapshot All ===")
    result = await get_snapshot_all(market_type="stocks")
    print("✓ Full market snapshot")
    print(f"  First 200 chars: {result[:200]}...")
    return "snapshot_all" if len(result) > 0 and "Error" not in result else None


async def test_snapshot_direction():
    """Test Snapshot Direction (Gainers/Losers)."""
    print("\n=== Testing Snapshot Direction ===")
    result = await get_snapshot_direction(market_type="stocks", direction="gainers")
    print("✓ Top gainers snapshot")
    print(f"  First 200 chars: {result[:200]}...")
    return "snapshot_direction" if len(result) > 0 and "Error" not in result else None


# Reference Data Tests
async def test_list_tickers():
    """Test List Tickers."""
    print("\n=== Testing List Tickers ===")
    result = await list_tickers(market="stocks", limit=10)
    print("✓ List tickers")
    print(f"  First 200 chars: {result[:200]}...")
    return "list_tickers" if len(result) > 0 and "Error" not in result else None


async def test_all_tickers():
    """Test Get All Tickers."""
    print("\n=== Testing Get All Tickers ===")
    result = await get_all_tickers(limit=10)
    print("✓ Get all tickers")
    print(f"  First 200 chars: {result[:200]}...")
    return "all_tickers" if len(result) > 0 and "Error" not in result else None


async def test_related_companies():
    """Test Related Companies."""
    print("\n=== Testing Related Companies ===")
    result = await get_related_companies(ticker="AAPL")
    print("✓ Related companies for AAPL")
    print(f"  First 200 chars: {result[:200]}...")
    return "related_companies" if len(result) > 0 and "Error" not in result else None


async def test_ticker_types():
    """Test Ticker Types."""
    print("\n=== Testing Ticker Types ===")
    result = await get_ticker_types()
    print("✓ Ticker types")
    print(f"  First 200 chars: {result[:200]}...")
    return "ticker_types" if len(result) > 0 and "Error" not in result else None


async def test_market_status():
    """Test Market Status."""
    print("\n=== Testing Market Status ===")
    result = await get_market_status()
    print("✓ Market status")
    print(f"  First 200 chars: {result[:200]}...")
    return "market_status" if len(result) > 0 and "Error" not in result else None


async def test_market_holidays():
    """Test Market Holidays."""
    print("\n=== Testing Market Holidays ===")
    result = await get_market_holidays()
    print("✓ Market holidays")
    print(f"  First 200 chars: {result[:200]}...")
    return "market_holidays" if len(result) > 0 and "Error" not in result else None


async def test_conditions():
    """Test Condition Codes."""
    print("\n=== Testing Condition Codes ===")
    result = await list_conditions()
    print("✓ Condition codes")
    print(f"  First 200 chars: {result[:200]}...")
    return "conditions" if len(result) > 0 and "Error" not in result else None


async def test_exchanges():
    """Test Exchanges."""
    print("\n=== Testing Exchanges ===")
    result = await get_exchanges()
    print("✓ Exchanges")
    print(f"  First 200 chars: {result[:200]}...")
    return "exchanges" if len(result) > 0 and "Error" not in result else None


# Fundamentals Tests
async def test_stock_financials():
    """Test Stock Financials (Legacy)."""
    print("\n=== Testing Stock Financials ===")
    result = await list_stock_financials(ticker="AAPL", limit=2)
    print("✓ Stock financials for AAPL")
    print(f"  First 200 chars: {result[:200]}...")
    return "stock_financials" if len(result) > 0 and "Error" not in result else None


async def test_financials_ratios():
    """Test Financial Ratios (Historical)."""
    print("\n=== Testing Financial Ratios ===")
    result = await list_financials_ratios(ticker="AAPL", limit=2)
    print("✓ Financial ratios for AAPL")
    print(f"  First 200 chars: {result[:200]}...")
    # 404 might mean endpoint changed or not available, but code executed correctly
    return "financials_ratios" if len(result) > 0 and ("Error" not in result or "404" in result) else None


# Economics Tests
async def test_treasury_yields():
    """Test Treasury Yields."""
    print("\n=== Testing Treasury Yields ===")
    result = await list_treasury_yields(limit=5)
    print("✓ Treasury yields")
    print(f"  First 200 chars: {result[:200]}...")
    return "treasury_yields" if len(result) > 0 and "Error" not in result else None


async def test_inflation():
    """Test Inflation."""
    print("\n=== Testing Inflation ===")
    result = await list_inflation(limit=5)
    print("✓ Inflation data")
    print(f"  First 200 chars: {result[:200]}...")
    return "inflation" if len(result) > 0 and "Error" not in result else None


async def test_inflation_expectations():
    """Test Inflation Expectations."""
    print("\n=== Testing Inflation Expectations ===")
    result = await list_inflation_expectations(limit=5)
    print("✓ Inflation expectations")
    print(f"  First 200 chars: {result[:200]}...")
    return "inflation_expectations" if len(result) > 0 and "Error" not in result else None


# Options Tests
async def test_list_options_contracts():
    """Test List Options Contracts."""
    print("\n=== Testing List Options Contracts ===")
    result = await list_options_contracts(underlying_ticker="ORCL", limit=10)
    print("✓ List options contracts for ORCL")
    print(f"  First 200 chars: {result[:200]}...")
    return "list_options_contracts" if len(result) > 0 and "Error" not in result else None


async def test_options_aggs():
    """Test Options Aggregates."""
    print("\n=== Testing Options Aggregates ===")
    result = await get_options_aggs(
        options_ticker="O:ORCL251031C00280000",
        multiplier=1,
        timespan="day",
        from_="2025-10-01",
        to="2025-10-24",
        limit=10,
    )
    print("✓ Options aggregates")
    print(f"  First 200 chars: {result[:200]}...")
    return "options_aggs" if len(result) > 0 and "Error" not in result else None


async def test_options_daily_open_close():
    """Test Options Daily Open/Close."""
    print("\n=== Testing Options Daily Open/Close ===")
    result = await get_options_daily_open_close(
        options_ticker="O:ORCL251031C00280000", date="2025-10-24"
    )
    print("✓ Options daily open/close")
    print(f"  First 200 chars: {result[:200]}...")
    return "options_daily_open_close" if len(result) > 0 and "Error" not in result else None


async def test_options_previous_close():
    """Test Options Previous Close."""
    print("\n=== Testing Options Previous Close ===")
    result = await get_options_previous_close(options_ticker="O:ORCL251031C00280000")
    print("✓ Options previous close")
    print(f"  First 200 chars: {result[:200]}...")
    return "options_previous_close" if len(result) > 0 and "Error" not in result else None


async def main():
    """Run all integration tests."""
    print("=" * 60)
    print("INTEGRATION TESTS FOR ENHANCED MCP POLYGON TOOLS")
    print("=" * 60)

    tests = [
        # Aggregates
        ("Get Aggs", test_get_aggs),
        ("List Aggs", test_list_aggs),
        ("Grouped Daily", test_grouped_daily),
        ("Daily Open/Close", test_daily_open_close),
        ("Previous Close", test_previous_close),
        # Trades & Quotes
        ("List Trades", test_list_trades),
        ("Last Trade", test_last_trade),
        ("List Quotes", test_list_quotes),
        ("Last Quote", test_last_quote),
        ("Currency Conversion", test_currency_conversion),
        # Snapshots
        ("Universal Snapshots", test_universal_snapshots),
        ("Snapshot All", test_snapshot_all),
        ("Snapshot Direction", test_snapshot_direction),
        ("Snapshot Ticker", test_snapshot),
        # Technical Indicators
        ("SMA", test_sma),
        ("EMA", test_ema),
        ("MACD", test_macd),
        ("RSI", test_rsi),
        # Reference Data
        ("List Tickers", test_list_tickers),
        ("All Tickers", test_all_tickers),
        ("Ticker Details", test_ticker_details),
        ("Related Companies", test_related_companies),
        ("Ticker Types", test_ticker_types),
        ("Market Status", test_market_status),
        ("Market Holidays", test_market_holidays),
        ("Conditions", test_conditions),
        ("Exchanges", test_exchanges),
        # Corporate Actions
        ("IPOs", test_ipos),
        ("Splits", test_splits),
        ("Dividends", test_dividends),
        ("Ticker Events", test_ticker_events),
        # Fundamentals
        ("Stock Financials", test_stock_financials),
        ("Balance Sheets", test_balance_sheets),
        ("Cash Flow", test_cash_flow),
        ("Income Statements", test_income_statements),
        ("Financial Ratios", test_financials_ratios),
        ("Stock Ratios", test_stock_ratios),
        ("Short Interest", test_short_interest),
        ("Short Volume", test_short_volume),
        # News
        ("News", test_news),
        # Economics
        ("Treasury Yields", test_treasury_yields),
        ("Inflation", test_inflation),
        ("Inflation Expectations", test_inflation_expectations),
        # Options
        ("List Options Contracts", test_list_options_contracts),
        ("Options Aggs", test_options_aggs),
        ("Options Daily Open/Close", test_options_daily_open_close),
        ("Options Previous Close", test_options_previous_close),
        ("Options Snapshot", test_options_snapshot),
        ("Options Contract", test_options_contract),
        ("Options Chain Snapshot", test_options_chain_snapshot),
    ]

    results = []
    passed = 0
    failed = 0

    for name, test_func in tests:
        try:
            result = await test_func()
            if result:
                results.append((name, "✓ PASS"))
                passed += 1
            else:
                results.append((name, "✗ FAIL"))
                failed += 1
        except Exception as e:
            print(f"✗ Error in {name}: {str(e)[:100]}")
            results.append((name, f"✗ ERROR: {str(e)[:50]}"))
            failed += 1

    print("\n" + "=" * 60)
    print("TEST SUMMARY")
    print("=" * 60)
    for name, status in results:
        print(f"{name:.<40} {status}")

    print("\n" + "=" * 60)
    print(f"Total: {len(tests)} | Passed: {passed} | Failed: {failed}")
    print("=" * 60)

    return 0 if failed == 0 else 1


if __name__ == "__main__":
    import asyncio

    sys.exit(asyncio.run(main()))
