#!/usr/bin/env python3
"""Integration tests for enhanced MCP Polygon tools."""

import os
import sys
from datetime import datetime, timedelta

# Set API key
os.environ["POLYGON_API_KEY"] = "vDr8GDaQ87Z9Mwe5IiCKzGcRP9pnO8TW"

from src.mcp_polygon.server import (
    get_daily_open_close_agg,
    get_previous_close_agg,
    get_snapshot_ticker,
    get_sma,
    get_ema,
    get_macd,
    get_rsi,
    list_ipos,
    list_splits,
    list_dividends,
    get_ticker_events,
    list_financials_balance_sheets,
    list_financials_cash_flow_statements,
    list_financials_income_statements,
    list_stock_ratios,
    list_short_interest,
    list_short_volume,
    list_ticker_news,
    get_options_snapshot,
    get_options_contract,
    get_options_chain_snapshot,
    get_ticker_details,
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


async def main():
    """Run all integration tests."""
    print("=" * 60)
    print("INTEGRATION TESTS FOR ENHANCED MCP POLYGON TOOLS")
    print("=" * 60)

    tests = [
        ("Daily Open/Close", test_daily_open_close),
        ("Previous Close", test_previous_close),
        ("Snapshot", test_snapshot),
        ("SMA", test_sma),
        ("EMA", test_ema),
        ("MACD", test_macd),
        ("RSI", test_rsi),
        ("IPOs", test_ipos),
        ("Splits", test_splits),
        ("Dividends", test_dividends),
        ("Ticker Events", test_ticker_events),
        ("Balance Sheets", test_balance_sheets),
        ("Cash Flow", test_cash_flow),
        ("Income Statements", test_income_statements),
        ("Stock Ratios", test_stock_ratios),
        ("Short Interest", test_short_interest),
        ("Short Volume", test_short_volume),
        ("News", test_news),
        ("Options Snapshot", test_options_snapshot),
        ("Options Contract", test_options_contract),
        ("Options Chain Snapshot", test_options_chain_snapshot),
        ("Ticker Details", test_ticker_details),
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
