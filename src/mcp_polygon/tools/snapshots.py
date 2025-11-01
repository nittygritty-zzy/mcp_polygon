"""Auto-generated tool definitions."""

from typing import Optional, Any, Dict, List
from mcp.types import ToolAnnotations
from ..clients import poly_mcp, polygon_client
from ..formatters import json_to_csv
from ..tool_integration import process_tool_response, create_batch_writer
from ..parallel_fetcher import PolygonParallelFetcher
from ..utils import build_params


@poly_mcp.tool(annotations=ToolAnnotations(readOnlyHint=True))
async def list_universal_snapshots(
    type: Optional[str] = None,
    ticker: Optional[str] = None,
    ticker_any_of: Optional[List[str]] = None,
    ticker_gte: Optional[str] = None,
    ticker_gt: Optional[str] = None,
    ticker_lte: Optional[str] = None,
    ticker_lt: Optional[str] = None,
    order: Optional[str] = None,
    limit: Optional[int] = 10,
    fetch_all: Optional[bool] = True,
    sort: Optional[str] = None,
    params: Optional[Dict[str, Any]] = None,
) -> str:
    """
    Get unified snapshots for multiple asset classes (stocks, options, forex, crypto) in one request.

    Reference: https://polygon.io/docs/rest/stocks/snapshots/unified-snapshot

    Parameters:
    - type: Asset type ("stocks", "options", "forex", "crypto", "indices")
    - ticker_any_of: List of specific tickers (up to 250, e.g., ["AAPL", "TSLA"])
    - ticker_gte/lt: Ticker range filters
    - limit: Number of results per page (default: 10, max: 250)
    - fetch_all: If True (recommended), fetch ALL data and cache to disk for DuckDB queries (default: True)

    RECOMMENDED: Always use fetch_all=True to cache complete snapshot data locally for efficient DuckDB analysis.

    Example: list_universal_snapshots(type="stocks", ticker_any_of=["AAPL", "MSFT", "GOOGL"], fetch_all=True)
    Example: list_universal_snapshots(ticker_any_of=["AAPL", "O:NCLH221014C00005000", "X:BTCUSD"], fetch_all=True)

    Returns: ticker, type, market_status, last_trade, last_quote. Stocks include session data, options include greeks/IV.
    """
    try:
        tool_params = build_params(
            type=type,
            ticker_any_of=ticker_any_of,
            limit=limit,
            fetch_all=fetch_all,
        )

        param_dict = {
            **(params or {}),
            **{
                k: v
                for k, v in {
                    "ticker": ticker,
                    "ticker.gte": ticker_gte,
                    "ticker.gt": ticker_gt,
                    "ticker.lte": ticker_lte,
                    "ticker.lt": ticker_lt,
                }.items()
                if v is not None
            },
        }

        if fetch_all:
            # Use batch writing for memory efficiency
            batch_callback, finalize = create_batch_writer(
                "list_universal_snapshots", tool_params
            )

            if batch_callback:
                # Streaming mode - write batches to disk incrementally
                fetcher = PolygonParallelFetcher(polygon_client, num_workers=5)
                await fetcher.fetch_all(
                    method_name="list_universal_snapshots",
                    batch_callback=batch_callback,
                    type=type,
                    ticker_any_of=ticker_any_of,
                    order=order,
                    limit=limit,
                    sort=sort,
                    params=param_dict,
                )
                # Finalize and return cache metadata
                return await finalize()
            else:
                # Memory mode (fallback if batch writing not available)
                fetcher = PolygonParallelFetcher(polygon_client, num_workers=5)
                snapshots_list = await fetcher.fetch_all(
                    method_name="list_universal_snapshots",
                    type=type,
                    ticker_any_of=ticker_any_of,
                    order=order,
                    limit=limit,
                    sort=sort,
                    params=param_dict,
                )
                csv_data = json_to_csv({"results": snapshots_list})
                return await process_tool_response(
                    "list_universal_snapshots", tool_params, csv_data
                )
        else:
            # Single page approach
            results = polygon_client.list_universal_snapshots(
                type=type,
                ticker_any_of=ticker_any_of,
                order=order,
                limit=limit,
                sort=sort,
                params=param_dict,
                raw=True,
            )

            import json

            data = json.loads(results.data.decode("utf-8"))
            snapshots_list = data.get("results", [])

            # Create data structure for JSON to CSV conversion
            data = {"results": snapshots_list, "status": "OK"}

            # Convert to CSV
            csv_data = json_to_csv(data)

            # Process with intelligent caching
            return await process_tool_response(
                "list_universal_snapshots", tool_params, csv_data
            )
    except Exception as e:
        return f"Error: {e}"


@poly_mcp.tool(annotations=ToolAnnotations(readOnlyHint=True))
async def get_snapshot_all(
    market_type: str,
    tickers: Optional[List[str]] = None,
    include_otc: Optional[bool] = None,
    params: Optional[Dict[str, Any]] = None,
) -> str:
    """
    Get full market snapshot for 10,000+ tickers in a single response. Auto-cached to disk for DuckDB queries.

    Reference: https://polygon.io/docs/rest/stocks/snapshots/full-market-snapshot

    Parameters:
    - market_type: Market type ("stocks", "crypto", "fx", "otc", "indices")
    - tickers: Optional list to filter specific tickers (e.g., ["AAPL", "TSLA"])
    - include_otc: Include OTC securities (default: False)

    RECOMMENDED: Use this tool for full market analysis - data is automatically cached locally for efficient DuckDB queries.

    Example: get_snapshot_all("stocks")
    Example: get_snapshot_all("stocks", tickers=["AAPL", "MSFT", "GOOGL"])

    Returns: ticker, day (OHLC), min, prevDay, lastTrade, lastQuote, todaysChange, todaysChangePerc. Large dataset.
    """
    try:
        results = polygon_client.get_snapshot_all(
            market_type=market_type,
            tickers=tickers,
            include_otc=include_otc,
            params=params,
            raw=True,
        )

        csv_data = json_to_csv(results.data.decode("utf-8"))

        # Process with intelligent caching - this is a large dataset
        return await process_tool_response(
            tool_name="get_snapshot_all",
            params={
                "market_type": market_type,
                "tickers": tickers,
                "include_otc": include_otc,
            },
            csv_data=csv_data,
        )
    except Exception as e:
        return f"Error: {e}"


@poly_mcp.tool(annotations=ToolAnnotations(readOnlyHint=True))
async def get_snapshot_direction(
    market_type: str,
    direction: str,
    include_otc: Optional[bool] = None,
    params: Optional[Dict[str, Any]] = None,
) -> str:
    """
    Get top 20 market gainers or losers (minimum 10,000 volume).

    Reference: https://polygon.io/docs/rest/stocks/snapshots/top-market-movers

    Parameters:
    - market_type: Market type ("stocks", "crypto", "fx", "otc", "indices")
    - direction: Direction ("gainers" or "losers")
    - include_otc: Include OTC securities (default: False)

    Example: get_snapshot_direction("stocks", "gainers")
    Example: get_snapshot_direction("stocks", "losers")

    Returns: ticker, day, min, prevDay, lastTrade, lastQuote, todaysChange, todaysChangePerc. Top 20 by % change.
    """
    try:
        results = polygon_client.get_snapshot_direction(
            market_type=market_type,
            direction=direction,
            include_otc=include_otc,
            params=params,
            raw=True,
        )

        return json_to_csv(results.data.decode("utf-8"))
    except Exception as e:
        return f"Error: {e}"


@poly_mcp.tool(annotations=ToolAnnotations(readOnlyHint=True))
async def get_snapshot_ticker(
    ticker: str,
    market_type: str = "stocks",
    params: Optional[Dict[str, Any]] = None,
) -> str:
    """
    Get market snapshot for a single ticker with latest trade, quote, and aggregate data.

    Reference: https://polygon.io/docs/rest/stocks/snapshots/single-ticker-snapshot

    Parameters:
    - ticker: Stock symbol (e.g., "AAPL", "MSFT")
    - market_type: Market type (default: "stocks")

    Example: get_snapshot_ticker("AAPL")
    Example: get_snapshot_ticker("TSLA")

    Returns: day (OHLC), min, prevDay, lastTrade, lastQuote, todaysChange, todaysChangePerc. Real-time or delayed.
    """
    try:
        results = polygon_client.get_snapshot_ticker(
            market_type=market_type, ticker=ticker, params=params, raw=True
        )

        return json_to_csv(results.data.decode("utf-8"))
    except Exception as e:
        return f"Error: {e}"


@poly_mcp.tool(annotations=ToolAnnotations(readOnlyHint=True))
async def get_snapshot_option(
    underlying_asset: str,
    option_contract: str,
    params: Optional[Dict[str, Any]] = None,
) -> str:
    """
    Get snapshot for an option contract with greeks, IV, and pricing data.

    Parameters:
    - underlying_asset: Stock ticker (e.g., "AAPL")
    - option_contract: Contract ticker (e.g., "O:AAPL251219C00150000")

    Example: get_snapshot_option("AAPL", "O:AAPL251219C00150000")

    Returns: break_even, greeks, implied_volatility, last_trade, last_quote, open_interest, underlying_asset.
    """
    try:
        results = polygon_client.get_snapshot_option(
            underlying_asset=underlying_asset,
            option_contract=option_contract,
            params=params,
            raw=True,
        )

        # Parse the response and extract the results object
        import json
        import traceback

        data = json.loads(results.data.decode("utf-8"))
        if "results" in data:
            # Wrap the results object in an array for CSV formatting
            formatted_data = {"results": [data["results"]]}
            return json_to_csv(formatted_data)
        return json_to_csv(results.data.decode("utf-8"))
    except Exception as e:
        import traceback

        return f"Error: {e}\nTraceback: {traceback.format_exc()}"


@poly_mcp.tool(annotations=ToolAnnotations(readOnlyHint=True))
async def get_snapshot_crypto_book(
    ticker: str,
    params: Optional[Dict[str, Any]] = None,
) -> str:
    """
    Get order book snapshot for a cryptocurrency ticker.

    Parameters:
    - ticker: Crypto ticker (e.g., "X:BTCUSD")

    Example: get_snapshot_crypto_book("X:BTCUSD")

    Returns: Order book with bids and asks at various price levels.
    """
    try:
        results = polygon_client.get_snapshot_crypto_book(
            ticker=ticker, params=params, raw=True
        )

        return json_to_csv(results.data.decode("utf-8"))
    except Exception as e:
        return f"Error: {e}"


@poly_mcp.tool(annotations=ToolAnnotations(readOnlyHint=True))
async def get_snapshot_indices(
    ticker_any_of: Optional[List[str]] = None,
    params: Optional[Dict[str, Any]] = None,
) -> str:
    """
    Get current values for major market indices (S&P 500, NASDAQ, Dow Jones, etc.).

    Reference: https://polygon.io/docs/stocks/get_v3_snapshot_indices

    Parameters:
    - ticker_any_of: List of index tickers (e.g., ["I:SPX", "I:NDX", "I:DJI"])
    - params: Additional query parameters

    Common Index Tickers:
    - I:SPX - S&P 500
    - I:NDX - NASDAQ 100
    - I:DJI - Dow Jones Industrial Average
    - I:RUT - Russell 2000
    - I:VIX - CBOE Volatility Index

    Example: get_snapshot_indices(ticker_any_of=["I:SPX", "I:NDX", "I:DJI"])
    Example: get_snapshot_indices()  # All available indices

    Returns: ticker, value, session (open, high, low, close), previous_session. Real-time or delayed index values.
    """
    try:
        # Convert single string to list if needed
        if isinstance(ticker_any_of, str):
            ticker_any_of = [ticker_any_of]

        results = polygon_client.get_snapshot_indices(
            ticker_any_of=ticker_any_of,
            params=params,
            raw=True,
        )

        return json_to_csv(results.data.decode("utf-8"))
    except Exception as e:
        return f"Error: {e}"


@poly_mcp.tool(annotations=ToolAnnotations(readOnlyHint=True))
async def get_summaries(
    ticker_any_of: Optional[List[str]] = None,
    params: Optional[Dict[str, Any]] = None,
) -> str:
    """
    Get comprehensive market summaries with OHLC, volume, VWAP, and intraday data for multiple tickers.

    Reference: https://polygon.io/docs/stocks/get_v1_summaries

    Parameters:
    - ticker_any_of: List of tickers (e.g., ["AAPL", "MSFT", "GOOGL"])
    - params: Additional query parameters

    Example: get_summaries(ticker_any_of=["AAPL", "MSFT", "TSLA"])
    Example: get_summaries()  # All available tickers (large dataset)

    Returns: ticker, session (OHLC, volume, VWAP), previous_session, price (today's change/%), type, market_status.

    Note: Similar to snapshots but with more detailed session breakdown and aggregate data.
    """
    try:
        # Convert single string to list if needed
        if isinstance(ticker_any_of, str):
            ticker_any_of = [ticker_any_of]

        results = polygon_client.get_summaries(
            ticker_any_of=ticker_any_of,
            params=params,
            raw=True,
        )

        return json_to_csv(results.data.decode("utf-8"))
    except Exception as e:
        return f"Error: {e}"
