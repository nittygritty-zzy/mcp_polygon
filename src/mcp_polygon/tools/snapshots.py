"""Auto-generated tool definitions."""

from typing import Optional, Any, Dict, Union, List
from mcp.types import ToolAnnotations
from datetime import datetime, date
from ..clients import poly_mcp, polygon_client
from ..formatters import json_to_csv


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
    - limit: Number of results (default: 10, max: 250)

    Example: list_universal_snapshots(type="stocks", ticker_any_of=["AAPL", "MSFT", "GOOGL"])
    Example: list_universal_snapshots(ticker_any_of=["AAPL", "O:NCLH221014C00005000", "X:BTCUSD"])

    Returns: ticker, type, market_status, last_trade, last_quote. Stocks include session data, options include greeks/IV.
    """
    try:
        results = polygon_client.list_universal_snapshots(
            type=type,
            ticker_any_of=ticker_any_of,
            order=order,
            limit=limit,
            sort=sort,
            params={
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
            },
            raw=True,
        )

        return json_to_csv(results.data.decode("utf-8"))
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
    Get full market snapshot for 10,000+ tickers in a single response.

    Reference: https://polygon.io/docs/rest/stocks/snapshots/full-market-snapshot

    Parameters:
    - market_type: Market type ("stocks", "crypto", "fx", "otc", "indices")
    - tickers: Optional list to filter specific tickers (e.g., ["AAPL", "TSLA"])
    - include_otc: Include OTC securities (default: False)

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

        return json_to_csv(results.data.decode("utf-8"))
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
