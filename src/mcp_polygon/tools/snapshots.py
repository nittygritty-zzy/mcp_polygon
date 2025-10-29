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
    Retrieve unified snapshots of market data for multiple asset classes including stocks, options,
    forex, and cryptocurrencies in a single request. This endpoint consolidates key metrics such as
    last trade, last quote, open, high, low, close, and volume for a comprehensive view of current
    market conditions. By aggregating data from various sources into one response, users can efficiently
    monitor, compare, and act on information spanning multiple markets and asset types.

    Reference: https://polygon.io/docs/rest/stocks/snapshots/unified-snapshot

    Parameters:
    - type: Query by asset type ("stocks", "options", "forex", "crypto", "indices")
    - ticker: Search a range of tickers lexicographically
    - ticker_any_of: Comma-separated list of specific tickers (up to 250, e.g., ["AAPL", "TSLA", "GOOG"])
                     If no tickers are passed, all results will be returned in a paginated manner
    - ticker_gte: Range by ticker - greater than or equal to (lexicographic)
    - ticker_gt: Range by ticker - greater than (lexicographic)
    - ticker_lte: Range by ticker - less than or equal to (lexicographic)
    - ticker_lt: Range by ticker - less than (lexicographic)
    - order: Order results based on the sort field ("asc" or "desc")
    - limit: Limit the number of results returned (default: 10, max: 250)
    - sort: Sort field used for ordering (e.g., "ticker")

    Response includes (varies by asset type):
    Common fields for all asset types:
    - ticker: The ticker symbol for the asset
    - type: The asset class (stocks, options, fx, crypto, indices)
    - market_status: Market status (open, closed, early_trading, late_trading, regular_trading)
    - name: The name of the asset
    - last_trade: Most recent trade (price, size, conditions, exchange, timestamp, timeframe)
    - last_quote: Most recent quote (bid, ask, bid_size, ask_size, exchanges, midpoint, timeframe)
    - last_updated: Nanosecond timestamp of when this information was updated
    - timeframe: Time relevance of the data (DELAYED or REAL-TIME)

    For stocks:
    - last_minute: Most recent minute aggregate (close, high, low, open, transactions, volume, vwap)
    - session: Comprehensive trading session metrics with early/regular/late trading changes

    For options:
    - break_even_price: Price for contract to break even (call: strike + premium, put: strike - premium)
    - details: Contract specifications (type, style, expiration, strike, shares per contract, underlying)
    - greeks: Delta, gamma, theta, vega (not returned for deep in-the-money contracts)
    - implied_volatility: Market's forecast for underlying volatility based on option price
    - open_interest: Quantity of contracts held at end of last trading day
    - underlying_asset: Information on the underlying stock (price, ticker, change_to_break_even)
    - fmv: Fair Market Value (Business plans only - proprietary real-time algorithm)
    - fmv_last_updated: Nanosecond timestamp of last FMV calculation

    For indices:
    - value: Current value of the index

    Error handling:
    - error: Error code if ticker lookup failed (e.g., "NOT_FOUND")
    - message: Error message describing the issue (e.g., "Ticker not found.")

    Use Cases:
    - Cross-market analysis: Compare performance across stocks, options, forex, and crypto
    - Diversified portfolio monitoring: Track all positions regardless of asset class
    - Global market insights: Monitor international markets and multiple asset types
    - Multi-asset trading strategies: Build strategies that span different market types

    Example: list_universal_snapshots(type="stocks", ticker_any_of=["AAPL", "MSFT", "GOOGL"])
             retrieves snapshots for Apple, Microsoft, and Google stocks with comprehensive
             market data including last trade, quote, minute bar, and session metrics

    Example: list_universal_snapshots(ticker_any_of=["AAPL", "O:NCLH221014C00005000", "X:BTCUSD"])
             retrieves mixed asset types: Apple stock, NCLH options contract, and Bitcoin/USD
             forex pair - all in a single response

    Example: list_universal_snapshots(type="options", ticker_gte="O:AAPL", ticker_lt="O:AAPLZ", limit=100)
             retrieves up to 100 Apple options contracts using lexicographic range, including
             greeks, break-even prices, and underlying asset data

    Example: list_universal_snapshots(type="crypto", limit=50, sort="ticker", order="asc")
             retrieves top 50 cryptocurrency snapshots sorted alphabetically by ticker

    Note: This endpoint is ideal for applications requiring cross-market monitoring. Individual
          ticker errors don't fail the entire request - they're returned with error/message fields.
          Up to 250 tickers can be queried via ticker_any_of. Response structure varies by asset
          type. For single ticker snapshots, use get_snapshot_ticker. For full market coverage of
          a single asset type, use get_snapshot_all.
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
    Retrieve a comprehensive snapshot of the entire U.S. stock market, covering 10,000+ actively
    traded tickers in a single response. This endpoint consolidates key information like pricing,
    volume, and trade activity to provide a full-market view, eliminating the need for multiple
    queries. By accessing all tickers at once, users can efficiently monitor broad market conditions,
    perform bulk analyses, and power applications that require complete, current market information.

    Reference: https://polygon.io/docs/rest/stocks/snapshots/full-market-snapshot

    Parameters:
    - market_type: The market type (stocks, crypto, fx, otc, indices)
    - tickers: Optional case-sensitive list of specific tickers to filter (e.g., ["AAPL", "TSLA", "GOOG"])
               If not provided, returns all tickers in the market
    - include_otc: Include OTC (over-the-counter) securities in the response (default: False)

    Response includes (for each ticker):
    - ticker: The ticker symbol
    - day: Today's aggregate (open, high, low, close, volume, vwap)
    - min: Latest minute bar aggregate with similar metrics
    - prevDay: Previous trading day's aggregate data
    - lastTrade: Most recent trade (price, size, exchange, conditions, timestamp)
    - lastQuote: Current bid/ask spread (bid price, bid size, ask price, ask size)
    - todaysChange: Absolute price change from previous close
    - todaysChangePerc: Percentage change from previous close
    - updated: Timestamp of last update

    Data Timing:
    - Snapshot data is cleared at 3:30 AM EST daily
    - Updates begin as exchanges report new data (as early as 4:00 AM EST)
    - Provides real-time data during market hours

    Use Cases:
    - Market overview: Get a bird's-eye view of the entire market at once
    - Bulk data processing: Analyze thousands of securities simultaneously
    - Heat maps/dashboards: Power visualizations showing market-wide trends
    - Automated monitoring: Build systems that track all market movements

    Example: get_snapshot_all("stocks") returns complete snapshots for all 10,000+ stock tickers
             in the U.S. market with full OHLC, volume, and trade data for each

    Example: get_snapshot_all("stocks", tickers=["AAPL", "MSFT", "GOOGL"]) returns snapshots
             for just Apple, Microsoft, and Google

    Example: get_snapshot_all("stocks", include_otc=True) returns all stocks including
             over-the-counter securities

    Note: This can return a very large dataset when querying all tickers (10,000+ stocks).
          Consider using the tickers parameter to filter to specific symbols when you don't
          need the full market. For a single ticker, use get_snapshot_ticker instead. For
          more advanced filtering, use list_universal_snapshots.
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
    Retrieve snapshot data highlighting the top 20 gainers or losers in the U.S. stock market.
    Gainers are stocks with the largest percentage increase since the previous day's close, and
    losers are those with the largest percentage decrease. To ensure meaningful insights, only
    tickers with a minimum trading volume of 10,000 are included. By focusing on these market
    movers, users can quickly identify significant price shifts and monitor evolving market dynamics.

    Reference: https://polygon.io/docs/rest/stocks/snapshots/top-market-movers

    Parameters:
    - market_type: The market type (stocks, crypto, fx, otc, indices)
    - direction: The direction of snapshot results to return ("gainers" or "losers")
    - include_otc: Include OTC (over-the-counter) securities in the response (default: False)

    Response includes (for each ticker in top 20):
    - ticker: The ticker symbol
    - day: Today's aggregate (open, high, low, close, volume, vwap)
    - min: Latest minute bar aggregate with similar metrics
    - prevDay: Previous trading day's aggregate data
    - lastTrade: Most recent trade (price, size, exchange, conditions, timestamp)
    - lastQuote: Current bid/ask spread (bid price, bid size, ask price, ask size)
    - todaysChange: Absolute price change from previous close
    - todaysChangePerc: Percentage change from previous close
    - updated: Timestamp of last update

    Data Timing:
    - Snapshot data is cleared at 3:30 AM EST daily
    - Updates begin as exchanges report new information (around 4:00 AM EST)
    - Provides real-time data during market hours

    Filtering:
    - Only includes tickers with minimum trading volume of 10,000
    - Returns top 20 movers by percentage change
    - Sorted by percentage change (descending for gainers, ascending for losers)

    Use Cases:
    - Market movers identification: Quickly spot stocks with significant price movements
    - Trading strategies: Identify momentum plays and reversal opportunities
    - Market sentiment analysis: Gauge overall market direction and strength
    - Portfolio adjustments: React to major price shifts in holdings or watchlist

    Example: get_snapshot_direction("stocks", "gainers") returns top 20 stocks with the
             largest percentage increase since previous close, filtered to stocks with
             10,000+ trading volume

    Example: get_snapshot_direction("stocks", "losers") returns top 20 stocks with the
             largest percentage decrease since previous close

    Example: get_snapshot_direction("stocks", "gainers", include_otc=True) returns top 20
             gainers including over-the-counter securities

    Note: This endpoint focuses on the most significant market movements. The volume filter
          (10,000 minimum) ensures results represent liquid, actively traded stocks rather
          than low-volume penny stocks with volatile percentage swings. For broader market
          coverage, use get_snapshot_all. For specific tickers, use get_snapshot_ticker.
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
    Retrieve the most recent market data snapshot for a single stock ticker. This endpoint consolidates
    the latest trade, quote, and aggregated data (minute bar, today's day bar, and previous day)
    for the specified ticker. By focusing on a single ticker, users can closely monitor real-time
    developments and incorporate up-to-date information into trading strategies, alerts, or
    company-level reporting.

    Reference: https://polygon.io/docs/rest/stocks/snapshots/single-ticker-snapshot

    API Endpoint: GET /v2/snapshot/locale/us/markets/stocks/tickers/{stocksTicker}

    Parameters:
    - ticker: Case-sensitive stock ticker symbol (e.g., "AAPL" for Apple Inc., "MSFT" for Microsoft)
    - market_type: Market type (default: "stocks") - This endpoint is specific to stocks

    Response includes:
    - day: Today's aggregate (open, high, low, close, volume, vwap)
    - min: Latest minute bar aggregate with similar metrics
    - prevDay: Previous trading day's aggregate data
    - lastTrade: Most recent trade (price, size, exchange, conditions, timestamp)
    - lastQuote: Current bid/ask spread (bid price, bid size, ask price, ask size)
    - todaysChange: Absolute price change from previous close
    - todaysChangePerc: Percentage change from previous close
    - updated: Timestamp of last update

    Data Timing:
    - Snapshot data is cleared at 3:30 AM EST daily
    - Updates begin as exchanges report new information (as early as 4:00 AM EST)
    - Provides real-time data during market hours

    Use Cases:
    - Focused monitoring: Track specific stocks intensively during trading hours
    - Real-time analysis: Make decisions based on latest trade and quote data
    - Price alerts: Trigger notifications when targets are reached
    - Investor relations: Provide stakeholders with current company stock status

    Data Recency by Plan:
    - Stocks Basic: Limited access, not included
    - Stocks Starter/Developer: 15-minute delayed data
    - Stocks Advanced/Business: Real-time data

    Example: get_snapshot_ticker("AAPL") returns Apple's complete market snapshot
             including current day OHLC, last trade, current quote, and previous day data

    Example: get_snapshot_ticker("TSLA") returns Tesla's real-time snapshot with
             all consolidated market data

    Example: get_snapshot_ticker("MSFT") returns Microsoft's current snapshot with
             today's change percentage and absolute price change

    Note: This provides consolidated real-time market data for a single stock ticker.
          Snapshot data clears daily at 3:30 AM EST and updates begin around 4:00 AM EST.
          For multiple tickers, use list_universal_snapshots or get_snapshot_all instead.
          For historical data, use aggregate endpoints like get_aggs or get_daily_open_close_agg.
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
    Get snapshot for a specific option contract.
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
    Get snapshot for a crypto ticker's order book.
    """
    try:
        results = polygon_client.get_snapshot_crypto_book(
            ticker=ticker, params=params, raw=True
        )

        return json_to_csv(results.data.decode("utf-8"))
    except Exception as e:
        return f"Error: {e}"
