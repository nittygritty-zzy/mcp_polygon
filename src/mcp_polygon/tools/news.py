"""Auto-generated tool definitions."""

from typing import Optional, Any, Dict, Union, List
from mcp.types import ToolAnnotations
from datetime import datetime, date
from ..clients import poly_mcp, polygon_client
from ..formatters import json_to_csv


@poly_mcp.tool(annotations=ToolAnnotations(readOnlyHint=True))
async def get_aggs(
    ticker: str,
    multiplier: int,
    timespan: str,
    from_: Union[str, int, datetime, date],
    to: Union[str, int, datetime, date],
    adjusted: Optional[bool] = None,
    sort: Optional[str] = None,
    limit: Optional[int] = 10,
    params: Optional[Dict[str, Any]] = None,
) -> str:
    """
    Retrieve aggregated historical OHLC (Open, High, Low, Close) and volume data for a specified
    stock ticker over a custom date range and time interval in Eastern Time (ET). Aggregates are
    constructed exclusively from qualifying trades that meet specific conditions.

    Reference: https://polygon.io/docs/rest/stocks/aggregates/custom-bars

    Essential for data visualization, technical analysis, backtesting strategies, and market research.
    Supports pre-market, regular market, and after-hours sessions with flexible time intervals.

    Parameters:
    - ticker: Case-sensitive ticker symbol (e.g., "AAPL", "MSFT", "GOOGL")
    - multiplier: Size of the timespan multiplier (e.g., 1, 5, 15, 30)
    - timespan: Size of the time window (minute, hour, day, week, month, quarter, year)
    - from_: Start of aggregate window (YYYY-MM-DD or millisecond timestamp)
    - to: End of aggregate window (YYYY-MM-DD or millisecond timestamp)
    - adjusted: Whether to adjust for splits (default: True)
    - sort: Sort order - "asc" (oldest first) or "desc" (newest first)
    - limit: Number of base aggregates queried (default: 10, max: 50000)
    - params: Additional query parameters

    Example: get_aggs("AAPL", 1, "day", "2023-01-01", "2023-01-31")
             gets daily bars for Apple in January 2023
    Example: get_aggs("MSFT", 5, "minute", "2023-06-15", "2023-06-15", limit=100)
             gets 5-minute bars for Microsoft on June 15, 2023
    Example: get_aggs("GOOGL", 1, "hour", "2023-03-01", "2023-03-31")
             gets hourly bars for Google in March 2023
    Example: get_aggs("TSLA", 15, "minute", "2023-09-20 09:30:00", "2023-09-20 16:00:00")
             gets 15-minute bars for Tesla during regular market hours
    Example: get_aggs("SPY", 1, "week", "2022-01-01", "2022-12-31")
             gets weekly bars for SPY for entire year 2022

    Response includes:
    - c: Close price
    - h: High price
    - l: Low price
    - o: Open price
    - v: Trading volume
    - vw: Volume weighted average price (VWAP)
    - t: Timestamp (milliseconds since epoch)
    - n: Number of transactions

    Note: Custom bars considerations:
    - All times in Eastern Time (ET)
    - Aggregates built only from qualifying trades
    - Empty intervals indicate no trading activity (no bars produced)
    - Covers pre-market (4:00 AM - 9:30 AM ET), regular (9:30 AM - 4:00 PM ET), and after-hours (4:00 PM - 8:00 PM ET)
    - Multiplier × timespan = bar size (e.g., 5 × minute = 5-minute bars)
    - Default limit is 10, increase for longer time ranges
    - Max limit is 50000 base aggregates
    - Use pagination (next_url) for very large datasets
    - Adjusted=true accounts for stock splits in historical data
    - VWAP provides average price weighted by volume
    - Transaction count (n) indicates trading activity level
    - Sort asc for chronological order, desc for reverse chronological

    Use case: Building a stock charting application - fetch 5-minute bars for intraday analysis,
    daily bars for swing trading, or weekly bars for long-term trend analysis. Combine with
    technical indicators to identify entry/exit points and backtest trading strategies.
    """
    try:
        results = polygon_client.get_aggs(
            ticker=ticker,
            multiplier=multiplier,
            timespan=timespan,
            from_=from_,
            to=to,
            adjusted=adjusted,
            sort=sort,
            limit=limit,
            params=params,
            raw=True,
        )

        # Parse the binary data to string and then to JSON
        return json_to_csv(results.data.decode("utf-8"))
    except Exception as e:
        return f"Error: {e}"


@poly_mcp.tool(annotations=ToolAnnotations(readOnlyHint=True))
async def list_aggs(
    ticker: str,
    multiplier: int,
    timespan: str,
    from_: Union[str, int, datetime, date],
    to: Union[str, int, datetime, date],
    adjusted: Optional[bool] = None,
    sort: Optional[str] = None,
    limit: Optional[int] = 10,
    params: Optional[Dict[str, Any]] = None,
) -> str:
    """
    Iterate through aggregate bars (OHLC) for a ticker over a given date range.
    Similar to get_aggs but designed for pagination through large result sets.

    Reference: https://polygon.io/docs/rest/stocks/aggregates/custom-bars

    Parameters:
    - ticker: The ticker symbol (e.g., "AAPL", "MSFT")
    - multiplier: Size of the timespan multiplier (e.g., 1 for 1 day, 5 for 5 minutes)
    - timespan: Size of the time window (minute, hour, day, week, month, quarter, year)
    - from_: Start date (YYYY-MM-DD) or timestamp
    - to: End date (YYYY-MM-DD) or timestamp
    - adjusted: Whether to adjust for splits (default: True)
    - sort: Sort order (asc or desc)
    - limit: Number of results to return (default: 10)
    """
    try:
        results = polygon_client.list_aggs(
            ticker=ticker,
            multiplier=multiplier,
            timespan=timespan,
            from_=from_,
            to=to,
            adjusted=adjusted,
            sort=sort,
            limit=limit,
            params=params,
            raw=True,
        )

        return json_to_csv(results.data.decode("utf-8"))
    except Exception as e:
        return f"Error: {e}"


@poly_mcp.tool(annotations=ToolAnnotations(readOnlyHint=True))
async def get_grouped_daily_aggs(
    date: str,
    adjusted: Optional[bool] = None,
    include_otc: Optional[bool] = None,
    locale: Optional[str] = None,
    market_type: Optional[str] = None,
    params: Optional[Dict[str, Any]] = None,
) -> str:
    """
    Retrieve daily OHLC (open, high, low, close), volume, and volume-weighted average price (VWAP)
    data for all U.S. stocks on a specified trading date. This endpoint returns comprehensive market
    coverage in a single request, enabling wide-scale analysis, bulk data processing, and research
    into broad market performance.

    Reference: https://polygon.io/docs/rest/stocks/aggregates/daily-market-summary

    Essential for market overview, bulk data processing, historical research, and portfolio
    comparison. Returns data for thousands of stocks in one request.

    Parameters:
    - date: Trading date for the aggregate window (YYYY-MM-DD, e.g., "2023-01-09")
    - adjusted: Whether to adjust for splits (default: True)
    - include_otc: Include OTC securities in response (default: False)
    - locale: Filter by locale (us, global)
    - market_type: Filter by market type (stocks, crypto, fx, otc, indices)
    - params: Additional query parameters

    Example: get_grouped_daily_aggs("2023-01-09")
             gets daily bars for all US stocks on January 9, 2023
    Example: get_grouped_daily_aggs("2023-06-15", include_otc=True)
             gets daily bars for all stocks including OTC securities
    Example: get_grouped_daily_aggs("2023-03-20", adjusted=False)
             gets unadjusted daily bars for all stocks
    Example: get_grouped_daily_aggs("2022-12-30", locale="us", market_type="stocks")
             gets daily bars for US stocks on December 30, 2022

    Response includes (for each ticker):
    - T: Ticker symbol
    - o: Open price
    - h: High price
    - l: Low price
    - c: Close price
    - v: Trading volume
    - vw: Volume weighted average price (VWAP)
    - t: Timestamp (milliseconds since epoch)
    - n: Number of transactions

    Note: Daily market summary considerations:
    - Returns data for ALL stocks traded on specified date
    - Can return thousands of tickers in single response (large dataset)
    - Includes pre-market, regular, and after-hours trading activity
    - Default excludes OTC securities (set include_otc=True to include)
    - Adjusted=true accounts for stock splits in historical data
    - VWAP provides volume-weighted average for the day
    - Transaction count (n) indicates daily trading activity level
    - Timestamp is beginning of the trading day
    - Useful for market-wide analysis and screening
    - Perfect for identifying top gainers/losers across market
    - Enables bulk historical data collection
    - Results include delisted stocks if they traded on that date
    - Market-wide volatility and volume analysis

    Use case: Building a market heatmap - fetch grouped daily data to identify the day's top
    performers and worst performers across the entire market, calculate market-wide metrics like
    average volume and volatility, and visualize sector performance by aggregating related tickers.
    """
    try:
        results = polygon_client.get_grouped_daily_aggs(
            date=date,
            adjusted=adjusted,
            include_otc=include_otc,
            locale=locale,
            market_type=market_type,
            params=params,
            raw=True,
        )

        return json_to_csv(results.data.decode("utf-8"))
    except Exception as e:
        return f"Error: {e}"


@poly_mcp.tool(annotations=ToolAnnotations(readOnlyHint=True))
async def get_daily_open_close_agg(
    ticker: str,
    date: str,
    adjusted: Optional[bool] = None,
    params: Optional[Dict[str, Any]] = None,
) -> str:
    """
    Retrieve the opening and closing prices for a specific stock ticker on a given date,
    along with any pre-market and after-hours trade prices. This endpoint provides essential
    daily pricing details including OHLC (open, high, low, close), volume, and extended
    trading session data.

    Reference: https://polygon.io/docs/rest/stocks/aggregates/daily-ticker-summary

    Parameters:
    - ticker: Case-sensitive ticker symbol (e.g., "AAPL" for Apple Inc., "MSFT" for Microsoft)
    - date: The date for the requested open/close in YYYY-MM-DD format (e.g., "2023-01-09")
    - adjusted: Whether results are adjusted for splits (default: True). Set to False for
                unadjusted results.

    Response includes:
    - open, high, low, close: Regular trading session OHLC prices
    - preMarket: Opening price in pre-market trading session
    - afterHours: Closing price in after-hours trading session
    - volume: Total trading volume for the day
    - otc: Whether this is an OTC ticker (if applicable)

    Use Cases:
    - Daily performance analysis: Track day-over-day price movements
    - Historical data collection: Build historical price datasets
    - After-hours insights: Analyze extended trading session activity
    - Portfolio tracking: Monitor daily changes in holdings

    Example: get_daily_open_close_agg("AAPL", "2023-01-09") returns AAPL's complete daily
             summary for Jan 9, 2023, including pre-market and after-hours prices

    Example: get_daily_open_close_agg("TSLA", "2024-03-15", adjusted=False) returns
             unadjusted prices for Tesla on March 15, 2024

    Note: For multiple days of data, use get_aggs instead. For the most recent trading day,
          use get_previous_close_agg instead.
    """
    try:
        results = polygon_client.get_daily_open_close_agg(
            ticker=ticker, date=date, adjusted=adjusted, params=params, raw=True
        )

        return json_to_csv(results.data.decode("utf-8"))
    except Exception as e:
        return f"Error: {e}"


@poly_mcp.tool(annotations=ToolAnnotations(readOnlyHint=True))
async def get_previous_close_agg(
    ticker: str,
    adjusted: Optional[bool] = None,
    params: Optional[Dict[str, Any]] = None,
) -> str:
    """
    Retrieve the previous trading day's open, high, low, and close (OHLC) data for a
    specified stock ticker. This endpoint provides key pricing metrics including volume
    and VWAP (volume weighted average price) to help assess recent performance and
    inform trading strategies.

    Reference: https://polygon.io/docs/rest/stocks/aggregates/previous-day-bar

    Parameters:
    - ticker: Case-sensitive ticker symbol (e.g., "AAPL" for Apple Inc., "MSFT" for Microsoft)
    - adjusted: Whether results are adjusted for splits (default: True). Set to False for
                unadjusted results.

    Response includes:
    - o (open): Opening price for the previous trading day
    - h (high): Highest price during the previous trading day
    - l (low): Lowest price during the previous trading day
    - c (close): Closing price for the previous trading day
    - v (volume): Total trading volume for the day
    - vw (vwap): Volume weighted average price
    - t (timestamp): Unix timestamp of the bar
    - T (ticker): The ticker symbol

    Use Cases:
    - Baseline comparison: Compare current prices to previous day's performance
    - Technical analysis: Calculate daily indicators and price changes
    - Market research: Analyze recent trading patterns and momentum
    - Daily reporting: Generate end-of-day summaries and alerts

    Example: get_previous_close_agg("AAPL") returns Apple's most recent trading day data
             with full OHLC metrics and volume

    Example: get_previous_close_agg("TSLA", adjusted=False) returns Tesla's previous day
             data without split adjustments

    Note: This automatically retrieves the last available trading day, accounting for
          weekends and holidays. For a specific historical date, use get_daily_open_close_agg
          instead. For multiple days of historical data, use get_aggs.
    """
    try:
        results = polygon_client.get_previous_close_agg(
            ticker=ticker, adjusted=adjusted, params=params, raw=True
        )

        return json_to_csv(results.data.decode("utf-8"))
    except Exception as e:
        return f"Error: {e}"


# @poly_mcp.tool(annotations=ToolAnnotations(readOnlyHint=True))  # DISABLED
async def list_trades(
    ticker: str,
    timestamp: Optional[Union[str, int, datetime, date]] = None,
    timestamp_lt: Optional[Union[str, int, datetime, date]] = None,
    timestamp_lte: Optional[Union[str, int, datetime, date]] = None,
    timestamp_gt: Optional[Union[str, int, datetime, date]] = None,
    timestamp_gte: Optional[Union[str, int, datetime, date]] = None,
    limit: Optional[int] = 10,
    sort: Optional[str] = None,
    order: Optional[str] = None,
    params: Optional[Dict[str, Any]] = None,
) -> str:
    """
    Get trades for a ticker symbol.
    """
    try:
        results = polygon_client.list_trades(
            ticker=ticker,
            timestamp=timestamp,
            timestamp_lt=timestamp_lt,
            timestamp_lte=timestamp_lte,
            timestamp_gt=timestamp_gt,
            timestamp_gte=timestamp_gte,
            limit=limit,
            sort=sort,
            order=order,
            params=params,
            raw=True,
        )

        return json_to_csv(results.data.decode("utf-8"))
    except Exception as e:
        return f"Error: {e}"


# @poly_mcp.tool(annotations=ToolAnnotations(readOnlyHint=True))  # DISABLED
async def get_last_trade(
    ticker: str,
    params: Optional[Dict[str, Any]] = None,
) -> str:
    """
    Get the most recent trade for a ticker symbol.
    """
    try:
        results = polygon_client.get_last_trade(ticker=ticker, params=params, raw=True)

        return json_to_csv(results.data.decode("utf-8"))
    except Exception as e:
        return f"Error: {e}"


# @poly_mcp.tool(annotations=ToolAnnotations(readOnlyHint=True))  # DISABLED
async def get_last_crypto_trade(
    from_: str,
    to: str,
    params: Optional[Dict[str, Any]] = None,
) -> str:
    """
    Get the most recent trade for a crypto pair.
    """
    try:
        results = polygon_client.get_last_crypto_trade(
            from_=from_, to=to, params=params, raw=True
        )

        return json_to_csv(results.data.decode("utf-8"))
    except Exception as e:
        return f"Error: {e}"


# @poly_mcp.tool(annotations=ToolAnnotations(readOnlyHint=True))  # DISABLED
async def list_quotes(
    ticker: str,
    timestamp: Optional[Union[str, int, datetime, date]] = None,
    timestamp_lt: Optional[Union[str, int, datetime, date]] = None,
    timestamp_lte: Optional[Union[str, int, datetime, date]] = None,
    timestamp_gt: Optional[Union[str, int, datetime, date]] = None,
    timestamp_gte: Optional[Union[str, int, datetime, date]] = None,
    limit: Optional[int] = 10,
    sort: Optional[str] = None,
    order: Optional[str] = None,
    params: Optional[Dict[str, Any]] = None,
) -> str:
    """
    Get quotes for a ticker symbol.
    """
    try:
        results = polygon_client.list_quotes(
            ticker=ticker,
            timestamp=timestamp,
            timestamp_lt=timestamp_lt,
            timestamp_lte=timestamp_lte,
            timestamp_gt=timestamp_gt,
            timestamp_gte=timestamp_gte,
            limit=limit,
            sort=sort,
            order=order,
            params=params,
            raw=True,
        )

        return json_to_csv(results.data.decode("utf-8"))
    except Exception as e:
        return f"Error: {e}"


# @poly_mcp.tool(annotations=ToolAnnotations(readOnlyHint=True))  # DISABLED
async def get_last_quote(
    ticker: str,
    params: Optional[Dict[str, Any]] = None,
) -> str:
    """
    Get the most recent quote for a ticker symbol.
    """
    try:
        results = polygon_client.get_last_quote(ticker=ticker, params=params, raw=True)

        return json_to_csv(results.data.decode("utf-8"))
    except Exception as e:
        return f"Error: {e}"


# @poly_mcp.tool(annotations=ToolAnnotations(readOnlyHint=True))  # DISABLED
async def get_last_forex_quote(
    from_: str,
    to: str,
    params: Optional[Dict[str, Any]] = None,
) -> str:
    """
    Get the most recent forex quote.
    """
    try:
        results = polygon_client.get_last_forex_quote(
            from_=from_, to=to, params=params, raw=True
        )

        return json_to_csv(results.data.decode("utf-8"))
    except Exception as e:
        return f"Error: {e}"


@poly_mcp.tool(annotations=ToolAnnotations(readOnlyHint=True))
async def get_real_time_currency_conversion(
    from_: str,
    to: str,
    amount: Optional[float] = None,
    precision: Optional[int] = None,
    params: Optional[Dict[str, Any]] = None,
) -> str:
    """
    Get real-time currency conversion.
    """
    try:
        results = polygon_client.get_real_time_currency_conversion(
            from_=from_,
            to=to,
            amount=amount,
            precision=precision,
            params=params,
            raw=True,
        )

        return json_to_csv(results.data.decode("utf-8"))
    except Exception as e:
        return f"Error: {e}"


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


@poly_mcp.tool(annotations=ToolAnnotations(readOnlyHint=True))
async def get_sma(
    ticker: str,
    timestamp: Optional[Union[str, int, datetime, date]] = None,
    timespan: Optional[str] = "day",
    adjusted: Optional[bool] = True,
    window: Optional[int] = 50,
    series_type: Optional[str] = "close",
    expand_underlying: Optional[bool] = False,
    order: Optional[str] = "desc",
    limit: Optional[int] = 10,
    timestamp_gte: Optional[Union[str, int, datetime, date]] = None,
    timestamp_gt: Optional[Union[str, int, datetime, date]] = None,
    timestamp_lte: Optional[Union[str, int, datetime, date]] = None,
    timestamp_lt: Optional[Union[str, int, datetime, date]] = None,
    params: Optional[Dict[str, Any]] = None,
) -> str:
    """
    Retrieve the Simple Moving Average (SMA) for a specified ticker over a defined time range.
    The SMA calculates the average price across a set number of periods, smoothing price fluctuations
    to reveal underlying trends and potential signals. Works with both stocks and options.

    Reference (Stocks): https://polygon.io/docs/rest/stocks/technical-indicators/simple-moving-average
    Reference (Options): https://polygon.io/docs/options/get_v1_indicators_sma__optionsticker

    Parameters:
    - ticker: Case-sensitive ticker symbol (e.g., "AAPL" for stocks, "O:SPY241220P00720000" for options)
    - timestamp: Query by specific timestamp (YYYY-MM-DD format or millisecond timestamp)
    - timespan: Size of the aggregate time window (minute, hour, day, week, month, quarter, year)
                Default: "day"
    - adjusted: Whether aggregates used to calculate SMA are adjusted for splits (default: True)
                Set to False for unadjusted results
    - window: Window size used to calculate the SMA (default: 50)
              e.g., window=10 with daily aggregates = 10-day moving average
    - series_type: Price in the aggregate used to calculate SMA (default: "close")
                   Options: "close", "open", "high", "low"
                   e.g., "close" uses closing prices to calculate the SMA
    - expand_underlying: Whether to include the aggregates used to calculate this indicator in the
                         response (default: False)
    - order: Order in which to return results, ordered by timestamp ("asc" or "desc")
             Default: "desc"
    - limit: Limit the number of results returned (default: 10, max: 5000)
    - timestamp_gte: Range by timestamp - greater than or equal to
    - timestamp_gt: Range by timestamp - greater than
    - timestamp_lte: Range by timestamp - less than or equal to
    - timestamp_lt: Range by timestamp - less than

    Response includes:
    - results.values: Array of objects with timestamp and SMA value pairs
      - timestamp: Unix timestamp in milliseconds
      - value: Calculated SMA value for that period
    - results.underlying: The underlying aggregates used (if expand_underlying=True)
      - aggregates: Array of OHLCV data (close, high, low, open, volume, vwap, timestamp, # trades)
      - url: API URL to fetch the underlying aggregate data
    - next_url: If present, use to fetch the next page of data
    - request_id: Server-assigned request identifier
    - status: Response status (e.g., "OK")

    Use Cases:
    - Trend analysis: Identify the direction and strength of price trends over time
    - Trading signal generation: Detect SMA crossovers (e.g., 50-day crossing 200-day) for buy/sell signals
    - Identifying support/resistance: SMAs often act as dynamic support or resistance levels
    - Refining entry/exit timing: Use SMA to confirm trend direction before entering or exiting positions

    Example: get_sma("AAPL", window=50, timespan="day") returns 50-day SMA for Apple stock,
             showing the average closing price over the last 50 trading days

    Example: get_sma("MSFT", window=200, timespan="day", limit=30) returns 30 data points
             of the 200-day SMA for Microsoft - useful for identifying major trend direction

    Example: get_sma("AAPL", window=20, timespan="day", expand_underlying=True) returns
             20-day SMA with the underlying OHLCV data used in the calculation

    Example: get_sma("O:SPY241220P00720000", window=20, timespan="day") returns 20-day
             SMA for SPY put option premium to identify trends in option pricing

    Note: SMA considerations and best practices:
    - Common windows: 50-day (intermediate trend), 100-day, and 200-day (long-term trend)
    - Golden Cross: 50-day SMA crossing above 200-day SMA (bullish signal)
    - Death Cross: 50-day SMA crossing below 200-day SMA (bearish signal)
    - Shorter windows (10-20 periods) are more responsive to recent price changes
    - Longer windows (100-200 periods) better filter out noise and identify major trends
    - Window size of 10 with daily aggregates = 10-day moving average
    - For options, track SMA of premium prices to identify momentum in option demand
    - Use expand_underlying=True to analyze the raw price data behind the SMA calculation
    - Series_type="close" is most common, but "high"/"low" can identify breakout levels
    - Use timestamp range parameters (timestamp_gte, timestamp_lt) for specific date ranges
    """
    try:
        # Build params dict for timestamp range filters
        final_params = {**(params or {})}
        if timestamp_gte is not None:
            final_params["timestamp.gte"] = timestamp_gte
        if timestamp_gt is not None:
            final_params["timestamp.gt"] = timestamp_gt
        if timestamp_lte is not None:
            final_params["timestamp.lte"] = timestamp_lte
        if timestamp_lt is not None:
            final_params["timestamp.lt"] = timestamp_lt

        # Build kwargs conditionally to avoid passing empty params
        kwargs = {
            "ticker": ticker,
            "timespan": timespan,
            "adjusted": adjusted,
            "window": window,
            "series_type": series_type,
            "expand_underlying": expand_underlying,
            "order": order,
            "limit": limit,
            "raw": True,
        }
        if timestamp is not None:
            kwargs["timestamp"] = timestamp
        if final_params:
            kwargs["params"] = final_params

        results = polygon_client.get_sma(**kwargs)

        # Parse the response and extract the values array
        import json

        data = json.loads(results.data.decode("utf-8"))
        if "results" in data and "values" in data["results"]:
            # Wrap the values in a results key for consistent CSV formatting
            formatted_data = {"results": data["results"]["values"]}
            return json_to_csv(formatted_data)
        return json_to_csv(results.data.decode("utf-8"))
    except Exception as e:
        return f"Error: {e}"


@poly_mcp.tool(annotations=ToolAnnotations(readOnlyHint=True))
async def get_ema(
    ticker: str,
    timestamp: Optional[Union[str, int, datetime, date]] = None,
    timespan: Optional[str] = "day",
    adjusted: Optional[bool] = True,
    window: Optional[int] = 50,
    series_type: Optional[str] = "close",
    expand_underlying: Optional[bool] = False,
    order: Optional[str] = "desc",
    limit: Optional[int] = 10,
    timestamp_gte: Optional[Union[str, int, datetime, date]] = None,
    timestamp_gt: Optional[Union[str, int, datetime, date]] = None,
    timestamp_lte: Optional[Union[str, int, datetime, date]] = None,
    timestamp_lt: Optional[Union[str, int, datetime, date]] = None,
    params: Optional[Dict[str, Any]] = None,
) -> str:
    """
    Retrieve the Exponential Moving Average (EMA) for a specified ticker over a defined time range.
    The EMA places greater weight on recent prices, enabling quicker trend detection and more responsive
    signals compared to the Simple Moving Average (SMA). Works with both stocks and options.

    Reference (Stocks): https://polygon.io/docs/rest/stocks/technical-indicators/exponential-moving-average
    Reference (Options): https://polygon.io/docs/options/get_v1_indicators_ema__optionsticker

    Parameters:
    - ticker: Case-sensitive ticker symbol (e.g., "AAPL" for stocks, "O:SPY241220P00720000" for options)
    - timestamp: Query by specific timestamp (YYYY-MM-DD format or millisecond timestamp)
    - timespan: Size of the aggregate time window (minute, hour, day, week, month, quarter, year)
                Default: "day"
    - adjusted: Whether aggregates used to calculate EMA are adjusted for splits (default: True)
                Set to False for unadjusted results
    - window: Window size used to calculate the EMA (default: 50)
              e.g., window=10 with daily aggregates = 10-day exponential moving average
    - series_type: Price in the aggregate used to calculate EMA (default: "close")
                   Options: "close", "open", "high", "low"
                   e.g., "close" uses closing prices to calculate the EMA
    - expand_underlying: Whether to include the aggregates used to calculate this indicator in the
                         response (default: False)
    - order: Order in which to return results, ordered by timestamp ("asc" or "desc")
             Default: "desc"
    - limit: Limit the number of results returned (default: 10, max: 5000)
    - timestamp_gte: Range by timestamp - greater than or equal to
    - timestamp_gt: Range by timestamp - greater than
    - timestamp_lte: Range by timestamp - less than or equal to
    - timestamp_lt: Range by timestamp - less than

    Response includes:
    - results.values: Array of objects with timestamp and EMA value pairs
      - timestamp: Unix timestamp in milliseconds
      - value: Calculated EMA value for that period
    - results.underlying: The underlying aggregates used (if expand_underlying=True)
      - url: API URL to fetch the underlying aggregate data
    - next_url: If present, use to fetch the next page of data
    - request_id: Server-assigned request identifier
    - status: Response status (e.g., "OK")

    Use Cases:
    - Trend identification: Detect the direction and strength of trends with more recent price emphasis
    - EMA crossover signals: Generate buy/sell signals when EMAs of different periods cross
    - Dynamic support/resistance levels: Use EMAs as adaptive support/resistance zones
    - Adjusting strategies based on recent market volatility: React faster to market changes than SMA

    Example: get_ema("AAPL", window=12, timespan="day") returns 12-day EMA for Apple stock,
             with greater weight on recent prices for faster trend detection

    Example: get_ema("MSFT", window=26, timespan="day", limit=30) returns 30 data points
             of the 26-day EMA for Microsoft - commonly used with 12-day EMA for MACD

    Example: get_ema("AAPL", window=12, timespan="day", expand_underlying=True) returns
             12-day EMA with the underlying OHLCV data used in the calculation

    Example: get_ema("O:SPY241220P00720000", window=12, timespan="day") returns 12-day
             EMA for SPY put option premium to identify rapid premium changes

    Note: EMA considerations and best practices:
    - Common windows: 12-day and 26-day (for MACD component EMAs), 50-day, and 200-day
    - EMA vs SMA: EMA responds faster to price changes due to exponential weighting of recent data
    - Shorter windows (8-20 periods) detect trend changes quickly but may generate false signals
    - Longer windows (50-200 periods) provide stronger trend confirmation with less noise
    - Window size of 10 with daily aggregates = 10-day exponential moving average
    - EMA crossovers generate earlier signals than SMA crossovers (e.g., 12 crossing 26)
    - For options, EMA helps identify rapid premium changes and volatility shifts faster than SMA
    - Use expand_underlying=True to analyze the raw price data behind the EMA calculation
    - Series_type="close" is most common, but "high"/"low" can identify breakout levels
    - Use timestamp range parameters (timestamp_gte, timestamp_lt) for specific date ranges
    - More responsive to recent price action makes EMA ideal for short-term trading strategies
    """
    try:
        # Build params dict for timestamp range filters
        final_params = {**(params or {})}
        if timestamp_gte is not None:
            final_params["timestamp.gte"] = timestamp_gte
        if timestamp_gt is not None:
            final_params["timestamp.gt"] = timestamp_gt
        if timestamp_lte is not None:
            final_params["timestamp.lte"] = timestamp_lte
        if timestamp_lt is not None:
            final_params["timestamp.lt"] = timestamp_lt

        # Build kwargs conditionally to avoid passing empty params
        kwargs = {
            "ticker": ticker,
            "timespan": timespan,
            "adjusted": adjusted,
            "window": window,
            "series_type": series_type,
            "expand_underlying": expand_underlying,
            "order": order,
            "limit": limit,
            "raw": True,
        }
        if timestamp is not None:
            kwargs["timestamp"] = timestamp
        if final_params:
            kwargs["params"] = final_params

        results = polygon_client.get_ema(**kwargs)

        # Parse the response and extract the values array
        import json

        data = json.loads(results.data.decode("utf-8"))
        if "results" in data and "values" in data["results"]:
            # Wrap the values in a results key for consistent CSV formatting
            formatted_data = {"results": data["results"]["values"]}
            return json_to_csv(formatted_data)
        return json_to_csv(results.data.decode("utf-8"))
    except Exception as e:
        return f"Error: {e}"


@poly_mcp.tool(annotations=ToolAnnotations(readOnlyHint=True))
async def get_macd(
    ticker: str,
    timestamp: Optional[Union[str, int, datetime, date]] = None,
    timespan: Optional[str] = "day",
    adjusted: Optional[bool] = True,
    short_window: Optional[int] = 12,
    long_window: Optional[int] = 26,
    signal_window: Optional[int] = 9,
    series_type: Optional[str] = "close",
    expand_underlying: Optional[bool] = False,
    order: Optional[str] = "desc",
    limit: Optional[int] = 10,
    timestamp_gte: Optional[Union[str, int, datetime, date]] = None,
    timestamp_gt: Optional[Union[str, int, datetime, date]] = None,
    timestamp_lte: Optional[Union[str, int, datetime, date]] = None,
    timestamp_lt: Optional[Union[str, int, datetime, date]] = None,
    params: Optional[Dict[str, Any]] = None,
) -> str:
    """
    Retrieve the Moving Average Convergence/Divergence (MACD) for a specified ticker over a defined
    time range. MACD is a momentum indicator derived from two moving averages, helping to identify
    trend strength, direction, and potential trading signals. Works with both stocks and options.

    Reference (Stocks): https://polygon.io/docs/rest/stocks/technical-indicators/moving-average-convergence-divergence
    Reference (Options): https://polygon.io/docs/options/get_v1_indicators_macd__optionsticker

    Parameters:
    - ticker: Case-sensitive ticker symbol (e.g., "AAPL" for stocks, "O:SPY241220P00720000" for options)
    - timestamp: Query by specific timestamp (YYYY-MM-DD format or millisecond timestamp)
    - timespan: Size of the aggregate time window (minute, hour, day, week, month, quarter, year)
                Default: "day"
    - adjusted: Whether aggregates used to calculate MACD are adjusted for splits (default: True)
                Set to False for unadjusted results
    - short_window: Short window size used to calculate MACD data (default: 12)
                    Used for the fast EMA in the MACD calculation
    - long_window: Long window size used to calculate MACD data (default: 26)
                   Used for the slow EMA in the MACD calculation
    - signal_window: Window size used to calculate the MACD signal line (default: 9)
                     Signal line is the EMA of the MACD line
    - series_type: Price in the aggregate used to calculate MACD (default: "close")
                   Options: "close", "open", "high", "low"
                   e.g., "close" uses closing prices to calculate the MACD
    - expand_underlying: Whether to include the aggregates used to calculate this indicator in the
                         response (default: False)
    - order: Order in which to return results, ordered by timestamp ("asc" or "desc")
             Default: "desc"
    - limit: Limit the number of results returned (default: 10, max: 5000)
    - timestamp_gte: Range by timestamp - greater than or equal to
    - timestamp_gt: Range by timestamp - greater than
    - timestamp_lte: Range by timestamp - less than or equal to
    - timestamp_lt: Range by timestamp - less than

    Response includes:
    - results.values: Array of objects with MACD indicator data for each timestamp
      - timestamp: Unix timestamp in milliseconds
      - value: MACD line value (short_window EMA - long_window EMA)
      - signal: Signal line value (signal_window EMA of MACD line)
      - histogram: MACD histogram value (MACD line - signal line)
    - results.underlying: The underlying aggregates used (if expand_underlying=True)
      - url: API URL to fetch the underlying aggregate data
    - next_url: If present, use to fetch the next page of data
    - request_id: Server-assigned request identifier
    - status: Response status (e.g., "OK")

    MACD Calculation:
    - MACD Line = short_window EMA - long_window EMA (typically 12-day EMA - 26-day EMA)
    - Signal Line = signal_window EMA of MACD Line (typically 9-day EMA)
    - Histogram = MACD Line - Signal Line

    Use Cases:
    - Momentum analysis: Measure the strength and direction of price momentum
    - Signal generation (crossover events): Identify buy/sell signals from MACD/signal line crosses
    - Spotting overbought/oversold conditions: Extreme MACD values may indicate reversals
    - Confirming trend directions: MACD position relative to zero confirms trend direction

    Example: get_macd("AAPL") returns MACD with standard parameters (12, 26, 9) for Apple stock,
             showing MACD line, signal line, and histogram for momentum analysis

    Example: get_macd("MSFT", short_window=8, long_window=17, signal_window=9) returns custom
             MACD for Microsoft with faster-responding parameters for short-term trading

    Example: get_macd("AAPL", timespan="day", limit=50, expand_underlying=True) returns 50
             data points of MACD with the underlying OHLCV data used in calculations

    Example: get_macd("O:SPY241220P00720000", timespan="day") returns MACD for SPY put
             option premium to identify momentum shifts in option pricing

    Note: MACD considerations and best practices:
    - Standard parameters: short=12, long=26, signal=9 (known as "12, 26, 9" MACD)
    - MACD line = 12-day EMA minus 26-day EMA (measures momentum)
    - Signal line = 9-day EMA of MACD line (triggers trading signals)
    - Histogram = MACD minus Signal (visualizes convergence/divergence)
    - Bullish signal: MACD crosses above signal line (histogram turns positive)
    - Bearish signal: MACD crosses below signal line (histogram turns negative)
    - Centerline crossover: MACD crossing zero line indicates trend change
    - Divergence: MACD moving opposite to price can signal trend reversal
      - Bullish divergence: Price makes lower lows, MACD makes higher lows
      - Bearish divergence: Price makes higher highs, MACD makes lower highs
    - Histogram increasing = momentum strengthening in current direction
    - Histogram decreasing = momentum weakening, potential reversal ahead
    - For options, track MACD of premium prices to identify volatility expansion/contraction
    - Faster parameters (e.g., 5, 13, 5) for shorter timeframes and day trading
    - Use expand_underlying=True to verify price data quality behind calculations
    - Combine with other indicators (RSI, volume) for confirmation
    - Use timestamp range parameters (timestamp_gte, timestamp_lt) for specific date ranges
    """
    try:
        # Build params dict for timestamp range filters
        final_params = {**(params or {})}
        if timestamp_gte is not None:
            final_params["timestamp.gte"] = timestamp_gte
        if timestamp_gt is not None:
            final_params["timestamp.gt"] = timestamp_gt
        if timestamp_lte is not None:
            final_params["timestamp.lte"] = timestamp_lte
        if timestamp_lt is not None:
            final_params["timestamp.lt"] = timestamp_lt

        # Build kwargs conditionally to avoid passing empty params
        kwargs = {
            "ticker": ticker,
            "timespan": timespan,
            "adjusted": adjusted,
            "short_window": short_window,
            "long_window": long_window,
            "signal_window": signal_window,
            "series_type": series_type,
            "expand_underlying": expand_underlying,
            "order": order,
            "limit": limit,
            "raw": True,
        }
        if timestamp is not None:
            kwargs["timestamp"] = timestamp
        if final_params:
            kwargs["params"] = final_params

        results = polygon_client.get_macd(**kwargs)

        # Parse the response and extract the values array
        import json

        data = json.loads(results.data.decode("utf-8"))
        if "results" in data and "values" in data["results"]:
            # Wrap the values in a results key for consistent CSV formatting
            formatted_data = {"results": data["results"]["values"]}
            return json_to_csv(formatted_data)
        return json_to_csv(results.data.decode("utf-8"))
    except Exception as e:
        return f"Error: {e}"


@poly_mcp.tool(annotations=ToolAnnotations(readOnlyHint=True))
async def get_rsi(
    ticker: str,
    timestamp: Optional[Union[str, int, datetime, date]] = None,
    timespan: Optional[str] = "day",
    adjusted: Optional[bool] = True,
    window: Optional[int] = 14,
    series_type: Optional[str] = "close",
    expand_underlying: Optional[bool] = False,
    order: Optional[str] = "desc",
    limit: Optional[int] = 10,
    timestamp_gte: Optional[Union[str, int, datetime, date]] = None,
    timestamp_gt: Optional[Union[str, int, datetime, date]] = None,
    timestamp_lte: Optional[Union[str, int, datetime, date]] = None,
    timestamp_lt: Optional[Union[str, int, datetime, date]] = None,
    params: Optional[Dict[str, Any]] = None,
) -> str:
    """
    Retrieve the Relative Strength Index (RSI) for a specified ticker over a defined time range.
    The RSI measures the speed and magnitude of price changes, oscillating between 0 and 100 to
    help identify overbought or oversold conditions. Works with both stocks and options.

    Reference (Stocks): https://polygon.io/docs/rest/stocks/technical-indicators/relative-strength-index
    Reference (Options): https://polygon.io/docs/options/get_v1_indicators_rsi__optionsticker

    Parameters:
    - ticker: Case-sensitive ticker symbol (e.g., "AAPL" for stocks, "O:SPY241220P00720000" for options)
    - timestamp: Query by specific timestamp (YYYY-MM-DD format or millisecond timestamp)
    - timespan: Size of the aggregate time window (minute, hour, day, week, month, quarter, year)
                Default: "day"
    - adjusted: Whether aggregates used to calculate RSI are adjusted for splits (default: True)
                Set to False for unadjusted results
    - window: Window size used to calculate the relative strength index (default: 14)
              Standard is 14-period RSI for most applications
    - series_type: Price in the aggregate used to calculate RSI (default: "close")
                   Options: "close", "open", "high", "low"
                   e.g., "close" uses closing prices to calculate the RSI
    - expand_underlying: Whether to include the aggregates used to calculate this indicator in the
                         response (default: False)
    - order: Order in which to return results, ordered by timestamp ("asc" or "desc")
             Default: "desc"
    - limit: Limit the number of results returned (default: 10, max: 5000)
    - timestamp_gte: Range by timestamp - greater than or equal to
    - timestamp_gt: Range by timestamp - greater than
    - timestamp_lte: Range by timestamp - less than or equal to
    - timestamp_lt: Range by timestamp - less than

    Response includes:
    - results.values: Array of objects with timestamp and RSI value pairs
      - timestamp: Unix timestamp in milliseconds
      - value: Calculated RSI value (0-100 scale)
    - results.underlying: The underlying aggregates used (if expand_underlying=True)
      - url: API URL to fetch the underlying aggregate data
    - next_url: If present, use to fetch the next page of data
    - request_id: Server-assigned request identifier
    - status: Response status (e.g., "OK")

    RSI Calculation:
    - RSI = 100 - (100 / (1 + RS))
    - RS (Relative Strength) = Average Gain / Average Loss over the window period
    - Average Gain = Sum of gains over window / window size
    - Average Loss = Sum of losses over window / window size
    - RSI oscillates between 0 (extreme weakness) and 100 (extreme strength)

    Use Cases:
    - Overbought/oversold detection: Identify when prices have moved too far too fast
    - Divergence analysis: Spot when RSI and price move in opposite directions
    - Trend confirmation: Validate the strength of existing trends
    - Refining market entry/exit strategies: Time entries and exits based on momentum extremes

    Example: get_rsi("AAPL") returns 14-day RSI for Apple stock with standard parameters,
             showing momentum strength on a 0-100 scale

    Example: get_rsi("MSFT", window=9, limit=30) returns 30 data points of 9-day RSI for
             Microsoft - faster RSI for more responsive signals

    Example: get_rsi("AAPL", window=14, timespan="day", expand_underlying=True) returns
             14-day RSI with the underlying OHLCV data used in calculations

    Example: get_rsi("O:SPY241220P00720000", window=14, timespan="day") returns 14-day
             RSI for SPY put option premium to identify overbought/oversold conditions

    Note: RSI considerations and best practices:
    - Standard window: 14 periods (14-day RSI for daily charts is most common)
    - RSI ranges: 0 (extreme weakness) to 100 (extreme strength)
    - Traditional interpretation:
      * RSI > 70: Overbought condition (potential sell signal or take profits)
      * RSI < 30: Oversold condition (potential buy signal or mean reversion)
      * RSI = 50: Neutral momentum (no directional bias)
      * RSI 40-60: Typical range in sideways markets
    - Strong trends can keep RSI overbought (>70) or oversold (<30) for extended periods
      * In strong uptrends, RSI may stay between 40-90
      * In strong downtrends, RSI may stay between 10-60
    - Alternative thresholds:
      * Conservative: 80/20 levels for fewer but stronger signals
      * Aggressive: 60/40 levels for earlier but more frequent signals
    - Window variations:
      * Shorter windows (7-9): More sensitive, more signals, more false positives
      * Standard window (14): Balanced sensitivity and reliability
      * Longer windows (21-25): Smoother, stronger confirmation, more lag
    - Divergence patterns (powerful reversal signals):
      * Bullish divergence: Price makes lower lows, RSI makes higher lows (reversal up)
      * Bearish divergence: Price makes higher highs, RSI makes lower highs (reversal down)
    - Failure swings (advanced patterns):
      * Bullish: RSI drops below 30, rallies above 30, pulls back but stays above 30, then breaks higher
      * Bearish: RSI rises above 70, falls below 70, rallies but stays below 70, then breaks lower
    - For options trading:
      * Track RSI of option premium to identify extreme pricing
      * RSI < 30 on premium may indicate oversold conditions for mean reversion
      * RSI > 70 on premium may indicate overbought conditions or volatility expansion
    - Combining RSI with other indicators:
      * RSI + MACD: Confirm momentum and trend direction
      * RSI + Volume: Validate the strength of RSI signals
      * RSI + Support/Resistance: Time entries at key levels with RSI confirmation
    - Use expand_underlying=True to verify price data quality behind calculations
    - Use timestamp range parameters (timestamp_gte, timestamp_lt) for specific date ranges
    """
    try:
        # Build params dict for timestamp range filters
        final_params = {**(params or {})}
        if timestamp_gte is not None:
            final_params["timestamp.gte"] = timestamp_gte
        if timestamp_gt is not None:
            final_params["timestamp.gt"] = timestamp_gt
        if timestamp_lte is not None:
            final_params["timestamp.lte"] = timestamp_lte
        if timestamp_lt is not None:
            final_params["timestamp.lt"] = timestamp_lt

        # Build kwargs conditionally to avoid passing empty params
        kwargs = {
            "ticker": ticker,
            "timespan": timespan,
            "adjusted": adjusted,
            "window": window,
            "series_type": series_type,
            "expand_underlying": expand_underlying,
            "order": order,
            "limit": limit,
            "raw": True,
        }
        if timestamp is not None:
            kwargs["timestamp"] = timestamp
        if final_params:
            kwargs["params"] = final_params

        results = polygon_client.get_rsi(**kwargs)

        # Parse the response and extract the values array
        import json

        data = json.loads(results.data.decode("utf-8"))
        if "results" in data and "values" in data["results"]:
            # Wrap the values in a results key for consistent CSV formatting
            formatted_data = {"results": data["results"]["values"]}
            return json_to_csv(formatted_data)
        return json_to_csv(results.data.decode("utf-8"))
    except Exception as e:
        return f"Error: {e}"


@poly_mcp.tool(annotations=ToolAnnotations(readOnlyHint=True))
async def get_market_holidays(
    params: Optional[Dict[str, Any]] = None,
) -> str:
    """
    Get upcoming market holidays and special market hours (early close days).
    Returns details about when US stock markets are closed or have modified hours.

    Reference: https://polygon.io/docs/rest/stocks/market-operations/market-holidays

    Returns information about:
    - Market holiday dates (when markets are closed)
    - Holiday names
    - Early close days (when markets close early)
    - Modified trading hours

    Example: get_market_holidays() returns all upcoming market holidays and special hours

    Note: Useful for planning trading strategies and understanding when markets will be closed.
    Common US market holidays include New Year's Day, Independence Day, Thanksgiving, and Christmas.
    """
    try:
        results = polygon_client.get_market_holidays(params=params, raw=True)

        return json_to_csv(results.data.decode("utf-8"))
    except Exception as e:
        return f"Error: {e}"


@poly_mcp.tool(annotations=ToolAnnotations(readOnlyHint=True))
async def get_market_status(
    params: Optional[Dict[str, Any]] = None,
) -> str:
    """
    Get current trading status of exchanges and financial markets.
    """
    try:
        results = polygon_client.get_market_status(params=params, raw=True)

        return json_to_csv(results.data.decode("utf-8"))
    except Exception as e:
        return f"Error: {e}"


@poly_mcp.tool(annotations=ToolAnnotations(readOnlyHint=True))
async def list_tickers(
    ticker: Optional[str] = None,
    type: Optional[str] = None,
    market: Optional[str] = None,
    exchange: Optional[str] = None,
    cusip: Optional[str] = None,
    cik: Optional[str] = None,
    date: Optional[Union[str, datetime, date]] = None,
    search: Optional[str] = None,
    active: Optional[bool] = None,
    ticker_gte: Optional[str] = None,
    ticker_gt: Optional[str] = None,
    ticker_lte: Optional[str] = None,
    ticker_lt: Optional[str] = None,
    sort: Optional[str] = None,
    order: Optional[str] = None,
    limit: Optional[int] = 10,
    params: Optional[Dict[str, Any]] = None,
) -> str:
    """
    Retrieve a comprehensive list of ticker symbols supported by Polygon.io across various asset classes.
    Each ticker entry provides essential details such as symbol, name, market, currency, and active status.

    Reference: https://polygon.io/docs/rest/stocks/tickers/all-tickers

    Essential for asset discovery, data integration, filtering/selection, and application development.
    Query tickers across stocks, options, indices, forex, and crypto markets with advanced filtering.

    Parameters:
    - ticker: Specific ticker symbol to query (defaults to all tickers)
    - type: Filter by ticker type (CS=Common Stock, ETF, ADRC, etc. - see Ticker Types API)
    - market: Filter by market type (stocks, crypto, fx, otc, indices)
    - exchange: Primary exchange MIC code (e.g., XNYS, XNAS) per ISO 10383
    - cusip: CUSIP code of the asset (note: not returned in response due to legal reasons)
    - cik: CIK code from SEC EDGAR system
    - date: Point in time to retrieve tickers available on that date (defaults to most recent)
    - search: Search terms within ticker and/or company name
    - active: Filter for actively traded tickers (default: None, which returns all)
    - ticker_gte: Ticker greater than or equal to (lexicographic range)
    - ticker_gt: Ticker greater than (lexicographic range)
    - ticker_lte: Ticker less than or equal to (lexicographic range)
    - ticker_lt: Ticker less than (lexicographic range)
    - sort: Field to sort by (e.g., "ticker", "name")
    - order: Sort order ("asc" or "desc")
    - limit: Number of results to return (default: 10, max: 1000)
    - params: Additional query parameters

    Example: list_tickers(market="stocks", active=True, limit=100)
             gets first 100 active stock tickers
    Example: list_tickers(search="Apple", market="stocks")
             searches for Apple-related stock tickers
    Example: list_tickers(type="ETF", active=True, limit=50)
             gets first 50 active ETFs
    Example: list_tickers(ticker_gte="A", ticker_lt="B", market="stocks")
             gets all stock tickers starting with 'A'
    Example: list_tickers(exchange="XNAS", market="stocks", limit=100)
             gets first 100 stocks from NASDAQ
    Example: list_tickers(cik="0001090872")
             gets tickers for specific CIK (Agilent Technologies)

    Response includes:
    - ticker: Exchange symbol
    - name: Asset name (company name for stocks, currency pair for forex/crypto)
    - market: Market type (stocks, crypto, fx, otc, indices)
    - type: Asset type (CS, ETF, ADRC, etc.)
    - active: Whether actively traded (false means delisted)
    - primary_exchange: ISO code of primary listing exchange
    - currency_symbol/currency_name: Trading currency (ISO 4217)
    - base_currency_symbol/base_currency_name: Pricing currency (for forex/crypto)
    - locale: Asset locale (us, global)
    - cik: Central Index Key for SEC filings
    - composite_figi/share_class_figi: OpenFIGI identifiers
    - last_updated_utc: Data freshness timestamp
    - delisted_utc: Last trading date (if delisted)

    Note: Ticker listing considerations:
    - Query by CUSIP is supported but CUSIP not returned in response (legal restrictions)
    - Default limit is 10, max is 1000 per request
    - Use pagination (next_url) for large result sets
    - Active=true filters to currently trading assets only
    - Date parameter allows historical ticker lookups
    - Ticker range queries enable efficient alphabetical scanning
    - Search matches both ticker symbol and company name
    - Exchange parameter uses Market Identifier Code (MIC) standard
    - Type values vary by market (use Ticker Types API for complete list)

    Use case: Building a stock screener - query all active US stocks on NASDAQ exchange,
    then filter by specific criteria like type (CS for common stock, ETF for funds), and
    use ticker range queries to paginate through results alphabetically for efficient data loading.
    """
    try:
        results = polygon_client.list_tickers(
            ticker=ticker,
            type=type,
            market=market,
            exchange=exchange,
            cusip=cusip,
            cik=cik,
            date=date,
            search=search,
            active=active,
            sort=sort,
            order=order,
            limit=limit,
            params={
                **(params or {}),
                **{
                    k: v
                    for k, v in {
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
async def get_all_tickers(
    market: Optional[str] = None,
    type: Optional[str] = None,
    active: Optional[bool] = True,
    limit: Optional[int] = 100,
    sort: Optional[str] = "ticker",
    order: Optional[str] = "asc",
    params: Optional[Dict[str, Any]] = None,
) -> str:
    """
    Get all ticker symbols with optional filtering by market type and active status.
    This is a simplified interface to the Tickers endpoint (https://polygon.io/docs/rest/stocks/tickers/all-tickers).

    Common parameters:
    - market: Filter by market type (stocks, crypto, fx, otc, indices)
    - type: Filter by security type (CS=Common Stock, ETF, etc.)
    - active: Include only active tickers (default: True)
    - limit: Number of results to return (default: 100)
    """
    try:
        results = polygon_client.list_tickers(
            market=market,
            type=type,
            active=active,
            sort=sort,
            order=order,
            limit=limit,
            params=params,
            raw=True,
        )

        return json_to_csv(results.data.decode("utf-8"))
    except Exception as e:
        return f"Error: {e}"


@poly_mcp.tool(annotations=ToolAnnotations(readOnlyHint=True))
async def get_ticker_details(
    ticker: str,
    date: Optional[Union[str, datetime, date]] = None,
    params: Optional[Dict[str, Any]] = None,
) -> str:
    """
    Retrieve comprehensive details for a single ticker supported by Polygon.io. This endpoint offers
    a deep look into a company's fundamental attributes, including primary exchange, standardized
    identifiers (CIK, FIGI), market cap, industry classification, and key dates. Also provides
    branding assets (logos, icons) for visual identification.

    Reference: https://polygon.io/docs/rest/stocks/tickers/ticker-overview

    Essential for company research, data integration, application enhancement, due diligence,
    and compliance. Provides rich fundamental data and visual assets in a single request.

    Parameters:
    - ticker: Case-sensitive ticker symbol (e.g., "AAPL", "MSFT", "GOOGL")
    - date: Point in time to get ticker information (YYYY-MM-DD, defaults to most recent)
           When querying SEC filings, this date is compared with the period of report date.
           For example, an SEC filing submitted 2019-07-31 with period ending 2019-06-29
           would be returned when querying date=2019-06-29
    - params: Additional query parameters

    Example: get_ticker_details("AAPL")
             gets comprehensive Apple Inc. details with latest data
    Example: get_ticker_details("MSFT", date="2020-01-15")
             gets Microsoft details as of January 15, 2020
    Example: get_ticker_details("GOOGL")
             gets Alphabet/Google details with branding assets
    Example: get_ticker_details("TSLA")
             gets Tesla details including market cap, employees, SIC code

    Response includes:
    - ticker: Exchange symbol (ticker root and suffix if applicable)
    - name: Company registered name
    - market: Market type (stocks, crypto, fx, otc, indices)
    - type: Asset type (CS=Common Stock, ETF, ADRC, etc.)
    - active: Whether actively traded (false means delisted)
    - locale: Asset locale (us, global)
    - primary_exchange: ISO code of primary listing exchange
    - currency_name: Trading currency
    - description: Detailed company description and business overview
    - homepage_url: Company website
    - list_date: Date symbol was first publicly listed (YYYY-MM-DD)
    - delisted_utc: Last trading date (if delisted)
    - address: Headquarters address (address1, city, state, postal_code)
    - phone_number: Company contact phone
    - total_employees: Approximate employee count
    - market_cap: Most recent close price × weighted outstanding shares
    - share_class_shares_outstanding: Outstanding shares for this class
    - weighted_shares_outstanding: Total shares assuming all classes converted
    - round_lot: Standard trading lot size
    - cik: Central Index Key for SEC filings
    - composite_figi: Composite OpenFIGI identifier
    - share_class_figi: Share class OpenFIGI identifier
    - sic_code: Standard Industrial Classification code
    - sic_description: SIC code description
    - branding: Visual assets (logo_url, icon_url) for UI integration

    Note: Ticker details considerations:
    - Date parameter enables historical company data lookups
    - SEC filing data aligned with period of report date, not submission date
    - Branding URLs provide direct access to company logos and icons
    - Market cap calculated as close price × weighted shares outstanding
    - SIC codes classify companies by industry (see SEC's SIC Code List)
    - FIGI codes provide global financial instrument identification
    - CIK enables SEC EDGAR filing lookups
    - Weighted shares outstanding accounts for all share class conversions
    - Description field provides comprehensive business overview
    - Address and phone enable direct company contact

    Use case: Building a stock research dashboard - fetch ticker details to display company
    overview with logo, business description, key metrics (market cap, employees), industry
    classification (SIC), and contact information, all enriched with branding assets for
    professional presentation.
    """
    try:
        results = polygon_client.get_ticker_details(
            ticker=ticker, date=date, params=params, raw=True
        )

        # Parse the response and extract the results object
        import json

        data = json.loads(results.data.decode("utf-8"))
        if "results" in data:
            # Wrap the results object in an array for CSV formatting
            formatted_data = {"results": [data["results"]]}
            return json_to_csv(formatted_data)
        return json_to_csv(results.data.decode("utf-8"))
    except Exception as e:
        return f"Error: {e}"


@poly_mcp.tool(annotations=ToolAnnotations(readOnlyHint=True))
async def get_related_companies(
    ticker: str,
    params: Optional[Dict[str, Any]] = None,
) -> str:
    """
    Retrieve a list of tickers related to a specified ticker, identified through analysis of news
    coverage and returns data. This endpoint helps users discover peers, competitors, or thematically
    similar companies, aiding in comparative analysis, portfolio diversification, and market research.

    Reference: https://polygon.io/docs/rest/stocks/tickers/related-tickers

    Essential for peer identification, comparative analysis, portfolio diversification, and market
    research. Discovers related companies based on news correlation and return patterns.

    Parameters:
    - ticker: The ticker symbol to search for related companies (e.g., "AAPL", "MSFT", "TSLA")
    - params: Additional query parameters

    Example: get_related_companies("AAPL")
             gets companies related to Apple (may include MSFT, GOOGL, AMZN, META, etc.)
    Example: get_related_companies("TSLA")
             gets companies related to Tesla (may include RIVN, LCID, F, GM, etc.)
    Example: get_related_companies("JPM")
             gets companies related to JPMorgan Chase (other major banks and financial institutions)
    Example: get_related_companies("NVDA")
             gets companies related to NVIDIA (other semiconductor and AI chip companies)

    Response includes:
    - ticker: Related ticker symbol
    - Results array contains list of related company tickers

    Note: Related companies considerations:
    - Relationships identified through news coverage analysis
    - Returns data correlation also contributes to relatedness
    - Typically returns companies in same sector or industry
    - May include direct competitors and peer companies
    - Useful for discovering investment alternatives
    - Helps identify stocks that move together
    - Relationship strength varies (no explicit scoring provided)
    - Results based on recent news and market behavior
    - Number of results varies by ticker
    - Useful for building watch lists of similar companies
    - Aids in sector rotation strategies
    - Helps identify portfolio concentration risks

    Use case: Building a comparative analysis dashboard - after researching Apple, use this endpoint
    to discover related tech companies (Microsoft, Google, Amazon, Meta, etc.), then fetch financial
    metrics and performance data for all related tickers to compare valuations, growth rates, and
    market positioning across the entire peer group.
    """
    try:
        results = polygon_client.get_related_companies(
            ticker=ticker, params=params, raw=True
        )

        return json_to_csv(results.data.decode("utf-8"))
    except Exception as e:
        return f"Error: {e}"


@poly_mcp.tool(annotations=ToolAnnotations(readOnlyHint=True))
async def list_ticker_news(
    ticker: Optional[str] = None,
    published_utc: Optional[Union[str, datetime, date]] = None,
    ticker_gte: Optional[str] = None,
    ticker_gt: Optional[str] = None,
    ticker_lte: Optional[str] = None,
    ticker_lt: Optional[str] = None,
    published_utc_gte: Optional[Union[str, datetime, date]] = None,
    published_utc_gt: Optional[Union[str, datetime, date]] = None,
    published_utc_lte: Optional[Union[str, datetime, date]] = None,
    published_utc_lt: Optional[Union[str, datetime, date]] = None,
    limit: Optional[int] = 10,
    sort: Optional[str] = None,
    order: Optional[str] = None,
    params: Optional[Dict[str, Any]] = None,
) -> str:
    """
    Retrieve the most recent news articles related to stocks, featuring summaries, source details,
    and AI-powered sentiment analysis for informed decision-making.

    This endpoint consolidates relevant financial news in one place, extracting associated tickers,
    assigning sentiment with reasoning, and providing direct links to original sources. By incorporating
    publisher information, article metadata, and sentiment insights, users can quickly gauge market
    sentiment, stay informed on company developments, and integrate news intelligence into trading
    or research workflows.

    Official API Documentation:
    https://polygon.io/docs/rest/stocks/reference/news

    Parameters
    ----------
    ticker : str, optional
        Case-sensitive ticker symbol to filter news.
        Example: "AAPL" for Apple Inc.

    published_utc : str or date, optional
        Return results published on, before, or after this date/time.
        Format: RFC3339 (YYYY-MM-DDTHH:MM:SSZ) or simple date (YYYY-MM-DD)
        Example: "2024-06-24" or "2024-06-24T18:33:53Z"

    ticker_gte : str, optional
        Filter ticker greater than or equal to value (lexicographic).

    ticker_gt : str, optional
        Filter ticker greater than value (lexicographic).

    ticker_lte : str, optional
        Filter ticker less than or equal to value (lexicographic).

    ticker_lt : str, optional
        Filter ticker less than value (lexicographic).

    published_utc_gte : str or date, optional
        Filter published_utc greater than or equal to value.
        Format: RFC3339 or YYYY-MM-DD

    published_utc_gt : str or date, optional
        Filter published_utc greater than value.
        Format: RFC3339 or YYYY-MM-DD

    published_utc_lte : str or date, optional
        Filter published_utc less than or equal to value.
        Format: RFC3339 or YYYY-MM-DD

    published_utc_lt : str or date, optional
        Filter published_utc less than value.
        Format: RFC3339 or YYYY-MM-DD

    limit : int, optional
        Maximum number of results to return.
        Default: 10, Maximum: 1000

    sort : str, optional
        Sort field for ordering results.
        Common: "published_utc"

    order : str, optional
        Order results based on sort field.
        Options: "asc" or "desc"

    params : dict, optional
        Additional query parameters for advanced filtering.

    Returns
    -------
    str
        CSV-formatted string containing news articles with the following fields:

        Article Metadata:
        - id: Unique identifier for the article
        - title: Article title
        - author: Article author
        - published_utc: UTC publication date/time (RFC3339 format)
        - description: Article summary/description
        - keywords: Associated keywords (array)

        URLs:
        - article_url: Link to the full news article
        - amp_url: Mobile-friendly AMP URL
        - image_url: Article's featured image URL

        Publisher Information:
        - publisher_name: Publisher name (e.g., "Investing.com")
        - publisher_homepage_url: Publisher's homepage
        - publisher_logo_url: Publisher's logo URL
        - publisher_favicon_url: Publisher's favicon URL

        Tickers:
        - tickers: Ticker symbols mentioned in article (array)

        Sentiment Insights (per ticker):
        - insights_ticker: Ticker the insight applies to
        - insights_sentiment: Sentiment classification (positive, negative, neutral)
        - insights_sentiment_reasoning: AI-generated explanation for sentiment

    Understanding News Sentiment Analysis
    -------------------------------------
    Each article includes AI-powered sentiment analysis with reasoning:

    1. **Sentiment Classifications**:
       - **Positive**: Bullish news likely to support price increases
         * Earnings beats, positive guidance, new product launches
         * Strategic partnerships, market share gains
         * Analyst upgrades, institutional buying
       - **Negative**: Bearish news likely to pressure prices
         * Earnings misses, lowered guidance, product recalls
         * Legal issues, regulatory challenges
         * Analyst downgrades, executive departures
       - **Neutral**: Informational without clear directional bias
         * Routine announcements, general market commentary
         * Balanced analysis pieces

    2. **Sentiment Reasoning**:
       - AI explains why the sentiment was assigned
       - Highlights key facts driving the sentiment
       - Ticker-specific analysis when multiple tickers mentioned
       - Helps validate automated sentiment classification

    3. **Multi-Ticker Articles**:
       - Single article may mention multiple companies
       - Each ticker gets individual sentiment + reasoning
       - Allows sector-wide or comparative analysis
       - Example: Fed rate decision affects multiple financial stocks

    4. **Publisher Context**:
       - Source credibility varies by publisher
       - Major outlets (Bloomberg, Reuters, WSJ) vs aggregators
       - Publisher logo/favicon helps identify source quality
       - Homepage URL enables source verification

    Use Cases
    ---------
    1. **Market Sentiment Analysis**: Aggregate news sentiment across tickers or sectors
       to gauge overall market mood and identify sentiment shifts.

    2. **Investment Research**: Stay informed on company-specific developments, earnings,
       product launches, and strategic initiatives affecting investment decisions.

    3. **Automated Monitoring**: Build news alerts and automated workflows to track
       specific tickers, keywords, or sentiment patterns for timely decision-making.

    4. **Portfolio Strategy Refinement**: Incorporate news flow and sentiment into trading
       strategies, risk management, and position sizing decisions.

    Examples
    --------
    Example 1: Get latest Apple news with sentiment
        list_ticker_news(
            ticker="AAPL",
            limit=20,
            order="desc"
        )

        Returns 20 most recent Apple articles with AI sentiment analysis.

    Example 2: Get Tesla news for specific date range
        list_ticker_news(
            ticker="TSLA",
            published_utc_gte="2025-03-01",
            published_utc_lte="2025-03-31",
            order="desc"
        )

        Returns all Tesla news published in March 2025.

    Example 3: Monitor recent market news across all stocks
        list_ticker_news(
            limit=50,
            order="desc"
        )

        Returns 50 most recent market news articles without ticker filter.

    Example 4: Track news since specific date for NVIDIA
        list_ticker_news(
            ticker="NVDA",
            published_utc_gte="2025-01-01",
            order="asc",
            limit=100
        )

        Returns NVIDIA news since start of 2025 in chronological order.

    Example 5: Get latest news for multiple tickers (using ticker range)
        list_ticker_news(
            ticker_gte="AAPL",
            ticker_lte="MSFT",
            limit=100,
            order="desc"
        )

        Returns recent news for tickers alphabetically between AAPL and MSFT.

    Notes
    -----
    - **Case-Sensitive Ticker**: The ticker parameter is case-sensitive. Use uppercase
      ticker symbols (e.g., "AAPL" not "aapl") for reliable filtering.

    - **AI Sentiment**: Sentiment analysis is AI-generated and should be validated:
      * Use sentiment_reasoning to understand the AI's logic
      * Cross-reference with article description and title
      * Consider publisher credibility and source
      * Not a substitute for human analysis on critical decisions

    - **Publication Timestamps**: published_utc uses RFC3339 format with UTC timezone:
      * Full format: "2024-06-24T18:33:53Z"
      * Simple format: "2024-06-24" (midnight UTC assumed)
      * All times in UTC, convert to local as needed
      * Useful for precise event timing and chronological analysis

    - **Multi-Ticker Articles**: A single article may reference multiple companies:
      * Each ticker gets separate sentiment analysis in insights array
      * Tickers array lists all mentioned symbols
      * Useful for sector analysis and correlation studies
      * Example: Federal Reserve news affects multiple financial stocks differently

    - **Publisher Information**: Evaluate source credibility:
      * Major financial outlets: Bloomberg, Reuters, WSJ, CNBC
      * Aggregators: Investing.com, Yahoo Finance, MarketWatch
      * Specialist sources: Benzinga, Seeking Alpha
      * Check publisher homepage_url for legitimacy
      * Logo/favicon helps quick visual source identification

    - **Keyword Analysis**: keywords array provides topic categorization:
      * Varies by publisher (not standardized)
      * Useful for topic clustering and trend analysis
      * Examples: "earnings", "Federal Reserve", "AI technology"
      * Can build custom alerts based on keyword patterns

    - **Image URLs**: image_url provides article's featured image:
      * Useful for visual content in applications
      * May be null if article has no featured image
      * Hosted on publisher's CDN or Polygon proxy
      * Check URL availability before displaying

    - **AMP URLs**: amp_url provides mobile-friendly version:
      * Accelerated Mobile Pages for fast mobile loading
      * Useful for mobile app integrations
      * May not be available for all articles
      * Falls back to article_url if AMP not available

    - **Article URLs**: Direct links to original source:
      * Always verify article_url accessibility
      * Some articles may be paywalled
      * URLs may expire or change over time
      * Respect publisher's terms of service

    - **Sorting and Ordering**: Customize result ordering:
      * sort="published_utc", order="desc" → Latest news first (default)
      * sort="published_utc", order="asc" → Oldest news first (chronological)
      * Useful for historical analysis vs real-time monitoring

    - **Limit Constraints**: Balance between coverage and performance:
      * Default: 10 articles (quick overview)
      * Max: 1000 articles (comprehensive analysis)
      * Use pagination (next_url) for larger datasets
      * Higher limits increase response time

    - **Date Range Filtering**: Effective strategies:
      * Recent news: published_utc_gte with recent date
      * Specific event: narrow date range around event
      * Historical analysis: broader date ranges with pagination
      * Real-time monitoring: poll with recent timestamp threshold

    - **Sentiment Aggregation**: Build sentiment scores:
      * Count positive/negative/neutral articles per ticker
      * Weight by publisher credibility or recency
      * Track sentiment shifts over time
      * Compare sentiment to price movements

    - **News Impact Analysis**: Correlate news with market data:
      * Match published_utc with price/volume data
      * Measure price reaction to positive/negative news
      * Identify sentiment-price divergences
      * Build event-driven trading strategies

    - **Automated Workflows**: Integration patterns:
      * Poll endpoint periodically for new articles
      * Filter by sentiment for automated alerts
      * Store articles in database for historical analysis
      * Trigger trades or notifications based on keywords/sentiment

    - **Data Freshness**: News is updated continuously:
      * Articles appear shortly after publication
      * May have slight delay vs real-time news feeds
      * Check published_utc for article age
      * More frequent polling for time-sensitive use cases

    - **Pagination**: For extensive news searches:
      * Use next_url from response to fetch more results
      * Maintains query parameters across pages
      * Iterate until next_url is null
      * Be mindful of rate limits when paginating

    - **Rate Limits**: Respect API usage limits:
      * Check your plan's rate limit (requests per minute)
      * Implement exponential backoff on errors
      * Cache results when possible
      * Use date filtering to avoid redundant queries

    - **Quality Filtering**: Build robust news pipelines:
      * Filter by publisher credibility (whitelist known sources)
      * Validate sentiment_reasoning makes sense
      * Remove duplicates (same story from multiple sources)
      * Check article_url accessibility
      * Monitor for spam or low-quality sources

    - **Language**: Articles are primarily English:
      * Some international sources may include non-English content
      * Check description/title for language identification
      * Keywords may include non-English terms
      * Publisher name can indicate language (e.g., "Le Figaro")

    - **Historical Data**: News archive depth varies:
      * Recent news (days/weeks) most reliable
      * Historical news (months/years) availability varies
      * Some publishers may remove old articles
      * Plan accordingly for backtesting strategies

    - **Ticker Association**: How tickers are extracted:
      * Automated extraction from article content
      * May include mentioned companies beyond primary subject
      * Can result in false positives (unrelated mentions)
      * Validate ticker relevance with title/description
      * Use ticker_gte/ticker_lte for range filtering

    - **Insights Structure**: Understanding the insights array:
      * Array of objects, one per ticker mentioned
      * Each object: {ticker, sentiment, sentiment_reasoning}
      * Same ticker may appear multiple times if mentioned in different contexts
      * Use as starting point, not definitive analysis
    """
    try:
        results = polygon_client.list_ticker_news(
            ticker=ticker,
            published_utc=published_utc,
            limit=limit,
            sort=sort,
            order=order,
            params={
                **(params or {}),
                **{
                    k: v
                    for k, v in {
                        "ticker.gte": ticker_gte,
                        "ticker.gt": ticker_gt,
                        "ticker.lte": ticker_lte,
                        "ticker.lt": ticker_lt,
                        "published_utc.gte": published_utc_gte,
                        "published_utc.gt": published_utc_gt,
                        "published_utc.lte": published_utc_lte,
                        "published_utc.lt": published_utc_lt,
                    }.items()
                    if v is not None
                },
            },
            raw=True,
        )

        return json_to_csv(results.data.decode("utf-8"))
    except Exception as e:
        return f"Error: {e}"
