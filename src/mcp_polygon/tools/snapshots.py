"""Auto-generated tool definitions."""
from typing import Optional, Any, Dict, Union, List
from mcp.types import ToolAnnotations
from datetime import datetime, date
from ..clients import poly_mcp, polygon_client
from ..formatters import json_to_csv
import json



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

