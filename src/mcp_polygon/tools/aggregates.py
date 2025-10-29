"""Auto-generated tool definitions."""

from typing import Optional, Any, Dict, Union
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
