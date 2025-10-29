"""Auto-generated tool definitions."""

from typing import Optional, Any, Dict, Union, List
from mcp.types import ToolAnnotations
from datetime import datetime, date
from ..clients import poly_mcp, polygon_client
from ..formatters import json_to_csv
from ..tool_integration import process_tool_response





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
        # Convert to CSV
        csv_data = json_to_csv(results.data.decode("utf-8"))

        # Process with intelligent caching
        return await process_tool_response(
            tool_name="get_sma",
            params={
                "ticker": ticker,
                "window": window,
                "timespan": timespan,
                "limit": limit,
            },
            csv_data=csv_data,
        )
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
        # Convert to CSV
        csv_data = json_to_csv(results.data.decode("utf-8"))

        # Process with intelligent caching
        return await process_tool_response(
            tool_name="get_ema",
            params={
                "ticker": ticker,
                "window": window,
                "timespan": timespan,
                "limit": limit,
            },
            csv_data=csv_data,
        )
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
        # Convert to CSV
        csv_data = json_to_csv(results.data.decode("utf-8"))

        # Process with intelligent caching
        return await process_tool_response(
            tool_name="get_macd",
            params={
                "ticker": ticker,
                "short_window": short_window,
                "long_window": long_window,
                "signal_window": signal_window,
                "limit": limit,
            },
            csv_data=csv_data,
        )
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
    Calculate Relative Strength Index (RSI) to identify overbought/oversold conditions.

    Reference: https://polygon.io/docs/rest/stocks/technical-indicators/relative-strength-index

    Parameters:
    - ticker: Symbol (e.g., "AAPL" for stocks, "O:SPY241220P00720000" for options)
    - window: Period for calculation (default: 14)
    - timespan: Aggregation period ("day", "hour", "minute")
    - limit: Number of results (default: 10)

    Example: get_rsi(ticker="TSLA", window=14, limit=100)
    Example: get_rsi(ticker="O:NVDA251219C00800000", window=14, timespan="day")

    Returns: timestamp, value (RSI 0-100, <30 oversold, >70 overbought)
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
        # Convert to CSV
        csv_data = json_to_csv(results.data.decode("utf-8"))

        # Process with intelligent caching
        return await process_tool_response(
            tool_name="get_rsi",
            params={
                "ticker": ticker,
                "window": window,
                "timespan": timespan,
                "limit": limit,
            },
            csv_data=csv_data,
        )
    except Exception as e:
        return f"Error: {e}"
