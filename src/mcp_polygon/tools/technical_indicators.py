"""Auto-generated tool definitions."""

from typing import Optional, Any, Dict, Union
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
    fetch_all: Optional[bool] = True,
    timestamp_gte: Optional[Union[str, int, datetime, date]] = None,
    timestamp_gt: Optional[Union[str, int, datetime, date]] = None,
    timestamp_lte: Optional[Union[str, int, datetime, date]] = None,
    timestamp_lt: Optional[Union[str, int, datetime, date]] = None,
    params: Optional[Dict[str, Any]] = None,
) -> str:
    """
    Calculate Simple Moving Average (SMA) to smooth price fluctuations and identify trends.
    Works with both stocks and options.

    Reference: https://polygon.io/docs/rest/stocks/technical-indicators/simple-moving-average

    Parameters:
    - ticker: Symbol (e.g., "AAPL" for stocks, "O:SPY241220P00720000" for options)
    - window: Period for calculation (default: 50)
    - timespan: Aggregation period ("day", "hour", "minute")
    - series_type: Price type ("close", "open", "high", "low")
    - limit: Number of results per request (default: 10, max: 5000)
    - fetch_all: If True (recommended), fetch maximum data (5000 points) and cache to disk for DuckDB queries (default: True)

    RECOMMENDED: Always use fetch_all=True to cache complete indicator data locally for efficient DuckDB analysis.

    Example: get_sma("AAPL", window=50, timespan="day", fetch_all=True)
    Example: get_sma("MSFT", window=200, fetch_all=True)
    Example: get_sma("O:SPY241220P00720000", window=20, fetch_all=True)

    Returns: timestamp, value. Common windows: 50-day, 200-day. Golden Cross (50>200)=bullish, Death Cross (50<200)=bearish.
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

        # Use maximum limit when fetch_all is True
        actual_limit = 5000 if fetch_all else limit

        # Build kwargs conditionally to avoid passing empty params
        kwargs = {
            "ticker": ticker,
            "timespan": timespan,
            "adjusted": adjusted,
            "window": window,
            "series_type": series_type,
            "expand_underlying": expand_underlying,
            "order": order,
            "limit": actual_limit,
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
            csv_data = json_to_csv(formatted_data)
        else:
            # Convert to CSV
            csv_data = json_to_csv(results.data.decode("utf-8"))

        # Process with intelligent caching
        return await process_tool_response(
            tool_name="get_sma",
            params={
                "ticker": ticker,
                "window": window,
                "timespan": timespan,
                "limit": actual_limit,
                "fetch_all": fetch_all,
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
    fetch_all: Optional[bool] = True,
    timestamp_gte: Optional[Union[str, int, datetime, date]] = None,
    timestamp_gt: Optional[Union[str, int, datetime, date]] = None,
    timestamp_lte: Optional[Union[str, int, datetime, date]] = None,
    timestamp_lt: Optional[Union[str, int, datetime, date]] = None,
    params: Optional[Dict[str, Any]] = None,
) -> str:
    """
    Calculate Exponential Moving Average (EMA) with greater weight on recent prices for faster trend detection.
    Works with both stocks and options.

    Reference: https://polygon.io/docs/rest/stocks/technical-indicators/exponential-moving-average

    Parameters:
    - ticker: Symbol (e.g., "AAPL" for stocks, "O:SPY241220P00720000" for options)
    - window: Period for calculation (default: 50)
    - timespan: Aggregation period ("day", "hour", "minute")
    - series_type: Price type ("close", "open", "high", "low")
    - limit: Number of results per request (default: 10, max: 5000)
    - fetch_all: If True (recommended), fetch maximum data (5000 points) and cache to disk for DuckDB queries (default: True)

    RECOMMENDED: Always use fetch_all=True to cache complete indicator data locally for efficient DuckDB analysis.

    Example: get_ema("AAPL", window=12, timespan="day", fetch_all=True)
    Example: get_ema("MSFT", window=26, fetch_all=True)
    Example: get_ema("O:SPY241220P00720000", window=12, fetch_all=True)

    Returns: timestamp, value. Common windows: 12, 26 (MACD components), 50, 200. More responsive than SMA.
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

        # Use maximum limit when fetch_all is True
        actual_limit = 5000 if fetch_all else limit

        # Build kwargs conditionally to avoid passing empty params
        kwargs = {
            "ticker": ticker,
            "timespan": timespan,
            "adjusted": adjusted,
            "window": window,
            "series_type": series_type,
            "expand_underlying": expand_underlying,
            "order": order,
            "limit": actual_limit,
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
            csv_data = json_to_csv(formatted_data)
        else:
            # Convert to CSV
            csv_data = json_to_csv(results.data.decode("utf-8"))

        # Process with intelligent caching
        return await process_tool_response(
            tool_name="get_ema",
            params={
                "ticker": ticker,
                "window": window,
                "timespan": timespan,
                "limit": actual_limit,
                "fetch_all": fetch_all,
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
    fetch_all: Optional[bool] = True,
    timestamp_gte: Optional[Union[str, int, datetime, date]] = None,
    timestamp_gt: Optional[Union[str, int, datetime, date]] = None,
    timestamp_lte: Optional[Union[str, int, datetime, date]] = None,
    timestamp_lt: Optional[Union[str, int, datetime, date]] = None,
    params: Optional[Dict[str, Any]] = None,
) -> str:
    """
    Calculate MACD (Moving Average Convergence/Divergence) to identify momentum and trend changes.
    Works with both stocks and options.

    Reference: https://polygon.io/docs/rest/stocks/technical-indicators/moving-average-convergence-divergence

    Parameters:
    - ticker: Symbol (e.g., "AAPL" for stocks, "O:SPY241220P00720000" for options)
    - short_window: Fast EMA period (default: 12)
    - long_window: Slow EMA period (default: 26)
    - signal_window: Signal line period (default: 9)
    - timespan: Aggregation period ("day", "hour", "minute")
    - limit: Number of results per request (default: 10, max: 5000)
    - fetch_all: If True (recommended), fetch maximum data (5000 points) and cache to disk for DuckDB queries (default: True)

    RECOMMENDED: Always use fetch_all=True to cache complete indicator data locally for efficient DuckDB analysis.

    Example: get_macd("AAPL", fetch_all=True)  # Standard 12,26,9
    Example: get_macd("MSFT", short_window=8, long_window=17, signal_window=9, fetch_all=True)
    Example: get_macd("O:SPY241220P00720000", fetch_all=True)

    Returns: timestamp, value (MACD line), signal (signal line), histogram. Bullish: MACD crosses above signal.
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

        # Use maximum limit when fetch_all is True
        actual_limit = 5000 if fetch_all else limit

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
            "limit": actual_limit,
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
            csv_data = json_to_csv(formatted_data)
        else:
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
                "limit": actual_limit,
                "fetch_all": fetch_all,
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
    fetch_all: Optional[bool] = True,
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
    - limit: Number of results per request (default: 10, max: 5000)
    - fetch_all: If True (recommended), fetch maximum data (5000 points) and cache to disk for DuckDB queries (default: True)

    RECOMMENDED: Always use fetch_all=True to cache complete indicator data locally for efficient DuckDB analysis.

    Example: get_rsi(ticker="TSLA", window=14, fetch_all=True)
    Example: get_rsi(ticker="O:NVDA251219C00800000", window=14, timespan="day", fetch_all=True)

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

        # Use maximum limit when fetch_all is True
        actual_limit = 5000 if fetch_all else limit

        # Build kwargs conditionally to avoid passing empty params
        kwargs = {
            "ticker": ticker,
            "timespan": timespan,
            "adjusted": adjusted,
            "window": window,
            "series_type": series_type,
            "expand_underlying": expand_underlying,
            "order": order,
            "limit": actual_limit,
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
            csv_data = json_to_csv(formatted_data)
        else:
            # Convert to CSV
            csv_data = json_to_csv(results.data.decode("utf-8"))

        # Process with intelligent caching
        return await process_tool_response(
            tool_name="get_rsi",
            params={
                "ticker": ticker,
                "window": window,
                "timespan": timespan,
                "limit": actual_limit,
                "fetch_all": fetch_all,
            },
            csv_data=csv_data,
        )
    except Exception as e:
        return f"Error: {e}"
