"""Auto-generated tool definitions."""

from typing import Optional, Any, Dict, Union
from mcp.types import ToolAnnotations
from datetime import datetime, date
from ..clients import poly_mcp, polygon_client
from ..formatters import json_to_csv
from ..tool_integration import process_tool_response, create_batch_writer
from ..parallel_fetcher import PolygonParallelFetcher
from ..utils import build_params


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
    fetch_all: Optional[bool] = True,
    params: Optional[Dict[str, Any]] = None,
) -> str:
    """
    Get historical OHLC (open, high, low, close) and volume data for a stock over a custom date range and interval.

    Parameters:
    - ticker: Stock symbol (e.g., "AAPL", "MSFT")
    - multiplier: Timespan multiplier (e.g., 1, 5, 15)
    - timespan: Time window (minute, hour, day, week, month, quarter, year)
    - from_: Start date (YYYY-MM-DD or timestamp)
    - to: End date (YYYY-MM-DD or timestamp)
    - adjusted: Adjust for splits (default: True)
    - sort: Sort order - "asc" or "desc" (default: asc)
    - limit: Max results per page (default: 10, max: 50000)
    - fetch_all: If True (recommended), fetch ALL data and cache to disk for DuckDB queries (default: True)

    RECOMMENDED: Always use fetch_all=True to cache complete OHLC data locally for efficient DuckDB analysis.

    Returns: OHLC data with columns: o, h, l, c, v (volume), vw (VWAP), t (timestamp), n (trades)

    Examples:
    - Daily: get_aggs("AAPL", 1, "day", "2023-01-01", "2023-01-31", fetch_all=True)
    - Intraday: get_aggs("MSFT", 5, "minute", "2023-06-15", "2023-06-15", fetch_all=True)
    - Weekly: get_aggs("SPY", 1, "week", "2022-01-01", "2022-12-31", fetch_all=True)

    Note: Covers pre-market, regular, and after-hours sessions (ET). Use higher limits for longer ranges.
    """
    try:
        tool_params = build_params(
            ticker=ticker,
            multiplier=multiplier,
            timespan=timespan,
            from_=str(from_),
            to=str(to),
            limit=limit,
            fetch_all=fetch_all,
        )

        if fetch_all:
            # Use batch writing for memory efficiency
            batch_callback, finalize = create_batch_writer("get_aggs", tool_params)

            if batch_callback:
                # Streaming mode - write batches to disk incrementally
                fetcher = PolygonParallelFetcher(polygon_client, num_workers=5)
                await fetcher.fetch_all(
                    method_name="get_aggs",
                    batch_callback=batch_callback,
                    ticker=ticker,
                    multiplier=multiplier,
                    timespan=timespan,
                    from_=from_,
                    to=to,
                    adjusted=adjusted,
                    sort=sort,
                    limit=limit,
                    params=params,
                )
                # Finalize and return cache metadata
                return await finalize()
            else:
                # Memory mode (fallback if batch writing not available)
                fetcher = PolygonParallelFetcher(polygon_client, num_workers=5)
                aggs_list = await fetcher.fetch_all(
                    method_name="get_aggs",
                    ticker=ticker,
                    multiplier=multiplier,
                    timespan=timespan,
                    from_=from_,
                    to=to,
                    adjusted=adjusted,
                    sort=sort,
                    limit=limit,
                    params=params,
                )
                csv_data = json_to_csv({"results": aggs_list})
                return await process_tool_response("get_aggs", tool_params, csv_data)
        else:
            # Single page approach
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

            import json

            data = json.loads(results.data.decode("utf-8"))
            aggs_list = data.get("results", [])

            # Create data structure for JSON to CSV conversion
            data = {"results": aggs_list, "status": "OK"}

            # Convert to CSV
            csv_data = json_to_csv(data)

            # Process with intelligent caching
            return await process_tool_response("get_aggs", tool_params, csv_data)
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
    fetch_all: Optional[bool] = True,
    params: Optional[Dict[str, Any]] = None,
) -> str:
    """
    Paginate through aggregate bars (OHLC) for a ticker. Similar to get_aggs but optimized for large datasets.

    Parameters:
    - ticker: Stock symbol (e.g., "AAPL", "MSFT")
    - multiplier: Timespan multiplier (e.g., 1, 5, 15)
    - timespan: Time window (minute, hour, day, week, month, quarter, year)
    - from_: Start date (YYYY-MM-DD or timestamp)
    - to: End date (YYYY-MM-DD or timestamp)
    - adjusted: Adjust for splits (default: True)
    - sort: Sort order - "asc" or "desc"
    - limit: Max results per page (default: 10)
    - fetch_all: If True (recommended), fetch ALL data and cache to disk for DuckDB queries (default: True)

    RECOMMENDED: Always use fetch_all=True to cache complete OHLC data locally for efficient DuckDB analysis.
    """
    try:
        tool_params = build_params(
            ticker=ticker,
            multiplier=multiplier,
            timespan=timespan,
            from_=str(from_),
            to=str(to),
            limit=limit,
            fetch_all=fetch_all,
        )

        if fetch_all:
            # Use batch writing for memory efficiency
            batch_callback, finalize = create_batch_writer("list_aggs", tool_params)

            if batch_callback:
                # Streaming mode - write batches to disk incrementally
                fetcher = PolygonParallelFetcher(polygon_client, num_workers=5)
                await fetcher.fetch_all(
                    method_name="list_aggs",
                    batch_callback=batch_callback,
                    ticker=ticker,
                    multiplier=multiplier,
                    timespan=timespan,
                    from_=from_,
                    to=to,
                    adjusted=adjusted,
                    sort=sort,
                    limit=limit,
                    params=params,
                )
                # Finalize and return cache metadata
                return await finalize()
            else:
                # Memory mode (fallback if batch writing not available)
                fetcher = PolygonParallelFetcher(polygon_client, num_workers=5)
                aggs_list = await fetcher.fetch_all(
                    method_name="list_aggs",
                    ticker=ticker,
                    multiplier=multiplier,
                    timespan=timespan,
                    from_=from_,
                    to=to,
                    adjusted=adjusted,
                    sort=sort,
                    limit=limit,
                    params=params,
                )
                csv_data = json_to_csv({"results": aggs_list})
                return await process_tool_response("list_aggs", tool_params, csv_data)
        else:
            # Single page approach
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

            import json

            data = json.loads(results.data.decode("utf-8"))
            aggs_list = data.get("results", [])

            # Create data structure for JSON to CSV conversion
            data = {"results": aggs_list, "status": "OK"}

            # Convert to CSV
            csv_data = json_to_csv(data)

            # Process with intelligent caching
            return await process_tool_response("list_aggs", tool_params, csv_data)
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
    Get daily OHLC and volume data for ALL U.S. stocks on a specific date. Returns market-wide snapshot with thousands of tickers.

    Parameters:
    - date: Trading date (YYYY-MM-DD, e.g., "2023-01-09")
    - adjusted: Adjust for splits (default: True)
    - include_otc: Include OTC securities (default: False)
    - locale: Filter by locale (us, global)
    - market_type: Filter by market (stocks, crypto, fx, otc, indices)

    Returns: Per-ticker data with columns: T (ticker), o, h, l, c, v (volume), vw (VWAP), t (timestamp), n (trades)

    Examples:
    - get_grouped_daily_aggs("2023-01-09") - all US stocks
    - get_grouped_daily_aggs("2023-06-15", include_otc=True) - include OTC
    - get_grouped_daily_aggs("2023-03-20", adjusted=False) - unadjusted

    Note: Large dataset. Useful for market screening, heatmaps, and identifying top gainers/losers.
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

        # Convert to CSV
        csv_data = json_to_csv(results.data.decode("utf-8"))

        # Process with intelligent caching
        return await process_tool_response(
            tool_name="get_grouped_daily_aggs",
            params={
                "date": date,
                "adjusted": adjusted,
                "include_otc": include_otc,
            },
            csv_data=csv_data,
        )
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
    Get OHLC prices for a specific ticker and date, including pre-market and after-hours data.

    Parameters:
    - ticker: Stock symbol (e.g., "AAPL", "MSFT")
    - date: Trading date (YYYY-MM-DD, e.g., "2023-01-09")
    - adjusted: Adjust for splits (default: True)

    Returns: Daily summary with open, high, low, close, volume, preMarket, afterHours

    Examples:
    - get_daily_open_close_agg("AAPL", "2023-01-09") - full day summary
    - get_daily_open_close_agg("TSLA", "2024-03-15", adjusted=False) - unadjusted

    Note: For multiple days, use get_aggs. For most recent day, use get_previous_close_agg.
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
    Get OHLC data for the previous trading day. Automatically handles weekends and holidays.

    Parameters:
    - ticker: Stock symbol (e.g., "AAPL", "MSFT")
    - adjusted: Adjust for splits (default: True)

    Returns: Previous day data with columns: o, h, l, c, v (volume), vw (VWAP), t (timestamp), T (ticker)

    Examples:
    - get_previous_close_agg("AAPL") - most recent trading day
    - get_previous_close_agg("TSLA", adjusted=False) - unadjusted

    Note: For specific dates, use get_daily_open_close_agg. For multiple days, use get_aggs.
    """
    try:
        results = polygon_client.get_previous_close_agg(
            ticker=ticker, adjusted=adjusted, params=params, raw=True
        )

        return json_to_csv(results.data.decode("utf-8"))
    except Exception as e:
        return f"Error: {e}"
