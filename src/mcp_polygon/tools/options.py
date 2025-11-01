"""Auto-generated tool definitions."""

from typing import Optional, Any, Dict, Union
from mcp.types import ToolAnnotations
from datetime import datetime, date
from ..clients import poly_mcp, polygon_client
from ..formatters import (
    json_to_csv,
    enrich_options_with_gex_and_advanced_greeks,
)
from ..tool_integration import process_tool_response, create_batch_writer
from ..parallel_fetcher import PolygonParallelFetcher
from ..utils import build_params
import json


@poly_mcp.tool(annotations=ToolAnnotations(readOnlyHint=True))
async def list_options_contracts(
    underlying_ticker: Optional[str] = None,
    contract_type: Optional[str] = None,
    expiration_date: Optional[Union[str, datetime, date]] = None,
    as_of: Optional[Union[str, datetime, date]] = None,
    strike_price: Optional[float] = None,
    expired: Optional[bool] = False,
    limit: Optional[int] = 10,
    sort: Optional[str] = None,
    order: Optional[str] = None,
    fetch_all: Optional[bool] = True,
    params: Optional[Dict[str, Any]] = None,
) -> str:
    """
    Get options contracts index with filtering by underlying, type, strike, expiration.

    Reference: https://polygon.io/docs/options/get_v3_reference_options_contracts

    Parameters:
    - underlying_ticker: Filter by stock (e.g., "AAPL", "TSLA")
    - contract_type: Filter by type ("call", "put")
    - expiration_date: Filter by expiration (YYYY-MM-DD)
    - strike_price: Filter by strike price
    - limit: Number of results per page (default: 10, max: 1000)
    - fetch_all: If True (recommended), fetch ALL data and cache to disk for DuckDB queries (default: True)
    - params: Range filters (expiration_date.gte, strike_price.lte, etc.)

    RECOMMENDED: Always use fetch_all=True to cache complete data locally for efficient DuckDB analysis.

    Example: list_options_contracts(underlying_ticker="AAPL", contract_type="call", fetch_all=True)
    Example: list_options_contracts(underlying_ticker="TSLA", params={"expiration_date.gte": "2025-06-01"}, fetch_all=True)

    Returns: ticker (O:AAPL251219C00150000 format), strike, expiration, type, exercise style, shares per contract.
    """
    try:
        tool_params = build_params(
            underlying_ticker=underlying_ticker,
            contract_type=contract_type,
            expiration_date=str(expiration_date) if expiration_date else None,
            strike_price=strike_price,
            limit=limit,
            fetch_all=fetch_all,
        )

        if fetch_all:
            # Use batch writing for memory efficiency
            batch_callback, finalize = create_batch_writer(
                "list_options_contracts", tool_params
            )

            if batch_callback:
                # Streaming mode - write batches to disk incrementally
                fetcher = PolygonParallelFetcher(polygon_client, num_workers=5)
                await fetcher.fetch_all(
                    method_name="list_options_contracts",
                    batch_callback=batch_callback,
                    underlying_ticker=underlying_ticker,
                    contract_type=contract_type,
                    expiration_date=expiration_date,
                    as_of=as_of,
                    strike_price=strike_price,
                    expired=expired,
                    limit=limit,
                    sort=sort,
                    order=order,
                    params=params,
                )
                # Finalize and return cache metadata
                return await finalize()
            else:
                # Memory mode (fallback if batch writing not available)
                fetcher = PolygonParallelFetcher(polygon_client, num_workers=5)
                contracts_list = await fetcher.fetch_all(
                    method_name="list_options_contracts",
                    underlying_ticker=underlying_ticker,
                    contract_type=contract_type,
                    expiration_date=expiration_date,
                    as_of=as_of,
                    strike_price=strike_price,
                    expired=expired,
                    limit=limit,
                    sort=sort,
                    order=order,
                    params=params,
                )
                csv_data = json_to_csv({"results": contracts_list})
                return await process_tool_response(
                    "list_options_contracts", tool_params, csv_data
                )
        else:
            # Single page approach (existing behavior)
            results = polygon_client.list_options_contracts(
                underlying_ticker=underlying_ticker,
                contract_type=contract_type,
                expiration_date=expiration_date,
                as_of=as_of,
                strike_price=strike_price,
                expired=expired,
                limit=limit,
                sort=sort,
                order=order,
                params=params,
                raw=True,
            )

            # Parse the JSON response
            data = json.loads(results.data.decode("utf-8"))
            contracts_list = data.get("results", [])

            # Create data structure for JSON to CSV conversion
            data = {"results": contracts_list, "status": "OK"}

            # Convert to CSV
            csv_data = json_to_csv(data)

            # Process with intelligent caching
            return await process_tool_response(
                "list_options_contracts", tool_params, csv_data
            )

    except Exception as e:
        return f"Error: {e}"


@poly_mcp.tool(annotations=ToolAnnotations(readOnlyHint=True))
async def get_options_contract(
    options_ticker: str,
    as_of: Optional[Union[str, datetime, date]] = None,
) -> str:
    """
    Get detailed information for a specific options contract by ticker.

    Reference: https://polygon.io/docs/options/get_v3_reference_options_contracts__options_ticker

    Parameters:
    - options_ticker: Contract ticker (e.g., "O:AAPL251219C00150000")
      Format: O:SYMBOL[YYMMDD][C/P][STRIKE8]
    - as_of: Historical date for specs (YYYY-MM-DD)

    Example: get_options_contract(options_ticker="O:AAPL251219C00150000")
    Example: get_options_contract(options_ticker="O:TSLA250620P00700000")

    Returns: type, strike, expiration, exercise_style (american/european), shares_per_contract (usually 100), underlying.
    """
    try:
        results = polygon_client.get_options_contract(
            ticker=options_ticker, as_of=as_of, raw=True
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
async def get_options_aggs(
    options_ticker: str,
    multiplier: int,
    timespan: str,
    from_: Union[str, int, datetime, date],
    to: Union[str, int, datetime, date],
    adjusted: Optional[bool] = True,
    sort: Optional[str] = None,
    limit: Optional[int] = 5000,
    fetch_all: Optional[bool] = True,
    params: Optional[Dict[str, Any]] = None,
) -> str:
    """
    Get OHLC aggregate bars for an options contract over a custom date range and interval.

    Reference: https://polygon.io/docs/options/get_v2_aggs_ticker__optionsticker__range__multiplier___timespan___from___to

    Parameters:
    - options_ticker: Contract ticker (e.g., "O:AAPL251219C00150000")
    - multiplier: Timespan multiplier (e.g., 1, 5, 15)
    - timespan: Time window (minute, hour, day, week, month)
    - from_: Start date (YYYY-MM-DD)
    - to: End date (YYYY-MM-DD)
    - limit: Max results per page (default: 5000, max: 50000)
    - fetch_all: If True (recommended), fetch all data and cache to disk for DuckDB queries (default: True)

    Example: get_options_aggs("O:AAPL251219C00150000", 1, "day", "2025-01-01", "2025-03-31", fetch_all=True)
    Example: get_options_aggs("O:SPY251219C00500000", 5, "minute", "2025-03-20", "2025-03-20", fetch_all=True)

    Returns: o, h, l, c, v, vw (VWAP), t (timestamp), n (trades). Times in ET. Gaps indicate no trading.
    """
    try:
        tool_params = build_params(
            options_ticker=options_ticker,
            multiplier=multiplier,
            timespan=timespan,
            from_=str(from_),
            to=str(to),
            limit=limit,
            fetch_all=fetch_all,
        )

        if fetch_all:
            # Use batch writing for memory efficiency
            batch_callback, finalize = create_batch_writer(
                "get_options_aggs", tool_params
            )

            if batch_callback:
                # Streaming mode - write batches to disk incrementally
                fetcher = PolygonParallelFetcher(polygon_client, num_workers=5)
                await fetcher.fetch_all(
                    method_name="get_aggs",
                    batch_callback=batch_callback,
                    ticker=options_ticker,
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
                aggs_data = polygon_client.get_aggs(
                    ticker=options_ticker,
                    multiplier=multiplier,
                    timespan=timespan,
                    from_=from_,
                    to=to,
                    adjusted=adjusted,
                    sort=sort,
                    limit=limit,
                    params=params,
                    raw=False,
                )
                # Convert Agg objects to dict
                aggs_list = []
                for agg in aggs_data:
                    aggs_list.append(
                        {
                            "o": agg.open,
                            "h": agg.high,
                            "l": agg.low,
                            "c": agg.close,
                            "v": agg.volume,
                            "vw": agg.vwap,
                            "t": agg.timestamp,
                            "n": agg.transactions,
                        }
                    )
                csv_data = json_to_csv({"results": aggs_list})
                return await process_tool_response(
                    "get_options_aggs", tool_params, csv_data
                )
        else:
            # Single page approach
            results = polygon_client.get_aggs(
                ticker=options_ticker,
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
            data = json.loads(results.data.decode("utf-8"))
            aggs_list = data.get("results", [])

            # Create data structure for JSON to CSV conversion
            data = {"results": aggs_list, "status": "OK"}

            # Convert to CSV
            csv_data = json_to_csv(data)

            # Process with intelligent caching
            return await process_tool_response(
                "get_options_aggs", tool_params, csv_data
            )
    except Exception as e:
        return f"Error: {e}"


@poly_mcp.tool(annotations=ToolAnnotations(readOnlyHint=True))
async def get_options_daily_open_close(
    options_ticker: str,
    date: Union[str, datetime, date],
    adjusted: Optional[bool] = True,
) -> str:
    """
    Get daily OHLC and volume for a specific options contract on a given date.

    Reference: https://polygon.io/docs/options/get_v1_open-close__optionsticker___date

    Parameters:
    - options_ticker: Contract ticker (e.g., "O:AAPL251219C00150000")
    - date: Trading date (YYYY-MM-DD)
    - adjusted: Adjust for splits (default: True)

    Example: get_options_daily_open_close("O:AAPL251219C00150000", "2025-03-20")
    Example: get_options_daily_open_close("O:TSLA250620P00700000", "2025-06-15")

    Returns: open, high, low, close, volume, preMarket, afterHours (if available).
    """
    try:
        results = polygon_client.get_daily_open_close_agg(
            ticker=options_ticker,
            date=date,
            adjusted=adjusted,
            raw=True,
        )

        return json_to_csv(results.data.decode("utf-8"))
    except Exception as e:
        return f"Error: {e}"


@poly_mcp.tool(annotations=ToolAnnotations(readOnlyHint=True))
async def get_options_previous_close(
    options_ticker: str,
    adjusted: Optional[bool] = True,
) -> str:
    """
    Get previous trading day's OHLC data for an options contract.

    Reference: https://polygon.io/docs/options/get_v2_aggs_ticker__optionsticker__prev

    Parameters:
    - options_ticker: Contract ticker (e.g., "O:TSLA210903C00700000")
    - adjusted: Adjust for splits (default: True)

    Example: get_options_previous_close("O:TSLA210903C00700000")
    Example: get_options_previous_close("O:AAPL251219C00150000")

    Returns: T (ticker), o, h, l, c, v, vw (VWAP), n (trades), t (timestamp).
    """
    try:
        results = polygon_client.get_previous_close_agg(
            ticker=options_ticker,
            adjusted=adjusted,
            raw=True,
        )

        return json_to_csv(results.data.decode("utf-8"))
    except Exception as e:
        return f"Error: {e}"


@poly_mcp.tool(annotations=ToolAnnotations(readOnlyHint=True))
async def get_options_snapshot(
    underlying_asset: str,
    option_contract: str,
) -> str:
    """
    Get comprehensive snapshot of an options contract with greeks, IV, quotes, and underlying price.

    Reference: https://polygon.io/docs/options/get_v3_snapshot_options__underlyingasset___optioncontract

    Parameters:
    - underlying_asset: Stock ticker (e.g., "AAPL", "TSLA")
    - option_contract: Contract ticker (e.g., "O:AAPL230616C00150000")

    Example: get_options_snapshot("AAPL", "O:AAPL230616C00150000")
    Example: get_options_snapshot("TSLA", "O:TSLA210903C00700000")

    Returns: break_even, day (OHLC), greeks (delta/gamma/theta/vega), implied_volatility, last_quote, last_trade, open_interest.
             Includes enriched GEX and advanced Greeks (charm, vanna, vomma, zomma, speed, color).
    """
    try:
        results = polygon_client.get_snapshot_option(
            underlying_asset=underlying_asset,
            option_contract=option_contract,
            raw=True,
        )

        # Parse the response and extract the results object
        import json

        data = json.loads(results.data.decode("utf-8"))

        options_list = []
        if "results" in data:
            options_list = [data["results"]]

        # Get current stock price for GEX/Greeks calculations
        stock_price = None
        try:
            snapshot_result = polygon_client.get_snapshot_ticker(
                market_type="stocks",
                ticker=underlying_asset,
                raw=True,
            )
            snapshot_data = json.loads(snapshot_result.data.decode("utf-8"))
            if "ticker" in snapshot_data and "day" in snapshot_data["ticker"]:
                stock_price = snapshot_data["ticker"]["day"].get("c")  # Closing price
            # Fallback: try prevDay close if day close not available
            if (
                not stock_price
                and "ticker" in snapshot_data
                and "prevDay" in snapshot_data["ticker"]
            ):
                stock_price = snapshot_data["ticker"]["prevDay"].get("c")
        except Exception as e:
            # If we can't get the stock price, continue without enrichment
            import sys

            print(
                f"Warning: Could not fetch stock price for {underlying_asset}: {e}",
                file=sys.stderr,
            )

        # Enrich options data with GEX and advanced Greeks
        if stock_price and options_list:
            enriched_options = enrich_options_with_gex_and_advanced_greeks(
                options_list, stock_price
            )
            options_list = enriched_options

        # Format for CSV
        if options_list:
            formatted_data = {"results": options_list}
            return json_to_csv(formatted_data)

        return json_to_csv(results.data.decode("utf-8"))
    except Exception as e:
        return f"Error: {e}"


@poly_mcp.tool(annotations=ToolAnnotations(readOnlyHint=True))
async def list_snapshot_options_chain(
    underlying_asset: str,
    strike_price: Optional[float] = None,
    expiration_date: Optional[Union[str, date]] = None,
    contract_type: Optional[str] = None,
    strike_price_gte: Optional[float] = None,
    strike_price_gt: Optional[float] = None,
    strike_price_lte: Optional[float] = None,
    strike_price_lt: Optional[float] = None,
    expiration_date_gte: Optional[Union[str, date]] = None,
    expiration_date_gt: Optional[Union[str, date]] = None,
    expiration_date_lte: Optional[Union[str, date]] = None,
    expiration_date_lt: Optional[Union[str, date]] = None,
    order: Optional[str] = None,
    limit: Optional[int] = 10,
    sort: Optional[str] = None,
    fetch_all: Optional[bool] = True,
    params: Optional[Dict[str, Any]] = None,
) -> str:
    """
    Get full options chain snapshot with pricing, greeks, IV, and open interest for all contracts.

    Reference: https://polygon.io/docs/options/get_v3_snapshot_options__underlyingasset

    Parameters:
    - underlying_asset: Stock ticker (e.g., "AAPL", "TSLA", "SPY")
    - contract_type: Filter by type ("call" or "put")
    - expiration_date: Filter by expiration (YYYY-MM-DD)
    - strike_price_gte/lte: Filter by strike range
    - expiration_date_gte/lte: Filter by expiration range
    - limit: Number of results per page (default: 10, max: 250)
    - fetch_all: If True (recommended), fetch ALL data and cache to disk for DuckDB queries (default: True)

    RECOMMENDED: Always use fetch_all=True to cache complete options chain data locally for efficient DuckDB analysis.

    Example: list_snapshot_options_chain("AAPL", contract_type="call", fetch_all=True)
    Example: list_snapshot_options_chain("SPY", strike_price_gte=450, strike_price_lte=500, contract_type="put", fetch_all=True)

    Returns: break_even, day, greeks, implied_volatility, last_quote, last_trade, open_interest per contract. Includes GEX and advanced greeks.
    """
    try:
        tool_params = build_params(
            underlying_asset=underlying_asset,
            strike_price=strike_price,
            expiration_date=str(expiration_date) if expiration_date else None,
            contract_type=contract_type,
            limit=limit,
            fetch_all=fetch_all,
        )

        param_dict = {
            **(params or {}),
            **{
                k: v
                for k, v in {
                    "strike_price": strike_price,
                    "expiration_date": expiration_date,
                    "contract_type": contract_type,
                    "strike_price.gte": strike_price_gte,
                    "strike_price.gt": strike_price_gt,
                    "strike_price.lte": strike_price_lte,
                    "strike_price.lt": strike_price_lt,
                    "expiration_date.gte": expiration_date_gte,
                    "expiration_date.gt": expiration_date_gt,
                    "expiration_date.lte": expiration_date_lte,
                    "expiration_date.lt": expiration_date_lt,
                    "order": order,
                    "limit": limit,
                    "sort": sort,
                }.items()
                if v is not None
            },
        }

        if fetch_all:
            # Use batch writing for memory efficiency
            batch_callback, finalize = create_batch_writer(
                "list_snapshot_options_chain", tool_params
            )

            if batch_callback:
                # Streaming mode - write batches to disk incrementally
                fetcher = PolygonParallelFetcher(polygon_client, num_workers=5)
                await fetcher.fetch_all(
                    method_name="list_snapshot_options_chain",
                    batch_callback=batch_callback,
                    underlying_asset=underlying_asset,
                    params=param_dict,
                )
                # Finalize and return cache metadata
                return await finalize()
            else:
                # Memory mode (fallback if batch writing not available)
                fetcher = PolygonParallelFetcher(polygon_client, num_workers=5)
                options_list = await fetcher.fetch_all(
                    method_name="list_snapshot_options_chain",
                    underlying_asset=underlying_asset,
                    params=param_dict,
                )
                # Get current stock price by fetching the underlying ticker snapshot
                stock_price = None
                try:
                    snapshot_result = polygon_client.get_snapshot_ticker(
                        market_type="stocks",
                        ticker=underlying_asset,
                        raw=True,
                    )
                    snapshot_data = json.loads(snapshot_result.data.decode("utf-8"))
                    if "ticker" in snapshot_data and "day" in snapshot_data["ticker"]:
                        stock_price = snapshot_data["ticker"]["day"].get("c")
                    if (
                        not stock_price
                        and "ticker" in snapshot_data
                        and "prevDay" in snapshot_data["ticker"]
                    ):
                        stock_price = snapshot_data["ticker"]["prevDay"].get("c")
                except Exception as e:
                    import sys

                    print(
                        f"Warning: Could not fetch stock price for {underlying_asset}: {e}",
                        file=sys.stderr,
                    )
                # Enrich options data with GEX and advanced Greeks
                if stock_price and options_list:
                    enriched_options = enrich_options_with_gex_and_advanced_greeks(
                        options_list, stock_price
                    )
                    options_list = enriched_options
                csv_data = json_to_csv({"results": options_list})
                return await process_tool_response(
                    "list_snapshot_options_chain", tool_params, csv_data
                )
        else:
            # Single page approach (existing behavior)
            results = polygon_client.list_snapshot_options_chain(
                underlying_asset=underlying_asset,
                params=param_dict,
                raw=True,
            )

            # Parse the JSON response
            data = json.loads(results.data.decode("utf-8"))
            options_list = data.get("results", [])

            # Get current stock price by fetching the underlying ticker snapshot
            stock_price = None
            try:
                snapshot_result = polygon_client.get_snapshot_ticker(
                    market_type="stocks",
                    ticker=underlying_asset,
                    raw=True,
                )
                snapshot_data = json.loads(snapshot_result.data.decode("utf-8"))
                if "ticker" in snapshot_data and "day" in snapshot_data["ticker"]:
                    stock_price = snapshot_data["ticker"]["day"].get("c")
                if (
                    not stock_price
                    and "ticker" in snapshot_data
                    and "prevDay" in snapshot_data["ticker"]
                ):
                    stock_price = snapshot_data["ticker"]["prevDay"].get("c")
            except Exception as e:
                import sys

                print(
                    f"Warning: Could not fetch stock price for {underlying_asset}: {e}",
                    file=sys.stderr,
                )

            # Enrich options data with GEX and advanced Greeks
            if stock_price and options_list:
                enriched_options = enrich_options_with_gex_and_advanced_greeks(
                    options_list, stock_price
                )
                options_list = enriched_options

            # Create data structure for JSON to CSV conversion
            data = {"results": options_list, "status": "OK"}

            # Convert to CSV
            csv_data = json_to_csv(data)

            # Process with intelligent caching
            return await process_tool_response(
                "list_snapshot_options_chain", tool_params, csv_data
            )

    except Exception as e:
        return f"Error: {e}"


# @poly_mcp.tool(annotations=ToolAnnotations(readOnlyHint=True))  # DISABLED
