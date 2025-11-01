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

        # Convert to CSV
        csv_data = json_to_csv(results.data.decode("utf-8"))

        # Process with intelligent caching
        return await process_tool_response(
            tool_name="get_market_holidays",
            params={},
            csv_data=csv_data,
        )
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
    fetch_all: Optional[bool] = True,
    params: Optional[Dict[str, Any]] = None,
) -> str:
    """
    Get list of ticker symbols with filtering by market, type, exchange, and search terms.

    Reference: https://polygon.io/docs/rest/stocks/tickers/all-tickers

    Parameters:
    - market: Market type ("stocks", "crypto", "fx", "otc", "indices")
    - type: Ticker type (CS=Common Stock, ETF, ADRC, etc.)
    - search: Search ticker/company name
    - active: Only active tickers (default: None)
    - ticker_gte/lt: Ticker range filters
    - exchange: Exchange MIC code (e.g., XNYS, XNAS)
    - limit: Number of results per page (default: 10, max: 1000)
    - fetch_all: If True (recommended), fetch ALL data and cache to disk for DuckDB queries (default: True)

    RECOMMENDED: Always use fetch_all=True to cache complete ticker data locally for efficient DuckDB analysis.

    Example: list_tickers(market="stocks", active=True, fetch_all=True)
    Example: list_tickers(search="Apple", market="stocks", fetch_all=True)

    Returns: ticker, name, market, type, active, primary_exchange, currency, cik.
    """
    try:
        tool_params = build_params(
            market=market,
            active=active,
            limit=limit,
            fetch_all=fetch_all,
        )

        param_dict = {
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
        }

        if fetch_all:
            # Use batch writing for memory efficiency
            batch_callback, finalize = create_batch_writer("list_tickers", tool_params)

            if batch_callback:
                # Streaming mode - write batches to disk incrementally
                fetcher = PolygonParallelFetcher(polygon_client, num_workers=5)
                await fetcher.fetch_all(
                    method_name="list_tickers",
                    batch_callback=batch_callback,
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
                    params=param_dict,
                )
                # Finalize and return cache metadata
                return await finalize()
            else:
                # Memory mode (fallback if batch writing not available)
                fetcher = PolygonParallelFetcher(polygon_client, num_workers=5)
                tickers_list = await fetcher.fetch_all(
                    method_name="list_tickers",
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
                    params=param_dict,
                )
                csv_data = json_to_csv({"results": tickers_list})
                return await process_tool_response(
                    "list_tickers", tool_params, csv_data
                )
        else:
            # Single page approach
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
                params=param_dict,
                raw=True,
            )

            import json

            data = json.loads(results.data.decode("utf-8"))
            tickers_list = data.get("results", [])

            # Create data structure for JSON to CSV conversion
            data = {"results": tickers_list, "status": "OK"}

            # Convert to CSV
            csv_data = json_to_csv(data)

            # Process with intelligent caching
            return await process_tool_response("list_tickers", tool_params, csv_data)
    except Exception as e:
        return f"Error: {e}"


@poly_mcp.tool(annotations=ToolAnnotations(readOnlyHint=True))
async def get_all_tickers(
    market: Optional[str] = None,
    type: Optional[str] = None,
    active: Optional[bool] = True,
    limit: Optional[int] = 100,
    fetch_all: Optional[bool] = True,
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
    - limit: Number of results per page (default: 100)
    - fetch_all: If True (recommended), fetch ALL data and cache to disk for DuckDB queries (default: True)

    RECOMMENDED: Always use fetch_all=True to cache complete ticker data locally for efficient DuckDB analysis.

    Example: get_all_tickers(market="stocks", active=True, fetch_all=True)
    Example: get_all_tickers(market="crypto", fetch_all=True)
    """
    try:
        tool_params = build_params(
            market=market,
            type=type,
            active=active,
            limit=limit,
            fetch_all=fetch_all,
        )

        if fetch_all:
            # Use batch writing for memory efficiency
            batch_callback, finalize = create_batch_writer(
                "get_all_tickers", tool_params
            )

            if batch_callback:
                # Streaming mode - write batches to disk incrementally
                fetcher = PolygonParallelFetcher(polygon_client, num_workers=5)
                await fetcher.fetch_all(
                    method_name="list_tickers",
                    batch_callback=batch_callback,
                    market=market,
                    type=type,
                    active=active,
                    sort=sort,
                    order=order,
                    limit=limit,
                    params=params,
                )
                # Finalize and return cache metadata
                return await finalize()
            else:
                # Memory mode (fallback if batch writing not available)
                fetcher = PolygonParallelFetcher(polygon_client, num_workers=5)
                tickers_list = await fetcher.fetch_all(
                    method_name="list_tickers",
                    market=market,
                    type=type,
                    active=active,
                    sort=sort,
                    order=order,
                    limit=limit,
                    params=params,
                )
                csv_data = json_to_csv({"results": tickers_list})
                return await process_tool_response(
                    "get_all_tickers", tool_params, csv_data
                )
        else:
            # Single page approach
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

            import json

            data = json.loads(results.data.decode("utf-8"))
            tickers_list = data.get("results", [])

            # Create data structure for JSON to CSV conversion
            data = {"results": tickers_list, "status": "OK"}

            # Convert to CSV
            csv_data = json_to_csv(data)

            # Process with intelligent caching
            return await process_tool_response("get_all_tickers", tool_params, csv_data)
    except Exception as e:
        return f"Error: {e}"


@poly_mcp.tool(annotations=ToolAnnotations(readOnlyHint=True))
async def get_ticker_details(
    ticker: str,
    date: Optional[Union[str, datetime, date]] = None,
    params: Optional[Dict[str, Any]] = None,
) -> str:
    """
    Get comprehensive company details including fundamentals, identifiers, and branding assets.

    Reference: https://polygon.io/docs/rest/stocks/tickers/ticker-overview

    Parameters:
    - ticker: Stock symbol (e.g., "AAPL", "MSFT")
    - date: Historical date for data (YYYY-MM-DD, default: most recent)

    Example: get_ticker_details("AAPL")
    Example: get_ticker_details("MSFT", date="2020-01-15")

    Returns: name, description, market_cap, total_employees, homepage_url, address, cik, sic_code, branding (logo/icon).
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
    Get list of related tickers based on news coverage and returns correlation.

    Reference: https://polygon.io/docs/rest/stocks/tickers/related-tickers

    Parameters:
    - ticker: Stock symbol (e.g., "AAPL", "MSFT", "TSLA")

    Example: get_related_companies("AAPL")
    Example: get_related_companies("TSLA")

    Returns: Array of related ticker symbols (peers, competitors, sector companies).
    """
    try:
        results = polygon_client.get_related_companies(
            ticker=ticker, params=params, raw=True
        )

        return json_to_csv(results.data.decode("utf-8"))
    except Exception as e:
        return f"Error: {e}"


@poly_mcp.tool(annotations=ToolAnnotations(readOnlyHint=True))
async def get_ticker_types(
    asset_class: Optional[str] = None,
    locale: Optional[str] = None,
    params: Optional[Dict[str, Any]] = None,
) -> str:
    """
    Get list of all ticker types with codes and descriptions.

    Reference: https://polygon.io/docs/rest/stocks/tickers/ticker-types

    Parameters:
    - asset_class: Filter by asset class ("stocks", "options", "crypto", "fx", "indices")
    - locale: Filter by locale ("us", "global")

    Example: get_ticker_types()
    Example: get_ticker_types(asset_class="stocks")

    Returns: code, description, asset_class, locale. Common codes: CS (Common Stock), ETF, REIT, CALL/PUT (options).
    """
    try:
        results = polygon_client.get_ticker_types(
            asset_class=asset_class, locale=locale, params=params, raw=True
        )

        # Convert to CSV
        csv_data = json_to_csv(results.data.decode("utf-8"))

        # Process with intelligent caching
        return await process_tool_response(
            tool_name="get_ticker_types",
            params={},
            csv_data=csv_data,
        )
    except Exception as e:
        return f"Error: {e}"


@poly_mcp.tool(annotations=ToolAnnotations(readOnlyHint=True))
async def list_conditions(
    asset_class: Optional[str] = None,
    data_type: Optional[str] = None,
    id: Optional[int] = None,
    sip: Optional[str] = None,
    params: Optional[Dict[str, Any]] = None,
) -> str:
    """
    List all condition codes used by Polygon.io for trades and quotes.
    Condition codes provide additional context about trades and quotes (e.g., extended hours, odd lot).

    Reference: https://polygon.io/docs/rest/stocks/market-operations/condition-codes

    Parameters:
    - asset_class: Filter by asset class (stocks, options, crypto, fx)
    - data_type: Filter by data type (trade, quote, bbo)
    - id: Filter by specific condition ID
    - sip: Filter by SIP (CTA, UTP, OPRA)

    Example: list_conditions() returns all condition codes
    Example: list_conditions(asset_class="stocks", data_type="trade") returns stock trade conditions
    Example: list_conditions(id=1) returns details for condition code 1

    Note: Condition codes are essential for understanding trade and quote characteristics.
    Common examples include codes for extended hours trading, odd lots, and various execution venues.
    """
    try:
        results = polygon_client.list_conditions(
            asset_class=asset_class,
            data_type=data_type,
            id=id,
            sip=sip,
            params=params,
            raw=True,
        )

        return json_to_csv(results.data.decode("utf-8"))
    except Exception as e:
        return f"Error: {e}"


@poly_mcp.tool(annotations=ToolAnnotations(readOnlyHint=True))
async def get_exchanges(
    asset_class: Optional[str] = None,
    locale: Optional[str] = None,
    params: Optional[Dict[str, Any]] = None,
) -> str:
    """
    List all known stock exchanges and their properties.
    Returns exchange details including name, type, MIC (Market Identifier Code), and operating status.

    Reference: https://polygon.io/docs/rest/stocks/market-operations/exchanges

    Parameters:
    - asset_class: Filter by asset class (stocks, options, crypto, fx)
    - locale: Filter by locale (us, global)

    Example: get_exchanges() returns all known exchanges
    Example: get_exchanges(asset_class="stocks", locale="us") returns US stock exchanges

    Note: MIC (Market Identifier Code) is a unique identifier for each exchange (e.g., XNYS for NYSE).
    """
    try:
        results = polygon_client.get_exchanges(
            asset_class=asset_class, locale=locale, params=params, raw=True
        )

        # Convert to CSV
        csv_data = json_to_csv(results.data.decode("utf-8"))

        # Process with intelligent caching
        return await process_tool_response(
            tool_name="get_exchanges",
            params={},
            csv_data=csv_data,
        )
    except Exception as e:
        return f"Error: {e}"
