"""Auto-generated tool definitions."""

from typing import Optional, Any, Dict, Union
from mcp.types import ToolAnnotations
from datetime import date
from ..clients import poly_mcp, polygon_client
from ..formatters import json_to_csv
from ..tool_integration import process_tool_response


@poly_mcp.tool(annotations=ToolAnnotations(readOnlyHint=True))
async def list_futures_aggregates(
    ticker: str,
    resolution: str,
    window_start: Optional[str] = None,
    window_start_lt: Optional[str] = None,
    window_start_lte: Optional[str] = None,
    window_start_gt: Optional[str] = None,
    window_start_gte: Optional[str] = None,
    limit: Optional[int] = 10,
    fetch_all: Optional[bool] = True,
    sort: Optional[str] = None,
    params: Optional[Dict[str, Any]] = None,
) -> str:
    """
    Get aggregates (OHLC bars) for a futures contract in a given time range.

    Parameters:
    - ticker: Futures ticker (e.g., "ES", "CL")
    - resolution: Bar resolution (e.g., "minute", "hour", "day")
    - limit: Number of results per page (default: 10)
    - fetch_all: If True (recommended), fetch ALL data and cache to disk for DuckDB queries (default: True)

    RECOMMENDED: Always use fetch_all=True to cache complete aggregate data locally for efficient DuckDB analysis.

    Example: list_futures_aggregates("ES", "day", fetch_all=True)
    """
    try:
        aggregates_list = []

        if fetch_all:
            # Use iterator approach for automatic pagination
            for agg in polygon_client.list_futures_aggregates(
                ticker=ticker,
                resolution=resolution,
                window_start=window_start,
                window_start_lt=window_start_lt,
                window_start_lte=window_start_lte,
                window_start_gt=window_start_gt,
                window_start_gte=window_start_gte,
                limit=limit,
                sort=sort,
                params=params,
                raw=False,
            ):
                aggregates_list.append(agg.to_dict())
        else:
            # Single page approach
            results = polygon_client.list_futures_aggregates(
                ticker=ticker,
                resolution=resolution,
                window_start=window_start,
                window_start_lt=window_start_lt,
                window_start_lte=window_start_lte,
                window_start_gt=window_start_gt,
                window_start_gte=window_start_gte,
                limit=limit,
                sort=sort,
                params=params,
                raw=True,
            )

            import json
            data = json.loads(results.data.decode("utf-8"))
            aggregates_list = data.get("results", [])

        # Create data structure for JSON to CSV conversion
        data = {"results": aggregates_list, "status": "OK"}

        # Convert to CSV
        csv_data = json_to_csv(data)

        # Process with intelligent caching
        return await process_tool_response(
            tool_name="list_futures_aggregates",
            params={
                "ticker": ticker,
                "resolution": resolution,
                "limit": limit,
                "fetch_all": fetch_all,
            },
            csv_data=csv_data,
        )
    except Exception as e:
        return f"Error: {e}"


@poly_mcp.tool(annotations=ToolAnnotations(readOnlyHint=True))
async def list_futures_contracts(
    product_code: Optional[str] = None,
    first_trade_date: Optional[Union[str, date]] = None,
    last_trade_date: Optional[Union[str, date]] = None,
    as_of: Optional[Union[str, date]] = None,
    active: Optional[str] = None,
    type: Optional[str] = None,
    limit: Optional[int] = 10,
    fetch_all: Optional[bool] = True,
    sort: Optional[str] = None,
    params: Optional[Dict[str, Any]] = None,
) -> str:
    """
    Get a paginated list of futures contracts with filtering and sorting.

    Parameters:
    - product_code: Filter by product code (e.g., "ES" for E-mini S&P 500)
    - active: Filter by active status
    - limit: Number of results per page (default: 10)
    - fetch_all: If True (recommended), fetch ALL data and cache to disk for DuckDB queries (default: True)

    RECOMMENDED: Always use fetch_all=True to cache complete contract data locally for efficient DuckDB analysis.

    Example: list_futures_contracts(product_code="ES", fetch_all=True)
    """
    try:
        contracts_list = []

        if fetch_all:
            # Use iterator approach for automatic pagination
            for contract in polygon_client.list_futures_contracts(
                product_code=product_code,
                first_trade_date=first_trade_date,
                last_trade_date=last_trade_date,
                as_of=as_of,
                active=active,
                type=type,
                limit=limit,
                sort=sort,
                params=params,
                raw=False,
            ):
                contracts_list.append(contract.to_dict())
        else:
            # Single page approach
            results = polygon_client.list_futures_contracts(
                product_code=product_code,
                first_trade_date=first_trade_date,
                last_trade_date=last_trade_date,
                as_of=as_of,
                active=active,
                type=type,
                limit=limit,
                sort=sort,
                params=params,
                raw=True,
            )

            import json
            data = json.loads(results.data.decode("utf-8"))
            contracts_list = data.get("results", [])

        # Create data structure for JSON to CSV conversion
        data = {"results": contracts_list, "status": "OK"}

        # Convert to CSV
        csv_data = json_to_csv(data)

        # Process with intelligent caching
        return await process_tool_response(
            tool_name="list_futures_contracts",
            params={
                "product_code": product_code,
                "active": active,
                "limit": limit,
                "fetch_all": fetch_all,
            },
            csv_data=csv_data,
        )
    except Exception as e:
        return f"Error: {e}"


@poly_mcp.tool(annotations=ToolAnnotations(readOnlyHint=True))
async def get_futures_contract_details(
    ticker: str,
    as_of: Optional[Union[str, date]] = None,
    params: Optional[Dict[str, Any]] = None,
) -> str:
    """
    Get details for a single futures contract at a specified point in time.
    """
    try:
        results = polygon_client.get_futures_contract_details(
            ticker=ticker,
            as_of=as_of,
            params=params,
            raw=True,
        )

        return json_to_csv(results.data.decode("utf-8"))
    except Exception as e:
        return f"Error: {e}"


@poly_mcp.tool(annotations=ToolAnnotations(readOnlyHint=True))
async def list_futures_products(
    name: Optional[str] = None,
    name_search: Optional[str] = None,
    as_of: Optional[Union[str, date]] = None,
    trading_venue: Optional[str] = None,
    sector: Optional[str] = None,
    sub_sector: Optional[str] = None,
    asset_class: Optional[str] = None,
    asset_sub_class: Optional[str] = None,
    type: Optional[str] = None,
    limit: Optional[int] = 10,
    fetch_all: Optional[bool] = True,
    sort: Optional[str] = None,
    params: Optional[Dict[str, Any]] = None,
) -> str:
    """
    Get a list of futures products (including combos) with filtering options.

    Parameters:
    - name_search: Search by product name
    - sector: Filter by sector (e.g., "equity", "energy", "metals")
    - asset_class: Filter by asset class
    - limit: Number of results per page (default: 10)
    - fetch_all: If True (recommended), fetch ALL data and cache to disk for DuckDB queries (default: True)

    RECOMMENDED: Always use fetch_all=True to cache complete product data locally for efficient DuckDB analysis.

    Example: list_futures_products(sector="energy", fetch_all=True)
    """
    try:
        products_list = []

        if fetch_all:
            # Use iterator approach for automatic pagination
            for product in polygon_client.list_futures_products(
                name=name,
                name_search=name_search,
                as_of=as_of,
                trading_venue=trading_venue,
                sector=sector,
                sub_sector=sub_sector,
                asset_class=asset_class,
                asset_sub_class=asset_sub_class,
                type=type,
                limit=limit,
                sort=sort,
                params=params,
                raw=False,
            ):
                products_list.append(product.to_dict())
        else:
            # Single page approach
            results = polygon_client.list_futures_products(
                name=name,
                name_search=name_search,
                as_of=as_of,
                trading_venue=trading_venue,
                sector=sector,
                sub_sector=sub_sector,
                asset_class=asset_class,
                asset_sub_class=asset_sub_class,
                type=type,
                limit=limit,
                sort=sort,
                params=params,
                raw=True,
            )

            import json
            data = json.loads(results.data.decode("utf-8"))
            products_list = data.get("results", [])

        # Create data structure for JSON to CSV conversion
        data = {"results": products_list, "status": "OK"}

        # Convert to CSV
        csv_data = json_to_csv(data)

        # Process with intelligent caching
        return await process_tool_response(
            tool_name="list_futures_products",
            params={
                "sector": sector,
                "asset_class": asset_class,
                "limit": limit,
                "fetch_all": fetch_all,
            },
            csv_data=csv_data,
        )
    except Exception as e:
        return f"Error: {e}"


@poly_mcp.tool(annotations=ToolAnnotations(readOnlyHint=True))
async def get_futures_product_details(
    product_code: str,
    type: Optional[str] = None,
    as_of: Optional[Union[str, date]] = None,
    params: Optional[Dict[str, Any]] = None,
) -> str:
    """
    Get details for a single futures product as it was at a specific day.
    """
    try:
        results = polygon_client.get_futures_product_details(
            product_code=product_code,
            type=type,
            as_of=as_of,
            params=params,
            raw=True,
        )

        return json_to_csv(results.data.decode("utf-8"))
    except Exception as e:
        return f"Error: {e}"


# @poly_mcp.tool(annotations=ToolAnnotations(readOnlyHint=True))  # DISABLED


@poly_mcp.tool(annotations=ToolAnnotations(readOnlyHint=True))
async def list_futures_schedules(
    session_end_date: Optional[str] = None,
    trading_venue: Optional[str] = None,
    limit: Optional[int] = 10,
    fetch_all: Optional[bool] = True,
    sort: Optional[str] = None,
    params: Optional[Dict[str, Any]] = None,
) -> str:
    """
    Get trading schedules for multiple futures products on a specific date.

    Parameters:
    - session_end_date: Filter by session end date (YYYY-MM-DD)
    - trading_venue: Filter by trading venue
    - limit: Number of results per page (default: 10)
    - fetch_all: If True (recommended), fetch ALL data and cache to disk for DuckDB queries (default: True)

    RECOMMENDED: Always use fetch_all=True to cache complete schedule data locally for efficient DuckDB analysis.

    Example: list_futures_schedules(session_end_date="2025-01-15", fetch_all=True)
    """
    try:
        schedules_list = []

        if fetch_all:
            # Use iterator approach for automatic pagination
            for schedule in polygon_client.list_futures_schedules(
                session_end_date=session_end_date,
                trading_venue=trading_venue,
                limit=limit,
                sort=sort,
                params=params,
                raw=False,
            ):
                schedules_list.append(schedule.to_dict())
        else:
            # Single page approach
            results = polygon_client.list_futures_schedules(
                session_end_date=session_end_date,
                trading_venue=trading_venue,
                limit=limit,
                sort=sort,
                params=params,
                raw=True,
            )

            import json
            data = json.loads(results.data.decode("utf-8"))
            schedules_list = data.get("results", [])

        # Create data structure for JSON to CSV conversion
        data = {"results": schedules_list, "status": "OK"}

        # Convert to CSV
        csv_data = json_to_csv(data)

        # Process with intelligent caching
        return await process_tool_response(
            tool_name="list_futures_schedules",
            params={
                "session_end_date": session_end_date,
                "trading_venue": trading_venue,
                "limit": limit,
                "fetch_all": fetch_all,
            },
            csv_data=csv_data,
        )
    except Exception as e:
        return f"Error: {e}"


@poly_mcp.tool(annotations=ToolAnnotations(readOnlyHint=True))
async def list_futures_schedules_by_product_code(
    product_code: str,
    session_end_date: Optional[str] = None,
    session_end_date_lt: Optional[str] = None,
    session_end_date_lte: Optional[str] = None,
    session_end_date_gt: Optional[str] = None,
    session_end_date_gte: Optional[str] = None,
    limit: Optional[int] = 10,
    fetch_all: Optional[bool] = True,
    sort: Optional[str] = None,
    params: Optional[Dict[str, Any]] = None,
) -> str:
    """
    Get schedule data for a single futures product across many trading dates.

    Parameters:
    - product_code: Futures product code (e.g., "ES" for E-mini S&P 500)
    - session_end_date_gte: Filter schedules on or after this date (YYYY-MM-DD)
    - limit: Number of results per page (default: 10)
    - fetch_all: If True (recommended), fetch ALL data and cache to disk for DuckDB queries (default: True)

    RECOMMENDED: Always use fetch_all=True to cache complete schedule data locally for efficient DuckDB analysis.

    Example: list_futures_schedules_by_product_code("ES", fetch_all=True)
    """
    try:
        schedules_list = []

        if fetch_all:
            # Use iterator approach for automatic pagination
            for schedule in polygon_client.list_futures_schedules_by_product_code(
                product_code=product_code,
                session_end_date=session_end_date,
                session_end_date_lt=session_end_date_lt,
                session_end_date_lte=session_end_date_lte,
                session_end_date_gt=session_end_date_gt,
                session_end_date_gte=session_end_date_gte,
                limit=limit,
                sort=sort,
                params=params,
                raw=False,
            ):
                schedules_list.append(schedule.to_dict())
        else:
            # Single page approach
            results = polygon_client.list_futures_schedules_by_product_code(
                product_code=product_code,
                session_end_date=session_end_date,
                session_end_date_lt=session_end_date_lt,
                session_end_date_lte=session_end_date_lte,
                session_end_date_gt=session_end_date_gt,
                session_end_date_gte=session_end_date_gte,
                limit=limit,
                sort=sort,
                params=params,
                raw=True,
            )

            import json
            data = json.loads(results.data.decode("utf-8"))
            schedules_list = data.get("results", [])

        # Create data structure for JSON to CSV conversion
        data = {"results": schedules_list, "status": "OK"}

        # Convert to CSV
        csv_data = json_to_csv(data)

        # Process with intelligent caching
        return await process_tool_response(
            tool_name="list_futures_schedules_by_product_code",
            params={
                "product_code": product_code,
                "limit": limit,
                "fetch_all": fetch_all,
            },
            csv_data=csv_data,
        )
    except Exception as e:
        return f"Error: {e}"


@poly_mcp.tool(annotations=ToolAnnotations(readOnlyHint=True))
async def list_futures_market_statuses(
    product_code_any_of: Optional[str] = None,
    product_code: Optional[str] = None,
    limit: Optional[int] = 10,
    fetch_all: Optional[bool] = True,
    sort: Optional[str] = None,
    params: Optional[Dict[str, Any]] = None,
) -> str:
    """
    Get market statuses for futures products (open, closed, early close, etc.).

    Parameters:
    - product_code: Filter by specific product code (e.g., "ES")
    - product_code_any_of: Filter by multiple product codes (comma-separated)
    - limit: Number of results per page (default: 10)
    - fetch_all: If True (recommended), fetch ALL data and cache to disk for DuckDB queries (default: True)

    RECOMMENDED: Always use fetch_all=True to cache complete market status data locally for efficient DuckDB analysis.

    Example: list_futures_market_statuses(product_code="ES", fetch_all=True)
    """
    try:
        statuses_list = []

        if fetch_all:
            # Use iterator approach for automatic pagination
            for status in polygon_client.list_futures_market_statuses(
                product_code_any_of=product_code_any_of,
                product_code=product_code,
                limit=limit,
                sort=sort,
                params=params,
                raw=False,
            ):
                statuses_list.append(status.to_dict())
        else:
            # Single page approach
            results = polygon_client.list_futures_market_statuses(
                product_code_any_of=product_code_any_of,
                product_code=product_code,
                limit=limit,
                sort=sort,
                params=params,
                raw=True,
            )

            import json
            data = json.loads(results.data.decode("utf-8"))
            statuses_list = data.get("results", [])

        # Create data structure for JSON to CSV conversion
        data = {"results": statuses_list, "status": "OK"}

        # Convert to CSV
        csv_data = json_to_csv(data)

        # Process with intelligent caching
        return await process_tool_response(
            tool_name="list_futures_market_statuses",
            params={
                "product_code": product_code,
                "limit": limit,
                "fetch_all": fetch_all,
            },
            csv_data=csv_data,
        )
    except Exception as e:
        return f"Error: {e}"


@poly_mcp.tool(annotations=ToolAnnotations(readOnlyHint=True))
async def get_futures_snapshot(
    ticker: Optional[str] = None,
    ticker_any_of: Optional[str] = None,
    ticker_gt: Optional[str] = None,
    ticker_gte: Optional[str] = None,
    ticker_lt: Optional[str] = None,
    ticker_lte: Optional[str] = None,
    product_code: Optional[str] = None,
    product_code_any_of: Optional[str] = None,
    product_code_gt: Optional[str] = None,
    product_code_gte: Optional[str] = None,
    product_code_lt: Optional[str] = None,
    product_code_lte: Optional[str] = None,
    limit: Optional[int] = 10,
    sort: Optional[str] = None,
    params: Optional[Dict[str, Any]] = None,
) -> str:
    """
    Get snapshots for futures contracts.
    """
    try:
        results = polygon_client.get_futures_snapshot(
            ticker=ticker,
            ticker_any_of=ticker_any_of,
            ticker_gt=ticker_gt,
            ticker_gte=ticker_gte,
            ticker_lt=ticker_lt,
            ticker_lte=ticker_lte,
            product_code=product_code,
            product_code_any_of=product_code_any_of,
            product_code_gt=product_code_gt,
            product_code_gte=product_code_gte,
            product_code_lt=product_code_lt,
            product_code_lte=product_code_lte,
            limit=limit,
            sort=sort,
            params=params,
            raw=True,
        )

        return json_to_csv(results.data.decode("utf-8"))
    except Exception as e:
        return f"Error: {e}"


# Directly expose the MCP server object
# It will be run from entrypoint.py
