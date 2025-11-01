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
async def list_treasury_yields(
    date: Optional[Union[str, datetime, date]] = None,
    date_any_of: Optional[str] = None,
    date_lt: Optional[Union[str, datetime, date]] = None,
    date_lte: Optional[Union[str, datetime, date]] = None,
    date_gt: Optional[Union[str, datetime, date]] = None,
    date_gte: Optional[Union[str, datetime, date]] = None,
    limit: Optional[int] = 10,
    fetch_all: Optional[bool] = True,
    sort: Optional[str] = None,
    order: Optional[str] = None,
    params: Optional[Dict[str, Any]] = None,
) -> str:
    """
    Get historical U.S. Treasury yield data for maturities from 1-month to 30-years (data back to 1962).

    Reference: https://polygon.io/docs/rest/economy/treasury-yields

    Parameters:
    - date_gte: Filter dates >= this date (YYYY-MM-DD)
    - limit: Number of results per page (default: 10, max: 50000)
    - fetch_all: If True (recommended), fetch ALL data and cache to disk for DuckDB queries (default: True)

    RECOMMENDED: Always use fetch_all=True to cache complete treasury yield data locally for efficient DuckDB analysis.

    Example: list_treasury_yields(date_gte="2025-01-01", fetch_all=True)
    Example: list_treasury_yields(date="2025-03-15", fetch_all=True)

    Returns: yield_1_month through yield_30_year. Inverted curve (short > long) signals recession risk.
    """
    try:
        tool_params = build_params(
            date_gte=date_gte,
            limit=limit,
            fetch_all=fetch_all,
        )

        if fetch_all:
            # Use batch writing for memory efficiency
            batch_callback, finalize = create_batch_writer(
                "list_treasury_yields", tool_params
            )

            if batch_callback:
                # Streaming mode - write batches to disk incrementally
                fetcher = PolygonParallelFetcher(polygon_client, num_workers=5)
                await fetcher.fetch_all(
                    method_name="list_treasury_yields",
                    batch_callback=batch_callback,
                    date=date,
                    date_lt=date_lt,
                    date_lte=date_lte,
                    date_gt=date_gt,
                    date_gte=date_gte,
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
                yields_list = await fetcher.fetch_all(
                    method_name="list_treasury_yields",
                    date=date,
                    date_lt=date_lt,
                    date_lte=date_lte,
                    date_gt=date_gt,
                    date_gte=date_gte,
                    limit=limit,
                    sort=sort,
                    order=order,
                    params=params,
                )
                csv_data = json_to_csv({"results": yields_list})
                return await process_tool_response(
                    "list_treasury_yields", tool_params, csv_data
                )
        else:
            # Single page approach
            results = polygon_client.list_treasury_yields(
                date=date,
                date_lt=date_lt,
                date_lte=date_lte,
                date_gt=date_gt,
                date_gte=date_gte,
                limit=limit,
                sort=sort,
                order=order,
                params=params,
                raw=True,
            )

            import json

            data = json.loads(results.data.decode("utf-8"))
            yields_list = data.get("results", [])

            # Create data structure for JSON to CSV conversion
            data = {"results": yields_list, "status": "OK"}

            # Convert to CSV
            csv_data = json_to_csv(data)

            # Process with intelligent caching
            return await process_tool_response(
                "list_treasury_yields", tool_params, csv_data
            )
    except Exception as e:
        return f"Error: {e}"


@poly_mcp.tool(annotations=ToolAnnotations(readOnlyHint=True))
async def list_inflation(
    date: Optional[Union[str, datetime, date]] = None,
    date_any_of: Optional[str] = None,
    date_gt: Optional[Union[str, datetime, date]] = None,
    date_gte: Optional[Union[str, datetime, date]] = None,
    date_lt: Optional[Union[str, datetime, date]] = None,
    date_lte: Optional[Union[str, datetime, date]] = None,
    limit: Optional[int] = 10,
    fetch_all: Optional[bool] = True,
    sort: Optional[str] = None,
    params: Optional[Dict[str, Any]] = None,
) -> str:
    """
    Get U.S. inflation indicators including CPI and PCE indexes with historical data.

    Reference: https://polygon.io/docs/rest/economy/inflation

    Parameters:
    - date_gte: Filter dates >= this date (YYYY-MM-DD)
    - limit: Number of results per page (default: 10, max: 50000)
    - fetch_all: If True (recommended), fetch ALL data and cache to disk for DuckDB queries (default: True)

    RECOMMENDED: Always use fetch_all=True to cache complete inflation data locally for efficient DuckDB analysis.

    Example: list_inflation(date_gte="2024-01-01", fetch_all=True)
    Example: list_inflation(date="2025-06-01", fetch_all=True)

    Returns: CPI, CPI Core, PCE, PCE Core (Fed's preferred measure), YoY changes. Fed targets 2% PCE Core.
    """
    try:
        tool_params = build_params(
            date_gte=date_gte,
            limit=limit,
            fetch_all=fetch_all,
        )

        if fetch_all:
            # Use batch writing for memory efficiency
            batch_callback, finalize = create_batch_writer(
                "list_inflation", tool_params
            )

            if batch_callback:
                # Streaming mode - write batches to disk incrementally
                fetcher = PolygonParallelFetcher(polygon_client, num_workers=5)
                await fetcher.fetch_all(
                    method_name="list_inflation",
                    batch_callback=batch_callback,
                    date=date,
                    date_any_of=date_any_of,
                    date_gt=date_gt,
                    date_gte=date_gte,
                    date_lt=date_lt,
                    date_lte=date_lte,
                    limit=limit,
                    sort=sort,
                    params=params,
                )
                # Finalize and return cache metadata
                return await finalize()
            else:
                # Memory mode (fallback if batch writing not available)
                fetcher = PolygonParallelFetcher(polygon_client, num_workers=5)
                inflation_list = await fetcher.fetch_all(
                    method_name="list_inflation",
                    date=date,
                    date_any_of=date_any_of,
                    date_gt=date_gt,
                    date_gte=date_gte,
                    date_lt=date_lt,
                    date_lte=date_lte,
                    limit=limit,
                    sort=sort,
                    params=params,
                )
                csv_data = json_to_csv({"results": inflation_list})
                return await process_tool_response(
                    "list_inflation", tool_params, csv_data
                )
        else:
            # Single page approach
            results = polygon_client.list_inflation(
                date=date,
                date_any_of=date_any_of,
                date_gt=date_gt,
                date_gte=date_gte,
                date_lt=date_lt,
                date_lte=date_lte,
                limit=limit,
                sort=sort,
                params=params,
                raw=True,
            )

            import json

            data = json.loads(results.data.decode("utf-8"))
            inflation_list = data.get("results", [])

            # Create data structure for JSON to CSV conversion
            data = {"results": inflation_list, "status": "OK"}

            # Convert to CSV
            csv_data = json_to_csv(data)

            # Process with intelligent caching
            return await process_tool_response("list_inflation", tool_params, csv_data)
    except Exception as e:
        return f"Error: {e}"


@poly_mcp.tool(annotations=ToolAnnotations(readOnlyHint=True))
async def list_inflation_expectations(
    date: Optional[Union[str, datetime, date]] = None,
    date_any_of: Optional[str] = None,
    date_gt: Optional[Union[str, datetime, date]] = None,
    date_gte: Optional[Union[str, datetime, date]] = None,
    date_lt: Optional[Union[str, datetime, date]] = None,
    date_lte: Optional[Union[str, datetime, date]] = None,
    limit: Optional[int] = 100,
    fetch_all: Optional[bool] = True,
    sort: Optional[str] = None,
    params: Optional[Dict[str, Any]] = None,
) -> str:
    """
    Get U.S. inflation expectations from markets (TIPS breakeven) and models (Cleveland Fed).

    Reference: https://polygon.io/docs/rest/economy/inflation-expectations

    Parameters:
    - date_gte: Filter dates >= this date (YYYY-MM-DD)
    - limit: Number of results per page (default: 100, max: 50000)
    - fetch_all: If True (recommended), fetch ALL data and cache to disk for DuckDB queries (default: True)

    RECOMMENDED: Always use fetch_all=True to cache complete inflation expectations data locally for efficient DuckDB analysis.

    Example: list_inflation_expectations(date_gte="2024-01-01", fetch_all=True)
    Example: list_inflation_expectations(date="2025-06-17", fetch_all=True)

    Returns: market_5_year, market_10_year (TIPS breakeven), model_1/5/10/30_year (Cleveland Fed), forward_years_5_to_10.
    """
    try:
        tool_params = build_params(
            date_gte=date_gte,
            limit=limit,
            fetch_all=fetch_all,
        )

        # Build the params dictionary
        request_params = params or {}
        if date:
            request_params["date"] = date
        if date_any_of:
            request_params["date.any_of"] = date_any_of
        if date_gt:
            request_params["date.gt"] = date_gt
        if date_gte:
            request_params["date.gte"] = date_gte
        if date_lt:
            request_params["date.lt"] = date_lt
        if date_lte:
            request_params["date.lte"] = date_lte
        if sort:
            request_params["sort"] = sort

        if fetch_all:
            # Use batch writing for memory efficiency
            batch_callback, finalize = create_batch_writer(
                "list_inflation_expectations", tool_params
            )

            if batch_callback:
                # Streaming mode - write batches to disk incrementally
                fetcher = PolygonParallelFetcher(polygon_client, num_workers=5)
                await fetcher.fetch_all(
                    method_name="list_inflation_expectations",
                    batch_callback=batch_callback,
                    date=date,
                    date_any_of=date_any_of,
                    date_gt=date_gt,
                    date_gte=date_gte,
                    date_lt=date_lt,
                    date_lte=date_lte,
                    limit=limit,
                    sort=sort,
                    params=params,
                )
                # Finalize and return cache metadata
                return await finalize()
            else:
                # Memory mode (fallback if batch writing not available)
                request_params["limit"] = 50000
                results = polygon_client._get(
                    "/fed/v1/inflation-expectations", params=request_params
                )

                import json

                if isinstance(results, str):
                    data = json.loads(results)
                else:
                    data = results

                expectations_list = data.get("results", [])
                csv_data = json_to_csv({"results": expectations_list})
                return await process_tool_response(
                    "list_inflation_expectations", tool_params, csv_data
                )
        else:
            # Single page approach with specified limit
            request_params["limit"] = limit

            # Make the request to the inflation expectations endpoint
            results = polygon_client._get(
                "/fed/v1/inflation-expectations", params=request_params
            )

            import json

            if isinstance(results, str):
                data = json.loads(results)
            else:
                data = results

            expectations_list = data.get("results", [])

            # Create data structure for JSON to CSV conversion
            data = {"results": expectations_list, "status": "OK"}

            # Convert to CSV
            csv_data = json_to_csv(data)

            # Process with intelligent caching
            return await process_tool_response(
                "list_inflation_expectations", tool_params, csv_data
            )
    except Exception as e:
        return f"Error: {e}"
