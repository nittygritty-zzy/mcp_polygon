"""Auto-generated tool definitions."""

from typing import Optional, Any, Dict, Union
from mcp.types import ToolAnnotations
from datetime import datetime, date
from ..clients import poly_mcp, polygon_client
from ..formatters import json_to_csv
from ..tool_integration import process_tool_response


@poly_mcp.tool(annotations=ToolAnnotations(readOnlyHint=True))
async def list_treasury_yields(
    date: Optional[Union[str, datetime, date]] = None,
    date_any_of: Optional[str] = None,
    date_lt: Optional[Union[str, datetime, date]] = None,
    date_lte: Optional[Union[str, datetime, date]] = None,
    date_gt: Optional[Union[str, datetime, date]] = None,
    date_gte: Optional[Union[str, datetime, date]] = None,
    limit: Optional[int] = 10,
    sort: Optional[str] = None,
    order: Optional[str] = None,
    params: Optional[Dict[str, Any]] = None,
) -> str:
    """
    Get historical U.S. Treasury yield data for maturities from 1-month to 30-years (data back to 1962).

    Reference: https://polygon.io/docs/rest/economy/treasury-yields

    Parameters:
    - date_gte: Filter dates >= this date (YYYY-MM-DD)
    - limit: Number of results (default: 10, max: 50000)

    Example: list_treasury_yields(date_gte="2025-01-01", limit=100)
    Example: list_treasury_yields(date="2025-03-15")

    Returns: yield_1_month through yield_30_year. Inverted curve (short > long) signals recession risk.
    """
    try:
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

        # Convert to CSV
        csv_data = json_to_csv(results.data.decode("utf-8"))

        # Process with intelligent caching
        return await process_tool_response(
            tool_name="list_treasury_yields",
            params={
                "date_gte": date_gte,
                "limit": limit,
            },
            csv_data=csv_data,
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
    sort: Optional[str] = None,
    params: Optional[Dict[str, Any]] = None,
) -> str:
    """
    Get U.S. inflation indicators including CPI and PCE indexes with historical data.

    Reference: https://polygon.io/docs/rest/economy/inflation

    Parameters:
    - date_gte: Filter dates >= this date (YYYY-MM-DD)
    - limit: Number of results (default: 10, max: 50000)

    Example: list_inflation(date_gte="2024-01-01", limit=12)
    Example: list_inflation(date="2025-06-01")

    Returns: CPI, CPI Core, PCE, PCE Core (Fed's preferred measure), YoY changes. Fed targets 2% PCE Core.
    """
    try:
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

        # Convert to CSV
        csv_data = json_to_csv(results.data.decode("utf-8"))

        # Process with intelligent caching
        return await process_tool_response(
            tool_name="list_inflation",
            params={
                "date_gte": date_gte,
                "limit": limit,
            },
            csv_data=csv_data,
        )
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
    sort: Optional[str] = None,
    params: Optional[Dict[str, Any]] = None,
) -> str:
    """
    Get U.S. inflation expectations from markets (TIPS breakeven) and models (Cleveland Fed).

    Reference: https://polygon.io/docs/rest/economy/inflation-expectations

    Parameters:
    - date_gte: Filter dates >= this date (YYYY-MM-DD)
    - limit: Number of results (default: 100, max: 50000)

    Example: list_inflation_expectations(date_gte="2024-01-01", limit=250)
    Example: list_inflation_expectations(date="2025-06-17")

    Returns: market_5_year, market_10_year (TIPS breakeven), model_1/5/10/30_year (Cleveland Fed), forward_years_5_to_10.
    """
    try:
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
        if limit:
            request_params["limit"] = limit
        if sort:
            request_params["sort"] = sort

        # Make the request to the inflation expectations endpoint
        results = polygon_client._get(
            "/fed/v1/inflation-expectations", params=request_params
        )

        # Convert to CSV
        csv_data = json_to_csv(results)

        # Process with intelligent caching
        return await process_tool_response(
            tool_name="list_inflation_expectations",
            params={
                "date_gte": date_gte,
                "limit": limit,
            },
            csv_data=csv_data,
        )
    except Exception as e:
        return f"Error: {e}"
