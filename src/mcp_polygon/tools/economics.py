"""Auto-generated tool definitions."""

from typing import Optional, Any, Dict, Union, List
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
    Retrieve historical U.S. Treasury yield data for standard maturities from 1-month to 30-years.
    Returns daily yield curves with historical records dating back to 1962, showing how interest rates change over time.

    Reference: https://polygon.io/docs/rest/economy/treasury-yields

    Treasury yields represent market interest rates for U.S. government debt at various maturities.
    The yield curve (plotting short-term to long-term rates) is a key economic indicator used to
    assess recession risk, inflation expectations, and Federal Reserve policy effectiveness.

    Parameters:
    - date: Filter by exact calendar date (YYYY-MM-DD)
    - date_any_of: Filter equal to any comma-separated dates
    - date_lt: Filter for dates less than this date
    - date_lte: Filter for dates less than or equal to this date
    - date_gt: Filter for dates greater than this date
    - date_gte: Filter for dates greater than or equal to this date
    - limit: Number of results to return (default: 10, max: 50000)
    - sort: Sort field (default: "date")
    - order: Sort order ("asc" or "desc")
    - params: Additional filtering parameters

    Available yield maturities in response:
    - Short-term: yield_1_month, yield_3_month, yield_6_month
    - Mid-term: yield_1_year, yield_2_year, yield_3_year, yield_5_year, yield_7_year
    - Long-term: yield_10_year, yield_20_year, yield_30_year

    Example: list_treasury_yields(date_gte="2025-01-01", limit=100)
             gets 100 days of yield data since January 2025
    Example: list_treasury_yields(date="2025-03-15")
             gets yield curve snapshot for a specific date
    Example: list_treasury_yields(date_gte="2020-01-01", date_lte="2020-12-31", limit=1000)
             gets entire 2020 yield curve history
    Example: list_treasury_yields(date_gte="2024-01-01", sort="date", order="desc")
             gets recent yields in reverse chronological order

    Note: Yield curve analysis is critical for economic forecasting:
    - Normal curve (long-term > short-term): Healthy economy expected
    - Inverted curve (short-term > long-term): Recession warning signal
    - Flat curve: Economic uncertainty or transition period
    The 10-year minus 2-year spread is widely watched as a recession predictor.
    Historical data back to 1962 enables long-term trend analysis and comparison to past economic cycles.
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
    Retrieve key U.S. inflation indicators including CPI and PCE indexes with historical data.
    Returns both headline and core inflation measures, tracking actual changes in consumer prices and spending behavior.

    Reference: https://polygon.io/docs/rest/economy/inflation

    Provides comprehensive inflation data essential for monetary policy analysis, purchasing power evaluation,
    and economic forecasting. Includes the Federal Reserve's preferred inflation measure (Core PCE).

    Parameters:
    - date: Filter by exact calendar date (YYYY-MM-DD)
    - date_any_of: Filter equal to any comma-separated dates
    - date_gt: Filter for dates greater than this date
    - date_gte: Filter for dates greater than or equal to this date
    - date_lt: Filter for dates less than this date
    - date_lte: Filter for dates less than or equal to this date
    - limit: Number of results to return (default: 10, max: 50000)
    - sort: Sort field (default: "date")
    - params: Additional filtering parameters

    Available inflation metrics in response:
    - CPI: Consumer Price Index (headline inflation - all urban consumers, fixed basket)
    - CPI Core: CPI excluding food and energy (underlying inflation trends)
    - CPI Year-over-Year: % change in CPI (most commonly cited inflation rate)
    - PCE: Personal Consumption Expenditures Price Index (broader measure, Fed uses this)
    - PCE Core: PCE excluding food and energy (Fed's PREFERRED inflation measure)
    - PCE Spending: Nominal consumer spending in billions (not inflation-adjusted)

    Example: list_inflation(date_gte="2024-01-01", limit=12)
             gets monthly inflation data for 2024
    Example: list_inflation(date="2025-06-01")
             gets inflation snapshot for a specific month
    Example: list_inflation(date_gte="2020-01-01", date_lte="2023-12-31", limit=1000)
             gets full inflation history for 2020-2023 period
    Example: list_inflation(date_gte="2022-01-01", sort="date", order="desc")
             gets recent inflation data in reverse chronological order

    Note: Understanding inflation measures:
    - CPI (Consumer Price Index): Fixed basket of goods/services, widely cited in media
    - PCE (Personal Consumption Expenditures): Captures changing spending patterns, Fed's preferred measure
    - Core inflation (excludes food/energy): Shows underlying trends without volatile components
    - Fed targets 2% PCE Core inflation for price stability
    - CPI typically runs ~0.3-0.5% higher than PCE due to methodology differences
    Year-over-year CPI is the most commonly referenced in public discourse and policy decisions.
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
    Retrieve U.S. inflation expectations from financial markets and economic models across multiple time horizons.
    Returns both market-based (TIPS breakeven) and model-based (Cleveland Fed) inflation outlook data.

    Reference: https://polygon.io/docs/rest/economy/inflation-expectations

    Inflation expectations are critical for understanding how investors and forecasters perceive future inflation risk.
    Unlike realized inflation (list_inflation), these forward-looking measures help predict Fed policy changes and
    assess market sentiment about price stability.

    Parameters:
    - date: Filter by exact calendar date (YYYY-MM-DD)
    - date_any_of: Filter equal to any comma-separated dates
    - date_gt: Filter for dates greater than this date
    - date_gte: Filter for dates greater than or equal to this date
    - date_lt: Filter for dates less than this date
    - date_lte: Filter for dates less than or equal to this date
    - limit: Number of results to return (default: 100, max: 50000)
    - sort: Sort field (default: "date")
    - params: Additional filtering parameters

    Available inflation expectation metrics in response:

    Market-based (TIPS Breakeven Rates):
    - market_5_year: 5-year breakeven inflation rate (5Y nominal yield - 5Y TIPS yield)
    - market_10_year: 10-year breakeven inflation rate (10Y nominal yield - 10Y TIPS yield)
    - forward_years_5_to_10: 5-year forward 5-year rate (inflation expected in years 5-10)

    Model-based (Cleveland Fed Estimates):
    - model_1_year: Cleveland Fed 1-year inflation expectation
    - model_5_year: Cleveland Fed 5-year inflation expectation
    - model_10_year: Cleveland Fed 10-year inflation expectation
    - model_30_year: Cleveland Fed 30-year inflation expectation

    Example: list_inflation_expectations(date_gte="2024-01-01", limit=250)
             gets inflation expectations since 2024
    Example: list_inflation_expectations(date="2025-06-17")
             gets inflation expectations snapshot for a specific date
    Example: list_inflation_expectations(date_gte="2020-01-01", date_lte="2023-12-31", limit=1000)
             gets full inflation expectations history for 2020-2023
    Example: list_inflation_expectations(date_gte="2023-01-01", sort="date", order="desc")
             gets recent inflation expectations in reverse order

    Note: Understanding inflation expectations:
    - Market breakeven rates: Derived from TIPS (Treasury Inflation-Protected Securities) spreads
    - Cleveland Fed model: Combines Treasury yields, inflation data, swaps, and surveys
    - 5Y5Y forward rate: Key Fed-watched indicator of long-term inflation anchoring
    - Well-anchored expectations (near 2%): Sign of Fed credibility
    - Rising expectations: May signal need for tighter monetary policy
    - Divergence between market and model: Can indicate risk premium or liquidity issues

    Use case: Compare market_10_year vs model_10_year to assess inflation risk premium.
    If market_10_year > model_10_year, investors demand extra compensation for inflation uncertainty.
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
