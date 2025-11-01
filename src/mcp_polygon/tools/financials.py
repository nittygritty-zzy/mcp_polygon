"""Auto-generated tool definitions."""

from typing import Optional, Any, Dict, Union
from mcp.types import ToolAnnotations
from datetime import datetime, date
from ..clients import poly_mcp, polygon_client
from ..formatters import json_to_csv
from ..tool_integration import process_tool_response, create_batch_writer
from ..parallel_fetcher import PolygonParallelFetcher


@poly_mcp.tool(annotations=ToolAnnotations(readOnlyHint=True))
async def list_stock_financials(
    ticker: Optional[str] = None,
    cik: Optional[str] = None,
    company_name: Optional[str] = None,
    company_name_search: Optional[str] = None,
    sic: Optional[str] = None,
    filing_date: Optional[Union[str, datetime, date]] = None,
    filing_date_lt: Optional[Union[str, datetime, date]] = None,
    filing_date_lte: Optional[Union[str, datetime, date]] = None,
    filing_date_gt: Optional[Union[str, datetime, date]] = None,
    filing_date_gte: Optional[Union[str, datetime, date]] = None,
    period_of_report_date: Optional[Union[str, datetime, date]] = None,
    period_of_report_date_lt: Optional[Union[str, datetime, date]] = None,
    period_of_report_date_lte: Optional[Union[str, datetime, date]] = None,
    period_of_report_date_gt: Optional[Union[str, datetime, date]] = None,
    period_of_report_date_gte: Optional[Union[str, datetime, date]] = None,
    timeframe: Optional[str] = None,
    include_sources: Optional[bool] = None,
    limit: Optional[int] = 10,
    fetch_all: Optional[bool] = True,
    sort: Optional[str] = None,
    order: Optional[str] = None,
    params: Optional[Dict[str, Any]] = None,
) -> str:
    """
    Get fundamental financial data for companies including balance sheets, income statements, and cash flow statements.
    Returns comprehensive financial data from SEC filings (10-K, 10-Q).

    Reference: https://polygon.io/docs/rest/stocks/fundamentals/balance-sheets

    Parameters:
    - ticker: Filter by ticker symbol (e.g., "AAPL", "MSFT")
    - cik: Filter by SEC Central Index Key (CIK)
    - timeframe: Filter by timeframe (annual, quarterly, ttm)
    - filing_date_gte/lte: Range filters for filing date
    - period_of_report_date_gte/lte: Range filters for period end date
    - limit: Number of results per page (default: 10)
    - fetch_all: If True (recommended), fetch ALL data and cache to disk for DuckDB queries (default: True)

    RECOMMENDED: Always use fetch_all=True to cache complete financial data locally for efficient DuckDB analysis.

    Example: list_stock_financials(ticker="AAPL", fetch_all=True)
    Example: list_stock_financials(ticker="MSFT", timeframe="annual", fetch_all=True)

    Returns: Balance sheet (assets, liabilities, equity), income statement (revenue, expenses, profit),
    and cash flow statement (operating, investing, financing activities) data from SEC filings.
    """
    try:
        tool_params = {
            "ticker": ticker,
            "timeframe": timeframe,
            "limit": limit,
            "fetch_all": fetch_all,
        }

        if fetch_all:
            # Use batch writing for memory efficiency
            batch_callback, finalize = create_batch_writer(
                "list_stock_financials", tool_params
            )

            if batch_callback:
                # Streaming mode - write batches to disk incrementally
                fetcher = PolygonParallelFetcher(polygon_client, num_workers=5)
                await fetcher.fetch_all(
                    method_name="list_stock_financials",
                    use_vx=True,
                    batch_callback=batch_callback,
                    ticker=ticker,
                    cik=cik,
                    company_name=company_name,
                    company_name_search=company_name_search,
                    sic=sic,
                    filing_date=filing_date,
                    filing_date_lt=filing_date_lt,
                    filing_date_lte=filing_date_lte,
                    filing_date_gt=filing_date_gt,
                    filing_date_gte=filing_date_gte,
                    period_of_report_date=period_of_report_date,
                    period_of_report_date_lt=period_of_report_date_lt,
                    period_of_report_date_lte=period_of_report_date_lte,
                    period_of_report_date_gt=period_of_report_date_gt,
                    period_of_report_date_gte=period_of_report_date_gte,
                    timeframe=timeframe,
                    include_sources=include_sources,
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
                financials_list = await fetcher.fetch_all(
                    method_name="list_stock_financials",
                    use_vx=True,
                    ticker=ticker,
                    cik=cik,
                    company_name=company_name,
                    company_name_search=company_name_search,
                    sic=sic,
                    filing_date=filing_date,
                    filing_date_lt=filing_date_lt,
                    filing_date_lte=filing_date_lte,
                    filing_date_gt=filing_date_gt,
                    filing_date_gte=filing_date_gte,
                    period_of_report_date=period_of_report_date,
                    period_of_report_date_lt=period_of_report_date_lt,
                    period_of_report_date_lte=period_of_report_date_lte,
                    period_of_report_date_gt=period_of_report_date_gt,
                    period_of_report_date_gte=period_of_report_date_gte,
                    timeframe=timeframe,
                    include_sources=include_sources,
                    limit=limit,
                    sort=sort,
                    order=order,
                    params=params,
                )
                csv_data = json_to_csv({"results": financials_list})
                return await process_tool_response(
                    "list_stock_financials", tool_params, csv_data
                )
        else:
            # Single page approach
            results = polygon_client.vx.list_stock_financials(
                ticker=ticker,
                cik=cik,
                company_name=company_name,
                company_name_search=company_name_search,
                sic=sic,
                filing_date=filing_date,
                filing_date_lt=filing_date_lt,
                filing_date_lte=filing_date_lte,
                filing_date_gt=filing_date_gt,
                filing_date_gte=filing_date_gte,
                period_of_report_date=period_of_report_date,
                period_of_report_date_lt=period_of_report_date_lt,
                period_of_report_date_lte=period_of_report_date_lte,
                period_of_report_date_gt=period_of_report_date_gt,
                period_of_report_date_gte=period_of_report_date_gte,
                timeframe=timeframe,
                include_sources=include_sources,
                limit=limit,
                sort=sort,
                order=order,
                params=params,
                raw=True,
            )

            import json

            data = json.loads(results.data.decode("utf-8"))
            financials_list = data.get("results", [])

            # Create data structure for JSON to CSV conversion
            data = {"results": financials_list, "status": "OK"}

            # Convert to CSV
            csv_data = json_to_csv(data)

            # Process with intelligent caching
            return await process_tool_response(
                "list_stock_financials", tool_params, csv_data
            )
    except Exception as e:
        return f"Error: {e}"


@poly_mcp.tool(annotations=ToolAnnotations(readOnlyHint=True))
async def list_financials_balance_sheets(
    cik: Optional[str] = None,
    tickers: Optional[str] = None,
    period_end: Optional[Union[str, datetime, date]] = None,
    filing_date: Optional[Union[str, datetime, date]] = None,
    fiscal_year: Optional[int] = None,
    fiscal_quarter: Optional[int] = None,
    timeframe: Optional[str] = None,
    cik_any_of: Optional[str] = None,
    cik_gt: Optional[str] = None,
    cik_gte: Optional[str] = None,
    cik_lt: Optional[str] = None,
    cik_lte: Optional[str] = None,
    tickers_all_of: Optional[str] = None,
    tickers_any_of: Optional[str] = None,
    period_end_gt: Optional[Union[str, datetime, date]] = None,
    period_end_gte: Optional[Union[str, datetime, date]] = None,
    period_end_lt: Optional[Union[str, datetime, date]] = None,
    period_end_lte: Optional[Union[str, datetime, date]] = None,
    filing_date_gt: Optional[Union[str, datetime, date]] = None,
    filing_date_gte: Optional[Union[str, datetime, date]] = None,
    filing_date_lt: Optional[Union[str, datetime, date]] = None,
    filing_date_lte: Optional[Union[str, datetime, date]] = None,
    fiscal_year_gt: Optional[int] = None,
    fiscal_year_gte: Optional[int] = None,
    fiscal_year_lt: Optional[int] = None,
    fiscal_year_lte: Optional[int] = None,
    fiscal_quarter_gt: Optional[int] = None,
    fiscal_quarter_gte: Optional[int] = None,
    fiscal_quarter_lt: Optional[int] = None,
    fiscal_quarter_lte: Optional[int] = None,
    timeframe_any_of: Optional[str] = None,
    timeframe_gt: Optional[str] = None,
    timeframe_gte: Optional[str] = None,
    timeframe_lt: Optional[str] = None,
    timeframe_lte: Optional[str] = None,
    limit: Optional[int] = 100,
    sort: Optional[str] = None,
    params: Optional[Dict[str, Any]] = None,
) -> str:
    """
    Get balance sheet data from SEC filings with assets, liabilities, and equity breakdowns.

    Reference: https://polygon.io/docs/rest/stocks/fundamentals/balance-sheets

    Parameters:
    - tickers: Ticker symbol (e.g., "AAPL")
    - cik: SEC Central Index Key
    - fiscal_year/fiscal_quarter: Fiscal period filters
    - fiscal_year_gte/lte: Year range filters
    - timeframe: "quarterly" or "annual"
    - period_end_gte/lte: Date range filters (YYYY-MM-DD)
    - limit: Number of results (default: 100, max: 50000)

    Example: list_financials_balance_sheets(tickers="AAPL", limit=1)
    Example: list_financials_balance_sheets(tickers="MSFT", timeframe="annual", fiscal_year_gte=2020)

    Returns: Assets, Liabilities, Equity. Formula: Assets = Liabilities + Equity. Point-in-time snapshots from 10-K/10-Q.
    """
    try:
        # Build the params dictionary with range parameters
        results = polygon_client._get(
            "/stocks/financials/v1/balance-sheets",
            params={
                **(params or {}),
                **{
                    k: v
                    for k, v in {
                        "cik": cik,
                        "tickers": tickers,
                        "period_end": period_end,
                        "filing_date": filing_date,
                        "fiscal_year": fiscal_year,
                        "fiscal_quarter": fiscal_quarter,
                        "timeframe": timeframe,
                        "cik.any_of": cik_any_of,
                        "cik.gt": cik_gt,
                        "cik.gte": cik_gte,
                        "cik.lt": cik_lt,
                        "cik.lte": cik_lte,
                        "tickers.all_of": tickers_all_of,
                        "tickers.any_of": tickers_any_of,
                        "period_end.gt": period_end_gt,
                        "period_end.gte": period_end_gte,
                        "period_end.lt": period_end_lt,
                        "period_end.lte": period_end_lte,
                        "filing_date.gt": filing_date_gt,
                        "filing_date.gte": filing_date_gte,
                        "filing_date.lt": filing_date_lt,
                        "filing_date.lte": filing_date_lte,
                        "fiscal_year.gt": fiscal_year_gt,
                        "fiscal_year.gte": fiscal_year_gte,
                        "fiscal_year.lt": fiscal_year_lt,
                        "fiscal_year.lte": fiscal_year_lte,
                        "fiscal_quarter.gt": fiscal_quarter_gt,
                        "fiscal_quarter.gte": fiscal_quarter_gte,
                        "fiscal_quarter.lt": fiscal_quarter_lt,
                        "fiscal_quarter.lte": fiscal_quarter_lte,
                        "timeframe.any_of": timeframe_any_of,
                        "timeframe.gt": timeframe_gt,
                        "timeframe.gte": timeframe_gte,
                        "timeframe.lt": timeframe_lt,
                        "timeframe.lte": timeframe_lte,
                        "limit": limit,
                        "sort": sort,
                    }.items()
                    if v is not None
                },
            },
        )

        # Convert to CSV
        csv_data = json_to_csv(results)

        # Process with intelligent caching
        return await process_tool_response(
            tool_name="list_financials_balance_sheets",
            params={
                "tickers": tickers,
                "timeframe": timeframe,
                "limit": limit,
            },
            csv_data=csv_data,
        )
    except Exception as e:
        return f"Error: {e}"


@poly_mcp.tool(annotations=ToolAnnotations(readOnlyHint=True))
async def list_financials_cash_flow_statements(
    cik: Optional[str] = None,
    tickers: Optional[str] = None,
    period_end: Optional[Union[str, datetime, date]] = None,
    filing_date: Optional[Union[str, datetime, date]] = None,
    fiscal_year: Optional[int] = None,
    fiscal_quarter: Optional[int] = None,
    timeframe: Optional[str] = None,
    cik_any_of: Optional[str] = None,
    cik_gt: Optional[str] = None,
    cik_gte: Optional[str] = None,
    cik_lt: Optional[str] = None,
    cik_lte: Optional[str] = None,
    period_end_gt: Optional[Union[str, datetime, date]] = None,
    period_end_gte: Optional[Union[str, datetime, date]] = None,
    period_end_lt: Optional[Union[str, datetime, date]] = None,
    period_end_lte: Optional[Union[str, datetime, date]] = None,
    filing_date_gt: Optional[Union[str, datetime, date]] = None,
    filing_date_gte: Optional[Union[str, datetime, date]] = None,
    filing_date_lt: Optional[Union[str, datetime, date]] = None,
    filing_date_lte: Optional[Union[str, datetime, date]] = None,
    tickers_all_of: Optional[str] = None,
    tickers_any_of: Optional[str] = None,
    fiscal_year_gt: Optional[int] = None,
    fiscal_year_gte: Optional[int] = None,
    fiscal_year_lt: Optional[int] = None,
    fiscal_year_lte: Optional[int] = None,
    fiscal_quarter_gt: Optional[int] = None,
    fiscal_quarter_gte: Optional[int] = None,
    fiscal_quarter_lt: Optional[int] = None,
    fiscal_quarter_lte: Optional[int] = None,
    timeframe_any_of: Optional[str] = None,
    timeframe_gt: Optional[str] = None,
    timeframe_gte: Optional[str] = None,
    timeframe_lt: Optional[str] = None,
    timeframe_lte: Optional[str] = None,
    limit: Optional[int] = 100,
    sort: Optional[str] = None,
    params: Optional[Dict[str, Any]] = None,
) -> str:
    """
    Get cash flow statement data from SEC filings with operating, investing, and financing activities.

    Reference: https://polygon.io/docs/rest/stocks/fundamentals/cash-flow-statements

    Parameters:
    - tickers: Ticker symbol (e.g., "AAPL")
    - cik: SEC Central Index Key
    - fiscal_year/fiscal_quarter: Fiscal period filters
    - fiscal_year_gte/lte: Year range filters
    - timeframe: "quarterly", "annual", or "trailing_twelve_months"
    - period_end_gte/lte: Date range filters (YYYY-MM-DD)
    - limit: Number of results (default: 100, max: 50000)

    Example: list_financials_cash_flow_statements(tickers="AAPL", limit=1)
    Example: list_financials_cash_flow_statements(tickers="MSFT", timeframe="annual", fiscal_year_gte=2020)

    Returns: Operating CF, Investing CF, Financing CF. Formula: Change in Cash = Operating + Investing + Financing + FX. Period data from 10-K/10-Q.
    """
    try:
        # Build the params dictionary with range parameters
        results = polygon_client._get(
            "/stocks/financials/v1/cash-flow-statements",
            params={
                **(params or {}),
                **{
                    k: v
                    for k, v in {
                        "cik": cik,
                        "tickers": tickers,
                        "period_end": period_end,
                        "filing_date": filing_date,
                        "fiscal_year": fiscal_year,
                        "fiscal_quarter": fiscal_quarter,
                        "timeframe": timeframe,
                        "cik.any_of": cik_any_of,
                        "cik.gt": cik_gt,
                        "cik.gte": cik_gte,
                        "cik.lt": cik_lt,
                        "cik.lte": cik_lte,
                        "period_end.gt": period_end_gt,
                        "period_end.gte": period_end_gte,
                        "period_end.lt": period_end_lt,
                        "period_end.lte": period_end_lte,
                        "filing_date.gt": filing_date_gt,
                        "filing_date.gte": filing_date_gte,
                        "filing_date.lt": filing_date_lt,
                        "filing_date.lte": filing_date_lte,
                        "tickers.all_of": tickers_all_of,
                        "tickers.any_of": tickers_any_of,
                        "fiscal_year.gt": fiscal_year_gt,
                        "fiscal_year.gte": fiscal_year_gte,
                        "fiscal_year.lt": fiscal_year_lt,
                        "fiscal_year.lte": fiscal_year_lte,
                        "fiscal_quarter.gt": fiscal_quarter_gt,
                        "fiscal_quarter.gte": fiscal_quarter_gte,
                        "fiscal_quarter.lt": fiscal_quarter_lt,
                        "fiscal_quarter.lte": fiscal_quarter_lte,
                        "timeframe.any_of": timeframe_any_of,
                        "timeframe.gt": timeframe_gt,
                        "timeframe.gte": timeframe_gte,
                        "timeframe.lt": timeframe_lt,
                        "timeframe.lte": timeframe_lte,
                        "limit": limit,
                        "sort": sort,
                    }.items()
                    if v is not None
                },
            },
        )

        # Convert to CSV
        csv_data = json_to_csv(results)

        # Process with intelligent caching
        return await process_tool_response(
            tool_name="list_financials_cash_flow_statements",
            params={
                "tickers": tickers,
                "timeframe": timeframe,
                "limit": limit,
            },
            csv_data=csv_data,
        )
    except Exception as e:
        return f"Error: {e}"


@poly_mcp.tool(annotations=ToolAnnotations(readOnlyHint=True))
async def list_financials_income_statements(
    cik: Optional[str] = None,
    tickers: Optional[str] = None,
    period_end: Optional[Union[str, datetime, date]] = None,
    filing_date: Optional[Union[str, datetime, date]] = None,
    fiscal_year: Optional[int] = None,
    fiscal_quarter: Optional[int] = None,
    timeframe: Optional[str] = None,
    cik_any_of: Optional[str] = None,
    cik_gt: Optional[str] = None,
    cik_gte: Optional[str] = None,
    cik_lt: Optional[str] = None,
    cik_lte: Optional[str] = None,
    tickers_all_of: Optional[str] = None,
    tickers_any_of: Optional[str] = None,
    period_end_gt: Optional[Union[str, datetime, date]] = None,
    period_end_gte: Optional[Union[str, datetime, date]] = None,
    period_end_lt: Optional[Union[str, datetime, date]] = None,
    period_end_lte: Optional[Union[str, datetime, date]] = None,
    filing_date_gt: Optional[Union[str, datetime, date]] = None,
    filing_date_gte: Optional[Union[str, datetime, date]] = None,
    filing_date_lt: Optional[Union[str, datetime, date]] = None,
    filing_date_lte: Optional[Union[str, datetime, date]] = None,
    fiscal_year_gt: Optional[int] = None,
    fiscal_year_gte: Optional[int] = None,
    fiscal_year_lt: Optional[int] = None,
    fiscal_year_lte: Optional[int] = None,
    fiscal_quarter_gt: Optional[int] = None,
    fiscal_quarter_gte: Optional[int] = None,
    fiscal_quarter_lt: Optional[int] = None,
    fiscal_quarter_lte: Optional[int] = None,
    timeframe_any_of: Optional[str] = None,
    timeframe_gt: Optional[str] = None,
    timeframe_gte: Optional[str] = None,
    timeframe_lt: Optional[str] = None,
    timeframe_lte: Optional[str] = None,
    limit: Optional[int] = 100,
    sort: Optional[str] = None,
    params: Optional[Dict[str, Any]] = None,
) -> str:
    """
    Get income statement data from SEC filings with revenue, expenses, and profitability metrics.

    Reference: https://polygon.io/docs/rest/stocks/fundamentals/income-statements

    Parameters:
    - tickers: Ticker symbol (e.g., "AAPL")
    - cik: SEC Central Index Key
    - fiscal_year/fiscal_quarter: Fiscal period filters
    - fiscal_year_gte/lte: Year range filters
    - timeframe: "quarterly", "annual", or "trailing_twelve_months"
    - period_end_gte/lte: Date range filters (YYYY-MM-DD)
    - limit: Number of results (default: 100, max: 50000)

    Example: list_financials_income_statements(tickers="AAPL", limit=1)
    Example: list_financials_income_statements(tickers="MSFT", timeframe="annual", fiscal_year_gte=2020)

    Returns: Revenue, Gross Profit, Operating Income, Net Income, EPS. Formula: Net Income = Revenue - COGS - Operating Expenses - Taxes. Period data from 10-K/10-Q.
    """
    try:
        # Build the params dictionary with range parameters
        results = polygon_client._get(
            "/stocks/financials/v1/income-statements",
            params={
                **(params or {}),
                **{
                    k: v
                    for k, v in {
                        "cik": cik,
                        "tickers": tickers,
                        "period_end": period_end,
                        "filing_date": filing_date,
                        "fiscal_year": fiscal_year,
                        "fiscal_quarter": fiscal_quarter,
                        "timeframe": timeframe,
                        "cik.any_of": cik_any_of,
                        "cik.gt": cik_gt,
                        "cik.gte": cik_gte,
                        "cik.lt": cik_lt,
                        "cik.lte": cik_lte,
                        "tickers.all_of": tickers_all_of,
                        "tickers.any_of": tickers_any_of,
                        "period_end.gt": period_end_gt,
                        "period_end.gte": period_end_gte,
                        "period_end.lt": period_end_lt,
                        "period_end.lte": period_end_lte,
                        "filing_date.gt": filing_date_gt,
                        "filing_date.gte": filing_date_gte,
                        "filing_date.lt": filing_date_lt,
                        "filing_date.lte": filing_date_lte,
                        "fiscal_year.gt": fiscal_year_gt,
                        "fiscal_year.gte": fiscal_year_gte,
                        "fiscal_year.lt": fiscal_year_lt,
                        "fiscal_year.lte": fiscal_year_lte,
                        "fiscal_quarter.gt": fiscal_quarter_gt,
                        "fiscal_quarter.gte": fiscal_quarter_gte,
                        "fiscal_quarter.lt": fiscal_quarter_lt,
                        "fiscal_quarter.lte": fiscal_quarter_lte,
                        "timeframe.any_of": timeframe_any_of,
                        "timeframe.gt": timeframe_gt,
                        "timeframe.gte": timeframe_gte,
                        "timeframe.lt": timeframe_lt,
                        "timeframe.lte": timeframe_lte,
                        "limit": limit,
                        "sort": sort,
                    }.items()
                    if v is not None
                },
            },
        )

        # Convert to CSV
        csv_data = json_to_csv(results)

        # Process with intelligent caching
        return await process_tool_response(
            tool_name="list_financials_income_statements",
            params={
                "tickers": tickers,
                "timeframe": timeframe,
                "limit": limit,
            },
            csv_data=csv_data,
        )
    except Exception as e:
        return f"Error: {e}"


@poly_mcp.tool(annotations=ToolAnnotations(readOnlyHint=True))
async def list_financials_ratios(
    cik: Optional[str] = None,
    ticker: Optional[str] = None,
    period_end: Optional[Union[str, datetime, date]] = None,
    filing_date: Optional[Union[str, datetime, date]] = None,
    fiscal_year: Optional[int] = None,
    fiscal_quarter: Optional[int] = None,
    timeframe: Optional[str] = None,
    limit: Optional[int] = 10,
    sort: Optional[str] = None,
    params: Optional[Dict[str, Any]] = None,
) -> str:
    """
    Get financial ratios for companies, providing key metrics for analyzing financial health and performance.
    Returns calculated ratios from SEC filings including profitability, liquidity, leverage, and efficiency metrics.

    Reference: https://polygon.io/docs/rest/stocks/fundamentals/financial-ratios

    Parameters:
    - cik: Filter by SEC Central Index Key (CIK)
    - ticker: Filter by ticker symbol (e.g., "AAPL", "MSFT")
    - period_end: Filter by period end date (YYYY-MM-DD)
    - filing_date: Filter by SEC filing date (YYYY-MM-DD)
    - fiscal_year: Filter by fiscal year
    - fiscal_quarter: Filter by fiscal quarter (1-4)
    - timeframe: Filter by reporting period (quarterly, annual)
    - limit: Number of results to return (default: 10, max: 50000)
    - sort: Sort by fields with .asc or .desc suffix

    Example: list_financials_ratios(ticker="AAPL") returns Apple's financial ratios
    Example: list_financials_ratios(ticker="MSFT", timeframe="annual") returns annual financial ratios
    Example: list_financials_ratios(ticker="GOOGL", fiscal_year=2023) returns 2023 financial ratios

    Note: Financial ratios provide insights into company performance:
    - Profitability: ROE (Return on Equity), ROA (Return on Assets), Net Profit Margin
    - Liquidity: Current Ratio, Quick Ratio - ability to meet short-term obligations
    - Leverage: Debt-to-Equity, Debt Ratio - how much debt finances the company
    - Efficiency: Asset Turnover, Inventory Turnover - how effectively assets generate revenue
    These ratios are calculated from balance sheet, income statement, and cash flow data.
    """
    try:
        # Build the params dictionary
        request_params = params or {}
        if cik:
            request_params["cik"] = cik
        if ticker:
            request_params["ticker"] = ticker
        if period_end:
            request_params["period_end"] = period_end
        if filing_date:
            request_params["filing_date"] = filing_date
        if fiscal_year:
            request_params["fiscal_year"] = fiscal_year
        if fiscal_quarter:
            request_params["fiscal_quarter"] = fiscal_quarter
        if timeframe:
            request_params["timeframe"] = timeframe
        if limit:
            request_params["limit"] = limit
        if sort:
            request_params["sort"] = sort

        # Make the request to the financial ratios endpoint
        results = polygon_client._get(
            "/vX/reference/financials/ratios", params=request_params
        )

        # Convert to CSV
        csv_data = json_to_csv(results)

        # Process with intelligent caching
        return await process_tool_response(
            tool_name="list_financials_ratios",
            params={
                "ticker": ticker,
                "limit": limit,
            },
            csv_data=csv_data,
        )
    except Exception as e:
        return f"Error: {e}"


@poly_mcp.tool(annotations=ToolAnnotations(readOnlyHint=True))
async def list_stock_ratios(
    ticker: Optional[str] = None,
    cik: Optional[str] = None,
    price: Optional[float] = None,
    average_volume: Optional[float] = None,
    market_cap: Optional[float] = None,
    earnings_per_share: Optional[float] = None,
    price_to_earnings: Optional[float] = None,
    price_to_book: Optional[float] = None,
    price_to_sales: Optional[float] = None,
    price_to_cash_flow: Optional[float] = None,
    price_to_free_cash_flow: Optional[float] = None,
    dividend_yield: Optional[float] = None,
    return_on_assets: Optional[float] = None,
    return_on_equity: Optional[float] = None,
    debt_to_equity: Optional[float] = None,
    current: Optional[float] = None,
    quick: Optional[float] = None,
    cash: Optional[float] = None,
    ev_to_sales: Optional[float] = None,
    ev_to_ebitda: Optional[float] = None,
    enterprise_value: Optional[float] = None,
    free_cash_flow: Optional[float] = None,
    ticker_any_of: Optional[str] = None,
    ticker_gt: Optional[str] = None,
    ticker_gte: Optional[str] = None,
    ticker_lt: Optional[str] = None,
    ticker_lte: Optional[str] = None,
    cik_any_of: Optional[str] = None,
    cik_gt: Optional[str] = None,
    cik_gte: Optional[str] = None,
    cik_lt: Optional[str] = None,
    cik_lte: Optional[str] = None,
    price_gt: Optional[float] = None,
    price_gte: Optional[float] = None,
    price_lt: Optional[float] = None,
    price_lte: Optional[float] = None,
    average_volume_gt: Optional[float] = None,
    average_volume_gte: Optional[float] = None,
    average_volume_lt: Optional[float] = None,
    average_volume_lte: Optional[float] = None,
    market_cap_gt: Optional[float] = None,
    market_cap_gte: Optional[float] = None,
    market_cap_lt: Optional[float] = None,
    market_cap_lte: Optional[float] = None,
    earnings_per_share_gt: Optional[float] = None,
    earnings_per_share_gte: Optional[float] = None,
    earnings_per_share_lt: Optional[float] = None,
    earnings_per_share_lte: Optional[float] = None,
    price_to_earnings_gt: Optional[float] = None,
    price_to_earnings_gte: Optional[float] = None,
    price_to_earnings_lt: Optional[float] = None,
    price_to_earnings_lte: Optional[float] = None,
    price_to_book_gt: Optional[float] = None,
    price_to_book_gte: Optional[float] = None,
    price_to_book_lt: Optional[float] = None,
    price_to_book_lte: Optional[float] = None,
    price_to_sales_gt: Optional[float] = None,
    price_to_sales_gte: Optional[float] = None,
    price_to_sales_lt: Optional[float] = None,
    price_to_sales_lte: Optional[float] = None,
    price_to_cash_flow_gt: Optional[float] = None,
    price_to_cash_flow_gte: Optional[float] = None,
    price_to_cash_flow_lt: Optional[float] = None,
    price_to_cash_flow_lte: Optional[float] = None,
    price_to_free_cash_flow_gt: Optional[float] = None,
    price_to_free_cash_flow_gte: Optional[float] = None,
    price_to_free_cash_flow_lt: Optional[float] = None,
    price_to_free_cash_flow_lte: Optional[float] = None,
    dividend_yield_gt: Optional[float] = None,
    dividend_yield_gte: Optional[float] = None,
    dividend_yield_lt: Optional[float] = None,
    dividend_yield_lte: Optional[float] = None,
    return_on_assets_gt: Optional[float] = None,
    return_on_assets_gte: Optional[float] = None,
    return_on_assets_lt: Optional[float] = None,
    return_on_assets_lte: Optional[float] = None,
    return_on_equity_gt: Optional[float] = None,
    return_on_equity_gte: Optional[float] = None,
    return_on_equity_lt: Optional[float] = None,
    return_on_equity_lte: Optional[float] = None,
    debt_to_equity_gt: Optional[float] = None,
    debt_to_equity_gte: Optional[float] = None,
    debt_to_equity_lt: Optional[float] = None,
    debt_to_equity_lte: Optional[float] = None,
    current_gt: Optional[float] = None,
    current_gte: Optional[float] = None,
    current_lt: Optional[float] = None,
    current_lte: Optional[float] = None,
    quick_gt: Optional[float] = None,
    quick_gte: Optional[float] = None,
    quick_lt: Optional[float] = None,
    quick_lte: Optional[float] = None,
    cash_gt: Optional[float] = None,
    cash_gte: Optional[float] = None,
    cash_lt: Optional[float] = None,
    cash_lte: Optional[float] = None,
    ev_to_sales_gt: Optional[float] = None,
    ev_to_sales_gte: Optional[float] = None,
    ev_to_sales_lt: Optional[float] = None,
    ev_to_sales_lte: Optional[float] = None,
    ev_to_ebitda_gt: Optional[float] = None,
    ev_to_ebitda_gte: Optional[float] = None,
    ev_to_ebitda_lt: Optional[float] = None,
    ev_to_ebitda_lte: Optional[float] = None,
    enterprise_value_gt: Optional[float] = None,
    enterprise_value_gte: Optional[float] = None,
    enterprise_value_lt: Optional[float] = None,
    enterprise_value_lte: Optional[float] = None,
    free_cash_flow_gt: Optional[float] = None,
    free_cash_flow_gte: Optional[float] = None,
    free_cash_flow_lt: Optional[float] = None,
    free_cash_flow_lte: Optional[float] = None,
    limit: Optional[int] = 100,
    sort: Optional[str] = None,
    params: Optional[Dict[str, Any]] = None,
) -> str:
    """
    Get current financial ratios for stock screening using TTM financials and latest stock prices.

    Reference: https://polygon.io/docs/rest/stocks/fundamentals/financial-ratios

    Parameters:
    - ticker: Ticker symbol (e.g., "AAPL")
    - price_to_earnings_lt/gte: P/E ratio filters
    - return_on_equity_gt/lt: ROE filters
    - debt_to_equity_lt: Debt-to-equity filters
    - market_cap_gte: Market cap minimum
    - dividend_yield_gt: Dividend yield minimum
    - limit: Number of results (default: 100, max: 50000)

    Example: list_stock_ratios(ticker="AAPL")
    Example: list_stock_ratios(price_to_earnings_lt=15, dividend_yield_gt=0.03, market_cap_gte=1000000000)

    Returns: P/E, P/B, P/S, ROE, ROA, Current Ratio, D/E, Dividend Yield. Ratios in decimals (0.044 = 4.4% yield). TTM data for screening.
    """
    try:
        # Build the params dictionary with all range parameters
        results = polygon_client._get(
            "/stocks/financials/v1/ratios",
            params={
                **(params or {}),
                **{
                    k: v
                    for k, v in {
                        "ticker": ticker,
                        "cik": cik,
                        "price": price,
                        "average_volume": average_volume,
                        "market_cap": market_cap,
                        "earnings_per_share": earnings_per_share,
                        "price_to_earnings": price_to_earnings,
                        "price_to_book": price_to_book,
                        "price_to_sales": price_to_sales,
                        "price_to_cash_flow": price_to_cash_flow,
                        "price_to_free_cash_flow": price_to_free_cash_flow,
                        "dividend_yield": dividend_yield,
                        "return_on_assets": return_on_assets,
                        "return_on_equity": return_on_equity,
                        "debt_to_equity": debt_to_equity,
                        "current": current,
                        "quick": quick,
                        "cash": cash,
                        "ev_to_sales": ev_to_sales,
                        "ev_to_ebitda": ev_to_ebitda,
                        "enterprise_value": enterprise_value,
                        "free_cash_flow": free_cash_flow,
                        "ticker.any_of": ticker_any_of,
                        "ticker.gt": ticker_gt,
                        "ticker.gte": ticker_gte,
                        "ticker.lt": ticker_lt,
                        "ticker.lte": ticker_lte,
                        "cik.any_of": cik_any_of,
                        "cik.gt": cik_gt,
                        "cik.gte": cik_gte,
                        "cik.lt": cik_lt,
                        "cik.lte": cik_lte,
                        "price.gt": price_gt,
                        "price.gte": price_gte,
                        "price.lt": price_lt,
                        "price.lte": price_lte,
                        "average_volume.gt": average_volume_gt,
                        "average_volume.gte": average_volume_gte,
                        "average_volume.lt": average_volume_lt,
                        "average_volume.lte": average_volume_lte,
                        "market_cap.gt": market_cap_gt,
                        "market_cap.gte": market_cap_gte,
                        "market_cap.lt": market_cap_lt,
                        "market_cap.lte": market_cap_lte,
                        "earnings_per_share.gt": earnings_per_share_gt,
                        "earnings_per_share.gte": earnings_per_share_gte,
                        "earnings_per_share.lt": earnings_per_share_lt,
                        "earnings_per_share.lte": earnings_per_share_lte,
                        "price_to_earnings.gt": price_to_earnings_gt,
                        "price_to_earnings.gte": price_to_earnings_gte,
                        "price_to_earnings.lt": price_to_earnings_lt,
                        "price_to_earnings.lte": price_to_earnings_lte,
                        "price_to_book.gt": price_to_book_gt,
                        "price_to_book.gte": price_to_book_gte,
                        "price_to_book.lt": price_to_book_lt,
                        "price_to_book.lte": price_to_book_lte,
                        "price_to_sales.gt": price_to_sales_gt,
                        "price_to_sales.gte": price_to_sales_gte,
                        "price_to_sales.lt": price_to_sales_lt,
                        "price_to_sales.lte": price_to_sales_lte,
                        "price_to_cash_flow.gt": price_to_cash_flow_gt,
                        "price_to_cash_flow.gte": price_to_cash_flow_gte,
                        "price_to_cash_flow.lt": price_to_cash_flow_lt,
                        "price_to_cash_flow.lte": price_to_cash_flow_lte,
                        "price_to_free_cash_flow.gt": price_to_free_cash_flow_gt,
                        "price_to_free_cash_flow.gte": price_to_free_cash_flow_gte,
                        "price_to_free_cash_flow.lt": price_to_free_cash_flow_lt,
                        "price_to_free_cash_flow.lte": price_to_free_cash_flow_lte,
                        "dividend_yield.gt": dividend_yield_gt,
                        "dividend_yield.gte": dividend_yield_gte,
                        "dividend_yield.lt": dividend_yield_lt,
                        "dividend_yield.lte": dividend_yield_lte,
                        "return_on_assets.gt": return_on_assets_gt,
                        "return_on_assets.gte": return_on_assets_gte,
                        "return_on_assets.lt": return_on_assets_lt,
                        "return_on_assets.lte": return_on_assets_lte,
                        "return_on_equity.gt": return_on_equity_gt,
                        "return_on_equity.gte": return_on_equity_gte,
                        "return_on_equity.lt": return_on_equity_lt,
                        "return_on_equity.lte": return_on_equity_lte,
                        "debt_to_equity.gt": debt_to_equity_gt,
                        "debt_to_equity.gte": debt_to_equity_gte,
                        "debt_to_equity.lt": debt_to_equity_lt,
                        "debt_to_equity.lte": debt_to_equity_lte,
                        "current.gt": current_gt,
                        "current.gte": current_gte,
                        "current.lt": current_lt,
                        "current.lte": current_lte,
                        "quick.gt": quick_gt,
                        "quick.gte": quick_gte,
                        "quick.lt": quick_lt,
                        "quick.lte": quick_lte,
                        "cash.gt": cash_gt,
                        "cash.gte": cash_gte,
                        "cash.lt": cash_lt,
                        "cash.lte": cash_lte,
                        "ev_to_sales.gt": ev_to_sales_gt,
                        "ev_to_sales.gte": ev_to_sales_gte,
                        "ev_to_sales.lt": ev_to_sales_lt,
                        "ev_to_sales.lte": ev_to_sales_lte,
                        "ev_to_ebitda.gt": ev_to_ebitda_gt,
                        "ev_to_ebitda.gte": ev_to_ebitda_gte,
                        "ev_to_ebitda.lt": ev_to_ebitda_lt,
                        "ev_to_ebitda.lte": ev_to_ebitda_lte,
                        "enterprise_value.gt": enterprise_value_gt,
                        "enterprise_value.gte": enterprise_value_gte,
                        "enterprise_value.lt": enterprise_value_lt,
                        "enterprise_value.lte": enterprise_value_lte,
                        "free_cash_flow.gt": free_cash_flow_gt,
                        "free_cash_flow.gte": free_cash_flow_gte,
                        "free_cash_flow.lt": free_cash_flow_lt,
                        "free_cash_flow.lte": free_cash_flow_lte,
                        "limit": limit,
                        "sort": sort,
                    }.items()
                    if v is not None
                },
            },
        )

        # Convert to CSV
        csv_data = json_to_csv(results)

        # Process with intelligent caching
        return await process_tool_response(
            tool_name="list_stock_ratios",
            params={
                "ticker": ticker,
                "limit": limit,
            },
            csv_data=csv_data,
        )
    except Exception as e:
        return f"Error: {e}"


@poly_mcp.tool(annotations=ToolAnnotations(readOnlyHint=True))
async def list_short_interest(
    ticker: Optional[str] = None,
    days_to_cover: Optional[float] = None,
    settlement_date: Optional[Union[str, datetime, date]] = None,
    avg_daily_volume: Optional[int] = None,
    ticker_any_of: Optional[str] = None,
    ticker_gt: Optional[str] = None,
    ticker_gte: Optional[str] = None,
    ticker_lt: Optional[str] = None,
    ticker_lte: Optional[str] = None,
    days_to_cover_any_of: Optional[str] = None,
    days_to_cover_gt: Optional[float] = None,
    days_to_cover_gte: Optional[float] = None,
    days_to_cover_lt: Optional[float] = None,
    days_to_cover_lte: Optional[float] = None,
    settlement_date_any_of: Optional[str] = None,
    settlement_date_gt: Optional[Union[str, datetime, date]] = None,
    settlement_date_gte: Optional[Union[str, datetime, date]] = None,
    settlement_date_lt: Optional[Union[str, datetime, date]] = None,
    settlement_date_lte: Optional[Union[str, datetime, date]] = None,
    avg_daily_volume_any_of: Optional[str] = None,
    avg_daily_volume_gt: Optional[int] = None,
    avg_daily_volume_gte: Optional[int] = None,
    avg_daily_volume_lt: Optional[int] = None,
    avg_daily_volume_lte: Optional[int] = None,
    limit: Optional[int] = 10,
    fetch_all: Optional[bool] = True,
    sort: Optional[str] = None,
    params: Optional[Dict[str, Any]] = None,
) -> str:
    """
    Get bi-monthly short interest data from FINRA for monitoring bearish sentiment and short squeeze potential.

    Reference: https://polygon.io/docs/rest/stocks/fundamentals/short-interest

    Parameters:
    - ticker: Ticker symbol (e.g., "GME")
    - days_to_cover_gt: Days to cover threshold (5-7+ indicates high squeeze risk)
    - settlement_date_gte/lte: Settlement date range (YYYY-MM-DD)
    - avg_daily_volume_lt: Volume filter for liquidity screening
    - limit: Number of results per page (default: 10, max: 50000)
    - fetch_all: If True (recommended), fetch ALL data and cache to disk for DuckDB queries (default: True)

    RECOMMENDED: Always use fetch_all=True to cache complete short interest data locally for efficient DuckDB analysis.

    Example: list_short_interest(ticker="GME", fetch_all=True)
    Example: list_short_interest(days_to_cover_gt=5, settlement_date_gte="2025-01-01", fetch_all=True)

    Returns: short_interest, days_to_cover, avg_daily_volume, settlement_date. Formula: days_to_cover = short_interest ÷ avg_daily_volume. Bi-monthly FINRA snapshots.
    """
    try:
        tool_params = {
            "ticker": ticker,
            "limit": limit,
            "fetch_all": fetch_all,
        }

        param_dict = {
            **(params or {}),
            **{
                k: v
                for k, v in {
                    "days_to_cover": days_to_cover,
                    "avg_daily_volume": avg_daily_volume,
                    "ticker.any_of": ticker_any_of,
                    "ticker.gt": ticker_gt,
                    "ticker.gte": ticker_gte,
                    "ticker.lt": ticker_lt,
                    "ticker.lte": ticker_lte,
                    "days_to_cover.any_of": days_to_cover_any_of,
                    "days_to_cover.gt": days_to_cover_gt,
                    "days_to_cover.gte": days_to_cover_gte,
                    "days_to_cover.lt": days_to_cover_lt,
                    "days_to_cover.lte": days_to_cover_lte,
                    "settlement_date.any_of": settlement_date_any_of,
                    "avg_daily_volume.any_of": avg_daily_volume_any_of,
                    "avg_daily_volume.gt": avg_daily_volume_gt,
                    "avg_daily_volume.gte": avg_daily_volume_gte,
                    "avg_daily_volume.lt": avg_daily_volume_lt,
                    "avg_daily_volume.lte": avg_daily_volume_lte,
                }.items()
                if v is not None
            },
        }

        if fetch_all:
            # Use batch writing for memory efficiency
            batch_callback, finalize = create_batch_writer(
                "list_short_interest", tool_params
            )

            if batch_callback:
                # Streaming mode - write batches to disk incrementally
                fetcher = PolygonParallelFetcher(polygon_client, num_workers=5)
                await fetcher.fetch_all(
                    method_name="list_short_interest",
                    batch_callback=batch_callback,
                    ticker=ticker,
                    settlement_date=settlement_date,
                    settlement_date_lt=settlement_date_lt,
                    settlement_date_lte=settlement_date_lte,
                    settlement_date_gt=settlement_date_gt,
                    settlement_date_gte=settlement_date_gte,
                    limit=limit,
                    sort=sort,
                    params=param_dict,
                )
                # Finalize and return cache metadata
                return await finalize()
            else:
                # Memory mode (fallback if batch writing not available)
                fetcher = PolygonParallelFetcher(polygon_client, num_workers=5)
                short_interest_list = await fetcher.fetch_all(
                    method_name="list_short_interest",
                    ticker=ticker,
                    settlement_date=settlement_date,
                    settlement_date_lt=settlement_date_lt,
                    settlement_date_lte=settlement_date_lte,
                    settlement_date_gt=settlement_date_gt,
                    settlement_date_gte=settlement_date_gte,
                    limit=limit,
                    sort=sort,
                    params=param_dict,
                )
                csv_data = json_to_csv({"results": short_interest_list})
                return await process_tool_response(
                    "list_short_interest", tool_params, csv_data
                )
        else:
            # Single page approach
            results = polygon_client.list_short_interest(
                ticker=ticker,
                settlement_date=settlement_date,
                settlement_date_lt=settlement_date_lt,
                settlement_date_lte=settlement_date_lte,
                settlement_date_gt=settlement_date_gt,
                settlement_date_gte=settlement_date_gte,
                limit=limit,
                sort=sort,
                params=param_dict,
                raw=True,
            )

            import json

            data = json.loads(results.data.decode("utf-8"))
            short_interest_list = data.get("results", [])

            # Create data structure for JSON to CSV conversion
            data = {"results": short_interest_list, "status": "OK"}

            # Convert to CSV
            csv_data = json_to_csv(data)

            # Process with intelligent caching
            return await process_tool_response(
                "list_short_interest", tool_params, csv_data
            )
    except Exception as e:
        return f"Error: {e}"


@poly_mcp.tool(annotations=ToolAnnotations(readOnlyHint=True))
async def list_short_volume(
    ticker: Optional[str] = None,
    date: Optional[Union[str, datetime, date]] = None,
    short_volume_ratio: Optional[float] = None,
    total_volume: Optional[int] = None,
    ticker_any_of: Optional[str] = None,
    ticker_gt: Optional[str] = None,
    ticker_gte: Optional[str] = None,
    ticker_lt: Optional[str] = None,
    ticker_lte: Optional[str] = None,
    date_any_of: Optional[str] = None,
    date_gt: Optional[Union[str, datetime, date]] = None,
    date_gte: Optional[Union[str, datetime, date]] = None,
    date_lt: Optional[Union[str, datetime, date]] = None,
    date_lte: Optional[Union[str, datetime, date]] = None,
    short_volume_ratio_any_of: Optional[str] = None,
    short_volume_ratio_gt: Optional[float] = None,
    short_volume_ratio_gte: Optional[float] = None,
    short_volume_ratio_lt: Optional[float] = None,
    short_volume_ratio_lte: Optional[float] = None,
    total_volume_any_of: Optional[str] = None,
    total_volume_gt: Optional[int] = None,
    total_volume_gte: Optional[int] = None,
    total_volume_lt: Optional[int] = None,
    total_volume_lte: Optional[int] = None,
    limit: Optional[int] = 10,
    fetch_all: Optional[bool] = True,
    sort: Optional[str] = None,
    params: Optional[Dict[str, Any]] = None,
) -> str:
    """
    Get daily short sale volume data from FINRA for tracking daily short-selling activity trends.

    Reference: https://polygon.io/docs/rest/stocks/fundamentals/short-volume

    Parameters:
    - ticker: Ticker symbol (e.g., "TSLA")
    - date_gte/lte: Date range (YYYY-MM-DD)
    - short_volume_ratio_gt: Short volume ratio threshold (>50% is high bearish pressure)
    - total_volume_gt: Volume filter
    - limit: Number of results per page (default: 10, max: 50000)
    - fetch_all: If True (recommended), fetch ALL data and cache to disk for DuckDB queries (default: True)

    RECOMMENDED: Always use fetch_all=True to cache complete short volume data locally for efficient DuckDB analysis.

    Example: list_short_volume(ticker="TSLA", date_gte="2025-03-01", fetch_all=True)
    Example: list_short_volume(short_volume_ratio_gt=50, date_gte="2025-01-01", fetch_all=True)

    Returns: short_volume, total_volume, short_volume_ratio, date. Formula: short_volume_ratio = (short_volume / total_volume) × 100. Daily FINRA transactions (T+1).
    """
    try:
        tool_params = {
            "ticker": ticker,
            "limit": limit,
            "fetch_all": fetch_all,
        }

        param_dict = {
            **(params or {}),
            **{
                k: v
                for k, v in {
                    "short_volume_ratio": short_volume_ratio,
                    "total_volume": total_volume,
                    "ticker.any_of": ticker_any_of,
                    "ticker.gt": ticker_gt,
                    "ticker.gte": ticker_gte,
                    "ticker.lt": ticker_lt,
                    "ticker.lte": ticker_lte,
                    "date.any_of": date_any_of,
                    "short_volume_ratio.any_of": short_volume_ratio_any_of,
                    "short_volume_ratio.gt": short_volume_ratio_gt,
                    "short_volume_ratio.gte": short_volume_ratio_gte,
                    "short_volume_ratio.lt": short_volume_ratio_lt,
                    "short_volume_ratio.lte": short_volume_ratio_lte,
                    "total_volume.any_of": total_volume_any_of,
                    "total_volume.gt": total_volume_gt,
                    "total_volume.gte": total_volume_gte,
                    "total_volume.lt": total_volume_lt,
                    "total_volume.lte": total_volume_lte,
                }.items()
                if v is not None
            },
        }

        if fetch_all:
            # Use batch writing for memory efficiency
            batch_callback, finalize = create_batch_writer(
                "list_short_volume", tool_params
            )

            if batch_callback:
                # Streaming mode - write batches to disk incrementally
                fetcher = PolygonParallelFetcher(polygon_client, num_workers=5)
                await fetcher.fetch_all(
                    method_name="list_short_volume",
                    batch_callback=batch_callback,
                    ticker=ticker,
                    date=date,
                    date_lt=date_lt,
                    date_lte=date_lte,
                    date_gt=date_gt,
                    date_gte=date_gte,
                    limit=limit,
                    sort=sort,
                    params=param_dict,
                )
                # Finalize and return cache metadata
                return await finalize()
            else:
                # Memory mode (fallback if batch writing not available)
                fetcher = PolygonParallelFetcher(polygon_client, num_workers=5)
                short_volume_list = await fetcher.fetch_all(
                    method_name="list_short_volume",
                    ticker=ticker,
                    date=date,
                    date_lt=date_lt,
                    date_lte=date_lte,
                    date_gt=date_gt,
                    date_gte=date_gte,
                    limit=limit,
                    sort=sort,
                    params=param_dict,
                )
                csv_data = json_to_csv({"results": short_volume_list})
                return await process_tool_response(
                    "list_short_volume", tool_params, csv_data
                )
        else:
            # Single page approach
            results = polygon_client.list_short_volume(
                ticker=ticker,
                date=date,
                date_lt=date_lt,
                date_lte=date_lte,
                date_gt=date_gt,
                date_gte=date_gte,
                limit=limit,
                sort=sort,
                params=param_dict,
                raw=True,
            )

            import json

            data = json.loads(results.data.decode("utf-8"))
            short_volume_list = data.get("results", [])

            # Create data structure for JSON to CSV conversion
            data = {"results": short_volume_list, "status": "OK"}

            # Convert to CSV
            csv_data = json_to_csv(data)

            # Process with intelligent caching
            return await process_tool_response(
                "list_short_volume", tool_params, csv_data
            )
    except Exception as e:
        return f"Error: {e}"
