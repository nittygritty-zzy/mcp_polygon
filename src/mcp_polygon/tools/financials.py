"""Auto-generated tool definitions."""

from typing import Optional, Any, Dict, Union, List
from mcp.types import ToolAnnotations
from datetime import datetime, date
from ..clients import poly_mcp, polygon_client
from ..formatters import json_to_csv
from ..tool_integration import process_tool_response





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
    - company_name: Filter by exact company name
    - company_name_search: Search by partial company name
    - sic: Filter by Standard Industrial Classification (SIC) code
    - filing_date: Filter by exact filing date (YYYY-MM-DD)
    - filing_date_lt/lte/gt/gte: Range filters for filing date
    - period_of_report_date: Filter by exact period end date
    - period_of_report_date_lt/lte/gt/gte: Range filters for period end date
    - timeframe: Filter by timeframe (annual, quarterly, ttm)
    - include_sources: Include source URLs for the data
    - limit: Number of results to return (default: 10)
    - sort/order: Sorting options

    Example: list_stock_financials(ticker="AAPL") returns Apple's financial statements
    Example: list_stock_financials(ticker="MSFT", timeframe="annual") returns annual financials
    Example: list_stock_financials(ticker="GOOGL", limit=4) returns 4 most recent filings

    Note: Returns balance sheet (assets, liabilities, equity), income statement (revenue, expenses, profit),
    and cash flow statement (operating, investing, financing activities) data from SEC filings.
    """
    try:
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

        return json_to_csv(results.data.decode("utf-8"))
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
    Retrieve comprehensive balance sheet data for public companies, containing quarterly and annual
    financial positions with detailed asset, liability, and equity breakdowns from SEC filings.

    Balance sheets represent point-in-time snapshots of a company's financial position, showing what
    the company owns (assets), owes (liabilities), and shareholders' equity at specific period end
    dates. This data is sourced from official SEC filings (10-K for annual, 10-Q for quarterly).

    Official API Documentation:
    https://polygon.io/docs/rest/stocks/fundamentals/balance-sheets

    Parameters
    ----------
    cik : str, optional
        The company's Central Index Key (CIK), a unique identifier assigned by the SEC.
        Example: "0000320193" for Apple Inc.
        Look up CIKs at: https://www.sec.gov/search-filings/cik-lookup

    tickers : str, optional
        Filter for arrays that contain the ticker value.
        Example: "AAPL"

    period_end : str or date, optional
        The last date of the reporting period (point-in-time snapshot date).
        Format: "YYYY-MM-DD"
        Example: "2025-06-28" for Apple's Q3 2025

    filing_date : str or date, optional
        The date when the financial statement was filed with the SEC.
        Format: "YYYY-MM-DD"
        Example: "2025-08-01"

    fiscal_year : int, optional
        The fiscal year for the reporting period.
        Example: 2025

    fiscal_quarter : int, optional
        The fiscal quarter number (1, 2, 3, or 4).
        Example: 3 for Q3

    timeframe : str, optional
        The reporting period type.
        Options: "quarterly", "annual"

    cik_any_of : str, optional
        Filter equal to any of the CIK values (comma-separated).
        Example: "0000320193,0000789019" for Apple and Microsoft

    cik_gt : str, optional
        Filter CIK greater than the value (lexicographic comparison).

    cik_gte : str, optional
        Filter CIK greater than or equal to the value.

    cik_lt : str, optional
        Filter CIK less than the value.

    cik_lte : str, optional
        Filter CIK less than or equal to the value.

    tickers_all_of : str, optional
        Filter for arrays that contain all of the ticker values (comma-separated).
        Example: "AAPL,AAPL.O" requires both tickers present

    tickers_any_of : str, optional
        Filter for arrays that contain any of the ticker values (comma-separated).
        Example: "AAPL,MSFT" matches either ticker

    period_end_gt : str or date, optional
        Filter period end date greater than the value.
        Format: "YYYY-MM-DD"

    period_end_gte : str or date, optional
        Filter period end date greater than or equal to the value.
        Format: "YYYY-MM-DD"

    period_end_lt : str or date, optional
        Filter period end date less than the value.
        Format: "YYYY-MM-DD"

    period_end_lte : str or date, optional
        Filter period end date less than or equal to the value.
        Format: "YYYY-MM-DD"

    filing_date_gt : str or date, optional
        Filter filing date greater than the value.
        Format: "YYYY-MM-DD"

    filing_date_gte : str or date, optional
        Filter filing date greater than or equal to the value.
        Format: "YYYY-MM-DD"

    filing_date_lt : str or date, optional
        Filter filing date less than the value.
        Format: "YYYY-MM-DD"

    filing_date_lte : str or date, optional
        Filter filing date less than or equal to the value.
        Format: "YYYY-MM-DD"

    fiscal_year_gt : int, optional
        Filter fiscal year greater than the value.

    fiscal_year_gte : int, optional
        Filter fiscal year greater than or equal to the value.

    fiscal_year_lt : int, optional
        Filter fiscal year less than the value.

    fiscal_year_lte : int, optional
        Filter fiscal year less than or equal to the value.

    fiscal_quarter_gt : int, optional
        Filter fiscal quarter greater than the value.

    fiscal_quarter_gte : int, optional
        Filter fiscal quarter greater than or equal to the value.

    fiscal_quarter_lt : int, optional
        Filter fiscal quarter less than the value.

    fiscal_quarter_lte : int, optional
        Filter fiscal quarter less than or equal to the value.

    timeframe_any_of : str, optional
        Filter equal to any of the timeframe values (comma-separated).
        Example: "quarterly,annual"

    timeframe_gt : str, optional
        Filter timeframe greater than the value (lexicographic).

    timeframe_gte : str, optional
        Filter timeframe greater than or equal to the value.

    timeframe_lt : str, optional
        Filter timeframe less than the value.

    timeframe_lte : str, optional
        Filter timeframe less than or equal to the value.

    limit : int, optional
        Maximum number of results to return.
        Default: 100, Maximum: 50000

    sort : str, optional
        Comma-separated list of sort columns with direction.
        Format: "field.asc" or "field.desc"
        Default: "period_end.desc"
        Example: "period_end.asc,fiscal_year.desc"

    params : dict, optional
        Additional query parameters for advanced filtering.

    Returns
    -------
    str
        CSV-formatted string containing balance sheet data with the following key fields:

        Identification:
        - cik: SEC Central Index Key
        - tickers: Array of ticker symbols
        - period_end: Balance sheet snapshot date
        - filing_date: SEC filing date
        - fiscal_year: Fiscal year
        - fiscal_quarter: Fiscal quarter (1-4)
        - timeframe: "quarterly" or "annual"

        Assets (what the company owns):
        - total_assets: Sum of all assets
        - total_current_assets: Assets convertible to cash within one year
        - cash_and_equivalents: Cash and highly liquid investments
        - short_term_investments: Marketable securities (< 1 year maturity)
        - receivables: Amounts owed by customers
        - inventories: Raw materials, WIP, and finished goods
        - other_current_assets: Other assets (< 1 year)
        - property_plant_equipment_net: Fixed assets net of depreciation
        - goodwill: Excess of acquisition cost over fair value
        - intangible_assets_net: Patents, trademarks, etc. net of amortization
        - other_assets: Other long-term assets

        Liabilities (what the company owes):
        - total_liabilities: Sum of all liabilities
        - total_current_liabilities: Obligations due within one year
        - accounts_payable: Amounts owed to suppliers
        - debt_current: Short-term borrowings and current portion of long-term debt
        - deferred_revenue_current: Customer prepayments (< 1 year)
        - accrued_and_other_current_liabilities: Other current obligations
        - long_term_debt_and_capital_lease_obligations: Long-term borrowings
        - other_noncurrent_liabilities: Other long-term obligations

        Equity (shareholders' ownership):
        - total_equity: Sum of all equity components
        - total_equity_attributable_to_parent: Parent company equity
        - common_stock: Par value of common shares
        - preferred_stock: Par value of preferred shares
        - additional_paid_in_capital: Capital received above par value
        - retained_earnings_deficit: Cumulative earnings less dividends
        - accumulated_other_comprehensive_income: Unrealized gains/losses
        - treasury_stock: Repurchased shares held in treasury (negative value)
        - noncontrolling_interest: Minority shareholders' ownership
        - other_equity: Other equity components

        Other:
        - commitments_and_contingencies: Potential liabilities
        - total_liabilities_and_equity: Total liabilities + equity (equals total_assets)

    Understanding Balance Sheet Structure
    -------------------------------------
    The balance sheet follows the fundamental accounting equation:

        **Assets = Liabilities + Shareholders' Equity**

    1. **Assets** (what the company owns or controls):
       - Current Assets: Convertible to cash within one year
         * Cash and equivalents
         * Short-term investments
         * Accounts receivable
         * Inventories
       - Non-Current Assets: Long-term holdings
         * Property, plant, and equipment (PP&E)
         * Intangible assets and goodwill
         * Long-term investments

    2. **Liabilities** (what the company owes):
       - Current Liabilities: Due within one year
         * Accounts payable
         * Short-term debt
         * Deferred revenue
       - Non-Current Liabilities: Long-term obligations
         * Long-term debt
         * Pension obligations
         * Deferred tax liabilities

    3. **Shareholders' Equity** (owners' residual interest):
       - Contributed Capital: Common stock + additional paid-in capital
       - Retained Earnings: Cumulative profits reinvested
       - Accumulated Other Comprehensive Income: Unrealized gains/losses
       - Treasury Stock: Repurchased shares (reduces equity)

    Use Cases
    ---------
    1. **Financial Position Analysis**: Assess company's asset composition, debt levels,
       and equity structure to evaluate financial health and stability.

    2. **Liquidity Assessment**: Calculate current ratio (current assets / current liabilities)
       and quick ratio to measure ability to meet short-term obligations.

    3. **Leverage Analysis**: Evaluate debt-to-equity ratio and debt-to-assets ratio to
       assess financial leverage and solvency risk.

    4. **Trend Analysis**: Compare balance sheets across multiple periods to identify
       changes in asset allocation, debt levels, and equity growth patterns.

    Examples
    --------
    Example 1: Get most recent balance sheet for Apple
        list_financials_balance_sheets(
            tickers="AAPL",
            limit=1,
            sort="period_end.desc"
        )

        Returns Apple's latest balance sheet showing current financial position.

    Example 2: Get annual balance sheets for Microsoft from 2020-2024
        list_financials_balance_sheets(
            tickers="MSFT",
            timeframe="annual",
            fiscal_year_gte=2020,
            fiscal_year_lte=2024,
            sort="fiscal_year.asc"
        )

        Returns five years of annual balance sheets for trend analysis.

    Example 3: Get Q3 balance sheets across multiple years for Tesla
        list_financials_balance_sheets(
            tickers="TSLA",
            fiscal_quarter=3,
            timeframe="quarterly",
            limit=5,
            sort="fiscal_year.desc"
        )

        Returns last 5 years of Q3 balance sheets for seasonal comparison.

    Example 4: Get balance sheets filed in 2025 for Amazon
        list_financials_balance_sheets(
            tickers="AMZN",
            filing_date_gte="2025-01-01",
            filing_date_lt="2026-01-01",
            sort="filing_date.desc"
        )

        Returns all balance sheets filed with SEC during 2025.

    Example 5: Compare balance sheets for multiple tech companies
        list_financials_balance_sheets(
            tickers_any_of="AAPL,MSFT,GOOGL,META",
            timeframe="annual",
            fiscal_year=2024,
            limit=50
        )

        Returns 2024 annual balance sheets for comparative analysis across tech giants.

    Notes
    -----
    - **Point-in-Time Snapshots**: Balance sheets represent the financial position at a specific
      date (period_end), unlike income statements and cash flow statements which show activity
      over a period of time.

    - **Fundamental Equation**: total_assets always equals total_liabilities_and_equity. This
      is the double-entry accounting principle that ensures balance sheet integrity.

    - **Filing Lag**: filing_date is typically several weeks after period_end. Use period_end
      for analyzing financial position at a specific time, and filing_date for tracking when
      information became publicly available.

    - **Fiscal vs Calendar Periods**: Companies may use fiscal years different from calendar
      years. Apple's fiscal year ends in September, so fiscal Q4 2025 ends in September 2025,
      not December 2025.

    - **Quarterly vs Annual Data**: Quarterly balance sheets (timeframe="quarterly") show
      positions at quarter ends (10-Q filings). Annual balance sheets (timeframe="annual")
      show year-end positions (10-K filings) and typically include more detailed disclosures.

    - **Key Financial Ratios**: Use balance sheet data to calculate:
      * Current Ratio = total_current_assets / total_current_liabilities (liquidity)
      * Quick Ratio = (cash + receivables) / total_current_liabilities (immediate liquidity)
      * Debt-to-Equity = total_liabilities / total_equity (leverage)
      * Debt-to-Assets = total_liabilities / total_assets (leverage)
      * Asset Turnover = revenue / total_assets (efficiency, requires income statement)
      * Return on Assets (ROA) = net_income / total_assets (profitability)
      * Return on Equity (ROE) = net_income / total_equity (shareholder returns)

    - **Working Capital**: Calculate net working capital = total_current_assets -
      total_current_liabilities to measure short-term financial health and operational efficiency.

    - **Treasury Stock**: Appears as negative value reducing total equity. Represents shares
      the company repurchased but did not retire, held for potential future reissuance.

    - **Goodwill and Intangibles**: Goodwill arises from acquisitions and represents the premium
      paid over fair value. Intangible assets include patents, trademarks, customer relationships.
      Both may be subject to impairment charges.

    - **Data Completeness**: Not all fields are present for all companies. Field availability
      depends on company structure and reporting practices. Use None/null checks when analyzing.

    - **Multiple Ticker Symbols**: Some companies trade under multiple symbols (different share
      classes). The tickers array may contain multiple values like ["GOOGL", "GOOG"] for Alphabet.

    - **Range Filtering**: Use range parameters for flexible queries:
      * Date ranges: period_end_gte + period_end_lte for specific time windows
      * Year ranges: fiscal_year_gte + fiscal_year_lte for multi-year analysis
      * Multiple companies: cik_any_of or tickers_any_of for batch queries

    - **Pagination**: For large result sets, use limit parameter and next_url from response
      metadata to retrieve subsequent pages. Default limit is 100, maximum is 50000.

    - **Comparative Analysis**: Combine balance sheet data with income statements and cash flow
      statements for comprehensive financial analysis. Cross-reference period_end, fiscal_year,
      and fiscal_quarter to align data across statement types.
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
    Retrieve comprehensive cash flow statement data for public companies, including quarterly, annual,
    and trailing twelve-month cash flows with detailed operating, investing, and financing activities.

    Cash flow statements show how cash moves through a business over a period of time, tracking the
    inflows and outflows across three core activities. This data is sourced from official SEC filings
    (10-K for annual, 10-Q for quarterly) and includes TTM calculations that sum components over
    four quarters.

    Official API Documentation:
    https://polygon.io/docs/rest/stocks/fundamentals/cash-flow-statements

    Parameters
    ----------
    cik : str, optional
        The company's Central Index Key (CIK), a unique identifier assigned by the SEC.
        Example: "0000320193" for Apple Inc.
        Look up CIKs at: https://www.sec.gov/search-filings/cik-lookup

    tickers : str, optional
        Filter for arrays that contain the ticker value.
        Example: "AAPL"

    period_end : str or date, optional
        The last date of the reporting period.
        Format: "YYYY-MM-DD"
        Example: "2025-06-28" for Apple's Q3 2025

    filing_date : str or date, optional
        The date when the financial statement was filed with the SEC.
        Format: "YYYY-MM-DD"
        Example: "2025-08-01"

    fiscal_year : int, optional
        The fiscal year for the reporting period.
        Example: 2025

    fiscal_quarter : int, optional
        The fiscal quarter number (1, 2, 3, or 4).
        Example: 3 for Q3

    timeframe : str, optional
        The reporting period type.
        Options: "quarterly", "annual", "trailing_twelve_months"

    cik_any_of : str, optional
        Filter equal to any of the CIK values (comma-separated).
        Example: "0000320193,0000789019" for Apple and Microsoft

    cik_gt : str, optional
        Filter CIK greater than the value (lexicographic comparison).

    cik_gte : str, optional
        Filter CIK greater than or equal to the value.

    cik_lt : str, optional
        Filter CIK less than the value.

    cik_lte : str, optional
        Filter CIK less than or equal to the value.

    period_end_gt : str or date, optional
        Filter period end date greater than the value.
        Format: "YYYY-MM-DD"

    period_end_gte : str or date, optional
        Filter period end date greater than or equal to the value.
        Format: "YYYY-MM-DD"

    period_end_lt : str or date, optional
        Filter period end date less than the value.
        Format: "YYYY-MM-DD"

    period_end_lte : str or date, optional
        Filter period end date less than or equal to the value.
        Format: "YYYY-MM-DD"

    filing_date_gt : str or date, optional
        Filter filing date greater than the value.
        Format: "YYYY-MM-DD"

    filing_date_gte : str or date, optional
        Filter filing date greater than or equal to the value.
        Format: "YYYY-MM-DD"

    filing_date_lt : str or date, optional
        Filter filing date less than the value.
        Format: "YYYY-MM-DD"

    filing_date_lte : str or date, optional
        Filter filing date less than or equal to the value.
        Format: "YYYY-MM-DD"

    tickers_all_of : str, optional
        Filter for arrays that contain all of the ticker values (comma-separated).
        Example: "AAPL,AAPL.O" requires both tickers present

    tickers_any_of : str, optional
        Filter for arrays that contain any of the ticker values (comma-separated).
        Example: "AAPL,MSFT" matches either ticker

    fiscal_year_gt : int, optional
        Filter fiscal year greater than the value.

    fiscal_year_gte : int, optional
        Filter fiscal year greater than or equal to the value.

    fiscal_year_lt : int, optional
        Filter fiscal year less than the value.

    fiscal_year_lte : int, optional
        Filter fiscal year less than or equal to the value.

    fiscal_quarter_gt : int, optional
        Filter fiscal quarter greater than the value.

    fiscal_quarter_gte : int, optional
        Filter fiscal quarter greater than or equal to the value.

    fiscal_quarter_lt : int, optional
        Filter fiscal quarter less than the value.

    fiscal_quarter_lte : int, optional
        Filter fiscal quarter less than or equal to the value.

    timeframe_any_of : str, optional
        Filter equal to any of the timeframe values (comma-separated).
        Example: "quarterly,annual,trailing_twelve_months"

    timeframe_gt : str, optional
        Filter timeframe greater than the value (lexicographic).

    timeframe_gte : str, optional
        Filter timeframe greater than or equal to the value.

    timeframe_lt : str, optional
        Filter timeframe less than the value.

    timeframe_lte : str, optional
        Filter timeframe less than or equal to the value.

    limit : int, optional
        Maximum number of results to return.
        Default: 100, Maximum: 50000

    sort : str, optional
        Comma-separated list of sort columns with direction.
        Format: "field.asc" or "field.desc"
        Default: "period_end.desc"
        Example: "period_end.asc,fiscal_year.desc"

    params : dict, optional
        Additional query parameters for advanced filtering.

    Returns
    -------
    str
        CSV-formatted string containing cash flow statement data with the following key fields:

        Identification:
        - cik: SEC Central Index Key
        - tickers: Array of ticker symbols
        - period_end: Reporting period end date
        - filing_date: SEC filing date
        - fiscal_year: Fiscal year
        - fiscal_quarter: Fiscal quarter (1-4)
        - timeframe: "quarterly", "annual", or "trailing_twelve_months"

        Operating Activities (cash from core business):
        - net_cash_from_operating_activities: Total operating cash flow
        - cash_from_operating_activities_continuing_operations: Operating cash from continuing ops
        - net_cash_from_operating_activities_discontinued_operations: Operating cash from discontinued ops
        - net_income: Net income (starting point for indirect method)
        - depreciation_depletion_and_amortization: Non-cash D&A add-back
        - change_in_other_operating_assets_and_liabilities_net: Working capital changes
        - other_operating_activities: Other operating adjustments
        - income_loss_from_discontinued_operations: Discontinued operations income/loss

        Investing Activities (cash from asset transactions):
        - net_cash_from_investing_activities: Total investing cash flow
        - net_cash_from_investing_activities_continuing_operations: Investing cash from continuing ops
        - net_cash_from_investing_activities_discontinued_operations: Investing cash from discontinued ops
        - purchase_of_property_plant_and_equipment: Capital expenditures (CapEx, negative)
        - sale_of_property_plant_and_equipment: Asset sales proceeds (positive)
        - other_investing_activities: Other investing transactions

        Financing Activities (cash from capital transactions):
        - net_cash_from_financing_activities: Total financing cash flow
        - net_cash_from_financing_activities_continuing_operations: Financing cash from continuing ops
        - net_cash_from_financing_activities_discontinued_operations: Financing cash from discontinued ops
        - dividends: Dividend payments (negative)
        - long_term_debt_issuances_repayments: Net long-term debt transactions
        - short_term_debt_issuances_repayments: Net short-term debt transactions
        - other_financing_activities: Other financing transactions (often includes share repurchases)
        - noncontrolling_interests: Cash flows related to minority shareholders

        Summary:
        - change_in_cash_and_equivalents: Net change in cash (sum of operating + investing + financing + FX effects)
        - effect_of_currency_exchange_rate: Impact of foreign exchange rate changes
        - other_cash_adjustments: Other miscellaneous adjustments

    Understanding Cash Flow Statement Structure
    -------------------------------------------
    The cash flow statement tracks cash movements across three core activities:

    1. **Operating Activities** (cash from core business):
       - Starts with net income (from income statement)
       - Adds back non-cash expenses (depreciation, amortization)
       - Adjusts for working capital changes:
         * Increase in receivables → cash outflow (customers owe more)
         * Increase in inventory → cash outflow (more inventory purchased)
         * Increase in payables → cash inflow (more owed to suppliers)
       - Result: Cash generated by day-to-day operations

    2. **Investing Activities** (cash from asset transactions):
       - Capital expenditures (CapEx): Purchases of PP&E (negative)
       - Asset sales: Proceeds from disposals (positive)
       - Acquisitions: Purchases of businesses (negative)
       - Investments: Purchases/sales of securities
       - Result: Cash used for growth and long-term investments

    3. **Financing Activities** (cash from capital transactions):
       - Debt issuance: Borrowing cash (positive)
       - Debt repayment: Paying down debt (negative)
       - Dividends: Payments to shareholders (negative)
       - Share repurchases: Buying back stock (negative, in other_financing_activities)
       - Share issuance: Selling new shares (positive)
       - Result: Cash flows between company and capital providers

    **Cash Flow Equation**:
        Change in Cash = Operating CF + Investing CF + Financing CF + FX Effects

    Use Cases
    ---------
    1. **Liquidity Assessment**: Evaluate company's ability to generate cash from operations
       and meet short-term obligations without external financing.

    2. **Operational Efficiency**: Analyze operating cash flow trends and working capital
       management to assess business quality and cash conversion efficiency.

    3. **Investment Analysis**: Track capital expenditures (CapEx), acquisitions, and free
       cash flow (operating cash - CapEx) to evaluate growth investments and shareholder returns.

    4. **Financial Health Monitoring**: Compare cash flows across activities to identify
       whether growth is funded by operations, debt, or equity, and assess dividend sustainability.

    Examples
    --------
    Example 1: Get most recent cash flow statement for Apple
        list_financials_cash_flow_statements(
            tickers="AAPL",
            limit=1,
            sort="period_end.desc"
        )

        Returns Apple's latest cash flow statement showing recent cash generation and usage.

    Example 2: Get annual cash flow statements for Microsoft from 2020-2024
        list_financials_cash_flow_statements(
            tickers="MSFT",
            timeframe="annual",
            fiscal_year_gte=2020,
            fiscal_year_lte=2024,
            sort="fiscal_year.asc"
        )

        Returns five years of annual cash flows for trend analysis and free cash flow calculation.

    Example 3: Get trailing twelve-month (TTM) cash flows for Tesla
        list_financials_cash_flow_statements(
            tickers="TSLA",
            timeframe="trailing_twelve_months",
            limit=4,
            sort="period_end.desc"
        )

        Returns most recent TTM cash flows, useful for current run-rate analysis.

    Example 4: Get Q4 cash flows across multiple years for Amazon
        list_financials_cash_flow_statements(
            tickers="AMZN",
            fiscal_quarter=4,
            timeframe="quarterly",
            limit=5,
            sort="fiscal_year.desc"
        )

        Returns last 5 years of Q4 cash flows for seasonal comparison and holiday period analysis.

    Example 5: Compare cash flows for multiple tech companies
        list_financials_cash_flow_statements(
            tickers_any_of="AAPL,MSFT,GOOGL,META",
            timeframe="annual",
            fiscal_year=2024,
            limit=50
        )

        Returns 2024 annual cash flows for comparative analysis of cash generation efficiency.

    Notes
    -----
    - **Period vs Point-in-Time**: Cash flow statements represent activity over a period (from
      start to end of quarter/year), unlike balance sheets which are point-in-time snapshots.

    - **Indirect vs Direct Method**: Most companies use the indirect method, starting with net
      income and adjusting for non-cash items and working capital changes. The direct method
      (rarely used) shows actual cash receipts and payments.

    - **Free Cash Flow (FCF)**: Calculate as operating cash flow minus capital expenditures:
      * FCF = net_cash_from_operating_activities - purchase_of_property_plant_and_equipment
      * Represents cash available for distribution to shareholders or debt reduction
      * Key metric for valuation and dividend sustainability analysis

    - **Cash vs Earnings Quality**: Compare net income to operating cash flow:
      * Operating CF > Net Income → High quality earnings (cash-backed)
      * Operating CF < Net Income → Potential earnings quality issues (accrual-heavy)
      * Persistent divergence may indicate aggressive accounting or business issues

    - **Working Capital Impact**: change_in_other_operating_assets_and_liabilities_net shows
      working capital changes. Negative values can indicate:
      * Growing receivables (customers paying slower)
      * Inventory buildup (potential demand issues or poor inventory management)
      * Positive values may indicate improving collections or inventory efficiency

    - **Trailing Twelve Months (TTM)**: timeframe="trailing_twelve_months" provides rolling
      12-month cash flows, useful for:
      * Smoothing seasonal variations
      * Getting current run-rate without waiting for annual reports
      * Valuation using most recent full-year data

    - **Share Repurchases**: Often reported in other_financing_activities rather than a separate
      field. These are negative cash flows representing shareholder returns alongside dividends.

    - **CapEx Trends**: Track purchase_of_property_plant_and_equipment over time:
      * Increasing CapEx → Growth investments, expansion
      * Decreasing CapEx → Mature business, slower growth, or cost cutting
      * Compare CapEx to depreciation to assess asset base maintenance

    - **Dividend Coverage**: Calculate dividend coverage ratio:
      * Coverage = net_cash_from_operating_activities / abs(dividends)
      * Ratio > 1.0 → Dividends covered by operating cash (sustainable)
      * Ratio < 1.0 → Dividends funded by debt/equity (potential risk)

    - **Debt Service**: Monitor debt issuance and repayment patterns:
      * Positive net debt → Increasing leverage, potential liquidity needs
      * Negative net debt → Deleveraging, improving financial position
      * Compare to operating cash flow for debt serviceability assessment

    - **Discontinued Operations**: Fields with "discontinued_operations" suffix show cash flows
      from business segments being wound down. These are non-recurring and should be excluded
      from ongoing analysis.

    - **Currency Effects**: effect_of_currency_exchange_rate shows impact of FX rate changes on
      cash held in foreign currencies. Material for multinational companies with significant
      international operations.

    - **Data Completeness**: Not all fields are present for all companies. Field availability
      depends on company structure and reporting practices. Use None/null checks when analyzing.

    - **Quarterly vs Annual**: Quarterly cash flows can be volatile due to working capital
      timing. Annual or TTM data provides smoother trends for analysis.

    - **Cross-Statement Analysis**: Combine with balance sheets and income statements:
      * Net income (income statement) → Starting point for operating cash flow
      * Change in cash (balance sheet) → Should equal change_in_cash_and_equivalents
      * Working capital changes → Link to balance sheet current asset/liability changes

    - **Pagination**: For large result sets, use limit parameter and next_url from response
      metadata to retrieve subsequent pages. Default limit is 100, maximum is 50000.
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
    Retrieve comprehensive income statement data for public companies, including revenue, expenses,
    and net income across quarterly, annual, and trailing twelve-month periods.

    Income statements (also called profit and loss or P&L statements) show a company's financial
    performance over a period of time, detailing revenues, expenses, and profitability. This data
    is sourced from official SEC filings (10-K for annual, 10-Q for quarterly) and includes TTM
    calculations that sum components over four quarters.

    Official API Documentation:
    https://polygon.io/docs/rest/stocks/fundamentals/income-statements

    Parameters
    ----------
    cik : str, optional
        The company's Central Index Key (CIK), a unique identifier assigned by the SEC.
        Example: "0000320193" for Apple Inc.
        Look up CIKs at: https://www.sec.gov/search-filings/cik-lookup

    tickers : str, optional
        Filter for arrays that contain the ticker value.
        Example: "AAPL"

    period_end : str or date, optional
        The last date of the reporting period.
        Format: "YYYY-MM-DD"
        Example: "2025-06-28" for Apple's Q3 2025

    filing_date : str or date, optional
        The date when the financial statement was filed with the SEC.
        Format: "YYYY-MM-DD"
        Example: "2025-08-01"

    fiscal_year : int, optional
        The fiscal year for the reporting period.
        Example: 2025

    fiscal_quarter : int, optional
        The fiscal quarter number (1, 2, 3, or 4).
        Example: 3 for Q3

    timeframe : str, optional
        The reporting period type.
        Options: "quarterly", "annual", "trailing_twelve_months"

    cik_any_of : str, optional
        Filter equal to any of the CIK values (comma-separated).
        Example: "0000320193,0000789019" for Apple and Microsoft

    cik_gt : str, optional
        Filter CIK greater than the value (lexicographic comparison).

    cik_gte : str, optional
        Filter CIK greater than or equal to the value.

    cik_lt : str, optional
        Filter CIK less than the value.

    cik_lte : str, optional
        Filter CIK less than or equal to the value.

    tickers_all_of : str, optional
        Filter for arrays that contain all of the ticker values (comma-separated).
        Example: "AAPL,AAPL.O" requires both tickers present

    tickers_any_of : str, optional
        Filter for arrays that contain any of the ticker values (comma-separated).
        Example: "AAPL,MSFT" matches either ticker

    period_end_gt : str or date, optional
        Filter period end date greater than the value.
        Format: "YYYY-MM-DD"

    period_end_gte : str or date, optional
        Filter period end date greater than or equal to the value.
        Format: "YYYY-MM-DD"

    period_end_lt : str or date, optional
        Filter period end date less than the value.
        Format: "YYYY-MM-DD"

    period_end_lte : str or date, optional
        Filter period end date less than or equal to the value.
        Format: "YYYY-MM-DD"

    filing_date_gt : str or date, optional
        Filter filing date greater than the value.
        Format: "YYYY-MM-DD"

    filing_date_gte : str or date, optional
        Filter filing date greater than or equal to the value.
        Format: "YYYY-MM-DD"

    filing_date_lt : str or date, optional
        Filter filing date less than the value.
        Format: "YYYY-MM-DD"

    filing_date_lte : str or date, optional
        Filter filing date less than or equal to the value.
        Format: "YYYY-MM-DD"

    fiscal_year_gt : int, optional
        Filter fiscal year greater than the value.

    fiscal_year_gte : int, optional
        Filter fiscal year greater than or equal to the value.

    fiscal_year_lt : int, optional
        Filter fiscal year less than the value.

    fiscal_year_lte : int, optional
        Filter fiscal year less than or equal to the value.

    fiscal_quarter_gt : int, optional
        Filter fiscal quarter greater than the value.

    fiscal_quarter_gte : int, optional
        Filter fiscal quarter greater than or equal to the value.

    fiscal_quarter_lt : int, optional
        Filter fiscal quarter less than the value.

    fiscal_quarter_lte : int, optional
        Filter fiscal quarter less than or equal to the value.

    timeframe_any_of : str, optional
        Filter equal to any of the timeframe values (comma-separated).
        Example: "quarterly,annual,trailing_twelve_months"

    timeframe_gt : str, optional
        Filter timeframe greater than the value (lexicographic).

    timeframe_gte : str, optional
        Filter timeframe greater than or equal to the value.

    timeframe_lt : str, optional
        Filter timeframe less than the value.

    timeframe_lte : str, optional
        Filter timeframe less than or equal to the value.

    limit : int, optional
        Maximum number of results to return.
        Default: 100, Maximum: 50000

    sort : str, optional
        Comma-separated list of sort columns with direction.
        Format: "field.asc" or "field.desc"
        Default: "period_end.desc"
        Example: "period_end.asc,fiscal_year.desc"

    params : dict, optional
        Additional query parameters for advanced filtering.

    Returns
    -------
    str
        CSV-formatted string containing income statement data with the following key fields:

        Identification:
        - cik: SEC Central Index Key
        - tickers: Array of ticker symbols
        - period_end: Reporting period end date
        - filing_date: SEC filing date
        - fiscal_year: Fiscal year
        - fiscal_quarter: Fiscal quarter (1-4)
        - timeframe: "quarterly", "annual", or "trailing_twelve_months"

        Revenue and Gross Profit:
        - revenue: Total revenue/net sales
        - cost_of_revenue: Direct costs of goods/services sold (COGS)
        - gross_profit: Revenue minus cost of revenue

        Operating Expenses:
        - selling_general_administrative: SG&A expenses
        - research_development: R&D expenses
        - depreciation_depletion_amortization: Non-cash D&A expense
        - other_operating_expenses: Other operating costs
        - total_operating_expenses: Sum of all operating expenses

        Operating Performance:
        - operating_income: Profit from operations (gross profit - operating expenses)
        - ebitda: Earnings before interest, taxes, depreciation, and amortization

        Non-Operating Items:
        - interest_income: Income from interest-bearing investments
        - interest_expense: Cost of borrowed funds
        - other_income_expense: Other non-operating income/expenses
        - total_other_income_expense: Net total of non-operating items

        Pre-Tax and Net Income:
        - income_before_income_taxes: Pre-tax income
        - income_taxes: Income tax expense
        - consolidated_net_income_loss: Total net income including all subsidiaries
        - discontinued_operations: After-tax results from discontinued segments
        - extraordinary_items: Unusual and infrequent items
        - equity_in_affiliates: Share of income from equity method investments

        Net Income Attribution:
        - noncontrolling_interest: Net income attributable to minority shareholders
        - preferred_stock_dividends_declared: Preferred stock dividends
        - net_income_loss_attributable_common_shareholders: Net income available to common shareholders

        Per-Share Metrics:
        - basic_shares_outstanding: Weighted average basic shares
        - basic_earnings_per_share: Basic EPS
        - diluted_shares_outstanding: Weighted average diluted shares (including options/warrants)
        - diluted_earnings_per_share: Diluted EPS

    Understanding Income Statement Structure
    -----------------------------------------
    The income statement follows a multi-step format showing profitability at different levels:

    1. **Revenue and Gross Profit**:
       - Revenue: Total sales and operating income
       - Cost of Revenue (COGS): Direct production costs
       - **Gross Profit = Revenue - Cost of Revenue**
       - Gross Margin % = Gross Profit / Revenue

    2. **Operating Income**:
       - Operating Expenses:
         * Selling, General & Administrative (SG&A)
         * Research & Development (R&D)
         * Depreciation & Amortization (D&A)
       - **Operating Income = Gross Profit - Total Operating Expenses**
       - Operating Margin % = Operating Income / Revenue

    3. **Pre-Tax Income**:
       - Non-Operating Items:
         * Interest income (positive)
         * Interest expense (negative)
         * Other income/expense
       - **Income Before Taxes = Operating Income + Total Other Income/Expense**

    4. **Net Income**:
       - Income tax expense
       - **Net Income = Income Before Taxes - Income Taxes**
       - Net Margin % = Net Income / Revenue

    5. **Earnings Per Share (EPS)**:
       - **Basic EPS = Net Income / Basic Shares Outstanding**
       - **Diluted EPS = Net Income / Diluted Shares Outstanding**
         (includes dilutive effect of options, warrants, convertibles)

    Use Cases
    ---------
    1. **Profitability Analysis**: Evaluate revenue growth, margin trends, and bottom-line
       profitability to assess financial performance and business model efficiency.

    2. **Revenue Trend Analysis**: Track revenue growth rates across quarters and years
       to identify business momentum, seasonality, and market share dynamics.

    3. **Expense Management Evaluation**: Analyze operating expense ratios and cost structure
       to assess operational efficiency and cost discipline.

    4. **Earnings Assessment**: Calculate earnings quality metrics, compare earnings to
       cash flow, and evaluate sustainability of profitability for valuation purposes.

    Examples
    --------
    Example 1: Get most recent income statement for Apple
        list_financials_income_statements(
            tickers="AAPL",
            limit=1,
            sort="period_end.desc"
        )

        Returns Apple's latest income statement showing recent revenue and profitability.

    Example 2: Get annual income statements for Microsoft from 2020-2024
        list_financials_income_statements(
            tickers="MSFT",
            timeframe="annual",
            fiscal_year_gte=2020,
            fiscal_year_lte=2024,
            sort="fiscal_year.asc"
        )

        Returns five years of annual income statements for revenue and earnings trend analysis.

    Example 3: Get trailing twelve-month (TTM) income for Tesla
        list_financials_income_statements(
            tickers="TSLA",
            timeframe="trailing_twelve_months",
            limit=4,
            sort="period_end.desc"
        )

        Returns most recent TTM income statements for current run-rate profitability analysis.

    Example 4: Get Q4 income statements across multiple years for Amazon
        list_financials_income_statements(
            tickers="AMZN",
            fiscal_quarter=4,
            timeframe="quarterly",
            limit=5,
            sort="fiscal_year.desc"
        )

        Returns last 5 years of Q4 income statements for holiday season performance comparison.

    Example 5: Compare profitability for multiple tech companies
        list_financials_income_statements(
            tickers_any_of="AAPL,MSFT,GOOGL,META",
            timeframe="annual",
            fiscal_year=2024,
            limit=50
        )

        Returns 2024 annual income statements for comparative margin and profitability analysis.

    Notes
    -----
    - **Period-Based Statement**: Income statements show activity over a period (quarter/year),
      unlike balance sheets which are point-in-time snapshots. Revenue and expenses accrue
      during the reporting period.

    - **GAAP Revenue Recognition**: Revenue is recognized when earned, not necessarily when
      cash is received. Compare to cash flow from operations to assess cash collection quality.

    - **Key Profitability Margins**: Calculate and track these margin metrics:
      * Gross Margin = gross_profit / revenue (pricing power and production efficiency)
      * Operating Margin = operating_income / revenue (operational efficiency)
      * EBITDA Margin = ebitda / revenue (cash generation potential)
      * Net Margin = net_income / revenue (overall profitability)

    - **Margin Trend Analysis**: Compare margins across periods:
      * Expanding margins → Improving efficiency, pricing power, operating leverage
      * Contracting margins → Competitive pressure, cost inflation, inefficiency
      * Stable margins → Mature business, consistent operations

    - **Basic vs Diluted EPS**: Diluted EPS accounts for potential dilution from stock options,
      warrants, and convertible securities. For TTM records, EPS is recalculated using TTM net
      income divided by average shares over four quarters.

    - **Share Count Changes**: Track basic_shares_outstanding and diluted_shares_outstanding:
      * Decreasing shares → Share buybacks (shareholder-friendly)
      * Increasing shares → Share issuance or dilution from employee compensation
      * Dilution gap (diluted - basic) → Potential dilution from stock options/warrants

    - **EBITDA vs Operating Income**: EBITDA adds back depreciation and amortization to
      operating income, providing a proxy for cash-based operating performance. Useful for
      comparing companies with different capital structures and depreciation policies.

    - **Revenue Quality**: Assess revenue sustainability by examining:
      * Revenue growth consistency across periods
      * Revenue concentration by customer/geography
      * Deferred revenue trends (future revenue visibility)
      * Revenue recognition policies (aggressive vs conservative)

    - **Expense Analysis**: Break down expense structure:
      * cost_of_revenue / revenue = Cost structure efficiency
      * selling_general_administrative / revenue = Sales and admin efficiency
      * research_development / revenue = Innovation investment level
      * Compare expense ratios to industry peers for competitive positioning

    - **Non-Operating Items**: Separate operating from non-operating performance:
      * other_income_expense includes gains/losses on investments, asset sales
      * discontinued_operations shows results from exited businesses (non-recurring)
      * extraordinary_items are unusual and infrequent (rare under current GAAP)

    - **Tax Rate Analysis**: Calculate effective tax rate:
      * Effective Tax Rate = income_taxes / income_before_income_taxes
      * Compare to statutory rates to identify tax benefits/liabilities
      * Track tax rate trends for future tax expense forecasting

    - **Trailing Twelve Months (TTM)**: timeframe="trailing_twelve_months" provides rolling
      12-month results, useful for:
      * Smoothing seasonal variations in quarterly results
      * Getting current full-year run-rate for valuation (P/E ratio using TTM EPS)
      * Avoiding distortions from one-time items in single quarters

    - **Quarterly Seasonality**: Many businesses have seasonal patterns:
      * Retail: Q4 holiday season typically strongest
      * Tech: Enterprise spending often weighted to Q4
      * Compare same quarters year-over-year for apples-to-apples comparison

    - **Year-over-Year Growth**: Calculate growth rates:
      * Revenue Growth % = (Current Period Revenue / Prior Period Revenue) - 1
      * EPS Growth % = (Current Period EPS / Prior Period EPS) - 1
      * Use same timeframe (Q3 2025 vs Q3 2024) for quarterly comparisons

    - **Earnings Quality**: Assess earnings quality by comparing to cash flow:
      * High quality: Net income ≈ Operating cash flow (from cash flow statement)
      * Low quality: Net income >> Operating cash flow (aggressive accruals)
      * Persistent divergence may indicate accounting issues or business deterioration

    - **Data Completeness**: Not all fields are present for all companies. Field availability
      depends on business model and reporting practices. Financial services companies have
      different income statement structures than industrial companies.

    - **Multiple Ticker Symbols**: Some companies trade under multiple symbols (different share
      classes). The tickers array may contain multiple values like ["GOOGL", "GOOG"] for Alphabet.

    - **Cross-Statement Analysis**: Combine with balance sheets and cash flow statements:
      * Revenue (income statement) vs Receivables (balance sheet) → Collection efficiency
      * Net income (income statement) vs Operating cash flow (cash flow) → Earnings quality
      * Operating expenses (income statement) vs CapEx (cash flow) → Investment vs expense

    - **Pagination**: For large result sets, use limit parameter and next_url from response
      metadata to retrieve subsequent pages. Default limit is 100, maximum is 50000.

    - **Valuation Metrics**: Use income statement data for valuation:
      * P/E Ratio = Market Cap / Net Income (or Price per Share / EPS)
      * P/S Ratio = Market Cap / Revenue
      * EV/EBITDA = Enterprise Value / EBITDA
      * PEG Ratio = P/E Ratio / EPS Growth Rate
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
    Retrieve comprehensive financial ratios data for stock screening and comparative analysis,
    combining trailing twelve-month (TTM) financials with current stock prices.

    This endpoint provides key valuation, profitability, liquidity, and leverage metrics calculated
    from the most recent TTM financial data and the latest trading day's stock price. Unlike
    historical financial statement endpoints, this is designed for stock screening and real-time
    comparative analysis across companies.

    Official API Documentation:
    https://polygon.io/docs/rest/stocks/fundamentals/financial-ratios

    Parameters
    ----------
    ticker : str, optional
        Stock ticker symbol for the company.
        Example: "AAPL"

    cik : str, optional
        Central Index Key (CIK) assigned by the SEC.
        Example: "320193" for Apple

    price : float, optional
        Stock price used in ratio calculations (typically closing price).

    average_volume : float, optional
        Average trading volume over the last 30 trading days.

    market_cap : float, optional
        Market capitalization (stock price × total shares outstanding).

    earnings_per_share : float, optional
        Earnings per share (net income ÷ weighted shares outstanding).

    price_to_earnings : float, optional
        Price-to-earnings ratio (P/E = price ÷ EPS).
        Only calculated when EPS is positive.

    price_to_book : float, optional
        Price-to-book ratio (P/B = price ÷ book value per share).

    price_to_sales : float, optional
        Price-to-sales ratio (P/S = price ÷ revenue per share).

    price_to_cash_flow : float, optional
        Price-to-cash-flow ratio (P/CF = price ÷ operating cash flow per share).
        Only calculated when cash flow per share is positive.

    price_to_free_cash_flow : float, optional
        Price-to-free-cash-flow ratio (P/FCF = price ÷ free cash flow per share).
        Only calculated when FCF per share is positive.

    dividend_yield : float, optional
        Dividend yield (annual dividends per share ÷ price).
        Example: 0.044 represents 4.4% yield

    return_on_assets : float, optional
        Return on assets (ROA = net income ÷ total assets).
        Example: 0.3075 represents 30.75% ROA

    return_on_equity : float, optional
        Return on equity (ROE = net income ÷ shareholders' equity).
        Example: 1.5284 represents 152.84% ROE

    debt_to_equity : float, optional
        Debt-to-equity ratio ((current debt + long-term debt) ÷ equity).

    current : float, optional
        Current ratio (current assets ÷ current liabilities).

    quick : float, optional
        Quick ratio/acid-test ratio ((current assets - inventories) ÷ current liabilities).

    cash : float, optional
        Cash ratio (cash and equivalents ÷ current liabilities).

    ev_to_sales : float, optional
        Enterprise value to sales ratio (EV ÷ revenue).

    ev_to_ebitda : float, optional
        Enterprise value to EBITDA ratio (EV ÷ EBITDA).

    enterprise_value : float, optional
        Enterprise value (market cap + total debt - cash and equivalents).

    free_cash_flow : float, optional
        Free cash flow (operating cash flow - capital expenditures).

    ticker_any_of : str, optional
        Filter equal to any of the ticker values (comma-separated).
        Example: "AAPL,MSFT,GOOGL"

    ticker_gt : str, optional
        Filter ticker greater than the value (lexicographic).

    ticker_gte : str, optional
        Filter ticker greater than or equal to the value.

    ticker_lt : str, optional
        Filter ticker less than the value.

    ticker_lte : str, optional
        Filter ticker less than or equal to the value.

    cik_any_of : str, optional
        Filter equal to any of the CIK values (comma-separated).

    cik_gt : str, optional
        Filter CIK greater than the value (lexicographic).

    cik_gte : str, optional
        Filter CIK greater than or equal to the value.

    cik_lt : str, optional
        Filter CIK less than the value.

    cik_lte : str, optional
        Filter CIK less than or equal to the value.

    [Additional range parameters for all ratio fields follow the pattern:
     {field}_gt, {field}_gte, {field}_lt, {field}_lte for filtering]

    limit : int, optional
        Maximum number of results to return.
        Default: 100, Maximum: 50000

    sort : str, optional
        Comma-separated list of sort columns with direction.
        Format: "field.asc" or "field.desc"
        Default: "ticker.asc"
        Example: "price_to_earnings.asc,market_cap.desc"

    params : dict, optional
        Additional query parameters for advanced filtering.

    Returns
    -------
    str
        CSV-formatted string containing financial ratios data with the following fields:

        Identification:
        - ticker: Stock ticker symbol
        - cik: SEC Central Index Key
        - date: Date for which ratios are calculated (latest trading date)

        Market Data:
        - price: Stock price used in calculations
        - average_volume: 30-day average trading volume
        - market_cap: Market capitalization
        - enterprise_value: Market cap + debt - cash
        - free_cash_flow: Operating cash flow - CapEx

        Valuation Ratios:
        - price_to_earnings: P/E ratio (only if EPS > 0)
        - price_to_book: P/B ratio
        - price_to_sales: P/S ratio
        - price_to_cash_flow: P/CF ratio (only if OCF per share > 0)
        - price_to_free_cash_flow: P/FCF ratio (only if FCF per share > 0)
        - ev_to_sales: EV/Sales ratio
        - ev_to_ebitda: EV/EBITDA ratio

        Profitability Ratios:
        - earnings_per_share: EPS
        - return_on_assets: ROA
        - return_on_equity: ROE

        Liquidity Ratios:
        - current: Current ratio
        - quick: Quick ratio (acid-test)
        - cash: Cash ratio

        Leverage Ratios:
        - debt_to_equity: D/E ratio

        Income:
        - dividend_yield: Annual dividend yield

    Understanding Financial Ratios
    ------------------------------
    Financial ratios are categorized into four main groups:

    1. **Valuation Ratios** (how expensive is the stock?):
       - **P/E (Price-to-Earnings)**: price / EPS
         * Lower P/E → cheaper relative to earnings
         * Higher P/E → more expensive or higher growth expectations
         * Only calculated when EPS > 0 (profitable companies)
       - **P/B (Price-to-Book)**: price / book value per share
         * P/B < 1 → trading below book value
         * P/B > 1 → market values company above accounting value
       - **P/S (Price-to-Sales)**: price / revenue per share
         * Useful for unprofitable growth companies
         * Lower P/S → cheaper relative to sales
       - **P/CF (Price-to-Cash-Flow)**: price / operating cash flow per share
         * Alternative to P/E focusing on cash generation
       - **EV/EBITDA**: enterprise value / EBITDA
         * Accounts for debt, better for comparing leveraged companies
         * Lower EV/EBITDA → cheaper on cash earnings basis

    2. **Profitability Ratios** (how profitable is the business?):
       - **ROA (Return on Assets)**: net income / total assets
         * Measures asset efficiency
         * Higher ROA → better at generating profit from assets
       - **ROE (Return on Equity)**: net income / shareholders' equity
         * Measures shareholder return
         * Higher ROE → better returns for shareholders
         * Can be inflated by high leverage

    3. **Liquidity Ratios** (can the company pay bills?):
       - **Current Ratio**: current assets / current liabilities
         * > 1.0 → can cover short-term obligations
         * < 1.0 → potential liquidity issues
       - **Quick Ratio**: (current assets - inventory) / current liabilities
         * More conservative, excludes inventory
         * > 1.0 → strong immediate liquidity
       - **Cash Ratio**: cash and equivalents / current liabilities
         * Most conservative liquidity measure
         * > 0.5 → very strong cash position

    4. **Leverage Ratios** (how much debt does the company have?):
       - **Debt-to-Equity**: total debt / shareholders' equity
         * Higher ratio → more leveraged, higher financial risk
         * Lower ratio → more conservative capital structure
         * 0 → no debt (all equity financing)

    Use Cases
    ---------
    1. **Stock Screening**: Filter stocks by ratio criteria to find investment candidates
       meeting specific financial criteria (e.g., low P/E value stocks, high ROE quality stocks).

    2. **Comparative Analysis**: Compare financial metrics across companies in the same
       industry to identify leaders and laggards in profitability, efficiency, and valuation.

    3. **Financial Health Assessment**: Evaluate liquidity, leverage, and profitability
       ratios to assess overall financial health and stability.

    4. **Investment Strategy Implementation**: Build factor-based investment strategies
       (value, quality, momentum) using ratio-based screening and ranking.

    Examples
    --------
    Example 1: Get current ratios for Apple
        list_stock_ratios(ticker="AAPL")

        Returns all current financial ratios for Apple using TTM financials and latest price.

    Example 2: Screen for value stocks (low P/E, high dividend yield)
        list_stock_ratios(
            price_to_earnings_lt=15,
            dividend_yield_gt=0.03,
            market_cap_gte=1000000000,
            sort="price_to_earnings.asc",
            limit=50
        )

        Finds stocks with P/E < 15, dividend yield > 3%, market cap >= $1B,
        sorted by P/E ratio ascending (cheapest first).

    Example 3: Screen for quality growth stocks (high ROE, low debt)
        list_stock_ratios(
            return_on_equity_gt=0.20,
            debt_to_equity_lt=0.5,
            earnings_per_share_gt=0,
            sort="return_on_equity.desc",
            limit=100
        )

        Finds profitable stocks with ROE > 20%, D/E < 0.5 (low debt), positive earnings,
        sorted by ROE descending (highest quality first).

    Example 4: Screen for high cash flow generators
        list_stock_ratios(
            price_to_free_cash_flow_lt=20,
            free_cash_flow_gt=1000000000,
            current_gte=1.5,
            sort="free_cash_flow.desc"
        )

        Finds stocks with P/FCF < 20, FCF > $1B, current ratio >= 1.5 (good liquidity),
        sorted by free cash flow descending.

    Example 5: Compare tech giants
        list_stock_ratios(
            ticker_any_of="AAPL,MSFT,GOOGL,META,AMZN",
            sort="ticker.asc"
        )

        Returns current ratios for major tech companies for comparative analysis.

    Notes
    -----
    - **TTM Data**: All ratios use trailing twelve-month (TTM) financial data combined with
      the most recent trading day's stock price. This provides the most current view of
      company fundamentals and valuation.

    - **Missing Ratios**: Some ratios are only calculated when the denominator is positive:
      * price_to_earnings: Only when earnings_per_share > 0
      * price_to_cash_flow: Only when operating cash flow per share > 0
      * price_to_free_cash_flow: Only when free cash flow per share > 0
      * For unprofitable companies, these ratios will be null/missing

    - **Enterprise Value**: Calculated as market_cap + total_debt - cash_and_equivalents.
      Represents the theoretical takeover price and is useful for comparing companies with
      different capital structures.

    - **Free Cash Flow**: Calculated as operating_cash_flow - capital_expenditures. Represents
      cash available for distribution to shareholders or debt reduction after maintaining/
      growing the business.

    - **Ratio Interpretation**: Compare ratios within the same industry for meaningful insights:
      * Tech companies typically have higher P/E ratios than utilities
      * Capital-intensive industries have lower ROA than asset-light businesses
      * Banks have different liquidity ratio norms than industrial companies

    - **Screening Best Practices**:
      * Combine multiple criteria for robust screening (e.g., value + quality + momentum)
      * Use appropriate industry context when setting thresholds
      * Consider market cap minimum to ensure liquidity
      * Verify fundamentals beyond ratios before investing

    - **Decimal vs Percentage**: Ratios are returned as decimals:
      * dividend_yield=0.044 means 4.4% yield
      * return_on_equity=0.25 means 25% ROE
      * Multiply by 100 for percentage representation

    - **Negative Values**: Some ratios can be negative:
      * Negative ROA/ROE → company is losing money
      * Negative debt_to_equity → not possible, indicates data issue
      * Negative free_cash_flow → company consuming cash

    - **Sorting**: Default sort is ticker.asc. Use sort parameter for custom ordering:
      * "price_to_earnings.asc" → cheapest P/E first
      * "return_on_equity.desc" → highest ROE first
      * "market_cap.desc,price_to_earnings.asc" → large caps with low P/E

    - **Volume Liquidity**: average_volume shows 30-day average trading volume. Higher
      volume indicates better liquidity for entering/exiting positions. Consider minimum
      volume thresholds for tradability.

    - **Data Currency**: date field shows when ratios were calculated. Ratios use the most
      recent TTM financials available and the latest trading day's price data.

    - **Pagination**: For large screening results, use limit parameter and next_url from
      response metadata. Default limit is 100, maximum is 50000.

    - **Comparison with Historical Ratios**: This endpoint provides current/latest ratios.
      For historical ratio trends over time, use the list_financials_ratios endpoint which
      provides ratios from SEC filing dates.

    - **Multi-Factor Screening**: Combine filters to implement factor strategies:
      * Value: low price_to_earnings, low price_to_book, high dividend_yield
      * Quality: high return_on_equity, high return_on_assets, low debt_to_equity
      * Growth: high earnings growth (calculate from financials), expanding margins
      * Momentum: Use with price data to identify trends
      * Defensive: high current ratio, low debt_to_equity, stable dividend_yield

    - **Sector Benchmarks**: Typical ratio ranges vary by sector:
      * Technology: High P/E (20-40+), high ROE (20%+), low debt
      * Utilities: Low P/E (10-15), low ROE (8-12%), moderate debt
      * Financials: Low P/B (0.5-2), high ROE (10-15%), leverage is normal
      * Industrials: Moderate across all ratios
      * Consumer Staples: Moderate P/E (15-25), stable margins

    - **Valuation Context**: Absolute ratio levels should be interpreted with market context:
      * Bull markets: Higher average P/E ratios acceptable
      * Bear markets: Flight to quality, focus on low debt and high cash
      * Rising rates: Lower P/E multiples, favor profitable companies
      * Recession: Emphasize liquidity ratios and low leverage
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
    sort: Optional[str] = None,
    params: Optional[Dict[str, Any]] = None,
) -> str:
    """
    Retrieve bi-monthly aggregated short interest data reported to FINRA by broker-dealers,
    providing insights into market sentiment and potential short squeeze opportunities.

    Short interest represents the total number of shares sold short but not yet covered or closed
    out, serving as a key indicator of bearish market sentiment. This data is reported bi-monthly
    to FINRA and includes metrics such as days to cover, which helps assess short squeeze potential
    and market positioning.

    Official API Documentation:
    https://polygon.io/docs/rest/stocks/fundamentals/short-interest

    Parameters
    ----------
    ticker : str, optional
        The primary ticker symbol for the stock.
        Example: "GME"

    days_to_cover : float, optional
        Calculated as short_interest ÷ avg_daily_volume, representing the estimated
        number of days it would take to cover all short positions based on average volume.

    settlement_date : str or date, optional
        The date (YYYY-MM-DD) on which short interest data is considered settled,
        based on exchange reporting schedules.
        Example: "2025-03-14"

    avg_daily_volume : int, optional
        The average daily trading volume over a specified period, used to contextualize
        short interest.

    ticker_any_of : str, optional
        Filter equal to any of the ticker values (comma-separated).
        Example: "GME,AMC,TSLA"

    ticker_gt : str, optional
        Filter ticker greater than the value (lexicographic).

    ticker_gte : str, optional
        Filter ticker greater than or equal to the value.

    ticker_lt : str, optional
        Filter ticker less than the value.

    ticker_lte : str, optional
        Filter ticker less than or equal to the value.

    days_to_cover_any_of : str, optional
        Filter equal to any of the days_to_cover values (comma-separated).

    days_to_cover_gt : float, optional
        Filter days_to_cover greater than the value.

    days_to_cover_gte : float, optional
        Filter days_to_cover greater than or equal to the value.

    days_to_cover_lt : float, optional
        Filter days_to_cover less than the value.

    days_to_cover_lte : float, optional
        Filter days_to_cover less than or equal to the value.

    settlement_date_any_of : str, optional
        Filter equal to any of the settlement_date values (comma-separated).

    settlement_date_gt : str or date, optional
        Filter settlement_date greater than the value.
        Format: "YYYY-MM-DD"

    settlement_date_gte : str or date, optional
        Filter settlement_date greater than or equal to the value.
        Format: "YYYY-MM-DD"

    settlement_date_lt : str or date, optional
        Filter settlement_date less than the value.
        Format: "YYYY-MM-DD"

    settlement_date_lte : str or date, optional
        Filter settlement_date less than or equal to the value.
        Format: "YYYY-MM-DD"

    avg_daily_volume_any_of : str, optional
        Filter equal to any of the avg_daily_volume values (comma-separated).

    avg_daily_volume_gt : int, optional
        Filter avg_daily_volume greater than the value.

    avg_daily_volume_gte : int, optional
        Filter avg_daily_volume greater than or equal to the value.

    avg_daily_volume_lt : int, optional
        Filter avg_daily_volume less than the value.

    avg_daily_volume_lte : int, optional
        Filter avg_daily_volume less than or equal to the value.

    limit : int, optional
        Maximum number of results to return.
        Default: 10, Maximum: 50000

    sort : str, optional
        Comma-separated list of sort columns with direction.
        Format: "field.asc" or "field.desc"
        Default: "ticker.asc"
        Example: "days_to_cover.desc,ticker.asc"

    params : dict, optional
        Additional query parameters for advanced filtering.

    Returns
    -------
    str
        CSV-formatted string containing short interest data with the following fields:

        - ticker: Stock ticker symbol
        - settlement_date: Date when short interest data is settled (YYYY-MM-DD)
        - short_interest: Total shares sold short but not yet covered
        - avg_daily_volume: Average daily trading volume
        - days_to_cover: Estimated days to cover shorts (short_interest ÷ avg_daily_volume)

    Understanding Short Interest
    ----------------------------
    Short interest provides critical insights into market sentiment and positioning:

    1. **What is Short Interest?**
       - Total shares sold short (borrowed and sold, expecting price to fall)
       - Not yet covered (bought back to return to lender)
       - Represents bearish positions in the stock

    2. **Days to Cover** (short ratio):
       - Formula: short_interest ÷ avg_daily_volume
       - Interpretation:
         * Low (0-3 days): Shorts can cover quickly, low squeeze risk
         * Moderate (3-5 days): Some squeeze potential if buying pressure emerges
         * High (5-7 days): Significant squeeze potential, difficult to cover quickly
         * Very High (7+ days): Extreme squeeze risk, positions may be trapped
       - Indicates how many days of average volume needed to close all shorts

    3. **Short Interest Ratio** (% of float):
       - Calculate: (short_interest ÷ shares outstanding) × 100
       - High percentage (>20%): Heavy bearish positioning
       - Combined with high days to cover → short squeeze setup

    4. **Reporting Schedule**:
       - Data reported bi-monthly to FINRA (twice per month)
       - Settlement dates typically mid-month and end-of-month
       - Reported by broker-dealers
       - Represents snapshot at settlement date, not real-time

    Short Squeeze Mechanics
    -----------------------
    A short squeeze occurs when heavily shorted stocks rise rapidly, forcing shorts
    to cover positions by buying shares, further accelerating price increases:

    1. **Setup Conditions**:
       - High short interest (>20% of float)
       - High days to cover (>5-7 days)
       - Positive catalyst or buying pressure emerges
       - Limited liquidity or low float

    2. **Squeeze Process**:
       - Stock price rises unexpectedly
       - Shorts face losses, margin calls
       - Forced covering creates buying pressure
       - Price accelerates upward in self-reinforcing cycle
       - Continues until shorts capitulate and cover

    3. **Risk Factors**:
       - Shorts have unlimited loss potential (stock can rise indefinitely)
       - Margin calls force covering at inopportune times
       - Coordinated buying can trigger squeezes (as seen in meme stock events)

    Use Cases
    ---------
    1. **Market Sentiment Analysis**: Monitor short interest trends to gauge bearish
       sentiment and identify heavily shorted stocks that may face selling pressure.

    2. **Short Squeeze Prediction**: Identify potential short squeeze candidates by
       screening for high days to cover combined with positive catalysts or momentum.

    3. **Risk Management**: Track short interest in portfolio holdings to anticipate
       volatility from potential squeezes or heavy bearish positioning.

    4. **Trading Strategy Refinement**: Incorporate short interest data into trading
       strategies for mean reversion, squeeze plays, or contrarian positioning.

    Examples
    --------
    Example 1: Get short interest history for GameStop
        list_short_interest(
            ticker="GME",
            limit=20,
            sort="settlement_date.desc"
        )

        Returns last 20 short interest reports for GME showing historical short positioning.

    Example 2: Find recent high short squeeze candidates
        list_short_interest(
            days_to_cover_gt=5,
            settlement_date_gte="2025-01-01",
            sort="days_to_cover.desc",
            limit=50
        )

        Finds stocks with >5 days to cover since Jan 2025, sorted by days to cover
        descending (highest squeeze potential first).

    Example 3: Track short interest for multiple meme stocks
        list_short_interest(
            ticker_any_of="GME,AMC,BBBY,BB",
            settlement_date_gte="2024-01-01",
            sort="settlement_date.desc"
        )

        Returns short interest history for multiple heavily-watched stocks since 2024.

    Example 4: Screen for low liquidity with high short interest
        list_short_interest(
            days_to_cover_gt=7,
            avg_daily_volume_lt=1000000,
            sort="days_to_cover.desc"
        )

        Finds stocks with very high days to cover (>7) and low volume (<1M shares/day),
        indicating potential squeeze setups with limited liquidity.

    Example 5: Monitor short interest trends for a specific stock
        list_short_interest(
            ticker="TSLA",
            settlement_date_gte="2024-01-01",
            sort="settlement_date.asc",
            limit=100
        )

        Returns all Tesla short interest reports since 2024 in chronological order
        for trend analysis.

    Notes
    -----
    - **Bi-Monthly Reporting**: Short interest data is reported twice monthly to FINRA,
      typically with settlement dates around the 15th and end of each month. Data lags
      by several days from settlement date to publication.

    - **Days to Cover Calculation**: days_to_cover = short_interest ÷ avg_daily_volume.
      This assumes average volume remains constant and shorts cover at normal pace.
      During squeezes, volume spikes dramatically, reducing actual covering time.

    - **Not Real-Time**: Short interest represents a snapshot at the settlement date.
      Actual short positions may have changed significantly by the time data is published.
      Use list_short_volume for daily short selling activity.

    - **Short Interest vs Short Volume**:
      * Short Interest = Outstanding short positions (cumulative, bi-monthly)
      * Short Volume = Daily short sale activity (transactions, daily)
      * Use short interest for positioning, short volume for sentiment shifts

    - **Interpretation Context**: High short interest isn't always bearish signal:
      * May indicate rational skepticism about overvaluation
      * Experienced shorts may have good reasons for bearish positions
      * Not all heavily shorted stocks squeeze
      * Consider fundamentals alongside short metrics

    - **Float Percentage**: To calculate short interest as % of float:
      * Get shares outstanding from ticker details
      * Calculate: (short_interest ÷ shares outstanding) × 100
      * >20% typically considered high
      * >40% considered extremely high

    - **Historical Patterns**: Analyze short interest trends over time:
      * Rising short interest → increasing bearish sentiment
      * Falling short interest → shorts covering or sentiment improving
      * Stable short interest → entrenched bearish positioning
      * Sudden drops → potential squeeze or bearish thesis broken

    - **Combine with Price Action**: Most effective when combined with price data:
      * Rising price + rising short interest → shorts fighting rally (squeeze risk)
      * Falling price + falling short interest → shorts taking profits
      * Rising price + falling short interest → squeeze potentially underway
      * Falling price + rising short interest → bears in control

    - **Liquidity Considerations**: avg_daily_volume provides context for short interest:
      * High short interest with low volume → illiquid, higher squeeze risk
      * High short interest with high volume → more liquid, easier to cover
      * Compare to typical volume patterns for the stock

    - **Regulatory Changes**: Short interest reporting rules and schedules may change.
      FINRA publishes official short interest data; verify against official sources
      for compliance and regulatory purposes.

    - **Short Squeeze Examples**: Notable historical squeezes:
      * Volkswagen (2008): Days to cover exceeded 100, briefly became world's most valuable company
      * GameStop (2021): Days to cover ~6, sparked by retail coordination
      * Tesla (multiple): High short interest squeezed repeatedly during 2020 rally
      * Study these events to understand squeeze mechanics and dynamics

    - **Risk Warning**: Short squeeze plays are highly speculative and volatile:
      * Can reverse suddenly when buying pressure exhausts
      * Fundamentals may not support elevated prices
      * Regulatory or company actions can impact outcomes
      * Use appropriate position sizing and risk management

    - **Sorting Options**: Useful sorting strategies:
      * "days_to_cover.desc" → Find highest squeeze candidates
      * "settlement_date.desc" → Most recent data first
      * "ticker.asc" → Alphabetical for systematic analysis
      * "avg_daily_volume.asc" → Find illiquid candidates

    - **Screening Best Practices**: Effective short interest screening:
      * Combine days_to_cover threshold with volume filters
      * Look for trend changes (compare to prior periods)
      * Cross-reference with news/catalysts
      * Verify float calculations for accuracy
      * Monitor multiple settlement dates for trend confirmation

    - **Pagination**: For large screening results, use limit parameter and next_url
      from response metadata. Default limit is 10, maximum is 50000.

    - **Data Quality**: While FINRA data is authoritative, reporting may have errors:
      * Corporate actions (splits, offerings) affect calculations
      * Float changes impact short interest percentages
      * Verify unusually high/low values
      * Cross-reference with company filings for share counts
    """
    try:
        results = polygon_client.list_short_interest(
            ticker=ticker,
            settlement_date=settlement_date,
            settlement_date_lt=settlement_date_lt,
            settlement_date_lte=settlement_date_lte,
            settlement_date_gt=settlement_date_gt,
            settlement_date_gte=settlement_date_gte,
            limit=limit,
            sort=sort,
            params={
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
            },
            raw=True,
        )

        # Convert to CSV
        csv_data = json_to_csv(results.data.decode("utf-8"))

        # Process with intelligent caching
        return await process_tool_response(
            tool_name="list_short_interest",
            params={
                "ticker": ticker,
                "limit": limit,
            },
            csv_data=csv_data,
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
    sort: Optional[str] = None,
    params: Optional[Dict[str, Any]] = None,
) -> str:
    """
    Retrieve daily aggregated short sale volume data reported to FINRA from off-exchange trading
    venues and alternative trading systems (ATS), enabling real-time monitoring of short-selling activity.

    Unlike short interest (which measures outstanding short positions bi-monthly), short volume captures
    the daily trading activity of short sales. This provides immediate insights into market sentiment
    shifts, trading behavior patterns, and potential price movements, helping traders detect trends
    before they appear in lagging short interest reports.

    Official API Documentation:
    https://polygon.io/docs/rest/stocks/fundamentals/short-volume

    Parameters
    ----------
    ticker : str, optional
        The primary ticker symbol for the stock.
        Example: "TSLA"

    date : str or date, optional
        The date of trade activity reported.
        Format: "YYYY-MM-DD"
        Example: "2025-03-25"

    short_volume_ratio : float, optional
        The percentage of total volume that was sold short.
        Calculated as (short_volume / total_volume) × 100
        Example: 31.57 represents 31.57%

    total_volume : int, optional
        Total reported volume across all venues for the ticker on the given date.

    ticker_any_of : str, optional
        Filter equal to any of the ticker values (comma-separated).
        Example: "TSLA,NVDA,AMD"

    ticker_gt : str, optional
        Filter ticker greater than the value (lexicographic).

    ticker_gte : str, optional
        Filter ticker greater than or equal to the value.

    ticker_lt : str, optional
        Filter ticker less than the value.

    ticker_lte : str, optional
        Filter ticker less than or equal to the value.

    date_any_of : str, optional
        Filter equal to any of the date values (comma-separated).

    date_gt : str or date, optional
        Filter date greater than the value.
        Format: "YYYY-MM-DD"

    date_gte : str or date, optional
        Filter date greater than or equal to the value.
        Format: "YYYY-MM-DD"

    date_lt : str or date, optional
        Filter date less than the value.
        Format: "YYYY-MM-DD"

    date_lte : str or date, optional
        Filter date less than or equal to the value.
        Format: "YYYY-MM-DD"

    short_volume_ratio_any_of : str, optional
        Filter equal to any of the short_volume_ratio values (comma-separated).

    short_volume_ratio_gt : float, optional
        Filter short_volume_ratio greater than the value.

    short_volume_ratio_gte : float, optional
        Filter short_volume_ratio greater than or equal to the value.

    short_volume_ratio_lt : float, optional
        Filter short_volume_ratio less than the value.

    short_volume_ratio_lte : float, optional
        Filter short_volume_ratio less than or equal to the value.

    total_volume_any_of : str, optional
        Filter equal to any of the total_volume values (comma-separated).

    total_volume_gt : int, optional
        Filter total_volume greater than the value.

    total_volume_gte : int, optional
        Filter total_volume greater than or equal to the value.

    total_volume_lt : int, optional
        Filter total_volume less than the value.

    total_volume_lte : int, optional
        Filter total_volume less than or equal to the value.

    limit : int, optional
        Maximum number of results to return.
        Default: 10, Maximum: 50000

    sort : str, optional
        Comma-separated list of sort columns with direction.
        Format: "field.asc" or "field.desc"
        Default: "ticker.asc"
        Example: "short_volume_ratio.desc,date.asc"

    params : dict, optional
        Additional query parameters for advanced filtering.

    Returns
    -------
    str
        CSV-formatted string containing daily short volume data with the following fields:

        Basic Information:
        - ticker: Stock ticker symbol
        - date: Trade date (YYYY-MM-DD)
        - total_volume: Total reported volume across all venues
        - short_volume: Total shares sold short
        - short_volume_ratio: Percentage of volume sold short ((short_volume / total_volume) × 100)

        Exempt vs Non-Exempt:
        - exempt_volume: Short volume exempt from Regulation SHO
        - non_exempt_volume: Short volume subject to Regulation SHO (short_volume - exempt_volume)

        Venue Breakdown:
        - nyse_short_volume: Short volume from NYSE facilities (non-exempt)
        - nyse_short_volume_exempt: Short volume from NYSE (exempt)
        - nasdaq_carteret_short_volume: Short volume from Nasdaq Carteret facility (non-exempt)
        - nasdaq_carteret_short_volume_exempt: Short volume from Nasdaq Carteret (exempt)
        - nasdaq_chicago_short_volume: Short volume from Nasdaq Chicago facility (non-exempt)
        - nasdaq_chicago_short_volume_exempt: Short volume from Nasdaq Chicago (exempt)
        - adf_short_volume: Short volume via Alternative Display Facility (non-exempt)
        - adf_short_volume_exempt: Short volume via ADF (exempt)

    Understanding Short Volume
    ---------------------------
    Short volume provides daily insights into short-selling activity:

    1. **What is Short Volume?**
       - Number of shares sold short on a specific trading day
       - Reported daily from off-exchange venues and ATS
       - Captures short sale transactions, not outstanding positions
       - Differs from short interest (which is cumulative, bi-monthly)

    2. **Short Volume Ratio Interpretation**:
       - Formula: (short_volume / total_volume) × 100
       - Low (< 30%): Normal or bullish sentiment
       - Moderate (30-40%): Typical for many stocks
       - High (40-50%): Elevated short selling
       - Very High (> 50%): Heavy bearish pressure
       - Extreme (> 60%): Intense bearish sentiment or hedging activity

    3. **Exempt vs Non-Exempt Volume**:
       - **Exempt Volume**: Short sales exempt from Regulation SHO
         * Market maker hedging activities
         * Bona fide market making
         * Not subject to locate requirements
       - **Non-Exempt Volume**: Regular short sales
         * Subject to Regulation SHO
         * Must locate shares before selling
         * Directional bearish bets

    4. **Venue Breakdown**:
       - **NYSE**: New York Stock Exchange facilities
       - **Nasdaq Carteret**: Nasdaq's primary facility in New Jersey
       - **Nasdaq Chicago**: Nasdaq's Chicago-based facility
       - **ADF**: Alternative Display Facility for off-exchange reporting
       - Venue distribution can indicate institutional vs retail activity

    Short Volume vs Short Interest
    -------------------------------
    Understanding the critical differences:

    | Metric | Short Volume | Short Interest |
    |--------|--------------|----------------|
    | **Frequency** | Daily | Bi-monthly |
    | **Measures** | Daily transactions | Outstanding positions |
    | **Timeframe** | Single day | Cumulative snapshot |
    | **Use Case** | Immediate sentiment | Long-term positioning |
    | **Lag** | Real-time (T+1) | Several days delay |
    | **Data Source** | FINRA TRF | FINRA reports |

    - High short volume doesn't always mean increasing short interest
    - Short volume includes intraday covering (buy-to-cover same day)
    - Market makers may short for liquidity without bearish intent
    - Use both metrics together for complete picture

    Use Cases
    ---------
    1. **Intraday Sentiment Analysis**: Monitor daily short volume ratio to detect shifts
       in bearish sentiment before they appear in bi-monthly short interest data.

    2. **Short-Sale Trend Identification**: Track short volume patterns over time to
       identify sustained bearish pressure or potential reversal signals.

    3. **Liquidity Analysis**: Analyze venue breakdowns and total volume to assess
       market maker activity and overall trading liquidity.

    4. **Trading Strategy Optimization**: Incorporate short volume signals into entry/exit
       timing, contrarian plays, or momentum strategies.

    Examples
    --------
    Example 1: Get daily short volume for Tesla over past month
        list_short_volume(
            ticker="TSLA",
            date_gte="2025-03-01",
            sort="date.desc",
            limit=30
        )

        Returns last 30 days of Tesla short volume data to identify trends.

    Example 2: Find days with extreme short selling (>50% ratio)
        list_short_volume(
            short_volume_ratio_gt=50,
            date_gte="2025-01-01",
            sort="short_volume_ratio.desc",
            limit=100
        )

        Identifies days with heavy short selling pressure across all stocks.

    Example 3: Monitor multiple tech stocks for short volume trends
        list_short_volume(
            ticker_any_of="NVDA,AMD,INTC",
            date_gte="2025-03-01",
            sort="date.desc"
        )

        Tracks daily short volume for semiconductor stocks for comparative analysis.

    Example 4: Analyze high-volume days with significant short activity
        list_short_volume(
            ticker="GME",
            total_volume_gt=10000000,
            short_volume_ratio_gt=40,
            sort="date.desc"
        )

        Finds GameStop trading days with high volume and elevated short selling.

    Example 5: Track short volume for specific date
        list_short_volume(
            ticker="AAPL",
            date="2025-03-25"
        )

        Gets Apple's short volume breakdown for a specific trading day.

    Notes
    -----
    - **Daily Reporting**: Short volume is reported daily (T+1) from off-exchange venues
      and ATS. On-exchange short volume is not included in this data.

    - **Not a Direct Indicator**: High short volume doesn't necessarily mean increasing
      short interest. Market makers, day traders, and arbitrageurs may short and cover
      within the same day, contributing to short volume without affecting short interest.

    - **Market Maker Activity**: Exempt volume often represents market maker hedging.
      High exempt volume relative to non-exempt suggests liquidity provision rather than
      directional bearish bets.

    - **Intraday Covering**: Short volume includes shorts that were covered same day.
      A stock with 1M short volume might have had shorts open and close positions,
      resulting in minimal net change to short interest.

    - **Baseline Levels**: Many stocks maintain consistent short volume ratios:
      * 30-40% is common for liquid large-cap stocks
      * Market making and hedging create baseline short volume
      * Look for deviations from stock's typical ratio

    - **Interpretation Context**: Analyze short volume with additional context:
      * Price action: Rising price + high short volume → shorts fighting rally
      * News events: High short volume after bad news is expected
      * Volatility: High vol days often have elevated short volume
      * Compare to stock's historical average

    - **Regulation SHO**: Exempt status relates to SEC Regulation SHO:
      * Non-exempt shorts must locate shares before selling (locate requirement)
      * Exempt shorts (market makers) don't need to locate
      * Helps distinguish directional bets from market making

    - **Venue Analysis**: Venue distribution provides insights:
      * Nasdaq facilities: Often high-frequency and institutional flow
      * NYSE: May indicate different trading patterns
      * ADF: Alternative venue, can show off-exchange activity concentration
      * Uneven distribution may indicate specific trading strategies

    - **Time Series Analysis**: Most valuable when analyzing trends:
      * Rising short volume ratio → increasing bearish pressure
      * Falling ratio → decreasing bearish activity
      * Spikes → Event-driven short selling (earnings, news)
      * Compare to price trends for divergences

    - **Contrarian Signals**: Extreme short volume can be contrarian:
      * Very high ratio (>60%) may indicate exhaustion
      * Unusually low ratio (<20%) after decline might signal capitulation
      * Use with other technical indicators for confirmation

    - **Combining with Short Interest**: Use together for complete picture:
      * High short interest + high short volume → Bears adding positions
      * High short interest + low short volume → Bears holding, not adding
      * Low short interest + high short volume → Intraday/short-term shorts
      * Rising short volume before SI report → Predict next SI increase

    - **Off-Exchange Focus**: This data covers off-exchange and ATS venues only:
      * Does not include on-exchange short sales
      * Represents significant portion but not total short volume
      * FINRA TRF (Trade Reporting Facility) data
      * Complements exchange-reported data

    - **Calculation Verification**: short_volume_ratio = (short_volume / total_volume) × 100
      * Verify calculations when analyzing data
      * Total_volume should be >= short_volume
      * Ratio should be between 0 and 100

    - **Historical Patterns**: Build baselines for interpretation:
      * Calculate average ratio for each stock over time
      * Identify typical range (e.g., 30-40% for stock X)
      * Flag anomalies (2+ standard deviations from mean)
      * Seasonal patterns (month-end, options expiration)

    - **Screening Strategies**: Effective short volume screening:
      * Filter by ratio thresholds (>50% for extreme days)
      * Combine with volume filters (high volume + high ratio)
      * Look for multi-day trends (3+ days increasing ratio)
      * Cross-reference with price action
      * Monitor stocks approaching short interest reporting dates

    - **Data Lag**: Reported on T+1 basis:
      * Monday's data available Tuesday
      * Weekend gaps (no Sat/Sun data)
      * Holidays create reporting gaps
      * Plan analysis accounting for lag

    - **Sorting Options**: Useful sorting strategies:
      * "short_volume_ratio.desc" → Find highest short pressure days
      * "date.desc" → Most recent data first
      * "total_volume.desc" → Highest activity days
      * "ticker.asc" → Alphabetical for systematic analysis

    - **Pagination**: For large screening results, use limit parameter and next_url
      from response metadata. Default limit is 10, maximum is 50000.

    - **Quality Considerations**: While FINRA data is authoritative:
      * Reporting errors can occur
      * Venue attribution may have inconsistencies
      * Cross-validate unusual values
      * Use official FINRA sources for compliance purposes
    """
    try:
        results = polygon_client.list_short_volume(
            ticker=ticker,
            date=date,
            date_lt=date_lt,
            date_lte=date_lte,
            date_gt=date_gt,
            date_gte=date_gte,
            limit=limit,
            sort=sort,
            params={
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
            },
            raw=True,
        )

        # Convert to CSV
        csv_data = json_to_csv(results.data.decode("utf-8"))

        # Process with intelligent caching
        return await process_tool_response(
            tool_name="list_short_volume",
            params={
                "ticker": ticker,
                "limit": limit,
            },
            csv_data=csv_data,
        )
    except Exception as e:
        return f"Error: {e}"
