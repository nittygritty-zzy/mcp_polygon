"""Auto-generated tool definitions."""

from typing import Optional, Any, Dict, Union, List
from mcp.types import ToolAnnotations
from datetime import datetime, date
from ..clients import poly_mcp, polygon_client
from ..formatters import json_to_csv
from ..tool_integration import process_tool_response





@poly_mcp.tool(annotations=ToolAnnotations(readOnlyHint=True))
async def list_splits(
    ticker: Optional[str] = None,
    execution_date: Optional[Union[str, datetime, date]] = None,
    reverse_split: Optional[bool] = None,
    ticker_gte: Optional[str] = None,
    ticker_gt: Optional[str] = None,
    ticker_lte: Optional[str] = None,
    ticker_lt: Optional[str] = None,
    execution_date_gte: Optional[Union[str, datetime, date]] = None,
    execution_date_gt: Optional[Union[str, datetime, date]] = None,
    execution_date_lte: Optional[Union[str, datetime, date]] = None,
    execution_date_lt: Optional[Union[str, datetime, date]] = None,
    order: Optional[str] = None,
    limit: Optional[int] = 10,
    sort: Optional[str] = None,
    params: Optional[Dict[str, Any]] = None,
) -> str:
    """
    Retrieve historical stock split events, including execution dates and ratio factors, to understand
    changes in a company's share structure over time. Polygon.io leverages this data for accurate price
    adjustments in other endpoints, such as the Aggregates API, ensuring that users can access both
    adjusted and unadjusted views of historical prices for more informed analysis.

    Reference: https://polygon.io/docs/rest/stocks/corporate-actions/splits

    Parameters:
    - ticker: Case-sensitive ticker symbol (e.g., "AAPL" for Apple Inc., "TSLA" for Tesla)
    - execution_date: Query by execution date (YYYY-MM-DD format) - the date when the split was applied
    - reverse_split: Query for reverse stock splits (default: not used, returns all splits)
                     * True = only reverse splits (split_from > split_to, e.g., 1-for-10)
                     * False = only forward splits (split_to > split_from, e.g., 2-for-1)
                     * None/not specified = both types
    - ticker_gte: Range by ticker - greater than or equal to (lexicographic)
    - ticker_gt: Range by ticker - greater than (lexicographic)
    - ticker_lte: Range by ticker - less than or equal to (lexicographic)
    - ticker_lt: Range by ticker - less than (lexicographic)
    - execution_date_gte: Range by execution_date - greater than or equal to
    - execution_date_gt: Range by execution_date - greater than
    - execution_date_lte: Range by execution_date - less than or equal to
    - execution_date_lt: Range by execution_date - less than
    - order: Order results based on the sort field ("asc" or "desc")
    - limit: Limit the number of results returned (default: 10, max: 1000)
    - sort: Sort field used for ordering (e.g., "execution_date", "ticker")

    Response includes:
    - results[]: Array of stock split events with:
      - ticker: Ticker symbol of the stock split
      - execution_date: Date when the stock split was applied (YYYY-MM-DD)
      - split_to: First number in the split ratio (e.g., in 2-for-1 split, split_to = 2)
      - split_from: Second number in the split ratio (e.g., in 2-for-1 split, split_from = 1)
      - id: Unique identifier for this stock split event
    - next_url: If present, use to fetch the next page of data
    - request_id: Server-assigned request identifier
    - status: Response status (e.g., "OK")

    Understanding Split Ratios:
    - Split ratio format: split_to-for-split_from
    - Forward split (increases shares, decreases price):
      * split_to > split_from
      * Example: 2-for-1 split → split_to=2, split_from=1
      * 100 shares at $100 becomes 200 shares at $50
    - Reverse split (decreases shares, increases price):
      * split_from > split_to
      * Example: 1-for-10 split → split_to=1, split_from=10
      * 100 shares at $10 becomes 10 shares at $100

    Price Adjustment Calculation:
    - Adjustment factor = split_to / split_from
    - Forward split (2-for-1): factor = 2/1 = 2.0
      * Pre-split price $100 → Post-split adjusted price = $100 / 2.0 = $50
    - Reverse split (1-for-10): factor = 1/10 = 0.1
      * Pre-split price $10 → Post-split adjusted price = $10 / 0.1 = $100

    Use Cases:
    - Historical analysis: Understand how share structure evolved over time
    - Price adjustments: Apply split factors to calculate adjusted historical prices
    - Data consistency: Ensure accurate price comparisons across split events
    - Modeling: Incorporate split effects into valuation and forecasting models

    Example: list_splits() returns recent stock splits with default parameters

    Example: list_splits(ticker="AAPL") returns all historical stock splits for Apple,
             showing the 4-for-1 split in 2020 and 2-for-1 split in 2005

    Example: list_splits(reverse_split=True, limit=50) returns up to 50 reverse stock
             splits, useful for identifying struggling companies

    Example: list_splits(execution_date_gte="2020-01-01", execution_date_lte="2024-12-31")
             returns all stock splits that occurred between 2020 and 2024

    Example: list_splits(ticker_gte="A", ticker_lt="B", execution_date_gte="2023-01-01")
             returns splits for tickers starting with 'A' that occurred since 2023

    Note: Stock splits considerations and best practices:
    - Polygon.io uses split data to provide adjusted prices in Aggregates API:
      * adjusted=True (default) - Historical prices adjusted for all splits
      * adjusted=False - Raw historical prices without split adjustments
    - Forward splits (most common):
      * Companies split when stock price becomes too high
      * Makes shares more affordable for retail investors
      * Common ratios: 2-for-1, 3-for-1, 4-for-1, 10-for-1
      * Example: Apple's 4-for-1 split in August 2020 (split_to=4, split_from=1)
      * Example: Tesla's 3-for-1 split in August 2022
    - Reverse splits (less common):
      * Companies use to boost low stock price
      * Often signals financial distress or exchange listing requirements
      * Common ratios: 1-for-5, 1-for-10, 1-for-20
      * NYSE requires minimum $1 price, NASDAQ requires $1-$5 depending on listing
      * Use reverse_split=True to filter for potentially distressed companies
    - Split effects:
      * Share price changes proportionally (inversely to split ratio)
      * Number of shares changes proportionally (directly to split ratio)
      * Market capitalization remains unchanged
      * Earnings per share (EPS) adjusts proportionally
      * Historical P/E ratios remain accurate when using adjusted prices
    - Data accuracy:
      * Execution_date is when split takes effect (post-split trading begins)
      * Price adjustments apply retroactively to all pre-split historical data
      * Important for backtesting strategies and performance analysis
    - Use cases by role:
      * Traders: Identify splits that may create short-term volatility or liquidity changes
      * Analysts: Adjust historical data for accurate trend analysis
      * Quants: Incorporate split factors in backtesting algorithms
      * Investors: Understand share structure changes for long-term holdings
    - Filtering strategies:
      * Use ticker ranges to scan alphabetically (ticker_gte="A", ticker_lt="B")
      * Use execution_date ranges for time-period analysis
      * Filter reverse_split=True to identify companies facing price pressure
      * Sort by execution_date to see chronological split history
    - Integration with other endpoints:
      * Aggregates API uses split data for adjusted prices
      * Ticker Details shows current shares_outstanding (post-split)
      * Compare pre-split and post-split data using adjusted parameter
    """
    try:
        results = polygon_client.list_splits(
            ticker=ticker,
            execution_date=execution_date,
            reverse_split=reverse_split,
            limit=limit,
            sort=sort,
            order=order,
            params={
                **(params or {}),
                **{
                    k: v
                    for k, v in {
                        "ticker.gte": ticker_gte,
                        "ticker.gt": ticker_gt,
                        "ticker.lte": ticker_lte,
                        "ticker.lt": ticker_lt,
                        "execution_date.gte": execution_date_gte,
                        "execution_date.gt": execution_date_gt,
                        "execution_date.lte": execution_date_lte,
                        "execution_date.lt": execution_date_lt,
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
            tool_name="list_splits",
            params={
                "ticker": ticker,
                "limit": limit,
            },
            csv_data=csv_data,
        )
    except Exception as e:
        return f"Error: {e}"





@poly_mcp.tool(annotations=ToolAnnotations(readOnlyHint=True))
async def list_dividends(
    ticker: Optional[str] = None,
    ex_dividend_date: Optional[Union[str, datetime, date]] = None,
    record_date: Optional[Union[str, datetime, date]] = None,
    declaration_date: Optional[Union[str, datetime, date]] = None,
    pay_date: Optional[Union[str, datetime, date]] = None,
    frequency: Optional[int] = None,
    cash_amount: Optional[float] = None,
    dividend_type: Optional[str] = None,
    ticker_gte: Optional[str] = None,
    ticker_gt: Optional[str] = None,
    ticker_lte: Optional[str] = None,
    ticker_lt: Optional[str] = None,
    ex_dividend_date_gte: Optional[Union[str, datetime, date]] = None,
    ex_dividend_date_gt: Optional[Union[str, datetime, date]] = None,
    ex_dividend_date_lte: Optional[Union[str, datetime, date]] = None,
    ex_dividend_date_lt: Optional[Union[str, datetime, date]] = None,
    record_date_gte: Optional[Union[str, datetime, date]] = None,
    record_date_gt: Optional[Union[str, datetime, date]] = None,
    record_date_lte: Optional[Union[str, datetime, date]] = None,
    record_date_lt: Optional[Union[str, datetime, date]] = None,
    declaration_date_gte: Optional[Union[str, datetime, date]] = None,
    declaration_date_gt: Optional[Union[str, datetime, date]] = None,
    declaration_date_lte: Optional[Union[str, datetime, date]] = None,
    declaration_date_lt: Optional[Union[str, datetime, date]] = None,
    pay_date_gte: Optional[Union[str, datetime, date]] = None,
    pay_date_gt: Optional[Union[str, datetime, date]] = None,
    pay_date_lte: Optional[Union[str, datetime, date]] = None,
    pay_date_lt: Optional[Union[str, datetime, date]] = None,
    cash_amount_gte: Optional[float] = None,
    cash_amount_gt: Optional[float] = None,
    cash_amount_lte: Optional[float] = None,
    cash_amount_lt: Optional[float] = None,
    order: Optional[str] = None,
    limit: Optional[int] = 10,
    sort: Optional[str] = None,
    params: Optional[Dict[str, Any]] = None,
) -> str:
    """
    Retrieve a historical record of cash dividend distributions for a given ticker, including declaration,
    ex-dividend, record, and pay dates, as well as payout amounts and frequency. This endpoint consolidates
    key dividend information, enabling users to account for dividend income in returns, develop dividend-
    focused strategies, and support tax reporting needs.

    Reference: https://polygon.io/docs/rest/stocks/corporate-actions/dividends

    Parameters:
    - ticker: Case-sensitive ticker symbol (e.g., "AAPL" for Apple Inc., "MSFT" for Microsoft)
    - ex_dividend_date: Query by ex-dividend date (YYYY-MM-DD) - date stock trades without dividend
    - record_date: Query by record date (YYYY-MM-DD) - date stock must be held to receive dividend
    - declaration_date: Query by declaration date (YYYY-MM-DD) - date dividend was announced
    - pay_date: Query by pay date (YYYY-MM-DD) - date dividend is paid out
    - frequency: Query by dividend payment frequency:
      * 0 = One-time (special dividend)
      * 1 = Annually
      * 2 = Bi-annually (twice per year)
      * 4 = Quarterly
      * 12 = Monthly
      * 24 = Bi-monthly
      * 52 = Weekly
    - cash_amount: Query by exact cash amount of dividend per share
    - dividend_type: Query by dividend type:
      * "CD" = Cash Dividend (regular, expected on consistent schedules)
      * "SC" = Special Cash (infrequent, unusual, not expected to recur)
      * "LT" = Long-Term capital gain distribution
      * "ST" = Short-Term capital gain distribution
    - ticker_gte: Range by ticker - greater than or equal to (lexicographic)
    - ticker_gt: Range by ticker - greater than (lexicographic)
    - ticker_lte: Range by ticker - less than or equal to (lexicographic)
    - ticker_lt: Range by ticker - less than (lexicographic)
    - ex_dividend_date_gte: Range by ex_dividend_date - greater than or equal to
    - ex_dividend_date_gt: Range by ex_dividend_date - greater than
    - ex_dividend_date_lte: Range by ex_dividend_date - less than or equal to
    - ex_dividend_date_lt: Range by ex_dividend_date - less than
    - record_date_gte: Range by record_date - greater than or equal to
    - record_date_gt: Range by record_date - greater than
    - record_date_lte: Range by record_date - less than or equal to
    - record_date_lt: Range by record_date - less than
    - declaration_date_gte: Range by declaration_date - greater than or equal to
    - declaration_date_gt: Range by declaration_date - greater than
    - declaration_date_lte: Range by declaration_date - less than or equal to
    - declaration_date_lt: Range by declaration_date - less than
    - pay_date_gte: Range by pay_date - greater than or equal to
    - pay_date_gt: Range by pay_date - greater than
    - pay_date_lte: Range by pay_date - less than or equal to
    - pay_date_lt: Range by pay_date - less than
    - cash_amount_gte: Range by cash_amount - greater than or equal to
    - cash_amount_gt: Range by cash_amount - greater than
    - cash_amount_lte: Range by cash_amount - less than or equal to
    - cash_amount_lt: Range by cash_amount - less than
    - order: Order results based on the sort field ("asc" or "desc")
    - limit: Limit the number of results returned (default: 10, max: 1000)
    - sort: Sort field used for ordering (e.g., "ex_dividend_date", "pay_date")

    Response includes:
    - results[]: Array of dividend events with:
      - ticker: Ticker symbol of the dividend
      - cash_amount: Cash amount of the dividend per share owned
      - currency: Currency in which the dividend is paid
      - declaration_date: Date dividend was announced by the company
      - ex_dividend_date: Date stock first trades without dividend (set by exchange)
      - record_date: Date stock must be held to receive dividend (set by company)
      - pay_date: Date dividend is paid out to shareholders
      - frequency: Number of times per year dividend is paid (0, 1, 2, 4, 12, 24, 52)
      - dividend_type: Type of dividend (CD, SC, LT, ST)
      - id: Unique identifier for this dividend event
    - next_url: If present, use to fetch the next page of data
    - request_id: Server-assigned request identifier
    - status: Response status (e.g., "OK")

    Understanding Dividend Dates:
    1. Declaration Date: Company announces the dividend
    2. Ex-Dividend Date: Stock trades "ex-dividend" (without dividend)
       - Must own shares BEFORE this date to receive dividend
       - Stock price typically drops by dividend amount on this date
    3. Record Date: Ownership recorded (usually 2 business days after ex-date)
    4. Pay Date: Dividend payment distributed to shareholders

    Date Timeline Example:
    - Declaration: Oct 28, 2021 (company announces $0.22 dividend)
    - Ex-Dividend: Nov 5, 2021 (must buy before Nov 5 to get dividend)
    - Record: Nov 8, 2021 (ownership recorded)
    - Pay: Nov 11, 2021 (dividend paid out)

    Use Cases:
    - Income analysis: Calculate dividend income for portfolio holdings
    - Total return calculations: Include dividends in investment return metrics
    - Dividend strategies: Screen for dividend growth, yield, consistency
    - Tax planning: Organize dividend income for tax reporting and qualified dividend treatment

    Example: list_dividends(ticker="AAPL") returns all historical Apple dividends with
             payment details, showing consistent quarterly CD dividends

    Example: list_dividends(frequency=4, cash_amount_gte=1.0, limit=100) returns up to
             100 quarterly dividends with payout of $1 or more per share

    Example: list_dividends(ticker="MSFT", ex_dividend_date_gte="2023-01-01",
             ex_dividend_date_lte="2023-12-31") returns Microsoft dividends with
             ex-dates in 2023 for annual tax reporting

    Example: list_dividends(dividend_type="SC", limit=50) returns up to 50 special cash
             dividends that are unusual or one-time events

    Example: list_dividends(ticker_gte="A", ticker_lt="B", frequency=4) returns
             quarterly dividends for all tickers starting with 'A'

    Note: Dividend considerations and best practices:
    - Dividend Types:
      * CD (Cash Dividend) - Regular, recurring dividends paid on consistent schedules
        - Most common type for established companies
        - Expected to continue in future based on historical pattern
      * SC (Special Cash) - One-time or irregular special dividends
        - Infrequent, unusual distributions
        - Cannot be expected to recur
        - Often from asset sales, windfalls, or excess cash
      * LT/ST (Capital Gains) - For funds/REITs distributing capital gains
        - LT = Long-term capital gain distributions (held > 1 year)
        - ST = Short-term capital gain distributions (held ≤ 1 year)

    - Frequency Analysis:
      * 0 (one-time) - Special dividends, non-recurring
      * 1 (annually) - Once per year, common for small-cap or international stocks
      * 4 (quarterly) - Most common for U.S. stocks, paid every 3 months
      * 12 (monthly) - REITs, income-focused stocks
      * Higher frequencies (24, 52) - Rare, specialized securities

    - Important Date Rules:
      * Ex-dividend date is T+1 (trade date + 1 business day) before record date
      * To receive dividend: Must own shares by market close BEFORE ex-dividend date
      * Stock price adjustment: Price drops by ~dividend amount on ex-dividend date
      * Dividend capture strategy: Buy before ex-date, sell after (risky due to price drop)

    - Dividend Yield Calculation:
      * Annual Dividend Yield = (Annual Dividend Per Share / Stock Price) × 100%
      * If frequency=4 and cash_amount=$0.25: Annual dividend = $0.25 × 4 = $1.00
      * If stock price = $50: Yield = ($1.00 / $50) × 100% = 2.0%

    - Tax Implications:
      * Qualified Dividends: Lower tax rate (0%, 15%, or 20%) if held > 60 days
      * Ordinary Dividends: Taxed at income tax rates
      * Pay_date typically determines which tax year dividend counts for
      * Track ex_dividend_date for holding period requirements

    - Dividend Screening Strategies:
      * Dividend Growth: Compare current vs. historical cash_amount
      * High Yield: Filter by cash_amount_gte for minimum payout
      * Consistency: Check frequency=4 for regular quarterly payers
      * Special Opportunities: Filter dividend_type="SC" for unusual payouts

    - Portfolio Analysis:
      * Use ex_dividend_date ranges to get annual dividend income
      * Calculate total return = price appreciation + dividend income
      * Track pay_dates for cash flow planning
      * Monitor frequency changes (quarterly → annual may signal trouble)

    - Data Quality:
      * Declaration_date to pay_date typically spans 2-4 weeks
      * Ex_dividend_date usually 2 business days before record_date (T+1 settlement)
      * Currency field shows payout currency (USD for U.S. stocks)
      * Historical data enables dividend growth rate calculations

    - Integration with other data:
      * Combine with stock price to calculate dividend yield
      * Compare with earnings to determine payout ratio
      * Use with split data for accurate historical dividend adjustments
      * Aggregate by year for tax reporting

    - Common Filtering Patterns:
      * Annual income: ex_dividend_date_gte="2024-01-01", ex_dividend_date_lte="2024-12-31"
      * High payers: cash_amount_gte=1.0, frequency=4 (quarterly $1+ dividends)
      * Special dividends: dividend_type="SC" (one-time bonuses)
      * Ticker scan: ticker_gte="A", ticker_lt="B" (alphabetical range)
      * Recent payments: pay_date_gte="2024-01-01", sort="pay_date", order="desc"
    """
    try:
        results = polygon_client.list_dividends(
            ticker=ticker,
            ex_dividend_date=ex_dividend_date,
            record_date=record_date,
            declaration_date=declaration_date,
            pay_date=pay_date,
            frequency=frequency,
            cash_amount=cash_amount,
            dividend_type=dividend_type,
            limit=limit,
            sort=sort,
            order=order,
            params={
                **(params or {}),
                **{
                    k: v
                    for k, v in {
                        "ticker.gte": ticker_gte,
                        "ticker.gt": ticker_gt,
                        "ticker.lte": ticker_lte,
                        "ticker.lt": ticker_lt,
                        "ex_dividend_date.gte": ex_dividend_date_gte,
                        "ex_dividend_date.gt": ex_dividend_date_gt,
                        "ex_dividend_date.lte": ex_dividend_date_lte,
                        "ex_dividend_date.lt": ex_dividend_date_lt,
                        "record_date.gte": record_date_gte,
                        "record_date.gt": record_date_gt,
                        "record_date.lte": record_date_lte,
                        "record_date.lt": record_date_lt,
                        "declaration_date.gte": declaration_date_gte,
                        "declaration_date.gt": declaration_date_gt,
                        "declaration_date.lte": declaration_date_lte,
                        "declaration_date.lt": declaration_date_lt,
                        "pay_date.gte": pay_date_gte,
                        "pay_date.gt": pay_date_gt,
                        "pay_date.lte": pay_date_lte,
                        "pay_date.lt": pay_date_lt,
                        "cash_amount.gte": cash_amount_gte,
                        "cash_amount.gt": cash_amount_gt,
                        "cash_amount.lte": cash_amount_lte,
                        "cash_amount.lt": cash_amount_lt,
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
            tool_name="list_dividends",
            params={
                "ticker": ticker,
                "limit": limit,
            },
            csv_data=csv_data,
        )
    except Exception as e:
        return f"Error: {e}"





@poly_mcp.tool(annotations=ToolAnnotations(readOnlyHint=True))
async def get_ticker_events(
    ticker: str,
    types: Optional[str] = None,
    params: Optional[Dict[str, Any]] = None,
) -> str:
    """
    Retrieve a unified, paginated timeline of corporate events for a ticker, including dividends,
    stock splits, and earnings, with optional event type filtering.

    This endpoint provides a comprehensive, chronological view of all significant corporate actions
    and events affecting a security in a single call, making it ideal for analyzing the complete
    event history and understanding the context of corporate actions.

    Official API Documentation:
    https://polygon.io/docs/rest/stocks/corporate-actions/ticker-events

    Parameters
    ----------
    ticker : str, required
        The ticker symbol to retrieve events for.
        Examples: "AAPL", "MSFT", "TSLA"

    types : str, optional
        Filter by specific event types. Accepts comma-separated values.
        Available types:
        - "dividend": Cash dividend distributions
        - "split": Stock split events
        - "earnings": Earnings releases
        Default: Returns all event types if not specified
        Examples:
        - "dividend" → Only dividend events
        - "dividend,split" → Dividends and splits only
        - "earnings" → Only earnings events

    params : dict, optional
        Additional query parameters for advanced filtering and pagination.
        Common parameters:
        - limit (int): Maximum number of results (default: 10, max: varies by plan)
        - sort (str): Sort field and direction (e.g., "event_date", "event_date.desc")
        - order (str): "asc" or "desc" for result ordering

    Returns
    -------
    str
        CSV-formatted string containing the unified event timeline with columns varying by event type.

        Common fields for all events:
        - ticker: The ticker symbol
        - name: Company name
        - event_type: Type of event (dividend, split, earnings)
        - event_date: Date of the event

        Dividend-specific fields:
        - cash_amount: Dividend amount per share
        - declaration_date: Date dividend was announced
        - ex_dividend_date: First date trading without dividend
        - record_date: Date to be on record for dividend
        - pay_date: Date dividend is paid
        - frequency: Payment frequency (0=one-time, 1=annual, 4=quarterly, 12=monthly)
        - dividend_type: Type of dividend (CD=cash, SC=stock, LT=long-term capital gains, ST=short-term capital gains)

        Split-specific fields:
        - execution_date: Date the split takes effect
        - split_from: Original share count in ratio
        - split_to: New share count in ratio

        Earnings-specific fields:
        - fiscal_period: Reporting period (Q1, Q2, Q3, Q4, FY)
        - fiscal_year: Fiscal year

    Understanding the Unified Timeline
    -----------------------------------
    This endpoint aggregates events from multiple sources into a single chronological view:

    1. **Event Types**:
       - Dividends: Track income distributions and payment schedules
       - Splits: Monitor share structure changes and price adjustments
       - Earnings: Follow quarterly and annual financial reporting

    2. **Timeline Benefits**:
       - Single API call for complete corporate action history
       - Chronological ordering reveals event patterns and sequences
       - Simplified correlation between different event types
       - Comprehensive view for fundamental analysis

    3. **Event Filtering**:
       - Filter by single type for focused analysis
       - Combine multiple types to see relationships (e.g., dividends + splits)
       - Omit types parameter for complete event history

    Use Cases
    ---------
    1. **Corporate Action Analysis**: Review complete history of dividends, splits, and earnings
       to understand company actions and shareholder value creation patterns.

    2. **Event Impact Studies**: Correlate different event types (e.g., dividend increases after
       strong earnings) to identify patterns in corporate decision-making.

    3. **Timeline Construction**: Build comprehensive timelines for research reports, presentations,
       or investor relations materials showing all major company milestones.

    4. **Automated Monitoring**: Track upcoming and historical events for portfolio holdings to
       stay informed about corporate actions affecting investments.

    Examples
    --------
    Example 1: Get all events for Apple
        get_ticker_events(ticker="AAPL")

        Returns comprehensive timeline including all dividends, splits, and earnings for AAPL,
        sorted chronologically.

    Example 2: Get only dividend and split events for Microsoft
        get_ticker_events(
            ticker="MSFT",
            types="dividend,split"
        )

        Returns only cash dividend distributions and stock split events, excluding earnings.
        Useful for analyzing shareholder return actions.

    Example 3: Get recent earnings events for Tesla
        get_ticker_events(
            ticker="TSLA",
            types="earnings"
        )

        Returns earnings release events only, showing fiscal periods and reporting dates.
        Ideal for tracking quarterly and annual financial reporting schedule.

    Example 4: Get limited number of recent events with custom sorting
        get_ticker_events(
            ticker="GOOGL",
            params={
                "limit": 20,
                "sort": "event_date.desc",
                "order": "desc"
            }
        )

        Returns 20 most recent events in descending chronological order, showing latest
        corporate actions first.

    Example 5: Analyze dividend history specifically
        get_ticker_events(
            ticker="KO",
            types="dividend",
            params={"limit": 50}
        )

        Returns up to 50 dividend events for Coca-Cola, useful for analyzing dividend
        growth, consistency, and payment patterns over time.

    Notes
    -----
    - **Unified View**: This endpoint consolidates data from multiple specialized endpoints
      (list_dividends, list_splits, earnings) into a single timeline, simplifying analysis
      of corporate action patterns and sequences.

    - **Event Type Filtering**: The types parameter accepts single values ("dividend") or
      comma-separated combinations ("dividend,split,earnings"). Omitting types returns all
      event types.

    - **Chronological Ordering**: Events are returned in chronological order by default,
      making it easy to see the sequence and timing of corporate actions. Use sort and
      order parameters for custom ordering.

    - **Variable Response Structure**: Response fields vary depending on event types returned.
      Dividend events include payment dates and amounts, splits include ratios, and earnings
      include fiscal periods.

    - **Pagination**: Results are paginated. Use limit parameter to control page size and
      next_url field in response metadata for retrieving subsequent pages of large result sets.

    - **Event Dates**: Different event types use different date fields (ex_dividend_date for
      dividends, execution_date for splits, event_date for earnings). When analyzing timelines,
      pay attention to which date field is relevant for each event type.

    - **Corporate Action Correlation**: This endpoint is particularly valuable for correlating
      different types of corporate actions. For example, analyzing whether dividend increases
      follow strong earnings, or whether splits occur during specific market conditions.

    - **Historical Analysis**: Combine this endpoint with price and volume data to study the
      market impact of different corporate actions and events over time.

    - **Data Completeness**: Event availability depends on data source coverage. Recent events
      are most complete; historical data coverage may vary by ticker and event type.

    - **Comparison with Specialized Endpoints**: While this endpoint provides a unified timeline,
      specialized endpoints (list_dividends, list_splits) offer more detailed filtering and
      search capabilities. Use this endpoint for timeline views and specialized endpoints for
      detailed queries.
    """
    try:
        results = polygon_client.get_ticker_events(
            ticker=ticker,
            types=types,
            params=params,
            raw=True,
        )

        # Parse the response and extract the events array
        import json

        data = json.loads(results.data.decode("utf-8"))
        if "results" in data and "events" in data["results"]:
            # Wrap the events in a results key for consistent CSV formatting
            formatted_data = {"results": data["results"]["events"]}
            return json_to_csv(formatted_data)
        return json_to_csv(results.data.decode("utf-8"))
    except Exception as e:
        return f"Error: {e}"





@poly_mcp.tool(annotations=ToolAnnotations(readOnlyHint=True))
async def list_ipos(
    ticker: Optional[str] = None,
    us_code: Optional[str] = None,
    isin: Optional[str] = None,
    listing_date: Optional[Union[str, datetime, date]] = None,
    listing_date_lt: Optional[Union[str, datetime, date]] = None,
    listing_date_lte: Optional[Union[str, datetime, date]] = None,
    listing_date_gt: Optional[Union[str, datetime, date]] = None,
    listing_date_gte: Optional[Union[str, datetime, date]] = None,
    ipo_status: Optional[str] = None,
    limit: Optional[int] = 10,
    sort: Optional[str] = None,
    order: Optional[str] = None,
    params: Optional[Dict[str, Any]] = None,
) -> str:
    """
    Retrieve comprehensive information on Initial Public Offerings (IPOs), including upcoming and
    historical events, starting from the year 2008. This endpoint provides key details such as issuer
    name, ticker symbol, security type, IPO date, number of shares offered, expected price ranges,
    final issue prices, and offering sizes. Users can filter results by IPO status (e.g., pending,
    new, rumors, historical) to target their research and inform investment decisions.

    Reference: https://polygon.io/docs/rest/stocks/corporate-actions/ipos

    Parameters:
    - ticker: Case-sensitive ticker symbol (e.g., "TSLA" for Tesla Inc.)
    - us_code: Unique nine-character alphanumeric code that identifies a North American financial
               security for facilitating clearing and settlement of trades
    - isin: International Securities Identification Number - unique twelve-digit code assigned to
            every security issuance in the world
    - listing_date: Specific listing date (YYYY-MM-DD) - the first trading date for the newly listed entity
    - listing_date_gte: Range by listing_date - greater than or equal to
    - listing_date_gt: Range by listing_date - greater than
    - listing_date_lte: Range by listing_date - less than or equal to
    - listing_date_lt: Range by listing_date - less than
    - ipo_status: Filter by IPO status:
      * "rumor" - Early stage, unconfirmed reports of potential IPO
      * "pending" - Confirmed IPO scheduled for future date
      * "new" - Listed today (on listing day)
      * "history" - Historical IPOs (after listing day)
      * "direct_listing_process" - Direct listing without traditional IPO process
      * "postponed" - IPO delayed to future date
      * "withdrawn" - IPO cancelled
    - limit: Limit the number of results returned (default: 10, max: 1000)
    - sort: Sort field used for ordering (e.g., "listing_date")
    - order: Order results based on the sort field ("asc" or "desc")

    Response includes:
    - results[]: Array of IPO events with detailed information:
      - ticker: Ticker symbol of the IPO event
      - issuer_name: Name of the company issuing shares
      - listing_date: First trading date for the newly listed entity
      - announced_date: Date when the IPO event was announced
      - last_updated: Date when the IPO event was last modified
      - ipo_status: Current status (rumor, pending, new, history, direct_listing_process, etc.)

      Pricing information:
      - final_issue_price: Price set by company and underwriters before IPO goes live
      - lowest_offer_price: Lowest price within the IPO price range
      - highest_offer_price: Highest price within the IPO price range

      Shares information:
      - min_shares_offered: Lower limit of shares company is willing to sell
      - max_shares_offered: Upper limit of shares company is offering
      - shares_outstanding: Total shares issued and held by investors after IPO
      - lot_size: Minimum number of shares that can be bought/sold in single transaction

      Security details:
      - security_type: Classification of the stock (e.g., "CS" for Common Stock)
      - security_description: Description of the security (e.g., "Ordinary Shares")
      - isin: International Securities Identification Number
      - us_code: Nine-character alphanumeric security identifier
      - currency_code: Underlying currency of the security

      Market information:
      - primary_exchange: Market Identifier Code (MIC) of the primary exchange (e.g., "XNAS" for NASDAQ)
      - total_offer_size: Total amount raised by the company for IPO
      - issue_start_date: Start date of the IPO offering period
      - issue_end_date: End date of the IPO offering period

    - next_url: If present, use to fetch the next page of data
    - request_id: Server-assigned request identifier
    - status: Response status (e.g., "OK")

    IPO Status Lifecycle:
    1. "rumor" or "pending" - Initial stages before listing
    2. "new" - On the listing day
    3. "history" - After the listing day

    Special status "direct_listing_process" indicates a company listing shares directly on an
    exchange without using investment banks or intermediaries (also called DPO - Direct Public Offering).

    Use Cases:
    - IPO research: Discover upcoming investment opportunities and evaluate IPO characteristics
    - Market trend analysis: Analyze IPO activity patterns across sectors and time periods
    - Investment screening: Filter IPOs by price range, offering size, or listing date
    - Historical event comparison: Study past IPOs to understand pricing and performance patterns

    Example: list_ipos() returns upcoming IPOs with default parameters showing recent and
             pending listings

    Example: list_ipos(ipo_status="pending", limit=50) returns up to 50 confirmed upcoming
             IPOs that are scheduled but haven't listed yet

    Example: list_ipos(listing_date_gte="2024-01-01", listing_date_lte="2024-12-31")
             returns all IPOs that listed during 2024 for year-end analysis

    Example: list_ipos(ticker="RAPP") returns detailed IPO information for Rapport Therapeutics
             including pricing, shares offered, and listing details

    Example: list_ipos(ipo_status="history", sort="listing_date", order="desc", limit=20)
             returns the 20 most recent historical IPOs sorted by listing date

    Note: IPO considerations and best practices:
    - Data available starting from 2008 - comprehensive historical coverage
    - IPO status progression: rumor → pending → new → history
    - Price range (lowest_offer_price to highest_offer_price) shows company's expected valuation
    - Final_issue_price is the actual IPO price set before going live
    - Total_offer_size = final_issue_price × shares offered (approximately)
    - Direct listings (direct_listing_process status) skip traditional IPO process:
      * No underwriters or investment banks involved
      * No new shares issued (existing shares sold by insiders)
      * No lock-up period for existing shareholders
      * Examples: Spotify (2018), Slack (2019), Coinbase (2021)
    - Lot_size indicates minimum purchase quantity (typically 100 shares)
    - Primary_exchange codes (MIC):
      * XNAS = NASDAQ
      * XNYS = NYSE
      * Other MIC codes for different exchanges
    - Monitor pending IPOs for upcoming investment opportunities
    - Compare IPO characteristics: pricing, offering size, shares outstanding
    - Track postponed or withdrawn IPOs to understand market conditions
    - Use listing_date ranges to analyze IPO activity during specific periods
    - Filter by ipo_status to focus on relevant stage (rumors, pending, or historical)
    - Historical IPOs useful for studying performance patterns and pricing accuracy
    """
    try:
        results = polygon_client.vx.list_ipos(
            ticker=ticker,
            us_code=us_code,
            isin=isin,
            listing_date=listing_date,
            listing_date_lt=listing_date_lt,
            listing_date_lte=listing_date_lte,
            listing_date_gt=listing_date_gt,
            listing_date_gte=listing_date_gte,
            ipo_status=ipo_status,
            limit=limit,
            sort=sort,
            order=order,
            params=params,
            raw=True,
        )

        return json_to_csv(results.data.decode("utf-8"))
    except Exception as e:
        return f"Error: {e}"
