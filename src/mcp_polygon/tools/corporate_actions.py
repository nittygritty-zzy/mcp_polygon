"""Auto-generated tool definitions."""

from typing import Optional, Any, Dict, Union
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
    Get historical stock split events with execution dates and ratio factors.

    Reference: https://polygon.io/docs/rest/stocks/corporate-actions/splits

    Parameters:
    - ticker: Stock symbol (e.g., "AAPL", "TSLA")
    - execution_date: Split execution date (YYYY-MM-DD)
    - reverse_split: Filter by type (True=reverse splits, False=forward splits)
    - execution_date_gte/lte: Date range filters
    - limit: Number of results (default: 10, max: 1000)

    Example: list_splits(ticker="AAPL")
    Example: list_splits(reverse_split=True, limit=50)

    Returns: ticker, execution_date, split_to, split_from. Ratio: split_to-for-split_from (e.g., 2-for-1).
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
    Get historical dividend distributions with declaration, ex-dividend, record, and pay dates.

    Reference: https://polygon.io/docs/rest/stocks/corporate-actions/dividends

    Parameters:
    - ticker: Stock symbol (e.g., "AAPL", "MSFT")
    - ex_dividend_date: Ex-dividend date (YYYY-MM-DD)
    - frequency: Payment frequency (0=one-time, 1=annual, 4=quarterly, 12=monthly)
    - dividend_type: Type ("CD"=cash, "SC"=special, "LT"/"ST"=capital gains)
    - cash_amount_gte/lte: Filter by dividend amount
    - ex_dividend_date_gte/lte: Date range filters
    - limit: Number of results (default: 10, max: 1000)

    Example: list_dividends(ticker="AAPL")
    Example: list_dividends(frequency=4, cash_amount_gte=1.0, limit=100)

    Returns: ticker, cash_amount, currency, declaration_date, ex_dividend_date, record_date, pay_date, frequency, dividend_type.
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
    Get unified timeline of corporate events (dividends, splits, earnings) for a ticker.

    Reference: https://polygon.io/docs/rest/stocks/corporate-actions/ticker-events

    Parameters:
    - ticker: Stock symbol (e.g., "AAPL", "MSFT")
    - types: Filter event types (comma-separated: "dividend", "split", "earnings")
    - params: Additional filters (limit, sort, order)

    Example: get_ticker_events(ticker="AAPL")
    Example: get_ticker_events(ticker="MSFT", types="dividend,split")

    Returns: ticker, event_type, event_date, plus type-specific fields (cash_amount, split_to/from, fiscal_period).
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
    Get IPO information including upcoming and historical events (data from 2008+).

    Reference: https://polygon.io/docs/rest/stocks/corporate-actions/ipos

    Parameters:
    - ticker: Stock symbol (e.g., "TSLA")
    - listing_date: IPO listing date (YYYY-MM-DD)
    - listing_date_gte/lte: Date range filters
    - ipo_status: Status filter ("rumor", "pending", "new", "history", "direct_listing_process")
    - limit: Number of results (default: 10, max: 1000)

    Example: list_ipos(ipo_status="pending", limit=50)
    Example: list_ipos(listing_date_gte="2024-01-01", listing_date_lte="2024-12-31")

    Returns: ticker, issuer_name, listing_date, ipo_status, final_issue_price, offer prices, shares, total_offer_size.
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
