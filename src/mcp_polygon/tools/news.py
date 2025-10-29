"""Auto-generated tool definitions."""

from typing import Optional, Any, Dict, Union
from mcp.types import ToolAnnotations
from datetime import datetime, date
from ..clients import poly_mcp, polygon_client
from ..formatters import json_to_csv
from ..tool_integration import process_tool_response


@poly_mcp.tool(annotations=ToolAnnotations(readOnlyHint=True))
async def list_ticker_news(
    ticker: Optional[str] = None,
    published_utc: Optional[Union[str, datetime, date]] = None,
    ticker_gte: Optional[str] = None,
    ticker_gt: Optional[str] = None,
    ticker_lte: Optional[str] = None,
    ticker_lt: Optional[str] = None,
    published_utc_gte: Optional[Union[str, datetime, date]] = None,
    published_utc_gt: Optional[Union[str, datetime, date]] = None,
    published_utc_lte: Optional[Union[str, datetime, date]] = None,
    published_utc_lt: Optional[Union[str, datetime, date]] = None,
    limit: Optional[int] = 10,
    fetch_all: Optional[bool] = True,
    sort: Optional[str] = None,
    order: Optional[str] = None,
    params: Optional[Dict[str, Any]] = None,
) -> str:
    """
    Get news articles for a specific ticker with sentiment analysis and full content.

    Reference: https://polygon.io/docs/rest/stocks/reference/news

    Parameters:
    - ticker: Stock symbol (e.g., "AAPL", "TSLA")
    - published_utc_gte: Filter articles published on or after this date (YYYY-MM-DD)
    - limit: Number of results per page (default: 10, max: 1000)
    - fetch_all: If True (recommended), fetch ALL data and cache to disk for DuckDB queries (default: True)

    RECOMMENDED: Always use fetch_all=True to cache complete news data locally for efficient DuckDB analysis.

    Example: list_ticker_news(ticker="AAPL", fetch_all=True)
    Example: list_ticker_news(ticker="TSLA", published_utc_gte="2024-01-01", fetch_all=True)

    Returns: title, author, published_utc, article_url, description, keywords,
    sentiment (label, score), insights (ticker, sentiment, reasoning), amp_url, image_url

    Note: Articles include full content with sentiment analysis and AI-generated insights.
    """
    try:
        news_list = []

        param_dict = {
            **(params or {}),
            **{
                k: v
                for k, v in {
                    "ticker.gte": ticker_gte,
                    "ticker.gt": ticker_gt,
                    "ticker.lte": ticker_lte,
                    "ticker.lt": ticker_lt,
                    "published_utc.gte": published_utc_gte,
                    "published_utc.gt": published_utc_gt,
                    "published_utc.lte": published_utc_lte,
                    "published_utc.lt": published_utc_lt,
                }.items()
                if v is not None
            },
        }

        if fetch_all:
            # Use iterator approach for automatic pagination
            for news_item in polygon_client.list_ticker_news(
                ticker=ticker,
                published_utc=published_utc,
                limit=limit,
                sort=sort,
                order=order,
                params=param_dict,
                raw=False,
            ):
                # Convert TickerNews object to dict
                news_list.append(news_item.to_dict())
        else:
            # Single page approach
            results = polygon_client.list_ticker_news(
                ticker=ticker,
                published_utc=published_utc,
                limit=limit,
                sort=sort,
                order=order,
                params=param_dict,
                raw=True,
            )

            import json
            data = json.loads(results.data.decode("utf-8"))
            news_list = data.get("results", [])

        # Create data structure for JSON to CSV conversion
        data = {"results": news_list, "status": "OK"}

        # Convert to CSV
        csv_data = json_to_csv(data)

        # Process with intelligent caching
        return await process_tool_response(
            tool_name="list_ticker_news",
            params={
                "ticker": ticker,
                "published_utc_gte": str(published_utc_gte)
                if published_utc_gte
                else None,
                "limit": limit,
                "fetch_all": fetch_all,
            },
            csv_data=csv_data,
        )
    except Exception as e:
        return f"Error: {e}"
