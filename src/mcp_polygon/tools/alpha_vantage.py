"""Alpha Vantage API tools."""

from typing import Optional
from mcp.types import ToolAnnotations
from ..clients import poly_mcp
from ..tool_integration import process_tool_response
import requests
import os


@poly_mcp.tool(annotations=ToolAnnotations(readOnlyHint=True))
async def get_earnings_calendar_alpha_vantage(
    alpha_vantage_api_key: Optional[str] = None,
    horizon: str = "3month",
    symbol: Optional[str] = None,
) -> str:
    """
    Get earnings calendar from Alpha Vantage with analyst EPS estimates (3-12 month forecasts).

    Reference: https://www.alphavantage.co/documentation/#earnings-calendar

    **Requires Alpha Vantage API key**: https://www.alphavantage.co/support/#api-key

    Parameters:
    - alpha_vantage_api_key: Your API key (or set ALPHA_VANTAGE_API_KEY env var)
    - horizon: Time period ("3month", "6month", "12month")
    - symbol: Filter by ticker (e.g., "AAPL")

    Example: get_earnings_calendar_alpha_vantage(alpha_vantage_api_key="YOUR_KEY", horizon="3month")
    Example: get_earnings_calendar_alpha_vantage(alpha_vantage_api_key="YOUR_KEY", symbol="AAPL")

    Returns: symbol, name, reportDate, fiscalDateEnding, estimate (analyst EPS), currency. Free tier: 25 requests/day.
    """
    try:
        # Get API key from parameter or environment variable
        api_key = alpha_vantage_api_key or os.getenv("ALPHA_VANTAGE_API_KEY")

        if not api_key:
            return (
                "Error: Alpha Vantage API key is required. "
                "Either pass alpha_vantage_api_key parameter or set ALPHA_VANTAGE_API_KEY environment variable. "
                "Get a free key at: https://www.alphavantage.co/support/#api-key"
            )

        # Build API request
        url = "https://www.alphavantage.co/query"
        params = {
            "function": "EARNINGS_CALENDAR",
            "horizon": horizon,
            "apikey": api_key,
        }

        # Add optional symbol filter
        if symbol:
            params["symbol"] = symbol

        # Make request
        response = requests.get(url, params=params)

        # Check for API errors
        if response.status_code != 200:
            return f"Error: HTTP {response.status_code} - {response.text[:200]}"

        # Check if response is JSON error message
        if response.text.startswith("{"):
            return f"Error: {response.text}"

        # Get CSV data
        csv_data = response.text

        # Process with intelligent caching (saves as Parquet, enables DuckDB queries)
        return await process_tool_response(
            tool_name="get_earnings_calendar_alpha_vantage",
            params={
                "horizon": horizon,
                "symbol": symbol,
            },
            csv_data=csv_data,
        )

    except Exception as e:
        return f"Error fetching Alpha Vantage earnings calendar: {e}"
