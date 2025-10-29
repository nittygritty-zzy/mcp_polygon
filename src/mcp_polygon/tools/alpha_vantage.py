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
    Retrieve earnings calendar from Alpha Vantage API with analyst estimates.

    This endpoint provides earnings forecasts for up to 12 months, including analyst
    EPS estimates and reporting dates. Data is returned in CSV format.

    **IMPORTANT**: This tool requires an Alpha Vantage API key.
    Get a free key at: https://www.alphavantage.co/support/#api-key

    Parameters
    ----------
    alpha_vantage_api_key : str, optional
        Your Alpha Vantage API key. If not provided, will use ALPHA_VANTAGE_API_KEY
        environment variable. Get a free key at: https://www.alphavantage.co/support/#api-key

    horizon : str, optional
        Time horizon for earnings calendar (default: "3month")
        Options: "3month", "6month", "12month"

    symbol : str, optional
        Filter by specific ticker symbol (e.g., "AAPL", "MSFT")
        If not provided, returns earnings for all stocks

    Returns
    -------
    str
        CSV data containing earnings calendar with the following columns:
        - symbol: Stock ticker symbol
        - name: Company name
        - reportDate: Expected earnings report date
        - fiscalDateEnding: Fiscal period end date
        - estimate: Analyst consensus EPS estimate
        - currency: Currency of the estimate

    Examples
    --------
    Example 1: Get 3-month earnings calendar for all stocks
        get_earnings_calendar_alpha_vantage(
            alpha_vantage_api_key="YOUR_API_KEY",
            horizon="3month"
        )

    Example 2: Get earnings calendar for specific stock
        get_earnings_calendar_alpha_vantage(
            alpha_vantage_api_key="YOUR_API_KEY",
            symbol="AAPL"
        )

    Example 3: Get 12-month earnings forecast
        get_earnings_calendar_alpha_vantage(
            alpha_vantage_api_key="YOUR_API_KEY",
            horizon="12month"
        )

    Notes
    -----
    - Free API tier: 25 requests per day, 5 per minute
    - Response is already in CSV format (no conversion needed)
    - Includes analyst EPS estimates (not available in Polygon earnings calendar)
    - Data covers 3-12 month forecasts vs. Polygon's historical data
    - No rate limits shown in response, monitor your usage externally
    - Data is cached as Parquet and queryable with DuckDB via query_cached_data tool

    API Reference
    -------------
    https://www.alphavantage.co/documentation/#earnings-calendar
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
