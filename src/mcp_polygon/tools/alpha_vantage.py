"""Alpha Vantage API tools."""

from typing import Optional
from mcp.types import ToolAnnotations
from ..clients import poly_mcp
import requests
from pathlib import Path
from datetime import datetime


@poly_mcp.tool(annotations=ToolAnnotations(readOnlyHint=True))
async def get_earnings_calendar_alpha_vantage(
    alpha_vantage_api_key: str,
    horizon: str = "3month",
    symbol: Optional[str] = None,
) -> str:
    """
    Retrieve earnings calendar from Alpha Vantage API with analyst estimates.

    This endpoint provides earnings forecasts for up to 12 months, including analyst
    EPS estimates and reporting dates. Data is returned in CSV format.

    **IMPORTANT**: You must provide your Alpha Vantage API key to use this tool.
    Get a free key at: https://www.alphavantage.co/support/#api-key

    Parameters
    ----------
    alpha_vantage_api_key : str, required
        Your Alpha Vantage API key. This is REQUIRED for the tool to work.
        Get a free key at: https://www.alphavantage.co/support/#api-key

    horizon : str, optional
        Time horizon for earnings calendar (default: "3month")
        Options: "3month", "6month", "12month"

    symbol : str, optional
        Filter by specific ticker symbol (e.g., "AAPL", "MSFT")
        If not provided, returns earnings for all stocks

    Returns
    -------
    str
        CSV-formatted string containing earnings calendar data with columns:
        - symbol: Stock ticker symbol
        - name: Company name
        - reportDate: Expected earnings report date
        - fiscalDateEnding: Fiscal period end date
        - estimate: Analyst consensus EPS estimate
        - currency: Currency of the estimate

        The data is also saved to: mcp_polygon/cache/earnings/earnings_{horizon}_{symbol}_{timestamp}.csv

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
    - Data is automatically cached to mcp_polygon/cache/earnings/ directory with timestamp

    API Reference
    -------------
    https://www.alphavantage.co/documentation/#earnings-calendar
    """
    try:
        # Build API request
        url = "https://www.alphavantage.co/query"
        params = {
            "function": "EARNINGS_CALENDAR",
            "horizon": horizon,
            "apikey": alpha_vantage_api_key,
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

        # Create cache directory if it doesn't exist
        cache_dir = Path("mcp_polygon/cache/earnings")
        cache_dir.mkdir(parents=True, exist_ok=True)

        # Generate filename with timestamp and parameters
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        symbol_suffix = f"_{symbol}" if symbol else "_all"
        filename = f"earnings_{horizon}{symbol_suffix}_{timestamp}.csv"
        filepath = cache_dir / filename

        # Save CSV data to file
        filepath.write_text(response.text)

        # Return CSV data with file location info
        return f"Data saved to: {filepath}\n\n{response.text}"

    except Exception as e:
        return f"Error fetching Alpha Vantage earnings calendar: {e}"
