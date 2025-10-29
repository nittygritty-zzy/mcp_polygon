"""Auto-generated tool definitions."""

from typing import Optional, Any, Dict
from mcp.types import ToolAnnotations
from ..clients import poly_mcp, polygon_client
from ..formatters import json_to_csv


@poly_mcp.tool(annotations=ToolAnnotations(readOnlyHint=True))
async def get_real_time_currency_conversion(
    from_: str,
    to: str,
    amount: Optional[float] = None,
    precision: Optional[int] = None,
    params: Optional[Dict[str, Any]] = None,
) -> str:
    """
    Get real-time currency conversion between forex pairs.

    Parameters:
    - from_: Source currency (e.g., "USD", "EUR")
    - to: Target currency (e.g., "JPY", "GBP")
    - amount: Amount to convert (optional)
    - precision: Decimal precision (optional)

    Example: get_real_time_currency_conversion(from_="USD", to="EUR", amount=100)

    Returns: Converted amount and exchange rate.
    """
    try:
        results = polygon_client.get_real_time_currency_conversion(
            from_=from_,
            to=to,
            amount=amount,
            precision=precision,
            params=params,
            raw=True,
        )

        return json_to_csv(results.data.decode("utf-8"))
    except Exception as e:
        return f"Error: {e}"
