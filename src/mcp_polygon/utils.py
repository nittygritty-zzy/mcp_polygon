"""Utility functions for MCP Polygon tools."""

import asyncio
from typing import Any, Dict
from functools import wraps


def build_params(**kwargs) -> Dict[str, Any]:
    """
    Build a parameters dictionary excluding None values.

    This ensures that cache partition keys use proper defaults instead of
    encountering None values that would cause path construction errors.

    Args:
        **kwargs: Key-value pairs to include in params

    Returns:
        Dictionary with only non-None values

    Example:
        >>> build_params(ticker="AAPL", limit=10, timeframe=None)
        {'ticker': 'AAPL', 'limit': 10}
    """
    return {k: v for k, v in kwargs.items() if v is not None}


def handle_cancellation(func):
    """
    Decorator to ensure asyncio.CancelledError propagates immediately.

    When a tool function is interrupted (e.g., user cancels during fetch_all),
    this decorator ensures the cancellation is properly propagated instead of
    being caught by generic Exception handlers.

    Usage:
        @handle_cancellation
        async def my_tool(...):
            try:
                # tool logic
            except Exception as e:
                return f"Error: {e}"
    """

    @wraps(func)
    async def wrapper(*args, **kwargs):
        try:
            return await func(*args, **kwargs)
        except asyncio.CancelledError:
            # Re-raise immediately to stop execution
            raise
        except Exception:
            # Let the original function handle other exceptions
            raise

    return wrapper
