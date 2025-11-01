"""Utility functions for MCP Polygon tools."""

from typing import Any, Dict


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
