"""
Stock screeners for identifying trading opportunities.

This module provides MCP tools for screening stocks based on various criteria:
- Short squeeze opportunities
- Contrarian entry points
- Earnings short setup patterns
"""

from .short_squeeze import screen_short_squeeze, validate_squeeze_candidate
from .contrarian_entry import screen_contrarian_entry
from .earnings_short_setup import screen_earnings_short_setup

__all__ = [
    "screen_short_squeeze",
    "validate_squeeze_candidate",
    "screen_contrarian_entry",
    "screen_earnings_short_setup",
]
