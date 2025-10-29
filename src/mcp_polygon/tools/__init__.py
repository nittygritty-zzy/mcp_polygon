"""
Tool modules for the Polygon MCP server.

This module imports all tool definitions from sub-modules.
Each tool is automatically registered with poly_mcp when imported.

Note: Duplicate tool registration warnings are suppressed in clients.py
to avoid noise during module imports.
"""

# Import all tool modules to register their tools with poly_mcp
from . import aggregates
from . import snapshots
from . import technical_indicators
from . import reference_data
from . import corporate_actions
from . import financials
from . import news
from . import economics
from . import options
from . import futures
from . import currency
from . import alpha_vantage
from . import query

__all__ = [
    "aggregates",
    "snapshots",
    "technical_indicators",
    "reference_data",
    "corporate_actions",
    "financials",
    "news",
    "economics",
    "options",
    "futures",
    "currency",
    "alpha_vantage",
    "query",
]
