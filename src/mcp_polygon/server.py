"""
Polygon MCP Server

This module provides a Model Context Protocol server for Polygon.io financial market data.
All tools are defined in the tools/ sub-package and automatically registered when imported.
"""

from typing import Literal
from .clients import poly_mcp

# Import all tools to register them with poly_mcp
from . import tools  # noqa: F401


def run(transport: Literal["stdio", "sse", "streamable-http"] = "stdio") -> None:
    """Run the Polygon MCP server."""
    poly_mcp.run(transport)
