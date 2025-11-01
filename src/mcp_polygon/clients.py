"""Shared client instances for the Polygon MCP server."""

import os
import logging
import warnings
from importlib.metadata import version, PackageNotFoundError

from mcp.server.fastmcp import FastMCP
from polygon import RESTClient

# Suppress duplicate tool registration warnings during import
# These warnings occur because multiple tool modules import poly_mcp,
# causing FastMCP to check for already-registered tools
logging.getLogger("mcp.server.fastmcp.tool_manager").setLevel(logging.CRITICAL)
logging.getLogger("mcp.server.fastmcp").setLevel(logging.CRITICAL)
warnings.filterwarnings("ignore", message=".*Tool already exists.*")

# Also suppress Rich console warnings if possible
try:
    logging.getLogger().handlers = []  # Clear existing handlers
    logging.getLogger().addHandler(logging.NullHandler())  # Add null handler
except Exception:  # noqa: S110
    pass

# Get API key from environment
POLYGON_API_KEY = os.environ.get("POLYGON_API_KEY", "")
if not POLYGON_API_KEY:
    print("Warning: POLYGON_API_KEY environment variable not set.")

# Get version for User-Agent
version_number = "MCP-Polygon/unknown"
try:
    version_number = f"MCP-Polygon/{version('mcp_polygon')}"
except PackageNotFoundError:
    pass

# Initialize Polygon REST client
polygon_client = RESTClient(POLYGON_API_KEY)
polygon_client.headers["User-Agent"] += f" {version_number}"

# Initialize MCP server
poly_mcp = FastMCP("Polygon", dependencies=["polygon"])
