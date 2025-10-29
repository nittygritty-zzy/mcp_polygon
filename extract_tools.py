#!/usr/bin/env python3
"""Helper script to extract tools from server.py into separate files."""

import re
from pathlib import Path

# Tool categorization
TOOL_CATEGORIES = {
    "aggregates": [
        "get_aggs",
        "list_aggs",
        "get_grouped_daily_aggs",
        "get_daily_open_close_agg",
        "get_previous_close_agg",
    ],
    "snapshots": [
        "list_universal_snapshots",
        "get_snapshot_all",
        "get_snapshot_direction",
        "get_snapshot_ticker",
        "get_snapshot_option",
        "get_snapshot_crypto_book",
    ],
    "technical_indicators": [
        "get_sma",
        "get_ema",
        "get_macd",
        "get_rsi",
    ],
    "reference_data": [
        "get_market_holidays",
        "get_market_status",
        "list_tickers",
        "get_all_tickers",
        "get_ticker_details",
        "get_related_companies",
        "get_ticker_types",
        "list_conditions",
        "get_exchanges",
    ],
    "corporate_actions": [
        "list_splits",
        "list_dividends",
        "get_ticker_events",
        "list_ipos",
    ],
    "financials": [
        "list_stock_financials",
        "list_financials_balance_sheets",
        "list_financials_cash_flow_statements",
        "list_financials_income_statements",
        "list_financials_ratios",
        "list_stock_ratios",
        "list_short_interest",
        "list_short_volume",
    ],
    "news": [
        "list_ticker_news",
    ],
    "economics": [
        "list_treasury_yields",
        "list_inflation",
        "list_inflation_expectations",
    ],
    "options": [
        "list_options_contracts",
        "get_options_contract",
        "get_options_aggs",
        "get_options_daily_open_close",
        "get_options_previous_close",
        "get_options_snapshot",
        "get_options_chain_snapshot",
    ],
    "futures": [
        "list_futures_aggregates",
        "list_futures_contracts",
        "get_futures_contract_details",
        "list_futures_products",
        "get_futures_product_details",
        "list_futures_schedules",
        "list_futures_schedules_by_product_code",
        "list_futures_market_statuses",
        "get_futures_snapshot",
    ],
    "currency": [
        "get_real_time_currency_conversion",
    ],
}

# Read server.py
server_file = Path("src/mcp_polygon/server.py")
content = server_file.read_text()

# Extract tools by finding function definitions
def extract_tool_code(content, tool_name):
    """Extract the complete code for a tool function."""
    # Find the start of the function (either @poly_mcp.tool or async def)
    pattern = rf'(@poly_mcp\.tool.*?\n)?async def {re.escape(tool_name)}\('
    match = re.search(pattern, content, re.DOTALL)

    if not match:
        return None

    start = match.start()

    # Find the end by looking for the next function definition or end of file
    next_func = re.search(r'\n(?:@poly_mcp\.tool|async def |def )', content[match.end():])
    if next_func:
        end = match.end() + next_func.start()
    else:
        end = len(content)

    return content[start:end].rstrip() + "\n\n"

# Create tool files
tools_dir = Path("src/mcp_polygon/tools")
tools_dir.mkdir(exist_ok=True)

# Common imports for all tool files
common_imports = '''"""Auto-generated tool definitions."""
from typing import Optional, Any, Dict, Union, List
from mcp.types import ToolAnnotations
from datetime import datetime, date

'''

for category, tool_names in TOOL_CATEGORIES.items():
    file_content = common_imports

    extracted_count = 0
    for tool_name in tool_names:
        tool_code = extract_tool_code(content, tool_name)
        if tool_code:
            file_content += tool_code
            extracted_count += 1

    if extracted_count > 0:
        output_file = tools_dir / f"{category}.py"
        output_file.write_text(file_content)
        print(f"✓ Created {output_file} with {extracted_count} tools")
    else:
        print(f"✗ No tools found for {category}")

print(f"\nExtraction complete!")
