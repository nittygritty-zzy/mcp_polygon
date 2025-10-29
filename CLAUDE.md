# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This is a Model Context Protocol (MCP) server that provides LLMs with access to Polygon.io financial market data API. The server exposes all Polygon.io API endpoints as MCP tools, converting JSON responses to CSV format for token efficiency.

## Development Environment

### Prerequisites
- Python 3.10+
- Astral UV package manager
- Polygon.io API key
- Virtual environment is managed by `uv`

### Environment Setup
Before running any Python commands, activate the virtual environment:
```bash
uv sync  # Install/sync dependencies
```

## Common Commands

### Development
```bash
# Run the server locally
POLYGON_API_KEY=your_api_key uv run mcp_polygon

# Run with specific transport (stdio, sse, streamable-http)
MCP_TRANSPORT=streamable-http POLYGON_API_KEY=your_api_key uv run mcp_polygon
```

### Code Quality
```bash
# Run linter (ruff format + ruff check --fix)
uv run just lint

# Or using just directly
just lint
```

### Testing
```bash
# Run all tests with pytest
uv run just test

# Or run pytest directly
uv run pytest -v tests

# Run specific test file
uv run pytest -v tests/test_formatters.py
```

### Debugging
```bash
# Use MCP Inspector for interactive testing
npx @modelcontextprotocol/inspector uv --directory /path/to/mcp_polygon run mcp_polygon
```

## Architecture

### Entry Point
- `entrypoint.py` - Main entry point that determines transport type (stdio/sse/streamable-http) and starts the server

### Core Components

#### Server (`src/mcp_polygon/server.py`)
- Uses FastMCP framework from `mcp.server.fastmcp`
- Initializes Polygon REST client with API key from environment
- Defines all MCP tools as async functions decorated with `@poly_mcp.tool()`
- Each tool wraps a Polygon.io SDK method
- All responses use `raw=True` to get binary data, then convert to CSV via `json_to_csv()`
- Returns CSV strings instead of JSON for better token efficiency with LLMs

#### Formatters (`src/mcp_polygon/formatters.py`)
- `json_to_csv()` - Main conversion function that handles:
  - JSON strings or dict objects
  - Extracting `results` key if present
  - Wrapping single objects in lists
  - Flattening nested structures via `_flatten_dict()`
- `_flatten_dict()` - Recursively flattens nested dicts by joining keys with underscores
  - Nested dicts become flat keys (e.g., `day.close` → `day_close`)
  - Lists are converted to string representations
  - Preserves None values

### Tool Implementation Pattern
All tools follow this pattern:
```python
@poly_mcp.tool(annotations=ToolAnnotations(readOnlyHint=True))
async def tool_name(
    required_param: type,
    optional_param: Optional[type] = None,
    params: Optional[Dict[str, Any]] = None,
) -> str:
    """Tool description for LLM."""
    try:
        results = polygon_client.method_name(
            param=param,
            raw=True,
        )
        return json_to_csv(results.data.decode("utf-8"))
    except Exception as e:
        return f"Error: {e}"
```

### Transport Configuration
The server supports three transport types via `MCP_TRANSPORT` environment variable:
- `stdio` (default) - Standard input/output
- `sse` - Server-Sent Events over HTTP
- `streamable-http` - Streamable HTTP transport

## API Coverage

The server implements tools for all major Polygon.io API endpoints:
- **Aggregates**: Stock, forex, crypto OHLC data (`get_aggs`, `list_aggs`, `get_grouped_daily_aggs`, etc.)
- **Trades & Quotes**: Historical and real-time market data (`list_trades`, `list_quotes`, `get_last_trade`, etc.)
- **Snapshots**: Current market state (`get_snapshot_ticker`, `list_universal_snapshots`, etc.)
- **Technical Indicators**: Stocks and options support: `get_sma` (Simple Moving Average), `get_ema` (Exponential Moving Average), `get_macd` (MACD), `get_rsi` (RSI)
- **Reference Data**:
  - Tickers: `get_all_tickers` (simplified interface), `list_tickers` (advanced filtering)
  - Details: `get_ticker_details`, `get_related_companies`, `get_ticker_types`
  - Market Info: `get_market_status`, `get_market_holidays`, `get_exchanges`, `list_conditions`
- **Fundamentals**:
  - Financial statements: `list_stock_financials`, `list_financials_balance_sheets`, `list_financials_cash_flow_statements`, `list_financials_income_statements`
  - Financial ratios: `list_financials_ratios` (historical from SEC filings), `list_stock_ratios` (current TTM ratios for stock screening)
  - Market sentiment: `list_short_interest` (bi-monthly short interest data), `list_short_volume` (daily short sale volume tracking)
- **Corporate Actions**: IPOs (`list_ipos`), splits (`list_splits`), dividends (`list_dividends`), events (`get_ticker_events`)
- **News & Insights**: Benzinga news and analyst data (`list_ticker_news`, `list_benzinga_*`)
- **Futures**: Contracts, quotes, trades, schedules (`list_futures_*`, `get_futures_*`)
- **Economics**: Treasury yields, inflation, and expectations (`list_treasury_yields`, `list_inflation`, `list_inflation_expectations`)
- **Options**: Options contracts index, details, pricing data, and snapshots (`list_options_contracts`, `get_options_contract`, `get_options_aggs`, `get_options_daily_open_close`, `get_options_previous_close`, `get_options_snapshot`, `list_snapshot_options_chain`)

### API Documentation References

When implementing new tools or debugging existing ones, refer to the official Polygon.io REST API documentation:
- **Aggregates (Custom Bars)**: https://polygon.io/docs/rest/stocks/aggregates/custom-bars
- **Daily Market Summary (Grouped Daily)**: https://polygon.io/docs/rest/stocks/aggregates/daily-market-summary
- **Daily Ticker Summary**: https://polygon.io/docs/rest/stocks/aggregates/daily-ticker-summary
- **Previous Day Bar**: https://polygon.io/docs/rest/stocks/aggregates/previous-day-bar
- **Single Ticker Snapshot**: https://polygon.io/docs/rest/stocks/snapshots/single-ticker-snapshot
- **Full Market Snapshot**: https://polygon.io/docs/rest/stocks/snapshots/full-market-snapshot
- **Unified Snapshot**: https://polygon.io/docs/rest/stocks/snapshots/unified-snapshot
- **Simple Moving Average (SMA)**: https://polygon.io/docs/rest/stocks/technical-indicators/simple-moving-average (stocks), https://polygon.io/docs/options/get_v1_indicators_sma__optionsticker (options)
- **Exponential Moving Average (EMA)**: https://polygon.io/docs/rest/stocks/technical-indicators/exponential-moving-average (stocks), https://polygon.io/docs/options/get_v1_indicators_ema__optionsticker (options)
- **MACD**: https://polygon.io/docs/rest/stocks/technical-indicators/moving-average-convergence-divergence (stocks), https://polygon.io/docs/options/get_v1_indicators_macd__optionsticker (options)
- **RSI**: https://polygon.io/docs/rest/stocks/technical-indicators/relative-strength-index (stocks), https://polygon.io/docs/options/get_v1_indicators_rsi__optionsticker (options)
- **Exchanges**: https://polygon.io/docs/rest/stocks/market-operations/exchanges
- **Market Holidays**: https://polygon.io/docs/rest/stocks/market-operations/market-holidays
- **Condition Codes**: https://polygon.io/docs/rest/stocks/market-operations/condition-codes
- **IPOs**: https://polygon.io/docs/rest/stocks/corporate-actions/ipos
- **Splits**: https://polygon.io/docs/rest/stocks/corporate-actions/splits
- **Dividends**: https://polygon.io/docs/rest/stocks/corporate-actions/dividends
- **Ticker Events**: https://polygon.io/docs/rest/stocks/corporate-actions/ticker-events
- **Balance Sheets (Financial Data)**: https://polygon.io/docs/rest/stocks/fundamentals/balance-sheets
- **Cash Flow Statements**: https://polygon.io/docs/rest/stocks/fundamentals/cash-flow-statements
- **Income Statements**: https://polygon.io/docs/rest/stocks/fundamentals/income-statements
- **Financial Ratios (Historical)**: https://polygon.io/docs/rest/stocks/fundamentals/financial-ratios (vX endpoint for SEC filings)
- **Financial Ratios (Stock Screener)**: https://polygon.io/docs/rest/stocks/fundamentals/financial-ratios (v1 endpoint for TTM ratios)
- **Short Interest**: https://polygon.io/docs/rest/stocks/fundamentals/short-interest
- **Short Volume**: https://polygon.io/docs/rest/stocks/fundamentals/short-volume
- **News (Stock News & Sentiment)**: https://polygon.io/docs/rest/stocks/reference/news
- **Treasury Yields (Economic Data)**: https://polygon.io/docs/rest/economy/treasury-yields
- **Inflation (CPI & PCE Indicators)**: https://polygon.io/docs/rest/economy/inflation
- **Inflation Expectations (Market & Model-Based)**: https://polygon.io/docs/rest/economy/inflation-expectations
- **Options Contracts (All)**: https://polygon.io/docs/options/get_v3_reference_options_contracts
- **Options Contract (Single)**: https://polygon.io/docs/options/get_v3_reference_options_contracts__options_ticker
- **Options Aggregates (OHLC Bars)**: https://polygon.io/docs/options/get_v2_aggs_ticker__optionsticker__range__multiplier___timespan___from___to
- **Options Daily Open/Close**: https://polygon.io/docs/options/get_v1_open-close__optionsticker___date
- **Options Previous Day Bar (OHLC)**: https://polygon.io/docs/options/get_v2_aggs_ticker__optionsticker__prev
- **Options Contract Snapshot**: https://polygon.io/docs/options/get_v3_snapshot_options__underlyingasset___optioncontract
- **Options Chain Snapshot**: https://polygon.io/docs/options/get_v3_snapshot_options__underlyingasset
- **All Tickers**: https://polygon.io/docs/rest/stocks/tickers/all-tickers
- **Ticker Overview (Details)**: https://polygon.io/docs/rest/stocks/tickers/ticker-overview
- **Related Tickers**: https://polygon.io/docs/rest/stocks/tickers/related-tickers
- **Ticker Types**: https://polygon.io/docs/rest/stocks/tickers/ticker-types
- **Full API Reference**: https://polygon.io/docs/rest/getting-started
- **Python SDK Documentation**: https://polygon-io-python.readthedocs.io/

## Code Organization

```
mcp_polygon/
├── entrypoint.py           # Entry point with transport selection
├── src/mcp_polygon/
│   ├── __init__.py         # Exports run() function
│   ├── server.py           # Main server with all MCP tools
│   └── formatters.py       # JSON to CSV conversion utilities
└── tests/
    └── test_formatters.py  # Comprehensive formatter tests
```

## Development Notes

### Adding New Tools
1. Add async function decorated with `@poly_mcp.tool()` in `server.py`
2. Use `ToolAnnotations(readOnlyHint=True)` for read-only operations
3. Follow existing pattern: accept parameters, call polygon_client method with `raw=True`, return `json_to_csv()` result
4. Handle errors with try/except, return string error messages

### CSV Format Rationale
The server returns CSV instead of JSON for token efficiency when working with LLMs. CSV is more compact and easier for LLMs to parse for tabular data.

### Import Paths
Use relative imports within the package:
```python
from .formatters import json_to_csv  # Correct
from mcp_polygon.formatters import json_to_csv  # Avoid
```

### Version Management
Version is set in `pyproject.toml` and automatically included in Polygon API client User-Agent header.
