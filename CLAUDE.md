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
- **Screeners**: High-performance composite tools combining multiple data sources
  - Short squeeze: `screen_short_squeeze` (comprehensive screening with fundamental validation), `validate_squeeze_candidate` (deep dive on single ticker)
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

## Advanced Features: Short Squeeze Screener

### Overview

The short squeeze screener (`screen_short_squeeze`) is a high-performance composite tool that combines multiple Polygon API endpoints to identify potential short squeeze candidates with comprehensive validation.

**Key Features:**
- **Speed**: Scans 1000s of stocks in ~30 seconds using parallel API calls and caching
- **Accuracy**: Mandatory fundamental validation avoids false positives (learned from FLWS case study - see `trading_journal/2025-11-01_VALIDATION_REPORT.md`)
- **Comprehensive**: Combines short metrics + fundamentals + optional catalyst detection

### Usage Examples

```python
# Basic scan with recommended defaults
screen_short_squeeze(min_days_to_cover=10.0)

# Conservative scan (strict fundamentals)
screen_short_squeeze(
    min_days_to_cover=10.0,
    min_market_cap=100_000_000,
    require_profitability=True,
    require_positive_fcf=True,
    max_debt_to_equity=1.0
)

# Quick scan (skip optional checks for speed)
screen_short_squeeze(
    check_catalysts=False,
    max_results=20
)
```

### Architecture

The screener is implemented as a **multi-step composite tool** (unlike single-endpoint tools):

1. **Step 1**: Fetch short interest data (`list_short_interest`) with `days_to_cover` filter
2. **Step 2**: Join with stock fundamentals (`list_stock_ratios`) with API-side filtering
3. **Step 3**: Optional catalyst detection (news scan via `list_ticker_news`)
4. **Step 4**: Score and rank candidates using composite algorithm
5. **Step 5**: Cache results to Parquet for DuckDB analysis

### Performance Optimizations

**Speed techniques used:**
- `PolygonParallelFetcher` with 5 concurrent workers for pagination
- API-side filtering (push filters to Polygon, reduce data transfer)
- In-memory pandas joins (faster than repeated API calls)
- Intelligent caching (results partitioned by `scan_date` for trend analysis)

**Typical performance:**
- 1000+ stock universe: 20-30 seconds
- 5000+ stock universe: 30-45 seconds (depends on result count)
- API calls: ~5-10 (significantly less than naive approach)

### Validation Strategy

The screener implements lessons learned from failed candidates (see trading journal 2025-11-01):

**Mandatory filters** (prevent value traps):
- `require_profitability=True` (default): Filter to EPS > 0
  - **Why**: FLWS had EPS=-$3.44 (unprofitable company)
- `min_market_cap=50M` (default): Ensure minimum liquidity
  - **Why**: Micro-caps with extreme volatility are untradeable
- `max_debt_to_equity=2.0` (default): Avoid overleveraged companies
  - **Why**: High debt + unprofitability = bankruptcy risk

**Optional validation** (improves signal quality):
- `require_positive_fcf=True`: Filter to Free Cash Flow > 0
  - Stricter than EPS (GAAP accounting vs actual cash)
- `check_catalysts=True`: Scan recent news for triggers
  - Squeezes need catalysts (earnings, product launch, etc.)
- `check_sector_context=True`: Compare to sector ETF short metrics
  - Avoid sector-wide bearishness (e.g., retail sector in FLWS case)

### Output Format

Returns CSV with columns:
- `ticker`: Stock symbol
- `days_to_cover`: Short interest ÷ avg daily volume (illiquidity metric)
- `short_interest_shares`: Total shares sold short
- `market_cap`: Market capitalization
- `price`: Current stock price
- `eps`: Earnings per share (TTM)
- `free_cash_flow`: Free cash flow (TTM)
- `debt_to_equity`: Total debt ÷ total equity
- `squeeze_score`: Composite score (0-100, higher = stronger candidate)
- `has_catalyst`: Boolean flag for recent news/events
- `validation_passed`: Summary of which criteria passed/failed

### Caching and Analysis

Results are cached to `./cache/screen_short_squeeze/YYYY-MM/data_*.parquet` for historical tracking.

**DuckDB analysis examples:**
```sql
-- Compare today's scan vs last week
SELECT
    t1.ticker,
    t1.squeeze_score as score_today,
    t2.squeeze_score as score_last_week,
    t1.squeeze_score - t2.squeeze_score as score_change
FROM read_parquet('./cache/screen_short_squeeze/2025-11/data_*.parquet') t1
LEFT JOIN read_parquet('./cache/screen_short_squeeze/2025-10/data_*.parquet') t2
  ON t1.ticker = t2.ticker
WHERE t1.squeeze_score > 50
ORDER BY score_change DESC;

-- Find stocks with improving fundamentals
SELECT ticker, days_to_cover, eps, debt_to_equity
FROM read_parquet('./cache/screen_short_squeeze/**/*.parquet')
WHERE eps > 0 AND debt_to_equity < 1.0
ORDER BY days_to_cover DESC;
```

### Important Limitations

1. **Data Freshness**: Short interest data is bi-monthly (published ~15th and 30th)
   - Latest data may be 2-4 weeks old
   - Use `list_short_volume` for daily short sale activity

2. **False Signals**: High days-to-cover can indicate:
   - **Good**: Short trap (shorts can't exit easily)
   - **Bad**: Liquidity crisis (no one wants to buy)
   - **Solution**: Fundamental validation separates these cases

3. **Sector Context**: Individual stock squeeze vs sector-wide rotation
   - Use `check_sector_context=True` to validate stock-specific setup

### Related Tools

- `validate_squeeze_candidate(ticker)`: Deep dive on single stock (TODO: full implementation)
- `list_short_interest(ticker)`: Check 6-month short interest trend
- `list_short_volume(ticker)`: Check daily short volume for covering signals
- `list_stock_ratios(ticker)`: Full fundamental analysis
- `duckdb_query(sql)`: Custom analysis of cached screener results

### Best Practices

**Before entering positions:**
1. Run screener to identify candidates
2. Validate 6-month short interest trend (should be DECLINING)
3. Check daily short volume trend (covering ongoing?)
4. Verify fundamentals (business quality, profitability)
5. Identify catalyst (earnings, news, events within 30 days)
6. Compare to sector ETF (stock-specific vs sector-wide pressure)

**Example workflow:**
```python
# Step 1: Find candidates
screen_short_squeeze(min_days_to_cover=15.0, require_profitability=True)

# Step 2: Validate top candidate (e.g., GME)
list_short_interest(ticker="GME", settlement_date_gte="2025-04-01", fetch_all=True)
list_short_volume(ticker="GME", date_gte="2025-10-01", fetch_all=True)
list_ticker_news(ticker="GME", published_utc_gte="2025-10-01", limit=10)

# Step 3: Analyze trends with DuckDB
duckdb_query("""
    SELECT settlement_date, short_interest, days_to_cover
    FROM read_parquet('./cache/list_short_interest/GME/**/*.parquet')
    ORDER BY settlement_date DESC
    LIMIT 10
""")
```

See `trading_journal/2025-11-01_VALIDATION_REPORT.md` for detailed case study on avoiding false positives.

## Advanced Features: Contrarian Entry Point Screener (逆向入场点)

### Overview
The contrarian entry screener (`screen_contrarian_entry`) identifies oversold stocks with excessive shorting at technical support levels for mean reversion opportunities.

**Key Features:**
- DuckDB-based consecutive day analysis (10-20s with cache)
- Validates increasing short interest (shorts adding positions = potential trap)
- Checks 4 technical support levels (50/200-day SMA, RSI < 30, 52-week low)
- Optional fundamental filters (market cap, profitability, leverage)

### Signal Logic

The screener finds stocks where shorts may be overextended and due for a bounce:

1. **Persistent High Short Volume**: 3+ consecutive days with short_volume_ratio > 60%
2. **Increasing Short Interest**: Bi-monthly FINRA data shows shorts ADDING positions (+% change)
3. **Technical Support**: Price within 5% of at least one support level
4. **Fundamental Health**: Optional filters for market cap, profitability, leverage

**Contrarian Thesis:**
- Heavy shorting (>60% of volume) for multiple days = extreme bearish pressure
- Shorts building positions (increasing SI) = potential for short trap
- Price holding at support = buyers defending level
- RSI oversold (<30) = technical bounce likely
- **Entry**: Buy when all signals align, stop loss below support

### Usage Examples

```python
# Basic contrarian scan (recommended defaults)
screen_contrarian_entry(
    min_short_volume_ratio=60.0,
    min_consecutive_days=3,
    support_proximity_pct=5.0
)

# Conservative scan (strict fundamentals + higher thresholds)
screen_contrarian_entry(
    min_short_volume_ratio=65.0,
    min_consecutive_days=5,
    support_proximity_pct=3.0,
    require_profitability=True,
    max_debt_to_equity=2.0
)

# Aggressive scan (extreme oversold conditions)
screen_contrarian_entry(
    min_short_volume_ratio=70.0,
    min_consecutive_days=7,
    support_proximity_pct=2.0,
    max_results=20
)

# Quick exploratory scan (relaxed filters)
screen_contrarian_entry(
    min_short_volume_ratio=55.0,
    min_consecutive_days=3,
    require_profitability=False,
    max_debt_to_equity=5.0,
    max_results=30
)
```

### Architecture

**5-Step Pipeline:**

```python
# Step 1: Fetch high short volume candidates (DuckDB or API)
sv_candidates = await _fetch_high_short_volume_candidates(
    min_ratio=60.0,
    min_consecutive_days=3,
    lookback_days=30
)
# Uses DuckDB window functions on cached data:
# ROW_NUMBER() OVER ... - ROW_NUMBER() OVER PARTITION BY ... = streak counting

# Step 2: Validate short interest trend (increasing = shorts trapped)
si_validated = await _validate_short_interest_trend(sv_candidates)
# Fetches 6-month history, calculates (last_SI - first_SI) / first_SI
# Filters for positive % change only

# Step 3: Check technical support (4 levels)
support_validated = await _check_technical_support(
    candidates=si_validated,
    proximity_pct=5.0
)
# Checks: 50-day SMA, 200-day SMA, RSI < 30, near 52-week low

# Step 4: Fundamental validation (reuse from short squeeze screener)
fundamental_validated = await _validate_fundamentals(...)

# Step 5: Score and rank
scored = _score_contrarian_signal(candidates, max_results=50)
```

**Consecutive Day Counting (DuckDB Window Functions):**

```sql
WITH daily_sv AS (
    SELECT ticker, date, short_volume_ratio,
        CASE WHEN short_volume_ratio > 60 THEN 1 ELSE 0 END as is_high_sv
    FROM read_parquet('./cache/list_short_volume/**/*.parquet')
),
streaks AS (
    SELECT ticker, date, short_volume_ratio,
        -- Streak ID: groups consecutive rows with same is_high_sv value
        ROW_NUMBER() OVER (PARTITION BY ticker ORDER BY date) -
        ROW_NUMBER() OVER (PARTITION BY ticker, is_high_sv ORDER BY date) as streak_id
    FROM daily_sv
    WHERE is_high_sv = 1
)
SELECT ticker, COUNT(*) as consecutive_days, AVG(short_volume_ratio) as avg_sv_ratio
FROM streaks
GROUP BY ticker, streak_id
HAVING COUNT(*) >= 3
```

### Technical Support Detection

The screener checks 4 support levels in parallel:

**1. 50-day Simple Moving Average (Medium-term support)**
- Most common support for swing trades
- Price within proximity_pct (default: 5%) triggers signal
- Example: Price $42.40, SMA-50 $42.00 → 0.95% distance → Support

**2. 200-day Simple Moving Average (Long-term support)**
- Major institutional support level
- Bull/bear market divider
- Stronger signal than 50-day SMA

**3. RSI < 30 (Technical oversold)**
- Relative Strength Index below 30 = oversold
- Mean reversion catalyst
- Independent of price/SMA

**4. Near 52-week Low (Extreme oversold)**
- Price within 10% of 52-week low
- Capitulation signal
- Higher risk but stronger bounce potential

**At least 1 support must trigger** for candidate to pass. Multiple supports = stronger signal.

### Scoring Algorithm

```python
contrarian_score = (
    consecutive_days_score * 0.35 +      # More days = stronger oversold
    short_interest_trend_score * 0.25 +  # Increasing SI = shorts trapped
    support_level_count_score * 0.25 +   # Multiple supports = stronger
    avg_sv_ratio_score * 0.15            # Higher ratio = more extreme
)
```

**Normalization:**
- `consecutive_days`: 0-10 days → 0-100 score (cap at 10 days)
- `si_trend`: 0-50% increase → 0-100 score (cap at +50%)
- `support_count`: 1-4 supports → 25-100 score
- `avg_sv_ratio`: 60-80% → 0-100 score

**Example Calculation:**
```
Ticker: XYZ
- 7 consecutive days (70% of 10 max) = 70 points * 0.35 = 24.5
- +25% SI increase (50% of 50% max) = 50 points * 0.25 = 12.5
- 3 supports (75% of 4 max) = 75 points * 0.25 = 18.75
- 70% avg SV ratio ((70-60)/20) = 50 points * 0.15 = 7.5
Total contrarian_score = 63.25
```

### Output Format

CSV columns:
```
ticker,consecutive_high_sv_days,avg_sv_ratio,short_interest_trend_pct,
price,support_level,rsi,market_cap,eps,debt_to_equity,
contrarian_score,entry_rationale
```

**Example Output:**
```csv
ticker,consecutive_high_sv_days,avg_sv_ratio,short_interest_trend_pct,price,support_level,rsi,market_cap,contrarian_score,entry_rationale
XYZ,5,67.5,15.2,42.40,at_50day_sma/rsi_oversold,28.5,500000000,87.3,"5 days >67.5% SV | at_50day_sma/rsi_oversold | SI +15.2%"
ABC,3,62.1,8.7,15.80,at_200day_sma,31.2,250000000,72.1,"3 days >62.1% SV | at_200day_sma | SI +8.7%"
```

### Performance

**Expected Runtime:**
- First run (no cache): 40-60 seconds (fetch indicators for all candidates)
- Subsequent runs: 10-20 seconds (DuckDB queries on cached data)
- With cache + limited candidates: 5-10 seconds

**Data Requirements:**
- Short volume (30 days): ~30 rows per ticker
- Short interest (6 months): ~12 rows per ticker (bi-monthly)
- Technical indicators: RSI, SMA-50, SMA-200, OHLC (252 days for 52-week low)

**Optimization Tips:**
1. Run `list_short_volume(fetch_all=True)` periodically to cache data
2. Cache technical indicators for watched tickers
3. Use `fetch_all=False` for quick exploratory scans
4. Increase `min_short_volume_ratio` to reduce candidate count

### Risk Management Example

From user specification:
```
Stock: XYZ
Screening Results:
- 5 consecutive days with 67% short volume ratio
- Short interest +15.2% over 6 months
- Price $42.40 at 50-day SMA ($42.00)
- RSI 28.5 (oversold)

Entry Strategy:
- Entry price: $42.40 (at support)
- Stop loss: $41.00 (below 50-day SMA)
- Risk: ($42.40 - $41.00) / $42.40 = 3.3% per trade
- Target: Mean reversion to recent high or resistance (5-10% gain)
- Position size: Risk 1-2% of portfolio per trade
```

### Important Limitations

**1. Data Lag:**
- Short volume: T+1 lag (yesterday's data available today)
- Short interest: Bi-monthly, 2-4 weeks old
- Technical indicators: Based on most recent close

**2. False Signals:**
- High short volume may indicate legitimate bearishness
- Support levels can break (use stop losses!)
- Contrarian plays are inherently risky (fight the trend)
- Macro events can override technical signals

**3. Validation Requirements:**
- **ALWAYS check fundamentals** (avoid failing companies)
- **Verify support is holding** (not just approaching)
- **Check volume** at support level (high volume = strong defense)
- **Review news** for catalyst or deterioration

**4. Market Conditions:**
- Works best in range-bound markets (mean reversion environment)
- Less effective in strong trends (don't fight the tape)
- Requires sufficient liquidity (min_market_cap filter)

### Best Practices

**1. Pre-Scan Setup:**
```python
# Cache short volume data for faster scans
await list_short_volume(date_gte="2025-10-01", fetch_all=True)

# Cache short interest for trend analysis
await list_short_interest(settlement_date_gte="2025-04-01", fetch_all=True)
```

**2. Multi-Tier Workflow:**
```python
# Tier 1: Quick exploratory scan (relaxed filters)
quick_scan = await screen_contrarian_entry(
    min_consecutive_days=3,
    fetch_all=False
)

# Tier 2: Filter to top candidates (strict filters)
conservative_scan = await screen_contrarian_entry(
    min_consecutive_days=5,
    min_short_volume_ratio=65.0,
    require_profitability=True,
    max_results=10
)

# Tier 3: Manual validation
# - Check daily charts for support confirmation
# - Review recent news for catalysts or warnings
# - Verify volume at support level
# - Calculate position size based on stop loss
```

**3. DuckDB Analysis (Post-Scan):**
```python
# Analyze historical performance of screened candidates
sql = """
SELECT
    ticker,
    MAX(consecutive_high_sv_days) as max_streak,
    AVG(contrarian_score) as avg_score,
    COUNT(*) as scan_appearances
FROM read_parquet('./cache/screen_contrarian_entry/**/*.parquet')
GROUP BY ticker
ORDER BY scan_appearances DESC, avg_score DESC
LIMIT 20
"""
```

### Related Tools

- `list_short_volume(ticker)` - Check daily short volume trend
- `list_short_interest(ticker)` - Verify 6-month SI trend
- `get_rsi(ticker)` - Get RSI oversold confirmation
- `get_sma(ticker, window=50)` - Check 50-day SMA distance
- `get_sma(ticker, window=200)` - Check 200-day SMA distance
- `get_aggs(ticker)` - Get price action and 52-week low
- `list_stock_ratios(ticker)` - Full fundamental analysis

### Differences from Short Squeeze Screener

| Feature | Short Squeeze | Contrarian Entry |
|---------|--------------|------------------|
| **Primary Signal** | Days-to-cover (illiquidity) | Consecutive high short volume (overselling) |
| **Short Interest Trend** | Prefer declining (covering) | Require increasing (building positions) |
| **Technical Analysis** | Optional | Mandatory (4 support levels) |
| **Profitability Filter** | Mandatory (default: True) | Optional (default: False) |
| **Leverage Tolerance** | Conservative (2.0) | Lenient (3.0) |
| **Risk Profile** | Lower (quality companies) | Higher (contrarian plays) |
| **Holding Period** | Weeks to months (squeeze development) | Days to weeks (mean reversion) |
| **Best Use Case** | Fundamental quality + short trap | Technical oversold + short trap |

### Example Journal Entry (Trading Workflow)

```markdown
## 2025-11-02 Contrarian Scan Results

### Screener Output
scan_contrarian_entry(min_consecutive_days=5, min_short_volume_ratio=65.0)

### Top Candidate: XYZ
- Consecutive days: 7 days >68% short volume
- Short interest trend: +22% over 6 months
- Support levels: at_50day_sma + rsi_oversold (2/4 supports)
- Price: $42.40 vs SMA-50: $42.00 (0.95% above)
- RSI: 27.3 (oversold)
- Fundamentals: $500M mcap, EPS $1.20, D/E 2.1

### Action Plan
1. **Entry**: $42.40 (current price at support)
2. **Stop Loss**: $41.00 (3.3% below entry, below 50-day SMA)
3. **Target**: $46.00 (8.5% gain, previous resistance)
4. **Position Size**: 2% portfolio risk = (2% / 3.3%) = 60% of normal size
5. **Catalyst**: Check for upcoming earnings or news

### Validation Checklist
- [x] Fundamentals acceptable (profitable, not overleveraged)
- [x] Support confirmed (price holding for 2+ days)
- [x] Volume analysis (above-average volume at support)
- [ ] News review (pending - check for deterioration)
- [ ] Sector context (check retail sector ETF for comparison)
```

---

**Implementation Details:** See `src/mcp_polygon/tools/screeners.py` lines 531-1153 for full implementation.
