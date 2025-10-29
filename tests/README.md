# Running Tests

## Setup

1. Set your Polygon API key in the environment:
```bash
export POLYGON_API_KEY="your_api_key_here"
```

Alternatively, create a `.env` file in the project root:
```bash
POLYGON_API_KEY=your_api_key_here
```

2. Install test dependencies (already included in pyproject.toml):
```bash
uv sync
```

## Running All Tool Tests

Run all 50 tool tests:
```bash
POLYGON_API_KEY=your_key uv run pytest tests/test_all_tools.py -v
```

Or with .env file:
```bash
uv run pytest tests/test_all_tools.py -v
```

## Running Specific Tests

Run tests for a specific module:
```bash
POLYGON_API_KEY=your_key uv run pytest tests/test_all_tools.py -v -k "aggregates"
POLYGON_API_KEY=your_key uv run pytest tests/test_all_tools.py -v -k "options"
```

Run a single test:
```bash
POLYGON_API_KEY=your_key uv run pytest tests/test_all_tools.py -v -k "test_get_aggs"
```

## Test Coverage

The test suite covers all 11 tool modules:

1. **Aggregates** (5 tests)
   - get_aggs, get_grouped_daily_aggs, get_previous_close_agg, get_daily_open_close_agg

2. **Snapshots** (4 tests)
   - get_snapshot_ticker, list_universal_snapshots, get_snapshot_all, get_snapshot_direction

3. **Technical Indicators** (4 tests)
   - get_sma, get_ema, get_macd, get_rsi

4. **Reference Data** (8 tests)
   - list_tickers, get_ticker_details, get_related_companies, get_ticker_types
   - get_market_status, get_market_holidays, get_exchanges, list_conditions

5. **Corporate Actions** (4 tests)
   - list_splits, list_dividends, get_ticker_events, list_ipos

6. **Financials** (6 tests)
   - list_financials_income_statements, list_financials_balance_sheets
   - list_financials_cash_flow_statements, list_stock_ratios
   - list_short_interest, list_short_volume

7. **News** (1 test)
   - list_ticker_news

8. **Economics** (3 tests)
   - list_treasury_yields, list_inflation, list_inflation_expectations

9. **Options** (3 tests)
   - list_options_contracts, get_options_contract, get_options_chain_snapshot

10. **Futures** (1 test)
    - list_futures_contracts

11. **Currency** (1 test)
    - get_real_time_currency_conversion

**Total: 40 comprehensive tests covering all major API endpoints**

## Notes

- Tests use real API calls and require a valid Polygon API key
- Some tests may fail if you don't have access to specific data tiers (e.g., options, futures)
- Tests use recent dates to ensure data availability
- All tests verify CSV output format
