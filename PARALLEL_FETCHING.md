# Parallel Fetching Implementation

## Overview

This document describes the parallel fetching implementation for the Polygon.io MCP server, which maximizes download speed when fetching paginated data from the Polygon.io API.

## Key Features

- **5 Parallel Workers**: Each API endpoint with `fetch_all=True` now uses 5 concurrent workers
- **Automatic Pagination**: Workers automatically fetch pages in parallel and combine results
- **Thread Pool Execution**: Uses `ThreadPoolExecutor` to avoid blocking the async event loop
- **Smart Queue Management**: Workers share a cursor queue for efficient page distribution

## Architecture

### Core Components

#### 1. `ParallelFetcher` (src/mcp_polygon/parallel_fetcher.py)
Base class for parallel pagination:
- Manages worker pool (default: 5 workers)
- Handles page fetching with cursor-based pagination
- Coordinates workers via async queue

#### 2. `PolygonParallelFetcher` (src/mcp_polygon/parallel_fetcher.py)
Polygon.io-specific implementation:
- Wraps Polygon SDK methods
- Handles both regular and VX client methods
- Parses JSON responses and extracts pagination cursors

### How It Works

```python
# Sequential (OLD - removed)
for item in polygon_client.get_aggs(...):
    aggs_list.append(vars(item))

# Parallel (NEW)
fetcher = PolygonParallelFetcher(polygon_client, num_workers=5)
aggs_list = await fetcher.fetch_all(
    method_name='get_aggs',
    ticker='AAPL',
    ...
)
```

#### Execution Flow:

1. **Fetch First Page**: Determine pagination structure
2. **Start Workers**: Launch 5 async workers
3. **Distribute Work**: Workers pull cursors from queue
4. **Fetch Pages**: Each worker fetches pages independently
5. **Queue Next**: Workers add new cursors to queue
6. **Combine Results**: All pages merged into single list

## Converted Tools

All 28 tools with `fetch_all` parameter now use parallel fetching:

### Aggregates (2 tools)
- `get_aggs`
- `list_aggs`

### Reference Data (2 tools)
- `list_tickers`
- `get_all_tickers`

### News (1 tool)
- `list_ticker_news`

### Economics (3 tools)
- `list_treasury_yields`
- `list_inflation`
- `list_inflation_expectations`

### Corporate Actions (3 tools)
- `list_splits`
- `list_dividends`
- `list_ipos` (uses VX client)

### Financials (3 tools)
- `list_stock_financials` (uses VX client)
- `list_short_interest`
- `list_short_volume`

### Futures (6 tools)
- `list_futures_aggregates`
- `list_futures_contracts`
- `list_futures_products`
- `list_futures_schedules`
- `list_futures_schedules_by_product_code`
- `list_futures_market_statuses`

### Technical Indicators (4 tools)
- `get_sma`
- `get_ema`
- `get_macd`
- `get_rsi`

### Snapshots (1 tool)
- `list_universal_snapshots`

### Options (3 tools)
- `list_options_contracts`
- `get_options_aggs`
- `list_snapshot_options_chain`

## Performance Benefits

### Speed Improvements
- **5x faster** for large datasets (theoretical maximum)
- **Actual speedup** depends on:
  - API rate limits
  - Network latency
  - Number of pages to fetch

### Example: Fetching 1 Year of Daily AAPL Data

**Sequential (OLD)**:
- 252 trading days ÷ 50 per page = ~5 pages
- 5 pages × 200ms = 1000ms

**Parallel (NEW)**:
- 5 workers fetch simultaneously
- ~200-400ms total (2.5-5x faster)

### Example: Fetching All US Tickers

**Sequential (OLD)**:
- ~10,000 tickers ÷ 100 per page = 100 pages
- 100 pages × 200ms = 20,000ms (20 seconds)

**Parallel (NEW)**:
- 5 workers share the load
- ~4,000-8,000ms (5-10x faster)

## Configuration

### Number of Workers

Default: 5 workers per API call

To customize (not currently exposed, but can be modified in code):
```python
fetcher = PolygonParallelFetcher(polygon_client, num_workers=10)
```

### API Rate Limits

The Polygon.io API has different rate limits based on subscription tier:
- **Starter**: 5 requests/minute
- **Developer**: 100 requests/minute
- **Advanced**: 1,000 requests/minute
- **Unlimited**: No limits

With 5 workers, you're making up to 5 concurrent requests. Ensure your API tier supports this.

## Testing

Run parallel fetcher tests:
```bash
uv run python tests/test_parallel_fetcher.py
```

Tests verify:
- Single page fetching
- Multi-page pagination
- VX client support
- Error handling

## Backward Compatibility

The implementation is **fully backward compatible**:

- `fetch_all=True` (default): Uses parallel fetching
- `fetch_all=False`: Uses single-page API call (no changes)

## Implementation Details

### VX Client Support

Some endpoints use `polygon_client.vx.method`:
```python
fetcher.fetch_all(
    method_name='list_ipos',
    use_vx=True,  # Uses client.vx.list_ipos
    ipo_status='pending',
)
```

### Cursor Extraction

Parses `next_url` from API responses:
```python
next_cursor = data_json.get('next_url', '')
if next_cursor and 'cursor=' in next_cursor:
    next_cursor = next_cursor.split('cursor=')[1].split('&')[0]
```

### Thread Safety

Uses `asyncio.Lock` for thread-safe result aggregation:
```python
async with results_lock:
    results.extend(data)
```

## Migration Scripts

### Conversion Scripts (in scripts/)
- `update_all_parallel.sh`: Add imports to all tool files
- `convert_to_parallel.py`: Convert iterator loops to parallel fetcher
- `final_parallel_conversion.py`: Handle remaining edge cases

These scripts automated the conversion of all 28 tools.

## Future Improvements

### Potential Enhancements
1. **Adaptive Worker Count**: Adjust workers based on API tier
2. **Rate Limit Handling**: Automatic backoff on rate limit errors
3. **Progress Callbacks**: Report download progress to users
4. **Streaming Results**: Yield results as they arrive instead of waiting for all pages

### Monitoring
Consider adding:
- Request timing metrics
- Worker utilization stats
- Error rate tracking

## Troubleshooting

### Slow Downloads
- Check your API tier rate limits
- Reduce `num_workers` if hitting rate limits
- Verify network connectivity

### Missing Data
- Ensure `fetch_all=True` is set (default)
- Check for API errors in logs
- Verify pagination cursors are being extracted correctly

### Memory Issues
- Very large datasets may consume significant memory
- Consider streaming or batching for massive downloads
- Monitor cache size in `./cache/` directory

## Related Files

- `src/mcp_polygon/parallel_fetcher.py` - Core implementation
- `src/mcp_polygon/tools/*.py` - All tool files using parallel fetcher
- `tests/test_parallel_fetcher.py` - Unit tests
- `scripts/` - Conversion and utility scripts

## References

- [Polygon.io API Docs](https://polygon.io/docs)
- [Python asyncio](https://docs.python.org/3/library/asyncio.html)
- [ThreadPoolExecutor](https://docs.python.org/3/library/concurrent.futures.html)
