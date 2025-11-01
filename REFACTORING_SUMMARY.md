# Refactoring Summary: Better Fix for None Parameter Handling

## Problem
The original error occurred when `list_short_interest` and `list_short_volume` were called with `fetch_all=True` and `ticker=None`:

```
Error: unsupported operand type(s) for /: 'PosixPath' and 'NoneType'
```

## Root Cause
Tool functions were building `tool_params` dictionaries with `None` values:

```python
tool_params = {
    "ticker": ticker,  # Could be None!
    "limit": limit,
    "fetch_all": fetch_all,
}
```

When `ticker=None`, the dictionary had a key with a `None` value, and `params.get("ticker", "all")` would return `None` instead of the default `"all"`, causing path construction errors.

## First Attempt (Defensive)
Changed `cache_manager.py` to use:
```python
ticker = params.get("ticker") or "all"
```

**Issues:**
- Treats symptom, not root cause
- Uses truthiness (empty strings would also default)
- Defensive code throughout cache_manager.py
- Not maintainable

## Better Solution (Root Cause Fix)
Created a `build_params()` utility to filter out `None` values at the source:

### 1. Added `utils.py`
```python
def build_params(**kwargs) -> Dict[str, Any]:
    """Build a parameters dictionary excluding None values."""
    return {k: v for k, v in kwargs.items() if v is not None}
```

### 2. Updated Tool Functions
In `tools/financials.py`:
```python
from ..utils import build_params

# Before
tool_params = {
    "ticker": ticker,
    "limit": limit,
    "fetch_all": fetch_all,
}

# After
tool_params = build_params(
    ticker=ticker,
    limit=limit,
    fetch_all=fetch_all,
)
```

### 3. Reverted Cache Manager
`cache_manager.py` back to clean, original pattern:
```python
ticker = params.get("ticker", "all")  # Works correctly now!
```

## Benefits

1. **Fixes root cause**: No `None` values in params dictionaries
2. **Cleaner code**: `cache_manager.py` uses standard `dict.get()` with defaults
3. **More maintainable**: One utility function instead of defensive checks everywhere
4. **Explicit**: Clearly shows intent to filter `None` values
5. **Preserves falsy values**: Keeps `""`, `0`, `False` but filters `None`

## Files Changed

- **Created**: `src/mcp_polygon/utils.py` (new utility module)
- **Updated**: `src/mcp_polygon/tools/financials.py` (7 tools use `build_params`)
- **Reverted**: `src/mcp_polygon/cache_manager.py` (back to clean pattern)
- **Updated**: `tests/test_short_data_fix.py` (9 comprehensive tests)

## Test Results

All 82 tests pass, including:
- 9 new tests for `build_params` utility
- All existing tests still pass
- No regressions

## Future Work

Consider applying `build_params()` to other tool files:
- `tools/aggregates.py`
- `tools/options.py`
- `tools/corporate_actions.py`
- `tools/technical_indicators.py`
- etc.

This will ensure consistent handling of `None` parameters across all tools.
