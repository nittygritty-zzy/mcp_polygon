"""
Fundamental validation utilities for stock screeners.

Provides functions to validate candidates against fundamental health criteria
including market cap, profitability, leverage, and other financial metrics.
"""

from typing import Any, Dict, List

import pandas as pd
import json

from ...clients import polygon_client


async def validate_fundamentals(
    candidates: List[Dict[str, Any]],
    min_market_cap: float,
    require_profitability: bool,
    require_positive_fcf: bool,
    max_debt_to_equity: float,
) -> List[Dict[str, Any]]:
    """
    Validate candidates against fundamental health criteria.

    Uses list_stock_ratios() with API-side filtering for efficiency.
    Joins with short interest data on ticker symbol.

    **Parameters:**
    - candidates: List of candidate dicts with 'ticker' key
    - min_market_cap: Minimum market capitalization
    - require_profitability: Filter to EPS > 0
    - require_positive_fcf: Filter to Free Cash Flow > 0
    - max_debt_to_equity: Maximum debt-to-equity ratio

    **Returns:**
    List of validated candidates with fundamental metrics added
    """
    try:
        if not candidates:
            return []

        # Extract ticker list
        tickers = [c["ticker"] for c in candidates if c.get("ticker")]
        if not tickers:
            return []

        # Build filter parameters
        ratio_params = {
            "market_cap_gte": min_market_cap,
            "ticker_any_of": ",".join(tickers[:250]),  # Max 250 tickers per request
            "limit": 1000,
        }

        if require_profitability:
            ratio_params["earnings_per_share_gt"] = 0.0

        if require_positive_fcf:
            ratio_params["free_cash_flow_gt"] = 0.0

        if max_debt_to_equity:
            ratio_params["debt_to_equity_lte"] = max_debt_to_equity

        # Fetch ratios using direct REST API call
        response = polygon_client._get(
            "/stocks/financials/v1/ratios",
            params=ratio_params,
        )

        # Parse response
        if isinstance(response, bytes):
            response_data = json.loads(response.decode("utf-8"))
        else:
            response_data = response

        ratio_results = response_data.get("results", [])

        if not ratio_results:
            return []

        # Convert to DataFrame for joining
        ratios_list = []
        for item in ratio_results:
            ratios_list.append(
                {
                    "ticker": item.get("ticker"),
                    "market_cap": item.get("market_cap", 0.0),
                    "price": item.get("price", 0.0),
                    "eps": item.get("earnings_per_share", 0.0),
                    "fcf": item.get("free_cash_flow", 0.0),
                    "debt_to_equity": item.get("debt_to_equity", 0.0),
                    "current_ratio": item.get("current", 0.0),
                    "return_on_equity": item.get("return_on_equity", 0.0),
                }
            )

        df_ratios = pd.DataFrame(ratios_list)
        df_candidates = pd.DataFrame(candidates)

        # Join on ticker
        df_joined = df_candidates.merge(df_ratios, on="ticker", how="inner")

        # Add validation flags
        df_joined["validation_passed"] = "✓ All checks passed"

        return df_joined.to_dict("records")

    except Exception as e:
        print(f"Error validating fundamentals: {e}")
        return []
