"""
Earnings-specific utilities for earnings trading screeners.

Provides functions for:
- Fetching earnings calendar from Alpha Vantage
- Analyzing short volume patterns (acceleration/deceleration/reversal)
- Classifying short scenarios for trading setups
"""

import os
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Tuple

import numpy as np
import pandas as pd
from scipy import stats

from ...clients import polygon_client


async def fetch_earnings_calendar(
    alpha_vantage_api_key: Optional[str] = None,
    horizon: str = "3month",
) -> List[Dict[str, Any]]:
    """
    Fetch earnings calendar from Alpha Vantage API.

    **Parameters:**
    - alpha_vantage_api_key: Alpha Vantage API key (defaults to env var)
    - horizon: Time window ("3month", "6month", "12month")

    **Returns:**
    List of earnings events with keys: symbol, name, reportDate, estimate
    """
    import requests
    import io
    import csv

    # Get API key from parameter or environment
    api_key = alpha_vantage_api_key or os.getenv("ALPHA_VANTAGE_API_KEY")
    if not api_key:
        raise ValueError(
            "Alpha Vantage API key required. Set ALPHA_VANTAGE_API_KEY environment variable or pass as parameter."
        )

    # Alpha Vantage earnings calendar endpoint
    url = "https://www.alphavantage.co/query"
    params = {
        "function": "EARNINGS_CALENDAR",
        "horizon": horizon,
        "apikey": api_key,
    }

    response = requests.get(url, params=params, timeout=30)
    response.raise_for_status()

    # Parse CSV response
    earnings_list = []
    csv_data = io.StringIO(response.text)
    reader = csv.DictReader(csv_data)

    for row in reader:
        earnings_list.append(
            {
                "symbol": row.get("symbol", ""),
                "name": row.get("name", ""),
                "reportDate": row.get("reportDate", ""),
                "fiscalDateEnding": row.get("fiscalDateEnding", ""),
                "estimate": float(row.get("estimate", 0.0) or 0.0),
                "currency": row.get("currency", "USD"),
            }
        )

    return earnings_list


def filter_upcoming_earnings(
    earnings_list: List[Dict[str, Any]],
    min_days_ahead: int = 0,
    max_days_ahead: int = 21,
) -> List[Dict[str, Any]]:
    """
    Filter earnings calendar to upcoming window.

    **Parameters:**
    - earnings_list: Output from fetch_earnings_calendar()
    - min_days_ahead: Minimum days until earnings (default: 0 = today)
    - max_days_ahead: Maximum days until earnings (default: 21 = 3 weeks)

    **Returns:**
    Filtered list with 'days_until_earnings' field added
    """
    today = datetime.now().date()
    filtered = []

    for event in earnings_list:
        report_date_str = event.get("reportDate", "")
        if not report_date_str:
            continue

        try:
            report_date = datetime.strptime(report_date_str, "%Y-%m-%d").date()
        except ValueError:
            continue

        days_until = (report_date - today).days

        if min_days_ahead <= days_until <= max_days_ahead:
            event["days_until_earnings"] = days_until
            filtered.append(event)

    return filtered


async def fetch_short_volume_trends(
    tickers: List[str],
    lookback_days: int = 30,
) -> Dict[str, pd.DataFrame]:
    """
    Fetch short volume history for multiple tickers.

    **Parameters:**
    - tickers: List of ticker symbols
    - lookback_days: Number of days to fetch (default: 30)

    **Returns:**
    Dict mapping ticker -> DataFrame with columns: date, short_volume_ratio
    """
    import asyncio

    today = datetime.now().date()
    start_date = today - timedelta(days=lookback_days)

    results = {}

    # Fetch in batches to avoid overwhelming API
    batch_size = 50
    for i in range(0, len(tickers), batch_size):
        batch = tickers[i : i + batch_size]

        # Create tasks for parallel fetching
        tasks = []
        for ticker in batch:
            params = {
                "ticker": ticker,
                "date_gte": start_date.strftime("%Y-%m-%d"),
                "limit": 50,
            }
            tasks.append(_fetch_single_ticker_short_volume(ticker, params))

        # Execute batch in parallel
        batch_results = await asyncio.gather(*tasks, return_exceptions=True)

        for ticker, df in zip(batch, batch_results):
            if isinstance(df, Exception):
                continue
            if df is not None and not df.empty:
                results[ticker] = df

    return results


async def _fetch_single_ticker_short_volume(
    ticker: str, params: Dict[str, Any]
) -> Optional[pd.DataFrame]:
    """Helper to fetch short volume for single ticker."""
    import json

    try:
        response = polygon_client._get(
            "/stocks/fundamentals/short-volume",
            params=params,
        )

        if isinstance(response, bytes):
            response_data = json.loads(response.decode("utf-8"))
        else:
            response_data = response

        results = response_data.get("results", [])
        if not results:
            return None

        # Convert to DataFrame
        df_data = []
        for item in results:
            df_data.append(
                {
                    "date": item.get("date"),
                    "short_volume": item.get("short_volume", 0),
                    "total_volume": item.get("total_volume", 1),
                    "short_volume_ratio": item.get("short_volume_ratio", 0.0),
                }
            )

        df = pd.DataFrame(df_data)
        df["date"] = pd.to_datetime(df["date"])
        df = df.sort_values("date")

        return df

    except Exception:
        return None


def analyze_short_pattern(
    short_volume_df: pd.DataFrame,
) -> Dict[str, Any]:
    """
    Analyze short volume pattern to detect acceleration/deceleration/reversal.

    **Pattern Types:**
    1. Acceleration: Upward trend with slope > +1.5%/day
    2. Deceleration: Downward trend with slope < -1.5%/day
    3. Reversal: Direction change in last 5 days
    4. Steady: Stable trend (-1.5% to +1.5% slope)

    **Parameters:**
    - short_volume_df: DataFrame with columns: date, short_volume_ratio

    **Returns:**
    Dict with keys:
    - pattern_type: "acceleration" | "deceleration" | "reversal_up" | "reversal_down" | "steady"
    - current_avg: 10-day average short volume ratio
    - trend_slope: % change per day (linear regression)
    - volatility: Standard deviation of ratios
    - days_in_pattern: Number of days analyzed
    - pattern_strength: 0-100 score (clarity of pattern)
    """
    if short_volume_df.empty or len(short_volume_df) < 5:
        return {
            "pattern_type": "insufficient_data",
            "current_avg": 0.0,
            "trend_slope": 0.0,
            "volatility": 0.0,
            "days_in_pattern": 0,
            "pattern_strength": 0.0,
        }

    # Get last 10 days for moving average
    last_10 = short_volume_df.tail(10).copy()
    last_5 = short_volume_df.tail(5).copy()

    # Calculate 10-day average
    current_avg = last_10["short_volume_ratio"].mean()

    # Calculate trend slope using linear regression
    x = np.arange(len(last_10))
    y = last_10["short_volume_ratio"].values
    slope, intercept, r_value, p_value, std_err = stats.linregress(x, y)

    # Convert slope to % change per day
    trend_slope = slope  # Already in percentage points per day

    # Calculate volatility
    volatility = last_10["short_volume_ratio"].std()

    # Detect reversal in last 5 days
    reversal_detected = False
    reversal_direction = None
    if len(last_10) >= 10 and len(last_5) >= 5:
        # Compare first 5 and last 5 days
        first_5 = last_10.head(5)
        first_5_slope = stats.linregress(
            np.arange(len(first_5)), first_5["short_volume_ratio"].values
        )[0]
        last_5_slope = stats.linregress(
            np.arange(len(last_5)), last_5["short_volume_ratio"].values
        )[0]

        # Reversal if signs differ and magnitude > 1.0
        if first_5_slope > 1.0 and last_5_slope < -1.0:
            reversal_detected = True
            reversal_direction = "down"
        elif first_5_slope < -1.0 and last_5_slope > 1.0:
            reversal_detected = True
            reversal_direction = "up"

    # Classify pattern type
    if reversal_detected:
        pattern_type = f"reversal_{reversal_direction}"
    elif trend_slope > 1.5:
        pattern_type = "acceleration"
    elif trend_slope < -1.5:
        pattern_type = "deceleration"
    else:
        pattern_type = "steady"

    # Calculate pattern strength (0-100)
    # Higher R² = clearer trend = higher strength
    # Lower volatility = more consistent = higher strength
    r_squared = r_value**2
    volatility_penalty = min(volatility / 10.0, 1.0)  # Cap at 10% volatility

    pattern_strength = (r_squared * 70 + (1 - volatility_penalty) * 30) * 100

    return {
        "pattern_type": pattern_type,
        "current_avg": round(current_avg, 2),
        "trend_slope": round(trend_slope, 2),
        "volatility": round(volatility, 2),
        "days_in_pattern": len(last_10),
        "pattern_strength": round(pattern_strength, 1),
    }


def classify_short_scenario(
    pattern: Dict[str, Any],
) -> Tuple[str, str]:
    """
    Classify short pattern into trading scenario with recommended setup.

    **Scenarios:**
    1. high_buildup: Acceleration pattern with avg >55%
       - Setup: Straddle (volatility play)
       - Logic: Heavy shorting → High volatility expected

    2. declining_shorts: Deceleration pattern with avg <45%
       - Setup: Bullish bias if fundamentals strong
       - Logic: Shorts unwinding → Less downside pressure

    3. reversal_up: Reversal from high to low
       - Setup: Bullish directional
       - Logic: Short covering underway

    4. reversal_down: Reversal from low to high
       - Setup: Bearish directional or protective puts
       - Logic: Shorts building positions

    5. normal: Steady pattern 45-55% range
       - Setup: Fundamentals-driven, short data non-factor
       - Logic: No significant short bias

    **Parameters:**
    - pattern: Output from analyze_short_pattern()

    **Returns:**
    Tuple of (scenario, trade_setup)
    """
    pattern_type = pattern["pattern_type"]
    current_avg = pattern["current_avg"]

    if pattern_type == "acceleration" and current_avg > 55:
        return "high_buildup", "straddle"

    elif pattern_type == "deceleration" and current_avg < 45:
        return "declining_shorts", "bullish_if_beat"

    elif pattern_type == "reversal_up":
        return "reversal_shorts_covering", "bullish_directional"

    elif pattern_type == "reversal_down":
        return "reversal_shorts_building", "bearish_or_puts"

    elif pattern_type == "steady":
        return "normal", "fundamentals_only"

    elif pattern_type == "acceleration":
        return "moderate_buildup", "slight_volatility"

    elif pattern_type == "deceleration":
        return "moderate_decline", "slight_bullish"

    else:
        return "unknown", "avoid"
