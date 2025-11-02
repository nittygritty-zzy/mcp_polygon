"""
Earnings short setup screener.

Identifies high-risk/high-reward earnings trading opportunities based on
short selling positioning patterns (acceleration, deceleration, reversal).
"""

import asyncio
from typing import Any, Dict, List, Optional
from datetime import datetime

from mcp.types import ToolAnnotations

from ..clients import poly_mcp
from ..tool_integration import process_tool_response

from .common import validate_fundamentals, format_screener_results
from .common.earnings_helpers import (
    fetch_earnings_calendar,
    filter_upcoming_earnings,
    fetch_short_volume_trends,
    analyze_short_pattern,
    classify_short_scenario,
)


@poly_mcp.tool(annotations=ToolAnnotations(readOnlyHint=True))
async def screen_earnings_short_setup(
    alpha_vantage_api_key: Optional[str] = None,
    earnings_window_days: int = 21,
    min_short_volume_ratio: float = 55.0,
    min_market_cap: float = 50_000_000.0,
    require_profitability: bool = False,
    max_debt_to_equity: float = 3.0,
    max_results: int = 50,
    fetch_all: Optional[bool] = True,
    params: Optional[Dict[str, Any]] = None,
) -> str:
    """
    Earnings trading screener based on short selling positioning patterns.

    Identifies stocks with upcoming earnings (next 2-4 weeks) and analyzes
    short volume trends to classify trading setups: high buildup (straddle),
    declining shorts (bullish bias), or steady (fundamentals-driven).

    **Key Features:**
    - **Pattern Recognition**: Detects acceleration, deceleration, reversal patterns
    - **Trading Scenarios**: Maps patterns to actionable setups (straddle, bullish, bearish)
    - **Comprehensive Data**: Combines Alpha Vantage earnings + Polygon short volume

    **Methodology:**
    1. Fetch upcoming earnings calendar (Alpha Vantage)
    2. Fetch 30-day short volume history for each candidate
    3. Analyze short pattern (acceleration/deceleration/reversal/steady)
    4. Classify trading scenario and recommended setup
    5. Validate fundamentals (optional but recommended)
    6. Score and rank by setup quality
    7. Cache results to Parquet for DuckDB analysis

    **Parameters:**
    - alpha_vantage_api_key: Alpha Vantage API key (defaults to ALPHA_VANTAGE_API_KEY env var)
      - Required for earnings calendar data
      - Free tier: 25 requests/day
      - Get key at: https://www.alphavantage.co/support/#api-key

    - earnings_window_days: Days ahead to scan for earnings (default: 21 = 3 weeks)
      - Range: 7-60 days
      - Shorter window = more imminent setups
      - Longer window = more candidates but less urgent

    - min_short_volume_ratio: Minimum 10-day avg short volume ratio (default: 55%)
      - Higher threshold = more extreme short positioning
      - Typical values: 50-55 (moderate), 55-65 (high), 65+ (extreme)

    - min_market_cap: Minimum market capitalization (default: $50M)
      - Ensures some liquidity for earnings trades
      - Avoid micro-caps with extreme volatility

    - require_profitability: Filter to EPS > 0 (default: False)
      - Earnings trades focus on volatility, not necessarily quality
      - Set True for conservative approach (avoid unprofitable companies)

    - max_debt_to_equity: Maximum leverage ratio (default: 3.0)
      - More lenient than short squeeze screener (2.0)
      - Earnings trades are short-term, less concerned with bankruptcy risk
      - Set lower (e.g., 2.0) for conservative approach

    - max_results: Maximum candidates to return (default: 50)
      - Top N ranked by earnings_score
      - Lower for focused watchlist, higher for comprehensive scan

    - fetch_all: Cache full results to Parquet (default: True)
      - Recommended for DuckDB analysis
      - Allows tracking pattern evolution over time

    **Trading Scenarios (Pattern → Setup):**

    1. **High Buildup** (Acceleration >55% avg)
       - Pattern: Short volume accelerating upward (trend slope >+1.5%/day)
       - Logic: Heavy shorting → High volatility expected
       - Setup: **Straddle** (buy call + put, profit from volatility)
       - Example: NFLX pre-earnings with 58% avg short volume, rising trend

    2. **Declining Shorts** (Deceleration <45% avg)
       - Pattern: Short volume declining (trend slope <-1.5%/day)
       - Logic: Shorts unwinding → Less downside pressure
       - Setup: **Bullish if beat** (if fundamentals strong)
       - Example: TSLA Q3 2023 with 41% avg short volume, low buildup

    3. **Reversal Up** (Shorts covering)
       - Pattern: High→Low reversal in last 5 days
       - Logic: Short covering already underway
       - Setup: **Bullish directional** (buy calls or stock)

    4. **Reversal Down** (Shorts building)
       - Pattern: Low→High reversal in last 5 days
       - Logic: Shorts aggressively building positions
       - Setup: **Bearish or protective puts**

    5. **Normal** (Steady 45-55% range)
       - Pattern: Stable trend, low volatility
       - Logic: No significant short bias
       - Setup: **Fundamentals-driven** (short data non-factor)

    **Returns:**
    CSV with columns:
    - ticker: Stock symbol
    - earnings_date: Scheduled earnings date (YYYY-MM-DD)
    - days_until_earnings: Days from today to earnings
    - short_pattern_type: acceleration | deceleration | reversal_up | reversal_down | steady
    - short_volume_10d_avg: 10-day average short volume ratio (%)
    - short_trend_slope: Trend slope (% change per day)
    - scenario: high_buildup | declining_shorts | reversal_* | normal
    - trade_setup: straddle | bullish_if_beat | bullish_directional | bearish_or_puts | fundamentals_only
    - current_price: Stock price
    - market_cap: Market capitalization
    - eps: Earnings per share (TTM)
    - debt_to_equity: Leverage ratio
    - earnings_score: Composite score (0-100, higher = better setup)
    - rationale: Summary of pattern + scenario + setup

    **Example Usage:**
    ```python
    # Basic scan (recommended defaults)
    screen_earnings_short_setup()

    # Conservative scan (strict fundamentals)
    screen_earnings_short_setup(
        earnings_window_days=14,  # Next 2 weeks only
        require_profitability=True,
        max_debt_to_equity=2.0
    )

    # Aggressive scan (high short activity)
    screen_earnings_short_setup(
        min_short_volume_ratio=65.0,  # Extreme short positioning
        earnings_window_days=7,       # Imminent earnings only
        max_results=20
    )

    # Custom window (next 30 days)
    screen_earnings_short_setup(
        earnings_window_days=30,
        min_market_cap=100_000_000  # Large caps only
    )
    ```

    **Performance:**
    - Typical runtime: 40-60 seconds (first run, no cache)
    - Typical runtime: 10-20 seconds (subsequent runs with cache)
    - API calls: Alpha Vantage (1) + Polygon short volume (N tickers)
    - Data sources: Alpha Vantage earnings calendar, Polygon FINRA short volume (T+1)

    **Important Notes:**
    - **Alpha Vantage Key Required**: Set ALPHA_VANTAGE_API_KEY environment variable
    - **Free Tier Limit**: 25 requests/day on Alpha Vantage (cache earnings data daily)
    - **Earnings Dates May Change**: Companies can reschedule; re-run daily for updates
    - **Short Volume Lag**: T+1 lag (yesterday's data available today)
    - **Pattern Strength**: Low pattern_strength (<60/100) may indicate noise, not signal
    - **Risk Management**: Use stop losses! Earnings can move against setup

    **Real-World Examples (from spec):**

    *Tesla Q3 2023:*
    - Earnings: October 18, 2023
    - Pre-earnings short volume (Sep 25 - Oct 18): 41% avg (Low)
    - Short interest: 3.1% of float (Low)
    - Scenario: declining_shorts
    - Actual result: Beat earnings → +12% (purely fundamental move, no squeeze amplification)

    *Tesla Q1 2023:*
    - Pre-earnings short volume: 55% avg (High)
    - Short interest: 4.8% of float
    - Scenario: high_buildup
    - Actual result: Beat earnings → +10% fundamental + short squeeze → +18% total

    **Related Tools:**
    - list_short_volume(ticker, date_gte) - Check daily short volume trend
    - list_short_interest(ticker) - Bi-monthly short interest positioning
    - get_earnings_calendar_alpha_vantage() - Direct earnings calendar access
    - list_ticker_news(ticker) - Check recent news for catalysts
    - duckdb_query() - Analyze cached screener results
    """
    try:
        # Step 1: Fetch upcoming earnings calendar
        earnings_data = await fetch_earnings_calendar(
            alpha_vantage_api_key=alpha_vantage_api_key,
            horizon="3month",
        )

        # Filter to earnings window
        upcoming_earnings = filter_upcoming_earnings(
            earnings_data,
            min_days_ahead=0,
            max_days_ahead=earnings_window_days,
        )

        if not upcoming_earnings:
            return f"No earnings found in next {earnings_window_days} days"

        # Step 2: Fetch short volume trends for all candidates
        tickers = [e["symbol"] for e in upcoming_earnings]
        short_volume_data = await fetch_short_volume_trends(
            tickers=tickers,
            lookback_days=30,
        )

        if not short_volume_data:
            return "No short volume data found for earnings candidates"

        # Step 3: Analyze short patterns and classify scenarios
        candidates = []
        for event in upcoming_earnings:
            ticker = event["symbol"]
            if ticker not in short_volume_data:
                continue

            # Analyze pattern
            pattern = analyze_short_pattern(short_volume_data[ticker])

            # Skip if insufficient data or pattern strength too low
            if pattern["pattern_type"] == "insufficient_data":
                continue
            if pattern["pattern_strength"] < 30.0:  # Minimum clarity threshold
                continue

            # Classify scenario
            scenario, trade_setup = classify_short_scenario(pattern)

            # Filter by min_short_volume_ratio
            if pattern["current_avg"] < min_short_volume_ratio:
                continue

            # Build candidate
            candidates.append(
                {
                    "ticker": ticker,
                    "earnings_date": event["reportDate"],
                    "days_until_earnings": event["days_until_earnings"],
                    "short_pattern_type": pattern["pattern_type"],
                    "short_volume_10d_avg": pattern["current_avg"],
                    "short_trend_slope": pattern["trend_slope"],
                    "pattern_strength": pattern["pattern_strength"],
                    "volatility": pattern["volatility"],
                    "scenario": scenario,
                    "trade_setup": trade_setup,
                }
            )

        if not candidates:
            return f"No candidates found with short_volume_ratio >= {min_short_volume_ratio}%"

        # Step 4: Validate fundamentals (optional but recommended)
        validated_candidates = await validate_fundamentals(
            candidates=candidates,
            min_market_cap=min_market_cap,
            require_profitability=require_profitability,
            require_positive_fcf=False,  # Not critical for earnings trades
            max_debt_to_equity=max_debt_to_equity,
        )

        if not validated_candidates:
            return "No candidates passed fundamental validation (check market cap, profitability, leverage filters)"

        # Step 5: Score and rank candidates
        scored_candidates = _score_and_rank(
            candidates=validated_candidates,
            max_results=max_results,
        )

        # Step 6: Format output
        output_columns = [
            "ticker",
            "earnings_date",
            "days_until_earnings",
            "short_pattern_type",
            "short_volume_10d_avg",
            "short_trend_slope",
            "scenario",
            "trade_setup",
            "price",
            "market_cap",
            "eps",
            "debt_to_equity",
            "earnings_score",
            "rationale",
        ]

        csv_data = format_screener_results(
            scored_candidates,
            output_columns,
            "No earnings short setup candidates found",
        )

        # Step 7: Cache results if requested
        if fetch_all:
            tool_params = {
                "earnings_window_days": earnings_window_days,
                "min_short_volume_ratio": min_short_volume_ratio,
                "scan_date": datetime.now().strftime("%Y-%m-%d"),
            }
            return await process_tool_response(
                "screen_earnings_short_setup",
                tool_params,
                csv_data,
            )

        return csv_data

    except asyncio.CancelledError:
        raise
    except Exception as e:
        return f"Error screening earnings short setups: {e}"


def _score_and_rank(
    candidates: List[Dict[str, Any]],
    max_results: int,
) -> List[Dict[str, Any]]:
    """
    Score and rank earnings trading setups.

    **Scoring Algorithm:**
    earnings_score = (
        pattern_strength * 0.40 +      # Pattern clarity (0-100)
        earnings_proximity * 0.25 +    # Closer to earnings = higher score
        fundamental_quality * 0.20 +   # Profitability, leverage, market cap
        short_trend_magnitude * 0.15   # How extreme is the short activity
    )

    **Parameters:**
    - candidates: Validated candidates with pattern and fundamental data
    - max_results: Top N to return

    **Returns:**
    Sorted list of top N candidates with earnings_score and rationale
    """
    scored = []

    for candidate in candidates:
        # Pattern strength (already 0-100 from analyze_short_pattern)
        pattern_strength = candidate.get("pattern_strength", 0.0)

        # Earnings proximity (closer = higher score)
        days_until = candidate.get("days_until_earnings", 30)
        earnings_proximity = max(
            0, 100 * (1 - days_until / 30)
        )  # 0 days = 100, 30 days = 0

        # Fundamental quality (normalized 0-100)
        eps = candidate.get("eps", 0.0)
        debt_to_equity = candidate.get("debt_to_equity", 3.0)
        market_cap = candidate.get("market_cap", 0.0)

        profitability_score = 100 if eps > 0 else 0
        leverage_score = max(
            0, 100 * (1 - debt_to_equity / 5.0)
        )  # 0 D/E = 100, 5 D/E = 0
        size_score = min(100, (market_cap / 1_000_000_000) * 50)  # $1B = 50, $2B = 100

        fundamental_quality = (profitability_score + leverage_score + size_score) / 3

        # Short trend magnitude (how extreme is the short activity)
        short_avg = candidate.get("short_volume_10d_avg", 50.0)
        trend_slope = abs(candidate.get("short_trend_slope", 0.0))

        # Normalize: 50-70% avg → 0-100, >70% = 100
        avg_score = min(100, ((short_avg - 50) / 20) * 100)
        # Normalize: 0-5% slope → 0-100, >5% = 100
        slope_score = min(100, (trend_slope / 5.0) * 100)

        short_trend_magnitude = (avg_score + slope_score) / 2

        # Compute composite score
        earnings_score = (
            pattern_strength * 0.40
            + earnings_proximity * 0.25
            + fundamental_quality * 0.20
            + short_trend_magnitude * 0.15
        )

        # Build rationale
        pattern_type = candidate.get("short_pattern_type", "unknown")
        scenario = candidate.get("scenario", "unknown")
        trade_setup = candidate.get("trade_setup", "unknown")
        days_str = f"{days_until}d"
        avg_str = f"{short_avg:.1f}%"

        rationale = (
            f"{pattern_type} pattern | {scenario} | {trade_setup} | "
            f"{days_str} to earnings | {avg_str} avg SV"
        )

        candidate["earnings_score"] = round(earnings_score, 1)
        candidate["rationale"] = rationale

        scored.append(candidate)

    # Sort by earnings_score descending
    scored.sort(key=lambda x: x["earnings_score"], reverse=True)

    return scored[:max_results]
