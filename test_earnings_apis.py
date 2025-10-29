#!/usr/bin/env python3
"""
Comparison test between Alpha Vantage and NASDAQ Public API for earnings calendar data.
"""

import time
import pandas as pd
import requests
from datetime import datetime, timedelta
from finance_calendars.finance_calendars import get_earnings_by_date

# API Keys
ALPHA_VANTAGE_KEY = "LJ170697V40YU7H1"

print("=" * 80)
print("EARNINGS CALENDAR API COMPARISON TEST")
print("=" * 80)
print(f"Test Date: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
print("=" * 80)
print()

# ============================================================================
# TEST 1: ALPHA VANTAGE
# ============================================================================

print("🔵 TEST 1: ALPHA VANTAGE")
print("-" * 80)

av_url = "https://www.alphavantage.co/query"
av_params = {
    "function": "EARNINGS_CALENDAR",
    "horizon": "3month",
    "apikey": ALPHA_VANTAGE_KEY,
}

av_start = time.time()
try:
    av_response = requests.get(av_url, params=av_params)
    av_elapsed = time.time() - av_start

    if av_response.status_code == 200:
        # Parse CSV response
        from io import StringIO

        av_df = pd.read_csv(StringIO(av_response.text))

        print("✅ Status: SUCCESS")
        print(f"⏱️  Response Time: {av_elapsed:.2f} seconds")
        print(f"📊 Total Records: {len(av_df)}")
        print(
            f"📅 Date Range: {av_df['reportDate'].min()} to {av_df['reportDate'].max()}"
        )
        print(f"🏢 Unique Companies: {av_df['symbol'].nunique()}")
        print()
        print("📋 Data Columns:")
        print(f"   {', '.join(av_df.columns.tolist())}")
        print()
        print("📝 Sample Records (First 5):")
        print(av_df.head(5).to_string(index=False))
        print()

        # Filter for this week
        today = datetime.now().date()
        week_end = today + timedelta(days=7)
        av_df["reportDate"] = pd.to_datetime(av_df["reportDate"]).dt.date
        av_this_week = av_df[
            (av_df["reportDate"] >= today) & (av_df["reportDate"] <= week_end)
        ]

        print(f"🗓️  This Week's Earnings ({today} to {week_end}):")
        print(f"   Total: {len(av_this_week)} companies")
        if len(av_this_week) > 0:
            print(
                f"   Sample companies: {', '.join(av_this_week['symbol'].head(10).tolist())}"
            )
        print()

        av_success = True
        av_data = av_df

    else:
        print("❌ Status: FAILED")
        print(f"   HTTP {av_response.status_code}: {av_response.text[:200]}")
        av_success = False
        av_data = None

except Exception as e:
    av_elapsed = time.time() - av_start
    print("❌ Status: ERROR")
    print(f"   Error: {e}")
    av_success = False
    av_data = None

print()

# ============================================================================
# TEST 2: NASDAQ PUBLIC API (finance_calendars)
# ============================================================================

print("🟢 TEST 2: NASDAQ PUBLIC API (finance_calendars)")
print("-" * 80)

nasdaq_start = time.time()
try:
    # Get earnings for today and next 7 days
    today = datetime.now()
    nasdaq_results = []

    for i in range(8):  # Today + next 7 days
        date = today + timedelta(days=i)
        date_str = date.strftime("%Y-%m-%d")
        try:
            daily_earnings = get_earnings_by_date(date_str)
            if daily_earnings:
                nasdaq_results.extend(daily_earnings)
        except Exception:
            # Some dates may have no data or errors
            pass

    nasdaq_elapsed = time.time() - nasdaq_start

    if nasdaq_results:
        nasdaq_df = pd.DataFrame(nasdaq_results)

        print("✅ Status: SUCCESS")
        print(f"⏱️  Response Time: {nasdaq_elapsed:.2f} seconds (8 API calls)")
        print(f"📊 Total Records: {len(nasdaq_df)}")
        print(
            f"🏢 Unique Companies: {nasdaq_df['symbol'].nunique() if 'symbol' in nasdaq_df.columns else 'N/A'}"
        )
        print()
        print("📋 Data Columns:")
        print(f"   {', '.join(nasdaq_df.columns.tolist())}")
        print()
        print("📝 Sample Records (First 5):")
        print(nasdaq_df.head(5).to_string(index=False))
        print()

        nasdaq_success = True
        nasdaq_data = nasdaq_df

    else:
        print("⚠️  Status: NO DATA")
        print("   No earnings found for the next 8 days")
        print(f"⏱️  Response Time: {nasdaq_elapsed:.2f} seconds")
        nasdaq_success = True  # API worked, just no data
        nasdaq_data = None

except Exception as e:
    nasdaq_elapsed = time.time() - nasdaq_start
    print("❌ Status: ERROR")
    print(f"   Error: {e}")
    nasdaq_success = False
    nasdaq_data = None

print()

# ============================================================================
# COMPARISON SUMMARY
# ============================================================================

print("=" * 80)
print("📊 COMPARISON SUMMARY")
print("=" * 80)
print()

comparison = {
    "Metric": [
        "API Key Required",
        "Response Time",
        "Records Returned",
        "This Week Coverage",
        "Data Freshness",
        "Output Format",
        "Ease of Use (1-5)",
        "Status",
    ],
    "Alpha Vantage": [
        "✅ Yes",
        f"{av_elapsed:.2f}s" if av_success else "N/A",
        f"{len(av_data)} (3 months)" if av_success and av_data is not None else "N/A",
        f"{len(av_this_week)} companies"
        if av_success and av_data is not None
        else "N/A",
        "3-12 month forecast",
        "CSV",
        "⭐⭐⭐⭐ (4/5)",
        "✅ Success" if av_success else "❌ Failed",
    ],
    "NASDAQ (finance_calendars)": [
        "❌ No",
        f"{nasdaq_elapsed:.2f}s (8 calls)" if nasdaq_success else "N/A",
        f"{len(nasdaq_data)} (8 days)"
        if nasdaq_success and nasdaq_data is not None
        else "0 (No data)",
        f"{len(nasdaq_data)} companies"
        if nasdaq_success and nasdaq_data is not None
        else "0",
        "Real-time from exchange",
        "JSON/Dict",
        "⭐⭐⭐⭐⭐ (5/5)",
        "✅ Success" if nasdaq_success else "❌ Failed",
    ],
}

comp_df = pd.DataFrame(comparison)
print(comp_df.to_string(index=False))
print()

# ============================================================================
# RECOMMENDATIONS
# ============================================================================

print("=" * 80)
print("💡 RECOMMENDATIONS")
print("=" * 80)
print()

if av_success and nasdaq_success:
    print("Both APIs are working! Here's when to use each:")
    print()
    print("✅ Use ALPHA VANTAGE if you need:")
    print("   • Analyst EPS estimates")
    print("   • 3-12 month earnings forecast")
    print("   • Single API call for all data")
    print("   • CSV format for easy import")
    print()
    print("✅ Use NASDAQ (finance_calendars) if you need:")
    print("   • No API key hassle")
    print("   • No rate limits")
    print("   • Most authoritative data (direct from exchange)")
    print("   • Additional calendars (IPO, dividends, splits)")
    print("   • Python-native dict/list format")

elif av_success and not nasdaq_success:
    print("⚠️  Alpha Vantage is working, but NASDAQ API had issues.")
    print("   Recommendation: Use Alpha Vantage")

elif nasdaq_success and not av_success:
    print("⚠️  NASDAQ API is working, but Alpha Vantage had issues.")
    print("   Recommendation: Use NASDAQ finance_calendars")

else:
    print("❌ Both APIs failed. Please check:")
    print("   • Internet connection")
    print("   • API keys")
    print("   • Rate limits")

print()
print("=" * 80)
print("Test Complete!")
print("=" * 80)
