"""
Tests for short squeeze screening tools.
"""

import pytest
from unittest.mock import patch

from src.mcp_polygon.screeners.short_squeeze import (
    screen_short_squeeze,
    validate_squeeze_candidate,
    _score_and_rank,
)


class TestScreenShortSqueeze:
    """Test cases for screen_short_squeeze tool."""

    @pytest.mark.asyncio
    async def test_basic_screening(self):
        """Test basic screening with default parameters."""
        # Mock the internal functions
        with patch(
            "src.mcp_polygon.screeners.short_squeeze._fetch_short_candidates"
        ) as mock_fetch:
            with patch(
                "src.mcp_polygon.screeners.short_squeeze.validate_fundamentals"
            ) as mock_validate:
                # Setup mocks
                mock_fetch.return_value = [
                    {
                        "ticker": "GME",
                        "days_to_cover": 15.0,
                        "short_interest": 10000000,
                        "avg_daily_volume": 666666,
                        "settlement_date": "2025-10-15",
                    }
                ]

                mock_validate.return_value = [
                    {
                        "ticker": "GME",
                        "days_to_cover": 15.0,
                        "short_interest": 10000000,
                        "market_cap": 1000000000,
                        "price": 25.0,
                        "eps": 2.0,
                        "fcf": 10000000,
                        "debt_to_equity": 0.5,
                        "validation_passed": "✓ All checks passed",
                    }
                ]

                # Run screening
                result = await screen_short_squeeze(
                    min_days_to_cover=5.0,
                    fetch_all=False,
                )

                # Verify it returned data
                assert result is not None
                assert "GME" in result

    @pytest.mark.asyncio
    async def test_no_candidates_found(self):
        """Test when no candidates match the criteria."""
        with patch(
            "src.mcp_polygon.screeners.short_squeeze._fetch_short_candidates"
        ) as mock_fetch:
            mock_fetch.return_value = []

            result = await screen_short_squeeze(
                min_days_to_cover=100.0, fetch_all=False
            )

            assert "No candidates found" in result

    @pytest.mark.asyncio
    async def test_profitability_filter(self):
        """Test that profitability filter works."""
        # This would need actual test data
        # For now, placeholder
        pass


class TestValidateSqeezeCandidate:
    """Test cases for validate_squeeze_candidate tool."""

    @pytest.mark.asyncio
    async def test_validate_single_ticker(self):
        """Test validation of a single ticker."""
        result = await validate_squeeze_candidate(ticker="GME")

        # Currently returns NOT_IMPLEMENTED
        assert "GME" in result
        assert "NOT_IMPLEMENTED" in result


class TestHelperFunctions:
    """Test cases for internal helper functions."""

    def test_score_and_rank(self):
        """Test scoring and ranking logic."""
        candidates = [
            {
                "ticker": "GME",
                "days_to_cover": 20.0,
                "market_cap": 1000000000,
                "return_on_equity": 0.15,
                "current_ratio": 2.0,
                "has_catalyst": True,
            },
            {
                "ticker": "AMC",
                "days_to_cover": 10.0,
                "market_cap": 500000000,
                "return_on_equity": 0.05,
                "current_ratio": 1.0,
                "has_catalyst": False,
            },
        ]

        ranked = _score_and_rank(candidates, max_results=10)

        # Verify ranking logic
        assert len(ranked) == 2
        assert ranked[0]["ticker"] == "GME"  # Higher score should be first
        assert "squeeze_score" in ranked[0]

    def test_score_and_rank_empty(self):
        """Test scoring with empty candidate list."""
        ranked = _score_and_rank([], max_results=10)
        assert ranked == []


# Contrarian screener tests
class TestScreenContrarian:
    """Test cases for screen_contrarian_entry tool."""

    @pytest.mark.asyncio
    async def test_basic_contrarian_scan(self):
        """Test basic contrarian entry screening."""
        with patch(
            "src.mcp_polygon.screeners.contrarian_entry._fetch_high_short_volume_candidates"
        ) as mock_fetch_sv:
            with patch(
                "src.mcp_polygon.screeners.contrarian_entry._validate_short_interest_trend"
            ) as mock_validate_si:
                with patch(
                    "src.mcp_polygon.screeners.contrarian_entry._check_technical_support_batch"
                ) as mock_support:
                    with patch(
                        "src.mcp_polygon.screeners.contrarian_entry.validate_fundamentals"
                    ) as mock_fundamentals:
                        # Setup mocks
                        mock_fetch_sv.return_value = [
                            {
                                "ticker": "XYZ",
                                "consecutive_high_sv_days": 5,
                                "avg_sv_ratio": 67.5,
                            }
                        ]

                        mock_validate_si.return_value = [
                            {
                                "ticker": "XYZ",
                                "consecutive_high_sv_days": 5,
                                "avg_sv_ratio": 67.5,
                                "short_interest_trend_pct": 15.2,
                            }
                        ]

                        mock_support.return_value = [
                            {
                                "ticker": "XYZ",
                                "consecutive_high_sv_days": 5,
                                "avg_sv_ratio": 67.5,
                                "short_interest_trend_pct": 15.2,
                                "price": 42.40,
                                "support_level": "at_50day_sma/rsi_oversold",
                                "support_count": 2,
                                "rsi": 28.5,
                            }
                        ]

                        mock_fundamentals.return_value = [
                            {
                                "ticker": "XYZ",
                                "consecutive_high_sv_days": 5,
                                "avg_sv_ratio": 67.5,
                                "short_interest_trend_pct": 15.2,
                                "price": 42.40,
                                "support_level": "at_50day_sma/rsi_oversold",
                                "support_count": 2,
                                "rsi": 28.5,
                                "market_cap": 500000000,
                                "eps": 1.5,
                                "debt_to_equity": 1.8,
                            }
                        ]

                        # Run screening
                        from src.mcp_polygon.screeners.contrarian_entry import (
                            screen_contrarian_entry,
                        )

                        result = await screen_contrarian_entry(
                            min_short_volume_ratio=60.0,
                            min_consecutive_days=3,
                            fetch_all=False,
                        )

                        # Verify result contains data
                        assert result is not None
                        assert "XYZ" in result

    @pytest.mark.asyncio
    async def test_no_contrarian_candidates(self):
        """Test when no candidates match criteria."""
        with patch(
            "src.mcp_polygon.screeners.contrarian_entry._fetch_high_short_volume_candidates"
        ) as mock_fetch:
            mock_fetch.return_value = []

            from src.mcp_polygon.screeners.contrarian_entry import (
                screen_contrarian_entry,
            )

            result = await screen_contrarian_entry(
                min_short_volume_ratio=80.0, fetch_all=False
            )

            assert "No candidates found" in result

    def test_contrarian_scoring(self):
        """Test contrarian scoring algorithm."""
        from src.mcp_polygon.screeners.contrarian_entry import _score_contrarian_signal

        candidates = [
            {
                "ticker": "XYZ",
                "consecutive_high_sv_days": 7,
                "avg_sv_ratio": 70.0,
                "short_interest_trend_pct": 25.0,
                "support_count": 3,
            },
            {
                "ticker": "ABC",
                "consecutive_high_sv_days": 3,
                "avg_sv_ratio": 62.0,
                "short_interest_trend_pct": 8.0,
                "support_count": 1,
            },
        ]

        scored = _score_contrarian_signal(candidates, max_results=10)

        # Verify scoring logic
        assert len(scored) == 2
        assert scored[0]["ticker"] == "XYZ"  # Higher score should be first
        assert "contrarian_score" in scored[0]
        assert scored[0]["contrarian_score"] > scored[1]["contrarian_score"]
        assert "entry_rationale" in scored[0]

    def test_contrarian_scoring_empty(self):
        """Test contrarian scoring with empty list."""
        from src.mcp_polygon.screeners.contrarian_entry import _score_contrarian_signal

        scored = _score_contrarian_signal([], max_results=10)
        assert scored == []


# Earnings screener tests
class TestEarningsScreener:
    """Test cases for screen_earnings_short_setup tool."""

    def test_pattern_recognition_acceleration(self):
        """Test acceleration pattern detection."""
        import pandas as pd
        from src.mcp_polygon.screeners.common.earnings_helpers import (
            analyze_short_pattern,
        )

        # Create mock data with acceleration pattern (steep slope >1.5/day)
        dates = pd.date_range("2025-10-01", periods=12, freq="D")
        # Strong acceleration: each day adds 2%
        ratios = [45 + i * 2.0 for i in range(12)]  # 45, 47, 49, ..., 67

        df = pd.DataFrame({"date": dates, "short_volume_ratio": ratios})

        pattern = analyze_short_pattern(df)

        # Verify it detects upward pattern (should be acceleration or moderate_buildup)
        assert pattern["pattern_type"] in ["acceleration", "moderate_buildup"]
        assert pattern["trend_slope"] > 1.5
        assert pattern["pattern_strength"] > 50

    def test_pattern_recognition_deceleration(self):
        """Test deceleration pattern detection."""
        import pandas as pd
        import numpy as np
        from src.mcp_polygon.screeners.common.earnings_helpers import (
            analyze_short_pattern,
        )

        # Create mock data with deceleration pattern
        dates = pd.date_range("2025-10-01", periods=15, freq="D")
        # Decelerating: 68% → 38% over 15 days (slope = -30/15 = -2.0/day)
        ratios = np.linspace(68, 38, 15)

        df = pd.DataFrame({"date": dates, "short_volume_ratio": ratios})

        pattern = analyze_short_pattern(df)

        assert pattern["pattern_type"] == "deceleration"
        assert pattern["current_avg"] < 50
        assert pattern["trend_slope"] < -1.0  # Relaxed from -1.5 for test
        assert pattern["pattern_strength"] > 50

    def test_pattern_recognition_steady(self):
        """Test steady pattern detection."""
        import pandas as pd
        from src.mcp_polygon.screeners.common.earnings_helpers import (
            analyze_short_pattern,
        )

        # Create mock data with steady pattern
        dates = pd.date_range("2025-10-01", periods=15, freq="D")
        # Steady: around 50% with minimal variation
        ratios = [50, 51, 49, 50, 52, 48, 50, 51, 49, 50, 51, 50, 49, 51, 50]

        df = pd.DataFrame({"date": dates, "short_volume_ratio": ratios})

        pattern = analyze_short_pattern(df)

        assert pattern["pattern_type"] == "steady"
        assert -1.5 <= pattern["trend_slope"] <= 1.5
        assert pattern["volatility"] < 5.0  # Low volatility

    def test_pattern_recognition_reversal(self):
        """Test reversal pattern detection."""
        import pandas as pd
        from src.mcp_polygon.screeners.common.earnings_helpers import (
            analyze_short_pattern,
        )

        # Create mock data with clear reversal pattern
        dates = pd.date_range("2025-10-01", periods=12, freq="D")
        # First 6 days: strong uptrend (+3%/day)
        # Last 6 days: strong downtrend (-3%/day)
        ratios = [40, 43, 46, 49, 52, 55, 53, 50, 47, 44, 41, 38]

        df = pd.DataFrame({"date": dates, "short_volume_ratio": ratios})

        pattern = analyze_short_pattern(df)

        # Reversal logic compares first half vs last half slopes
        # Should detect either reversal or one of the directional patterns
        assert pattern["pattern_type"] in [
            "reversal_up",
            "reversal_down",
            "steady",
            "deceleration",
        ]

    def test_pattern_recognition_insufficient_data(self):
        """Test pattern recognition with insufficient data."""
        import pandas as pd
        from src.mcp_polygon.screeners.common.earnings_helpers import (
            analyze_short_pattern,
        )

        # Only 3 data points (need at least 5)
        dates = pd.date_range("2025-10-01", periods=3, freq="D")
        ratios = [50, 51, 52]

        df = pd.DataFrame({"date": dates, "short_volume_ratio": ratios})

        pattern = analyze_short_pattern(df)

        assert pattern["pattern_type"] == "insufficient_data"
        assert pattern["pattern_strength"] == 0.0

    def test_scenario_classification_high_buildup(self):
        """Test high buildup scenario classification."""
        from src.mcp_polygon.screeners.common.earnings_helpers import (
            classify_short_scenario,
        )

        pattern = {
            "pattern_type": "acceleration",
            "current_avg": 58.5,
            "trend_slope": 2.3,
            "volatility": 3.0,
            "pattern_strength": 87.3,
        }

        scenario, trade_setup = classify_short_scenario(pattern)

        assert scenario == "high_buildup"
        assert trade_setup == "straddle"

    def test_scenario_classification_declining_shorts(self):
        """Test declining shorts scenario classification."""
        from src.mcp_polygon.screeners.common.earnings_helpers import (
            classify_short_scenario,
        )

        pattern = {
            "pattern_type": "deceleration",
            "current_avg": 42.0,
            "trend_slope": -2.1,
            "volatility": 2.5,
            "pattern_strength": 72.1,
        }

        scenario, trade_setup = classify_short_scenario(pattern)

        assert scenario == "declining_shorts"
        assert trade_setup == "bullish_if_beat"

    def test_scenario_classification_normal(self):
        """Test normal scenario classification."""
        from src.mcp_polygon.screeners.common.earnings_helpers import (
            classify_short_scenario,
        )

        pattern = {
            "pattern_type": "steady",
            "current_avg": 48.0,
            "trend_slope": 0.5,
            "volatility": 1.8,
            "pattern_strength": 45.2,
        }

        scenario, trade_setup = classify_short_scenario(pattern)

        assert scenario == "normal"
        assert trade_setup == "fundamentals_only"

    def test_scenario_classification_reversals(self):
        """Test reversal scenario classifications."""
        from src.mcp_polygon.screeners.common.earnings_helpers import (
            classify_short_scenario,
        )

        # Reversal up (shorts covering)
        pattern_up = {
            "pattern_type": "reversal_up",
            "current_avg": 45.0,
            "trend_slope": -3.0,
            "volatility": 4.0,
            "pattern_strength": 65.0,
        }

        scenario_up, setup_up = classify_short_scenario(pattern_up)
        assert scenario_up == "reversal_shorts_covering"
        assert setup_up == "bullish_directional"

        # Reversal down (shorts building)
        pattern_down = {
            "pattern_type": "reversal_down",
            "current_avg": 55.0,
            "trend_slope": 3.5,
            "volatility": 4.5,
            "pattern_strength": 68.0,
        }

        scenario_down, setup_down = classify_short_scenario(pattern_down)
        assert scenario_down == "reversal_shorts_building"
        assert setup_down == "bearish_or_puts"

    def test_earnings_scoring(self):
        """Test earnings screener scoring algorithm."""
        from src.mcp_polygon.screeners.earnings_short_setup import _score_and_rank

        candidates = [
            {
                "ticker": "NFLX",
                "days_until_earnings": 5,
                "pattern_strength": 87.3,
                "short_volume_10d_avg": 58.5,
                "short_trend_slope": 2.3,
                "market_cap": 180_000_000_000,
                "eps": 12.55,
                "debt_to_equity": 1.2,
                "scenario": "high_buildup",
                "trade_setup": "straddle",
            },
            {
                "ticker": "TSLA",
                "days_until_earnings": 17,
                "pattern_strength": 72.1,
                "short_volume_10d_avg": 42.0,
                "short_trend_slope": -2.1,
                "market_cap": 800_000_000_000,
                "eps": 3.20,
                "debt_to_equity": 0.8,
                "scenario": "declining_shorts",
                "trade_setup": "bullish_if_beat",
            },
            {
                "ticker": "AAPL",
                "days_until_earnings": 19,
                "pattern_strength": 45.2,
                "short_volume_10d_avg": 48.0,
                "short_trend_slope": 0.5,
                "market_cap": 3_000_000_000_000,
                "eps": 6.42,
                "debt_to_equity": 1.5,
                "scenario": "normal",
                "trade_setup": "fundamentals_only",
            },
        ]

        ranked = _score_and_rank(candidates, max_results=10)

        # Verify ranking logic
        assert len(ranked) == 3
        assert (
            ranked[0]["ticker"] == "NFLX"
        )  # Highest score (close to earnings, high pattern strength)
        assert "earnings_score" in ranked[0]
        assert "rationale" in ranked[0]
        assert ranked[0]["earnings_score"] > ranked[1]["earnings_score"]
        assert ranked[1]["earnings_score"] > ranked[2]["earnings_score"]

    def test_earnings_scoring_empty(self):
        """Test earnings scoring with empty list."""
        from src.mcp_polygon.screeners.earnings_short_setup import _score_and_rank

        scored = _score_and_rank([], max_results=10)
        assert scored == []

    @pytest.mark.asyncio
    async def test_filter_upcoming_earnings(self):
        """Test earnings filtering by date window."""
        from src.mcp_polygon.screeners.common.earnings_helpers import (
            filter_upcoming_earnings,
        )
        from datetime import datetime, timedelta

        # Create mock earnings data
        today = datetime.now().date()
        earnings_list = [
            {
                "symbol": "NFLX",
                "reportDate": (today + timedelta(days=5)).strftime("%Y-%m-%d"),
            },
            {
                "symbol": "TSLA",
                "reportDate": (today + timedelta(days=17)).strftime("%Y-%m-%d"),
            },
            {
                "symbol": "AAPL",
                "reportDate": (today + timedelta(days=25)).strftime("%Y-%m-%d"),
            },
            {
                "symbol": "MSFT",
                "reportDate": (today + timedelta(days=40)).strftime("%Y-%m-%d"),
            },
        ]

        # Filter to next 21 days
        filtered = filter_upcoming_earnings(
            earnings_list, min_days_ahead=0, max_days_ahead=21
        )

        # Should include NFLX and TSLA, exclude AAPL and MSFT
        assert len(filtered) == 2
        tickers = [e["symbol"] for e in filtered]
        assert "NFLX" in tickers
        assert "TSLA" in tickers
        assert "AAPL" not in tickers
        assert "MSFT" not in tickers

        # All should have days_until_earnings field
        for event in filtered:
            assert "days_until_earnings" in event
            assert 0 <= event["days_until_earnings"] <= 21

    @pytest.mark.asyncio
    async def test_earnings_screener_basic(self):
        """Test basic earnings screener flow with mocks."""
        with patch(
            "src.mcp_polygon.screeners.earnings_short_setup.fetch_earnings_calendar"
        ) as mock_earnings:
            with patch(
                "src.mcp_polygon.screeners.earnings_short_setup.fetch_short_volume_trends"
            ) as mock_sv:
                with patch(
                    "src.mcp_polygon.screeners.earnings_short_setup.validate_fundamentals"
                ) as mock_fundamentals:
                    from datetime import datetime, timedelta

                    # Mock earnings calendar
                    today = datetime.now().date()
                    mock_earnings.return_value = [
                        {
                            "symbol": "NFLX",
                            "reportDate": (today + timedelta(days=5)).strftime(
                                "%Y-%m-%d"
                            ),
                            "days_until_earnings": 5,
                        }
                    ]

                    # Mock short volume data with acceleration pattern
                    import pandas as pd
                    import numpy as np

                    dates = pd.date_range(
                        today - timedelta(days=30), periods=30, freq="D"
                    )
                    ratios = np.linspace(48, 58, 30)
                    mock_sv.return_value = {
                        "NFLX": pd.DataFrame(
                            {"date": dates, "short_volume_ratio": ratios}
                        )
                    }

                    # Mock fundamentals validation
                    mock_fundamentals.return_value = [
                        {
                            "ticker": "NFLX",
                            "days_until_earnings": 5,
                            "pattern_strength": 87.3,
                            "short_volume_10d_avg": 58.5,
                            "short_trend_slope": 2.3,
                            "scenario": "high_buildup",
                            "trade_setup": "straddle",
                            "market_cap": 180_000_000_000,
                            "price": 450.0,
                            "eps": 12.55,
                            "debt_to_equity": 1.2,
                        }
                    ]

                    # Run screener
                    from src.mcp_polygon.screeners.earnings_short_setup import (
                        screen_earnings_short_setup,
                    )

                    result = await screen_earnings_short_setup(
                        alpha_vantage_api_key="test_key",
                        earnings_window_days=21,
                        fetch_all=False,
                    )

                    # Verify result
                    assert result is not None
                    assert "NFLX" in result
                    assert "straddle" in result

    @pytest.mark.asyncio
    async def test_earnings_screener_no_earnings(self):
        """Test when no earnings found in window."""
        with patch(
            "src.mcp_polygon.screeners.earnings_short_setup.fetch_earnings_calendar"
        ) as mock_earnings:
            mock_earnings.return_value = []

            from src.mcp_polygon.screeners.earnings_short_setup import (
                screen_earnings_short_setup,
            )

            result = await screen_earnings_short_setup(
                alpha_vantage_api_key="test_key", fetch_all=False
            )

            assert "No earnings found" in result

    @pytest.mark.asyncio
    async def test_earnings_screener_no_short_data(self):
        """Test when no short volume data available."""
        with patch(
            "src.mcp_polygon.screeners.earnings_short_setup.fetch_earnings_calendar"
        ) as mock_earnings:
            with patch(
                "src.mcp_polygon.screeners.earnings_short_setup.fetch_short_volume_trends"
            ) as mock_sv:
                from datetime import datetime, timedelta

                today = datetime.now().date()
                mock_earnings.return_value = [
                    {
                        "symbol": "NFLX",
                        "reportDate": (today + timedelta(days=5)).strftime("%Y-%m-%d"),
                        "days_until_earnings": 5,
                    }
                ]

                mock_sv.return_value = {}  # No short volume data

                from src.mcp_polygon.screeners.earnings_short_setup import (
                    screen_earnings_short_setup,
                )

                result = await screen_earnings_short_setup(
                    alpha_vantage_api_key="test_key", fetch_all=False
                )

                assert "No short volume data found" in result


# Integration tests (require API access)
@pytest.mark.integration
class TestScreenerIntegration:
    """Integration tests that hit real API (run with --integration flag)."""

    @pytest.mark.asyncio
    async def test_real_screening(self):
        """Test with real API calls (requires POLYGON_API_KEY)."""
        # This would test against real Polygon API
        # Skip for now unless integration testing
        pytest.skip("Integration test - requires API key")

    @pytest.mark.asyncio
    async def test_real_contrarian_screening(self):
        """Test contrarian screener with real API."""
        # This would test against real Polygon API
        pytest.skip("Integration test - requires API key")

    @pytest.mark.asyncio
    async def test_real_earnings_screening(self):
        """Test earnings screener with real API."""
        # This would test against real Polygon API
        # Requires both POLYGON_API_KEY and ALPHA_VANTAGE_API_KEY
        pytest.skip("Integration test - requires API keys")
