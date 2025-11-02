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
