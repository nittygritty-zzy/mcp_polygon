"""Test Alpha Vantage with environment variable."""

import pytest
import os
from src.mcp_polygon.tools.alpha_vantage import get_earnings_calendar_alpha_vantage


class TestAlphaVantageEnvironment:
    """Test Alpha Vantage API key from environment variable."""

    @pytest.mark.asyncio
    async def test_api_key_from_environment(self, monkeypatch):
        """Test that API key is read from environment variable."""
        # Set environment variable
        monkeypatch.setenv("ALPHA_VANTAGE_API_KEY", "demo")

        # Call without passing API key parameter
        result = await get_earnings_calendar_alpha_vantage(horizon="3month")

        # Should work and return data
        assert result is not None
        assert len(result) > 0
        assert "symbol" in result.lower()

    @pytest.mark.asyncio
    async def test_parameter_overrides_environment(self, monkeypatch):
        """Test that parameter overrides environment variable."""
        # Set environment to wrong key
        monkeypatch.setenv("ALPHA_VANTAGE_API_KEY", "wrong_key")

        # Pass correct key as parameter
        result = await get_earnings_calendar_alpha_vantage(
            alpha_vantage_api_key="demo",
            horizon="3month"
        )

        # Should work with parameter key
        assert result is not None
        assert len(result) > 0
        assert "symbol" in result.lower()

    @pytest.mark.asyncio
    async def test_missing_api_key_error(self, monkeypatch):
        """Test error message when API key is missing."""
        # Remove environment variable
        monkeypatch.delenv("ALPHA_VANTAGE_API_KEY", raising=False)

        # Call without API key
        result = await get_earnings_calendar_alpha_vantage(horizon="3month")

        # Should return error message
        assert "Error" in result
        assert "API key is required" in result
        assert "ALPHA_VANTAGE_API_KEY" in result
