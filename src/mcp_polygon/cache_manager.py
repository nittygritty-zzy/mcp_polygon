"""
Cache management for Polygon MCP tools.

Handles Parquet file storage, partitioning schemes, TTL, and size limits.
"""

import os
import shutil
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
import json
import hashlib

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq


class CacheManager:
    """
    Manages caching of Polygon API responses to Parquet files with intelligent partitioning.

    Features:
    - TTL-based expiration (default: 7 days)
    - Size-based eviction with LRU (max: 5 GB)
    - Tool-specific partition schemes
    - Metadata tracking for efficient queries
    """

    def __init__(
        self,
        cache_dir: str = "./cache",
        ttl_days: int = 7,
        max_size_gb: float = 5.0,
    ):
        """
        Initialize cache manager.

        Args:
            cache_dir: Root directory for cache storage
            ttl_days: Time-to-live in days before cache expires
            max_size_gb: Maximum cache size in gigabytes
        """
        self.cache_dir = Path(cache_dir)
        self.cache_dir.mkdir(parents=True, exist_ok=True)

        self.ttl_days = ttl_days
        self.max_size_bytes = int(max_size_gb * 1024 * 1024 * 1024)

        # Metadata file tracks cache entries for efficient cleanup
        self.metadata_file = self.cache_dir / "_metadata.json"
        self.metadata = self._load_metadata()

    def _load_metadata(self) -> Dict[str, Any]:
        """Load cache metadata from disk."""
        if self.metadata_file.exists():
            with open(self.metadata_file, "r") as f:
                return json.load(f)
        return {"entries": {}, "total_size_bytes": 0, "last_cleanup": None}

    def _save_metadata(self):
        """Save cache metadata to disk."""
        with open(self.metadata_file, "w") as f:
            json.dump(self.metadata, f, indent=2, default=str)

    def _get_partition_path(
        self, tool_name: str, params: Dict[str, Any]
    ) -> Tuple[Path, str]:
        """
        Generate partition path based on tool name and parameters.

        Returns:
            Tuple of (full_path, partition_key)
        """
        partition_key = self._generate_partition_key(tool_name, params)
        full_path = self.cache_dir / tool_name / partition_key
        return full_path, partition_key

    def _generate_partition_key(self, tool_name: str, params: Dict[str, Any]) -> str:
        """
        Generate partition key based on tool type and parameters.

        Partition schemes by tool category:
        - Aggregates: ticker/year/month
        - Grouped daily: date
        - Tickers: market_active/date
        - Financials: ticker/timeframe
        - Corporate actions: ticker
        - News: ticker/year-month
        - Technical indicators: ticker/indicator_params
        - Options: underlying/expiration
        - Economics: year
        """

        # Time-series aggregates (get_aggs, list_aggs)
        if tool_name in ["get_aggs", "list_aggs"]:
            ticker = params.get("ticker", "UNKNOWN")
            from_date = params.get("from_", "")
            if from_date:
                try:
                    dt = datetime.strptime(from_date[:10], "%Y-%m-%d")
                    return f"{ticker}/{dt.year}/{dt.month:02d}"
                except ValueError:
                    pass
            return f"{ticker}/unknown"

        # Market-wide snapshots
        elif tool_name in ["get_grouped_daily_aggs", "get_snapshot_all"]:
            date = params.get("date", datetime.now().strftime("%Y-%m-%d"))
            return date[:10]  # YYYY-MM-DD

        # Ticker listings
        elif tool_name in ["list_tickers", "get_all_tickers"]:
            market = params.get("market", "stocks")
            active = params.get("active", True)
            status = "active" if active else "inactive"
            date = datetime.now().strftime("%Y-%m-%d")
            return f"{market}_{status}/{date}"

        # Financials
        elif tool_name.startswith("list_financials_"):
            ticker = params.get("tickers", "UNKNOWN")
            timeframe = params.get("timeframe", "quarterly")
            return f"{ticker}/{timeframe}"

        # Financial ratios
        elif tool_name in ["list_stock_ratios", "list_financials_ratios"]:
            ticker = params.get("ticker", "UNKNOWN")
            return ticker

        # Corporate actions
        elif tool_name in ["list_dividends", "list_splits"]:
            ticker = params.get("ticker", "UNKNOWN")
            return ticker

        # IPOs (not ticker-specific)
        elif tool_name == "list_ipos":
            status = params.get("ipo_status", "all")
            return f"all/{status}"

        # News
        elif tool_name == "list_ticker_news":
            ticker = params.get("ticker", "UNKNOWN")
            # Use published date if available, otherwise current month
            published = params.get("published_utc_gte") or params.get("published_utc")
            if published:
                try:
                    dt = datetime.strptime(published[:10], "%Y-%m-%d")
                    return f"{ticker}/{dt.year}-{dt.month:02d}"
                except ValueError:
                    pass
            now = datetime.now()
            return f"{ticker}/{now.year}-{now.month:02d}"

        # Technical indicators
        elif tool_name in ["get_sma", "get_ema", "get_macd", "get_rsi"]:
            ticker = params.get("ticker", "UNKNOWN")

            # Build parameter string
            if tool_name == "get_sma":
                window = params.get("window", 50)
                timespan = params.get("timespan", "day")
                param_str = f"window_{window}_timespan_{timespan}"
            elif tool_name == "get_ema":
                window = params.get("window", 12)
                timespan = params.get("timespan", "day")
                param_str = f"window_{window}_timespan_{timespan}"
            elif tool_name == "get_rsi":
                window = params.get("window", 14)
                timespan = params.get("timespan", "day")
                param_str = f"window_{window}_timespan_{timespan}"
            elif tool_name == "get_macd":
                short_window = params.get("short_window", 12)
                long_window = params.get("long_window", 26)
                signal_window = params.get("signal_window", 9)
                param_str = f"short_{short_window}_long_{long_window}_signal_{signal_window}"
            else:
                param_str = "default"

            return f"{ticker}/{param_str}"

        # Options
        elif tool_name in ["list_options_contracts", "get_options_chain_snapshot"]:
            underlying = params.get("underlying_ticker") or params.get("underlying_asset", "UNKNOWN")
            expiration = params.get("expiration_date", "all")
            return f"{underlying}/{expiration}"

        # Options aggregates
        elif tool_name in ["get_options_aggs", "get_options_daily_open_close", "get_options_previous_close"]:
            options_ticker = params.get("options_ticker", "UNKNOWN")
            # Extract underlying and expiration from options ticker format
            # O:AAPL251219C00150000 -> AAPL/2025-12-19
            if options_ticker.startswith("O:"):
                try:
                    parts = options_ticker[2:]  # Remove O:
                    # Find where the date starts (6 digits YYMMDD)
                    for i, char in enumerate(parts):
                        if char.isdigit():
                            underlying = parts[:i]
                            date_part = parts[i:i+6]
                            if len(date_part) == 6:
                                year = int("20" + date_part[:2])
                                month = int(date_part[2:4])
                                day = int(date_part[4:6])
                                expiration = f"{year}-{month:02d}-{day:02d}"
                                return f"{underlying}/{expiration}"
                            break
                except:
                    pass
            return options_ticker

        # Economics data
        elif tool_name in ["list_treasury_yields", "list_inflation", "list_inflation_expectations"]:
            date = params.get("date_gte") or params.get("date")
            if date:
                try:
                    dt = datetime.strptime(date[:10], "%Y-%m-%d")
                    return str(dt.year)
                except ValueError:
                    pass
            return str(datetime.now().year)

        # Snapshots (should not be cached, but handle anyway)
        elif tool_name in ["get_snapshot_ticker", "list_universal_snapshots"]:
            ticker = params.get("ticker") or params.get("ticker_any_of", ["UNKNOWN"])[0]
            date = datetime.now().strftime("%Y-%m-%d")
            return f"{ticker}/{date}"

        # Reference data (rarely changes)
        elif tool_name in ["get_exchanges", "get_ticker_types", "get_market_holidays"]:
            date = datetime.now().strftime("%Y-%m-%d")
            return date

        # Alpha Vantage earnings calendar
        elif tool_name == "get_earnings_calendar_alpha_vantage":
            horizon = params.get("horizon", "3month")
            symbol = params.get("symbol")
            if symbol:
                return f"{symbol}/{horizon}"
            return f"all/{horizon}"

        # Ticker details
        elif tool_name == "get_ticker_details":
            ticker = params.get("ticker", "UNKNOWN")
            date = datetime.now().strftime("%Y-%m-%d")
            return f"{ticker}/{date}"

        # Default: hash of parameters
        else:
            param_str = json.dumps(params, sort_keys=True)
            param_hash = hashlib.md5(param_str.encode()).hexdigest()[:8]
            return f"params_{param_hash}"

    def should_cache(self, tool_name: str, params: Dict[str, Any], response_size_bytes: int) -> bool:
        """
        Determine if a response should be cached based on tool, parameters, and size.

        Returns:
            True if caching is beneficial, False to return directly.
        """

        # Rule 1: Never cache real-time/volatile data
        if tool_name in ["get_snapshot_ticker", "list_universal_snapshots", "get_market_status"]:
            return False

        # Rule 2: Always cache reference data (changes rarely)
        if tool_name in ["get_exchanges", "get_ticker_types", "get_market_holidays"]:
            return True

        # Rule 2b: Always cache earnings calendar (large forecast data, limited API calls)
        if tool_name == "get_earnings_calendar_alpha_vantage":
            return True

        # Rule 3: Always cache large responses (> 100 KB)
        if response_size_bytes > 100 * 1024:
            return True

        # Rule 4: Cache based on query scope parameters

        # Market-wide queries (always large)
        if tool_name in ["get_grouped_daily_aggs", "get_snapshot_all"]:
            return True

        # Time-series with large limits
        if tool_name in ["get_aggs", "list_aggs"]:
            if params.get("limit", 0) > 1000:
                return True

        # Full ticker listings
        if tool_name in ["list_tickers", "get_all_tickers"]:
            if params.get("limit", 0) > 500:
                return True

        # Multi-quarter financials
        if tool_name.startswith("list_financials_"):
            if params.get("limit", 0) > 10:
                return True

        # Large news queries
        if tool_name == "list_ticker_news":
            if params.get("limit", 0) > 50:
                return True

        # Full options chains (no filters)
        if tool_name in ["list_options_contracts", "get_options_chain_snapshot"]:
            if not params.get("strike_price") and not params.get("expiration_date"):
                return True

        # Technical indicators with large windows
        if tool_name in ["get_sma", "get_ema", "get_rsi", "get_macd"]:
            if params.get("limit", 0) > 100:
                return True

        # Rule 5: Default to direct return for small, one-off queries
        return False

    def save(
        self,
        tool_name: str,
        params: Dict[str, Any],
        csv_data: str,
        columns: List[str],
    ) -> Dict[str, Any]:
        """
        Save CSV data to Parquet with appropriate partitioning.

        Args:
            tool_name: Name of the tool
            params: Parameters used in the API call
            csv_data: CSV string data
            columns: Column names

        Returns:
            Metadata dictionary with cache location and query info
        """
        import io
        import csv

        # Parse CSV to DataFrame
        reader = csv.DictReader(io.StringIO(csv_data))
        rows = list(reader)

        if not rows:
            raise ValueError("No data to cache")

        df = pd.DataFrame(rows)

        # Get partition path
        partition_path, partition_key = self._get_partition_path(tool_name, params)
        partition_path.mkdir(parents=True, exist_ok=True)

        # Save to Parquet
        parquet_file = partition_path / "data.parquet"
        table = pa.Table.from_pandas(df)
        pq.write_table(table, parquet_file, compression="snappy")

        # Update metadata
        cache_key = f"{tool_name}/{partition_key}"
        file_size = parquet_file.stat().st_size

        self.metadata["entries"][cache_key] = {
            "tool_name": tool_name,
            "partition_key": partition_key,
            "file_path": str(parquet_file),
            "file_size_bytes": file_size,
            "row_count": len(rows),
            "columns": columns,
            "parameters": params,
            "created_at": datetime.now().isoformat(),
            "last_accessed": datetime.now().isoformat(),
        }

        # Update total size
        self.metadata["total_size_bytes"] = sum(
            entry["file_size_bytes"] for entry in self.metadata["entries"].values()
        )

        self._save_metadata()

        # Check if cleanup needed
        if self.metadata["total_size_bytes"] > self.max_size_bytes:
            self._cleanup_lru()

        # Return cache metadata for response
        glob_pattern = str(partition_path / "*.parquet")

        return {
            "cached": True,
            "cache_location": glob_pattern,
            "partition_key": partition_key,
            "row_count": len(rows),
            "columns": columns,
            "file_size_bytes": file_size,
        }

    def get(self, tool_name: str, params: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """
        Retrieve cached data metadata if exists and not expired.

        Args:
            tool_name: Name of the tool
            params: Parameters used in the API call

        Returns:
            Cache metadata dict if found and valid, None otherwise
        """
        partition_path, partition_key = self._get_partition_path(tool_name, params)
        cache_key = f"{tool_name}/{partition_key}"

        # Check if exists in metadata
        if cache_key not in self.metadata["entries"]:
            return None

        entry = self.metadata["entries"][cache_key]

        # Check if expired
        created_at = datetime.fromisoformat(entry["created_at"])
        if datetime.now() - created_at > timedelta(days=self.ttl_days):
            # Expired, remove
            self._remove_entry(cache_key)
            return None

        # Update last accessed
        entry["last_accessed"] = datetime.now().isoformat()
        self._save_metadata()

        # Return metadata
        glob_pattern = str(partition_path / "*.parquet")

        return {
            "cached": True,
            "cache_location": glob_pattern,
            "partition_key": partition_key,
            "row_count": entry["row_count"],
            "columns": entry["columns"],
            "file_size_bytes": entry["file_size_bytes"],
            "cached_at": entry["created_at"],
        }

    def _remove_entry(self, cache_key: str):
        """Remove a cache entry and its files."""
        if cache_key not in self.metadata["entries"]:
            return

        entry = self.metadata["entries"][cache_key]
        file_path = Path(entry["file_path"])

        # Remove file and parent directory if empty
        if file_path.exists():
            file_path.unlink()
            try:
                file_path.parent.rmdir()  # Remove if empty
            except OSError:
                pass  # Directory not empty

        # Update metadata
        self.metadata["total_size_bytes"] -= entry["file_size_bytes"]
        del self.metadata["entries"][cache_key]
        self._save_metadata()

    def _cleanup_lru(self):
        """Clean up least recently used cache entries until under size limit."""
        # Sort entries by last accessed time
        sorted_entries = sorted(
            self.metadata["entries"].items(),
            key=lambda x: x[1]["last_accessed"],
        )

        # Remove oldest entries until under limit
        while self.metadata["total_size_bytes"] > self.max_size_bytes and sorted_entries:
            cache_key, _ = sorted_entries.pop(0)
            self._remove_entry(cache_key)

    def cleanup_expired(self):
        """Remove all expired cache entries."""
        expired_keys = []

        for cache_key, entry in self.metadata["entries"].items():
            created_at = datetime.fromisoformat(entry["created_at"])
            if datetime.now() - created_at > timedelta(days=self.ttl_days):
                expired_keys.append(cache_key)

        for cache_key in expired_keys:
            self._remove_entry(cache_key)

        self.metadata["last_cleanup"] = datetime.now().isoformat()
        self._save_metadata()

    def get_stats(self) -> Dict[str, Any]:
        """Get cache statistics."""
        return {
            "total_entries": len(self.metadata["entries"]),
            "total_size_bytes": self.metadata["total_size_bytes"],
            "total_size_mb": round(self.metadata["total_size_bytes"] / (1024 * 1024), 2),
            "total_size_gb": round(self.metadata["total_size_bytes"] / (1024 * 1024 * 1024), 2),
            "max_size_gb": self.max_size_bytes / (1024 * 1024 * 1024),
            "ttl_days": self.ttl_days,
            "last_cleanup": self.metadata.get("last_cleanup"),
        }

    def clear_all(self):
        """Clear all cache data."""
        if self.cache_dir.exists():
            shutil.rmtree(self.cache_dir)
        self.cache_dir.mkdir(parents=True, exist_ok=True)
        self.metadata = {"entries": {}, "total_size_bytes": 0, "last_cleanup": None}
        self._save_metadata()


# Singleton instance
_cache_manager_instance = None


def get_cache_manager() -> CacheManager:
    """
    Get the singleton CacheManager instance.

    Returns:
        CacheManager: Shared cache manager instance
    """
    global _cache_manager_instance
    if _cache_manager_instance is None:
        _cache_manager_instance = CacheManager()
    return _cache_manager_instance
