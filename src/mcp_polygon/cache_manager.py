"""
Cache management for Polygon MCP tools.

Handles Parquet file storage, partitioning schemes, TTL, and size limits.
"""

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
                param_str = (
                    f"short_{short_window}_long_{long_window}_signal_{signal_window}"
                )
            else:
                param_str = "default"

            return f"{ticker}/{param_str}"

        # Options
        elif tool_name in ["list_options_contracts", "list_snapshot_options_chain"]:
            underlying = params.get("underlying_ticker") or params.get(
                "underlying_asset", "UNKNOWN"
            )
            expiration = params.get("expiration_date") or "all"
            contract_type = params.get("contract_type") or "all"
            return f"{underlying}/{contract_type}_{expiration}"

        # Snapshots
        elif tool_name == "list_universal_snapshots":
            snapshot_type = params.get("type", "all")
            ticker_any_of = params.get("ticker_any_of")
            if ticker_any_of and len(ticker_any_of) <= 5:
                # For small ticker lists, use them in partition key
                ticker_str = "_".join(sorted(ticker_any_of))
                return f"{snapshot_type}/{ticker_str}"
            return f"{snapshot_type}/all"

        elif tool_name == "get_snapshot_all":
            market_type = params.get("market_type", "UNKNOWN")
            tickers = params.get("tickers")
            if tickers and len(tickers) <= 5:
                ticker_str = "_".join(sorted(tickers))
                return f"{market_type}/{ticker_str}"
            return f"{market_type}/all"

        # Options aggregates
        elif tool_name in [
            "get_options_aggs",
            "get_options_daily_open_close",
            "get_options_previous_close",
        ]:
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
                            date_part = parts[i : i + 6]
                            if len(date_part) == 6:
                                year = int("20" + date_part[:2])
                                month = int(date_part[2:4])
                                day = int(date_part[4:6])
                                expiration = f"{year}-{month:02d}-{day:02d}"
                                return f"{underlying}/{expiration}"
                            break
                except (ValueError, IndexError):
                    pass
            return options_ticker

        # Economics data
        elif tool_name in [
            "list_treasury_yields",
            "list_inflation",
            "list_inflation_expectations",
        ]:
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

        # Index snapshots
        elif tool_name == "get_snapshot_indices":
            ticker_any_of = params.get("ticker_any_of")
            if ticker_any_of and len(ticker_any_of) <= 5:
                ticker_str = "_".join(sorted(ticker_any_of))
                return ticker_str
            return "all_indices"

        # Market summaries
        elif tool_name == "get_summaries":
            ticker_any_of = params.get("ticker_any_of")
            if ticker_any_of and len(ticker_any_of) <= 5:
                ticker_str = "_".join(sorted(ticker_any_of))
                return ticker_str
            return "all_tickers"

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

        # Futures aggregates
        elif tool_name == "list_futures_aggregates":
            ticker = params.get("ticker", "UNKNOWN")
            resolution = params.get("resolution", "day")
            return f"{ticker}/{resolution}"

        # Futures contracts
        elif tool_name == "list_futures_contracts":
            product_code = params.get("product_code", "all")
            active = params.get("active", "all")
            return f"{product_code}/{active}"

        # Futures products
        elif tool_name == "list_futures_products":
            sector = params.get("sector", "all")
            asset_class = params.get("asset_class", "all")
            return f"{sector}/{asset_class}"

        # Futures schedules
        elif tool_name == "list_futures_schedules":
            session_date = params.get(
                "session_end_date", datetime.now().strftime("%Y-%m-%d")
            )
            trading_venue = params.get("trading_venue", "all")
            return f"{session_date}/{trading_venue}"

        # Futures schedules by product
        elif tool_name == "list_futures_schedules_by_product_code":
            product_code = params.get("product_code", "UNKNOWN")
            return product_code

        # Futures market statuses
        elif tool_name == "list_futures_market_statuses":
            product_code = params.get("product_code", "all")
            return product_code

        # Stock financials
        elif tool_name == "list_stock_financials":
            ticker = params.get("ticker", "UNKNOWN")
            timeframe = params.get("timeframe", "all")
            return f"{ticker}/{timeframe}"

        # Short interest
        elif tool_name == "list_short_interest":
            ticker = params.get("ticker", "all")
            settlement_date_gte = params.get("settlement_date_gte")
            if settlement_date_gte:
                try:
                    dt = datetime.strptime(str(settlement_date_gte)[:10], "%Y-%m-%d")
                    return f"{ticker}/{dt.year}-{dt.month:02d}"
                except ValueError:
                    pass
            return ticker

        # Short volume
        elif tool_name == "list_short_volume":
            ticker = params.get("ticker", "all")
            date_gte = params.get("date_gte")
            if date_gte:
                try:
                    dt = datetime.strptime(str(date_gte)[:10], "%Y-%m-%d")
                    return f"{ticker}/{dt.year}-{dt.month:02d}"
                except ValueError:
                    pass
            return ticker

        # Default: hash of parameters
        else:
            param_str = json.dumps(params, sort_keys=True)
            param_hash = hashlib.md5(param_str.encode()).hexdigest()[:8]
            return f"params_{param_hash}"

    def should_cache(
        self, tool_name: str, params: Dict[str, Any], response_size_bytes: int
    ) -> bool:
        """
        Determine if a response should be cached based on tool, parameters, and size.

        Returns:
            True if caching is beneficial, False to return directly.
        """

        # Rule 1: Never cache real-time/volatile data (except indices/summaries for specific tickers)
        if tool_name in [
            "get_snapshot_ticker",
            "get_market_status",
        ]:
            return False

        # Index snapshots and summaries: cache only if specific tickers requested
        if tool_name in ["get_snapshot_indices", "get_summaries"]:
            ticker_any_of = params.get("ticker_any_of")
            if ticker_any_of and len(ticker_any_of) > 0:
                return True  # Cache specific ticker requests
            return False  # Don't cache "all" requests (too volatile)

        # Universal snapshots: cache only with fetch_all
        if tool_name == "list_universal_snapshots":
            if params.get("fetch_all"):
                return True
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

        # Time-series with large limits or fetch_all
        if tool_name in ["get_aggs", "list_aggs"]:
            if params.get("fetch_all") or params.get("limit", 0) > 1000:
                return True

        # Full ticker listings
        if tool_name in ["list_tickers", "get_all_tickers"]:
            if params.get("fetch_all") or params.get("limit", 0) > 500:
                return True

        # Multi-quarter financials
        if tool_name.startswith("list_financials_"):
            if params.get("limit", 0) > 10:
                return True

        # Large news queries
        if tool_name == "list_ticker_news":
            if params.get("fetch_all") or params.get("limit", 0) > 50:
                return True

        # Full options chains (no filters or fetch_all enabled)
        if tool_name in ["list_options_contracts", "list_snapshot_options_chain"]:
            if params.get("fetch_all") or (
                not params.get("strike_price") and not params.get("expiration_date")
            ):
                return True

        # Options aggregates with fetch_all
        if tool_name == "get_options_aggs":
            if params.get("fetch_all"):
                return True

        # Technical indicators with large windows or fetch_all
        if tool_name in ["get_sma", "get_ema", "get_rsi", "get_macd"]:
            if params.get("fetch_all") or params.get("limit", 0) > 100:
                return True

        # Snapshot tools with fetch_all or large datasets
        if tool_name == "list_universal_snapshots":
            if params.get("fetch_all"):
                return True

        if tool_name == "get_snapshot_all":
            # Always cache full market snapshots - they're huge
            return True

        # Futures tools with fetch_all or large datasets
        if tool_name in [
            "list_futures_aggregates",
            "list_futures_contracts",
            "list_futures_products",
            "list_futures_schedules",
            "list_futures_schedules_by_product_code",
            "list_futures_market_statuses",
        ]:
            if params.get("fetch_all") or params.get("limit", 0) > 100:
                return True

        # Financial tools with fetch_all or large datasets
        if tool_name in [
            "list_stock_financials",
            "list_short_interest",
            "list_short_volume",
        ]:
            if params.get("fetch_all") or params.get("limit", 0) > 50:
                return True

        # Economics tools with fetch_all or large datasets
        if tool_name in [
            "list_treasury_yields",
            "list_inflation",
            "list_inflation_expectations",
        ]:
            if params.get("fetch_all") or params.get("limit", 0) > 100:
                return True

        # Corporate actions tools with fetch_all or large datasets
        if tool_name in [
            "list_splits",
            "list_dividends",
            "list_ipos",
        ]:
            if params.get("fetch_all") or params.get("limit", 0) > 50:
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
            "tool_name": tool_name,
            "params": params,
        }

    def save_batch(
        self,
        tool_name: str,
        params: Dict[str, Any],
        csv_data: str,
        batch_num: int,
        columns: Optional[List[str]] = None,
    ) -> Path:
        """
        Save a batch of CSV data to a numbered Parquet file.

        This allows incremental writing without holding all data in memory.
        Multiple batches are written as separate files (data_001.parquet, data_002.parquet, etc.)
        which can be queried together using glob patterns.

        Args:
            tool_name: Name of the tool
            params: Parameters used in the API call
            csv_data: CSV string data for this batch
            batch_num: Batch number (0-indexed)
            columns: Optional column names (extracted from first batch)

        Returns:
            Path to the written Parquet file
        """
        import io
        import csv as csv_module

        # Parse CSV to DataFrame
        reader = csv_module.DictReader(io.StringIO(csv_data))
        rows = list(reader)

        if not rows:
            # Empty batch, skip
            return None

        df = pd.DataFrame(rows)

        # Get partition path
        partition_path, partition_key = self._get_partition_path(tool_name, params)
        partition_path.mkdir(parents=True, exist_ok=True)

        # Save to numbered Parquet file
        parquet_file = partition_path / f"data_{batch_num:03d}.parquet"
        table = pa.Table.from_pandas(df)
        pq.write_table(table, parquet_file, compression="snappy")

        return parquet_file

    def finalize_batch_save(
        self,
        tool_name: str,
        params: Dict[str, Any],
        total_rows: int,
        columns: List[str],
    ) -> Dict[str, Any]:
        """
        Finalize batch writing by updating metadata.

        Call this after all batches have been written via save_batch().

        Args:
            tool_name: Name of the tool
            params: Parameters used in the API call
            total_rows: Total number of rows across all batches
            columns: Column names

        Returns:
            Metadata dictionary with cache location and query info
        """
        # Get partition path
        partition_path, partition_key = self._get_partition_path(tool_name, params)

        # Calculate total file size
        total_size = sum(
            f.stat().st_size
            for f in partition_path.glob("data_*.parquet")
            if f.is_file()
        )

        # Update metadata
        cache_key = f"{tool_name}/{partition_key}"

        self.metadata["entries"][cache_key] = {
            "tool_name": tool_name,
            "partition_key": partition_key,
            "file_path": str(partition_path / "data_*.parquet"),
            "file_size_bytes": total_size,
            "row_count": total_rows,
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
            "row_count": total_rows,
            "columns": columns,
            "file_size_bytes": total_size,
            "tool_name": tool_name,
            "params": params,
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
        while (
            self.metadata["total_size_bytes"] > self.max_size_bytes and sorted_entries
        ):
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
            "total_size_mb": round(
                self.metadata["total_size_bytes"] / (1024 * 1024), 2
            ),
            "total_size_gb": round(
                self.metadata["total_size_bytes"] / (1024 * 1024 * 1024), 2
            ),
            "max_size_gb": self.max_size_bytes / (1024 * 1024 * 1024),
            "ttl_days": self.ttl_days,
            "last_cleanup": self.metadata.get("last_cleanup"),
        }

    def infer_schema_from_csv(self, csv_data: str) -> Dict[str, str]:
        """
        Use DuckDB's CSV auto-detection to infer proper data types.

        This is more accurate than pattern matching on column names because
        it analyzes actual data values. DuckDB intelligently detects numeric,
        boolean, date, and timestamp types.

        Args:
            csv_data: CSV string data

        Returns:
            Dictionary mapping column names to DuckDB types (inferred from data)
        """
        import duckdb
        import tempfile
        import os

        # Write CSV to temp file for DuckDB to analyze
        with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as f:
            f.write(csv_data)
            temp_csv = f.name

        try:
            con = duckdb.connect()

            # Use DuckDB's auto-detection to infer schema
            result = con.execute(f"""
                DESCRIBE SELECT * FROM read_csv('{temp_csv}', AUTO_DETECT=TRUE)
            """).fetchall()

            schema = {}
            for col_name, col_type, *_ in result:
                schema[col_name] = col_type

            con.close()
            return schema

        finally:
            # Clean up temp file
            try:
                os.unlink(temp_csv)
            except OSError:
                pass

    def generate_query_examples(
        self, tool_name: str, params: Dict[str, Any], cache_location: str
    ) -> List[Dict[str, str]]:
        """
        Generate tool-specific DuckDB query examples.

        This is the single source of truth for query examples.
        The response_formatter should call this method instead of duplicating logic.

        Args:
            tool_name: Name of the tool
            params: Parameters used in API call
            cache_location: Glob pattern for parquet files

        Returns:
            List of query example dictionaries with 'description' and 'query' keys
        """
        examples = []

        # Time-series aggregates
        if tool_name in ["get_aggs", "list_aggs"]:
            examples = [
                {
                    "description": "View all data",
                    "query": f"SELECT * FROM read_parquet('{cache_location}') ORDER BY t",
                },
                {
                    "description": "Calculate daily returns",
                    "query": f"SELECT t, c as close, LAG(c) OVER (ORDER BY t) as prev_close, (c - LAG(c) OVER (ORDER BY t)) / LAG(c) OVER (ORDER BY t) * 100 as return_pct FROM read_parquet('{cache_location}') ORDER BY t",
                },
                {
                    "description": "Get summary statistics",
                    "query": f"SELECT COUNT(*) as days, MIN(l) as low, MAX(h) as high, AVG(c) as avg_close, SUM(v) as total_volume FROM read_parquet('{cache_location}')",
                },
            ]

        # Grouped daily aggregates (market-wide)
        elif tool_name == "get_grouped_daily_aggs":
            examples = [
                {
                    "description": "Top 20 gainers by percentage",
                    "query": f"SELECT T as ticker, c as close, todaysChangePerc FROM read_parquet('{cache_location}') WHERE todaysChangePerc > 0 ORDER BY todaysChangePerc DESC LIMIT 20",
                },
                {
                    "description": "Top 20 losers by percentage",
                    "query": f"SELECT T as ticker, c as close, todaysChangePerc FROM read_parquet('{cache_location}') WHERE todaysChangePerc < 0 ORDER BY todaysChangePerc ASC LIMIT 20",
                },
                {
                    "description": "Highest volume stocks",
                    "query": f"SELECT T as ticker, v as volume, c as close, todaysChangePerc FROM read_parquet('{cache_location}') ORDER BY v DESC LIMIT 20",
                },
            ]

        # Ticker listings
        elif tool_name in ["list_tickers", "get_all_tickers"]:
            examples = [
                {
                    "description": "Search by name",
                    "query": f"SELECT ticker, name, type, primary_exchange FROM read_parquet('{cache_location}') WHERE name ILIKE '%search_term%'",
                },
                {
                    "description": "Filter by exchange",
                    "query": f"SELECT ticker, name, type FROM read_parquet('{cache_location}') WHERE primary_exchange = 'XNAS' ORDER BY ticker",
                },
                {
                    "description": "Count by type",
                    "query": f"SELECT type, COUNT(*) as count FROM read_parquet('{cache_location}') GROUP BY type ORDER BY count DESC",
                },
            ]

        # Financials - Balance Sheets
        elif tool_name == "list_financials_balance_sheets":
            examples = [
                {
                    "description": "View latest balance sheet",
                    "query": f"SELECT * FROM read_parquet('{cache_location}') ORDER BY period_end DESC LIMIT 1",
                },
                {
                    "description": "Track asset growth",
                    "query": f"SELECT fiscal_year, fiscal_quarter, total_assets, LAG(total_assets) OVER (ORDER BY fiscal_year, fiscal_quarter) as prev_assets FROM read_parquet('{cache_location}') ORDER BY fiscal_year DESC, fiscal_quarter DESC",
                },
                {
                    "description": "Calculate key ratios",
                    "query": f"SELECT fiscal_year, fiscal_quarter, total_assets, total_liabilities, total_equity, ROUND(total_liabilities::DECIMAL / total_equity, 2) as debt_to_equity FROM read_parquet('{cache_location}') ORDER BY fiscal_year DESC, fiscal_quarter DESC",
                },
            ]

        # Financials - Income Statements
        elif tool_name == "list_financials_income_statements":
            examples = [
                {
                    "description": "View latest income statement",
                    "query": f"SELECT * FROM read_parquet('{cache_location}') ORDER BY period_end DESC LIMIT 1",
                },
                {
                    "description": "Revenue growth over time",
                    "query": f"SELECT fiscal_year, fiscal_quarter, revenue, LAG(revenue) OVER (ORDER BY fiscal_year, fiscal_quarter) as prev_revenue, ROUND((revenue - LAG(revenue) OVER (ORDER BY fiscal_year, fiscal_quarter)) / LAG(revenue) OVER (ORDER BY fiscal_year, fiscal_quarter) * 100, 2) as growth_pct FROM read_parquet('{cache_location}') ORDER BY fiscal_year DESC, fiscal_quarter DESC",
                },
                {
                    "description": "Calculate profit margins",
                    "query": f"SELECT fiscal_year, fiscal_quarter, revenue, operating_income, net_income_loss_attributable_common_shareholders as net_income, ROUND(operating_income::DECIMAL / revenue * 100, 2) as operating_margin, ROUND(net_income::DECIMAL / revenue * 100, 2) as net_margin FROM read_parquet('{cache_location}') ORDER BY fiscal_year DESC, fiscal_quarter DESC",
                },
            ]

        # Financials - Cash Flow
        elif tool_name == "list_financials_cash_flow_statements":
            examples = [
                {
                    "description": "View latest cash flow statement",
                    "query": f"SELECT * FROM read_parquet('{cache_location}') ORDER BY period_end DESC LIMIT 1",
                },
                {
                    "description": "Operating cash flow trend",
                    "query": f"SELECT fiscal_year, fiscal_quarter, net_cash_from_operating_activities, net_cash_from_investing_activities, net_cash_from_financing_activities FROM read_parquet('{cache_location}') ORDER BY fiscal_year DESC, fiscal_quarter DESC",
                },
                {
                    "description": "Calculate free cash flow",
                    "query": f"SELECT fiscal_year, fiscal_quarter, net_cash_from_operating_activities as operating_cf, purchase_of_property_plant_and_equipment as capex, (net_cash_from_operating_activities + purchase_of_property_plant_and_equipment) as free_cash_flow FROM read_parquet('{cache_location}') ORDER BY fiscal_year DESC, fiscal_quarter DESC",
                },
            ]

        # Corporate actions - Dividends
        elif tool_name == "list_dividends":
            examples = [
                {
                    "description": "Recent dividends",
                    "query": f"SELECT ex_dividend_date, cash_amount, frequency, dividend_type FROM read_parquet('{cache_location}') ORDER BY ex_dividend_date DESC LIMIT 10",
                },
                {
                    "description": "Dividend growth rate",
                    "query": f"SELECT ex_dividend_date, cash_amount, LAG(cash_amount) OVER (ORDER BY ex_dividend_date) as prev_amount, ROUND((cash_amount - LAG(cash_amount) OVER (ORDER BY ex_dividend_date)) / LAG(cash_amount) OVER (ORDER BY ex_dividend_date) * 100, 2) as growth_pct FROM read_parquet('{cache_location}') ORDER BY ex_dividend_date DESC",
                },
                {
                    "description": "Annual dividend summary",
                    "query": f"SELECT YEAR(ex_dividend_date::DATE) as year, SUM(cash_amount) as annual_dividend, COUNT(*) as payments FROM read_parquet('{cache_location}') GROUP BY year ORDER BY year DESC",
                },
            ]

        # Corporate actions - Splits
        elif tool_name == "list_splits":
            examples = [
                {
                    "description": "All splits",
                    "query": f"SELECT execution_date, split_from, split_to, ROUND(split_to::DECIMAL / split_from, 4) as split_ratio FROM read_parquet('{cache_location}') ORDER BY execution_date DESC",
                },
                {
                    "description": "Forward vs reverse splits",
                    "query": f"SELECT execution_date, split_from, split_to, CASE WHEN split_to > split_from THEN 'Forward' ELSE 'Reverse' END as split_type FROM read_parquet('{cache_location}') ORDER BY execution_date DESC",
                },
            ]

        # News
        elif tool_name == "list_ticker_news":
            examples = [
                {
                    "description": "Recent news",
                    "query": f"SELECT published_utc, title, author, article_url FROM read_parquet('{cache_location}') ORDER BY published_utc DESC LIMIT 20",
                },
                {
                    "description": "Sentiment analysis",
                    "query": f"SELECT published_utc::DATE as date, COUNT(*) as articles, SUM(CASE WHEN insights_sentiment = 'positive' THEN 1 ELSE 0 END) as positive, SUM(CASE WHEN insights_sentiment = 'negative' THEN 1 ELSE 0 END) as negative, SUM(CASE WHEN insights_sentiment = 'neutral' THEN 1 ELSE 0 END) as neutral FROM read_parquet('{cache_location}') GROUP BY date ORDER BY date DESC",
                },
            ]

        # Technical indicators
        elif tool_name in ["get_sma", "get_ema", "get_rsi"]:
            examples = [
                {
                    "description": "Recent values",
                    "query": f"SELECT * FROM read_parquet('{cache_location}') ORDER BY timestamp DESC LIMIT 20",
                },
                {
                    "description": "Current vs historical average",
                    "query": f"SELECT AVG(value) as avg_value, MIN(value) as min_value, MAX(value) as max_value FROM read_parquet('{cache_location}')",
                },
            ]

        # MACD
        elif tool_name == "get_macd":
            examples = [
                {
                    "description": "Recent MACD values",
                    "query": f"SELECT timestamp, value as macd, signal, histogram FROM read_parquet('{cache_location}') ORDER BY timestamp DESC LIMIT 20",
                },
                {
                    "description": "MACD crossovers (bullish signals)",
                    "query": f"SELECT timestamp, value as macd, signal, histogram FROM read_parquet('{cache_location}') WHERE LAG(histogram) OVER (ORDER BY timestamp) < 0 AND histogram > 0 ORDER BY timestamp DESC",
                },
            ]

        # Options
        elif tool_name in [
            "list_options_contracts",
            "get_options_chain_snapshot",
            "list_snapshot_options_chain",
        ]:
            examples = [
                {
                    "description": "Calls vs puts",
                    "query": f"SELECT details_contract_type as contract_type, COUNT(*) as count FROM read_parquet('{cache_location}') GROUP BY contract_type",
                },
                {
                    "description": "Highest open interest",
                    "query": f"SELECT CAST(details_strike_price AS DOUBLE) as strike_price, details_contract_type as contract_type, CAST(open_interest AS INTEGER) as oi, CAST(implied_volatility AS DOUBLE) as iv FROM read_parquet('{cache_location}') ORDER BY oi DESC LIMIT 20",
                },
                {
                    "description": "ATM options (near current price)",
                    "query": f"SELECT CAST(details_strike_price AS DOUBLE) as strike_price, details_contract_type as contract_type, CAST(open_interest AS INTEGER) as oi, CAST(implied_volatility AS DOUBLE) as iv, CAST(greeks_delta AS DOUBLE) as delta FROM read_parquet('{cache_location}') ORDER BY strike_price",
                },
            ]

        # Economics - Treasury Yields
        elif tool_name == "list_treasury_yields":
            examples = [
                {
                    "description": "Recent yield curve",
                    "query": f"SELECT * FROM read_parquet('{cache_location}') ORDER BY date DESC LIMIT 10",
                },
                {
                    "description": "Yield curve inversion (2Y vs 10Y)",
                    "query": f"SELECT date, yield_2_year, yield_10_year, (yield_2_year - yield_10_year) as inversion_spread FROM read_parquet('{cache_location}') WHERE yield_2_year > yield_10_year ORDER BY date DESC",
                },
            ]

        # Economics - Inflation
        elif tool_name == "list_inflation":
            examples = [
                {
                    "description": "Recent inflation data",
                    "query": f"SELECT * FROM read_parquet('{cache_location}') ORDER BY date DESC LIMIT 12",
                },
                {
                    "description": "CPI vs PCE comparison",
                    "query": f"SELECT date, consumer_price_index as cpi, personal_consumption_expenditures as pce FROM read_parquet('{cache_location}') ORDER BY date DESC",
                },
            ]

        # Stock ratios
        elif tool_name in ["list_stock_ratios", "list_financials_ratios"]:
            examples = [
                {
                    "description": "Current valuation ratios",
                    "query": f"SELECT ticker, price_to_earnings as pe, price_to_book as pb, price_to_sales as ps, dividend_yield FROM read_parquet('{cache_location}') LIMIT 1",
                },
                {
                    "description": "Profitability metrics",
                    "query": f"SELECT ticker, return_on_equity as roe, return_on_assets as roa FROM read_parquet('{cache_location}') LIMIT 1",
                },
            ]

        # Default examples if none matched
        if not examples:
            examples = [
                {
                    "description": "View all data",
                    "query": f"SELECT * FROM read_parquet('{cache_location}') LIMIT 100",
                },
                {
                    "description": "Count rows",
                    "query": f"SELECT COUNT(*) as total_rows FROM read_parquet('{cache_location}')",
                },
            ]

        return examples

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
