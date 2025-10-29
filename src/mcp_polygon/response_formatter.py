"""
Response formatting for Polygon MCP tools.

Handles decision between direct CSV return vs caching with DuckDB query instructions.
"""

from typing import Any, Dict, List, Optional
import json


class ResponseFormatter:
    """
    Formats tool responses based on caching decisions.

    Returns either:
    1. Direct CSV string for small/immediate results
    2. Structured cache metadata with DuckDB query examples for large/cached results
    """

    @staticmethod
    def format_direct(csv_data: str) -> str:
        """
        Format response for direct return (no caching).

        Args:
            csv_data: CSV string data

        Returns:
            CSV string as-is
        """
        return csv_data

    @staticmethod
    def format_cached(
        cache_metadata: Dict[str, Any],
        tool_name: str,
        params: Dict[str, Any],
        sample_rows: List[Dict[str, Any]],
    ) -> str:
        """
        Format response for cached data with DuckDB query instructions.

        Args:
            cache_metadata: Metadata from CacheManager.save()
            tool_name: Name of the tool
            params: Parameters used in API call
            sample_rows: First few rows of data for preview

        Returns:
            JSON string with cache location, schema, and query examples
        """
        # Generate tool-specific query examples
        query_examples = ResponseFormatter._generate_query_examples(
            tool_name, params, cache_metadata["cache_location"]
        )

        response = {
            "status": "cached",
            "message": f"Data cached successfully. Use the duckdb_query tool to analyze the data.",
            "cache_info": {
                "location": cache_metadata["cache_location"],
                "partition_key": cache_metadata["partition_key"],
                "row_count": cache_metadata["row_count"],
                "file_size_mb": round(cache_metadata["file_size_bytes"] / (1024 * 1024), 2),
            },
            "schema": {
                "columns": cache_metadata["columns"],
                "sample_rows": sample_rows[:3],  # First 3 rows
            },
            "query_examples": query_examples,
            "usage": {
                "tool": "duckdb_query",
                "description": "Use the duckdb_query tool with one of the example queries above, or write your own SQL query using DuckDB syntax.",
            },
        }

        return json.dumps(response, indent=2, default=str)

    @staticmethod
    def _generate_query_examples(
        tool_name: str, params: Dict[str, Any], cache_location: str
    ) -> List[Dict[str, str]]:
        """
        Generate tool-specific DuckDB query examples.

        Args:
            tool_name: Name of the tool
            params: Parameters used in API call
            cache_location: Glob pattern for parquet files

        Returns:
            List of query example dictionaries
        """
        examples = []

        # Time-series aggregates
        if tool_name in ["get_aggs", "list_aggs"]:
            ticker = params.get("ticker", "UNKNOWN")
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
            date = params.get("date", "today")
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
            market = params.get("market", "stocks")
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

        # Financials
        elif tool_name.startswith("list_financials_"):
            ticker = params.get("tickers", "UNKNOWN")

            if "balance_sheets" in tool_name:
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
            elif "income_statements" in tool_name:
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
            elif "cash_flow" in tool_name:
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

        # Corporate actions
        elif tool_name == "list_dividends":
            ticker = params.get("ticker", "UNKNOWN")
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

        elif tool_name == "list_splits":
            ticker = params.get("ticker", "UNKNOWN")
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
            ticker = params.get("ticker", "UNKNOWN")
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
            ticker = params.get("ticker", "UNKNOWN")
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

        elif tool_name == "get_macd":
            ticker = params.get("ticker", "UNKNOWN")
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
        elif tool_name in ["list_options_contracts", "get_options_chain_snapshot"]:
            underlying = params.get("underlying_ticker") or params.get("underlying_asset", "UNKNOWN")
            examples = [
                {
                    "description": "Calls vs puts",
                    "query": f"SELECT contract_type, COUNT(*) as count FROM read_parquet('{cache_location}') GROUP BY contract_type",
                },
                {
                    "description": "Highest open interest",
                    "query": f"SELECT strike_price, contract_type, open_interest, implied_volatility FROM read_parquet('{cache_location}') ORDER BY open_interest DESC LIMIT 20",
                },
                {
                    "description": "ATM options (near current price)",
                    "query": f"SELECT strike_price, contract_type, open_interest, implied_volatility, greeks_delta FROM read_parquet('{cache_location}') WHERE strike_price BETWEEN [stock_price - 10] AND [stock_price + 10] ORDER BY strike_price",
                },
            ]

        # Economics
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
            ticker = params.get("ticker", "UNKNOWN")
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
