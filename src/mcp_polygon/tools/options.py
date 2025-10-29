"""Auto-generated tool definitions."""

from typing import Optional, Any, Dict, Union, List
from mcp.types import ToolAnnotations
from datetime import datetime, date
from ..clients import poly_mcp, polygon_client
from ..formatters import json_to_csv
from ..tool_integration import process_tool_response
import json





@poly_mcp.tool(annotations=ToolAnnotations(readOnlyHint=True))
async def list_options_contracts(
    underlying_ticker: Optional[str] = None,
    contract_type: Optional[str] = None,
    expiration_date: Optional[Union[str, datetime, date]] = None,
    as_of: Optional[Union[str, datetime, date]] = None,
    strike_price: Optional[float] = None,
    expired: Optional[bool] = False,
    limit: Optional[int] = 10,
    sort: Optional[str] = None,
    order: Optional[str] = None,
    params: Optional[Dict[str, Any]] = None,
) -> str:
    """
    Retrieve a comprehensive index of options contracts, including both active and expired listings.
    Returns contract details including type (call/put), exercise style, expiration date, and strike price.

    Reference: https://polygon.io/docs/options/get_v3_reference_options_contracts

    Options contracts give the right (but not obligation) to buy (call) or sell (put) an underlying stock
    at a specified strike price before the expiration date. Use this endpoint to explore available contracts,
    analyze market characteristics, and develop trading strategies.

    Parameters:
    - underlying_ticker: Filter by underlying stock ticker (e.g., "AAPL", "TSLA")
    - contract_type: Filter by contract type ("call", "put")
    - expiration_date: Filter by exact expiration date (YYYY-MM-DD)
    - as_of: Specify point in time for contract listings (YYYY-MM-DD, defaults to today)
    - strike_price: Filter by exact strike price
    - expired: Include expired contracts (default: False, only active contracts)
    - limit: Number of results to return (default: 10, max: 1000)
    - sort: Sort field (e.g., "expiration_date", "strike_price")
    - order: Sort order ("asc" or "desc")
    - params: Additional filtering parameters.
      Use comparison operators like .gte, .gt, .lte, .lt with fields like:
      - underlying_ticker: Range filtering by ticker
      - expiration_date: Range filtering by expiration (e.g., expiration_date.gte)
      - strike_price: Range filtering by strike (e.g., strike_price.gte)

    Example: list_options_contracts(underlying_ticker="AAPL", contract_type="call", limit=50)
             gets 50 AAPL call options contracts
    Example: list_options_contracts(underlying_ticker="TSLA", params={"expiration_date.gte": "2025-06-01", "expiration_date.lte": "2025-12-31"})
             gets TSLA options expiring in second half of 2025
    Example: list_options_contracts(underlying_ticker="NVDA", contract_type="put", params={"strike_price.gte": 500, "strike_price.lte": 600})
             gets NVDA put options with strikes between $500-$600
    Example: list_options_contracts(underlying_ticker="SPY", expiration_date="2025-12-19", limit=100)
             gets all SPY options expiring on December 19, 2025

    Note: Understanding options contract details:
    - Ticker format: O:AAPL251219C00150000 = Options:Apple/Dec 19 2025/Call/$150 strike
    - Exercise style: American (can exercise anytime), European (only at expiration)
    - Shares per contract: Typically 100 shares per contract
    - Additional underlyings: Some contracts may have adjusted deliverables due to corporate actions
      (stock splits, mergers, spinoffs) - check additional_underlyings field
    - CFI code: ISO 10962 standard identifier for contract classification

    Use case: Options chain analysis - retrieve all contracts for a ticker to analyze
    strike distribution, identify liquid strikes, and spot unusual activity.
    """
    try:
        # Build the params dictionary
        request_params = params or {}
        if underlying_ticker:
            request_params["underlying_ticker"] = underlying_ticker
        if contract_type:
            request_params["contract_type"] = contract_type
        if expiration_date:
            request_params["expiration_date"] = expiration_date
        if as_of:
            request_params["as_of"] = as_of
        if strike_price is not None:
            request_params["strike_price"] = strike_price
        if expired is not None:
            request_params["expired"] = expired
        if limit:
            request_params["limit"] = limit
        if sort:
            request_params["sort"] = sort
        if order:
            request_params["order"] = order

        # Make the request to the options contracts endpoint
        results = polygon_client._get(
            "/v3/reference/options/contracts", params=request_params
        )

        return json_to_csv(results)
    except Exception as e:
        return f"Error: {e}"





@poly_mcp.tool(annotations=ToolAnnotations(readOnlyHint=True))
async def get_options_contract(
    options_ticker: str,
    as_of: Optional[Union[str, datetime, date]] = None,
) -> str:
    """
    Retrieve detailed information about a specific options contract by its ticker symbol.
    Returns contract specifications including type, strike, expiration, exercise style, and underlying details.

    Reference: https://polygon.io/docs/options/get_v3_reference_options_contracts__options_ticker

    Use this endpoint to get complete specifications for a known options contract. Essential for validating
    contract details, understanding deliverables, and integrating contracts into trading strategies.

    Parameters:
    - options_ticker: Options contract ticker (required)
      Format: O:SYMBOL[YY][MM][DD][C/P][STRIKE]
      Example: "O:AAPL251219C00150000" = Apple Dec 19 2025 Call $150 strike
    - as_of: Specify point in time for contract details (YYYY-MM-DD, defaults to today)
      Useful for retrieving historical contract specifications

    Options ticker format breakdown:
    - O: = Options prefix
    - AAPL = Underlying ticker symbol
    - 251219 = Expiration date (Dec 19, 2025 in YYMMDD format)
    - C = Contract type (C=Call, P=Put)
    - 00150000 = Strike price ($150.00 with 3 decimal precision)

    Example: get_options_contract(options_ticker="O:AAPL251219C00150000")
             gets details for Apple $150 call expiring Dec 19, 2025
    Example: get_options_contract(options_ticker="O:TSLA250620P00700000")
             gets details for Tesla $700 put expiring June 20, 2025
    Example: get_options_contract(options_ticker="O:SPY251219C00500000", as_of="2024-01-01")
             gets historical contract details as of January 1, 2024
    Example: get_options_contract(options_ticker="O:NVDA250117C01000000")
             gets details for NVIDIA $1000 call expiring Jan 17, 2025

    Response includes:
    - contract_type: "call", "put", or "other"
    - strike_price: Exercise price of the option
    - expiration_date: Contract expiration date (YYYY-MM-DD)
    - exercise_style: "american", "european", or "bermudan"
      - American: Can exercise anytime before expiration (most U.S. stock options)
      - European: Can only exercise at expiration
      - Bermudan: Can exercise on specific dates
    - shares_per_contract: Number of shares controlled (typically 100)
    - underlying_ticker: Stock symbol the option is based on
    - primary_exchange: MIC code of listing exchange
    - additional_underlyings: Adjusted deliverables from corporate actions
      - May include cash, additional stock from mergers/spinoffs
      - Example: Stock split might result in non-standard deliverables
    - cfi: ISO 10962 CFI code for contract classification

    Note: Additional underlyings appear when corporate actions modify standard deliverables.
    Examples include mergers, acquisitions, spinoffs, special dividends, and stock splits.
    Always check this field before trading to understand exact deliverables.

    Use case: Before executing an options trade, verify contract specifications to ensure
    the strike price, expiration date, and deliverables match your strategy requirements.
    """
    try:
        results = polygon_client.get_options_contract(
            ticker=options_ticker, as_of=as_of, raw=True
        )

        # Parse the response and extract the results object
        import json

        data = json.loads(results.data.decode("utf-8"))
        if "results" in data:
            # Wrap the results object in an array for CSV formatting
            formatted_data = {"results": [data["results"]]}
            return json_to_csv(formatted_data)
        # Convert to CSV
        csv_data = json_to_csv(results.data.decode("utf-8"))

        # Process with intelligent caching
        return await process_tool_response(
            tool_name="list_options_contracts",
            params={
                "underlying_ticker": underlying_ticker,
                "expiration_date": expiration_date,
                "limit": limit,
            },
            csv_data=csv_data,
        )
    except Exception as e:
        return f"Error: {e}"





@poly_mcp.tool(annotations=ToolAnnotations(readOnlyHint=True))
async def get_options_aggs(
    options_ticker: str,
    multiplier: int,
    timespan: str,
    from_: Union[str, int, datetime, date],
    to: Union[str, int, datetime, date],
    adjusted: Optional[bool] = True,
    sort: Optional[str] = None,
    limit: Optional[int] = 5000,
    params: Optional[Dict[str, Any]] = None,
) -> str:
    """
    Get aggregate OHLC bars for an options contract over a custom date range and time interval.
    Returns open, high, low, close, volume, and VWAP data for the specified time periods in Eastern Time (ET).

    Reference: https://polygon.io/docs/options/get_v2_aggs_ticker__optionsticker__range__multiplier___timespan___from___to

    Aggregates are derived from qualifying trades only. Empty intervals indicate no trading activity.
    Perfect for technical analysis, backtesting, and visualization of options price movements.

    Parameters:
    - options_ticker: Options contract ticker (e.g., "O:AAPL251219C00150000")
    - multiplier: Size of the timespan multiplier (e.g., 1 for 1 day, 5 for 5 minutes)
    - timespan: Size of the time window (minute, hour, day, week, month, quarter, year)
    - from_: Start date (YYYY-MM-DD) or timestamp in milliseconds
    - to: End date (YYYY-MM-DD) or timestamp in milliseconds
    - adjusted: Whether results are adjusted for splits (default: True)
    - sort: Sort order - "asc" (oldest first) or "desc" (newest first)
    - limit: Max base aggregates to query (default: 5000, max: 50000)
    - params: Additional query parameters

    Example: get_options_aggs(options_ticker="O:AAPL251219C00150000", multiplier=1, timespan="day", from_="2025-01-01", to="2025-03-31")
             gets daily OHLC data for Apple $150 call from Jan-Mar 2025
    Example: get_options_aggs(options_ticker="O:SPY251219C00500000", multiplier=5, timespan="minute", from_="2025-03-20", to="2025-03-20")
             gets 5-minute intraday bars for SPY $500 call on March 20, 2025
    Example: get_options_aggs(options_ticker="O:TSLA250620P00700000", multiplier=1, timespan="hour", from_="2025-06-01", to="2025-06-30")
             gets hourly bars for Tesla $700 put during June 2025
    Example: get_options_aggs(options_ticker="O:NVDA250117C01000000", multiplier=1, timespan="week", from_="2024-01-01", to="2024-12-31", limit=10000)
             gets weekly bars for NVIDIA $1000 call for all of 2024

    Response fields:
    - c: Close price (last trade price in the period)
    - h: High price (highest trade price in the period)
    - l: Low price (lowest trade price in the period)
    - o: Open price (first trade price in the period)
    - t: Timestamp (Unix milliseconds for the start of the aggregate window)
    - v: Volume (number of contracts traded)
    - vw: VWAP (Volume Weighted Average Price)
    - n: Number of trades (transactions count)

    Note: Options aggregate data considerations:
    - Times are in Eastern Time (ET), not UTC
    - Empty bars indicate no qualifying trades occurred in that period
    - Options can be less liquid than stocks - expect more gaps in data
    - Adjusted vs unadjusted: Most options don't need split adjustments, but use adjusted=True
      for contracts that may have been affected by underlying stock corporate actions
    - Intraday bars (minute/hour) capture detailed price movement for active contracts
    - Daily/weekly bars better for longer-term analysis and less liquid contracts

    Use case: Analyze options contract price behavior around earnings announcements by
    pulling hourly bars for the week surrounding the event date.
    """
    try:
        results = polygon_client.get_aggs(
            ticker=options_ticker,
            multiplier=multiplier,
            timespan=timespan,
            from_=from_,
            to=to,
            adjusted=adjusted,
            sort=sort,
            limit=limit,
            params=params,
            raw=True,
        )

        return json_to_csv(results.data.decode("utf-8"))
    except Exception as e:
        return f"Error: {e}"





@poly_mcp.tool(annotations=ToolAnnotations(readOnlyHint=True))
async def get_options_daily_open_close(
    options_ticker: str,
    date: Union[str, datetime, date],
    adjusted: Optional[bool] = True,
) -> str:
    """
    Get the daily open, close, high, low, and volume for a specific options contract on a given date.
    Includes pre-market and after-hours pricing when available.

    Reference: https://polygon.io/docs/options/get_v1_open-close__optionsticker___date

    Essential for daily performance analysis, historical data collection, and understanding
    trading activity outside regular market hours.

    Parameters:
    - options_ticker: Options contract ticker (e.g., "O:AAPL251219C00150000")
    - date: The date for the requested open/close data (YYYY-MM-DD)
    - adjusted: Whether results are adjusted for splits (default: True)

    Example: get_options_daily_open_close(options_ticker="O:AAPL251219C00150000", date="2025-03-20")
             gets daily OHLC for Apple $150 call on March 20, 2025
    Example: get_options_daily_open_close(options_ticker="O:TSLA250620P00700000", date="2025-06-15")
             gets daily OHLC for Tesla $700 put on June 15, 2025
    Example: get_options_daily_open_close(options_ticker="O:SPY251219C00500000", date="2025-01-15", adjusted=False)
             gets unadjusted daily OHLC for SPY $500 call
    Example: get_options_daily_open_close(options_ticker="O:NVDA250117C01000000", date="2025-01-10")
             gets daily OHLC for NVIDIA $1000 call

    Response includes:
    - open: Opening price for the day
    - high: Highest price during the day
    - low: Lowest price during the day
    - close: Closing price for the day
    - volume: Total contracts traded
    - preMarket: Open price during pre-market trading (if available)
    - afterHours: Close price during after-hours trading (if available)
    - from: The requested date
    - symbol: Options ticker
    - otc: Whether this is an OTC ticker (field omitted if false)

    Note: Daily open/close data considerations:
    - Single snapshot of the entire trading day's activity
    - Pre-market and after-hours data provide extended trading insights
    - Volume shows total contract activity for the day
    - Useful for quickly checking daily performance without full aggregate data
    - Less granular than get_options_aggs but faster for single-day lookups
    - Perfect for building daily history or tracking specific dates

    Use case: After an earnings announcement, check the daily open/close to see how
    options contracts responded - compare regular hours vs after-hours pricing to
    gauge market reaction timing.
    """
    try:
        results = polygon_client.get_daily_open_close_agg(
            ticker=options_ticker,
            date=date,
            adjusted=adjusted,
            raw=True,
        )

        return json_to_csv(results.data.decode("utf-8"))
    except Exception as e:
        return f"Error: {e}"





@poly_mcp.tool(annotations=ToolAnnotations(readOnlyHint=True))
async def get_options_previous_close(
    options_ticker: str,
    adjusted: Optional[bool] = True,
) -> str:
    """
    Get the previous trading day's OHLC (open, high, low, close) data for a specified options contract.
    Provides key pricing metrics and volume to assess recent performance and inform trading strategies.

    Reference: https://polygon.io/docs/options/get_v2_aggs_ticker__optionsticker__prev

    Essential for baseline comparisons, technical analysis, market research, and daily reporting.
    Use this for quick lookups of the most recent completed trading day.

    Parameters:
    - options_ticker: Options contract ticker (e.g., "O:TSLA210903C00700000")
    - adjusted: Whether results are adjusted for splits (default: True)

    Example: get_options_previous_close(options_ticker="O:TSLA210903C00700000")
             gets previous day OHLC for Tesla call option
    Example: get_options_previous_close(options_ticker="O:AAPL251219C00150000")
             gets previous day OHLC for Apple $150 call
    Example: get_options_previous_close(options_ticker="O:SPY251219P00450000", adjusted=False)
             gets unadjusted previous day OHLC for SPY $450 put
    Example: get_options_previous_close(options_ticker="O:NVDA250117C01000000")
             gets previous day OHLC for NVIDIA $1000 call

    Response includes:
    - T: Ticker symbol
    - o: Opening price
    - h: Highest price
    - l: Lowest price
    - c: Closing price
    - v: Total volume (contracts traded)
    - vw: Volume weighted average price
    - n: Number of transactions
    - t: Timestamp for the aggregate window

    Note: Previous day considerations:
    - Returns data for the most recent completed trading day
    - Faster than querying aggregates with specific date ranges
    - Perfect for quick baseline comparisons and daily reports
    - Volume shows total contract activity for that trading day
    - Use for technical analysis requiring yesterday's OHLC
    - Ideal for calculating daily changes and percentage moves

    Use case: Before the market opens, check previous day's close for key options contracts
    to establish baseline levels and identify overnight gaps when today's trading begins.
    """
    try:
        results = polygon_client.get_previous_close_agg(
            ticker=options_ticker,
            adjusted=adjusted,
            raw=True,
        )

        return json_to_csv(results.data.decode("utf-8"))
    except Exception as e:
        return f"Error: {e}"





@poly_mcp.tool(annotations=ToolAnnotations(readOnlyHint=True))
async def get_options_snapshot(
    underlying_asset: str,
    option_contract: str,
) -> str:
    """
    Get a comprehensive snapshot of an options contract with market data, greeks, and underlying asset info.
    Consolidates vital metrics including break-even price, implied volatility, open interest, greeks,
    latest quote/trade, and underlying asset price into a single response.

    Reference: https://polygon.io/docs/options/get_v3_snapshot_options__underlyingasset___optioncontract

    Essential for trade evaluation, market analysis, risk assessment, and strategy refinement.
    Provides a complete view of an options contract's current state and value.

    Parameters:
    - underlying_asset: Underlying ticker symbol (e.g., "AAPL", "TSLA", "SPY")
    - option_contract: Option contract identifier (e.g., "O:AAPL230616C00150000")

    Example: get_options_snapshot(underlying_asset="AAPL", option_contract="O:AAPL230616C00150000")
             gets full snapshot for Apple $150 call expiring June 16, 2023
    Example: get_options_snapshot(underlying_asset="TSLA", option_contract="O:TSLA210903C00700000")
             gets snapshot for Tesla $700 call with greeks and IV
    Example: get_options_snapshot(underlying_asset="SPY", option_contract="O:SPY251219P00450000")
             gets snapshot for SPY $450 put with break-even analysis
    Example: get_options_snapshot(underlying_asset="NVDA", option_contract="O:NVDA250117C01000000")
             gets snapshot for NVIDIA $1000 call with underlying asset price

    Response includes:
    - break_even_price: Price for contract to break even (strike + premium for calls, strike - premium for puts)
    - day: Most recent daily bar (open, high, low, close, volume, vwap, change, change_percent)
    - details: Contract specifications (type, style, expiration, strike, shares per contract)
    - greeks: Delta, gamma, theta, vega (if available, may be missing for deep ITM/OTM)
    - implied_volatility: Market's forecast for underlying volatility
    - last_quote: Most recent bid/ask with sizes and exchanges (if plan includes quotes)
    - last_trade: Most recent trade with price, size, conditions (if plan includes trades)
    - open_interest: Contracts held at end of last trading day
    - underlying_asset: Current underlying stock price and change to break-even
    - fmv: Fair Market Value (Business plans only)

    Note: Snapshot data considerations:
    - Real-time or near-real-time data depending on your plan
    - Greeks may not be available for deep in-the-money or out-of-the-money options
    - Break-even calculation includes premium paid
    - Implied volatility reflects current market pricing expectations
    - Quote and trade data availability depends on subscription tier
    - Underlying asset data shows how far stock needs to move to reach break-even
    - Perfect for quick contract evaluation before entering trades

    Use case: Before buying a call option, check the snapshot to see current implied volatility,
    greeks (especially delta for directional exposure), and break-even price relative to current
    underlying price to assess risk/reward and probability of profit.
    """
    try:
        results = polygon_client.get_snapshot_option(
            underlying_asset=underlying_asset,
            option_contract=option_contract,
            raw=True,
        )

        # Parse the response and extract the results object
        import json

        data = json.loads(results.data.decode("utf-8"))
        if "results" in data:
            # Wrap the results object in an array for CSV formatting
            formatted_data = {"results": [data["results"]]}
            return json_to_csv(formatted_data)
        return json_to_csv(results.data.decode("utf-8"))
    except Exception as e:
        return f"Error: {e}"





@poly_mcp.tool(annotations=ToolAnnotations(readOnlyHint=True))
async def get_options_chain_snapshot(
    underlying_asset: str,
    strike_price: Optional[float] = None,
    expiration_date: Optional[Union[str, date]] = None,
    contract_type: Optional[str] = None,
    strike_price_gte: Optional[float] = None,
    strike_price_gt: Optional[float] = None,
    strike_price_lte: Optional[float] = None,
    strike_price_lt: Optional[float] = None,
    expiration_date_gte: Optional[Union[str, date]] = None,
    expiration_date_gt: Optional[Union[str, date]] = None,
    expiration_date_lte: Optional[Union[str, date]] = None,
    expiration_date_lt: Optional[Union[str, date]] = None,
    order: Optional[str] = None,
    limit: Optional[int] = 10,
    sort: Optional[str] = None,
    params: Optional[Dict[str, Any]] = None,
) -> str:
    """
    Get comprehensive snapshots of all options contracts for a specified underlying asset.
    Returns the full options chain with pricing, greeks, implied volatility, quotes, trades,
    and open interest for each contract. Filter by strike price, expiration, and contract type.

    Reference: https://polygon.io/docs/options/get_v3_snapshot_options__underlyingasset

    Essential for market overview, strategy comparison, research/modeling, and portfolio refinement.
    Examine the entire options chain in a single request to evaluate market conditions and compare contracts.

    Parameters:
    - underlying_asset: Underlying ticker symbol (e.g., "AAPL", "TSLA", "SPY")
    - strike_price: Filter by exact strike price
    - expiration_date: Filter by expiration date (YYYY-MM-DD)
    - contract_type: Filter by contract type ("call" or "put")
    - strike_price_gte: Strike price greater than or equal to
    - strike_price_gt: Strike price greater than
    - strike_price_lte: Strike price less than or equal to
    - strike_price_lt: Strike price less than
    - expiration_date_gte: Expiration date greater than or equal to
    - expiration_date_gt: Expiration date greater than
    - expiration_date_lte: Expiration date less than or equal to
    - expiration_date_lt: Expiration date less than
    - order: Order results based on sort field ("asc" or "desc")
    - limit: Number of results to return (default: 10, max: 250)
    - sort: Field to sort by (e.g., "strike_price", "expiration_date")
    - params: Additional query parameters

    Example: get_options_chain_snapshot(underlying_asset="AAPL", contract_type="call", limit=50)
             gets all call options for Apple (up to 50 contracts)
    Example: get_options_chain_snapshot(underlying_asset="TSLA", expiration_date="2025-06-20", limit=100)
             gets all options expiring on June 20, 2025 for Tesla
    Example: get_options_chain_snapshot(underlying_asset="SPY", strike_price_gte=450, strike_price_lte=500, contract_type="put")
             gets SPY put options with strikes between $450-$500
    Example: get_options_chain_snapshot(underlying_asset="NVDA", expiration_date_gte="2025-01-17", expiration_date_lte="2025-03-21", sort="strike_price", order="asc")
             gets NVIDIA options expiring between Jan-Mar 2025, sorted by strike

    Response includes (for each contract):
    - break_even_price: Price for contract to break even
    - day: Most recent daily bar (OHLC, volume, change)
    - details: Contract specifications (type, style, expiration, strike)
    - greeks: Delta, gamma, theta, vega (when available)
    - implied_volatility: Market's volatility forecast
    - last_quote: Most recent bid/ask with sizes
    - last_trade: Most recent trade details
    - open_interest: Contracts held at end of last trading day
    - underlying_asset: Current stock price and change to break-even
    - fmv: Fair Market Value (Business plans only)

    Note: Options chain considerations:
    - Returns multiple contracts in a single request (up to 250)
    - Filter to find specific strategies (e.g., ATM calls, vertical spreads)
    - Compare implied volatility across strikes to identify skew
    - Analyze open interest to gauge market positioning
    - Use strike price ranges to focus on tradable strikes
    - Expiration date filters help analyze specific time horizons
    - Greeks show how contracts respond to market changes
    - Perfect for constructing multi-leg strategies

    Use case: Planning a bull call spread on AAPL - filter for calls expiring in 30-60 days
    with strikes around current price, compare implied volatility and delta across strikes
    to select optimal long and short legs for the spread.
    """
    try:
        results = polygon_client.list_snapshot_options_chain(
            underlying_asset=underlying_asset,
            params={
                **(params or {}),
                **{
                    k: v
                    for k, v in {
                        "strike_price": strike_price,
                        "expiration_date": expiration_date,
                        "contract_type": contract_type,
                        "strike_price.gte": strike_price_gte,
                        "strike_price.gt": strike_price_gt,
                        "strike_price.lte": strike_price_lte,
                        "strike_price.lt": strike_price_lt,
                        "expiration_date.gte": expiration_date_gte,
                        "expiration_date.gt": expiration_date_gt,
                        "expiration_date.lte": expiration_date_lte,
                        "expiration_date.lt": expiration_date_lt,
                        "order": order,
                        "limit": limit,
                        "sort": sort,
                    }.items()
                    if v is not None
                },
            },
            raw=True,
        )

        # Parse the JSON response to calculate GEX
        data = json.loads(results.data.decode("utf-8"))

        # Extract options data
        options_list = data.get("results", [])

        # Get current stock price by fetching the underlying ticker snapshot
        stock_price = None
        try:
            snapshot_result = polygon_client.get_snapshot_ticker(
                market_type="stocks",
                ticker=underlying_asset,
                raw=True,
            )
            snapshot_data = json.loads(snapshot_result.data.decode("utf-8"))
            if "ticker" in snapshot_data and "day" in snapshot_data["ticker"]:
                stock_price = snapshot_data["ticker"]["day"].get("c")  # Closing price
            # Fallback: try prevDay close if day close not available
            if (
                not stock_price
                and "ticker" in snapshot_data
                and "prevDay" in snapshot_data["ticker"]
            ):
                stock_price = snapshot_data["ticker"]["prevDay"].get("c")
        except Exception as e:
            # If we can't get the stock price, continue without enrichment
            import sys

            print(
                f"Warning: Could not fetch stock price for {underlying_asset}: {e}",
                file=sys.stderr,
            )

        # Enrich options data with GEX and advanced Greeks
        if stock_price and options_list:
            enriched_options = enrich_options_with_gex_and_advanced_greeks(
                options_list, stock_price
            )
            data["results"] = enriched_options

        # Convert to CSV
        csv_data = json_to_csv(data)

        # Process with intelligent caching
        return await process_tool_response(
            tool_name="get_options_chain_snapshot",
            params={
                "underlying_asset": underlying_asset,
                "strike_price": strike_price,
                "expiration_date": str(expiration_date) if expiration_date else None,
                "contract_type": contract_type,
                "limit": limit,
            },
            csv_data=csv_data,
        )

    except Exception as e:
        return f"Error: {e}"


# @poly_mcp.tool(annotations=ToolAnnotations(readOnlyHint=True))  # DISABLED
