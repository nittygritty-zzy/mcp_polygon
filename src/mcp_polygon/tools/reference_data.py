"""Auto-generated tool definitions."""

from typing import Optional, Any, Dict, Union, List
from mcp.types import ToolAnnotations
from datetime import datetime, date
from ..clients import poly_mcp, polygon_client
from ..formatters import json_to_csv
from ..tool_integration import process_tool_response





@poly_mcp.tool(annotations=ToolAnnotations(readOnlyHint=True))
async def get_market_holidays(
    params: Optional[Dict[str, Any]] = None,
) -> str:
    """
    Get upcoming market holidays and special market hours (early close days).
    Returns details about when US stock markets are closed or have modified hours.

    Reference: https://polygon.io/docs/rest/stocks/market-operations/market-holidays

    Returns information about:
    - Market holiday dates (when markets are closed)
    - Holiday names
    - Early close days (when markets close early)
    - Modified trading hours

    Example: get_market_holidays() returns all upcoming market holidays and special hours

    Note: Useful for planning trading strategies and understanding when markets will be closed.
    Common US market holidays include New Year's Day, Independence Day, Thanksgiving, and Christmas.
    """
    try:
        results = polygon_client.get_market_holidays(params=params, raw=True)

        # Convert to CSV
        csv_data = json_to_csv(results.data.decode("utf-8"))

        # Process with intelligent caching
        return await process_tool_response(
            tool_name="get_market_holidays",
            params={},
            csv_data=csv_data,
        )
    except Exception as e:
        return f"Error: {e}"





@poly_mcp.tool(annotations=ToolAnnotations(readOnlyHint=True))
async def get_market_status(
    params: Optional[Dict[str, Any]] = None,
) -> str:
    """
    Get current trading status of exchanges and financial markets.
    """
    try:
        results = polygon_client.get_market_status(params=params, raw=True)

        return json_to_csv(results.data.decode("utf-8"))
    except Exception as e:
        return f"Error: {e}"





@poly_mcp.tool(annotations=ToolAnnotations(readOnlyHint=True))
async def list_tickers(
    ticker: Optional[str] = None,
    type: Optional[str] = None,
    market: Optional[str] = None,
    exchange: Optional[str] = None,
    cusip: Optional[str] = None,
    cik: Optional[str] = None,
    date: Optional[Union[str, datetime, date]] = None,
    search: Optional[str] = None,
    active: Optional[bool] = None,
    ticker_gte: Optional[str] = None,
    ticker_gt: Optional[str] = None,
    ticker_lte: Optional[str] = None,
    ticker_lt: Optional[str] = None,
    sort: Optional[str] = None,
    order: Optional[str] = None,
    limit: Optional[int] = 10,
    params: Optional[Dict[str, Any]] = None,
) -> str:
    """
    Retrieve a comprehensive list of ticker symbols supported by Polygon.io across various asset classes.
    Each ticker entry provides essential details such as symbol, name, market, currency, and active status.

    Reference: https://polygon.io/docs/rest/stocks/tickers/all-tickers

    Essential for asset discovery, data integration, filtering/selection, and application development.
    Query tickers across stocks, options, indices, forex, and crypto markets with advanced filtering.

    Parameters:
    - ticker: Specific ticker symbol to query (defaults to all tickers)
    - type: Filter by ticker type (CS=Common Stock, ETF, ADRC, etc. - see Ticker Types API)
    - market: Filter by market type (stocks, crypto, fx, otc, indices)
    - exchange: Primary exchange MIC code (e.g., XNYS, XNAS) per ISO 10383
    - cusip: CUSIP code of the asset (note: not returned in response due to legal reasons)
    - cik: CIK code from SEC EDGAR system
    - date: Point in time to retrieve tickers available on that date (defaults to most recent)
    - search: Search terms within ticker and/or company name
    - active: Filter for actively traded tickers (default: None, which returns all)
    - ticker_gte: Ticker greater than or equal to (lexicographic range)
    - ticker_gt: Ticker greater than (lexicographic range)
    - ticker_lte: Ticker less than or equal to (lexicographic range)
    - ticker_lt: Ticker less than (lexicographic range)
    - sort: Field to sort by (e.g., "ticker", "name")
    - order: Sort order ("asc" or "desc")
    - limit: Number of results to return (default: 10, max: 1000)
    - params: Additional query parameters

    Example: list_tickers(market="stocks", active=True, limit=100)
             gets first 100 active stock tickers
    Example: list_tickers(search="Apple", market="stocks")
             searches for Apple-related stock tickers
    Example: list_tickers(type="ETF", active=True, limit=50)
             gets first 50 active ETFs
    Example: list_tickers(ticker_gte="A", ticker_lt="B", market="stocks")
             gets all stock tickers starting with 'A'
    Example: list_tickers(exchange="XNAS", market="stocks", limit=100)
             gets first 100 stocks from NASDAQ
    Example: list_tickers(cik="0001090872")
             gets tickers for specific CIK (Agilent Technologies)

    Response includes:
    - ticker: Exchange symbol
    - name: Asset name (company name for stocks, currency pair for forex/crypto)
    - market: Market type (stocks, crypto, fx, otc, indices)
    - type: Asset type (CS, ETF, ADRC, etc.)
    - active: Whether actively traded (false means delisted)
    - primary_exchange: ISO code of primary listing exchange
    - currency_symbol/currency_name: Trading currency (ISO 4217)
    - base_currency_symbol/base_currency_name: Pricing currency (for forex/crypto)
    - locale: Asset locale (us, global)
    - cik: Central Index Key for SEC filings
    - composite_figi/share_class_figi: OpenFIGI identifiers
    - last_updated_utc: Data freshness timestamp
    - delisted_utc: Last trading date (if delisted)

    Note: Ticker listing considerations:
    - Query by CUSIP is supported but CUSIP not returned in response (legal restrictions)
    - Default limit is 10, max is 1000 per request
    - Use pagination (next_url) for large result sets
    - Active=true filters to currently trading assets only
    - Date parameter allows historical ticker lookups
    - Ticker range queries enable efficient alphabetical scanning
    - Search matches both ticker symbol and company name
    - Exchange parameter uses Market Identifier Code (MIC) standard
    - Type values vary by market (use Ticker Types API for complete list)

    Use case: Building a stock screener - query all active US stocks on NASDAQ exchange,
    then filter by specific criteria like type (CS for common stock, ETF for funds), and
    use ticker range queries to paginate through results alphabetically for efficient data loading.
    """
    try:
        results = polygon_client.list_tickers(
            ticker=ticker,
            type=type,
            market=market,
            exchange=exchange,
            cusip=cusip,
            cik=cik,
            date=date,
            search=search,
            active=active,
            sort=sort,
            order=order,
            limit=limit,
            params={
                **(params or {}),
                **{
                    k: v
                    for k, v in {
                        "ticker.gte": ticker_gte,
                        "ticker.gt": ticker_gt,
                        "ticker.lte": ticker_lte,
                        "ticker.lt": ticker_lt,
                    }.items()
                    if v is not None
                },
            },
            raw=True,
        )

        # Convert to CSV
        csv_data = json_to_csv(results.data.decode("utf-8"))

        # Process with intelligent caching
        return await process_tool_response(
            tool_name="list_tickers",
            params={
                "market": market,
                "active": active,
                "limit": limit,
            },
            csv_data=csv_data,
        )
    except Exception as e:
        return f"Error: {e}"





@poly_mcp.tool(annotations=ToolAnnotations(readOnlyHint=True))
async def get_all_tickers(
    market: Optional[str] = None,
    type: Optional[str] = None,
    active: Optional[bool] = True,
    limit: Optional[int] = 100,
    sort: Optional[str] = "ticker",
    order: Optional[str] = "asc",
    params: Optional[Dict[str, Any]] = None,
) -> str:
    """
    Get all ticker symbols with optional filtering by market type and active status.
    This is a simplified interface to the Tickers endpoint (https://polygon.io/docs/rest/stocks/tickers/all-tickers).

    Common parameters:
    - market: Filter by market type (stocks, crypto, fx, otc, indices)
    - type: Filter by security type (CS=Common Stock, ETF, etc.)
    - active: Include only active tickers (default: True)
    - limit: Number of results to return (default: 100)
    """
    try:
        results = polygon_client.list_tickers(
            market=market,
            type=type,
            active=active,
            sort=sort,
            order=order,
            limit=limit,
            params=params,
            raw=True,
        )

        return json_to_csv(results.data.decode("utf-8"))
    except Exception as e:
        return f"Error: {e}"





@poly_mcp.tool(annotations=ToolAnnotations(readOnlyHint=True))
async def get_ticker_details(
    ticker: str,
    date: Optional[Union[str, datetime, date]] = None,
    params: Optional[Dict[str, Any]] = None,
) -> str:
    """
    Retrieve comprehensive details for a single ticker supported by Polygon.io. This endpoint offers
    a deep look into a company's fundamental attributes, including primary exchange, standardized
    identifiers (CIK, FIGI), market cap, industry classification, and key dates. Also provides
    branding assets (logos, icons) for visual identification.

    Reference: https://polygon.io/docs/rest/stocks/tickers/ticker-overview

    Essential for company research, data integration, application enhancement, due diligence,
    and compliance. Provides rich fundamental data and visual assets in a single request.

    Parameters:
    - ticker: Case-sensitive ticker symbol (e.g., "AAPL", "MSFT", "GOOGL")
    - date: Point in time to get ticker information (YYYY-MM-DD, defaults to most recent)
           When querying SEC filings, this date is compared with the period of report date.
           For example, an SEC filing submitted 2019-07-31 with period ending 2019-06-29
           would be returned when querying date=2019-06-29
    - params: Additional query parameters

    Example: get_ticker_details("AAPL")
             gets comprehensive Apple Inc. details with latest data
    Example: get_ticker_details("MSFT", date="2020-01-15")
             gets Microsoft details as of January 15, 2020
    Example: get_ticker_details("GOOGL")
             gets Alphabet/Google details with branding assets
    Example: get_ticker_details("TSLA")
             gets Tesla details including market cap, employees, SIC code

    Response includes:
    - ticker: Exchange symbol (ticker root and suffix if applicable)
    - name: Company registered name
    - market: Market type (stocks, crypto, fx, otc, indices)
    - type: Asset type (CS=Common Stock, ETF, ADRC, etc.)
    - active: Whether actively traded (false means delisted)
    - locale: Asset locale (us, global)
    - primary_exchange: ISO code of primary listing exchange
    - currency_name: Trading currency
    - description: Detailed company description and business overview
    - homepage_url: Company website
    - list_date: Date symbol was first publicly listed (YYYY-MM-DD)
    - delisted_utc: Last trading date (if delisted)
    - address: Headquarters address (address1, city, state, postal_code)
    - phone_number: Company contact phone
    - total_employees: Approximate employee count
    - market_cap: Most recent close price × weighted outstanding shares
    - share_class_shares_outstanding: Outstanding shares for this class
    - weighted_shares_outstanding: Total shares assuming all classes converted
    - round_lot: Standard trading lot size
    - cik: Central Index Key for SEC filings
    - composite_figi: Composite OpenFIGI identifier
    - share_class_figi: Share class OpenFIGI identifier
    - sic_code: Standard Industrial Classification code
    - sic_description: SIC code description
    - branding: Visual assets (logo_url, icon_url) for UI integration

    Note: Ticker details considerations:
    - Date parameter enables historical company data lookups
    - SEC filing data aligned with period of report date, not submission date
    - Branding URLs provide direct access to company logos and icons
    - Market cap calculated as close price × weighted shares outstanding
    - SIC codes classify companies by industry (see SEC's SIC Code List)
    - FIGI codes provide global financial instrument identification
    - CIK enables SEC EDGAR filing lookups
    - Weighted shares outstanding accounts for all share class conversions
    - Description field provides comprehensive business overview
    - Address and phone enable direct company contact

    Use case: Building a stock research dashboard - fetch ticker details to display company
    overview with logo, business description, key metrics (market cap, employees), industry
    classification (SIC), and contact information, all enriched with branding assets for
    professional presentation.
    """
    try:
        results = polygon_client.get_ticker_details(
            ticker=ticker, date=date, params=params, raw=True
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
async def get_related_companies(
    ticker: str,
    params: Optional[Dict[str, Any]] = None,
) -> str:
    """
    Retrieve a list of tickers related to a specified ticker, identified through analysis of news
    coverage and returns data. This endpoint helps users discover peers, competitors, or thematically
    similar companies, aiding in comparative analysis, portfolio diversification, and market research.

    Reference: https://polygon.io/docs/rest/stocks/tickers/related-tickers

    Essential for peer identification, comparative analysis, portfolio diversification, and market
    research. Discovers related companies based on news correlation and return patterns.

    Parameters:
    - ticker: The ticker symbol to search for related companies (e.g., "AAPL", "MSFT", "TSLA")
    - params: Additional query parameters

    Example: get_related_companies("AAPL")
             gets companies related to Apple (may include MSFT, GOOGL, AMZN, META, etc.)
    Example: get_related_companies("TSLA")
             gets companies related to Tesla (may include RIVN, LCID, F, GM, etc.)
    Example: get_related_companies("JPM")
             gets companies related to JPMorgan Chase (other major banks and financial institutions)
    Example: get_related_companies("NVDA")
             gets companies related to NVIDIA (other semiconductor and AI chip companies)

    Response includes:
    - ticker: Related ticker symbol
    - Results array contains list of related company tickers

    Note: Related companies considerations:
    - Relationships identified through news coverage analysis
    - Returns data correlation also contributes to relatedness
    - Typically returns companies in same sector or industry
    - May include direct competitors and peer companies
    - Useful for discovering investment alternatives
    - Helps identify stocks that move together
    - Relationship strength varies (no explicit scoring provided)
    - Results based on recent news and market behavior
    - Number of results varies by ticker
    - Useful for building watch lists of similar companies
    - Aids in sector rotation strategies
    - Helps identify portfolio concentration risks

    Use case: Building a comparative analysis dashboard - after researching Apple, use this endpoint
    to discover related tech companies (Microsoft, Google, Amazon, Meta, etc.), then fetch financial
    metrics and performance data for all related tickers to compare valuations, growth rates, and
    market positioning across the entire peer group.
    """
    try:
        results = polygon_client.get_related_companies(
            ticker=ticker, params=params, raw=True
        )

        return json_to_csv(results.data.decode("utf-8"))
    except Exception as e:
        return f"Error: {e}"





@poly_mcp.tool(annotations=ToolAnnotations(readOnlyHint=True))
async def get_ticker_types(
    asset_class: Optional[str] = None,
    locale: Optional[str] = None,
    params: Optional[Dict[str, Any]] = None,
) -> str:
    """
    Retrieve a list of all ticker types supported by Polygon.io. This endpoint categorizes tickers
    across asset classes, markets, and instruments, helping users understand the different
    classifications and their attributes.

    Reference: https://polygon.io/docs/rest/stocks/tickers/ticker-types

    Essential for data classification, filtering mechanisms, educational reference, and system
    integration. Provides a comprehensive mapping of ticker type codes to descriptions.

    Parameters:
    - asset_class: Filter by asset class (stocks, options, crypto, fx, indices)
    - locale: Filter by locale (us, global)
    - params: Additional query parameters

    Example: get_ticker_types()
             gets all ticker types across all asset classes and locales
    Example: get_ticker_types(asset_class="stocks")
             gets all stock ticker types (CS, ETF, ADRC, etc.)
    Example: get_ticker_types(asset_class="stocks", locale="us")
             gets US stock ticker types only
    Example: get_ticker_types(asset_class="options")
             gets options ticker types
    Example: get_ticker_types(locale="global")
             gets all global ticker types across asset classes

    Response includes:
    - code: Polygon.io code for this ticker type (e.g., CS, ETF, ADRC)
    - description: Short description of the ticker type (e.g., "Common Stock")
    - asset_class: Asset class group (stocks, options, crypto, fx, indices)
    - locale: Geographical location identifier (us, global)

    Common ticker type codes:
    Stocks:
    - CS: Common Stock
    - ETF: Exchange Traded Fund
    - ADRC: American Depository Receipt Common
    - ADRP: American Depository Receipt Preferred
    - ADRR: American Depository Receipt Rights
    - ADRW: American Depository Receipt Warrants
    - CEF: Closed-End Fund
    - ETS: Exchange Traded Security
    - PFD: Preferred Stock
    - REIT: Real Estate Investment Trust
    - SP: Structured Product
    - UNIT: Unit
    - WARRANT: Warrant
    - RIGHT: Rights

    Options:
    - CALL: Call Option
    - PUT: Put Option

    Crypto/FX:
    - Various currency and coin pair types

    Note: Ticker types considerations:
    - Ticker types help categorize instruments for filtering and analysis
    - Each type code is used in the list_tickers endpoint's type parameter
    - Asset class groups similar instruments (all stock types under "stocks")
    - Locale distinguishes US vs. global instruments
    - Type codes are consistent across the Polygon.io platform
    - Use this endpoint to discover available types for filtering
    - Types help distinguish between common stock, ETFs, preferred shares, etc.
    - Essential for building type-specific filters in applications

    Use case: Building a stock screener with type filters - first fetch all stock ticker types
    to populate a dropdown menu, allowing users to filter by CS (common stocks), ETF (funds),
    REIT (real estate), or other specific instrument types for targeted analysis.
    """
    try:
        results = polygon_client.get_ticker_types(
            asset_class=asset_class, locale=locale, params=params, raw=True
        )

        # Convert to CSV
        csv_data = json_to_csv(results.data.decode("utf-8"))

        # Process with intelligent caching
        return await process_tool_response(
            tool_name="get_ticker_types",
            params={},
            csv_data=csv_data,
        )
    except Exception as e:
        return f"Error: {e}"





@poly_mcp.tool(annotations=ToolAnnotations(readOnlyHint=True))
async def list_conditions(
    asset_class: Optional[str] = None,
    data_type: Optional[str] = None,
    id: Optional[int] = None,
    sip: Optional[str] = None,
    params: Optional[Dict[str, Any]] = None,
) -> str:
    """
    List all condition codes used by Polygon.io for trades and quotes.
    Condition codes provide additional context about trades and quotes (e.g., extended hours, odd lot).

    Reference: https://polygon.io/docs/rest/stocks/market-operations/condition-codes

    Parameters:
    - asset_class: Filter by asset class (stocks, options, crypto, fx)
    - data_type: Filter by data type (trade, quote, bbo)
    - id: Filter by specific condition ID
    - sip: Filter by SIP (CTA, UTP, OPRA)

    Example: list_conditions() returns all condition codes
    Example: list_conditions(asset_class="stocks", data_type="trade") returns stock trade conditions
    Example: list_conditions(id=1) returns details for condition code 1

    Note: Condition codes are essential for understanding trade and quote characteristics.
    Common examples include codes for extended hours trading, odd lots, and various execution venues.
    """
    try:
        results = polygon_client.list_conditions(
            asset_class=asset_class,
            data_type=data_type,
            id=id,
            sip=sip,
            params=params,
            raw=True,
        )

        return json_to_csv(results.data.decode("utf-8"))
    except Exception as e:
        return f"Error: {e}"





@poly_mcp.tool(annotations=ToolAnnotations(readOnlyHint=True))
async def get_exchanges(
    asset_class: Optional[str] = None,
    locale: Optional[str] = None,
    params: Optional[Dict[str, Any]] = None,
) -> str:
    """
    List all known stock exchanges and their properties.
    Returns exchange details including name, type, MIC (Market Identifier Code), and operating status.

    Reference: https://polygon.io/docs/rest/stocks/market-operations/exchanges

    Parameters:
    - asset_class: Filter by asset class (stocks, options, crypto, fx)
    - locale: Filter by locale (us, global)

    Example: get_exchanges() returns all known exchanges
    Example: get_exchanges(asset_class="stocks", locale="us") returns US stock exchanges

    Note: MIC (Market Identifier Code) is a unique identifier for each exchange (e.g., XNYS for NYSE).
    """
    try:
        results = polygon_client.get_exchanges(
            asset_class=asset_class, locale=locale, params=params, raw=True
        )

        # Convert to CSV
        csv_data = json_to_csv(results.data.decode("utf-8"))

        # Process with intelligent caching
        return await process_tool_response(
            tool_name="get_exchanges",
            params={},
            csv_data=csv_data,
        )
    except Exception as e:
        return f"Error: {e}"
