import json
import csv
import io
from typing import Any, Dict, List, Optional


def deep_vars(obj: Any) -> Any:
    """
    Recursively convert @modelclass objects to dictionaries.

    Handles:
    - Objects with __dict__ attribute (converts via vars())
    - Nested objects within dicts
    - Lists containing objects
    - Primitive types (returned as-is)

    Args:
        obj: Object to convert (can be @modelclass object, dict, list, or primitive)

    Returns:
        Fully converted structure with all nested objects as dicts
    """
    # Handle None and primitives
    if obj is None or isinstance(obj, (str, int, float, bool)):
        return obj

    # Handle lists
    if isinstance(obj, list):
        return [deep_vars(item) for item in obj]

    # Handle dicts
    if isinstance(obj, dict):
        return {key: deep_vars(value) for key, value in obj.items()}

    # Handle objects with __dict__ (like @modelclass objects)
    if hasattr(obj, "__dict__"):
        obj_dict = vars(obj)
        return {key: deep_vars(value) for key, value in obj_dict.items()}

    # Fallback for other types - convert to string
    return str(obj)


def json_to_csv(json_input: str | dict) -> str:
    """
    Convert JSON to flattened CSV format.

    Args:
        json_input: JSON string or dict. If the JSON has a 'results' key containing
                   a list, it will be extracted. Otherwise, the entire structure
                   will be wrapped in a list for processing.

    Returns:
        CSV string with headers and flattened rows
    """
    # Parse JSON if it's a string
    if isinstance(json_input, str):
        data = json.loads(json_input)
    else:
        data = json_input

    if isinstance(data, dict) and "results" in data:
        records = data["results"]
    elif isinstance(data, list):
        records = data
    else:
        records = [data]

    flattened_records = [_flatten_dict(record) for record in records]

    if not flattened_records:
        return ""

    # Get all unique keys across all records (for consistent column ordering)
    all_keys = []
    seen = set()
    for record in flattened_records:
        for key in record.keys():
            if key not in seen:
                all_keys.append(key)
                seen.add(key)

    output = io.StringIO()
    writer = csv.DictWriter(output, fieldnames=all_keys, lineterminator="\n")
    writer.writeheader()
    writer.writerows(flattened_records)

    return output.getvalue()


def _flatten_dict(
    d: dict[str, Any], parent_key: str = "", sep: str = "_"
) -> dict[str, Any]:
    """
    Flatten a nested dictionary by joining keys with separator.

    Args:
        d: Dictionary to flatten
        parent_key: Key from parent level (for recursion)
        sep: Separator to use between nested keys

    Returns:
        Flattened dictionary with no nested structures
    """
    # Handle non-dict input gracefully
    if not isinstance(d, dict):
        return {"value": str(d)}

    items = []
    for k, v in d.items():
        new_key = f"{parent_key}{sep}{k}" if parent_key else k

        if isinstance(v, dict):
            # Recursively flatten nested dicts
            items.extend(_flatten_dict(v, new_key, sep=sep).items())
        elif isinstance(v, list):
            # Convert lists to comma-separated strings
            items.append((new_key, str(v)))
        else:
            items.append((new_key, v))

    return dict(items)


def calculate_gex(
    options_data: List[Dict[str, Any]], stock_price: float
) -> Dict[str, Any]:
    """
    Calculate Gamma Exposure (GEX) from options chain data.

    GEX Formula per strike:
    - Call GEX = Open Interest × Gamma × 100 (shares per contract) × Stock Price²
    - Put GEX = Open Interest × Gamma × 100 × Stock Price² × -1 (negative)

    Args:
        options_data: List of option contracts with Greeks and open interest
        stock_price: Current underlying stock price

    Returns:
        Dictionary with:
        - total_gex: Net gamma exposure (calls positive, puts negative)
        - call_gex: Total call gamma exposure
        - put_gex: Total put gamma exposure (negative value)
        - gex_by_strike: Breakdown by strike price
        - max_gex_strike: Strike with highest absolute GEX (pin level)
    """
    total_call_gex = 0
    total_put_gex = 0
    gex_by_strike: Dict[float, Dict[str, float]] = {}

    for option in options_data:
        try:
            # Extract required fields
            strike = option.get("details", {}).get("strike_price")
            contract_type = option.get("details", {}).get("contract_type")
            open_interest = option.get("open_interest", 0)
            gamma = option.get("greeks", {}).get("gamma")

            # Skip if missing critical data
            if strike is None or gamma is None or open_interest == 0:
                continue

            # Calculate GEX for this contract
            # GEX = OI × Gamma × 100 × Price²
            gex_value = open_interest * gamma * 100 * (stock_price**2)

            # Puts are negative GEX
            if contract_type == "put":
                gex_value = -gex_value
                total_put_gex += gex_value
            else:
                total_call_gex += gex_value

            # Store by strike
            if strike not in gex_by_strike:
                gex_by_strike[strike] = {"call_gex": 0, "put_gex": 0, "net_gex": 0}

            if contract_type == "put":
                gex_by_strike[strike]["put_gex"] += gex_value
            else:
                gex_by_strike[strike]["call_gex"] += gex_value

            gex_by_strike[strike]["net_gex"] = (
                gex_by_strike[strike]["call_gex"] + gex_by_strike[strike]["put_gex"]
            )

        except (KeyError, TypeError, ValueError):
            # Skip malformed records
            continue

    # Find strike with maximum absolute GEX (key resistance/support level)
    max_gex_strike = None
    max_gex_value = 0

    for strike, gex_data in gex_by_strike.items():
        abs_gex = abs(gex_data["net_gex"])
        if abs_gex > max_gex_value:
            max_gex_value = abs_gex
            max_gex_strike = strike

    total_gex = total_call_gex + total_put_gex

    return {
        "total_gex": round(total_gex, 2),
        "call_gex": round(total_call_gex, 2),
        "put_gex": round(total_put_gex, 2),
        "max_gex_strike": max_gex_strike,
        "max_gex_value": round(max_gex_value, 2) if max_gex_strike else 0,
        "gex_by_strike": {
            strike: {
                "call_gex": round(data["call_gex"], 2),
                "put_gex": round(data["put_gex"], 2),
                "net_gex": round(data["net_gex"], 2),
            }
            for strike, data in sorted(gex_by_strike.items())
        },
        "interpretation": _interpret_gex(total_gex, stock_price, max_gex_strike),
    }


def _interpret_gex(
    total_gex: float, stock_price: float, max_gex_strike: Optional[float]
) -> str:
    """
    Provide human-readable interpretation of GEX values.

    Args:
        total_gex: Net gamma exposure
        stock_price: Current stock price
        max_gex_strike: Strike with highest GEX concentration

    Returns:
        Interpretation string
    """
    if total_gex > 0:
        gex_type = "POSITIVE (Market makers SHORT gamma)"
        behavior = "MMs will SELL into rallies and BUY dips (stabilizing)"
    elif total_gex < 0:
        gex_type = "NEGATIVE (Market makers LONG gamma)"
        behavior = "MMs will BUY rallies and SELL dips (amplifying moves, SQUEEZE RISK)"
    else:
        gex_type = "NEUTRAL"
        behavior = "No significant gamma hedging pressure"

    pin_info = ""
    if max_gex_strike:
        distance = ((max_gex_strike - stock_price) / stock_price) * 100
        if abs(distance) < 2:
            pin_info = f" Stock pinned near ${max_gex_strike:.2f} (current ${stock_price:.2f})."
        elif distance > 0:
            pin_info = f" Resistance at ${max_gex_strike:.2f} ({distance:+.1f}% above)."
        else:
            pin_info = f" Support at ${max_gex_strike:.2f} ({distance:+.1f}% below)."

    return f"{gex_type}. {behavior}.{pin_info}"


def enrich_options_with_gex_and_advanced_greeks(
    options_data: List[Dict[str, Any]], stock_price: float
) -> List[Dict[str, Any]]:
    """
    Enrich each option contract with GEX and advanced Greeks calculations.

    Adds the following fields to each option:
    - gex: Gamma exposure for this contract
    - gex_dollars: Dollar-normalized GEX
    - charm: Rate of delta decay (dDelta/dTime)
    - vanna: Sensitivity to volatility changes (dDelta/dVol)
    - vomma: Convexity of vega (dVega/dVol)
    - zomma: Convexity of gamma (dGamma/dVol)
    - speed: Rate of gamma change (dGamma/dPrice)
    - color: Rate of gamma decay (dGamma/dTime)

    Args:
        options_data: List of option contracts
        stock_price: Current underlying stock price

    Returns:
        Enriched list with additional calculated fields
    """

    enriched_data = []

    for option in options_data:
        # Create a copy to avoid modifying original
        enriched_option = option.copy()

        try:
            # Extract required fields
            strike = option.get("details", {}).get("strike_price")
            contract_type = option.get("details", {}).get("contract_type")
            open_interest = option.get("open_interest", 0)

            # Extract standard Greeks
            delta = option.get("greeks", {}).get("delta")
            gamma = option.get("greeks", {}).get("gamma")
            theta = option.get("greeks", {}).get("theta")
            vega = option.get("greeks", {}).get("vega")
            implied_vol = option.get("implied_volatility")

            # Calculate GEX if we have required data
            if gamma is not None and open_interest > 0:
                # GEX = OI × Gamma × 100 × Price²
                gex_value = open_interest * gamma * 100 * (stock_price**2)

                # Puts are negative GEX
                if contract_type == "put":
                    gex_value = -gex_value

                enriched_option["gex"] = round(gex_value, 2)
                enriched_option["gex_dollars"] = round(
                    gex_value * stock_price / 1_000_000, 2
                )  # In millions
            else:
                enriched_option["gex"] = None
                enriched_option["gex_dollars"] = None

            # Calculate advanced Greeks (approximations based on Black-Scholes)
            # These require additional data, so we'll provide reasonable estimates
            if all(
                x is not None for x in [delta, gamma, vega, theta, implied_vol, strike]
            ):
                # Time to expiration (estimate from expiration date)
                expiration_date = option.get("details", {}).get("expiration_date")
                if expiration_date:
                    from datetime import datetime

                    if isinstance(expiration_date, str):
                        exp_date = datetime.strptime(expiration_date, "%Y-%m-%d").date()
                    else:
                        exp_date = expiration_date
                    today = datetime.now().date()
                    days_to_expiry = (exp_date - today).days
                    time_to_expiry = max(
                        days_to_expiry / 365.0, 0.001
                    )  # In years, avoid zero
                else:
                    time_to_expiry = 0.083  # Default to ~1 month

                # Charm (dDelta/dTime) - Delta decay
                # Charm ≈ -Gamma × (r × Strike - q × Stock) / (2 × Time × Stock)
                # Simplified: Charm ≈ -Gamma × Strike / (2 × Time × Stock)
                charm = (
                    -gamma * strike / (2 * time_to_expiry * stock_price)
                    if time_to_expiry > 0
                    else 0
                )
                enriched_option["greeks_charm"] = round(charm, 6)

                # Vanna (dDelta/dVol or dVega/dPrice) - Delta sensitivity to volatility
                # Vanna ≈ Vega / Stock × (1 - d1 / (σ√T))
                # Simplified: Vanna ≈ Vega / Stock
                vanna = vega / stock_price if stock_price > 0 else 0
                enriched_option["greeks_vanna"] = round(vanna, 6)

                # Vomma (Volga) (dVega/dVol) - Vega convexity
                # Vomma ≈ Vega × d1 × d2 / σ
                # Simplified using relationship: Vomma ≈ Vega × Gamma × Stock² / Vega
                # More accurate: Vomma ≈ Vega / implied_vol if available
                if implied_vol and implied_vol > 0:
                    vomma = vega / implied_vol
                else:
                    vomma = 0
                enriched_option["greeks_vomma"] = round(vomma, 4)

                # Zomma (dGamma/dVol) - Gamma convexity
                # Zomma ≈ Gamma × (d1 × d2 - 1) / σ
                # Simplified: Zomma ≈ Gamma / implied_vol
                if implied_vol and implied_vol > 0:
                    zomma = gamma / implied_vol
                else:
                    zomma = 0
                enriched_option["greeks_zomma"] = round(zomma, 6)

                # Speed (dGamma/dPrice) - Rate of gamma change
                # Speed ≈ -Gamma / Stock × (d1 / (σ√T) + 1)
                # Simplified: Speed ≈ -Gamma / Stock
                speed = -gamma / stock_price if stock_price > 0 else 0
                enriched_option["greeks_speed"] = round(speed, 8)

                # Color (dGamma/dTime) - Gamma decay
                # Color ≈ -Gamma / (2 × Time) × (1 + (2r - q) × Time / (2 × σ²T))
                # Simplified: Color ≈ -Gamma / (2 × Time)
                color = -gamma / (2 * time_to_expiry) if time_to_expiry > 0 else 0
                enriched_option["greeks_color"] = round(color, 6)

            else:
                # Set advanced Greeks to None if insufficient data
                enriched_option["greeks_charm"] = None
                enriched_option["greeks_vanna"] = None
                enriched_option["greeks_vomma"] = None
                enriched_option["greeks_zomma"] = None
                enriched_option["greeks_speed"] = None
                enriched_option["greeks_color"] = None

        except (KeyError, TypeError, ValueError, ZeroDivisionError):
            # On error, set all new fields to None
            enriched_option["gex"] = None
            enriched_option["gex_dollars"] = None
            enriched_option["greeks_charm"] = None
            enriched_option["greeks_vanna"] = None
            enriched_option["greeks_vomma"] = None
            enriched_option["greeks_zomma"] = None
            enriched_option["greeks_speed"] = None
            enriched_option["greeks_color"] = None

        enriched_data.append(enriched_option)

    return enriched_data
