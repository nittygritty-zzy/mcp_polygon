#!/usr/bin/env python3
"""Test the json_to_csv conversion with the actual API response."""

import json
from src.mcp_polygon.formatters import json_to_csv

# This is the actual response from the API
response_json = '''{"results":{"day":{"change":0,"change_percent":0,"close":5.64,"high":6.18,"last_updated":1761336000000000000,"low":4.4,"open":5,"previous_close":5.64,"volume":1951,"vwap":5.9042},"details":{"contract_type":"call","exercise_style":"american","expiration_date":"2025-12-19","shares_per_contract":100,"strike_price":275,"ticker":"O:AAPL251219C00275000"},"greeks":{"delta":0.4461209929334776,"gamma":0.016082781536618015,"theta":-0.10498879244813095,"vega":0.4138059640477654},"implied_volatility":0.2418337896503819,"open_interest":23049,"underlying_asset":{"ticker":"AAPL"}},"status":"OK","request_id":"6bc32d8b1cda3762cf8c75916733a60e"}'''

data = json.loads(response_json)

print("Testing conversion as the server code does it:")
print("=" * 80)

# This is what get_snapshot_option does
if "results" in data:
    formatted_data = {"results": [data["results"]]}
    print(f"Formatted data structure: {list(formatted_data.keys())}")
    print(f"Type of formatted_data['results']: {type(formatted_data['results'])}")
    print(f"Length of list: {len(formatted_data['results'])}")
    print(f"Type of first item: {type(formatted_data['results'][0])}")

    try:
        csv_output = json_to_csv(formatted_data)
        print("\nSuccess! CSV output:")
        print(csv_output)
    except Exception as e:
        import traceback
        print(f"\nError: {e}")
        print("\nTraceback:")
        print(traceback.format_exc())
