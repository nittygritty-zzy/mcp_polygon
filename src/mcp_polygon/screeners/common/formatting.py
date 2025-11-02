"""
Result formatting utilities for stock screeners.

Provides functions to format screener results as CSV output.
"""

from typing import Any, Dict, List

import pandas as pd

from ...formatters import json_to_csv


def format_screener_results(
    candidates: List[Dict[str, Any]],
    output_columns: List[str],
    empty_message: str = "No candidates found matching criteria",
) -> str:
    """
    Format screener results as CSV.

    **Parameters:**
    - candidates: List of candidate dicts
    - output_columns: Columns to include in output (in order)
    - empty_message: Message to return if no candidates

    **Returns:**
    CSV-formatted string

    **Example:**
    ```python
    csv = format_screener_results(
        candidates=[{"ticker": "AAPL", "score": 95, "price": 150}],
        output_columns=["ticker", "score", "price"]
    )
    # Returns: "ticker,score,price\\nAAPL,95,150\\n"
    ```
    """
    if not candidates:
        return f"ticker,message\nNONE,{empty_message}"

    df = pd.DataFrame(candidates)

    # Select only columns that exist in the dataframe
    available_columns = [col for col in output_columns if col in df.columns]

    if not available_columns:
        return "ticker,message\nERROR,No valid output columns found"

    df = df[available_columns]

    return json_to_csv(df.to_dict("records"))
