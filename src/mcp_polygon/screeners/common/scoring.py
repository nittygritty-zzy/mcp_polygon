"""
Scoring and ranking utilities for stock screeners.

Provides functions to normalize metrics, calculate composite scores,
and rank candidates.
"""

from typing import Any, Dict, List

import pandas as pd


def normalize_metric(
    value: float, min_val: float, max_val: float, cap: bool = True
) -> float:
    """
    Normalize a metric to 0-100 scale.

    **Parameters:**
    - value: The value to normalize
    - min_val: Minimum value (maps to 0)
    - max_val: Maximum value (maps to 100)
    - cap: Whether to cap at 0-100 range

    **Returns:**
    Normalized value (0-100)
    """
    if max_val == min_val:
        return 50.0  # Neutral score if no range

    normalized = ((value - min_val) / (max_val - min_val)) * 100

    if cap:
        return max(0.0, min(100.0, normalized))

    return normalized


def calculate_composite_score(
    metrics: Dict[str, float],
    weights: Dict[str, float],
) -> float:
    """
    Calculate weighted composite score from multiple metrics.

    **Parameters:**
    - metrics: Dict of metric_name -> normalized_value (0-100)
    - weights: Dict of metric_name -> weight (should sum to 1.0)

    **Returns:**
    Composite score (0-100)

    **Example:**
    ```python
    score = calculate_composite_score(
        metrics={"dtc": 80, "mcap": 60, "roe": 40},
        weights={"dtc": 0.5, "mcap": 0.3, "roe": 0.2}
    )
    # Returns: 80*0.5 + 60*0.3 + 40*0.2 = 66.0
    ```
    """
    if not metrics or not weights:
        return 0.0

    score = sum(metrics.get(metric, 0.0) * weight for metric, weight in weights.items())

    return round(score, 2)


def rank_candidates(
    candidates: List[Dict[str, Any]],
    score_column: str = "score",
    max_results: int = 50,
) -> List[Dict[str, Any]]:
    """
    Rank candidates by score and return top N.

    **Parameters:**
    - candidates: List of candidate dicts
    - score_column: Column name to sort by
    - max_results: Maximum number of results to return

    **Returns:**
    Top N candidates sorted by score (highest first)
    """
    if not candidates:
        return []

    df = pd.DataFrame(candidates)

    if score_column not in df.columns:
        raise ValueError(f"Score column '{score_column}' not found in candidates")

    # Sort by score descending
    df = df.sort_values(score_column, ascending=False)

    # Return top N
    df = df.head(max_results)

    return df.to_dict("records")
