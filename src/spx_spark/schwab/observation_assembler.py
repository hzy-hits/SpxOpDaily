"""Merge fast pricing and slower option-structure observations by field group."""

from __future__ import annotations

from spx_spark.marketdata import Quote
from spx_spark.storage import merge_option_observations as _merge_option_observations


def merge_option_observations(left: Quote, right: Quote) -> Quote:
    """Keep the freshest pricing group and freshest non-empty structure group."""

    return _merge_option_observations(left, right)
