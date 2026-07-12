"""Compatibility facade for options analytics + LatestState orchestration.

Pure calculation lives in ``spx_spark.analytics.options``. Orchestration and CLI
live in this package; this module only re-exports the public API.
"""

from __future__ import annotations

from spx_spark.analytics.options import *  # noqa: F403
from spx_spark.analytics.options import __all__ as _ANALYTICS_ALL
from spx_spark.options_map.cli import main, parse_args, run
from spx_spark.options_map.orchestration import (
    actionable_chain_implied_spot,
    build_options_map,
    group_spxw_option_quotes,
    ibkr_provider_unavailable,
    select_underlier,
)
from spx_spark.options_map.render import format_number, print_options_map

__all__ = [
    *_ANALYTICS_ALL,
    "actionable_chain_implied_spot",
    "build_options_map",
    "format_number",
    "group_spxw_option_quotes",
    "ibkr_provider_unavailable",
    "main",
    "parse_args",
    "print_options_map",
    "run",
    "select_underlier",
]

if __name__ == "__main__":
    main()
