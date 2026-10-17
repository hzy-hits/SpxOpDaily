"""Process-local OptionsMap/exposure cache shared by concurrent Core tasks.

Market features, shock and alert evaluation load the same latest-state file and
each used to rebuild the option map. The cache key is a content fingerprint of
the loaded state plus the freshness settings; a new state version replaces the
single cached entry.
"""

from __future__ import annotations

import hashlib
import threading
from collections.abc import Mapping, Sequence
from typing import Any

from spx_spark.config import StorageSettings, current_storage_settings
from spx_spark.features.exposure_map import build_exposure_map
from spx_spark.marketdata import Quote
from spx_spark.options_map import build_options_map, group_spxw_option_quotes
from spx_spark.storage import LatestState

Snapshot = tuple[Mapping[str, Sequence[Quote]], Any, Any]

_LOCK = threading.Lock()
_CACHE: dict[str, Snapshot] = {}


def options_snapshot(
    state: LatestState,
    *,
    storage_settings: StorageSettings | None = None,
) -> Snapshot:
    """Return (grouped quotes, OptionsMap, ExposureMap) for this state version."""

    settings = storage_settings or current_storage_settings()
    key = _fingerprint(state, settings)
    with _LOCK:
        cached = _CACHE.get(key)
    if cached is not None:
        return cached
    grouped = group_spxw_option_quotes(state, storage_settings=settings)
    options = build_options_map(state, storage_settings=settings, grouped_quotes=grouped)
    exposure = build_exposure_map(state, grouped_quotes=grouped)
    snapshot: Snapshot = (grouped, options, exposure)
    with _LOCK:
        _CACHE.clear()
        _CACHE[key] = snapshot
    return snapshot


def clear_options_snapshot_cache() -> None:
    with _LOCK:
        _CACHE.clear()


def _fingerprint(state: LatestState, settings: StorageSettings) -> str:
    # Include Greeks, source clocks, spot routing, and raw rejection flags too.
    # Arrival time and BBO alone do not identify the analytical inputs.
    inputs = (
        state.as_of, state.failover_mode, state.quotes, state.best_quotes,
        state.provider_states, settings,
    )
    return hashlib.blake2b(repr(inputs).encode(), digest_size=16).hexdigest()
