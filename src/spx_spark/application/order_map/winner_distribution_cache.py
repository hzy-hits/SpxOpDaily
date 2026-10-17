"""Bounded reuse of completed debit path distributions."""

from __future__ import annotations

import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from pathlib import Path
from threading import Lock
from typing import Any, Callable, Mapping

REFRESH_SECONDS = 30.0
REFRESH_AFTER_SECONDS = 10.0

_CACHE: dict[str, tuple[float, dict[str, Any]]] = {}
_LOCK = Lock()
_EXECUTOR = ThreadPoolExecutor(max_workers=1, thread_name_prefix="winner-distribution")
_INFLIGHT: set[str] = set()
_GENERATION = 0


def winner_distribution_key(
    candidate: Mapping[str, Any],
    facts: Mapping[str, Any],
    *,
    data_root: str | Path | None,
) -> str:
    legs = tuple(
        str(_mapping(leg).get("contract_id") or _mapping(leg).get("strike"))
        for leg in candidate.get("legs") or ()
    )
    return "|".join(
        (
            str(data_root),
            str(facts.get("session_date") or ""),
            str(candidate.get("strategy_type") or ""),
            str(candidate.get("candidate_id") or ""),
            ",".join(legs),
        )
    )


def cached_winner_distribution(
    key: str,
    now: datetime,
    *,
    refresh: Callable[[], dict[str, Any]] | None = None,
) -> dict[str, Any] | None:
    with _LOCK:
        entry = _CACHE.get(key)
    if entry is None:
        return None
    stored_at, distribution = entry
    age = now.timestamp() - stored_at
    if age > REFRESH_SECONDS:
        return None
    if refresh is not None and age > REFRESH_AFTER_SECONDS:
        _schedule_refresh(key, refresh)
    return distribution


def _schedule_refresh(
    key: str, refresh: Callable[[], dict[str, Any]]
) -> None:
    with _LOCK:
        if key in _INFLIGHT:
            return
        _INFLIGHT.add(key)
        generation = _GENERATION

    def run() -> None:
        try:
            distribution = refresh()
        except Exception:
            return
        finally:
            with _LOCK:
                _INFLIGHT.discard(key)
        with _LOCK:
            if generation == _GENERATION and len(_CACHE) <= 8:
                _CACHE[key] = (time.time(), distribution)

    _EXECUTOR.submit(run)


def store_winner_distribution(
    key: str, now: datetime, distribution: dict[str, Any]
) -> None:
    with _LOCK:
        if len(_CACHE) > 8:
            _CACHE.clear()
        _CACHE[key] = (now.timestamp(), distribution)


def clear_path_distribution_cache() -> None:
    global _GENERATION
    with _LOCK:
        _CACHE.clear()
        _INFLIGHT.clear()
        _GENERATION += 1


def _mapping(value: object) -> Mapping[str, Any]:
    return value if isinstance(value, Mapping) else {}
