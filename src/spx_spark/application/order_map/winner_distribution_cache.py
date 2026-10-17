"""Bounded reuse of completed debit path distributions."""

from __future__ import annotations

from datetime import datetime
from pathlib import Path
from threading import Lock
from typing import Any, Mapping

REFRESH_SECONDS = 30.0

_CACHE: dict[str, tuple[float, dict[str, Any]]] = {}
_LOCK = Lock()


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
    key: str, now: datetime
) -> dict[str, Any] | None:
    with _LOCK:
        entry = _CACHE.get(key)
    if entry is None:
        return None
    stored_at, distribution = entry
    if (now.timestamp() - stored_at) > REFRESH_SECONDS:
        return None
    return distribution


def store_winner_distribution(
    key: str, now: datetime, distribution: dict[str, Any]
) -> None:
    with _LOCK:
        if len(_CACHE) > 8:
            _CACHE.clear()
        _CACHE[key] = (now.timestamp(), distribution)


def clear_path_distribution_cache() -> None:
    with _LOCK:
        _CACHE.clear()


def _mapping(value: object) -> Mapping[str, Any]:
    return value if isinstance(value, Mapping) else {}
