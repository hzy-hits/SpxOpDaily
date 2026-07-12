"""Base/hot/slow option row cache merge helpers."""

from __future__ import annotations

from spx_spark.ibkr.stream.models import OPTION_CACHE_TTL_SECONDS
from spx_spark.ibkr.verifier import VerifyRow


def mark_rows_stale(rows: list[VerifyRow]) -> list[VerifyRow]:
    for row in rows:
        row.stale = True
    return rows


def merge_slow_rows(
    rows: list[VerifyRow],
    slow_cache: dict[str, VerifyRow],
    subscribed_labels: set[str],
) -> list[VerifyRow]:
    rows.extend(row for label, row in slow_cache.items() if label not in subscribed_labels)
    return rows


def update_option_cache(
    cache: dict[str, tuple[float, VerifyRow]],
    rows: list[VerifyRow],
    *,
    now_monotonic: float,
    expiry: str | None,
    active_expiries: frozenset[str] | None = None,
    ttl_seconds: float = OPTION_CACHE_TTL_SECONDS,
) -> None:
    """Remember the latest row per rotated option; evict expired/rolled rows."""
    for row in rows:
        if row.kind != "option" or not row.subscribed:
            continue
        cache[row.label] = (now_monotonic, row)
    allowed_expiries = active_expiries or (frozenset({expiry}) if expiry else frozenset())
    expired = [
        label
        for label, (cached_at, row) in cache.items()
        if now_monotonic - cached_at > ttl_seconds
        or (
            allowed_expiries
            and not any(f":{active_expiry}:" in label for active_expiry in allowed_expiries)
        )
    ]
    for label in expired:
        del cache[label]


def merge_cached_option_rows(
    rows: list[VerifyRow],
    cache: dict[str, tuple[float, VerifyRow]],
    subscribed_labels: set[str],
) -> list[VerifyRow]:
    rows.extend(row for label, (_, row) in cache.items() if label not in subscribed_labels)
    return rows

