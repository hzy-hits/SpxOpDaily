"""Small shared helpers for immutable operator lifecycle identity."""

from __future__ import annotations

from typing import Mapping


def operator_generation(
    value: Mapping[str, object],
    *,
    field: str = "reentry_generation",
) -> int:
    generation = value.get(field, 0)
    if isinstance(generation, int) and not isinstance(generation, bool):
        return max(generation, 0)
    return 0


def operator_opportunity_id(
    value: Mapping[str, object],
    *fields: str,
    fallback: object = None,
) -> str | None:
    for field in fields:
        candidate = value.get(field)
        if candidate:
            return str(candidate)
    return str(fallback) if fallback else None
