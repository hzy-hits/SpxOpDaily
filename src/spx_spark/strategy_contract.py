"""Shared, fail-closed envelope for strategy decisions and lifecycle events.

The schema version belongs to persisted strategy *events*.  State files keep
their own independent schema versions so a state migration can never be
mistaken for a decision-policy change.
"""

from __future__ import annotations

import hashlib
import json
import math
from dataclasses import asdict, is_dataclass
from datetime import date, datetime, timezone
from enum import Enum
from pathlib import Path
from typing import Mapping, Sequence


STRATEGY_EVENT_SCHEMA_VERSION = 3

_COORDINATE_INSTRUMENTS = {
    "official_spx": "index:SPX",
    "chain_implied_spx": "synthetic:SPXW_PARITY",
    "es_equivalent": "future:ES",
    "raw_es": "future:ES",
}


def policy_version(namespace: str, policy: object) -> str:
    """Return a stable, non-secret version for one effective policy payload."""

    name = str(namespace).strip()
    if not name:
        raise ValueError("policy namespace must be non-empty")
    encoded = json.dumps(
        _jsonable(policy),
        ensure_ascii=True,
        sort_keys=True,
        separators=(",", ":"),
    ).encode()
    digest = hashlib.sha256(encoded).hexdigest()[:16]
    return f"{name}+sha256:{digest}"


def strategy_event_fields(
    *,
    policy_version_value: str,
    valid_until: datetime | str | None,
    coordinate: Mapping[str, object] | None,
    block_reasons: Sequence[object] = (),
) -> dict[str, object]:
    """Build the five mandatory fields shared by all strategy event types."""

    version = str(policy_version_value).strip()
    if not version:
        raise ValueError("policy_version must be non-empty")
    return {
        "schema_version": STRATEGY_EVENT_SCHEMA_VERSION,
        "policy_version": version,
        "valid_until": _canonical_time(valid_until),
        "coordinate": normalize_coordinate(coordinate),
        "block_reasons": normalize_block_reasons(block_reasons),
    }


def normalize_coordinate(value: Mapping[str, object] | None) -> dict[str, object]:
    """Copy one trigger coordinate without converting it to another price basis."""

    raw = dict(value) if isinstance(value, Mapping) else {}
    kind = str(raw.get("kind") or "unavailable").strip() or "unavailable"
    instrument = str(raw.get("instrument_id") or "").strip() or None
    result = {
        **raw,
        "kind": kind,
        "instrument_id": instrument,
        "observed_value": _finite_number(raw.get("observed_value")),
        "target_value": _finite_number(raw.get("target_value")),
        "spx_observed_value": _finite_number(raw.get("spx_observed_value")),
        "basis_points": _finite_number(raw.get("basis_points")),
        "as_of": _canonical_time(raw.get("as_of") or raw.get("source_at")),
    }
    return result


def normalize_block_reasons(values: Sequence[object] | object) -> list[str]:
    """Return unique non-empty string reason codes in deterministic input order."""

    if isinstance(values, str) or not isinstance(values, Sequence):
        values = (values,)
    reasons: list[str] = []
    seen: set[str] = set()
    for value in values:
        if not isinstance(value, str):
            continue
        reason = value.strip()
        if reason and reason not in seen:
            reasons.append(reason)
            seen.add(reason)
    return reasons


def strategy_contract_issues(
    payload: Mapping[str, object],
    *,
    require_valid_until: bool = False,
    require_actionable_coordinate: bool = False,
) -> tuple[str, ...]:
    """Validate the shared envelope without interpreting event-specific fields."""

    issues: list[str] = []
    if payload.get("schema_version") != STRATEGY_EVENT_SCHEMA_VERSION:
        issues.append("strategy_schema_unsupported")
    if not str(payload.get("policy_version") or "").strip():
        issues.append("policy_version_unavailable")

    raw_valid_until = payload.get("valid_until")
    parsed_valid_until = parse_aware_time(raw_valid_until)
    if raw_valid_until is not None and parsed_valid_until is None:
        issues.append("valid_until_invalid")
    if require_valid_until and parsed_valid_until is None:
        issues.append("valid_until_unavailable")

    raw_coordinate = payload.get("coordinate")
    if not isinstance(raw_coordinate, Mapping):
        issues.append("coordinate_unavailable")
    else:
        coordinate = normalize_coordinate(raw_coordinate)
        kind = str(coordinate.get("kind") or "unavailable")
        instrument = coordinate.get("instrument_id")
        expected = _COORDINATE_INSTRUMENTS.get(kind)
        if require_actionable_coordinate and kind == "unavailable":
            issues.append("coordinate_unavailable")
        elif expected is not None and instrument != expected:
            issues.append("coordinate_instrument_mismatch")
        elif require_actionable_coordinate and expected is None:
            issues.append("coordinate_kind_unsupported")

    raw_reasons = payload.get("block_reasons")
    if not isinstance(raw_reasons, list) or any(
        not isinstance(item, str) or not item.strip() for item in raw_reasons
    ):
        issues.append("block_reasons_invalid")
    elif raw_reasons != normalize_block_reasons(raw_reasons):
        issues.append("block_reasons_not_normalized")
    return tuple(dict.fromkeys(issues))


def actionable_strategy_contract_issues(
    payload: Mapping[str, object], *, now: datetime
) -> tuple[str, ...]:
    """Return all reasons an event envelope is not actionable at ``now``.

    Validity is a half-open interval: an event is actionable only while
    ``now < valid_until``.  Legacy expiry aliases are intentionally not read.
    """

    issues = list(
        strategy_contract_issues(
            payload,
            require_valid_until=True,
            require_actionable_coordinate=True,
        )
    )
    valid_until = parse_aware_time(payload.get("valid_until"))
    if valid_until is not None and _utc(now) >= valid_until:
        issues.append("strategy_event_expired")
    reasons = payload.get("block_reasons")
    if isinstance(reasons, list) and reasons:
        issues.append("strategy_event_blocked")
    return tuple(dict.fromkeys(issues))


def parse_aware_time(value: object) -> datetime | None:
    if isinstance(value, datetime):
        parsed = value
    elif isinstance(value, str):
        try:
            parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError:
            return None
    else:
        return None
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        return None
    return parsed.astimezone(timezone.utc)


def _canonical_time(value: object) -> str | None:
    if value is None:
        return None
    parsed = parse_aware_time(value)
    if parsed is None:
        raise ValueError("strategy timestamps must be timezone-aware ISO-8601 values")
    return parsed.isoformat()


def _finite_number(value: object) -> float | None:
    if isinstance(value, bool) or not isinstance(value, int | float):
        return None
    parsed = float(value)
    return parsed if math.isfinite(parsed) else None


def _jsonable(value: object) -> object:
    if is_dataclass(value) and not isinstance(value, type):
        return _jsonable(asdict(value))
    if isinstance(value, Mapping):
        return {str(key): _jsonable(item) for key, item in value.items()}
    if isinstance(value, (set, frozenset, tuple, list)):
        items = [_jsonable(item) for item in value]
        return sorted(items, key=lambda item: json.dumps(item, sort_keys=True)) if isinstance(
            value, (set, frozenset)
        ) else items
    if isinstance(value, Enum):
        return _jsonable(value.value)
    if isinstance(value, datetime):
        return _canonical_time(value)
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, Path):
        return str(value)
    if value is None or isinstance(value, (str, int, float, bool)):
        return value
    return repr(value)


def _utc(value: datetime) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError("strategy timestamps must be timezone-aware")
    return value.astimezone(timezone.utc)
