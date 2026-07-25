"""Validation helpers for the Spring Gamma RTH market-state overlay."""

from __future__ import annotations

import math
from collections.abc import Mapping
from datetime import datetime, timezone

from spx_spark.analytics.options.pricing import finite_float
from spx_spark.marketdata import as_utc


_SCHEMA_VERSION = "market_state_5m.v1"
_RULE_VERSION = "market_state_5m_eight_variable_rules.v1"
_KNOWN_STATES = frozenset(
    {
        "TREND_UP",
        "TREND_DOWN",
        "LOW_VOL_RANGE",
        "HIGH_VOL_CHOP",
        "LOW_VOL_PIN",
        "UNCERTAIN",
    }
)
_READY_STATES = frozenset(
    {"TREND_UP", "TREND_DOWN", "LOW_VOL_RANGE", "HIGH_VOL_CHOP"}
)


def extract_rth_market_state(market: Mapping[str, object]) -> dict[str, object]:
    """Return a canonical, validated state attached to the market frame."""

    candidate = _child(_child(market, "diagnostics"), "rth_market_state")
    market_at = _datetime(market.get("as_of"))
    state_at = _datetime(candidate.get("as_of"))
    if (
        candidate.get("schema_version") != _SCHEMA_VERSION
        or candidate.get("rule_version") != _RULE_VERSION
        or str(candidate.get("state") or "") not in _KNOWN_STATES
        or candidate.get("action_authority") != "none"
        or candidate.get("actionable") is not False
        or market_at is None
        or state_at is None
        or abs((market_at - state_at).total_seconds()) > 5.0
    ):
        return {}
    direction_score = finite_float(candidate.get("D"))
    if direction_score is not None and not -10.0 <= direction_score <= 10.0:
        return {}
    allowed = {
        key: candidate.get(key)
        for key in (
            "schema_version",
            "rule_version",
            "as_of",
            "as_of_et",
            "state",
            "market_state",
            "D",
            "Q",
            "V",
            "direction_components",
            "pin_proxy_candidate",
            "pin_confirmation",
            "low_vol_pin_emission_allowed",
            "input_availability",
            "status",
            "reasons",
            "action_authority",
            "actionable",
            "input_lineage",
        )
        if key in candidate
    }
    return _canonical(allowed)  # type: ignore[return-value]


def ready_rth_market_state(state: Mapping[str, object]) -> bool:
    """Return whether all eight inputs support using the state as a gate."""

    availability = _child(state, "input_availability")
    return (
        state.get("status") == "ready"
        and state.get("state") in _READY_STATES
        and availability.get("complete") is True
        and availability.get("required_count") == 8
        and availability.get("available_count") == 8
    )


def missing_rth_market_state(session: str) -> dict[str, object]:
    """Build the fail-closed state payload used when no valid overlay exists."""

    return {
        "schema_version": _SCHEMA_VERSION,
        "rule_version": _RULE_VERSION,
        "state": "UNCERTAIN",
        "status": "uncertain",
        "D": None,
        "Q": {
            "quality": None,
            "efficiency_ratio": None,
            "vwap_cross_count": None,
        },
        "V": {"state": None, "same_time_range_ratio": None},
        "input_availability": {
            "required_count": 8,
            "available_count": 0,
            "complete": False,
        },
        "reasons": [
            "rth_market_state_not_attached"
            if session == "rth"
            else "rth_market_state_not_applicable_outside_rth"
        ],
        "action_authority": "none",
        "actionable": False,
    }


def _datetime(value: object) -> datetime | None:
    if isinstance(value, datetime):
        parsed = value
    elif isinstance(value, str) and value:
        try:
            parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError:
            return None
    else:
        return None
    return (
        parsed.replace(tzinfo=timezone.utc)
        if parsed.tzinfo is None
        else parsed.astimezone(timezone.utc)
    )


def _mapping(value: object) -> Mapping[str, object]:
    if isinstance(value, Mapping):
        return value
    to_dict = getattr(value, "to_dict", None)
    payload = to_dict() if callable(to_dict) else None
    return payload if isinstance(payload, Mapping) else {}


def _child(parent: Mapping[str, object], key: str) -> Mapping[str, object]:
    return _mapping(parent.get(key))


def _canonical(value: object) -> object:
    if isinstance(value, Mapping):
        return {str(key): _canonical(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_canonical(item) for item in value]
    if isinstance(value, datetime):
        return as_utc(value).isoformat()
    if isinstance(value, float):
        return value if math.isfinite(value) else None
    if isinstance(value, (str, int, bool)) or value is None:
        return value
    to_dict = getattr(value, "to_dict", None)
    return _canonical(to_dict()) if callable(to_dict) else str(getattr(value, "value", value))


__all__ = [
    "extract_rth_market_state",
    "missing_rth_market_state",
    "ready_rth_market_state",
]
