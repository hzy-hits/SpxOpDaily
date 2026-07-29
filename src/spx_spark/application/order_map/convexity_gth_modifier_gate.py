"""Freshness and identity gates for optional GTH structure modifiers."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Mapping


GTH_OPTION_MODIFIER_MAX_AGE_SECONDS = 120.0
_ACTIVE_EVENT_PHASES = frozenset(
    {"accepted", "rejected", "retest", "testing", "confirmed"}
)


def gth_skew_rank_gate_reasons(
    shadow: Mapping[str, Any],
    *,
    candidate: Mapping[str, Any],
    mandate: Mapping[str, Any],
    now: datetime,
) -> list[str]:
    """Reject stale or wrong-expiry skew before it modifies a GTH rank."""

    if str(mandate.get("phase") or "") != "gth_preparation":
        return []
    expected_expiry = str(mandate.get("trading_date") or "").replace("-", "")
    source_expiry = str(shadow.get("expiry") or "")
    observed_at = _datetime(shadow.get("as_of"))
    evaluated_at = _utc(now)
    reasons: list[str] = []
    if not expected_expiry or source_expiry != expected_expiry:
        reasons.append("gth_skew_expiry_mismatch")
    if observed_at is None:
        reasons.append("gth_skew_as_of_missing")
    else:
        age_seconds = (evaluated_at - observed_at).total_seconds()
        if age_seconds < -2.0 or age_seconds > GTH_OPTION_MODIFIER_MAX_AGE_SECONDS:
            reasons.append("gth_skew_stale_or_future")
    if candidate and not _vertical_contracts_match_expiry(
        candidate,
        expected_expiry=expected_expiry,
    ):
        reasons.append("gth_skew_contract_expiry_mismatch")
    return list(dict.fromkeys(reasons))


def build_ranked_active_event(
    payload: Mapping[str, Any],
    *,
    mandate: Mapping[str, Any],
    now: datetime,
) -> dict[str, Any]:
    """Copy the active lifecycle and mark whether it may modify a GTH rank."""

    decision = _mapping(payload.get("level_decision"))
    event = {
        key: decision.get(key)
        for key in (
            "event_id",
            "phase",
            "thesis",
            "direction",
            "level_kind",
            "level",
            "formal_signal",
            "quality_ok",
            "quality_reason",
            "expires_at",
            "expiry",
            "session_date",
            "session_mode",
            "updated_at",
        )
    }
    reasons = _gth_active_event_rank_gate_reasons(
        event,
        mandate=mandate,
        now=now,
    )
    event["rank_eligible"] = not reasons
    event["rank_gate_reasons"] = reasons
    return event


def _gth_active_event_rank_gate_reasons(
    event: Mapping[str, Any],
    *,
    mandate: Mapping[str, Any],
    now: datetime,
) -> list[str]:
    if str(mandate.get("phase") or "") != "gth_preparation":
        return []
    phase = str(event.get("phase") or "").lower()
    if phase not in _ACTIVE_EVENT_PHASES:
        return []
    expected_session_date = str(mandate.get("trading_date") or "")
    expected_expiry = expected_session_date.replace("-", "")
    expires_at = _datetime(event.get("expires_at"))
    reasons: list[str] = []
    if not expected_expiry or str(event.get("expiry") or "") != expected_expiry:
        reasons.append("gth_active_event_expiry_mismatch")
    source_session_date = str(event.get("session_date") or "")
    if source_session_date and source_session_date != expected_session_date:
        reasons.append("gth_active_event_session_mismatch")
    source_session_mode = str(event.get("session_mode") or "").lower()
    if source_session_mode and source_session_mode not in {"gth", "globex"}:
        reasons.append("gth_active_event_session_mismatch")
    if expires_at is None:
        reasons.append("gth_active_event_expiry_missing")
    elif expires_at <= _utc(now):
        reasons.append("gth_active_event_expired")
    return list(dict.fromkeys(reasons))


def _vertical_contracts_match_expiry(
    candidate: Mapping[str, Any],
    *,
    expected_expiry: str,
) -> bool:
    if not expected_expiry:
        return False
    for key in ("long", "short"):
        leg = _mapping(candidate.get(key))
        contract_id = str(leg.get("contract_id") or "")
        leg_expiry = str(leg.get("expiry") or "")
        if leg_expiry:
            if leg_expiry != expected_expiry:
                return False
        elif contract_id and expected_expiry not in contract_id:
            return False
    return True


def _mapping(value: object) -> Mapping[str, Any]:
    return value if isinstance(value, Mapping) else {}


def _datetime(value: object) -> datetime | None:
    if isinstance(value, datetime):
        try:
            return _utc(value)
        except ValueError:
            return None
    if not isinstance(value, str) or not value.strip():
        return None
    try:
        return _utc(datetime.fromisoformat(value.replace("Z", "+00:00")))
    except ValueError:
        return None


def _utc(value: datetime) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError("timezone-aware datetime required")
    return value.astimezone(timezone.utc)


__all__ = ["build_ranked_active_event", "gth_skew_rank_gate_reasons"]
