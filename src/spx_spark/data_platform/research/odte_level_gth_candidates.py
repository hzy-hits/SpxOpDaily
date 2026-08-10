"""Contract-strict loader for the current GTH runtime candidate lane."""

from __future__ import annotations

import math
from datetime import date, datetime
from pathlib import Path
from typing import Mapping

from spx_spark.strategy_contract import strategy_contract_issues

from .odte_level_signals import (
    MAX_ENTRY_LEG_SKEW,
    MAX_ENTRY_QUOTE_AGE,
    SET_GTH_LEVEL_CANDIDATE,
    Signal,
    _float,
    _iter_jsonl,
    _parse_ts,
)

_CANDIDATE_KIND = "gth_spxw_level_manual_spread_candidate"
_POLICY_PREFIX = "gth_level_manual_candidate.v1+sha256:"
_STRATEGY_ID = "gth_level_manual_candidate"
_LIFECYCLE_STATUS = "legacy_production"
_RUNTIME_STATUS = "production_runtime"


def load_gth_level_candidate_signals(features_root: Path) -> list[Signal]:
    """Load fully quoted current-lane READY and non-actionable WATCH observations."""

    signals: list[Signal] = []
    seen: set[str] = set()
    for path in sorted(features_root.glob("gth_level_manual_candidates/date=*/events.jsonl")):
        for record in _iter_jsonl(path):
            candidate_id = str(record.get("candidate_id") or "")
            if not candidate_id or candidate_id in seen:
                continue
            signal = _candidate_signal(record, candidate_id=candidate_id)
            if signal is None:
                continue
            seen.add(candidate_id)
            signals.append(signal)
    return signals


def _candidate_signal(
    record: Mapping[str, object],
    *,
    candidate_id: str,
) -> Signal | None:
    evaluated_at = _parse_ts(record.get("evaluated_at"))
    valid_until = _parse_ts(record.get("valid_until"))
    exit_at = _parse_ts(record.get("exit_at"))
    session_date = _parse_session(record.get("session_date"))
    status = str(record.get("status") or "")
    authority_contract_ok = (
        status == "manual_ready"
        and record.get("manual_action_eligible") is True
        # Preserve pre-authority READY rows for historical research only. New
        # production generation cannot emit this status without the validated
        # closed authority value, while the loader must not erase older quote
        # observations merely because they predate the field.
        and record.get("edge_authority")
        in {None, "validated_first_touch_time_stop_net_pnl"}
    ) or (
        status in {"selector_candidate", "structure_watch"}
        and record.get("manual_action_eligible") is False
        and (
            status != "selector_candidate"
            or record.get("selector_evidence_eligible") is True
        )
        and record.get("edge_authority") == "none"
        and record.get("edge_authority_reason")
        == "first_touch_time_stop_net_pnl_authority_unavailable"
    )
    if (
        evaluated_at is None
        or valid_until is None
        or exit_at is None
        or session_date is None
        or not evaluated_at < valid_until <= exit_at
        or record.get("event") != "gth_level_manual_candidate_evaluated"
        or record.get("kind") != _CANDIDATE_KIND
        or not authority_contract_ok
        or record.get("strategy_id") != _STRATEGY_ID
        or record.get("strategy_lane") != _STRATEGY_ID
        or record.get("lifecycle_status") != _LIFECYCLE_STATUS
        or record.get("runtime_status") != _RUNTIME_STATUS
        or record.get("execution_eligible") is not False
        or record.get("broker_submission_allowed") is not False
        or record.get("automatic_ordering") is not False
        or not str(record.get("policy_version") or "").startswith(_POLICY_PREFIX)
        or strategy_contract_issues(
            record,
            require_valid_until=True,
            require_actionable_coordinate=True,
        )
    ):
        return None

    direction = str(record.get("direction") or "")
    position_type = str(record.get("position_type") or "")
    long_contract_id = str(record.get("long_contract_id") or "")
    short_contract_id = str(record.get("short_contract_id") or "")
    long_contract = _parse_contract(long_contract_id)
    short_contract = _parse_contract(short_contract_id)
    width = _float(record.get("spread_width_points"))
    if not _valid_contract_pair(
        long_contract,
        short_contract,
        session_date=session_date,
        direction=direction,
        position_type=position_type,
        width=width,
    ):
        return None
    assert long_contract is not None and short_contract is not None and width is not None

    snapshot = record.get("exact_spread_snapshot")
    quote = _decision_quote(snapshot, evaluated_at=evaluated_at)
    if quote is None:
        return None
    decision_bid = quote["bid"]
    decision_mid = quote["mid"]
    decision_ask = quote["ask"]
    if not all(
        _same_number(record.get(field), value)
        for field, value in (
            ("decision_bid", decision_bid),
            ("decision_mid", decision_mid),
            ("decision_ask", decision_ask),
        )
    ):
        return None
    entry_limit = _float(record.get("entry_limit"))
    target_spx = _float(record.get("target_spx"))
    invalidation_spx = _float(record.get("invalidation_spx"))
    invalidation_es = _float(record.get("invalidation_es"))
    trigger_level = _float(record.get("trigger_level"))
    if (
        entry_limit is None
        or not 0 < decision_ask <= entry_limit < width
        or target_spx is None
        or invalidation_spx is None
        or invalidation_es is None
        or trigger_level is None
        or record.get("contract_id") != f"{long_contract_id}|-{short_contract_id}"
    ):
        return None
    basis_points = invalidation_es - invalidation_spx
    return Signal(
        set_name=SET_GTH_LEVEL_CANDIDATE,
        key=candidate_id,
        at=evaluated_at,
        direction=direction,
        level=trigger_level,
        strike=long_contract[1],
        expiry=session_date,
        entry_at=evaluated_at,
        level_kind=str(record.get("path_kind") or "gth_level_manual_candidate"),
        thesis=str(record.get("path_kind") or "gth_level_manual_candidate"),
        walls=(target_spx,),
        entry_px=decision_ask,
        entry_limit=entry_limit,
        entry_expires_at=valid_until,
        entry_provider="ibkr",
        decision_bid=decision_bid,
        decision_ask=decision_ask,
        decision_leg_sides=(
            quote["long_bid"],
            quote["long_ask"],
            quote["short_bid"],
            quote["short_ask"],
        ),
        target_level=target_spx,
        recorded_time_stop_at=exit_at,
        basis_points=basis_points,
        underlier_instrument="future:ES",
        invalidation_level=invalidation_spx,
        invalidation_buffer=0.0,
        target_mode="wall",
        trend_regime=(
            str(record.get("trend_regime")) if record.get("trend_regime") is not None else None
        ),
        session_bucket="gth",
        contract_id=long_contract_id,
        recorded_short_strike=short_contract[1],
        recorded_spread_width=width,
    )


def _decision_quote(
    snapshot: object,
    *,
    evaluated_at: datetime,
) -> dict[str, float] | None:
    if not isinstance(snapshot, Mapping):
        return None
    long = snapshot.get("long")
    short = snapshot.get("short")
    quality = snapshot.get("quality")
    snapshot_at = _parse_ts(snapshot.get("at"))
    if (
        not isinstance(long, Mapping)
        or not isinstance(short, Mapping)
        or long.get("provider") != "ibkr"
        or short.get("provider") != "ibkr"
        or not isinstance(quality, Mapping)
        or quality.get("status") != "ok"
        or snapshot_at != evaluated_at
        or not _fresh_leg(long, evaluated_at=evaluated_at)
        or not _fresh_leg(short, evaluated_at=evaluated_at)
    ):
        return None
    long_source = _parse_ts(long.get("source_at"))
    short_source = _parse_ts(short.get("source_at"))
    if long_source is None or short_source is None:
        return None
    if abs(long_source - short_source) > MAX_ENTRY_LEG_SKEW:
        return None
    bid = _float(snapshot.get("bid"))
    mid = _float(snapshot.get("mid"))
    ask = _float(snapshot.get("ask"))
    long_bid = _float(long.get("bid"))
    long_ask = _float(long.get("ask"))
    short_bid = _float(short.get("bid"))
    short_ask = _float(short.get("ask"))
    if (
        bid is None
        or mid is None
        or ask is None
        or long_bid is None
        or long_ask is None
        or short_bid is None
        or short_ask is None
        or not 0 <= bid <= mid <= ask
        or not 0 <= long_bid < long_ask
        or not 0 <= short_bid < short_ask
        or not math.isclose(bid, long_bid - short_ask, abs_tol=1e-6)
        or not math.isclose(ask, long_ask - short_bid, abs_tol=1e-6)
    ):
        return None
    return {
        "bid": bid,
        "mid": mid,
        "ask": ask,
        "long_bid": long_bid,
        "long_ask": long_ask,
        "short_bid": short_bid,
        "short_ask": short_ask,
    }


def _fresh_leg(leg: Mapping[str, object], *, evaluated_at: datetime) -> bool:
    source_at = _parse_ts(leg.get("source_at"))
    transport_at = _parse_ts(leg.get("transport_at"))
    quality = leg.get("quality")
    return bool(
        source_at is not None
        and transport_at is not None
        and source_at <= evaluated_at
        and transport_at <= evaluated_at
        and evaluated_at - source_at <= MAX_ENTRY_QUOTE_AGE
        and evaluated_at - transport_at <= MAX_ENTRY_QUOTE_AGE
        and isinstance(quality, Mapping)
        and quality.get("status") == "ok"
    )


def _valid_contract_pair(
    long: tuple[date, float, str] | None,
    short: tuple[date, float, str] | None,
    *,
    session_date: date,
    direction: str,
    position_type: str,
    width: float | None,
) -> bool:
    if long is None or short is None or width is None:
        return False
    expected_right = "C" if direction == "up" else "P" if direction == "down" else None
    expected_type = (
        "call_debit_spread"
        if direction == "up"
        else "put_debit_spread"
        if direction == "down"
        else None
    )
    return bool(
        expected_right is not None
        and position_type == expected_type
        and long[0] == short[0] == session_date
        and long[2] == short[2] == expected_right
        and math.isclose(abs(short[1] - long[1]), width, abs_tol=1e-6)
        and (short[1] > long[1] if direction == "up" else short[1] < long[1])
    )


def _parse_contract(value: str) -> tuple[date, float, str] | None:
    parts = value.split(":")
    if len(parts) != 6 or parts[:3] != ["option", "SPX", "SPXW"]:
        return None
    try:
        expiry = date.fromisoformat(f"{parts[3][:4]}-{parts[3][4:6]}-{parts[3][6:]}")
        strike = float(parts[4])
    except (ValueError, TypeError):
        return None
    if len(parts[3]) != 8 or strike <= 0 or parts[5] not in {"C", "P"}:
        return None
    return expiry, strike, parts[5]


def _parse_session(value: object) -> date | None:
    try:
        return date.fromisoformat(str(value))
    except ValueError:
        return None


def _same_number(value: object, expected: float) -> bool:
    parsed = _float(value)
    return parsed is not None and math.isclose(parsed, expected, abs_tol=1e-9)
