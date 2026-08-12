"""Prior-close event views expressed with existing SPXW debit vertical machinery."""

from __future__ import annotations

from collections.abc import Mapping
from datetime import datetime
from typing import Any

from spx_spark.application.order_map.candidate_factory import (
    _hash,
    _map,
    _number,
    _round_to_strike,
    _rth_option_legs,
    _time,
    _vertical_candidate_from_evidence,
)
from spx_spark.application.order_map.strategy_regime import StrategyPolicy
from spx_spark.market_calendar import DEFAULT_MARKET_CALENDAR
from spx_spark.storage import LatestState

EVENT_SETTLEMENT_SETUP = "EVENT_SETTLEMENT_THRESHOLD"
EVENT_SETTLEMENT_WIDTH = 5.0


def enumerate_event_settlement_candidates(
    payload: Mapping[str, Any],
    facts: Mapping[str, Any],
    latest: LatestState,
    *,
    now: datetime,
    policy: StrategyPolicy,
) -> list[dict[str, Any]]:
    """Map prior-close views to four adjacent 5-point debit verticals."""

    context = event_settlement_context(payload, now=now)
    if not context:
        return []
    threshold = float(context["threshold_level"])
    anchor = _round_to_strike(threshold)
    if anchor is None:
        return []
    expiry = str(context["expiry"])
    specs = (
        ("UP", "C", anchor - EVENT_SETTLEMENT_WIDTH, anchor),
        ("UP", "C", anchor, anchor + EVENT_SETTLEMENT_WIDTH),
        ("DOWN", "P", anchor + EVENT_SETTLEMENT_WIDTH, anchor),
        ("DOWN", "P", anchor, anchor - EVENT_SETTLEMENT_WIDTH),
    )
    rows = []
    for direction, right, long_strike, short_strike in specs:
        legs = _rth_option_legs(
            latest,
            expiry,
            ((long_strike, right), (short_strike, right)),
            now=now,
            policy=policy,
        )
        if not legs:
            continue
        long, short = legs
        row = _vertical_candidate_from_evidence(
            {
                "setup_kind": EVENT_SETTLEMENT_SETUP,
                "setup_variant": f"CLOSE_{'ABOVE' if direction == 'UP' else 'BELOW'}_PRIOR_CLOSE",
                "setup_state": "ENTRY_WINDOW_OPEN",
                "direction": direction,
                "trigger_level": threshold,
                "target_spx": threshold,
                "invalidation_spx": None,
                "long": long,
                "short": short,
                "valid_until": context["release_at"],
                "source": "prior_close_event_view",
                "geometry_source": "event_settlement_threshold",
            },
            facts,
            now=now,
            policy=policy,
        )
        if not row:
            continue
        economics = _map(row.get("economics"))
        debit_fraction = _number(economics.get("debit_fraction_of_width"))
        breakeven = _number(economics.get("breakeven_spx"))
        gap = (
            breakeven - threshold
            if direction == "UP" and breakeven is not None
            else threshold - breakeven
            if direction == "DOWN" and breakeven is not None
            else None
        )
        event_kind = "terminal_above" if direction == "UP" else "terminal_below"
        identity = (
            facts.get("session_date"),
            EVENT_SETTLEMENT_SETUP,
            context.get("event_id"),
            expiry,
            direction,
            long_strike,
            short_strike,
            round(threshold, 4),
        )
        candidate_id = _hash(identity)[:16]
        row.update(
            {
                "candidate_id": candidate_id,
                "opportunity_id": f"strategy-opportunity:{_hash((identity, long.get('contract_id'), short.get('contract_id')))[:24]}",
                "manual_authority_eligible": True,
                "event_spans_release": True,
                "probability_event": {
                    "event_id": f"event-threshold:{_hash((context.get('event_id'), expiry, event_kind, round(threshold, 4)))[:24]}",
                    "kind": event_kind,
                    "target_at": context["target_at"],
                    "lower_level": round(threshold, 4) if direction == "UP" else None,
                    "upper_level": round(threshold, 4) if direction == "DOWN" else None,
                },
                "view": {
                    "source": "PRIOR_CLOSE",
                    "threshold_level": round(threshold, 4),
                    "target_at": context["target_at"],
                    "macro_event_id": context.get("event_id"),
                    "macro_event_name": context.get("event_name"),
                    "release_at": context["release_at"],
                    "market_odds_proxy": round(debit_fraction, 6) if debit_fraction is not None else None,
                    "breakeven_gap_points": round(gap, 4) if gap is not None else None,
                    "evidence_status": "thesis_driven_unvalidated",
                },
                "edge": {
                    "edge_status": "thesis_driven_unvalidated",
                    "required_p_breakeven": round(debit_fraction, 6) if debit_fraction is not None else None,
                    "model_p": None,
                    "advisories": ["physical_probability_not_estimated"],
                },
            }
        )
        rows.append(row)
    return rows


def event_settlement_generation_reason(
    payload: Mapping[str, Any], *, now: datetime
) -> str | None:
    return (
        "event_settlement_exact_two_leg_quote_unavailable"
        if event_settlement_context(payload, now=now)
        else None
    )


def event_settlement_context(
    payload: Mapping[str, Any], *, now: datetime
) -> dict[str, Any]:
    frame = _map(payload.get("option_structure_frame"))
    expiry = str(frame.get("front_expiry") or payload.get("expiry") or "")
    threshold = _number(_map(payload.get("day_move")).get("prior_close"))
    if not expiry or threshold is None or threshold <= 0.0:
        return {}
    macro = _map(payload.get("macro_event"))
    active, upcoming = _map(macro.get("active_event")), _map(macro.get("next_event"))
    event = active or upcoming
    release_at = _time(event.get("release_at"))
    if release_at is None or release_at <= now:
        event = upcoming
        release_at = _time(event.get("release_at"))
    if (
        release_at is None
        or release_at <= now
        or str(event.get("impact") or "").lower() not in {"high", "critical"}
    ):
        return {}
    try:
        session = DEFAULT_MARKET_CALENDAR.session(
            datetime.strptime(expiry, "%Y%m%d").date()
        )
    except ValueError:
        return {}
    if session is None or release_at > session.close_at:
        return {}
    return {
        "expiry": expiry,
        "threshold_level": threshold,
        "event_id": event.get("id"),
        "event_name": event.get("name"),
        "release_at": release_at.isoformat(),
        "target_at": session.close_at.isoformat(),
    }
