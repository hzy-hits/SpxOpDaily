"""Train candidate-level SPXW edge models from existing operational history.

This one-shot offline command replays conservative combination bids from the
quote lake, labels selected/no-trade/shadow candidates with the production
debit management contract (50% premium stop, trail, 15:45 ET hard close; no
20-minute time stop), performs session-purged walk-forward validation, and
emits the JSON artifact consumed by ``strategy_edge_model``.

The command never auto-promotes a weak model: each RTH/GTH structure family
must pass explicit out-of-sample PnL, drawdown, concentration, and coverage
gates before runtime may authorize a manual card.
"""

from __future__ import annotations

import argparse
from collections import defaultdict
from collections.abc import Mapping, Sequence
from datetime import date, datetime, timedelta, timezone
import json
import math
from pathlib import Path
import sqlite3
from typing import Any

import numpy as np

from spx_spark.analytics.options.strategy_payoff import (
    DEFAULT_MANAGEMENT_POLICY,
    conservative_vertical_bbo,
    policy_mark_horizon_end,
    simulate_management_policy,
    vertical_economics,
)
from spx_spark.application.order_map.strategy_edge_model import (
    ARTIFACT_RELATIVE_PATH,
    FEATURE_NAMES,
    FEATURE_VERSION,
    SCHEMA_VERSION,
    candidate_edge_features,
    edge_model_key,
    feature_vector,
)
from spx_spark.application.order_map.candidate_factory import enumerate_candidates
from spx_spark.application.order_map.strategy_regime import (
    DEFAULT_STRATEGY_POLICY,
    assess_regime,
)
from spx_spark.data_platform.research.odte_level_quotes import (
    QuoteStore,
    latest_state_from_lake,
)
from spx_spark.data_platform.research.strategy_policy_backfill import (
    _candidate_legs,
    _combo_bid_marks,
    _entry_ask,
)
from spx_spark.data_platform.research.strategy_v3_freeze_acceptance import (
    build_pass_b_payload_stub,
)
from spx_spark.market_calendar import DEFAULT_MARKET_CALENDAR


ENTRY_EDGE_POLICY = DEFAULT_MANAGEMENT_POLICY
GeometryKey = tuple[str, str, str, tuple[float, float]]
DEFAULT_THRESHOLDS = {
    "min_expected_pnl_points": 0.25,
    "min_expected_pnl_lcb_points": 0.0,
    "min_p_profit": 0.58,
    "max_p_stop_first_5m": 0.30,
    "min_return_on_risk": 0.08,
}
# Empirical q10 on ~10 OOF rows is the worst residual and can exceed a 10-wide
# debit. Until OOF coverage reaches promotion size, haircut at most 1 SPX point.
SMALL_OOF_RESIDUAL_FLOOR_POINTS = -1.0
SMALL_OOF_RESIDUAL_ROWS = 60
DEFAULT_PROMOTION_GATES = {
    "min_oof_trades": 60,
    "min_holdout_trades": 15,
    "min_profit_factor": 1.25,
    "min_average_pnl_points": 0.15,
    "min_positive_session_ratio": 0.55,
    "max_drawdown_r": 6.0,
    "max_top_session_profit_concentration": 0.35,
}


def load_candidate_labels(
    *,
    database_path: Path,
    data_root: Path,
    start_date: str | None = None,
    end_date: str | None = None,
    lookforward_minutes: int | None = None,
    enumerate_from_lake: bool = False,
    funnel: dict[str, Any] | None = None,
) -> list[dict[str, Any]]:
    """Label the earliest causal print of every eligible debit geometry."""

    decisions = _read_candidate_decisions(
        database_path,
        start_date=start_date,
        end_date=end_date,
        include_all_statuses=enumerate_from_lake,
    )
    counts: dict[str, Any] = {
        "raw_decisions": len(decisions),
        "malformed_payloads": 0,
        "empty_candidate_heartbeats": 0,
        "decisions_with_candidate_or_nearest": 0,
        "primary_non_debit_rows": 0,
        "candidate_occurrences": {
            "primary_debit": 0,
            "shadow_debit": 0,
            "considered_debit_passed_hard_gates": 0,
        },
        "considered": {
            "items": 0,
            "debit_vertical_items": 0,
            "debit_vertical_gate_failed": 0,
            "debit_vertical_gate_audit_missing": 0,
        },
    }

    # A sticky winner may reuse one opportunity_id for thousands of cycles.
    # Geometry is the training identity: session + RTH/GTH + type + ordered strikes.
    # Session mode is part of the key so a GTH print cannot steal the RTH row.
    deduped: dict[GeometryKey, dict[str, Any]] = {}
    for decision in decisions:
        if decision.get("_malformed_payload") is True:
            counts["malformed_payloads"] += 1
            continue
        for candidate, source, source_priority in _candidate_observations(
            decision,
            counts=counts,
        ):
            key = _candidate_geometry_key(decision, candidate)
            if key is None:
                counts.setdefault("invalid_candidate_geometries", 0)
                counts["invalid_candidate_geometries"] += 1
                continue
            _keep_earliest_geometry(
                deduped,
                key=key,
                decision=decision,
                candidate=candidate,
                source=source,
                source_priority=source_priority,
            )

    occurrences = sum(counts["candidate_occurrences"].values())
    invalid_geometries = int(counts.get("invalid_candidate_geometries") or 0)
    persisted_geometries = len(deduped)
    counts["eligible_candidate_occurrences"] = occurrences
    counts["persisted_candidate_geometries"] = persisted_geometries
    counts["duplicate_candidate_occurrences_dropped"] = (
        occurrences - invalid_geometries - persisted_geometries
    )

    store = QuoteStore(data_root)
    rows: list[dict[str, Any]] = []
    label_drops: defaultdict[str, int] = defaultdict(int)
    try:
        if enumerate_from_lake:
            counts["lake_enumeration"] = _enumerate_lake_geometries(
                decisions,
                deduped=deduped,
                store=store,
                data_root=data_root,
            )
        counts["unique_candidate_geometries"] = len(deduped)
        by_session: defaultdict[str, list[dict[str, Any]]] = defaultdict(list)
        for decision in deduped.values():
            by_session[str(decision.get("session_date") or "")].append(decision)
        source_by_session: defaultdict[str, list[dict[str, Any]]] = defaultdict(list)
        for decision in decisions:
            source_by_session[str(decision.get("session_date") or "")].append(decision)
        for session, session_decisions in sorted(by_session.items()):
            if enumerate_from_lake:
                window = _load_session_option_window(
                    store,
                    session=session,
                    decisions=source_by_session.get(session, ()),
                    candidates=session_decisions,
                )
                lake_session = counts["lake_enumeration"]["sessions"].setdefault(
                    session,
                    {},
                )
                lake_session["label_window_rows"] = window[1] if window else 0
            for decision in sorted(
                session_decisions,
                key=lambda item: str(item.get("decision_at") or ""),
            ):
                row = _label_decision(
                    decision,
                    store=store,
                    lookforward_minutes=lookforward_minutes,
                    drop_counts=label_drops,
                )
                if row is not None:
                    rows.append(row)
    finally:
        store.close()
    labeled_by_source: defaultdict[str, int] = defaultdict(int)
    labeled_by_model: defaultdict[str, int] = defaultdict(int)
    labeled_sessions_by_model: defaultdict[str, set[str]] = defaultdict(set)
    for row in rows:
        source = str(row.get("candidate_source") or "unknown")
        model_key = str(row.get("model_key") or "unknown")
        session = str(row.get("session_date") or "")
        labeled_by_source[source] += 1
        labeled_by_model[model_key] += 1
        if session:
            labeled_sessions_by_model[model_key].add(session)
    counts["labeling"] = {
        "geometries_attempted": len(deduped),
        "dropped": dict(sorted(label_drops.items())),
        "labeled_rows": len(rows),
        "labeled_by_source": dict(sorted(labeled_by_source.items())),
        "labeled_by_model": dict(sorted(labeled_by_model.items())),
        "sessions_by_model": {
            key: sorted(value) for key, value in sorted(labeled_sessions_by_model.items())
        },
    }
    if funnel is not None:
        funnel.update(counts)
    return rows


def _keep_earliest_geometry(
    deduped: dict[GeometryKey, dict[str, Any]],
    *,
    key: GeometryKey,
    decision: Mapping[str, Any],
    candidate: Mapping[str, Any],
    source: str,
    source_priority: int,
) -> str:
    observation = {
        **dict(decision),
        "candidate": dict(candidate),
        "candidate_source": source,
        "candidate_source_priority": source_priority,
    }
    order = (
        str(observation.get("decision_at") or ""),
        source_priority,
        str(observation.get("decision_id") or ""),
    )
    known = deduped.get(key)
    if known is None:
        deduped[key] = observation
        return "added"
    known_order = (
        str(known.get("decision_at") or ""),
        int(known.get("candidate_source_priority") or 0),
        str(known.get("decision_id") or ""),
    )
    if order < known_order:
        deduped[key] = observation
        return "replaced"
    return "duplicate"


def _enumerate_lake_geometries(
    decisions: Sequence[Mapping[str, Any]],
    *,
    deduped: dict[GeometryKey, dict[str, Any]],
    store: QuoteStore,
    data_root: Path,
) -> dict[str, Any]:
    counts: dict[str, Any] = {
        "enabled": True,
        "facts_source": "decisions.market_facts",
        "pre_v2_quote_only_history_excluded": True,
        "pre_v2_exclusion_reason": "market_facts_unavailable",
        "decisions_with_market_facts": 0,
        "invalid_decision_times": 0,
        "sampled_unique_utc_minutes": 0,
        "snapshots_ready": 0,
        "snapshots_unavailable": 0,
        "enumerated_candidates": 0,
        "debit_vertical_occurrences": 0,
        "unique_candidate_geometries_added": 0,
        "earlier_causal_geometries_replaced": 0,
        "duplicate_geometries_dropped": 0,
        "sessions": {},
    }
    sampled_by_session: defaultdict[str, list[Mapping[str, Any]]] = defaultdict(list)
    seen_minutes: set[datetime] = set()
    for decision in decisions:
        facts = _map(decision.get("market_facts"))
        if not facts:
            continue
        counts["decisions_with_market_facts"] += 1
        at = _time(decision.get("decision_at"))
        if at is None:
            counts["invalid_decision_times"] += 1
            continue
        minute = at.replace(second=0, microsecond=0)
        if minute in seen_minutes:
            continue
        seen_minutes.add(minute)
        sampled_by_session[str(decision.get("session_date") or "")].append(decision)
    counts["sampled_unique_utc_minutes"] = len(seen_minutes)

    for session, sampled in sorted(sampled_by_session.items()):
        persisted = [decision for key, decision in deduped.items() if key[0] == session]
        window = _load_session_option_window(
            store,
            session=session,
            decisions=sampled,
            candidates=persisted,
        )
        session_counts = {
            "sampled_minutes": len(sampled),
            "expiry": window[0] if window else None,
            "enumeration_window_rows": window[1] if window else 0,
            "snapshots_ready": 0,
            "snapshots_unavailable": 0,
            "enumerated_candidates": 0,
            "debit_vertical_occurrences": 0,
            "unique_candidate_geometries_added": 0,
        }
        counts["sessions"][session] = session_counts
        if window is None:
            counts["snapshots_unavailable"] += len(sampled)
            session_counts["snapshots_unavailable"] = len(sampled)
            continue
        expiry, _window_rows = window
        for decision in sampled:
            decision_at = _time(decision.get("decision_at"))
            facts = _map(decision.get("market_facts"))
            spot = _number(_map(facts.get("spot")).get("spx"))
            if decision_at is None or spot is None:
                counts["snapshots_unavailable"] += 1
                session_counts["snapshots_unavailable"] += 1
                continue
            trigger = _map(facts.get("trigger"))
            level = _number(trigger.get("level"))
            mode = "gth" if DEFAULT_MARKET_CALENDAR.is_spx_gth_open(decision_at) else "rth"
            max_age = (
                DEFAULT_STRATEGY_POLICY.gth_quote_max_age_seconds
                if mode == "gth"
                else DEFAULT_STRATEGY_POLICY.quote_max_age_seconds
            )
            latest = latest_state_from_lake(
                store,
                expiry=expiry,
                spot=spot,
                trigger=level,
                decision_at=decision_at,
                max_age_seconds=max_age,
            )
            if latest is None:
                counts["snapshots_unavailable"] += 1
                session_counts["snapshots_unavailable"] += 1
                continue
            counts["snapshots_ready"] += 1
            session_counts["snapshots_ready"] += 1
            regime = _map(decision.get("regime"))
            if not regime:
                regime = assess_regime(facts)
            if "pin" not in regime:
                regime = {**regime, "pin": {"top_centers": [], "depin_risk": 0.0}}
            direction = _enumeration_direction(facts, regime)
            if level is None:
                level = _enumeration_trigger_level(facts, spot=spot)
            payload = build_pass_b_payload_stub(
                row=decision,
                facts=facts,
                expiry=expiry,
                latest=latest,
                decision_at=decision_at,
                direction=direction,
                level=level,
                spot=spot,
                data_root=data_root,
                trade_intent={},
            )
            candidates = enumerate_candidates(
                payload,
                facts,
                regime,
                latest,
                now=decision_at,
                policy=DEFAULT_STRATEGY_POLICY,
            )
            counts["enumerated_candidates"] += len(candidates)
            session_counts["enumerated_candidates"] += len(candidates)
            for candidate in candidates:
                if not _is_debit_vertical(candidate):
                    continue
                counts["debit_vertical_occurrences"] += 1
                session_counts["debit_vertical_occurrences"] += 1
                key = _candidate_geometry_key(decision, candidate)
                if key is None:
                    continue
                result = _keep_earliest_geometry(
                    deduped,
                    key=key,
                    decision=decision,
                    candidate=candidate,
                    source="lake_enumeration",
                    source_priority=3,
                )
                if result == "added":
                    counts["unique_candidate_geometries_added"] += 1
                    session_counts["unique_candidate_geometries_added"] += 1
                elif result == "replaced":
                    counts["earlier_causal_geometries_replaced"] += 1
                else:
                    counts["duplicate_geometries_dropped"] += 1
    return counts


def _load_session_option_window(
    store: QuoteStore,
    *,
    session: str,
    decisions: Sequence[Mapping[str, Any]],
    candidates: Sequence[Mapping[str, Any]],
) -> tuple[str, int] | None:
    timestamps: list[datetime] = []
    anchors: list[float] = []
    expiries: list[date] = []
    for decision in (*decisions, *candidates):
        at = _time(decision.get("decision_at"))
        if at is not None:
            timestamps.append(at)
        facts = _map(decision.get("market_facts"))
        candidate = _candidate(decision)
        expiry = _candidate_expiry(candidate, facts)
        if expiry is not None:
            expiries.append(expiry)
        structure = _map(facts.get("structure"))
        raw_flip = structure.get("flip_zone")
        if isinstance(raw_flip, Sequence) and not isinstance(raw_flip, (str, bytes)):
            flip_values = tuple(raw_flip)
        else:
            flip_map = _map(raw_flip)
            flip_values = (flip_map.get("low"), flip_map.get("high"))
        for value in (
            _map(facts.get("spot")).get("spx"),
            _map(facts.get("trigger")).get("level"),
            structure.get("put_wall"),
            structure.get("zero_gamma"),
            structure.get("call_wall"),
            *flip_values,
            *_candidate_strikes(candidate),
        ):
            number = _number(value)
            if number is not None:
                anchors.append(number)
    if not timestamps or not anchors:
        return None
    if expiries:
        expiry_day = min(expiries)
    else:
        session_day = _date(session)
        if session_day is not None and DEFAULT_MARKET_CALENDAR.is_trading_day(session_day):
            expiry_day = session_day
        else:
            expiry_day = DEFAULT_MARKET_CALENDAR.research_expiry(min(timestamps))
    max_width = max(DEFAULT_STRATEGY_POLICY.gth_widths or (20.0,))
    max_offset = max(
        (abs(value) for value in DEFAULT_STRATEGY_POLICY.gth_long_offsets),
        default=0.0,
    )
    margin = max_width + max_offset + 5.0
    strike_min = math.floor((min(anchors) - margin) / 5.0) * 5.0
    strike_max = math.ceil((max(anchors) + margin) / 5.0) * 5.0
    start = min(timestamps) - timedelta(seconds=DEFAULT_STRATEGY_POLICY.gth_quote_max_age_seconds)
    end = max(timestamps)
    session_day = _date(session)
    if session_day is not None:
        for candidate in candidates:
            at = _time(candidate.get("decision_at"))
            if at is not None:
                end = max(
                    end,
                    policy_mark_horizon_end(
                        at,
                        ENTRY_EDGE_POLICY,
                        session_date=session_day,
                        lookforward_minutes=None,
                    ),
                )
    row_count = store.load_option_window(
        expiry=expiry_day,
        strike_min=strike_min,
        strike_max=strike_max,
        start=start,
        end=end,
    )
    return expiry_day.strftime("%Y%m%d"), row_count


def _enumeration_direction(
    facts: Mapping[str, Any],
    regime: Mapping[str, Any],
) -> str | None:
    sources = [
        _map(facts.get("trigger")).get("direction"),
        _map(facts.get("gth_evidence")).get("direction"),
        _map(facts.get("gth_dip_reclaim_evidence")).get("direction"),
        regime.get("path_direction"),
    ]
    sources.extend(_map(value).get("direction") for value in facts.get("rth_setups") or ())
    for value in sources:
        direction = str(value or "").upper()
        if direction in {"UP", "DOWN"}:
            return direction
    return None


def _enumeration_trigger_level(
    facts: Mapping[str, Any],
    *,
    spot: float,
) -> float:
    sources = [
        _map(facts.get("gth_evidence")).get("trigger_level"),
        _map(facts.get("gth_dip_reclaim_evidence")).get("trigger_level"),
    ]
    sources.extend(_map(value).get("trigger_level") for value in facts.get("rth_setups") or ())
    for value in sources:
        level = _number(value)
        if level is not None:
            return level
    return spot


def _label_decision(
    decision: Mapping[str, Any],
    *,
    store: QuoteStore,
    lookforward_minutes: int | None,
    drop_counts: defaultdict[str, int] | None = None,
) -> dict[str, Any] | None:
    candidate = _candidate(decision)
    decision_at = _time(decision.get("decision_at"))
    strategy_type = str(candidate.get("strategy_type") or "").upper()
    if not candidate or decision_at is None or not strategy_type.endswith("_DEBIT_VERTICAL"):
        return _label_drop(drop_counts, "invalid_candidate_or_decision_time")
    facts = _map(decision.get("market_facts"))
    regime = _map(decision.get("regime"))
    session_date = _date(decision.get("session_date"))
    if session_date is None:
        return _label_drop(drop_counts, "invalid_session_date")
    rebuilt, failure = _rebuild_candidate_at_decision(
        candidate,
        facts=facts,
        decision_at=decision_at,
        store=store,
    )
    if rebuilt is None:
        return _label_drop(drop_counts, failure or "entry_bbo_unavailable")
    candidate = rebuilt
    legs = _candidate_legs(candidate)
    if len(legs) < 2:
        return _label_drop(drop_counts, "candidate_legs_unavailable")
    entry_ask = _entry_ask(legs)
    if entry_ask is None:
        return _label_drop(drop_counts, "conservative_entry_ask_unavailable")
    provider = str(legs[0].get("provider") or "schwab")
    end = policy_mark_horizon_end(
        decision_at,
        ENTRY_EDGE_POLICY,
        session_date=session_date,
        lookforward_minutes=(
            None if ENTRY_EDGE_POLICY.time_stop_minutes is None else lookforward_minutes
        ),
    )
    marks = _combo_bid_marks(
        store,
        legs=legs,
        provider=provider,
        start=decision_at,
        end=end,
    )
    if not marks:
        return _label_drop(drop_counts, "exit_marks_unavailable")
    label = simulate_management_policy(
        marks,
        entry_ask=entry_ask,
        leg_count=len(legs),
        entry_at=decision_at,
        policy=ENTRY_EDGE_POLICY,
        session_date=session_date,
    )
    features = candidate_edge_features(
        candidate,
        facts,
        regime,
        now=decision_at,
    )
    exit_seconds = (
        (label.exit_at - decision_at).total_seconds() if label.exit_at is not None else None
    )
    max_loss = _number(_map(candidate.get("economics")).get("max_loss_points"))
    return {
        "schema_version": "strategy_edge_training_row.v1",
        "decision_id": decision.get("decision_id"),
        "session_date": session_date.isoformat(),
        "decision_at": decision_at.isoformat(),
        "model_key": edge_model_key(candidate, facts),
        "strategy_type": candidate.get("strategy_type"),
        "setup_kind": candidate.get("setup_kind"),
        "direction": candidate.get("direction"),
        "candidate_id": candidate.get("candidate_id"),
        "opportunity_id": candidate.get("opportunity_id"),
        "candidate_source": decision.get("candidate_source"),
        "entry_ask": entry_ask,
        "max_loss_points": max_loss if max_loss is not None else entry_ask,
        "features": features,
        "policy_pnl_points": label.policy_pnl_points,
        "profit": int(label.policy_pnl_points > 0.0),
        "stop_first_5m": int(
            label.exit_reason == "premium_stop"
            and exit_seconds is not None
            and exit_seconds <= 300.0
        ),
        "exit_reason": label.exit_reason,
        "exit_seconds": exit_seconds,
        "mfe_points": label.mfe_points,
        "mae_points": label.mae_points,
        "policy_version": label.policy_version,
    }


def _candidate_observations(
    decision: Mapping[str, Any],
    *,
    counts: dict[str, Any],
) -> list[tuple[Mapping[str, Any], str, int]]:
    observations: list[tuple[Mapping[str, Any], str, int]] = []
    selected = _map(decision.get("candidate"))
    nearest = _map(_map(decision.get("why_not")).get("nearest_candidate"))
    primary = selected or nearest
    if primary:
        counts["decisions_with_candidate_or_nearest"] += 1
        if _is_debit_vertical(primary):
            counts["candidate_occurrences"]["primary_debit"] += 1
            observations.append((primary, "candidate" if selected else "nearest_candidate", 0))
        else:
            counts["primary_non_debit_rows"] += 1
    else:
        counts["empty_candidate_heartbeats"] += 1

    raw_shadows = decision.get("shadow_candidates")
    if isinstance(raw_shadows, Sequence) and not isinstance(raw_shadows, (str, bytes)):
        for raw in raw_shadows:
            candidate = _map(raw)
            if not _is_debit_vertical(candidate):
                continue
            counts["candidate_occurrences"]["shadow_debit"] += 1
            observations.append((candidate, "shadow_candidate", 1))

    raw_considered = decision.get("candidates_considered")
    if isinstance(raw_considered, Sequence) and not isinstance(
        raw_considered,
        (str, bytes),
    ):
        for raw in raw_considered:
            counts["considered"]["items"] += 1
            candidate = _map(raw)
            if not _is_debit_vertical(candidate):
                continue
            counts["considered"]["debit_vertical_items"] += 1
            failures = candidate.get("gate_failures")
            if not isinstance(failures, Sequence) or isinstance(failures, (str, bytes)):
                counts["considered"]["debit_vertical_gate_audit_missing"] += 1
                continue
            if failures:
                counts["considered"]["debit_vertical_gate_failed"] += 1
                continue
            counts["candidate_occurrences"]["considered_debit_passed_hard_gates"] += 1
            observations.append((candidate, "candidates_considered", 2))
    return observations


def _candidate_geometry_key(
    decision: Mapping[str, Any],
    candidate: Mapping[str, Any],
) -> GeometryKey | None:
    session = str(decision.get("session_date") or "").strip()
    strategy_type = str(candidate.get("strategy_type") or "").upper()
    strikes = _candidate_strikes(candidate)
    if not session or not strategy_type.endswith("_DEBIT_VERTICAL") or len(strikes) != 2:
        return None
    return session, _session_mode(decision), strategy_type, (strikes[0], strikes[1])


def _session_mode(decision: Mapping[str, Any]) -> str:
    mode = str(
        _map(_map(decision.get("market_facts")).get("session")).get("mode") or ""
    ).strip().lower()
    if mode in {"rth", "gth"}:
        return mode
    at = _time(decision.get("decision_at"))
    if at is None:
        return "unknown"
    if DEFAULT_MARKET_CALENDAR.is_rth_open(at):
        return "rth"
    if DEFAULT_MARKET_CALENDAR.is_spx_gth_open(at):
        return "gth"
    return "unknown"


def _rebuild_candidate_at_decision(
    candidate: Mapping[str, Any],
    *,
    facts: Mapping[str, Any],
    decision_at: datetime,
    store: QuoteStore,
) -> tuple[dict[str, Any] | None, str | None]:
    strategy_type = str(candidate.get("strategy_type") or "").upper()
    if strategy_type == "CALL_DEBIT_VERTICAL":
        right, direction = "C", "UP"
    elif strategy_type == "PUT_DEBIT_VERTICAL":
        right, direction = "P", "DOWN"
    else:
        return None, "strategy_type_not_supported"
    strikes = _candidate_strikes(candidate)
    if len(strikes) != 2:
        return None, "ordered_strikes_unavailable"
    expiry = _candidate_expiry(candidate, facts)
    if expiry is None:
        return None, "front_expiry_unavailable"
    provider = _candidate_provider(candidate, facts)
    if provider is None:
        return None, "entry_provider_unavailable"
    mode = str(_map(facts.get("session")).get("mode") or "").lower()
    max_age = (
        DEFAULT_STRATEGY_POLICY.gth_quote_max_age_seconds
        if mode == "gth"
        else DEFAULT_STRATEGY_POLICY.quote_max_age_seconds
    )
    max_skew = (
        DEFAULT_STRATEGY_POLICY.gth_quote_max_skew_seconds
        if mode == "gth"
        else DEFAULT_STRATEGY_POLICY.quote_max_skew_seconds
    )
    existing_legs = _candidate_legs(candidate)
    rebuilt_legs: list[dict[str, Any]] = []
    for index, (strike, quantity) in enumerate(zip(strikes, (1.0, -1.0), strict=True)):
        ticks = store.option_series(
            provider=provider,
            expiry=expiry,
            strike=strike,
            right=right,
            start=decision_at - timedelta(seconds=max_age),
            end=decision_at,
        )
        tick = next(
            (
                value
                for value in reversed(ticks)
                if _number(value.bid) is not None
                and float(value.bid) >= 0.0
                and _number(value.ask) is not None
                and float(value.ask) > 0.0
            ),
            None,
        )
        if tick is None:
            return None, "entry_leg_live_nbbo_unavailable"
        base = dict(existing_legs[index]) if index < len(existing_legs) else {}
        rebuilt_legs.append(
            {
                **base,
                "expiry": expiry.isoformat(),
                "strike": strike,
                "right": right,
                "quantity": quantity,
                "provider": provider,
                "bid": float(tick.bid),
                "ask": float(tick.ask),
                "source_at": (tick.source_at or tick.at).isoformat(),
                "received_at": tick.at.isoformat(),
                "delta": tick.delta if tick.delta is not None else base.get("delta"),
                "implied_vol": base.get("implied_vol"),
            }
        )
    quote = conservative_vertical_bbo(
        rebuilt_legs[0],
        rebuilt_legs[1],
        now=decision_at,
        max_quote_age_seconds=max_age,
        max_source_skew_seconds=max_skew,
    )
    if quote.get("status") != "ready":
        reasons = {str(value) for value in quote.get("reasons") or ()}
        if "spread_leg_time_skew_exceeded" in reasons:
            return None, "entry_leg_time_skew_exceeded"
        return None, "conservative_entry_bbo_unavailable"
    try:
        economics = vertical_economics(
            long_strike=strikes[0],
            short_strike=strikes[1],
            net_debit=float(quote["ask"]),
            right=right,
        )
    except (KeyError, TypeError, ValueError):
        return None, "vertical_economics_unavailable"
    rebuilt = {
        **dict(candidate),
        "strategy_type": strategy_type,
        "direction": str(candidate.get("direction") or direction),
        "right": right,
        "long": rebuilt_legs[0],
        "short": rebuilt_legs[1],
        "legs": rebuilt_legs,
        "quote": dict(quote),
        "economics": economics,
    }
    return rebuilt, None


def _candidate_strikes(candidate: Mapping[str, Any]) -> tuple[float, ...]:
    raw = candidate.get("strikes")
    if isinstance(raw, Sequence) and not isinstance(raw, (str, bytes)):
        values = tuple(_number(value) for value in raw)
    else:
        legs = _candidate_legs(candidate)
        values = tuple(_number(leg.get("strike")) for leg in legs)
    return tuple(float(value) for value in values if value is not None)


def _candidate_expiry(
    candidate: Mapping[str, Any],
    facts: Mapping[str, Any],
) -> date | None:
    values: list[object] = [candidate.get("expiry")]
    legs = _candidate_legs(candidate)
    values.extend(leg.get("expiry") for leg in legs)
    for leg in legs:
        parts = str(leg.get("contract_id") or "").split(":")
        if len(parts) >= 6 and parts[0] == "option":
            values.append(parts[-3])
    structure = _map(facts.get("structure"))
    differential = _map(structure.get("strike_differential_context"))
    values.extend(
        (
            facts.get("front_expiry"),
            differential.get("front_expiry"),
            differential.get("expiry"),
        )
    )
    for value in values:
        parsed = _date(value)
        if parsed is not None:
            return parsed
    return None


def _candidate_provider(
    candidate: Mapping[str, Any],
    facts: Mapping[str, Any],
) -> str | None:
    providers = {
        str(leg.get("provider") or "").lower()
        for leg in _candidate_legs(candidate)
        if str(leg.get("provider") or "").strip()
    }
    quote_provider = str(_map(candidate.get("quote")).get("provider") or "").lower()
    if quote_provider:
        providers.add(quote_provider)
    if len(providers) == 1:
        return next(iter(providers))
    if len(providers) > 1:
        return None
    mode = str(_map(facts.get("session")).get("mode") or "").lower()
    if mode == "gth":
        return "ibkr"
    if mode == "rth":
        return "schwab"
    return None


def _is_debit_vertical(candidate: Mapping[str, Any]) -> bool:
    return str(candidate.get("strategy_type") or "").upper() in {
        "CALL_DEBIT_VERTICAL",
        "PUT_DEBIT_VERTICAL",
    }


def _label_drop(
    counts: defaultdict[str, int] | None,
    reason: str,
) -> None:
    if counts is not None:
        counts[reason] += 1
    return None


def train_edge_artifact(
    rows: Sequence[Mapping[str, Any]],
    *,
    generated_at: datetime,
    holdout_sessions: int = 8,
    min_train_sessions: int = 3,
    thresholds: Mapping[str, float] = DEFAULT_THRESHOLDS,
    promotion_gates: Mapping[str, float] = DEFAULT_PROMOTION_GATES,
) -> tuple[dict[str, Any], dict[str, Any]]:
    """Train independent session/family models with expanding walk-forward OOF."""

    by_key: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for raw in rows:
        row = dict(raw)
        model_key = str(row.get("model_key") or "")
        if model_key.startswith("unknown|"):
            continue
        if _valid_training_row(row):
            by_key[model_key].append(row)

    models: dict[str, Any] = {}
    reports: dict[str, Any] = {}
    for model_key, group in sorted(by_key.items()):
        model, report = _train_group(
            model_key,
            group,
            holdout_sessions=holdout_sessions,
            min_train_sessions=min_train_sessions,
            thresholds=thresholds,
            promotion_gates=promotion_gates,
        )
        reports[model_key] = report
        if model is not None:
            models[model_key] = model

    generated = _utc(generated_at)
    artifact = {
        "schema_version": SCHEMA_VERSION,
        "feature_version": FEATURE_VERSION,
        "artifact_version": f"entry_edge.v1:{generated.strftime('%Y%m%dT%H%M%SZ')}",
        "generated_at": generated.isoformat(),
        "valid_days": 14,
        "feature_names": list(FEATURE_NAMES),
        "management_policy_version": ENTRY_EDGE_POLICY.policy_version,
        "models": models,
    }
    report = {
        "schema_version": "strategy_edge_training_report.v1",
        "generated_at": generated.isoformat(),
        "rows": len(rows),
        "sessions": sorted({str(row.get("session_date") or "") for row in rows}),
        "models": reports,
        "promoted_models": sorted(
            key for key, value in models.items() if value.get("promoted") is True
        ),
    }
    return artifact, report


def _train_group(
    model_key: str,
    rows: Sequence[Mapping[str, Any]],
    *,
    holdout_sessions: int,
    min_train_sessions: int,
    thresholds: Mapping[str, float],
    promotion_gates: Mapping[str, float],
) -> tuple[dict[str, Any] | None, dict[str, Any]]:
    ordered = sorted(
        (dict(row) for row in rows),
        key=lambda item: (str(item["session_date"]), str(item["decision_at"])),
    )
    sessions = sorted({str(row["session_date"]) for row in ordered})
    # Need expanding-window train + one OOF session + one holdout session.
    if len(sessions) < min_train_sessions + 2:
        return None, {
            "model_key": model_key,
            "promoted": False,
            "reason": "insufficient_sessions",
            "sessions": len(sessions),
            "rows": len(ordered),
        }
    holdout_count = min(max(holdout_sessions, 1), max(1, len(sessions) // 3))
    development_sessions = sessions[:-holdout_count]
    holdout_set = set(sessions[-holdout_count:])
    if len(development_sessions) <= min_train_sessions:
        return None, {
            "model_key": model_key,
            "promoted": False,
            "reason": "insufficient_development_sessions",
            "sessions": len(sessions),
            "rows": len(ordered),
        }

    oof: list[dict[str, Any]] = []
    for index in range(min_train_sessions, len(development_sessions)):
        train_set = set(development_sessions[:index])
        validation_session = development_sessions[index]
        train_rows = [row for row in ordered if row["session_date"] in train_set]
        validation_rows = [row for row in ordered if row["session_date"] == validation_session]
        if len(train_rows) < 10 or not validation_rows:
            continue
        fitted = _fit_models(train_rows)
        oof.extend(_predict_rows(fitted, validation_rows))

    if not oof:
        return None, {
            "model_key": model_key,
            "promoted": False,
            "reason": "insufficient_walk_forward_predictions",
            "sessions": len(sessions),
            "rows": len(ordered),
            "oof_rows": len(oof),
        }
    residual_q10_empirical = float(
        np.quantile(
            np.asarray(
                [row["policy_pnl_points"] - row["expected_pnl_points"] for row in oof],
                dtype=float,
            ),
            0.10,
        )
    )
    residual_q10, residual_clip_reason = _bounded_residual_q10(
        residual_q10_empirical,
        oof_rows=len(oof),
    )
    oof_metrics = _selection_metrics(
        oof,
        residual_q10=residual_q10,
        thresholds=thresholds,
    )

    development_rows = [row for row in ordered if row["session_date"] in set(development_sessions)]
    holdout_rows = [row for row in ordered if row["session_date"] in holdout_set]
    development_fit = _fit_models(development_rows)
    holdout_predictions = _predict_rows(development_fit, holdout_rows)
    holdout_metrics = _selection_metrics(
        holdout_predictions,
        residual_q10=residual_q10,
        thresholds=thresholds,
    )
    promotion = _promotion_decision(
        oof_metrics,
        holdout_metrics,
        promotion_gates=promotion_gates,
    )

    final_fit = _fit_models(ordered)
    model_version = f"entry_edge_{model_key.replace('|', '_')}_v1"
    model = {
        **final_fit,
        "model_version": model_version,
        "promoted": promotion["promoted"],
        "promotion": promotion,
        "thresholds": dict(thresholds),
        "residual_q10_points": round(residual_q10, 8),
        "residual_q10_empirical_points": round(residual_q10_empirical, 8),
        "residual_clip_reason": residual_clip_reason,
        "trained_from": sessions[0],
        "trained_through": sessions[-1],
        "training_rows": len(ordered),
        "training_sessions": len(sessions),
        "oof_metrics": oof_metrics,
        "holdout_metrics": holdout_metrics,
    }
    report = {
        "model_key": model_key,
        "model_version": model_version,
        "promoted": promotion["promoted"],
        "promotion": promotion,
        "rows": len(ordered),
        "sessions": sessions,
        "development_sessions": development_sessions,
        "holdout_sessions": sorted(holdout_set),
        "residual_q10_points": round(residual_q10, 8),
        "residual_q10_empirical_points": round(residual_q10_empirical, 8),
        "residual_clip_reason": residual_clip_reason,
        "oof_metrics": oof_metrics,
        "oof_unfiltered": _unfiltered_pnl_metrics(oof),
        "holdout_metrics": holdout_metrics,
        "holdout_unfiltered": _unfiltered_pnl_metrics(holdout_predictions),
    }
    return model, report


def _fit_models(rows: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    from sklearn.linear_model import LogisticRegression, Ridge
    from sklearn.preprocessing import StandardScaler

    x = np.asarray(
        [feature_vector(_map(row.get("features"))) for row in rows],
        dtype=float,
    )
    pnl = np.asarray([float(row["policy_pnl_points"]) for row in rows], dtype=float)
    profit = np.asarray([int(row["profit"]) for row in rows], dtype=int)
    stop = np.asarray([int(row["stop_first_5m"]) for row in rows], dtype=int)
    scaler = StandardScaler().fit(x)
    z = scaler.transform(x)
    ridge = Ridge(alpha=2.0).fit(z, pnl)
    profit_model = _fit_logistic(LogisticRegression, z, profit)
    stop_model = _fit_logistic(LogisticRegression, z, stop)
    max_z = np.max(np.abs(z), axis=1) if len(z) else np.asarray([0.0])
    return {
        "feature_mean": [round(float(value), 12) for value in scaler.mean_],
        "feature_scale": [round(float(value), 12) for value in scaler.scale_],
        "pnl": {
            "intercept": round(float(ridge.intercept_), 12),
            "coef": [round(float(value), 12) for value in ridge.coef_],
        },
        "profit": profit_model,
        "stop_first_5m": stop_model,
        "max_abs_z": round(
            max(3.0, float(np.quantile(max_z, 0.99)) + 0.25),
            8,
        ),
    }


def _fit_logistic(factory: Any, x: np.ndarray, labels: np.ndarray) -> dict[str, Any]:
    unique = np.unique(labels)
    if len(unique) < 2:
        # Laplace-smoothed constant probability remains valid and explicit.
        probability = (float(np.sum(labels)) + 1.0) / (float(len(labels)) + 2.0)
        intercept = math.log(probability / (1.0 - probability))
        return {
            "intercept": round(intercept, 12),
            "coef": [0.0] * x.shape[1],
            "constant": True,
        }
    model = factory(
        C=0.5,
        class_weight="balanced",
        max_iter=2_000,
        solver="liblinear",
        random_state=0,
    ).fit(x, labels)
    return {
        "intercept": round(float(model.intercept_[0]), 12),
        "coef": [round(float(value), 12) for value in model.coef_[0]],
        "constant": False,
    }


def _predict_rows(
    fitted: Mapping[str, Any], rows: Sequence[Mapping[str, Any]]
) -> list[dict[str, Any]]:
    if not rows:
        return []
    mean = np.asarray(fitted["feature_mean"], dtype=float)
    scale = np.asarray(fitted["feature_scale"], dtype=float)
    x = np.asarray(
        [feature_vector(_map(row.get("features"))) for row in rows],
        dtype=float,
    )
    z = (x - mean) / scale
    expected = _linear_predictions(z, _map(fitted.get("pnl")))
    p_profit = _sigmoid_array(_linear_predictions(z, _map(fitted.get("profit"))))
    p_stop = _sigmoid_array(_linear_predictions(z, _map(fitted.get("stop_first_5m"))))
    result = []
    for row, pnl, profit, stop in zip(
        rows,
        expected,
        p_profit,
        p_stop,
        strict=True,
    ):
        result.append(
            {
                **dict(row),
                "expected_pnl_points": float(pnl),
                "p_profit": float(profit),
                "p_stop_first_5m": float(stop),
            }
        )
    return result


def _linear_predictions(x: np.ndarray, model: Mapping[str, Any]) -> np.ndarray:
    coefficients = np.asarray(model.get("coef"), dtype=float)
    intercept = float(model.get("intercept"))
    return x @ coefficients + intercept


def _sigmoid_array(values: np.ndarray) -> np.ndarray:
    clipped = np.clip(values, -50.0, 50.0)
    return 1.0 / (1.0 + np.exp(-clipped))


def _bounded_residual_q10(
    empirical: float,
    *,
    oof_rows: int,
) -> tuple[float, str | None]:
    if oof_rows < SMALL_OOF_RESIDUAL_ROWS and empirical < SMALL_OOF_RESIDUAL_FLOOR_POINTS:
        return SMALL_OOF_RESIDUAL_FLOOR_POINTS, "small_oof_residual_floor"
    return empirical, None


def _unfiltered_pnl_metrics(rows: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    pnl = [float(row["policy_pnl_points"]) for row in rows]
    return {
        "rows": len(pnl),
        "net_pnl_points": round(sum(pnl), 8) if pnl else 0.0,
        "average_pnl_points": round(sum(pnl) / len(pnl), 8) if pnl else 0.0,
        "hit_rate": round(sum(value > 0 for value in pnl) / len(pnl), 8) if pnl else 0.0,
    }


def _selection_metrics(
    rows: Sequence[Mapping[str, Any]],
    *,
    residual_q10: float,
    thresholds: Mapping[str, float],
) -> dict[str, Any]:
    selected: list[dict[str, Any]] = []
    gate_failures: dict[str, int] = {
        "expected_pnl": 0,
        "lcb": 0,
        "p_profit": 0,
        "stop_first_5m": 0,
        "return_on_risk": 0,
    }
    for raw in rows:
        row = dict(raw)
        expected = float(row["expected_pnl_points"])
        lower = expected + residual_q10
        max_loss = max(float(row.get("max_loss_points") or 0.0), 1e-9)
        return_on_risk = lower / max_loss
        if expected < float(thresholds["min_expected_pnl_points"]):
            gate_failures["expected_pnl"] += 1
            continue
        if lower < float(thresholds["min_expected_pnl_lcb_points"]):
            gate_failures["lcb"] += 1
            continue
        if float(row["p_profit"]) < float(thresholds["min_p_profit"]):
            gate_failures["p_profit"] += 1
            continue
        if float(row["p_stop_first_5m"]) > float(thresholds["max_p_stop_first_5m"]):
            gate_failures["stop_first_5m"] += 1
            continue
        if return_on_risk < float(thresholds["min_return_on_risk"]):
            gate_failures["return_on_risk"] += 1
            continue
        row["expected_pnl_lcb_points"] = lower
        row["return_on_risk"] = return_on_risk
        selected.append(row)
    selected.sort(key=lambda item: (str(item["session_date"]), str(item["decision_at"])))
    pnl = [float(row["policy_pnl_points"]) for row in selected]
    gains = sum(value for value in pnl if value > 0)
    losses = -sum(value for value in pnl if value < 0)
    profit_factor = gains / losses if losses > 0 else (float("inf") if gains > 0 else 0.0)
    by_session: dict[str, float] = defaultdict(float)
    equity_r = 0.0
    peak_r = 0.0
    max_drawdown_r = 0.0
    for row in selected:
        session = str(row["session_date"])
        value = float(row["policy_pnl_points"])
        by_session[session] += value
        equity_r += value / max(float(row.get("max_loss_points") or 0.0), 1e-9)
        peak_r = max(peak_r, equity_r)
        max_drawdown_r = max(max_drawdown_r, peak_r - equity_r)
    positive_sessions = [value for value in by_session.values() if value > 0]
    positive_session_ratio = len(positive_sessions) / len(by_session) if by_session else 0.0
    total_positive_session_pnl = sum(positive_sessions)
    concentration = (
        max(positive_sessions) / total_positive_session_pnl
        if positive_sessions and total_positive_session_pnl > 0
        else 1.0
    )
    return {
        "candidate_rows": len(rows),
        "trades": len(selected),
        "sessions_traded": len(by_session),
        "net_pnl_points": round(sum(pnl), 8),
        "average_pnl_points": (round(sum(pnl) / len(pnl), 8) if pnl else 0.0),
        "hit_rate": (round(sum(value > 0 for value in pnl) / len(pnl), 8) if pnl else 0.0),
        "profit_factor": ("inf" if math.isinf(profit_factor) else round(profit_factor, 8)),
        "positive_session_ratio": round(positive_session_ratio, 8),
        "max_drawdown_r": round(max_drawdown_r, 8),
        "top_session_profit_concentration": round(concentration, 8),
        "gate_failures": gate_failures,
    }


def _promotion_decision(
    oof: Mapping[str, Any],
    holdout: Mapping[str, Any],
    *,
    promotion_gates: Mapping[str, float],
) -> dict[str, Any]:
    checks = {
        "oof_trade_count": int(oof.get("trades") or 0) >= int(promotion_gates["min_oof_trades"]),
        "holdout_trade_count": int(holdout.get("trades") or 0)
        >= int(promotion_gates["min_holdout_trades"]),
        "oof_positive_pnl": float(oof.get("net_pnl_points") or 0.0) > 0.0,
        "holdout_positive_pnl": float(holdout.get("net_pnl_points") or 0.0) > 0.0,
        "oof_profit_factor": _metric_float(oof.get("profit_factor"))
        >= float(promotion_gates["min_profit_factor"]),
        "holdout_profit_factor": _metric_float(holdout.get("profit_factor"))
        >= float(promotion_gates["min_profit_factor"]),
        "oof_average_pnl": float(oof.get("average_pnl_points") or 0.0)
        >= float(promotion_gates["min_average_pnl_points"]),
        "holdout_average_pnl": float(holdout.get("average_pnl_points") or 0.0)
        >= float(promotion_gates["min_average_pnl_points"]),
        "oof_positive_sessions": float(oof.get("positive_session_ratio") or 0.0)
        >= float(promotion_gates["min_positive_session_ratio"]),
        "holdout_positive_sessions": float(holdout.get("positive_session_ratio") or 0.0)
        >= float(promotion_gates["min_positive_session_ratio"]),
        "oof_drawdown": _metric_value(oof, "max_drawdown_r", float("inf"))
        <= float(promotion_gates["max_drawdown_r"]),
        "holdout_drawdown": _metric_value(
            holdout,
            "max_drawdown_r",
            float("inf"),
        )
        <= float(promotion_gates["max_drawdown_r"]),
        "oof_concentration": _metric_value(
            oof,
            "top_session_profit_concentration",
            1.0,
        )
        <= float(promotion_gates["max_top_session_profit_concentration"]),
        "holdout_concentration": _metric_value(
            holdout,
            "top_session_profit_concentration",
            1.0,
        )
        <= float(promotion_gates["max_top_session_profit_concentration"]),
    }
    return {
        "promoted": all(checks.values()),
        "checks": checks,
        "gates": dict(promotion_gates),
        "failed": [name for name, passed in checks.items() if not passed],
    }


def _read_candidate_decisions(
    database_path: Path,
    *,
    start_date: str | None,
    end_date: str | None,
    include_all_statuses: bool = False,
) -> list[dict[str, Any]]:
    connection = sqlite3.connect(database_path)
    try:
        rows = connection.execute(
            """
            SELECT decision_id, event_key, session_date, decision_at, status, attributes_json
            FROM decisions
            WHERE strategy_name = 'strategy_signal_engine_v2'
              AND (? OR status IN ('selected', 'no_trade', 'shadow_candidate'))
              AND (? IS NULL OR session_date >= ?)
              AND (? IS NULL OR session_date <= ?)
            ORDER BY decision_at, decision_id
            """,
            (
                include_all_statuses,
                start_date,
                start_date,
                end_date,
                end_date,
            ),
        ).fetchall()
    finally:
        connection.close()
    result = []
    for decision_id, event_key, session_date, decision_at, status, attributes_json in rows:
        try:
            payload = json.loads(attributes_json)
        except (TypeError, json.JSONDecodeError):
            result.append(
                {
                    "decision_id": decision_id,
                    "event_key": event_key,
                    "session_date": session_date,
                    "decision_at": decision_at,
                    "status": status,
                    "_malformed_payload": True,
                }
            )
            continue
        result.append(
            {
                **dict(_map(payload)),
                "decision_id": decision_id,
                "event_key": event_key,
                "session_date": session_date,
                "decision_at": decision_at,
                "status": status,
            }
        )
    return result


def write_artifact(value: Mapping[str, Any], path: Path) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(
            value,
            ensure_ascii=False,
            indent=2,
            sort_keys=True,
            allow_nan=False,
        )
        + "\n",
        encoding="utf-8",
    )
    return path


def _candidate(decision: Mapping[str, Any]) -> Mapping[str, Any]:
    candidate = _map(decision.get("candidate"))
    if candidate:
        return candidate
    return _map(_map(decision.get("why_not")).get("nearest_candidate"))


def _valid_training_row(row: Mapping[str, Any]) -> bool:
    features = _map(row.get("features"))
    return (
        str(row.get("model_key") or "") != ""
        and all(name in features for name in FEATURE_NAMES)
        and _number(row.get("policy_pnl_points")) is not None
        and _number(row.get("max_loss_points")) is not None
        and float(row.get("max_loss_points") or 0.0) > 0
    )


def _metric_float(value: object) -> float:
    if value == "inf":
        return float("inf")
    return float(value or 0.0)


def _metric_value(metrics: Mapping[str, Any], key: str, default: float) -> float:
    value = metrics.get(key)
    return default if value is None else float(value)


def _map(value: object) -> Mapping[str, Any]:
    return value if isinstance(value, Mapping) else {}


def _number(value: object) -> float | None:
    if not isinstance(value, int | float) or isinstance(value, bool):
        return None
    result = float(value)
    return result if math.isfinite(result) else None


def _time(value: object) -> datetime | None:
    if not isinstance(value, str | datetime):
        return None
    try:
        parsed = (
            value
            if isinstance(value, datetime)
            else datetime.fromisoformat(value.replace("Z", "+00:00"))
        )
    except ValueError:
        return None
    if parsed.tzinfo is None:
        return None
    return parsed.astimezone(timezone.utc)


def _date(value: object) -> date | None:
    try:
        return date.fromisoformat(str(value))
    except ValueError:
        return None


def _utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        raise ValueError("training timestamp must be timezone-aware")
    return value.astimezone(timezone.utc)


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--database", type=Path, required=True)
    parser.add_argument("--data-root", type=Path, required=True)
    parser.add_argument("--start-date", type=str, default=None)
    parser.add_argument("--end-date", type=str, default=None)
    parser.add_argument("--holdout-sessions", type=int, default=8)
    parser.add_argument(
        "--min-train-sessions",
        type=int,
        default=3,
        help="Minimum expanding-window train sessions before the first OOF fold. "
        "Promotion gates are independent and still require coverage/PnL checks.",
    )
    parser.add_argument(
        "--lookforward-minutes",
        type=int,
        default=None,
        help="Optional quote-window cap in minutes. Omitted by default so labels "
        "follow the production 15:45 ET hard close instead of a 20-minute flatten.",
    )
    parser.add_argument(
        "--enumerate-from-lake",
        action="store_true",
        help="Reconstruct debit candidates from batched causal quote-lake snapshots.",
    )
    parser.add_argument("--artifact", type=Path, default=None)
    parser.add_argument("--report", type=Path, default=None)
    args = parser.parse_args(argv)

    if args.enumerate_from_lake:
        output_root = Path("/tmp/strategy-edge-backtest")
        if args.artifact is None or args.report is None:
            parser.error(
                "--enumerate-from-lake requires explicit --artifact and --report paths "
                "under /tmp/strategy-edge-backtest"
            )
        if not all(
            path.resolve().is_relative_to(output_root) for path in (args.artifact, args.report)
        ):
            parser.error(
                "lake-enumerated training outputs must stay under /tmp/strategy-edge-backtest"
            )

    funnel: dict[str, Any] = {}
    rows = load_candidate_labels(
        database_path=args.database,
        data_root=args.data_root,
        start_date=args.start_date,
        end_date=args.end_date,
        lookforward_minutes=args.lookforward_minutes,
        enumerate_from_lake=args.enumerate_from_lake,
        funnel=funnel,
    )
    artifact, report = train_edge_artifact(
        rows,
        generated_at=datetime.now(tz=timezone.utc),
        holdout_sessions=args.holdout_sessions,
        min_train_sessions=args.min_train_sessions,
    )
    report["candidate_funnel"] = funnel
    artifact_path = args.artifact or args.data_root.joinpath(*ARTIFACT_RELATIVE_PATH)
    report_path = args.report or args.data_root / "research" / "strategy_edge_model.v1.report.json"
    write_artifact(artifact, artifact_path)
    write_artifact(report, report_path)
    print(
        json.dumps(
            {
                "labeled_rows": len(rows),
                "artifact": str(artifact_path),
                "report": str(report_path),
                "promoted_models": report["promoted_models"],
            },
            ensure_ascii=False,
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
