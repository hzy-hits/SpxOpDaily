"""Exact execution evidence counters for strategy readiness reviews.

The readiness scorecard treats detector/session health separately from trade
opportunities.  This module owns the stricter side of that boundary: duplicate
opportunity detection and exact quote/spread joins used by the frozen cohorts.
"""

from __future__ import annotations

import math
from collections import Counter, defaultdict
from collections.abc import Mapping, Sequence
from datetime import date, datetime, time, timezone
from typing import Any, Protocol

from spx_spark.market_calendar import ET


_PUT_SHADOW_EXACT_QUOTE_POLICIES = {
    "put_shadow_exact_quote.v1": 15.0,
}
_PUT_SHADOW_ENTRY_WINDOW_POLICIES = {
    "rth_lanes_0945_1300_put_shadow.v1": (time(9, 45), time(13, 0)),
}
_PUT_SHADOW_LANE_POLICIES = {
    "long_0dte_rth_flip_low_breakdown_put_shadow": (
        "level_breakout_put",
        "breakout",
        frozenset({"flip_low"}),
    ),
    "long_0dte_rth_upper_rejection_put_shadow": (
        "level_fade_put",
        "fade",
        frozenset({"call_wall", "flip_high"}),
    ),
}
PUT_SHADOW_LANES = frozenset(_PUT_SHADOW_LANE_POLICIES)
TRADE_READY_DELIVERY_DIAGNOSTIC_STATUSES = frozenset({"ready_pending_delivery", "delivery_blocked"})


class ReadinessRecord(Protocol):
    """Structural record contract shared with ``strategy_readiness``."""

    source: str
    payload: Mapping[str, object]
    path: str
    line_number: int
    at: datetime | None
    session_date: date | None


def duplicate_audit(records: Sequence[ReadinessRecord]) -> dict[str, Any]:
    """Find repeated semantic opportunities without counting evaluations."""

    keyed: dict[str, list[ReadinessRecord]] = defaultdict(list)
    for record in records:
        key = _semantic_record_key(record)
        if key is not None:
            keyed[key].append(record)
    duplicate_records = 0
    duplicate_keys: list[str] = []
    sessions: set[str] = set()
    for key, rows in sorted(keyed.items()):
        if len(rows) <= 1:
            continue
        duplicate_records += len(rows) - 1
        duplicate_keys.append(key)
        sessions.update(
            row.session_date.isoformat() for row in rows if row.session_date is not None
        )
    return {
        "duplicate_records": duplicate_records,
        "keys": duplicate_keys,
        "sessions": sessions,
    }


def is_trade_ready_delivery_diagnostic(payload: Mapping[str, object]) -> bool:
    """Identify an internal TradeReady signal that never gained execution authority."""

    return bool(
        payload.get("signal_status") == "trade_ready"
        and payload.get("status") in TRADE_READY_DELIVERY_DIAGNOSTIC_STATUSES
    )


def trade_ready_delivery_diagnostic_summary(
    records: Sequence[ReadinessRecord],
    compliant_records: Sequence[ReadinessRecord],
) -> dict[str, object]:
    """Summarize non-executable internal signals retained for diagnostics."""

    statuses = Counter(
        str(record.payload.get("status") or "")
        for record in records
        if is_trade_ready_delivery_diagnostic(record.payload)
    )
    compliant_statuses = Counter(
        str(record.payload.get("status") or "")
        for record in compliant_records
        if is_trade_ready_delivery_diagnostic(record.payload)
    )
    return {
        "total": sum(statuses.values()),
        "compliant": sum(compliant_statuses.values()),
        "by_status": dict(sorted(statuses.items())),
        "compliant_by_status": dict(sorted(compliant_statuses.items())),
        "executable_samples": 0,
        "rule": (
            "signal_status=trade_ready delivery projections remain diagnostic evidence; "
            "only status=trade_ready can enter an executable cohort"
        ),
    }


def count_legacy_gth_exact_entries(
    records: Sequence[ReadinessRecord], *, eligible_sessions: set[str]
) -> dict[str, Any]:
    """Count deprecated dip-reclaim entries for legacy research reporting only."""

    signals = [record for record in records if record.source == "gth_dip_reclaim"]
    opens = [
        record
        for record in records
        if record.source == "virtual_strategy" and record.payload.get("event") == "virtual_opened"
    ]
    decisions = [
        record
        for record in records
        if record.source == "virtual_strategy"
        and record.payload.get("event") == "virtual_entry_decision"
        and _gth_virtual_decision_ready(record.payload)
        and record.payload.get("terminal") is True
    ]
    opens_by_source: dict[str, ReadinessRecord] = {}
    for record in sorted(
        opens, key=lambda item: item.at or datetime.min.replace(tzinfo=timezone.utc)
    ):
        source_id = record.payload.get("source_signal_id")
        if _nonempty_string(source_id):
            opens_by_source.setdefault(str(source_id), record)
    decisions_by_source: dict[str, ReadinessRecord] = {}
    for record in sorted(decisions, key=_record_sort_key):
        source_id = record.payload.get("source_signal_id")
        if _nonempty_string(source_id):
            decisions_by_source.setdefault(str(source_id), record)

    successes: set[str] = set()
    episodes: set[str] = set()
    eligible = 0
    exact_structures = 0
    excluded_incomplete = 0
    for signal in signals:
        session_id = signal.session_date.isoformat() if signal.session_date else ""
        if session_id not in eligible_sessions:
            excluded_incomplete += 1
            continue
        eligible += 1
        if not _exact_gth_structure(signal.payload):
            continue
        exact_structures += 1
        event_id = signal.payload.get("event_id")
        decision = decisions_by_source.get(str(event_id))
        opened = opens_by_source.get(str(event_id))
        if (
            decision is None
            or not _exact_spread_decision(decision.payload)
            or opened is None
            or not _exact_spread_open(signal.payload, opened.payload)
            or not _same_spread_snapshot(
                decision.payload.get("exact_spread_snapshot"),
                _entry_snapshot(opened.payload),
            )
            or (
                _nonempty_string(decision.payload.get("episode_id"))
                and decision.payload.get("episode_id") != opened.payload.get("episode_id")
            )
        ):
            continue
        successes.add(str(event_id))
        episode_id = opened.payload.get("episode_id")
        if _nonempty_string(episode_id):
            episodes.add(str(episode_id))
    return {
        "count": len(successes),
        "episode_ids": sorted(episodes),
        "eligible_signals": eligible,
        "signals_with_exact_structure": exact_structures,
        "unmatched_or_inexact_signals": eligible - len(successes),
        "excluded_incomplete_session": excluded_incomplete,
    }


def count_put_exact_entries(
    records: Sequence[ReadinessRecord], *, eligible_sessions: set[str]
) -> dict[str, int]:
    """Count exact Put quote entries from production and independent shadow lanes."""

    intent_groups: dict[str, list[ReadinessRecord]] = {}
    for record in sorted(records, key=_record_sort_key):
        if (
            record.source == "trade_intents"
            and record.payload.get("status") in {"trade_ready", "shadow_ready"}
            and record.payload.get("direction") == "down"
            and _nonempty_string(record.payload.get("intent_id"))
        ):
            identity = _put_intent_evidence_id(record.payload)
            intent_groups.setdefault(identity, []).append(record)
    opens = [
        record
        for record in records
        if record.source == "virtual_strategy" and record.payload.get("event") == "virtual_opened"
    ]
    shadow_entries = [
        record
        for record in records
        if record.source == "trade_candidates"
        and record.payload.get("event") == "candidate_terminal"
        and record.payload.get("phase") == "quote_reached_entry"
        and record.payload.get("shadow_mode") is True
    ]
    successes: set[str] = set()
    eligible = 0
    eligible_trade_ready = 0
    eligible_shadow_ready = 0
    exact_virtual_opens = 0
    exact_shadow_quote_entries = 0
    excluded_incomplete = 0
    for evidence_id, group in intent_groups.items():
        eligible_group = [
            intent
            for intent in group
            if (intent.session_date.isoformat() if intent.session_date else "") in eligible_sessions
        ]
        if not eligible_group:
            excluded_incomplete += 1
            continue
        eligible += 1
        trade_ready = [
            intent for intent in eligible_group if intent.payload.get("status") == "trade_ready"
        ]
        shadow_ready = [
            intent for intent in eligible_group if intent.payload.get("status") == "shadow_ready"
        ]
        if trade_ready:
            eligible_trade_ready += 1
            opened = next(
                (
                    row
                    for intent in trade_ready
                    for row in sorted(opens, key=_record_sort_key)
                    if _source_matches_intent(
                        row.payload,
                        str(intent.payload.get("intent_id") or ""),
                    )
                    and _exact_put_open(intent.payload, row.payload)
                ),
                None,
            )
            if opened is None:
                continue
            successes.add(evidence_id)
            exact_virtual_opens += 1
            continue
        if not shadow_ready:
            continue
        eligible_shadow_ready += 1
        shadow = next(
            (
                row
                for intent in shadow_ready
                for row in sorted(shadow_entries, key=_record_sort_key)
                if _shadow_source_matches_intent(row.payload, intent.payload)
                and _exact_put_shadow_entry(intent.payload, row.payload)
            ),
            None,
        )
        if shadow is not None:
            successes.add(evidence_id)
            exact_shadow_quote_entries += 1
    return {
        "count": len(successes),
        "eligible_trade_ready_puts": eligible_trade_ready,
        "eligible_shadow_ready_puts": eligible_shadow_ready,
        "exact_virtual_opens": exact_virtual_opens,
        "exact_shadow_quote_entries": exact_shadow_quote_entries,
        "unmatched_or_inexact_puts": eligible - len(successes),
        "excluded_incomplete_session": excluded_incomplete,
    }


def count_exact_spread_exits(
    records: Sequence[ReadinessRecord],
    *,
    eligible_sessions: set[str],
    successful_gth_episodes: set[str],
) -> dict[str, int]:
    """Count exact closes belonging to an already-qualified GTH entry."""

    closes = [
        record
        for record in records
        if record.source == "virtual_strategy"
        and record.payload.get("event") == "virtual_closed"
        and str(record.payload.get("episode_id") or "") in successful_gth_episodes
    ]
    successes: set[str] = set()
    excluded_incomplete = 0
    ineligible_exact = 0
    for record in closes:
        session_id = record.session_date.isoformat() if record.session_date else ""
        if session_id not in eligible_sessions:
            excluded_incomplete += 1
            continue
        episode_id = str(record.payload.get("episode_id") or "")
        if _exact_spread_close(record.payload):
            successes.add(episode_id)
        else:
            ineligible_exact += 1
    missing = max(len(successful_gth_episodes) - len(successes), 0)
    return {
        "count": len(successes),
        "unmatched_or_inexact_exits": max(missing, ineligible_exact),
        "excluded_incomplete_session": excluded_incomplete,
    }


def cohort_result(
    *,
    count: int,
    target: int,
    count_blocker: str,
    common_blockers: Sequence[str],
    details: Mapping[str, object],
) -> dict[str, object]:
    """Render one frozen cohort result with stable blocker ordering."""

    blockers = list(common_blockers)
    if count < target:
        blockers.append(count_blocker)
    blockers = _unique(blockers)
    return {
        "status": "ready" if not blockers else "collecting",
        "count": count,
        "target": target,
        "blockers": blockers,
        **details,
    }


def _semantic_record_key(record: ReadinessRecord) -> str | None:
    row = record.payload
    if record.source == "gth_dip_reclaim":
        event_id = row.get("event_id")
        return f"gth_signal:{event_id}" if _nonempty_string(event_id) else None
    if record.source == "trade_intents" and row.get("status") == "shadow_ready":
        # A shadow-ready row is a repeated detector evaluation, not a second
        # opportunity.  The consumed candidate lifecycle is audited below.
        return None
    if record.source == "trade_intents" and is_trade_ready_delivery_diagnostic(row):
        # Delivery projections are retained for diagnostic/contract evidence,
        # but they are not executable opportunities and cannot create a sample.
        return None
    if record.source == "trade_intents" and row.get("status") == "trade_ready":
        intent_id = row.get("intent_id")
        return f"trade_ready:{intent_id}" if _nonempty_string(intent_id) else None
    if record.source == "confirmed_gate_results":
        record_key = row.get("record_key") or row.get("event_id")
        return f"confirmed_gate:{record_key}" if _nonempty_string(record_key) else None
    if record.source == "trade_candidates":
        event = row.get("event")
        identity = _put_candidate_evidence_id(row)
        return f"trade_candidate:{event}:{identity}" if identity else None
    if record.source == "virtual_strategy" and row.get("event") == "virtual_entry_decision":
        decision_id = row.get("decision_id") or row.get("source_signal_id")
        return f"virtual_entry_decision:{decision_id}" if _nonempty_string(decision_id) else None
    if record.source == "virtual_strategy" and row.get("event") == "virtual_opened":
        source_id = row.get("source_signal_id") or row.get("episode_id")
        return f"virtual_opened:{source_id}" if _nonempty_string(source_id) else None
    if record.source == "virtual_strategy" and row.get("event") == "virtual_closed":
        episode_id = row.get("episode_id")
        return f"virtual_closed:{episode_id}" if _nonempty_string(episode_id) else None
    return None


def _exact_gth_structure(payload: Mapping[str, object]) -> bool:
    spread = payload.get("spread")
    session_id = str(payload.get("session_date") or "")
    if not isinstance(spread, Mapping) or spread.get("right") != "C":
        return False
    long_strike = _number(spread.get("long_strike"))
    short_strike = _number(spread.get("short_strike"))
    width = _number(spread.get("width_points"))
    return bool(
        long_strike is not None
        and short_strike is not None
        and width is not None
        and short_strike > long_strike > 0
        and math.isclose(short_strike - long_strike, width, abs_tol=1e-6)
        and spread.get("expiry_date") == session_id
    )


def _exact_spread_decision(payload: Mapping[str, object]) -> bool:
    snapshot = payload.get("exact_spread_snapshot")
    return bool(
        _gth_virtual_decision_ready(payload)
        and payload.get("terminal") is True
        and payload.get("position_type") == "call_debit_spread"
        and _nonempty_string(payload.get("source_signal_id"))
        and _nonempty_string(payload.get("episode_id"))
        and isinstance(snapshot, Mapping)
        and _exact_spread_snapshot(snapshot, at=_event_at(payload))
    )


def _gth_virtual_decision_ready(payload: Mapping[str, object]) -> bool:
    status = payload.get("status")
    if status == "trade_ready":
        # Backward compatibility for already persisted GTH virtual decisions.
        return True
    return bool(
        status == "virtual_ready"
        and payload.get("source_kind") == "gth_dip_reclaim_call"
        and payload.get("simulation_only") is True
        and payload.get("execution_eligible") is False
        and payload.get("automatic_ordering") is False
    )


def _exact_spread_open(signal: Mapping[str, object], opened: Mapping[str, object]) -> bool:
    if opened.get("position_type") != "call_debit_spread":
        return False
    spread = signal.get("spread")
    if not isinstance(spread, Mapping):
        return False
    long_contract = _parse_option_contract(opened.get("long_contract_id"))
    short_contract = _parse_option_contract(opened.get("short_contract_id"))
    if long_contract is None or short_contract is None:
        return False
    expiry = _parse_date(spread.get("expiry_date"))
    long_strike = _number(spread.get("long_strike"))
    short_strike = _number(spread.get("short_strike"))
    if (
        expiry is None
        or long_strike is None
        or short_strike is None
        or long_contract != (expiry, long_strike, "C")
        or short_contract != (expiry, short_strike, "C")
    ):
        return False
    snapshot = _entry_snapshot(opened)
    width = _number(spread.get("width_points"))
    ask = _number(snapshot.get("ask"))
    return bool(
        width is not None
        and ask is not None
        and 0 < ask < width
        and _exact_spread_snapshot(snapshot, at=_event_at(opened))
    )


def _exact_put_open(intent: Mapping[str, object], opened: Mapping[str, object]) -> bool:
    contract_id = str(intent.get("contract_id") or "")
    session_id = _parse_date(intent.get("session_id"))
    contract = _parse_option_contract(contract_id)
    if (
        contract is None
        or session_id is None
        or contract[0] != session_id
        or contract[2] != "P"
        or opened.get("contract_id") != contract_id
        or opened.get("position_type") != "single_option"
    ):
        return False
    snapshot = _entry_snapshot(opened)
    return _exact_quote_snapshot(snapshot, at=_event_at(opened), require_quality=True)


def _exact_put_shadow_entry(
    intent: Mapping[str, object],
    candidate: Mapping[str, object],
) -> bool:
    source = candidate.get("source_intent")
    observation = candidate.get("entry_observation")
    contract_id = str(intent.get("contract_id") or "")
    session_id = _parse_date(intent.get("session_id"))
    contract = _parse_option_contract(contract_id)
    entry_limit = _number(intent.get("entry_limit"))
    intent_at = _parse_time(intent.get("evaluated_at"))
    valid_until = _parse_time(intent.get("valid_until"))
    source_intent_at = (
        _parse_time(source.get("evaluated_at")) if isinstance(source, Mapping) else None
    )
    source_valid_until = (
        _parse_time(source.get("valid_until")) if isinstance(source, Mapping) else None
    )
    candidate_valid_until = _parse_time(candidate.get("valid_until"))
    source_entry_limit = _number(source.get("entry_limit")) if isinstance(source, Mapping) else None
    candidate_entry_limit = _number(candidate.get("entry_limit"))
    intent_lane = intent.get("strategy_lane")
    lane_policy = _PUT_SHADOW_LANE_POLICIES.get(str(intent_lane))
    window_contract_version = intent.get("trade_intent_contract_version")
    window_policy = (
        _PUT_SHADOW_ENTRY_WINDOW_POLICIES.get(str(window_contract_version))
        if _nonempty_string(window_contract_version)
        else None
    )
    entry_window_start_at = _parse_time(intent.get("entry_window_start_at"))
    hard_exit_at = _parse_time(intent.get("hard_exit_at"))
    source_entry_window_start_at = (
        _parse_time(source.get("entry_window_start_at")) if isinstance(source, Mapping) else None
    )
    source_hard_exit_at = (
        _parse_time(source.get("hard_exit_at")) if isinstance(source, Mapping) else None
    )
    candidate_entry_window_start_at = _parse_time(candidate.get("entry_window_start_at"))
    candidate_hard_exit_at = _parse_time(candidate.get("hard_exit_at"))
    quote_policy_version = (
        observation.get("exact_quote_policy_version") if isinstance(observation, Mapping) else None
    )
    quote_max_age = (
        _PUT_SHADOW_EXACT_QUOTE_POLICIES.get(str(quote_policy_version))
        if _nonempty_string(quote_policy_version)
        else None
    )
    declared_quote_max_age = (
        _number(observation.get("exact_quote_max_age_seconds"))
        if isinstance(observation, Mapping)
        else None
    )
    expected_candidate_id = (
        f"{source.get('intent_id')}|{source.get('event_id')}"
        if isinstance(source, Mapping)
        and _nonempty_string(source.get("intent_id"))
        and _nonempty_string(source.get("event_id"))
        else ""
    )
    observation_entry_limit = (
        _number(observation.get("entry_limit")) if isinstance(observation, Mapping) else None
    )
    if (
        contract is None
        or session_id is None
        or contract[0] != session_id
        or contract[2] != "P"
        or intent.get("status") != "shadow_ready"
        or intent.get("shadow_mode") is not True
        or intent.get("execution_eligible") is not False
        or intent.get("quote_observation_eligible") is not True
        or intent.get("automatic_ordering") is not False
        or intent_lane not in PUT_SHADOW_LANES
        or lane_policy is None
        or not _put_shadow_lane_contract_matches(intent, lane_policy)
        or intent_at is None
        or valid_until is None
        or window_policy is None
        or entry_window_start_at is None
        or hard_exit_at is None
        or not isinstance(source, Mapping)
        or not _same_put_intent_identity(source, intent)
        or source.get("execution_eligible") is not False
        or source.get("quote_observation_eligible") is not True
        or source.get("automatic_ordering") is not False
        or source.get("status") != "shadow_ready"
        or source.get("shadow_mode") is not True
        or source.get("direction") != "down"
        or source.get("session_id") != intent.get("session_id")
        or source.get("contract_id") != contract_id
        or source.get("strategy_lane") != intent_lane
        or not _put_shadow_lane_contract_matches(source, lane_policy)
        or source.get("policy_version") != intent.get("policy_version")
        or source.get("trade_intent_contract_version") != window_contract_version
        or source_intent_at != intent_at
        or source_valid_until != valid_until
        or source_entry_window_start_at != entry_window_start_at
        or source_hard_exit_at != hard_exit_at
        or source_entry_limit is None
        or entry_limit is None
        or not math.isclose(source_entry_limit, entry_limit, abs_tol=1e-9)
        or candidate.get("contract_id") != contract_id
        or candidate.get("strategy_lane") != intent_lane
        or not _put_shadow_lane_contract_matches(candidate, lane_policy)
        or candidate.get("event_id") != source.get("event_id")
        or candidate.get("intent_id") != source.get("intent_id")
        or candidate.get("semantic_key") != source.get("semantic_key")
        or candidate.get("candidate_id") != expected_candidate_id
        or candidate.get("direction") != source.get("direction")
        or not _nonempty_string(candidate.get("event_id"))
        or not _nonempty_string(candidate.get("intent_id"))
        or not _nonempty_string(candidate.get("semantic_key"))
        or candidate_entry_limit is None
        or not math.isclose(candidate_entry_limit, source_entry_limit, abs_tol=1e-9)
        or candidate_valid_until != source_valid_until
        or candidate.get("trade_intent_contract_version") != window_contract_version
        or candidate_entry_window_start_at != source_entry_window_start_at
        or candidate_hard_exit_at != source_hard_exit_at
        or candidate.get("event") != "candidate_terminal"
        or candidate.get("phase") != "quote_reached_entry"
        or candidate.get("shadow_mode") is not True
        or candidate.get("automatic_ordering") is not False
        or candidate.get("execution_claim") != "none"
        or candidate.get("broker_order_state") != "not_connected"
        or not isinstance(observation, Mapping)
        or observation.get("contract_id") != contract_id
        or observation_entry_limit is None
        or not math.isclose(observation_entry_limit, entry_limit, abs_tol=1e-9)
        or observation.get("entry_condition") != "displayed_ask_at_or_below_limit"
        or observation.get("quote_pricing_allowed") is not True
        or observation.get("exact_quote_freshness_ok") is not True
        or quote_max_age is None
        or declared_quote_max_age != quote_max_age
        or observation.get("quote_quality") != "live"
        or not _nonempty_string(observation.get("provider"))
        or _number(observation.get("ask")) is None
        or float(observation["ask"]) > entry_limit
    ):
        return False
    terminal_at = _parse_time(candidate.get("terminal_at"))
    observation_at = _parse_time(observation.get("at"))
    source_at = _parse_time(observation.get("quote_source_at"))
    transport_at = _parse_time(observation.get("quote_transport_at"))
    observed_source_age = _number(observation.get("quote_source_age_seconds"))
    observed_transport_age = _number(observation.get("quote_transport_age_seconds"))
    if (
        not _valid_nbbo(observation)
        or source_at is None
        or transport_at is None
        or terminal_at is None
        or observation_at != terminal_at
        or observed_source_age is None
        or observed_transport_age is None
    ):
        return False
    intent_local = intent_at.astimezone(ET)
    terminal_local = terminal_at.astimezone(ET)
    valid_until_local = valid_until.astimezone(ET)
    entry_window_start_et, hard_exit_et = window_policy
    expected_entry_window_start = datetime.combine(
        session_id,
        entry_window_start_et,
        tzinfo=ET,
    ).astimezone(timezone.utc)
    expected_hard_exit = datetime.combine(
        session_id,
        hard_exit_et,
        tzinfo=ET,
    ).astimezone(timezone.utc)
    if not (
        entry_window_start_at == expected_entry_window_start
        and hard_exit_at == expected_hard_exit
        and intent_local.date() == session_id
        and entry_window_start_et <= intent_local.time() < hard_exit_et
        and intent_at <= terminal_at < valid_until
        and terminal_local.date() == session_id
        and entry_window_start_et <= terminal_local.time() < hard_exit_et
        and valid_until_local.date() == session_id
        and valid_until <= hard_exit_at
    ):
        return False
    source_age = (terminal_at - source_at).total_seconds()
    transport_age = (terminal_at - transport_at).total_seconds()
    return bool(
        0.0 <= source_age <= quote_max_age
        and 0.0 <= transport_age <= quote_max_age
        and math.isclose(observed_source_age, source_age, abs_tol=1e-6)
        and math.isclose(observed_transport_age, transport_age, abs_tol=1e-6)
    )


def _put_shadow_lane_contract_matches(
    payload: Mapping[str, object],
    policy: tuple[str, str, frozenset[str]],
) -> bool:
    play, thesis, level_kinds = policy
    return bool(
        payload.get("play") == play
        and payload.get("thesis") == thesis
        and payload.get("level_kind") in level_kinds
    )


def _source_matches_intent(payload: Mapping[str, object], intent_id: str) -> bool:
    source_id = str(payload.get("source_signal_id") or "")
    return bool(source_id == intent_id or source_id.startswith(f"{intent_id}|"))


def _shadow_source_matches_intent(
    payload: Mapping[str, object],
    intent: Mapping[str, object],
) -> bool:
    source = payload.get("source_intent")
    return bool(isinstance(source, Mapping) and _same_put_intent_identity(source, intent))


def _same_put_intent_identity(
    left: Mapping[str, object],
    right: Mapping[str, object],
) -> bool:
    left_event_id = left.get("event_id")
    right_event_id = right.get("event_id")
    left_intent_id = left.get("intent_id")
    right_intent_id = right.get("intent_id")
    left_semantic_key = left.get("semantic_key")
    right_semantic_key = right.get("semantic_key")
    return bool(
        _nonempty_string(left_event_id)
        and _nonempty_string(right_event_id)
        and left_event_id == right_event_id
        and _nonempty_string(left_intent_id)
        and _nonempty_string(right_intent_id)
        and left_intent_id == right_intent_id
        and _nonempty_string(left_semantic_key)
        and _nonempty_string(right_semantic_key)
        and left_semantic_key == right_semantic_key
    )


def _put_intent_evidence_id(payload: Mapping[str, object]) -> str:
    event_id = payload.get("event_id")
    if _nonempty_string(event_id):
        return f"event_id:{event_id}"
    return f"intent_id:{payload.get('intent_id')}"


def _put_candidate_evidence_id(payload: Mapping[str, object]) -> str:
    source = payload.get("source_intent")
    source = source if isinstance(source, Mapping) else {}
    candidate_id = payload.get("candidate_id")
    if _nonempty_string(candidate_id):
        return f"candidate_id:{candidate_id}"
    event_id = payload.get("event_id") or source.get("event_id")
    intent_id = payload.get("intent_id") or source.get("intent_id")
    if _nonempty_string(event_id) and _nonempty_string(intent_id):
        return f"event_intent:{event_id}|{intent_id}"
    return ""


def _exact_spread_close(payload: Mapping[str, object]) -> bool:
    if payload.get("position_type") not in {"call_debit_spread", "put_debit_spread"}:
        return False
    opened_at = _parse_time(payload.get("opened_at"))
    closed_at = _parse_time(payload.get("closed_at"))
    if opened_at is None or closed_at is None or closed_at < opened_at:
        return False
    entry = _entry_snapshot(payload)
    exit_snapshot = payload.get("exit_snapshot")
    return bool(
        isinstance(exit_snapshot, Mapping)
        and _exact_spread_snapshot(entry, at=opened_at)
        and _exact_spread_snapshot(exit_snapshot, at=closed_at)
    )


def _entry_snapshot(payload: Mapping[str, object]) -> dict[str, object]:
    nested = payload.get("entry_snapshot")
    if not isinstance(nested, Mapping):
        nested = payload.get("last") if isinstance(payload.get("last"), Mapping) else {}
    result = dict(nested)
    for name in ("bid", "mid", "ask"):
        top_value = payload.get(f"entry_{name}")
        if _number(top_value) is not None:
            result[name] = top_value
    return result


def _exact_spread_snapshot(snapshot: Mapping[str, object], *, at: datetime | None) -> bool:
    long = snapshot.get("long")
    short = snapshot.get("short")
    quality = snapshot.get("quality")
    long_source_at = _parse_time(long.get("source_at")) if isinstance(long, Mapping) else None
    short_source_at = _parse_time(short.get("source_at")) if isinstance(short, Mapping) else None
    return bool(
        _valid_nbbo(snapshot)
        and isinstance(long, Mapping)
        and isinstance(short, Mapping)
        and _exact_quote_snapshot(long, at=at, require_quality=True)
        and _exact_quote_snapshot(short, at=at, require_quality=True)
        and long_source_at is not None
        and short_source_at is not None
        and abs((long_source_at - short_source_at).total_seconds()) <= 5.0
        and isinstance(quality, Mapping)
        and quality.get("status") == "ok"
    )


def _exact_quote_snapshot(
    snapshot: Mapping[str, object],
    *,
    at: datetime | None,
    require_quality: bool,
) -> bool:
    if not _valid_nbbo(snapshot):
        return False
    source_at = _parse_time(snapshot.get("source_at"))
    if source_at is None or at is None:
        return False
    quote_age = (at - source_at).total_seconds()
    if quote_age < -1.0 or quote_age > 5.0:
        return False
    if require_quality:
        quality = snapshot.get("quality")
        if not isinstance(quality, Mapping) or quality.get("status") != "ok":
            return False
    return True


def _valid_nbbo(snapshot: Mapping[str, object]) -> bool:
    bid = _number(snapshot.get("bid"))
    mid = _number(snapshot.get("mid"))
    ask = _number(snapshot.get("ask"))
    return bool(
        bid is not None
        and mid is not None
        and ask is not None
        and 0 <= bid
        and mid > 0
        and ask > 0
        and bid <= mid <= ask
    )


def _same_spread_snapshot(left: object, right: object) -> bool:
    if not isinstance(left, Mapping) or not isinstance(right, Mapping):
        return False
    for leg in ("long", "short"):
        left_leg = left.get(leg)
        right_leg = right.get(leg)
        if not isinstance(left_leg, Mapping) or not isinstance(right_leg, Mapping):
            return False
        for field in ("bid", "mid", "ask", "source_at"):
            if left_leg.get(field) != right_leg.get(field):
                return False
    return True


def _parse_option_contract(value: object) -> tuple[date, float, str] | None:
    if not isinstance(value, str):
        return None
    parts = value.split(":")
    if len(parts) != 6 or parts[:3] != ["option", "SPX", "SPXW"]:
        return None
    try:
        expiry = datetime.strptime(parts[3], "%Y%m%d").date()
        strike = float(parts[4])
    except ValueError:
        return None
    if strike <= 0 or parts[5] not in {"C", "P"}:
        return None
    return expiry, strike, parts[5]


def _record_sort_key(record: ReadinessRecord) -> tuple[datetime, str, int]:
    return (
        record.at or datetime.min.replace(tzinfo=timezone.utc),
        record.path,
        record.line_number,
    )


def _event_at(payload: Mapping[str, object]) -> datetime | None:
    event = payload.get("event")
    fields = {
        "virtual_closed": ("closed_at",),
        "virtual_opened": ("opened_at",),
        "virtual_horizon_outcome": ("observed_at",),
    }.get(str(event), ())
    for field in (
        *fields,
        "evaluated_at",
        "terminal_at",
        "armed_at",
        "confirmed_at",
        "closed_at",
        "opened_at",
        "observed_at",
        "at",
        "updated_at",
    ):
        parsed = _parse_time(payload.get(field))
        if parsed is not None:
            return parsed
    return None


def _parse_time(value: object) -> datetime | None:
    if not isinstance(value, str):
        return None
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        return None
    return parsed.astimezone(timezone.utc)


def _parse_date(value: object) -> date | None:
    if not isinstance(value, str):
        return None
    try:
        return date.fromisoformat(value)
    except ValueError:
        return None


def _number(value: object) -> float | None:
    if isinstance(value, bool) or not isinstance(value, int | float):
        return None
    parsed = float(value)
    return parsed if math.isfinite(parsed) else None


def _nonempty_string(value: object) -> bool:
    return isinstance(value, str) and bool(value.strip())


def _unique(values: Sequence[str]) -> list[str]:
    return list(dict.fromkeys(values))
