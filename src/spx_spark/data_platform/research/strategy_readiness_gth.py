"""Runtime/legacy GTH lane separation for strategy readiness.

The current operator lane emits manual level candidates.  The earlier dip
reclaim detector remains useful research telemetry, but it is not a substitute
for runtime-candidate or replay evidence.
"""

from __future__ import annotations

import math
from collections import Counter
from datetime import date, datetime, timezone
from typing import Iterable, Mapping, Protocol, Sequence

from spx_spark.strategy_contract import strategy_contract_issues

from .strategy_readiness_evidence import (
    _entry_snapshot,
    _event_at,
    _exact_spread_close,
    _exact_spread_snapshot,
    _nonempty_string,
    _number,
    _parse_option_contract,
    _parse_time,
    _record_sort_key,
    _same_spread_snapshot,
)


RUNTIME_GTH_SOURCE = "gth_level_manual_candidates"
RUNTIME_GTH_STRATEGY_ID = "gth_level_manual_candidate"
RUNTIME_GTH_LIFECYCLE_STATUS = "legacy_production"
RUNTIME_GTH_RUNTIME_STATUS = "production_runtime"
LEGACY_GTH_SOURCE = "gth_dip_reclaim"
LEGACY_GTH_STRATEGY_ID = "gth_dip_reclaim"
LEGACY_GTH_LIFECYCLE_STATUS = "legacy_research"
RUNTIME_GTH_SOURCE_KIND = "gth_spxw_level_manual_spread_candidate"


class GthReadinessRecord(Protocol):
    source: str
    payload: Mapping[str, object]
    at: datetime | None
    session_date: date | None


def summarize_gth_strategy_lanes(
    records: Sequence[GthReadinessRecord],
    *,
    eligible_sessions: set[str],
    exact_entry_target: int,
    exact_evidence: Mapping[str, object] | None = None,
) -> dict[str, object]:
    """Separate current runtime observations from deprecated research signals."""

    runtime = [record for record in records if record.source == RUNTIME_GTH_SOURCE]
    legacy = [record for record in records if record.source == LEGACY_GTH_SOURCE]
    status_counts = Counter(str(record.payload.get("status") or "unknown") for record in runtime)
    issue_counts: Counter[str] = Counter()
    compliant_material = []
    for record in runtime:
        payload = record.payload
        if not _material_runtime_candidate(payload):
            continue
        issues = list(
            strategy_contract_issues(
                payload,
                require_valid_until=True,
                require_actionable_coordinate=True,
            )
        )
        issues.extend(gth_runtime_identity_issues(payload))
        if issues:
            issue_counts.update(dict.fromkeys(issues, 1))
        else:
            compliant_material.append(record)

    manual_ready = _unique_candidates(
        record for record in compliant_material if record.payload.get("status") == "manual_ready"
    )
    eligible_ready = {
        key: record
        for key, record in manual_ready.items()
        if record.session_date is not None and record.session_date.isoformat() in eligible_sessions
    }
    policies = sorted(
        {
            str(record.payload["policy_version"])
            for record in compliant_material
            if str(record.payload.get("policy_version") or "").strip()
        }
    )
    blockers: list[str] = []
    if not runtime:
        blockers.append("gth_runtime_lane_events_unavailable")
    if issue_counts:
        blockers.append("gth_runtime_lane_contract_anomalies_present")
    if len(eligible_ready) < exact_entry_target:
        blockers.append("gth_runtime_manual_ready_candidates_below_20")
    evidence = dict(exact_evidence or {})
    replay_migrated = evidence.get("replay_migrated") is True
    if not replay_migrated:
        blockers.append("gth_runtime_exact_entry_replay_not_migrated")

    legacy_ids = {
        str(record.payload.get("event_id"))
        for record in legacy
        if str(record.payload.get("event_id") or "").strip()
    }
    declared_legacy = sum(
        record.payload.get("strategy_id") == LEGACY_GTH_STRATEGY_ID
        and record.payload.get("lifecycle_status") == LEGACY_GTH_LIFECYCLE_STATUS
        for record in legacy
    )
    return {
        "runtime": {
            "strategy_id": RUNTIME_GTH_STRATEGY_ID,
            "source": RUNTIME_GTH_SOURCE,
            "lifecycle_status": RUNTIME_GTH_LIFECYCLE_STATUS,
            "runtime_status": RUNTIME_GTH_RUNTIME_STATUS,
            "records": len(runtime),
            "material_records": sum(_material_runtime_candidate(row.payload) for row in runtime),
            "contract_compliant_records": len(compliant_material),
            "contract_issues": dict(sorted(issue_counts.items())),
            "status_counts": dict(sorted(status_counts.items())),
            "policy_versions": policies,
            "manual_ready_candidates": len(manual_ready),
            "eligible_manual_ready_candidates": len(eligible_ready),
            "exact_entry_count": int(evidence.get("exact_entry_count") or 0),
            "exact_exit_count": int(evidence.get("exact_exit_count") or 0),
            "eligible_exact_candidates": int(evidence.get("eligible_candidates") or 0),
            "unmatched_or_inexact_entries": int(evidence.get("unmatched_or_inexact_entries") or 0),
            "unmatched_or_inexact_exits": int(evidence.get("unmatched_or_inexact_exits") or 0),
            "exact_entry_target": exact_entry_target,
            "replay_migrated": replay_migrated,
            "readiness_authority": (
                "candidate_virtual_exact_nbbo_replay"
                if replay_migrated
                else "candidate_observation_only"
            ),
            "blockers": blockers,
        },
        "legacy_research": {
            "strategy_id": LEGACY_GTH_STRATEGY_ID,
            "source": LEGACY_GTH_SOURCE,
            "lifecycle_status": LEGACY_GTH_LIFECYCLE_STATUS,
            "records": len(legacy),
            "unique_signals": len(legacy_ids),
            "declared_legacy_research_records": declared_legacy,
            "excluded_from_runtime_readiness": True,
        },
        "selection_rule": (
            "gth_level_manual_candidate is the current runtime lane; gth_dip_reclaim "
            "is legacy research and cannot satisfy runtime readiness"
        ),
    }


def gth_runtime_identity_issues(payload: Mapping[str, object]) -> tuple[str, ...]:
    """Validate identity fields that separate the current lane from legacy research."""

    issues: list[str] = []
    if payload.get("strategy_id") != RUNTIME_GTH_STRATEGY_ID:
        issues.append("runtime_strategy_id_mismatch")
    if payload.get("lifecycle_status") != RUNTIME_GTH_LIFECYCLE_STATUS:
        issues.append("runtime_lifecycle_status_mismatch")
    if payload.get("runtime_status") != RUNTIME_GTH_RUNTIME_STATUS:
        issues.append("runtime_status_mismatch")
    return tuple(issues)


def count_runtime_gth_exact_evidence(
    records: Sequence[GthReadinessRecord],
    *,
    eligible_sessions: set[str],
) -> dict[str, object]:
    """Join current candidates to exact virtual decision/open/close evidence."""

    candidates = [
        record
        for record in records
        if record.source == RUNTIME_GTH_SOURCE
        and record.payload.get("status") == "manual_ready"
        and not gth_runtime_identity_issues(record.payload)
    ]
    virtual = [record for record in records if record.source == "virtual_strategy"]
    decisions = _first_by_source(
        record
        for record in virtual
        if record.payload.get("event") == "virtual_entry_decision"
        and record.payload.get("source_kind") == RUNTIME_GTH_SOURCE_KIND
    )
    opens = _first_by_source(
        record
        for record in virtual
        if record.payload.get("event") == "virtual_opened"
        and record.payload.get("source_kind") == RUNTIME_GTH_SOURCE_KIND
    )
    closes = {
        str(record.payload.get("episode_id")): record
        for record in sorted(virtual, key=_record_sort_key)
        if record.payload.get("event") == "virtual_closed"
        and record.payload.get("source_kind") == RUNTIME_GTH_SOURCE_KIND
        and _nonempty_string(record.payload.get("episode_id"))
    }
    successful_entries: set[str] = set()
    successful_episodes: set[str] = set()
    successful_exits: set[str] = set()
    eligible = 0
    excluded_incomplete = 0
    for candidate in candidates:
        session_id = candidate.session_date.isoformat() if candidate.session_date else ""
        if session_id not in eligible_sessions:
            excluded_incomplete += 1
            continue
        eligible += 1
        payload = candidate.payload
        candidate_id = str(payload.get("candidate_id") or "")
        decision = decisions.get(candidate_id)
        opened = opens.get(candidate_id)
        if (
            not _exact_runtime_candidate(payload)
            or decision is None
            or opened is None
            or not _exact_runtime_decision(payload, decision.payload)
            or not _exact_runtime_open(payload, opened.payload)
            or not _same_spread_snapshot(
                decision.payload.get("exact_spread_snapshot"),
                _entry_snapshot(opened.payload),
            )
        ):
            continue
        successful_entries.add(candidate_id)
        episode_id = str(opened.payload.get("episode_id") or "")
        if not episode_id:
            continue
        successful_episodes.add(episode_id)
        closed = closes.get(episode_id)
        if closed is not None and _exact_runtime_close(opened.payload, closed.payload):
            successful_exits.add(episode_id)
    return {
        "exact_entry_count": len(successful_entries),
        "exact_exit_count": len(successful_exits),
        "eligible_candidates": eligible,
        "successful_episode_ids": sorted(successful_episodes),
        "unmatched_or_inexact_entries": eligible - len(successful_entries),
        "unmatched_or_inexact_exits": len(successful_episodes - successful_exits),
        "excluded_incomplete_session": excluded_incomplete,
        # Do not claim replay migration from code presence alone. At least one
        # full current-lane entry/open/close chain must exist in eligible data.
        "replay_migrated": bool(successful_entries and successful_exits),
    }


def _first_by_source(
    records: Iterable[GthReadinessRecord],
) -> dict[str, GthReadinessRecord]:
    result: dict[str, GthReadinessRecord] = {}
    for record in sorted(records, key=_record_sort_key):
        source_id = str(record.payload.get("source_signal_id") or "")
        if source_id:
            result.setdefault(source_id, record)
    return result


def _exact_runtime_candidate(payload: Mapping[str, object]) -> bool:
    snapshot = payload.get("exact_spread_snapshot")
    evaluated_at = _event_at(payload)
    valid_until = _parse_time(payload.get("valid_until"))
    exit_at = _parse_time(payload.get("exit_at"))
    long = _parse_option_contract(payload.get("long_contract_id"))
    short = _parse_option_contract(payload.get("short_contract_id"))
    direction = payload.get("direction")
    expected_right = "C" if direction == "up" else "P" if direction == "down" else None
    expected_position = (
        "call_debit_spread"
        if direction == "up"
        else "put_debit_spread"
        if direction == "down"
        else None
    )
    width = _number(payload.get("spread_width_points"))
    entry_limit = _number(payload.get("entry_limit"))
    ask = _number(snapshot.get("ask")) if isinstance(snapshot, Mapping) else None
    session_date = str(payload.get("session_date") or "")
    return bool(
        payload.get("event") == "gth_level_manual_candidate_evaluated"
        and payload.get("kind") == RUNTIME_GTH_SOURCE_KIND
        and str(payload.get("policy_version") or "").startswith(
            "gth_level_manual_candidate.v1+sha256:"
        )
        and payload.get("strategy_lane") == RUNTIME_GTH_STRATEGY_ID
        and payload.get("status") == "manual_ready"
        and payload.get("manual_action_eligible") is True
        and payload.get("automatic_ordering") is False
        and payload.get("execution_eligible") is False
        and payload.get("broker_submission_allowed") is False
        and evaluated_at is not None
        and valid_until is not None
        and exit_at is not None
        and evaluated_at < valid_until <= exit_at
        and isinstance(snapshot, Mapping)
        and _ibkr_spread_snapshot(snapshot, at=evaluated_at)
        and _candidate_decision_nbbo_matches(payload, snapshot)
        and long is not None
        and short is not None
        and long[0] == short[0]
        and long[0].isoformat() == session_date
        and long[2] == short[2] == expected_right
        and payload.get("position_type") == expected_position
        and payload.get("contract_id")
        == f"{payload.get('long_contract_id')}|-{payload.get('short_contract_id')}"
        and width is not None
        and math.isclose(abs(short[1] - long[1]), width, abs_tol=1e-6)
        and (short[1] > long[1] if direction == "up" else short[1] < long[1])
        and entry_limit is not None
        and ask is not None
        and 0 < ask <= entry_limit < width
        and _number(payload.get("target_spx")) is not None
        and _number(payload.get("invalidation_spx")) is not None
        and _number(payload.get("invalidation_es")) is not None
    )


def _exact_runtime_decision(
    candidate: Mapping[str, object],
    decision: Mapping[str, object],
) -> bool:
    snapshot = decision.get("exact_spread_snapshot")
    return bool(
        decision.get("status") == "virtual_ready"
        and decision.get("terminal") is True
        and decision.get("source_kind") == RUNTIME_GTH_SOURCE_KIND
        and decision.get("source_policy_version") == candidate.get("policy_version")
        and decision.get("strategy_id") == RUNTIME_GTH_STRATEGY_ID
        and decision.get("lifecycle_status") == RUNTIME_GTH_LIFECYCLE_STATUS
        and decision.get("runtime_status") == RUNTIME_GTH_RUNTIME_STATUS
        and decision.get("position_type") == candidate.get("position_type")
        and decision.get("simulation_only") is True
        and decision.get("execution_eligible") is False
        and decision.get("automatic_ordering") is False
        and isinstance(snapshot, Mapping)
        and _ibkr_spread_snapshot(snapshot, at=_event_at(decision))
    )


def _exact_runtime_open(
    candidate: Mapping[str, object],
    opened: Mapping[str, object],
) -> bool:
    snapshot = _entry_snapshot(opened)
    entry_limit = _number(candidate.get("entry_limit"))
    ask = _number(snapshot.get("ask"))
    return bool(
        opened.get("source_kind") == RUNTIME_GTH_SOURCE_KIND
        and opened.get("source_policy_version") == candidate.get("policy_version")
        and opened.get("strategy_id") == RUNTIME_GTH_STRATEGY_ID
        and opened.get("lifecycle_status") == RUNTIME_GTH_LIFECYCLE_STATUS
        and opened.get("runtime_status") == RUNTIME_GTH_RUNTIME_STATUS
        and opened.get("position_type") == candidate.get("position_type")
        and opened.get("direction") == candidate.get("direction")
        and opened.get("long_contract_id") == candidate.get("long_contract_id")
        and opened.get("short_contract_id") == candidate.get("short_contract_id")
        and opened.get("automatic_ordering") is False
        and entry_limit is not None
        and ask is not None
        and ask <= entry_limit
        and _ibkr_spread_snapshot(snapshot, at=_event_at(opened))
    )


def _exact_runtime_close(
    opened: Mapping[str, object],
    closed: Mapping[str, object],
) -> bool:
    return bool(
        closed.get("source_kind") == RUNTIME_GTH_SOURCE_KIND
        and closed.get("episode_id") == opened.get("episode_id")
        and closed.get("automatic_ordering") is False
        and _exact_spread_close(closed)
        and _ibkr_spread_snapshot(
            closed.get("exit_snapshot"),
            at=_event_at(closed),
        )
    )


def _ibkr_spread_snapshot(snapshot: object, *, at: datetime | None) -> bool:
    if not isinstance(snapshot, Mapping) or not _exact_spread_snapshot(snapshot, at=at):
        return False
    long = snapshot.get("long")
    short = snapshot.get("short")
    snapshot_at = _parse_time(snapshot.get("at"))
    return bool(
        isinstance(long, Mapping)
        and isinstance(short, Mapping)
        and snapshot_at == at
        and long.get("provider") == "ibkr"
        and short.get("provider") == "ibkr"
        and _fresh_transport(long, at=at)
        and _fresh_transport(short, at=at)
        and _candidate_decision_nbbo_matches({}, snapshot, require_recorded=False)
    )


def _fresh_transport(snapshot: Mapping[str, object], *, at: datetime | None) -> bool:
    transport_at = _parse_time(snapshot.get("transport_at"))
    if transport_at is None or at is None:
        return False
    age = (at - transport_at).total_seconds()
    return -1.0 <= age <= 5.0


def _candidate_decision_nbbo_matches(
    candidate: Mapping[str, object],
    snapshot: Mapping[str, object],
    *,
    require_recorded: bool = True,
) -> bool:
    long = snapshot.get("long")
    short = snapshot.get("short")
    if not isinstance(long, Mapping) or not isinstance(short, Mapping):
        return False
    long_bid = _number(long.get("bid"))
    long_mid = _number(long.get("mid"))
    long_ask = _number(long.get("ask"))
    short_bid = _number(short.get("bid"))
    short_mid = _number(short.get("mid"))
    short_ask = _number(short.get("ask"))
    if None in {long_bid, long_mid, long_ask, short_bid, short_mid, short_ask}:
        return False
    assert long_bid is not None and long_mid is not None and long_ask is not None
    assert short_bid is not None and short_mid is not None and short_ask is not None
    expected = {
        "bid": long_bid - short_ask,
        "mid": long_mid - short_mid,
        "ask": long_ask - short_bid,
    }
    for name, expected_value in expected.items():
        top = _number(snapshot.get(name))
        recorded = _number(candidate.get(f"decision_{name}"))
        if top is None or not math.isclose(top, expected_value, abs_tol=1e-9):
            return False
        if require_recorded and (
            recorded is None or not math.isclose(recorded, expected_value, abs_tol=1e-9)
        ):
            return False
    return True


def _material_runtime_candidate(payload: Mapping[str, object]) -> bool:
    return bool(
        str(payload.get("source_signal_id") or "").strip()
        and payload.get("status") in {"blocked", "manual_ready", "structure_watch"}
    )


def _unique_candidates(
    records: Iterable[GthReadinessRecord],
) -> dict[str, GthReadinessRecord]:
    result: dict[str, GthReadinessRecord] = {}
    for record in sorted(
        records,
        key=lambda row: row.at or datetime.min.replace(tzinfo=timezone.utc),
    ):
        candidate_id = str(record.payload.get("candidate_id") or "").strip()
        if candidate_id:
            result.setdefault(candidate_id, record)
    return result
