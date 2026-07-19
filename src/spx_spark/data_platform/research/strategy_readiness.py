"""Forward-only readiness gates for strategy parameter adjudication.

The module deliberately separates elapsed, well-observed market sessions from
strategy opportunities.  A session can therefore count even when no signal
fires, while a signal or virtual execution can count only when it uses the
version-three common contract and belongs to a contract-consistent session.

This is a review gate, not a promotion mechanism.  ``automatic_promotion`` is
always false and insufficient evidence is represented as ``collecting``.
"""

from __future__ import annotations

import json
from collections import Counter, defaultdict
from collections.abc import Mapping, Sequence
from dataclasses import asdict, dataclass
from datetime import date, datetime, time, timedelta, timezone
from pathlib import Path
from typing import Any

from spx_spark.market_calendar import DEFAULT_MARKET_CALENDAR, ET

from .strategy_readiness_evidence import (
    _exact_spread_snapshot,
    cohort_result,
    count_exact_spread_exits,
    count_gth_exact_entries,
    count_put_exact_entries,
    duplicate_audit,
)


__all__ = (
    "DEFAULT_THRESHOLDS",
    "ReadinessThresholds",
    "_exact_spread_snapshot",
    "build_strategy_readiness",
    "measure_session_completeness",
    "validate_strategy_contract",
)


CONTRACT_SCHEMA_VERSION = 3
GTH_OPEN_ET = time(20, 15)
GTH_CLOSE_ET = time(9, 25)


@dataclass(frozen=True, slots=True)
class ReadinessThresholds:
    """Frozen minimums for the next strategy review."""

    complete_sessions: int = 20
    gth_exact_entries: int = 20
    put_exact_entries: int = 20
    exact_spread_exits: int = 20
    minute_coverage_ratio: float = 0.90
    contract_coverage_ratio: float = 1.0

    def __post_init__(self) -> None:
        counts = (
            self.complete_sessions,
            self.gth_exact_entries,
            self.put_exact_entries,
            self.exact_spread_exits,
        )
        if any(value <= 0 for value in counts):
            raise ValueError("readiness sample thresholds must be positive")
        if not 0 < self.minute_coverage_ratio <= 1:
            raise ValueError("minute coverage ratio must be in (0, 1]")
        if self.contract_coverage_ratio != 1.0:
            raise ValueError("contract coverage is frozen at 100%")


DEFAULT_THRESHOLDS = ReadinessThresholds()


@dataclass(frozen=True, slots=True)
class _Record:
    source: str
    payload: Mapping[str, object]
    path: str
    line_number: int
    at: datetime | None
    session_date: date | None
    partition_date: date | None
    malformed_json: bool = False


_EVENT_DATASETS = {
    "gth_detector_health": "gth_detector_health/date=*/*.jsonl",
    "gth_dip_reclaim": "gth_dip_reclaim/date=*/*.jsonl",
    "confirmed_gate_results": "confirmed_gate_results/date=*/*.jsonl",
    "trade_intents": "trade_intents/date=*/*.jsonl",
    "trade_candidates": "trade_candidates/date=*/*.jsonl",
    "virtual_strategy": "virtual_strategy/date=*/*.jsonl",
}

REQUIRED_POLICY_ROLES = (
    "gth_detector_runtime",
    "gth_signal",
    "trade_intent",
    "virtual_entry_decision",
    "virtual_lifecycle",
)
OPTIONAL_POLICY_ROLES = ("confirmed_gate", "trade_candidate")
POLICY_ROLES = (*REQUIRED_POLICY_ROLES, *OPTIONAL_POLICY_ROLES)


def build_strategy_readiness(
    features_root: Path,
    *,
    cutoff_at: datetime,
    policy_versions: Mapping[str, str] | None = None,
    generated_at: datetime | None = None,
) -> dict[str, object]:
    """Build a deterministic forward-cohort readiness scorecard.

    ``cutoff_at`` is exclusive and must be timezone-aware.  Policies are
    selected independently for each producer role: producer-specific hashes are
    not expected to be equal.  A role may not mix versions inside one cohort.
    """

    cutoff = _utc(cutoff_at)
    generated = _utc(generated_at or datetime.now(tz=timezone.utc))
    thresholds = DEFAULT_THRESHOLDS
    root = Path(features_root).expanduser().resolve()
    session_rows = measure_session_completeness(
        root,
        cutoff_at=cutoff,
        minimum_coverage=thresholds.minute_coverage_ratio,
    )
    records = _load_event_records(root, cutoff_at=cutoff)
    policy = _select_policy_bundle(records, requested=policy_versions)
    detector_start_session = _detector_health_start(session_rows)
    rollout_start_session = detector_start_session
    rollout_boundary_at = _detector_health_start_at(session_rows)
    post_reset_start_session = _next_trading_day(policy["version_reset_session"])
    effective_start_session = _latest_date(rollout_start_session, post_reset_start_session)
    contract = _contract_audit(
        records,
        selected_policies=policy["versions"],
        cohort_start_session=effective_start_session,
        policy_start_session=rollout_start_session,
        rollout_boundary_at=rollout_boundary_at,
    )

    duplicate_evidence = duplicate_audit(contract["compliant_records"])
    violating_sessions = set(contract["violating_sessions"])
    violating_sessions.update(duplicate_evidence["sessions"])
    cohort_start_session = effective_start_session
    health_complete = [row for row in session_rows if row["complete"] is True]
    consistent_sessions = [
        str(row["session_date"])
        for row in health_complete
        if cohort_start_session is not None
        and date.fromisoformat(str(row["session_date"])) >= cohort_start_session
        and str(row["session_date"]) not in violating_sessions
    ]
    consistent_set = set(consistent_sessions)

    compliant = contract["compliant_records"]
    gth = count_gth_exact_entries(compliant, eligible_sessions=consistent_set)
    put = count_put_exact_entries(compliant, eligible_sessions=consistent_set)
    exits = count_exact_spread_exits(
        compliant,
        eligible_sessions=consistent_set,
        successful_gth_episodes=set(gth["episode_ids"]),
    )

    common_blockers = list(policy["blockers"])
    if detector_start_session is None:
        common_blockers.append("gth_detector_health_rollout_unavailable")
    if len(consistent_sessions) < thresholds.complete_sessions:
        common_blockers.append("contract_consistent_complete_sessions_below_20")
    if contract["coverage_ratio"] < thresholds.contract_coverage_ratio:
        common_blockers.append("contract_compliance_below_100_percent")
    if contract["invalid_records"]:
        common_blockers.append("forward_contract_anomalies_present")
    if contract["issues"].get("role_policy_version_mismatch", 0):
        common_blockers.append("same_role_policy_version_drift_present")
    if duplicate_evidence["duplicate_records"]:
        common_blockers.append("duplicate_forward_samples_present")
    if cohort_start_session is None:
        common_blockers.append("forward_v3_contract_unavailable")
    common_blockers = _unique(common_blockers)

    cohorts = {
        "gth_exact_entry": cohort_result(
            count=int(gth["count"]),
            target=thresholds.gth_exact_entries,
            count_blocker="gth_exact_entries_below_20",
            common_blockers=common_blockers,
            details={
                "eligible_signals": gth["eligible_signals"],
                "signals_with_exact_structure": gth["signals_with_exact_structure"],
                "unmatched_or_inexact_signals": gth["unmatched_or_inexact_signals"],
                "excluded_incomplete_session": gth["excluded_incomplete_session"],
            },
        ),
        "put_exact_entry": cohort_result(
            count=int(put["count"]),
            target=thresholds.put_exact_entries,
            count_blocker="put_exact_entries_below_20",
            common_blockers=common_blockers,
            details={
                "eligible_trade_ready_puts": put["eligible_trade_ready_puts"],
                "unmatched_or_inexact_puts": put["unmatched_or_inexact_puts"],
                "excluded_incomplete_session": put["excluded_incomplete_session"],
            },
        ),
        "exact_spread_complete_exit": cohort_result(
            count=int(exits["count"]),
            target=thresholds.exact_spread_exits,
            count_blocker="exact_spread_complete_exits_below_20",
            common_blockers=common_blockers,
            details={
                "eligible_exact_entries": len(gth["episode_ids"]),
                "unmatched_or_inexact_exits": exits["unmatched_or_inexact_exits"],
                "excluded_incomplete_session": exits["excluded_incomplete_session"],
            },
        ),
    }
    all_blockers = _unique(
        [*common_blockers, *(item for row in cohorts.values() for item in row["blockers"])]
    )
    ready = not all_blockers and all(row["status"] == "ready" for row in cohorts.values())

    public_contract = {
        key: value
        for key, value in contract.items()
        if key not in {"compliant_records", "violating_sessions"}
    }
    public_contract.update(
        {
            "duplicate_records": duplicate_evidence["duplicate_records"],
            "duplicate_keys": duplicate_evidence["keys"],
            "required_coverage_ratio": thresholds.contract_coverage_ratio,
        }
    )
    session_details = []
    for row in session_rows:
        session_id = str(row["session_date"])
        detail = dict(row)
        detail["forward_policy_window"] = bool(
            cohort_start_session is not None
            and date.fromisoformat(session_id) >= cohort_start_session
        )
        detail["contract_consistent"] = bool(
            detail["complete"]
            and detail["forward_policy_window"]
            and session_id not in violating_sessions
        )
        if session_id in violating_sessions:
            detail["reasons"] = [*detail["reasons"], "forward_contract_violation"]
        session_details.append(detail)

    return {
        "schema_version": 2,
        "generated_at": generated.isoformat(),
        "cutoff_at": cutoff.isoformat(),
        "mode": "forward_shadow_readiness",
        "status": "ready_for_review" if ready else "collecting",
        "automatic_promotion": False,
        "policy_versions": policy["versions"],
        "policy_bundle": {
            "selection": policy["selection"],
            "role_started_at": policy["role_started_at"],
            "started_session": (
                rollout_start_session.isoformat() if rollout_start_session is not None else None
            ),
            "effective_started_session": (
                effective_start_session.isoformat() if effective_start_session is not None else None
            ),
            "version_reset_session": (
                policy["version_reset_session"].isoformat()
                if policy["version_reset_session"] is not None
                else None
            ),
            "post_reset_started_session": (
                post_reset_start_session.isoformat()
                if post_reset_start_session is not None
                else None
            ),
        },
        "thresholds": asdict(thresholds),
        "sessions": {
            "observed": len(session_rows),
            "health_complete": len(health_complete),
            "contract_consistent_complete": len(consistent_sessions),
            "target": thresholds.complete_sessions,
            "dates": consistent_sessions,
            "gth_detector_health_started_session": (
                detector_start_session.isoformat() if detector_start_session is not None else None
            ),
            "details": session_details,
        },
        "contract": public_contract,
        "legacy_exclusion": contract["legacy_exclusion"],
        "cohorts": cohorts,
        "blockers": all_blockers,
    }


def measure_session_completeness(
    features_root: Path,
    *,
    cutoff_at: datetime,
    minimum_coverage: float = 0.90,
) -> list[dict[str, object]]:
    """Measure GTH and RTH minute coverage without reading ``quality_ok``.

    The GTH window for trading date D is 20:15 ET on D-1 through 09:25 ET on
    D.  RTH uses the canonical calendar, including scheduled early closes.
    Both intervals are half-open and each distinct wall-clock minute counts at
    most once, so faster duplicate health samples cannot inflate coverage.
    """

    if not 0 < minimum_coverage <= 1:
        raise ValueError("minimum coverage must be in (0, 1]")
    cutoff = _utc(cutoff_at)
    grouped: dict[date, list[datetime]] = defaultdict(list)
    detector_grouped: dict[date, list[datetime]] = defaultdict(list)
    root = Path(features_root).expanduser().resolve()
    pattern = root / "level_decision_health/date=*"
    for partition in sorted(root.glob(str(pattern.relative_to(root)))):
        for path in sorted(partition.glob("*.jsonl")):
            for payload in _read_json_objects(path):
                at = _parse_time(payload.get("at"))
                session_day = _parse_date(payload.get("session_date"))
                if at is None or session_day is None or at >= cutoff:
                    continue
                grouped[session_day].append(at)

    detector_pattern = root / "gth_detector_health/date=*"
    for partition in sorted(root.glob(str(detector_pattern.relative_to(root)))):
        for path in sorted(partition.glob("*.jsonl")):
            for payload in _read_json_objects(path):
                at = _parse_time(payload.get("at"))
                session_day = _parse_date(payload.get("session_date"))
                if at is None or session_day is None or at >= cutoff:
                    continue
                detector_grouped[session_day].append(at)
    detector_started = min(detector_grouped, default=None)
    detector_started_at = min(
        (sample for samples in detector_grouped.values() for sample in samples),
        default=None,
    )

    rows: list[dict[str, object]] = []
    for session_day, samples in sorted(grouped.items()):
        session = DEFAULT_MARKET_CALENDAR.session(session_day)
        if session is None or session.close_at.astimezone(timezone.utc) > cutoff:
            continue
        gth_start = datetime.combine(session_day - timedelta(days=1), GTH_OPEN_ET, tzinfo=ET)
        gth_end = datetime.combine(session_day, GTH_CLOSE_ET, tzinfo=ET)
        gth = _window_coverage(samples, gth_start, gth_end)
        rth = _window_coverage(samples, session.open_at, session.close_at)
        detector_required = detector_started is not None and session_day >= detector_started
        detector_gth = (
            _window_coverage(detector_grouped.get(session_day, ()), gth_start, gth_end)
            if detector_required
            else None
        )
        reasons = []
        if gth["coverage_ratio"] < minimum_coverage:
            reasons.append("gth_minute_coverage_below_90_percent")
        if rth["coverage_ratio"] < minimum_coverage:
            reasons.append("rth_minute_coverage_below_90_percent")
        if detector_gth is not None and detector_gth["coverage_ratio"] < minimum_coverage:
            reasons.append("gth_detector_health_coverage_below_90_percent")
        rows.append(
            {
                "session_date": session_day.isoformat(),
                "complete": not reasons,
                "gth": gth,
                "rth": rth,
                "gth_detector_health": detector_gth,
                "gth_detector_health_required": detector_required,
                "gth_detector_health_started_session": (
                    detector_started.isoformat() if detector_started is not None else None
                ),
                "gth_detector_health_started_at": (
                    detector_started_at.isoformat() if detector_started_at is not None else None
                ),
                "reasons": reasons,
            }
        )
    return rows


def validate_strategy_contract(
    payload: Mapping[str, object],
    *,
    event_at: datetime | None = None,
) -> tuple[str, ...]:
    """Return stable issue codes for the five-field version-three contract."""

    issues: list[str] = []
    if (
        type(payload.get("schema_version")) is not int
        or payload.get("schema_version") != CONTRACT_SCHEMA_VERSION
    ):
        issues.append("schema_version_not_3")
    if not _nonempty_string(payload.get("policy_version")):
        issues.append("policy_version_missing")
    at = event_at or _event_at(payload)
    if at is None:
        issues.append("event_time_missing_or_invalid")
    if "valid_until" not in payload or _parse_time(payload.get("valid_until")) is None:
        issues.append("valid_until_missing_or_invalid")
    coordinate = payload.get("coordinate")
    if not isinstance(coordinate, Mapping):
        issues.append("coordinate_missing_or_invalid")
    elif not (
        _nonempty_string(coordinate.get("kind"))
        and _nonempty_string(coordinate.get("instrument_id"))
        and coordinate.get("kind") != "unavailable"
    ):
        issues.append("coordinate_missing_or_invalid")
    block_reasons = payload.get("block_reasons")
    if (
        not isinstance(block_reasons, list)
        or any(
            not isinstance(reason, str) or not reason.strip() or reason != reason.strip()
            for reason in block_reasons
        )
        or (isinstance(block_reasons, list) and len(block_reasons) != len(set(block_reasons)))
    ):
        issues.append("block_reasons_missing_or_invalid")
    return tuple(issues)


def _window_coverage(
    samples: Sequence[datetime], start: datetime, end: datetime
) -> dict[str, object]:
    start_utc = start.astimezone(timezone.utc)
    end_utc = end.astimezone(timezone.utc)
    expected = int((end_utc - start_utc).total_seconds() // 60)
    minutes = {
        sample.astimezone(timezone.utc).replace(second=0, microsecond=0)
        for sample in samples
        if start_utc <= sample.astimezone(timezone.utc) < end_utc
    }
    ratio = min(len(minutes) / expected, 1.0) if expected else 0.0
    return {
        "observed_minutes": len(minutes),
        "expected_minutes": expected,
        "coverage_ratio": round(ratio, 6),
    }


def _load_event_records(features_root: Path, *, cutoff_at: datetime) -> list[_Record]:
    records: list[_Record] = []
    for source, pattern in _EVENT_DATASETS.items():
        for path in sorted(features_root.glob(pattern)):
            partition_date = _partition_date(path)
            try:
                lines = path.read_text(encoding="utf-8").splitlines()
            except OSError:
                continue
            for line_number, line in enumerate(lines, 1):
                try:
                    raw = json.loads(line)
                except json.JSONDecodeError:
                    raw = {}
                    malformed = True
                else:
                    malformed = not isinstance(raw, dict)
                payload: Mapping[str, object] = raw if isinstance(raw, dict) else {}
                at = _event_at(payload)
                if at is not None and at >= cutoff_at:
                    continue
                if at is None and partition_date is not None:
                    if partition_date >= cutoff_at.date():
                        continue
                session_day = _record_session(payload, at=at, fallback=partition_date)
                records.append(
                    _Record(
                        source=source,
                        payload=payload,
                        path=str(path),
                        line_number=line_number,
                        at=at,
                        session_date=session_day,
                        partition_date=partition_date,
                        malformed_json=malformed,
                    )
                )
    return records


def _select_policy_bundle(
    records: Sequence[_Record], *, requested: Mapping[str, str] | None
) -> dict[str, Any]:
    by_role: dict[str, list[_Record]] = defaultdict(list)
    for record in records:
        role = _policy_role(record)
        if (
            role is not None
            and record.at is not None
            and (
                record.payload.get("schema_version") == CONTRACT_SCHEMA_VERSION
                or record.source == "gth_detector_health"
            )
            and _nonempty_string(record.payload.get("policy_version"))
        ):
            by_role[role].append(record)

    blockers: list[str] = []
    versions: dict[str, str | None] = {}
    starts: dict[str, datetime] = {}
    reset_sessions: list[date] = []
    selection = "explicit" if requested is not None else "latest_per_role"
    if requested is not None:
        unknown = sorted(set(requested) - set(POLICY_ROLES))
        if unknown:
            raise ValueError(f"unknown policy roles: {', '.join(unknown)}")

    for role in POLICY_ROLES:
        rows = sorted(by_role.get(role, ()), key=_record_sort_key)
        selected: str | None = None
        selected_start: datetime | None = None
        if requested is not None and (role in requested or role in REQUIRED_POLICY_ROLES):
            value = requested.get(role)
            if value is not None and not str(value).strip():
                raise ValueError(f"policy version for {role} must be non-empty")
            selected = str(value).strip() if value is not None else None
            matching = [
                row
                for row in rows
                if row.payload.get("policy_version") == selected and row.at is not None
            ]
            if matching:
                selected_start = matching[0].at
        elif rows:
            selected = str(rows[-1].payload["policy_version"]).strip()
            tail = len(rows) - 1
            while tail > 0 and rows[tail - 1].payload.get("policy_version") == selected:
                tail -= 1
            selected_start = rows[tail].at
            if tail > 0 and selected_start is not None:
                reset_session = _research_session(selected_start)
                if reset_session is not None:
                    reset_sessions.append(reset_session)
        versions[role] = selected
        if selected is None or selected_start is None:
            if role not in REQUIRED_POLICY_ROLES:
                continue
            blockers.append(f"policy_role_unavailable:{role}")
        else:
            starts[role] = selected_start

    return {
        "versions": versions,
        "selection": selection,
        "role_started_at": {
            role: starts[role].isoformat() if role in starts else None for role in POLICY_ROLES
        },
        "version_reset_session": max(reset_sessions, default=None),
        "blockers": blockers,
    }


def _contract_audit(
    records: Sequence[_Record],
    *,
    selected_policies: Mapping[str, str | None],
    cohort_start_session: date | None,
    policy_start_session: date | None,
    rollout_boundary_at: datetime | None,
) -> dict[str, Any]:
    compliant: list[_Record] = []
    invalid = 0
    issue_counts: Counter[str] = Counter()
    source_totals: Counter[str] = Counter()
    source_compliant: Counter[str] = Counter()
    role_totals: Counter[str] = Counter()
    role_compliant: Counter[str] = Counter()
    violating_sessions: set[str] = set()
    legacy: Counter[str] = Counter()
    other_policy_excluded: Counter[str] = Counter()
    telemetry: Counter[str] = Counter()
    policy_telemetry: Counter[str] = Counter()

    for record in records:
        role = _record_role(record)
        in_forward_window = _in_forward_window(record, cohort_start_session)
        before_rollout_boundary = bool(
            in_forward_window
            and rollout_boundary_at is not None
            and record.at is not None
            and record.at < rollout_boundary_at
        )
        if before_rollout_boundary:
            if record.payload.get("schema_version") == CONTRACT_SCHEMA_VERSION:
                other_policy_excluded[record.source] += 1
            else:
                legacy[record.source] += 1
            continue
        if role is None:
            if in_forward_window:
                target = policy_telemetry if record.source == "gth_detector_health" else telemetry
                target[record.source] += 1
            continue
        if not in_forward_window:
            if record.payload.get("schema_version") == CONTRACT_SCHEMA_VERSION:
                other_policy_excluded[record.source] += 1
            else:
                legacy[record.source] += 1
            continue
        source_totals[record.source] += 1
        role_totals[role] += 1
        issues = list(_material_contract_issues(record, role=role))
        if record.malformed_json:
            issues.append("malformed_json")
        if record.payload.get("policy_version") != selected_policies.get(role):
            issues.append("role_policy_version_mismatch")
        issues = _unique(issues)
        if issues:
            invalid += 1
            issue_counts.update(issues)
            if record.session_date is not None:
                violating_sessions.add(record.session_date.isoformat())
            continue
        compliant.append(record)
        source_compliant[record.source] += 1
        role_compliant[role] += 1

    total = sum(source_totals.values())
    coverage = len(compliant) / total if total else 0.0
    return {
        "cohort_started_session": (
            cohort_start_session.isoformat() if cohort_start_session is not None else None
        ),
        "policy_bundle_started_session": (
            policy_start_session.isoformat() if policy_start_session is not None else None
        ),
        "forward_records": total,
        "compliant_records_count": len(compliant),
        "invalid_records": invalid,
        "coverage_ratio": round(coverage, 6),
        "issues": dict(sorted(issue_counts.items())),
        "by_source": {
            source: {
                "forward_records": source_totals[source],
                "compliant_records": source_compliant[source],
            }
            for source in sorted(source_totals)
        },
        "by_role": {
            role: {
                "policy_version": selected_policies.get(role),
                "forward_records": role_totals[role],
                "compliant_records": role_compliant[role],
            }
            for role in POLICY_ROLES
        },
        "telemetry_excluded": {
            "total": sum(telemetry.values()),
            "by_source": dict(sorted(telemetry.items())),
            "rule": "non-terminal observing and non-decision audit rows are not opportunities",
        },
        "policy_telemetry_excluded": {
            "total": sum(policy_telemetry.values()),
            "by_source": dict(sorted(policy_telemetry.items())),
            "rule": "health samples declare policy continuity but are not decision telemetry",
        },
        "legacy_exclusion": {
            "total": sum(legacy.values()),
            "by_source": dict(sorted(legacy.items())),
            "other_policy_before_cohort": sum(other_policy_excluded.values()),
            "other_policy_by_source": dict(sorted(other_policy_excluded.items())),
            "rule": "pre-v3 and pre-cohort rows never count as forward evidence",
        },
        "compliant_records": compliant,
        "violating_sessions": violating_sessions,
    }


def _in_forward_window(
    record: _Record,
    cohort_session: date | None,
) -> bool:
    if cohort_session is None:
        return False
    if record.session_date is not None:
        return record.session_date >= cohort_session
    if record.partition_date is not None:
        return record.partition_date >= cohort_session
    return True


def _record_role(record: _Record) -> str | None:
    row = record.payload
    if record.source == "gth_dip_reclaim":
        return "gth_signal"
    if record.source == "trade_intents" and row.get("status") in {"blocked", "trade_ready"}:
        return "trade_intent"
    if (
        record.source == "confirmed_gate_results"
        and row.get("status") in {"blocked", "trade_ready"}
        and row.get("terminal") is True
    ):
        return "confirmed_gate"
    if record.source == "trade_candidates" and row.get("event") in {
        "candidate_armed",
        "candidate_terminal",
    }:
        return "trade_candidate"
    if record.source != "virtual_strategy":
        return None
    if row.get("event") == "virtual_entry_decision" and row.get("status") in {
        "blocked",
        "trade_ready",
    }:
        return "virtual_entry_decision"
    if row.get("event") in {"virtual_opened", "virtual_closed"}:
        return "virtual_lifecycle"
    return None


def _policy_role(record: _Record) -> str | None:
    if record.source == "gth_detector_health":
        return "gth_detector_runtime"
    if record.source == "trade_intents":
        return "trade_intent"
    if (
        record.source == "virtual_strategy"
        and record.payload.get("event") == "virtual_entry_decision"
    ):
        return "virtual_entry_decision"
    return _record_role(record)


def _material_contract_issues(record: _Record, *, role: str) -> tuple[str, ...]:
    issues = list(validate_strategy_contract(record.payload, event_at=record.at))
    valid_until = _parse_time(record.payload.get("valid_until"))
    if role == "virtual_entry_decision" and record.payload.get("terminal") is not True:
        issues.append("entry_decision_not_terminal")
    requires_live_ttl = (
        role == "gth_signal"
        or (role == "trade_intent" and record.payload.get("status") == "trade_ready")
        or (role == "virtual_entry_decision" and record.payload.get("status") == "trade_ready")
        or (role == "virtual_lifecycle" and record.payload.get("event") == "virtual_opened")
        or (role == "trade_candidate" and record.payload.get("event") == "candidate_armed")
    )
    if valid_until is not None and record.at is not None and requires_live_ttl:
        if valid_until <= record.at:
            issues.append("valid_until_not_after_decision")
    return tuple(_unique(issues))


def _record_sort_key(record: _Record) -> tuple[datetime, str, int]:
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


def _record_session(
    payload: Mapping[str, object],
    *,
    at: datetime | None,
    fallback: date | None,
) -> date | None:
    for field in ("session_date", "session_id"):
        parsed = _parse_date(payload.get(field))
        if parsed is not None:
            return parsed
    return _research_session(at) if at is not None else fallback


def _research_session(at: datetime | None) -> date | None:
    return DEFAULT_MARKET_CALENDAR.research_expiry(at) if at is not None else None


def _read_json_objects(path: Path) -> list[Mapping[str, object]]:
    try:
        lines = path.read_text(encoding="utf-8").splitlines()
    except OSError:
        return []
    rows: list[Mapping[str, object]] = []
    for line in lines:
        try:
            payload = json.loads(line)
        except json.JSONDecodeError:
            continue
        if isinstance(payload, dict):
            rows.append(payload)
    return rows


def _partition_date(path: Path) -> date | None:
    for parent in path.parents:
        if parent.name.startswith("date="):
            return _parse_date(parent.name.removeprefix("date="))
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


def _nonempty_string(value: object) -> bool:
    return isinstance(value, str) and bool(value.strip())


def _unique(values: Sequence[str]) -> list[str]:
    return list(dict.fromkeys(values))


def _detector_health_start(rows: Sequence[Mapping[str, object]]) -> date | None:
    starts = [
        parsed
        for row in rows
        if (parsed := _parse_date(row.get("gth_detector_health_started_session"))) is not None
    ]
    return min(starts, default=None)


def _detector_health_start_at(rows: Sequence[Mapping[str, object]]) -> datetime | None:
    times = [
        parsed
        for row in rows
        if (parsed := _parse_time(row.get("gth_detector_health_started_at"))) is not None
    ]
    return min(times, default=None)


def _latest_date(*values: object) -> date | None:
    dates = [value for value in values if isinstance(value, date)]
    return max(dates, default=None)


def _next_trading_day(value: object) -> date | None:
    if not isinstance(value, date):
        return None
    candidate = value + timedelta(days=1)
    while not DEFAULT_MARKET_CALENDAR.is_trading_day(candidate):
        candidate += timedelta(days=1)
    return candidate


def _utc(value: datetime) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError("readiness timestamps must be timezone-aware")
    return value.astimezone(timezone.utc)
