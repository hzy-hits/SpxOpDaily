"""Evidence-backed promotion report for the wall/flip shadow machine."""

from __future__ import annotations

import argparse
import json
from collections import Counter
from dataclasses import asdict, dataclass
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Iterable

from spx_spark.config import StorageSettings
from spx_spark.market_calendar import DEFAULT_MARKET_CALENDAR
from spx_spark.settings import load_app_settings
from spx_spark.settings.level_decision import LevelDecisionPolicy
from spx_spark.state_io import atomic_write_json_secure


@dataclass(frozen=True)
class SessionAcceptance:
    session_date: str
    completed: bool
    sample_count: int
    expected_samples: int
    coverage_ratio: float
    quality_ratio: float
    max_gap_seconds: float | None
    reasons: tuple[str, ...]


def build_acceptance_report(
    data_root: str | Path,
    *,
    policy: LevelDecisionPolicy,
    now: datetime | None = None,
) -> dict[str, object]:
    now = _utc(now or datetime.now(tz=timezone.utc))
    root = Path(data_root)
    transitions = tuple(_read_tree(root / "features" / "level_decision_audit"))
    outcomes = tuple(_read_tree(root / "features" / "level_decision_outcomes"))
    health = tuple(_read_tree(root / "features" / "level_decision_health"))
    confirmed_ids = {
        str(row.get("event_id"))
        for row in transitions
        if row.get("current_phase") == "confirmed" and row.get("event_id")
    }
    observed_sessions = {
        str(row.get("session_date")) for row in health if row.get("session_date")
    }
    session_reports = _session_reports(health, policy=policy, now=now)
    complete_sessions = [row for row in session_reports if row.completed]
    attribution_counts = Counter(
        str(row.get("attribution") or "unknown") for row in outcomes
    )
    gates = {
        "confirmed_events": len(confirmed_ids) >= policy.acceptance_min_events,
        "observed_sessions": len(observed_sessions) >= policy.acceptance_min_sessions,
        "complete_rth_sessions": (
            len(complete_sessions) >= policy.acceptance_min_complete_rth_sessions
        ),
    }
    eligible = all(gates.values())
    operator_override = policy.formal_signal_enabled
    return {
        "schema_version": 1,
        "generated_at": now.isoformat(),
        "mode": "shadow_acceptance",
        "formal_signal": operator_override,
        "promoted": operator_override,
        "promotion_basis": (
            "explicit_operator_override" if operator_override else "not_promoted"
        ),
        "acceptance_gates_passed": eligible,
        "eligible_for_explicit_review": eligible,
        "explicit_review_required": True,
        "gates": gates,
        "requirements": {
            "min_events": policy.acceptance_min_events,
            "min_sessions": policy.acceptance_min_sessions,
            "min_complete_rth_sessions": policy.acceptance_min_complete_rth_sessions,
        },
        "counts": {
            "confirmed_events": len(confirmed_ids),
            "outcome_rows": len(outcomes),
            "observed_sessions": len(observed_sessions),
            "complete_rth_sessions": len(complete_sessions),
        },
        "outcome_attribution": dict(sorted(attribution_counts.items())),
        "sessions": [asdict(row) for row in session_reports],
    }


def write_acceptance_report(
    data_root: str | Path,
    report: dict[str, object],
) -> Path:
    path = Path(data_root) / "latest" / "level_decision_acceptance.json"
    atomic_write_json_secure(path, report)
    return path


def _session_reports(
    rows: Iterable[dict[str, object]],
    *,
    policy: LevelDecisionPolicy,
    now: datetime,
) -> list[SessionAcceptance]:
    grouped: dict[str, list[dict[str, object]]] = {}
    for row in rows:
        if row.get("session_mode") != "rth":
            continue
        session_date = str(row.get("session_date") or "")
        if session_date:
            grouped.setdefault(session_date, []).append(row)
    reports: list[SessionAcceptance] = []
    for session_date, samples in sorted(grouped.items()):
        try:
            day = date.fromisoformat(session_date)
        except ValueError:
            continue
        session = DEFAULT_MARKET_CALENDAR.session(day)
        if session is None:
            continue
        times = sorted(
            parsed
            for row in samples
            if (parsed := _parse_at(row.get("at"))) is not None
            and session.open_at <= parsed.astimezone(session.open_at.tzinfo) < session.close_at
        )
        expected = max(
            int(
                (session.close_at - session.open_at).total_seconds()
                / policy.acceptance_expected_sample_seconds
            ),
            1,
        )
        coverage = min(len(set(times)) / expected, 1.0)
        quality_ratio = (
            sum(1 for row in samples if row.get("quality_ok") is True) / len(samples)
            if samples
            else 0.0
        )
        gaps = []
        if times:
            gaps.append((times[0] - session.open_at.astimezone(timezone.utc)).total_seconds())
            gaps.extend((right - left).total_seconds() for left, right in zip(times, times[1:]))
            gaps.append((session.close_at.astimezone(timezone.utc) - times[-1]).total_seconds())
        max_gap = max(gaps) if gaps else None
        reasons: list[str] = []
        if now < session.close_at.astimezone(timezone.utc):
            reasons.append("session_not_finished")
        if coverage < policy.acceptance_min_rth_sample_ratio:
            reasons.append("sample_coverage_below_threshold")
        if quality_ratio < policy.acceptance_min_rth_sample_ratio:
            reasons.append("quality_ratio_below_threshold")
        if max_gap is None or max_gap > policy.acceptance_max_rth_gap_seconds:
            reasons.append("sample_gap_above_threshold")
        reports.append(
            SessionAcceptance(
                session_date=session_date,
                completed=not reasons,
                sample_count=len(times),
                expected_samples=expected,
                coverage_ratio=round(coverage, 6),
                quality_ratio=round(quality_ratio, 6),
                max_gap_seconds=max_gap,
                reasons=tuple(reasons),
            )
        )
    return reports


def _read_tree(root: Path) -> Iterable[dict[str, object]]:
    if not root.exists():
        return ()
    rows: list[dict[str, object]] = []
    for path in sorted(root.glob("date=*/*.jsonl")):
        try:
            lines = path.read_text(encoding="utf-8").splitlines()
        except OSError:
            continue
        for line in lines:
            try:
                row = json.loads(line)
            except json.JSONDecodeError:
                continue
            if isinstance(row, dict):
                rows.append(row)
    return rows


def _parse_at(value: object) -> datetime | None:
    if not isinstance(value, str):
        return None
    try:
        parsed = datetime.fromisoformat(value)
    except ValueError:
        return None
    return _utc(parsed.replace(tzinfo=parsed.tzinfo or timezone.utc))


def _utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        raise ValueError("acceptance timestamps must be timezone-aware")
    return value.astimezone(timezone.utc)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Build wall/flip shadow acceptance report")
    parser.add_argument("--json", action="store_true")
    args = parser.parse_args(argv)
    storage = StorageSettings.from_env()
    policy = load_app_settings().level_decision
    report = build_acceptance_report(storage.data_root, policy=policy)
    path = write_acceptance_report(storage.data_root, report)
    if args.json:
        print(json.dumps({**report, "path": str(path)}, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
