"""Phase-1 opportunity ledger: record each committed decision once.

The Worker samples ``latest/strategy_decision.json`` every minute and appends a
row when the decision id or state changes. Phase 1 covers the final decision
surface (authorized / rejected) with stage and reason codes; detected, delivered
and acted states remain the documented target.
"""

from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Mapping

from spx_spark.state_io import (
    append_jsonl_secure,
    atomic_write_json_secure,
    read_json_object,
)

STATE_FILE = "latest/opportunity_ledger_state.json"
LEDGER_DIR = "opportunities"


def decision_opportunity_row(
    decision: Mapping[str, Any],
    *,
    now: datetime,
) -> dict[str, Any] | None:
    decision_id = str(decision.get("decision_id") or "").strip()
    if not decision_id:
        return None
    candidate = _mapping(decision.get("candidate"))
    decision_type = str(decision.get("decision_type") or "")
    funnel = _mapping(decision.get("rejection_funnel"))
    if bool(candidate) and decision_type != "NO_TRADE":
        state = "authorized"
    elif funnel.get("setup_detected"):
        state = "detected"
    else:
        state = "rejected"
    session_date = str(decision.get("session_date") or now.date().isoformat())
    return {
        "schema_version": 1,
        "opportunity_id": str(candidate.get("opportunity_id") or decision_id),
        "decision_id": decision_id,
        "session_date": session_date,
        "state": state,
        "decision_type": decision_type,
        "strategy_type": str(candidate.get("strategy_type") or decision_type),
        "direction": str(candidate.get("direction") or "none").lower(),
        "stage": funnel.get("current_stage"),
        "reason_codes": _reason_codes(decision, funnel),
        "correlation_id": decision.get("correlation_id"),
        "policy_version": decision.get("policy_version"),
        "decision_at": decision.get("decision_at"),
        "recorded_at": _utc(now).isoformat(),
    }


def record_decision(
    data_root: str | Path,
    decision: Mapping[str, Any],
    *,
    now: datetime,
) -> dict[str, Any] | None:
    row = decision_opportunity_row(decision, now=now)
    if row is None:
        return None
    root = Path(data_root)
    state_path = root / STATE_FILE
    previous = read_json_object(state_path)
    if (
        previous.get("decision_id") == row["decision_id"]
        and previous.get("state") == row["state"]
    ):
        return None
    append_jsonl_secure(
        root / LEDGER_DIR / f"date={row['session_date']}.jsonl",
        row,
    )
    atomic_write_json_secure(
        state_path,
        {
            "schema_version": 1,
            "decision_id": row["decision_id"],
            "state": row["state"],
            "updated_at": _utc(now).isoformat(),
        },
    )
    return row


def record_current_decision(
    data_root: str | Path,
    *,
    now: datetime | None = None,
) -> dict[str, Any] | None:
    root = Path(data_root)
    decision = read_json_object(root / "latest" / "strategy_decision.json")
    if not decision:
        return None
    return record_decision(root, decision, now=now or datetime.now(tz=timezone.utc))


def capture_summary(
    data_root: str | Path,
    session_date: str,
    *,
    database_path: str | Path | None = None,
) -> dict[str, Any]:
    rows = _read_rows(Path(data_root) / LEDGER_DIR / f"date={session_date}.jsonl")
    delivered = _delivered_correlations(
        Path(database_path) if database_path is not None else Path(data_root) / "spx.sqlite"
    )
    authorized = [row for row in rows if row.get("state") == "authorized"]
    delivered_authorized = None if delivered is None else sum(
        1
        for row in authorized
        if row.get("correlation_id") and row["correlation_id"] in delivered
    )
    return {
        "session_date": session_date,
        "total": len(rows),
        "by_state": _counts(rows, "state"),
        "by_stage": _counts(rows, "stage"),
        "by_strategy": _counts(rows, "strategy_type"),
        "top_blockers": _blockers(rows),
        "authorized_delivered": delivered_authorized,
        "authorized_not_delivered": (
            len(authorized) - delivered_authorized if delivered_authorized is not None else None
        ),
        "delivery_scope": "python_status_including_rust_handoff_not_final_receipt",
        "acted": None,  # No acknowledgement means unknown, not zero executions.
        "coverage": "minute_sampled_latest_decision",
    }


def capture_week_summary(
    data_root: str | Path,
    end_date: str,
    *,
    days: int = 7,
    database_path: str | Path | None = None,
) -> dict[str, Any]:
    from datetime import date, timedelta

    end = date.fromisoformat(end_date)
    dates = [(end - timedelta(days=offset)).isoformat() for offset in range(days)]
    summaries = [
        capture_summary(data_root, value, database_path=database_path) for value in dates
    ]
    totals = {
        "total": sum(item["total"] for item in summaries),
        "authorized_delivered": (sum(item["authorized_delivered"] for item in summaries)
                                 if all(item["authorized_delivered"] is not None for item in summaries) else None),
        "authorized_not_delivered": (sum(item["authorized_not_delivered"] for item in summaries)
                                     if all(item["authorized_not_delivered"] is not None for item in summaries) else None),
        "delivery_scope": "python_status_including_rust_handoff_not_final_receipt",
        "acted": None,
        "coverage": "minute_sampled_latest_decision",
    }
    by_state: dict[str, int] = {}
    by_stage: dict[str, int] = {}
    blockers: dict[str, int] = {}
    for item in summaries:
        for key, value in item["by_state"].items():
            by_state[key] = by_state.get(key, 0) + value
        for key, value in item["by_stage"].items():
            by_stage[key] = by_stage.get(key, 0) + value
        for key, value in item["top_blockers"].items():
            blockers[key] = blockers.get(key, 0) + value
    return {
        "week_ending": end_date,
        "days": days,
        **totals,
        "by_state": dict(sorted(by_state.items(), key=lambda item: item[1], reverse=True)),
        "by_stage": dict(sorted(by_stage.items(), key=lambda item: item[1], reverse=True)),
        "top_blockers": dict(
            sorted(blockers.items(), key=lambda item: item[1], reverse=True)[:10]
        ),
    }


def _read_rows(path: Path) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    try:
        for line in path.read_text(encoding="utf-8").splitlines():
            parsed = _json(line)
            if parsed is not None:
                rows.append(parsed)
    except OSError:
        pass
    return rows


def _delivered_correlations(database_path: Path) -> set[str] | None:
    import sqlite3

    if not database_path.exists():
        return None
    try:
        connection = sqlite3.connect(f"file:{database_path}?mode=ro", uri=True)
    except sqlite3.Error:
        return None
    try:
        rows = connection.execute(
            "SELECT DISTINCT correlation_id FROM notification_events "
            "WHERE status='delivered' AND correlation_id IS NOT NULL"
        ).fetchall()
    except sqlite3.Error:
        return None
    finally:
        connection.close()
    return {str(row[0]) for row in rows}


def _blockers(rows: list[Mapping[str, Any]]) -> dict[str, int]:
    blockers: dict[str, int] = {}
    for row in rows:
        for code in row.get("reason_codes") or ():
            blockers[str(code)] = blockers.get(str(code), 0) + 1
    return dict(sorted(blockers.items(), key=lambda item: item[1], reverse=True)[:10])


def _reason_codes(
    decision: Mapping[str, Any], funnel: Mapping[str, Any]
) -> list[str]:
    reasons: list[str] = []
    primary = funnel.get("primary_blocker")
    if primary:
        reasons.append(str(primary))
    for key in ("rejection_reasons", "block_reasons"):
        values = decision.get(key)
        if isinstance(values, (list, tuple)):
            reasons.extend(str(value) for value in values)
    evaluations = funnel.get("candidate_evaluations")
    if isinstance(evaluations, (list, tuple)):
        for row in evaluations[:10]:
            entry = _mapping(row)
            for key in ("failed_gates", "rejection_reasons", "block_reasons"):
                values = entry.get(key)
                if isinstance(values, (list, tuple)):
                    reasons.extend(str(value) for value in values)
    return list(dict.fromkeys(reasons))[:20]


def _counts(rows: list[Mapping[str, Any]], key: str) -> dict[str, int]:
    counts: dict[str, int] = {}
    for row in rows:
        value = str(row.get(key) or "unknown")
        counts[value] = counts.get(value, 0) + 1
    return dict(sorted(counts.items(), key=lambda item: item[1], reverse=True))


def _mapping(value: object) -> Mapping[str, Any]:
    return value if isinstance(value, Mapping) else {}


def _utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _json(line: str) -> dict[str, Any] | None:
    import json

    try:
        parsed = json.loads(line)
    except ValueError:
        return None
    return dict(parsed) if isinstance(parsed, dict) else None
