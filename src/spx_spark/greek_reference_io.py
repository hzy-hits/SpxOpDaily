"""Persistence and session reporting for the Greek reference calculator."""

from __future__ import annotations

import fcntl
import json
import os
from collections.abc import Iterable, Mapping
from pathlib import Path
from typing import Any

from spx_spark.greek_reference import AGGREGATE_METRICS, SCHEMA_VERSION, _expiry_date


def write_zero_dte_greeks_snapshot(
    payload: Mapping[str, Any],
    *,
    data_root: str | Path,
) -> dict[str, str] | None:
    """Persist a versioned snapshot under a cross-process lock."""

    if payload.get("schema_version") != SCHEMA_VERSION:
        return None
    if payload.get("status") not in {"ok", "degraded", "unavailable"}:
        return None
    expiry = str(payload.get("expiry") or "")
    if _expiry_date(expiry) is None:
        return None

    root = Path(data_root)
    raw_path = (
        root
        / "features"
        / "spxw_0dte_greeks_reference"
        / f"date={expiry[:4]}-{expiry[4:6]}-{expiry[6:8]}"
        / "snapshots.jsonl"
    )
    latest_path = root / "latest" / "spxw_0dte_greeks_reference.json"
    lock_path = root / "latest" / "spxw_0dte_greeks_reference.lock"
    serialized = json.dumps(dict(payload), sort_keys=True, separators=(",", ":"))
    raw_path.parent.mkdir(parents=True, exist_ok=True)
    latest_path.parent.mkdir(parents=True, exist_ok=True)
    with lock_path.open("a+", encoding="utf-8") as lock:
        fcntl.flock(lock.fileno(), fcntl.LOCK_EX)
        try:
            with raw_path.open("a", encoding="utf-8") as handle:
                handle.write(serialized)
                handle.write("\n")

            current_as_of = ""
            try:
                current = json.loads(latest_path.read_text(encoding="utf-8"))
                if isinstance(current, dict) and isinstance(current.get("as_of"), str):
                    current_as_of = str(current["as_of"])
            except (OSError, json.JSONDecodeError):
                pass
            incoming_as_of = str(payload.get("as_of") or "")
            if not current_as_of or incoming_as_of >= current_as_of:
                temporary = latest_path.with_name(f".{latest_path.name}.{os.getpid()}.tmp")
                temporary.write_text(serialized, encoding="utf-8")
                temporary.replace(latest_path)
        finally:
            fcntl.flock(lock.fileno(), fcntl.LOCK_UN)
    return {"raw_path": str(raw_path), "latest_path": str(latest_path)}


def load_zero_dte_greeks_snapshots(
    *,
    data_root: str | Path,
    trading_date: str,
) -> tuple[dict[str, Any], ...]:
    expiry = trading_date.replace("-", "")
    path = (
        Path(data_root)
        / "features"
        / "spxw_0dte_greeks_reference"
        / f"date={trading_date}"
        / "snapshots.jsonl"
    )
    if not path.exists():
        return ()
    by_as_of: dict[str, dict[str, Any]] = {}
    try:
        lines = path.read_text(encoding="utf-8").splitlines()
    except OSError:
        return ()
    for line in lines:
        try:
            row = json.loads(line)
        except json.JSONDecodeError:
            continue
        if not isinstance(row, dict):
            continue
        if row.get("schema_version") != SCHEMA_VERSION or row.get("expiry") != expiry:
            continue
        as_of = row.get("as_of")
        if isinstance(as_of, str) and as_of:
            by_as_of[as_of] = row
    return tuple(by_as_of[key] for key in sorted(by_as_of))


def summarize_zero_dte_greeks_session(
    snapshots: Iterable[Mapping[str, Any]],
    *,
    expiry: str,
) -> dict[str, Any]:
    rows = sorted(
        (
            dict(row)
            for row in snapshots
            if row.get("schema_version") == SCHEMA_VERSION
            and row.get("expiry") == expiry
            and row.get("status") in {"ok", "degraded", "unavailable"}
            and isinstance(row.get("as_of"), str)
        ),
        key=lambda row: str(row["as_of"]),
    )
    if not rows:
        return {
            "schema_version": SCHEMA_VERSION,
            "kind": "session_summary",
            "mode": "reference_only",
            "status": "unavailable",
            "expiry": expiry,
            "direction": "unknown",
            "position_sign": "unknown",
            "snapshot_count": 0,
            "metrics": {},
            "warnings": [],
        }

    usable_rows = [row for row in rows if row.get("status") in {"ok", "degraded"}]
    by_universe: dict[str, list[dict[str, Any]]] = {}
    for row in usable_rows:
        universe = row.get("aggregate_universe")
        fingerprint = (
            str(universe.get("fingerprint"))
            if isinstance(universe, Mapping) and universe.get("fingerprint")
            else f"missing:{row['as_of']}"
        )
        by_universe.setdefault(fingerprint, []).append(row)
    comparison_fingerprint = None
    comparison_rows: list[dict[str, Any]] = []
    if by_universe:
        comparison_fingerprint, comparison_rows = max(
            by_universe.items(),
            key=lambda item: (len(item[1]), str(item[1][-1]["as_of"])),
        )

    metric_rows: dict[str, dict[str, float]] = {}
    if len(comparison_rows) >= 2:
        for name in AGGREGATE_METRICS:
            values = [
                float(aggregate[name])
                for row in comparison_rows
                if isinstance(aggregate := row.get("aggregate"), Mapping)
                and isinstance(aggregate.get(name), int | float)
            ]
            if values:
                metric_rows[name] = {"first": values[0], "last": values[-1], "peak": max(values)}
    quality_counts = {
        status: sum(1 for row in rows if row.get("status") == status)
        for status in ("ok", "degraded", "unavailable")
    }
    usable_ratios = [
        float(coverage["usable_ratio"])
        for row in usable_rows
        if isinstance(coverage := row.get("coverage"), Mapping)
        and isinstance(coverage.get("usable_ratio"), int | float)
    ]
    oi_ratios = [
        float(coverage["oi_ratio"])
        for row in usable_rows
        if isinstance(coverage := row.get("coverage"), Mapping)
        and isinstance(coverage.get("oi_ratio"), int | float)
    ]

    def coverage_change(values: list[float]) -> dict[str, float] | None:
        if not values:
            return None
        return {"first": values[0], "last": values[-1], "min": min(values)}

    blocked_counts: dict[str, int] = {}
    quality_reason_counts: dict[str, int] = {}
    for row in usable_rows:
        for target, source_name in (
            (blocked_counts, "blocked_counts"),
            (quality_reason_counts, "quality_reason_counts"),
        ):
            source = row.get(source_name)
            if not isinstance(source, Mapping):
                continue
            for reason, count in source.items():
                if isinstance(count, int | float):
                    target[str(reason)] = target.get(str(reason), 0) + int(count)
    summary_degraded = (
        quality_counts["degraded"] > 0
        or quality_counts["unavailable"] > 0
        or len(comparison_rows) < 2
    )
    return {
        "schema_version": SCHEMA_VERSION,
        "kind": "session_summary",
        "mode": "reference_only",
        "status": "degraded" if summary_degraded else "ok",
        "expiry": expiry,
        "direction": "unknown",
        "position_sign": "unknown",
        "snapshot_count": len(rows),
        "usable_snapshot_count": len(usable_rows),
        "comparison_snapshot_count": len(comparison_rows),
        "comparison_universe_fingerprint": comparison_fingerprint,
        "aggregate_universe_count": len(by_universe),
        "first_as_of": rows[0]["as_of"],
        "last_as_of": rows[-1]["as_of"],
        "quality_counts": quality_counts,
        "coverage": {
            "usable_ratio": coverage_change(usable_ratios),
            "oi_ratio": coverage_change(oi_ratios),
        },
        "blocked_counts": blocked_counts,
        "quality_reason_counts": quality_reason_counts,
        "metrics": metric_rows,
        "warnings": sorted(
            {str(warning) for row in rows for warning in (row.get("warnings") or ())}
        ),
    }
