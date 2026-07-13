"""Realtime wall/flip event to option repricing orchestration."""

from __future__ import annotations

import json
import os
from dataclasses import asdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Mapping
from zoneinfo import ZoneInfo

from spx_spark.application.order_map.candidates import build_level_trigger_candidates
from spx_spark.application.order_map.touch_time_model import estimate_touch_time
from spx_spark.config import StorageSettings
from spx_spark.options_map import build_options_map
from spx_spark.settings.order_map import DEFAULT_ORDER_MAP_POLICY, OrderMapPolicy
from spx_spark.state_io import atomic_write_json_secure, exclusive_state_lock
from spx_spark.storage import LatestStateStore


REPRICING_PHASES = frozenset(
    {
        "testing",
        "break_pending",
        "reject_pending",
        "accepted",
        "rejected",
        "retest",
        "confirmed",
    }
)


def run_level_trigger_repricing(
    storage: StorageSettings,
    level_decision: Mapping[str, object],
    *,
    now: datetime,
    policy: OrderMapPolicy = DEFAULT_ORDER_MAP_POLICY,
) -> dict[str, object]:
    """Reprice active state-machine paths from the latest option quotes."""

    phase = str(level_decision.get("phase") or "far")
    event_id = str(level_decision.get("event_id") or "")
    level = _number(level_decision.get("level"))
    level_kind = str(level_decision.get("level_kind") or "")
    if phase not in REPRICING_PHASES or not event_id or level is None or not level_kind:
        return {"status": "idle", "phase": phase, "candidate_count": 0}

    state = LatestStateStore(storage).load(now=now)
    options_map = build_options_map(state)
    feature_context = _feature_context(storage)
    coordinate = level_decision.get("trigger_coordinate")
    coordinate_map = coordinate if isinstance(coordinate, Mapping) else {}
    observed = _number(coordinate_map.get("observed_value"))
    target = _number(coordinate_map.get("target_value"))
    expected_move = (
        _number(options_map.expiries[0].expected_move_points) if options_map.expiries else None
    )
    distance_over_em = (
        abs(observed - target) / expected_move
        if observed is not None and target is not None and expected_move
        else None
    )
    touch_estimate = estimate_touch_time(
        storage.data_root,
        distance_over_em=distance_over_em,
        session_bucket=_session_bucket(now),
        volatility_regime=str(feature_context.get("volatility_regime") or "unknown"),
        trend_regime=str(feature_context.get("trend_regime") or "unknown"),
    )
    empirical_fractions = (
        (
            float(touch_estimate.early_fraction),
            float(touch_estimate.base_fraction),
            float(touch_estimate.late_fraction),
        )
        if touch_estimate.calibrated
        and touch_estimate.early_fraction is not None
        and touch_estimate.base_fraction is not None
        and touch_estimate.late_fraction is not None
        else None
    )
    candidates, warnings = build_level_trigger_candidates(
        state,
        options_map,
        level=level,
        level_kind=level_kind,
        phase=phase,
        thesis=str(level_decision.get("thesis") or "none"),
        direction=(
            str(level_decision.get("direction"))
            if level_decision.get("direction") in {"up", "down"}
            else None
        ),
        now=now,
        policy=policy,
        empirical_touch_fractions=empirical_fractions,
        touch_time_model_source=touch_estimate.source,
    )
    payload: dict[str, object] = {
        "schema_version": 1,
        "kind": "level_trigger_repricing",
        "status": "repriced" if candidates else "blocked",
        "event_id": event_id,
        "phase": phase,
        "thesis": level_decision.get("thesis"),
        "direction": level_decision.get("direction"),
        "level_kind": level_kind,
        "spx_level": level,
        "trigger_coordinate": dict(coordinate) if isinstance(coordinate, Mapping) else {},
        "touch_time_estimate": touch_estimate.to_dict(),
        "as_of": _utc(now).isoformat(),
        "market_state_as_of": state.as_of.isoformat(),
        "expiry": options_map.expiries[0].expiry if options_map.expiries else None,
        "expected_move_points": (
            options_map.expiries[0].expected_move_points if options_map.expiries else None
        ),
        "pricing_spot": options_map.underlier.price,
        "trend_regime": feature_context.get("trend_regime"),
        "volatility_regime": feature_context.get("volatility_regime"),
        "candidates": [asdict(candidate) for candidate in candidates],
        "warnings": warnings,
        "executable_candidate_count": sum(
            candidate.execution_quote_status == "executable" for candidate in candidates
        ),
        "range_only_candidate_count": sum(
            candidate.execution_quote_status != "executable" for candidate in candidates
        ),
    }
    latest_path = default_level_trigger_repricing_path(storage)
    with exclusive_state_lock(latest_path):
        atomic_write_json_secure(latest_path, payload)
        _append_jsonl(_audit_path(storage, now), payload)
    return payload


def default_level_trigger_repricing_path(storage: StorageSettings) -> Path:
    return Path(storage.data_root) / "latest" / "level_trigger_repricing.json"


def _audit_path(storage: StorageSettings, now: datetime) -> Path:
    day = now.astimezone(timezone.utc).date().isoformat()
    return (
        Path(storage.data_root)
        / "features"
        / "level_trigger_repricing"
        / f"date={day}"
        / "events.jsonl"
    )


def _append_jsonl(path: Path, payload: Mapping[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fd = os.open(path, os.O_APPEND | os.O_CREAT | os.O_WRONLY, 0o600)
    try:
        os.write(fd, (json.dumps(payload, ensure_ascii=False, sort_keys=True) + "\n").encode())
        os.fsync(fd)
    finally:
        os.close(fd)


def _number(value: object) -> float | None:
    if not isinstance(value, int | float):
        return None
    parsed = float(value)
    return parsed if parsed > 0 and parsed == parsed else None


def _feature_context(storage: StorageSettings) -> dict[str, object]:
    root = Path(storage.data_root) / "latest"
    decision = _load_json(root / "decision_context.json")
    market = _load_json(root / "minute_market_frame.json")
    trend = decision.get("trend") if isinstance(decision.get("trend"), Mapping) else {}
    volatility = market.get("volatility") if isinstance(market.get("volatility"), Mapping) else {}
    ratio = _number(volatility.get("vix1d_vix_ratio"))
    vol_regime = "unknown"
    if ratio is not None:
        vol_regime = "stressed" if ratio >= 1.0 else "normal" if ratio >= 0.8 else "quiet"
    return {
        "trend_regime": str(trend.get("regime") or "unknown"),
        "volatility_regime": vol_regime,
    }


def _load_json(path: Path) -> dict[str, object]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    return payload if isinstance(payload, dict) else {}


def _utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        raise ValueError("repricing timestamp must be timezone-aware")
    return value.astimezone(timezone.utc)


def _session_bucket(now: datetime) -> str:
    local = now.astimezone(ZoneInfo("America/New_York"))
    minutes = local.hour * 60 + local.minute
    if minutes < 180:
        return "globex_early"
    if minutes < 480:
        return "europe"
    if minutes < 570:
        return "us_premarket"
    if minutes < 660:
        return "rth_open"
    if minutes < 870:
        return "rth_midday"
    return "rth_close"
