"""Automatic first-touch and option-price outcome attribution."""

from __future__ import annotations

import json
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Mapping
from zoneinfo import ZoneInfo

from spx_spark.config import StorageSettings
from spx_spark.state_io import atomic_write_json_secure, exclusive_state_lock
from spx_spark.storage import LatestStateStore, configured_quote_use_decision


HORIZONS_SECONDS = (60, 300, 900)
ET = ZoneInfo("America/New_York")


def advance_pricing_outcomes(
    storage: StorageSettings,
    repricing: Mapping[str, object],
    level_decision: Mapping[str, object],
    *,
    now: datetime,
) -> dict[str, object]:
    """Seed active repricings and update every open outcome from live quotes."""

    path = default_pricing_outcome_state_path(storage)
    with exclusive_state_lock(path):
        persisted = _load(path)
        open_rows = dict(persisted.get("open") or {})
        _seed(open_rows, repricing, now=now)
        state = LatestStateStore(storage).load(now=now)
        coordinate = level_decision.get("trigger_coordinate")
        completed: list[dict[str, object]] = []
        for key, raw in list(open_rows.items()):
            if not isinstance(raw, dict):
                open_rows.pop(key, None)
                continue
            result = _advance_row(raw, state, coordinate, now=now)
            if result is not None:
                completed.append(result)
                open_rows.pop(key, None)
        payload = {
            "schema_version": 1,
            "open": open_rows,
            "updated_at": _utc(now).isoformat(),
        }
        atomic_write_json_secure(path, payload)
        for row in completed:
            _append_jsonl(_outcome_path(storage, now), row)
    return {
        "status": "updated",
        "open_count": len(open_rows),
        "completed_count": len(completed),
    }


def default_pricing_outcome_state_path(storage: StorageSettings) -> Path:
    return Path(storage.data_root) / "latest" / "level_trigger_pricing_outcomes.json"


def _seed(open_rows: dict[str, object], repricing: Mapping[str, object], *, now: datetime) -> None:
    if repricing.get("status") not in {"repriced", "blocked"}:
        return
    event_id = str(repricing.get("event_id") or "")
    coordinate = repricing.get("trigger_coordinate")
    candidates = repricing.get("candidates")
    if not event_id or not isinstance(coordinate, Mapping) or not isinstance(candidates, list):
        return
    target = _number(coordinate.get("target_value"))
    observed = _number(coordinate.get("observed_value"))
    em = _number(repricing.get("expected_move_points"))
    if target is None or observed is None:
        return
    for candidate in candidates:
        if not isinstance(candidate, Mapping):
            continue
        contract_id = str(candidate.get("contract_id") or "")
        play = str(candidate.get("play") or "")
        if not contract_id or not play:
            continue
        key = f"{event_id}|{play}|{contract_id}"
        if key in open_rows:
            continue
        reference = _number(candidate.get("limit_aggressive"))
        open_rows[key] = {
            "schema_version": 1,
            "key": key,
            "event_id": event_id,
            "play": play,
            "contract_id": contract_id,
            "right": candidate.get("right"),
            "level_kind": repricing.get("level_kind"),
            "spx_level": repricing.get("spx_level"),
            "trigger_coordinate_kind": coordinate.get("kind"),
            "trigger_instrument_id": coordinate.get("instrument_id"),
            "trigger_target": target,
            "initial_trigger_value": observed,
            "last_trigger_value": observed,
            "initial_distance_points": abs(observed - target),
            "distance_over_em": abs(observed - target) / em if em else None,
            "expected_move_points": em,
            "session_bucket": _session_bucket(now),
            "session_date": now.astimezone(ET).date().isoformat(),
            "trend_regime": repricing.get("trend_regime"),
            "volatility_regime": repricing.get("volatility_regime"),
            "started_at": _utc(now).isoformat(),
            "model_reference_price": reference,
            "model_price_range_low": candidate.get("projection_range_low"),
            "model_price_range_high": candidate.get("projection_range_high"),
            "tau_start_minutes": candidate.get("projection_tau_now_minutes"),
            "prefill_before_touch": False,
            "touched": False,
            "max_mid_after_touch": None,
            "min_mid_after_touch": None,
            "horizons": {},
        }


def _advance_row(
    row: dict[str, object],
    state,
    coordinate: object,
    *,
    now: datetime,
) -> dict[str, object] | None:
    quote = next(
        (
            item
            for item in state.best_quotes
            if item.instrument.canonical_id == row.get("contract_id")
        ),
        None,
    )
    mid = quote.mid if quote is not None else None
    ask = quote.ask if quote is not None else None
    if quote is not None and not configured_quote_use_decision(quote, as_of=now).research_usable:
        mid = None
        ask = None

    observed = None
    if isinstance(coordinate, Mapping) and (
        coordinate.get("kind") == row.get("trigger_coordinate_kind")
        and coordinate.get("instrument_id") == row.get("trigger_instrument_id")
    ):
        observed = _number(coordinate.get("observed_value"))
    target = _number(row.get("trigger_target"))
    previous = _number(row.get("last_trigger_value"))
    reference = _number(row.get("model_reference_price"))
    if not row.get("touched") and ask is not None and reference is not None and ask <= reference:
        row["prefill_before_touch"] = True
        row["prefill_at"] = _utc(now).isoformat()
        row["prefill_ask"] = ask
    if observed is not None and target is not None:
        touched_now = abs(observed - target) <= 0.5 or (
            previous is not None and (previous - target) * (observed - target) <= 0
        )
        row["last_trigger_value"] = observed
        if touched_now and not row.get("touched"):
            row["touched"] = True
            row["first_touch_at"] = _utc(now).isoformat()
            row["first_touch_value"] = observed
            row["touch_mid"] = mid
            row["model_error_fraction"] = (
                mid / reference - 1.0 if mid is not None and reference else None
            )
            started = _datetime(row.get("started_at"))
            tau_minutes = _number(row.get("tau_start_minutes"))
            row["actual_touch_minutes"] = (
                (now - started).total_seconds() / 60.0 if started is not None else None
            )
            row["actual_touch_fraction"] = (
                (now - started).total_seconds() / 60.0 / tau_minutes
                if started is not None and tau_minutes and tau_minutes > 0
                else None
            )
    if row.get("touched") and mid is not None:
        maximum = _number(row.get("max_mid_after_touch"))
        minimum = _number(row.get("min_mid_after_touch"))
        row["max_mid_after_touch"] = mid if maximum is None else max(maximum, mid)
        row["min_mid_after_touch"] = mid if minimum is None else min(minimum, mid)
        _fill_horizons(row, mid=mid, now=now)
    if _complete(row, now=now):
        row["completed_at"] = _utc(now).isoformat()
        row["outcome_status"] = "complete" if row.get("touched") else "expired_untouched"
        return dict(row)
    return None


def _fill_horizons(row: dict[str, object], *, mid: float, now: datetime) -> None:
    touch_at = _datetime(row.get("first_touch_at"))
    touch_mid = _number(row.get("touch_mid"))
    horizons = row.get("horizons")
    if touch_at is None or touch_mid is None or not isinstance(horizons, dict):
        return
    elapsed = (now - touch_at).total_seconds()
    maximum = _number(row.get("max_mid_after_touch")) or mid
    minimum = _number(row.get("min_mid_after_touch")) or mid
    for seconds in HORIZONS_SECONDS:
        key = str(seconds)
        if elapsed >= seconds and key not in horizons:
            horizons[key] = {
                "sampled_at": _utc(now).isoformat(),
                "end_mid": mid,
                "return_fraction": mid / touch_mid - 1.0,
                "mfe_fraction": maximum / touch_mid - 1.0,
                "mae_fraction": minimum / touch_mid - 1.0,
            }


def _complete(row: Mapping[str, object], *, now: datetime) -> bool:
    if row.get("touched"):
        horizons = row.get("horizons")
        return isinstance(horizons, Mapping) and str(HORIZONS_SECONDS[-1]) in horizons
    started = _datetime(row.get("started_at"))
    return started is not None and (now - started).total_seconds() >= 6 * 3600


def _session_bucket(now: datetime) -> str:
    local = now.astimezone(ET)
    minutes = local.hour * 60 + local.minute
    if minutes < 3 * 60:
        return "globex_early"
    if minutes < 8 * 60:
        return "europe"
    if minutes < 9 * 60 + 30:
        return "us_premarket"
    if minutes < 11 * 60:
        return "rth_open"
    if minutes < 14 * 60 + 30:
        return "rth_midday"
    return "rth_close"


def _load(path: Path) -> dict[str, object]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    return payload if isinstance(payload, dict) else {}


def _outcome_path(storage: StorageSettings, now: datetime) -> Path:
    day = now.astimezone(ET).date().isoformat()
    return (
        Path(storage.data_root) / "features" / "pricing_outcomes" / f"date={day}" / "outcomes.jsonl"
    )


def _append_jsonl(path: Path, payload: Mapping[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fd = os.open(path, os.O_APPEND | os.O_CREAT | os.O_WRONLY, 0o600)
    try:
        os.write(fd, (json.dumps(payload, sort_keys=True) + "\n").encode())
        os.fsync(fd)
    finally:
        os.close(fd)


def _number(value: object) -> float | None:
    if not isinstance(value, int | float):
        return None
    parsed = float(value)
    return parsed if parsed == parsed else None


def _datetime(value: object) -> datetime | None:
    if not isinstance(value, str):
        return None
    try:
        parsed = datetime.fromisoformat(value)
    except ValueError:
        return None
    return _utc(parsed)


def _utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)
