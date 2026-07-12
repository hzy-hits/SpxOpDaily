from __future__ import annotations

import json
import os
from datetime import datetime
from pathlib import Path

from spx_spark.alert_engine.constants import (
    BASELINE_INSTRUMENTS,
    EM_MOVE_FRACTIONS,
    MOVE_THRESHOLDS_BPS,
)
from spx_spark.alert_engine.rules_data import find_best
from spx_spark.alert_engine.rules_system import hyperliquid_proxy_usable
from spx_spark.alert_model import Alert, severity_for_priority
from spx_spark.alert_profile import AlertWindow, active_window
from spx_spark.config import env_float
from spx_spark.market_calendar import DEFAULT_MARKET_CALENDAR
from spx_spark.market_context import build_market_context
from spx_spark.marketdata import Quote
from spx_spark.options_map import OptionsMap, build_options_map
from spx_spark.settings import DEFAULT_ALERT_SETTINGS
from spx_spark.storage import LatestState, configured_quote_use_decision


def move_from_close_bps(quote: Quote) -> float | None:
    price = quote.effective_price
    close = quote.close
    if price is None or close is None or close <= 0:
        return None
    return (price / close - 1.0) * 10_000.0


def effective_move_threshold_bps(
    priority: str,
    expected_move_pct: float | None,
) -> tuple[float, str]:
    """Return effective movement threshold in bps and its source label."""
    static = MOVE_THRESHOLDS_BPS.get(priority, MOVE_THRESHOLDS_BPS["normal"])
    if expected_move_pct is None or expected_move_pct <= 0:
        return (static, "static")
    fraction = EM_MOVE_FRACTIONS.get(priority, EM_MOVE_FRACTIONS["normal"])
    em_bps = expected_move_pct * 10_000.0 * fraction
    if em_bps > static:
        return (em_bps, "em_normalized")
    if priority == "low":
        floor = env_float(
            "ALERT_MOVE_QUIET_FLOOR_BPS",
            DEFAULT_ALERT_SETTINGS.move_quiet_floor_bps,
        )
        return (max(em_bps, floor), "em_normalized_quiet")
    return (static, "static")


def front_expected_move_pct(options_map: OptionsMap | None, *, as_of: datetime) -> float | None:
    if options_map is None or not options_map.expiries:
        return None
    front = options_map.expiries[0]
    try:
        expiry_date = datetime.strptime(front.expiry, "%Y%m%d").date()
    except ValueError:
        return None
    if expiry_date < DEFAULT_MARKET_CALENDAR.research_expiry(as_of):
        return None
    return front.expected_move_pct


def movement_threshold_for_window(
    window: AlertWindow,
    options_map: OptionsMap | None,
    *,
    as_of: datetime,
) -> tuple[float, str | None, float | None]:
    """Resolve movement threshold; when options_map is None match legacy static behavior."""
    static = MOVE_THRESHOLDS_BPS.get(window.priority, MOVE_THRESHOLDS_BPS["normal"])
    if options_map is None:
        return (static, None, None)
    expected_move_pct = front_expected_move_pct(options_map, as_of=as_of)
    threshold, source = effective_move_threshold_bps(window.priority, expected_move_pct)
    return (threshold, source, expected_move_pct)


def movement_bucket_and_direction(move_bps: float, threshold_bps: float) -> tuple[int, str]:
    direction = "up" if move_bps > 0 else "down"
    if abs(move_bps) < threshold_bps:
        return 0, direction
    return int(abs(move_bps) // threshold_bps), direction


def load_movement_state(path: str | Path) -> dict[str, object]:
    state_path = Path(path)
    try:
        payload = json.loads(state_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    return payload if isinstance(payload, dict) else {}


def save_movement_state(path: str | Path, payload: dict[str, object]) -> None:
    state_path = Path(path)
    state_path.parent.mkdir(parents=True, exist_ok=True)
    temp_path = state_path.with_suffix(f"{state_path.suffix}.tmp")
    temp_path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")
    temp_path.replace(state_path)


def movement_state_path() -> str:
    data_root = os.getenv("MARKET_DATA_DATA_ROOT") or os.getenv("MAINTENANCE_DATA_ROOT") or "data"
    return os.getenv(
        "ALERT_MOVEMENT_STATE_PATH",
        f"{data_root.rstrip('/')}/latest/movement_state.json",
    )


def parse_movement_instrument_state(value: object) -> dict[str, object] | None:
    if not isinstance(value, dict):
        return None
    bucket = value.get("bucket")
    direction = value.get("direction")
    if not isinstance(bucket, int) or direction not in {"up", "down"}:
        return None
    return {"bucket": bucket, "direction": str(direction)}


def build_movement_state_payload(
    state: LatestState,
    *,
    window: AlertWindow,
    market_context: dict[str, object] | None,
    options_map: OptionsMap | None = None,
) -> dict[str, object]:
    threshold, _threshold_source, _expected_move_pct = movement_threshold_for_window(
        window,
        options_map,
        as_of=state.as_of,
    )
    instruments: dict[str, object] = {}
    for instrument_id in BASELINE_INSTRUMENTS:
        if instrument_id.startswith("crypto_perp:") and not hyperliquid_proxy_usable(
            market_context
        ):
            continue
        quote = find_best(state, instrument_id)
        if (
            quote is None
            or not configured_quote_use_decision(quote, as_of=state.as_of).alert_allowed
        ):
            continue
        move_bps = move_from_close_bps(quote)
        if move_bps is None:
            continue
        bucket, direction = movement_bucket_and_direction(move_bps, threshold)
        if bucket >= 1:
            instruments[instrument_id] = {"bucket": bucket, "direction": direction}
    return {
        "instruments": instruments,
        "updated_at": state.as_of.isoformat(),
    }


def persist_movement_state_snapshot(
    state: LatestState,
    *,
    window: AlertWindow | None = None,
    market_context: dict[str, object] | None = None,
    options_map: OptionsMap | None = None,
) -> None:
    window = window or active_window(state.as_of)
    if market_context is None:
        market_context = build_market_context(state)
    if options_map is None:
        options_map = build_options_map(state)
    save_movement_state(
        movement_state_path(),
        build_movement_state_payload(
            state,
            window=window,
            market_context=market_context,
            options_map=options_map,
        ),
    )


def movement_alerts(
    state: LatestState,
    *,
    window: AlertWindow,
    market_context: dict[str, object] | None,
    persist: bool = False,
    options_map: OptionsMap | None = None,
) -> list[Alert]:
    threshold, threshold_source, expected_move_pct = movement_threshold_for_window(
        window,
        options_map,
        as_of=state.as_of,
    )
    state_path = movement_state_path()
    previous_payload = load_movement_state(state_path)
    previous_instruments = previous_payload.get("instruments")
    if not isinstance(previous_instruments, dict):
        previous_instruments = {}

    alerts: list[Alert] = []
    new_instruments: dict[str, object] = {}
    for instrument_id in BASELINE_INSTRUMENTS:
        if instrument_id.startswith("crypto_perp:") and not hyperliquid_proxy_usable(
            market_context
        ):
            continue
        quote = find_best(state, instrument_id)
        if (
            quote is None
            or not configured_quote_use_decision(quote, as_of=state.as_of).alert_allowed
        ):
            continue
        move_bps = move_from_close_bps(quote)
        if move_bps is None:
            continue
        bucket, direction = movement_bucket_and_direction(move_bps, threshold)
        if bucket == 0:
            continue
        previous = parse_movement_instrument_state(previous_instruments.get(instrument_id))
        should_alert = (
            previous is None
            or bucket > int(previous["bucket"])
            or direction != str(previous["direction"])
        )
        new_instruments[instrument_id] = {"bucket": bucket, "direction": direction}
        if not should_alert:
            continue
        detail = (
            f"{instrument_id} effective price moved {move_bps:.1f} bps from close "
            f"during {window.name}."
        )
        if threshold_source is not None:
            detail = (
                f"{detail} threshold_bps={threshold:.1f} "
                f"threshold_source={threshold_source} "
                f"expected_move_pct={expected_move_pct}"
            )
        severity = severity_for_priority(window.priority)
        if expected_move_pct is not None and expected_move_pct > 0:
            escalation_fraction = env_float(
                "ALERT_MOVE_HIGH_SEVERITY_EM_FRACTION",
                DEFAULT_ALERT_SETTINGS.move_high_severity_em_fraction,
            )
            em_day_bps = expected_move_pct * 10_000.0
            if abs(move_bps) >= em_day_bps * escalation_fraction and severity in (
                "info",
                "low",
                "medium",
            ):
                severity = "high"
                detail = (
                    f"{detail} em_consumed={abs(move_bps) / em_day_bps:.0%}"
                    " (escalated: move consumed most of the day's expected move)"
                )
        alerts.append(
            Alert(
                severity=severity,
                kind="price_move_from_close",
                instrument_id=instrument_id,
                title=f"{instrument_id} {direction} {move_bps:.1f} bps from close",
                detail=detail,
                provider=quote.provider.value,
                quality=quote.quality.value,
                value=move_bps,
                threshold=threshold,
                dedup_group=f"{direction}:{bucket}",
            )
        )

    if persist:
        save_movement_state(
            state_path,
            {
                "instruments": new_instruments,
                "updated_at": state.as_of.isoformat(),
            },
        )
    return alerts


