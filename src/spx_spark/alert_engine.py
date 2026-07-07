from __future__ import annotations

import argparse
import json
import os
from dataclasses import replace
from datetime import datetime
from pathlib import Path

from spx_spark.alert_model import Alert, severity_for_priority
from spx_spark.alert_profile import AlertWindow, active_window, parse_at
from spx_spark.config import (
    IvSurfaceSettings,
    NotificationSettings,
    StorageSettings,
    NY_TZ,
    env_bool,
    env_float,
)
from spx_spark.human_focus import build_human_focus_context
from spx_spark.iv_surface import (
    IvSurfaceSnapshot,
    load_latest_snapshot,
    load_recent_snapshots,
    summarize_surface_history,
)
from spx_spark.market_context import build_market_context
from spx_spark.marketdata import MarketDataQuality, Provider, ProviderState, ProviderStatus, Quote
from spx_spark.notifier import notify_payload
from spx_spark.options_map import OptionsMap, build_options_map
from spx_spark.position_alerts import position_holdings_alerts
from spx_spark.storage import LatestState, LatestStateStore


BASELINE_INSTRUMENTS = (
    "index:SPX",
    "index:VIX",
    "index:VIX1D",
    "index:VIX9D",
    "index:VIX3M",
    "index:VVIX",
    "index:SKEW",
    "index:NDX",
    "index:RUT",
    "index:DJX",
    "index:DJU",
    "equity:SPY",
    "equity:QQQ",
    "equity:IWM",
    "equity:DIA",
    "equity:HYG",
    "equity:LQD",
    "equity:TLT",
    "equity:IEF",
    "equity:SHY",
    "equity:UUP",
    "equity:GLD",
    "equity:USO",
    "equity:RSP",
    "equity:XLU",
    "future:ES",
    "future:MES",
    "crypto_perp:xyz:SP500",
)

MOVE_THRESHOLDS_BPS = {
    "critical": 20.0,
    "high": 30.0,
    "elevated": 45.0,
    "normal": 60.0,
    "low": 85.0,
    "off": 99999.0,
}

EM_MOVE_FRACTIONS = {
    "critical": 0.20,
    "high": 0.30,
    "elevated": 0.40,
    "normal": 0.50,
    "low": 0.70,
    "off": 9.0,
}

BAD_QUALITIES = {
    MarketDataQuality.MISSING,
    MarketDataQuality.ERROR,
    MarketDataQuality.STALE,
    MarketDataQuality.UNKNOWN,
}

OPTION_GAMMA_ALERT_STATES = {
    "negative_gamma_acceleration",
    "zero_gamma_transition",
}

BAD_SURFACE_QUALITIES = {"missing_options", "missing_atm_iv", "low_iv_coverage", "wide_quote_degraded"}
BLOCKING_SURFACE_QUALITIES = {"missing_options", "missing_atm_iv"}
DEGRADED_SURFACE_QUALITIES = {"low_iv_coverage", "wide_quote_degraded"}
ATM_IV_JUMP_THRESHOLD = 0.03
SKEW_STEEPENING_THRESHOLD = 0.08
SKEW_25D_STEEPENING_THRESHOLD = 0.02
SURFACE_SHIFT_THRESHOLD = 0.03
TERM_GAP_THRESHOLD = 0.05
SURFACE_SHIFT_1H_THRESHOLD = 0.05
ATM_IV_CHANGE_1H_THRESHOLD = 0.04
IBKR_INTERRUPTED_SESSION_STATUSES = {"competing_session", "unavailable"}
# Transitional statuses must not overwrite the persisted session status:
# "degraded" is what the stream collector reports between reconnect and the
# first flush, so persisting it would break the interrupted -> available
# transition and swallow the "restored" notification.
IBKR_TRANSITIONAL_SESSION_STATUSES = {"unknown", "degraded"}


def find_best(state: LatestState, instrument_id: str) -> Quote | None:
    return state.best_quote(instrument_id)


def quote_health_alert(
    *,
    instrument_id: str,
    quote: Quote | None,
    window: AlertWindow,
    required: bool,
) -> Alert | None:
    if quote is None:
        severity = severity_for_priority(window.priority) if required else "low"
        return Alert(
            severity=severity,
            kind="required_data_missing" if required else "optional_data_missing",
            instrument_id=instrument_id,
            title=f"{instrument_id} missing",
            detail=f"{instrument_id} has no usable best quote in latest state.",
        )

    if quote.quality in BAD_QUALITIES:
        severity = severity_for_priority(window.priority) if required else "low"
        return Alert(
            severity=severity,
            kind="required_data_degraded" if required else "optional_data_degraded",
            instrument_id=instrument_id,
            title=f"{instrument_id} {quote.quality.value}",
            detail=f"{instrument_id} best quote is {quote.quality.value}.",
            provider=quote.provider.value,
            quality=quote.quality.value,
        )

    return None


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
    return (static, "static")


def front_expected_move_pct(options_map: OptionsMap | None, *, as_of: datetime) -> float | None:
    if options_map is None or not options_map.expiries:
        return None
    front = options_map.expiries[0]
    try:
        expiry_date = datetime.strptime(front.expiry, "%Y%m%d").date()
    except ValueError:
        return None
    if expiry_date < as_of.astimezone(NY_TZ).date():
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
) -> dict[str, object]:
    threshold = MOVE_THRESHOLDS_BPS.get(window.priority, MOVE_THRESHOLDS_BPS["normal"])
    instruments: dict[str, object] = {}
    for instrument_id in BASELINE_INSTRUMENTS:
        if instrument_id.startswith("crypto_perp:") and not hyperliquid_proxy_usable(market_context):
            continue
        quote = find_best(state, instrument_id)
        if quote is None or quote.quality in BAD_QUALITIES:
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
) -> None:
    window = window or active_window(state.as_of)
    if market_context is None:
        market_context = build_market_context(state)
    save_movement_state(
        movement_state_path(),
        build_movement_state_payload(state, window=window, market_context=market_context),
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
        if instrument_id.startswith("crypto_perp:") and not hyperliquid_proxy_usable(market_context):
            continue
        quote = find_best(state, instrument_id)
        if quote is None or quote.quality in BAD_QUALITIES:
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
        alerts.append(
            Alert(
                severity=severity_for_priority(window.priority),
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


def evaluate_alerts(
    state: LatestState,
    *,
    window: AlertWindow,
    options_map: OptionsMap | None = None,
    iv_surface: IvSurfaceSnapshot | None = None,
    iv_surface_history_1h: dict[str, object] | None = None,
    market_context: dict[str, object] | None = None,
    persist_system_events: bool = False,
    persist_movement_state: bool = False,
) -> list[Alert]:
    alerts: list[Alert] = []
    required = set(window.required_instruments)
    optional = set(window.optional_instruments)
    for instrument_id in sorted(required | optional):
        alert = quote_health_alert(
            instrument_id=instrument_id,
            quote=find_best(state, instrument_id),
            window=window,
            required=instrument_id in required,
        )
        if alert is not None:
            alerts.append(alert)

    if market_context is None:
        market_context = build_market_context(state)
    alerts.extend(
        movement_alerts(
            state,
            window=window,
            market_context=market_context,
            persist=persist_movement_state,
            options_map=options_map,
        )
    )

    alerts.extend(option_map_alerts(options_map or build_options_map(state), window=window))
    if iv_surface is not None:
        alerts.extend(
            iv_surface_alerts(
                iv_surface,
                window=window,
                history_1h=iv_surface_history_1h,
            )
        )
    alerts.extend(position_holdings_alerts(state, options_map=options_map, window=window))
    alerts.extend(market_context_alerts(market_context))
    alerts.extend(system_event_alerts(state, persist=persist_system_events))
    alerts.extend(proxy_fallback_watch_alerts(state, window=window, market_context=market_context, options_map=options_map))
    return alerts


def option_coverage_is_fresh(expiry: object) -> bool:
    coverage = getattr(expiry, "coverage", None)
    if coverage is None or coverage.total <= 0:
        return False
    min_live_ratio = env_float("ALERT_MIN_OPTION_LIVE_RATIO", 0.5)
    if coverage.live / coverage.total < min_live_ratio:
        return False
    max_age_ms = coverage.max_age_ms
    if max_age_ms is not None and max_age_ms > env_float("ALERT_MAX_OPTION_QUOTE_AGE_MS", 20_000.0):
        return False
    if env_bool("ALERT_REQUIRE_OPTION_QUOTE_TIMESTAMPS", False):
        known_ratio = (coverage.total - coverage.unknown_age) / coverage.total
        if known_ratio < 0.75:
            return False
    return True


def option_freshness_alert(expiry: object, *, window: AlertWindow) -> Alert:
    coverage = getattr(expiry, "coverage")
    expiry_id = getattr(expiry, "expiry")
    live_ratio = coverage.live / max(coverage.total, 1)
    return Alert(
        severity="medium" if window.priority not in {"low", "off"} else "low",
        kind="option_quote_freshness_degraded",
        instrument_id=f"option_map:SPXW:{expiry_id}",
        title=f"SPXW {expiry_id} quote freshness degraded",
        detail=(
            f"SPXW {expiry_id} live ratio={live_ratio:.2f}, stale={coverage.stale}, "
            f"max_age_ms={coverage.max_age_ms}; wall/gamma alerts are suppressed."
        ),
        quality="degraded",
        value=live_ratio,
        threshold=env_float("ALERT_MIN_OPTION_LIVE_RATIO", 0.5),
    )


def option_map_alerts(options_map: OptionsMap, *, window: AlertWindow) -> list[Alert]:
    alerts: list[Alert] = []
    underlier = options_map.underlier.price
    wall_threshold = max(10.0, underlier * 0.002 if underlier else 10.0)
    for expiry in options_map.expiries:
        if not option_coverage_is_fresh(expiry):
            alerts.append(option_freshness_alert(expiry, window=window))
            continue
        if expiry.gamma_state in OPTION_GAMMA_ALERT_STATES:
            gamma_detail = (
                f"SPXW {expiry.expiry} gamma state is {expiry.gamma_state}; "
                f"zero gamma={expiry.zero_gamma}, net_gamma_ratio={expiry.net_gamma_ratio}."
            )
            if expiry.gamma_flip_zone is not None:
                left, right = expiry.gamma_flip_zone
                gamma_detail += f" flip_zone={left:.0f}-{right:.0f}."
            alerts.append(
                Alert(
                    severity=severity_for_priority(window.priority),
                    kind="option_gamma_regime",
                    instrument_id=f"option_map:SPXW:{expiry.expiry}",
                    title=f"SPXW {expiry.expiry} {expiry.gamma_state}",
                    detail=gamma_detail,
                    value=expiry.net_gamma_ratio,
                    dedup_group=expiry.gamma_state,
                )
            )
        if expiry.nearest_wall is not None and expiry.nearest_wall_distance_points is not None:
            distance = abs(expiry.nearest_wall_distance_points)
            if distance <= wall_threshold:
                wall_detail = (
                    f"Nearest SPXW wall for {expiry.expiry} is "
                    f"{expiry.nearest_wall:.0f}; threshold={wall_threshold:.1f} pts."
                )
                for lp in expiry.level_probabilities:
                    if (
                        lp.prob_touch is not None
                        and lp.level is not None
                        and abs(lp.level - expiry.nearest_wall) <= 0.01
                    ):
                        wall_detail += (
                            f" touch_prob≈{lp.prob_touch:.0%}, "
                            f"close_beyond≈{lp.prob_close_beyond:.0%}."
                        )
                        break
                alerts.append(
                    Alert(
                        severity=severity_for_priority(window.priority),
                        kind="option_wall_proximity",
                        instrument_id=f"option_map:SPXW:{expiry.expiry}",
                        title=(
                            f"SPX near SPXW wall {expiry.nearest_wall:.0f} "
                            f"({expiry.nearest_wall_distance_points:+.1f} pts)"
                        ),
                        detail=wall_detail,
                        value=expiry.nearest_wall_distance_points,
                        threshold=wall_threshold,
                        dedup_group=f"{expiry.nearest_wall:.0f}",
                    )
                )
    return alerts


def iv_surface_freshness_alert(surface: IvSurfaceSnapshot, *, now: datetime) -> Alert | None:
    max_age_seconds = env_float("ALERT_MAX_IV_SURFACE_AGE_SECONDS", 420.0)
    age_seconds = (now - surface.as_of).total_seconds()
    if age_seconds <= max_age_seconds:
        return None
    return Alert(
        severity="medium",
        kind="iv_surface_stale",
        instrument_id="iv_surface:SPXW",
        title="SPXW IV surface stale",
        detail=(
            f"SPXW IV surface age is {age_seconds:.0f}s; IV-surface alerts are suppressed "
            f"above {max_age_seconds:.0f}s."
        ),
        quality="stale",
        value=age_seconds,
        threshold=max_age_seconds,
    )


def hyperliquid_proxy_gate(market_context: dict[str, object] | None) -> dict[str, object]:
    if not isinstance(market_context, dict):
        return {}
    derived = market_context.get("derived")
    if not isinstance(derived, dict):
        return {}
    gate = derived.get("hyperliquid_spx_proxy")
    return gate if isinstance(gate, dict) else {}


def hyperliquid_proxy_usable(market_context: dict[str, object] | None) -> bool:
    return bool(hyperliquid_proxy_gate(market_context).get("usable_for_alert"))


def market_context_alerts(market_context: dict[str, object] | None) -> list[Alert]:
    gate = hyperliquid_proxy_gate(market_context)
    state = str(gate.get("state") or "")
    if state in {"", "missing", "basis_ok"}:
        return []
    severity = "low" if state == "unanchored_context_only" else "medium"
    return [
        Alert(
            severity=severity,
            kind="hyperliquid_proxy_quality_gate",
            instrument_id=str(gate.get("proxy") or "crypto_perp:xyz:SP500"),
            title=f"Hyperliquid SPX proxy {state}",
            detail=str(gate.get("reason") or "Hyperliquid proxy is not usable for alert scoring."),
            quality=state,
            value=gate.get("basis_bps") if isinstance(gate.get("basis_bps"), (int, float)) else None,
            threshold=gate.get("block_bps") if isinstance(gate.get("block_bps"), (int, float)) else None,
            research_only=True,
            source_gate="hyperliquid_spx_proxy",
        )
    ]


def provider_state_for(state: LatestState, provider: Provider) -> ProviderState | None:
    matches = [item for item in state.provider_states if item.provider == provider]
    if not matches:
        return None
    return max(matches, key=lambda item: item.checked_at)


def provider_state_is_recent(provider_state: ProviderState, *, now: datetime) -> bool:
    max_age_seconds = env_float("ALERT_BROKER_STATE_MAX_AGE_SECONDS", 900.0)
    age_seconds = (now - provider_state.checked_at).total_seconds()
    return 0 <= age_seconds <= max_age_seconds


def ibkr_feed_unavailable_for_fallback(state: LatestState) -> bool:
    provider_state = provider_state_for(state, Provider.IBKR)
    if provider_state is None or not provider_state_is_recent(provider_state, now=state.as_of):
        return False
    if provider_state.status == ProviderStatus.UNAVAILABLE:
        return True
    return provider_state.status == ProviderStatus.DEGRADED and provider_state.connected is not True


def ibkr_session_status(provider_state: ProviderState | None, *, now: datetime) -> str:
    if provider_state is None or not provider_state_is_recent(provider_state, now=now):
        return "unknown"
    reason = (provider_state.reason or "").lower()
    if provider_state.status == ProviderStatus.AVAILABLE:
        return "available"
    if "competing session" in reason or "10197" in reason:
        return "competing_session"
    if provider_state.status == ProviderStatus.UNAVAILABLE:
        return "unavailable"
    if provider_state.status == ProviderStatus.DEGRADED:
        return "degraded"
    return provider_state.status.value


def load_system_event_state(path: str | Path) -> dict[str, object]:
    state_path = Path(path)
    try:
        payload = json.loads(state_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    return payload if isinstance(payload, dict) else {}


def save_system_event_state(path: str | Path, payload: dict[str, object]) -> None:
    state_path = Path(path)
    state_path.parent.mkdir(parents=True, exist_ok=True)
    temp_path = state_path.with_suffix(f"{state_path.suffix}.tmp")
    temp_path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")
    temp_path.replace(state_path)


def system_event_state_path() -> str:
    data_root = os.getenv("MARKET_DATA_DATA_ROOT") or os.getenv("MAINTENANCE_DATA_ROOT") or "data"
    return os.getenv(
        "ALERT_SYSTEM_EVENT_STATE_PATH",
        f"{data_root.rstrip('/')}/latest/system_event_state.json",
    )


def build_system_event_state_payload(
    state: LatestState,
    provider_state: ProviderState,
    current_status: str,
    previous: dict[str, object],
) -> dict[str, object]:
    if current_status in IBKR_TRANSITIONAL_SESSION_STATUSES:
        payload = {
            **previous,
            "ibkr_last_observed_status": current_status,
            "ibkr_checked_at": provider_state.checked_at.isoformat(),
            "updated_at": state.as_of.isoformat(),
        }
        if previous.get("ibkr_session_status") is None:
            payload["ibkr_session_status"] = current_status
        return payload
    return {
        "ibkr_session_status": current_status,
        "ibkr_last_observed_status": current_status,
        "ibkr_checked_at": provider_state.checked_at.isoformat(),
        "updated_at": state.as_of.isoformat(),
    }


def persist_system_event_state(state: LatestState) -> None:
    provider_state = provider_state_for(state, Provider.IBKR)
    if provider_state is None:
        return
    current_status = ibkr_session_status(provider_state, now=state.as_of)
    state_path = system_event_state_path()
    previous = load_system_event_state(state_path)
    save_system_event_state(
        state_path,
        build_system_event_state_payload(state, provider_state, current_status, previous),
    )


def ibkr_session_event_alert(
    provider_state: ProviderState,
    *,
    previous_status: str | None,
    current_status: str,
) -> Alert | None:
    if (
        current_status in IBKR_INTERRUPTED_SESSION_STATUSES
        and previous_status not in IBKR_INTERRUPTED_SESSION_STATUSES
    ):
        return Alert(
            severity="high",
            kind="ibkr_session_interrupted",
            instrument_id="index:SPX",
            title="IBKR market-data session interrupted",
            detail=(
                "IBKR data session is unavailable"
                + (
                    " because another IBKR session appears to own market data."
                    if current_status == "competing_session"
                    else "."
                )
                + " Collector will keep fallback feeds running and probe again on the configured interval."
            ),
            provider=Provider.IBKR.value,
            quality=current_status,
            research_only=False,
            source_gate="ibkr_session_state",
        )
    if current_status == "available" and previous_status in IBKR_INTERRUPTED_SESSION_STATUSES:
        return Alert(
            severity="high",
            kind="ibkr_session_restored",
            instrument_id="index:SPX",
            title="IBKR market-data session restored",
            detail="IBKR data session is available again; SPX/SPXW/ES collection can resume.",
            provider=Provider.IBKR.value,
            quality=current_status,
            research_only=False,
            source_gate="ibkr_session_state",
        )
    return None


def system_event_alerts(state: LatestState, *, persist: bool = True) -> list[Alert]:
    if not env_bool("ALERT_SYSTEM_EVENTS_ENABLED", True):
        return []
    provider_state = provider_state_for(state, Provider.IBKR)
    if provider_state is None:
        return []
    current_status = ibkr_session_status(provider_state, now=state.as_of)
    state_path = system_event_state_path()
    previous = load_system_event_state(state_path)
    previous_status = previous.get("ibkr_session_status")
    if current_status in IBKR_TRANSITIONAL_SESSION_STATUSES:
        if persist:
            save_system_event_state(
                state_path,
                build_system_event_state_payload(state, provider_state, current_status, previous),
            )
        return []

    if persist:
        save_system_event_state(
            state_path,
            build_system_event_state_payload(state, provider_state, current_status, previous),
        )
    alert = ibkr_session_event_alert(
        provider_state,
        previous_status=str(previous_status) if previous_status else None,
        current_status=current_status,
    )
    return [alert] if alert is not None else []


def proxy_fallback_watch_alerts(
    state: LatestState,
    *,
    window: AlertWindow,
    market_context: dict[str, object] | None,
    options_map: OptionsMap | None = None,
) -> list[Alert]:
    if not env_bool("ALERT_ALLOW_BROKER_UNAVAILABLE_PROXY_WATCH", True):
        return []
    gate = hyperliquid_proxy_gate(market_context)
    if gate.get("usable_for_alert") is True:
        return []
    if str(gate.get("state") or "") != "unanchored_context_only":
        return []
    if not ibkr_feed_unavailable_for_fallback(state):
        return []

    quote = find_best(state, "crypto_perp:xyz:SP500")
    if quote is None or quote.quality in BAD_QUALITIES:
        return []
    move_bps = move_from_close_bps(quote)
    if options_map is None:
        threshold = env_float(
            "ALERT_PROXY_FALLBACK_MOVE_BPS",
            MOVE_THRESHOLDS_BPS.get(window.priority, MOVE_THRESHOLDS_BPS["normal"]),
        )
        threshold_source = None
        expected_move_pct = None
    else:
        threshold, threshold_source, expected_move_pct = movement_threshold_for_window(
            window,
            options_map,
            as_of=state.as_of,
        )
        threshold = env_float("ALERT_PROXY_FALLBACK_MOVE_BPS", threshold)
    if move_bps is None or abs(move_bps) < threshold:
        return []

    direction = "up" if move_bps > 0 else "down"
    detail = (
        "Broker SPX/ES feed is unavailable, likely because the trading session is in use. "
        "Proxy-only monitor moved enough to open the trading device and verify real SPX/SPXW "
        "quotes before any decision."
    )
    if threshold_source is not None:
        detail = (
            f"{detail} threshold_bps={threshold:.1f} "
            f"threshold_source={threshold_source} "
            f"expected_move_pct={expected_move_pct}"
        )
    return [
        Alert(
            severity=severity_for_priority(window.priority),
            kind="broker_unavailable_proxy_watch",
            instrument_id="index:SPX",
            title=f"SPX fallback monitor {direction} {move_bps:.1f} bps",
            detail=detail,
            provider=quote.provider.value,
            quality="degraded",
            value=move_bps,
            threshold=threshold,
            research_only=False,
            source_gate="broker_unavailable_fallback",
        )
    ]


def magnitude_bucket(value: float, threshold: float) -> str:
    """Dedup key for movement alerts: same direction and magnitude bucket share
    a cooldown slot, while a clearly larger move (next bucket) can still push
    through the cooldown."""
    direction = "up" if value >= 0 else "down"
    bucket = int(abs(value) // threshold) if threshold > 0 else 0
    return f"{direction}:{bucket}"


def iv_surface_movement_detail(body: str, *, degraded: bool) -> str:
    if degraded:
        return f"[degraded IV coverage] {body}"
    return body


def iv_surface_movement_severity(
    window: AlertWindow,
    *,
    value: float,
    threshold: float,
    degraded: bool,
) -> str:
    base = severity_for_priority(window.priority)
    if degraded and abs(value) >= threshold * 1.5:
        return "high" if base in {"medium", "low", "info"} else base
    return base


def iv_surface_alerts(
    surface: IvSurfaceSnapshot,
    *,
    window: AlertWindow,
    history_1h: dict[str, object] | None = None,
) -> list[Alert]:
    alerts: list[Alert] = []
    shift_1h_threshold = env_float("ALERT_IV_SURFACE_SHIFT_1H_THRESHOLD", SURFACE_SHIFT_1H_THRESHOLD)
    atm_change_1h_threshold = env_float("ALERT_IV_ATM_CHANGE_1H_THRESHOLD", ATM_IV_CHANGE_1H_THRESHOLD)
    if (
        surface.front_vs_next_atm_iv_gap is not None
        and abs(surface.front_vs_next_atm_iv_gap) >= TERM_GAP_THRESHOLD
    ):
        alerts.append(
            Alert(
                severity=severity_for_priority(window.priority),
                kind="iv_term_gap",
                instrument_id="iv_surface:SPXW",
                title=f"0DTE vs next ATM IV gap {surface.front_vs_next_atm_iv_gap:.3f}",
                detail=(
                    "Front SPXW ATM IV differs from next-expiry ATM IV by "
                    f"{surface.front_vs_next_atm_iv_gap:.3f}."
                ),
                value=surface.front_vs_next_atm_iv_gap,
                threshold=TERM_GAP_THRESHOLD,
                source_gate="iv_surface",
            )
        )
    for expiry in surface.expiries:
        instrument_id = f"iv_surface:SPXW:{expiry.expiry}"
        blocked = expiry.surface_fit_quality in BLOCKING_SURFACE_QUALITIES
        degraded = expiry.surface_fit_quality in DEGRADED_SURFACE_QUALITIES
        if expiry.surface_fit_quality in BAD_SURFACE_QUALITIES:
            alerts.append(
                Alert(
                    severity="low" if window.priority in {"low", "off"} else "medium",
                    kind="iv_surface_degraded",
                    instrument_id=instrument_id,
                    title=f"SPXW {expiry.expiry} surface {expiry.surface_fit_quality}",
                    detail=(
                        f"SPXW {expiry.expiry} IV surface quality is "
                        f"{expiry.surface_fit_quality}; movement alerts may be discounted."
                    ),
                    quality=expiry.surface_fit_quality,
                    source_gate="iv_surface",
                )
            )
        if blocked:
            continue
        if expiry.atm_iv_jump_5m is not None and abs(expiry.atm_iv_jump_5m) >= ATM_IV_JUMP_THRESHOLD:
            alerts.append(
                Alert(
                    severity=iv_surface_movement_severity(
                        window,
                        value=expiry.atm_iv_jump_5m,
                        threshold=ATM_IV_JUMP_THRESHOLD,
                        degraded=degraded,
                    ),
                    kind="atm_iv_jump_5m",
                    instrument_id=instrument_id,
                    title=f"SPXW {expiry.expiry} ATM IV jump {expiry.atm_iv_jump_5m:.3f}",
                    detail=iv_surface_movement_detail(
                        f"ATM IV changed {expiry.atm_iv_jump_5m:.3f} since the previous surface snapshot.",
                        degraded=degraded,
                    ),
                    value=expiry.atm_iv_jump_5m,
                    threshold=ATM_IV_JUMP_THRESHOLD,
                    quality=expiry.surface_fit_quality if degraded else None,
                    source_gate="iv_surface",
                    dedup_group=magnitude_bucket(expiry.atm_iv_jump_5m, ATM_IV_JUMP_THRESHOLD),
                )
            )
        skew_25d_threshold = env_float("ALERT_SKEW_25D_THRESHOLD", SKEW_25D_STEEPENING_THRESHOLD)
        if (
            expiry.put_skew_25d_change_5m is not None
            and expiry.put_skew_25d_change_5m >= skew_25d_threshold
        ):
            alerts.append(
                Alert(
                    severity=iv_surface_movement_severity(
                        window,
                        value=expiry.put_skew_25d_change_5m,
                        threshold=skew_25d_threshold,
                        degraded=degraded,
                    ),
                    kind="put_skew_steepening_5m",
                    instrument_id=instrument_id,
                    title=(
                        f"SPXW {expiry.expiry} put skew steepening "
                        f"{expiry.put_skew_25d_change_5m:.3f}"
                    ),
                    detail=iv_surface_movement_detail(
                        f"Put 25-delta skew widened {expiry.put_skew_25d_change_5m:.3f} vol points "
                        "since the previous surface snapshot (skew_source=delta_25).",
                        degraded=degraded,
                    ),
                    value=expiry.put_skew_25d_change_5m,
                    threshold=skew_25d_threshold,
                    quality=expiry.surface_fit_quality if degraded else None,
                    source_gate="iv_surface",
                    dedup_group=magnitude_bucket(expiry.put_skew_25d_change_5m, skew_25d_threshold),
                )
            )
        elif (
            expiry.put_skew_steepening_5m is not None
            and expiry.put_skew_steepening_5m >= SKEW_STEEPENING_THRESHOLD
        ):
            alerts.append(
                Alert(
                    severity=iv_surface_movement_severity(
                        window,
                        value=expiry.put_skew_steepening_5m,
                        threshold=SKEW_STEEPENING_THRESHOLD,
                        degraded=degraded,
                    ),
                    kind="put_skew_steepening_5m",
                    instrument_id=instrument_id,
                    title=f"SPXW {expiry.expiry} put skew steepening {expiry.put_skew_steepening_5m:.3f}",
                    detail=iv_surface_movement_detail(
                        f"Put skew ratio increased {expiry.put_skew_steepening_5m:.3f} "
                        "since the previous surface snapshot (skew_source=ratio).",
                        degraded=degraded,
                    ),
                    value=expiry.put_skew_steepening_5m,
                    threshold=SKEW_STEEPENING_THRESHOLD,
                    quality=expiry.surface_fit_quality if degraded else None,
                    source_gate="iv_surface",
                    dedup_group=magnitude_bucket(expiry.put_skew_steepening_5m, SKEW_STEEPENING_THRESHOLD),
                )
            )
        if (
            expiry.iv_surface_shift_5m is not None
            and abs(expiry.iv_surface_shift_5m) >= SURFACE_SHIFT_THRESHOLD
        ):
            alerts.append(
                Alert(
                    severity=iv_surface_movement_severity(
                        window,
                        value=expiry.iv_surface_shift_5m,
                        threshold=SURFACE_SHIFT_THRESHOLD,
                        degraded=degraded,
                    ),
                    kind="iv_surface_shift_5m",
                    instrument_id=instrument_id,
                    title=f"SPXW {expiry.expiry} surface shift {expiry.iv_surface_shift_5m:.3f}",
                    detail=iv_surface_movement_detail(
                        f"Average raw-grid IV shifted {expiry.iv_surface_shift_5m:.3f} "
                        "since the previous surface snapshot.",
                        degraded=degraded,
                    ),
                    value=expiry.iv_surface_shift_5m,
                    threshold=SURFACE_SHIFT_THRESHOLD,
                    quality=expiry.surface_fit_quality if degraded else None,
                    source_gate="iv_surface",
                    dedup_group=magnitude_bucket(expiry.iv_surface_shift_5m, SURFACE_SHIFT_THRESHOLD),
                )
            )
    if isinstance(history_1h, dict):
        expiry_rows = history_1h.get("expiries")
        if isinstance(expiry_rows, list):
            for row in expiry_rows:
                if not isinstance(row, dict):
                    continue
                expiry_name = str(row.get("expiry") or "")
                if not expiry_name:
                    continue
                fit_quality = str(row.get("surface_fit_quality") or "")
                blocked = fit_quality in BLOCKING_SURFACE_QUALITIES
                degraded = fit_quality in DEGRADED_SURFACE_QUALITIES
                instrument_id = f"iv_surface:SPXW:{expiry_name}"
                shift_1h = row.get("iv_surface_level_change_1h")
                if (
                    not blocked
                    and isinstance(shift_1h, (int, float))
                    and abs(float(shift_1h)) >= shift_1h_threshold
                ):
                    shift_value = float(shift_1h)
                    alerts.append(
                        Alert(
                            severity=iv_surface_movement_severity(
                                window,
                                value=shift_value,
                                threshold=shift_1h_threshold,
                                degraded=degraded,
                            ),
                            kind="iv_surface_shift_1h",
                            instrument_id=instrument_id,
                            title=f"SPXW {expiry_name} 1h surface shift {shift_value:.3f}",
                            detail=iv_surface_movement_detail(
                                f"Average raw-grid IV shifted {shift_value:.3f} over the last hour.",
                                degraded=degraded,
                            ),
                            value=shift_value,
                            threshold=shift_1h_threshold,
                            quality=fit_quality if degraded else None,
                            source_gate="iv_surface",
                            dedup_group=f"{int(shift_value * 100) // int(shift_1h_threshold * 100)}",
                        )
                    )
                atm_change_1h = row.get("atm_iv_change_1h")
                if (
                    not blocked
                    and isinstance(atm_change_1h, (int, float))
                    and abs(float(atm_change_1h)) >= atm_change_1h_threshold
                ):
                    atm_value = float(atm_change_1h)
                    alerts.append(
                        Alert(
                            severity=iv_surface_movement_severity(
                                window,
                                value=atm_value,
                                threshold=atm_change_1h_threshold,
                                degraded=degraded,
                            ),
                            kind="atm_iv_change_1h",
                            instrument_id=instrument_id,
                            title=f"SPXW {expiry_name} 1h ATM IV change {atm_value:.3f}",
                            detail=iv_surface_movement_detail(
                                f"ATM IV changed {atm_value:.3f} over the last hour.",
                                degraded=degraded,
                            ),
                            value=atm_value,
                            threshold=atm_change_1h_threshold,
                            quality=fit_quality if degraded else None,
                            source_gate="iv_surface",
                            dedup_group=f"{int(atm_value * 100) // int(atm_change_1h_threshold * 100)}",
                        )
                    )
    return alerts


def load_current_iv_surface(settings: IvSurfaceSettings | None = None) -> IvSurfaceSnapshot | None:
    settings = settings or IvSurfaceSettings.from_env()
    try:
        return load_latest_snapshot(settings.latest_surface_path)
    except (OSError, ValueError, json.JSONDecodeError, KeyError):
        return None


def evaluate_payload(
    state: LatestState,
    *,
    now: datetime | None = None,
    persist_system_events: bool = True,
    persist_movement_state: bool = False,
) -> dict[str, object]:
    now = now or state.as_of
    window = active_window(now)
    window_payload = window.to_dict(now=now)
    options_map = build_options_map(state)
    iv_settings = IvSurfaceSettings.from_env()
    iv_surface = load_current_iv_surface(iv_settings)
    iv_surface_history = load_recent_snapshots(iv_settings, as_of=state.as_of, lookback_minutes=60)
    iv_surface_history_1h = summarize_surface_history(iv_surface, iv_surface_history)
    market_context = build_market_context(state)
    iv_stale_alert = (
        iv_surface_freshness_alert(iv_surface, now=state.as_of) if iv_surface is not None else None
    )
    iv_surface_for_alerts = None if iv_stale_alert is not None else iv_surface
    alerts = evaluate_alerts(
        state,
        window=window,
        options_map=options_map,
        iv_surface=iv_surface_for_alerts,
        iv_surface_history_1h=iv_surface_history_1h,
        market_context=market_context,
        persist_system_events=persist_system_events,
        persist_movement_state=persist_movement_state,
    )
    if iv_stale_alert is not None:
        alerts.append(iv_stale_alert)
    return {
        "created_at": datetime.now(tz=now.tzinfo).isoformat(),
        "as_of": state.as_of.isoformat(),
        "window": window_payload,
        "market_context": market_context,
        "human_focus_context": build_human_focus_context(
            state,
            options_map=options_map,
            iv_surface=iv_surface,
            iv_surface_history_1h=iv_surface_history_1h,
            window=window_payload,
        ),
        "options_map": options_map.to_dict(),
        "iv_surface": iv_surface.to_dict() if iv_surface is not None else None,
        "iv_surface_history_1h": iv_surface_history_1h,
        "alert_count": len(alerts),
        "alerts": [alert.to_dict() for alert in alerts],
    }


def print_alerts(payload: dict[str, object]) -> None:
    window = payload["window"]
    assert isinstance(window, dict)
    print(f"Alert window: {window['name']} priority={window['priority']}")
    print(f"As of: {payload['as_of']}")
    print(f"Alerts: {payload['alert_count']}")
    alerts = payload["alerts"]
    assert isinstance(alerts, list)
    for item in alerts:
        assert isinstance(item, dict)
        print(f"- [{item['severity']}] {item['title']}")
        print(f"  {item['detail']}")


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Evaluate current SPX alert conditions.")
    parser.add_argument("--at", help="ISO timestamp. Naive timestamps are treated as Asia/Shanghai.")
    parser.add_argument("--json", action="store_true", help="Print JSON.")
    parser.add_argument("--notify", action="store_true", help="Send configured notifications.")
    parser.add_argument("--no-notify", action="store_true", help="Disable notifications for this run.")
    return parser.parse_args(argv)


def run(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    now = parse_at(args.at) if args.at else None
    state = LatestStateStore(StorageSettings.from_env()).load(now=now)
    notification_settings = NotificationSettings.from_env()
    if args.notify:
        notification_settings = replace(notification_settings, enabled=True)
    if args.no_notify:
        notification_settings = replace(notification_settings, enabled=False)
    payload = evaluate_payload(
        state,
        now=now or state.as_of,
        persist_system_events=False,
        persist_movement_state=False,
    )
    system_event_pending = any(
        isinstance(alert, dict) and alert.get("source_gate") == "ibkr_session_state"
        for alert in payload.get("alerts", [])
    )
    movement_pending = any(
        isinstance(alert, dict) and alert.get("kind") == "price_move_from_close"
        for alert in payload.get("alerts", [])
    )
    notification_result = None
    if notification_settings.enabled:
        notification_result = notify_payload(payload, settings=notification_settings)
        payload["notification"] = notification_result.to_dict()
    notified = notification_result is not None and notification_result.sent_count > 0
    settled = not notification_settings.enabled or notified
    if not system_event_pending or settled:
        persist_system_event_state(state)
    if not movement_pending or settled:
        persist_movement_state_snapshot(state)
    if args.json:
        print(json.dumps(payload, indent=2, sort_keys=True))
    else:
        print_alerts(payload)
    return 0


def main() -> None:
    raise SystemExit(run())


if __name__ == "__main__":
    main()
