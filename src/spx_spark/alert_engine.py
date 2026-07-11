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
    env_bool,
    env_float,
)
from spx_spark.market_calendar import DEFAULT_MARKET_CALENDAR
from spx_spark.human_focus import build_human_focus_context
from spx_spark.iv_surface import (
    IvSurfaceSnapshot,
    load_latest_snapshot,
    load_recent_snapshots,
    summarize_surface_history,
)
from spx_spark.market_context import build_market_context
from spx_spark.marketdata import (
    MarketDataQuality,
    Provider,
    ProviderState,
    ProviderStatus,
    Quote,
    as_utc,
    parse_timestamp,
)
from spx_spark.notifier import notify_payload
from spx_spark.options_map import OptionsMap, build_options_map
from spx_spark.position_alerts import (
    has_open_spxw_positions,
    position_holdings_alerts,
    reconcile_position_event_acknowledgements,
)
from spx_spark.provider_failover import FailoverMode, FailoverState
from spx_spark.provider_failover_controller import (
    ProviderFailoverSettings,
    load_failover_control,
)
from spx_spark.runtime_config import runtime_value
from spx_spark.storage import (
    LatestState,
    LatestStateStore,
    configured_quote_use_decision,
)
from spx_spark.strategy.steven import (
    annotate_alerts_with_steven_context,
    load_steven_state_for_alerts,
)


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
    "index:DJI",
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
    "low": 0.35,
    "off": 9.0,
}

# In quiet (low-priority) windows the static 85 bps bar is unreachable in
# low-vol regimes, so overnight dips never alert. When expected move is known,
# scale the bar down to the EM fraction instead (floored to avoid tick noise).
QUIET_EM_THRESHOLD_FLOOR_BPS_DEFAULT = 15.0

# A move consuming this fraction of the day's expected move is escalated to
# high severity so it clears the notify gate even in low-priority windows.
# Kept equal to the quiet EM fraction so any move that crosses the quiet bar
# also clears the notify severity gate.
MOVE_HIGH_SEVERITY_EM_FRACTION_DEFAULT = 0.35

BAD_QUALITIES = {
    MarketDataQuality.MISSING,
    MarketDataQuality.ERROR,
    MarketDataQuality.STALE,
    MarketDataQuality.UNKNOWN,
    MarketDataQuality.DELAYED,
    MarketDataQuality.DELAYED_FROZEN,
}

OPTION_GAMMA_ALERT_STATES = {
    "negative_gamma_acceleration",
    "zero_gamma_transition",
}

BAD_SURFACE_QUALITIES = {
    "missing_options",
    "missing_atm_iv",
    "low_iv_coverage",
    "wide_quote_degraded",
}
BLOCKING_SURFACE_QUALITIES = {"missing_options", "missing_atm_iv"}
DEGRADED_SURFACE_QUALITIES = {"low_iv_coverage", "wide_quote_degraded"}
# Algorithm thresholds in absolute IV / skew units (not env-tunable identities).
ATM_IV_JUMP_THRESHOLD = 0.03  # 5-minute ATM IV jump that opens an IV-jump alert
SKEW_STEEPENING_THRESHOLD = 0.08  # 5-minute put-skew steepening alert floor
SKEW_25D_STEEPENING_THRESHOLD = 0.02  # default 25-delta skew steepening floor
SURFACE_SHIFT_THRESHOLD = 0.03  # 5-minute whole-surface level shift floor
TERM_GAP_THRESHOLD = 0.05  # front-vs-next ATM IV term-structure gap floor
SURFACE_SHIFT_1H_THRESHOLD = 0.05  # default 1-hour surface shift floor (env-overridable)
ATM_IV_CHANGE_1H_THRESHOLD = 0.04  # default 1-hour ATM IV change floor (env-overridable)
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
    if priority == "low":
        floor = env_float(
            "ALERT_MOVE_QUIET_FLOOR_BPS",
            float(runtime_value("alerts.move_quiet_floor_bps")),
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
                float(runtime_value("alerts.move_high_severity_em_fraction")),
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
    alerts.extend(
        proxy_fallback_watch_alerts(
            state, window=window, market_context=market_context, options_map=options_map
        )
    )
    return alerts


def option_coverage_is_fresh(expiry: object) -> bool:
    coverage = getattr(expiry, "coverage", None)
    if coverage is None or coverage.total <= 0:
        return False
    min_live_ratio = env_float(
        "ALERT_MIN_OPTION_LIVE_RATIO",
        float(runtime_value("alerts.min_option_live_ratio")),
    )
    if coverage.live / coverage.total < min_live_ratio:
        return False
    max_age_ms = coverage.max_age_ms
    if max_age_ms is not None and max_age_ms > env_float(
        "ALERT_MAX_OPTION_QUOTE_AGE_MS",
        float(runtime_value("alerts.max_option_quote_age_ms")),
    ):
        return False
    if env_bool(
        "ALERT_REQUIRE_OPTION_QUOTE_TIMESTAMPS",
        bool(runtime_value("alerts.require_option_quote_timestamps")),
    ):
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
        threshold=env_float(
            "ALERT_MIN_OPTION_LIVE_RATIO",
            float(runtime_value("alerts.min_option_live_ratio")),
        ),
    )


# Walls recompute every cycle and drift a strike or two as OI updates; keying
# the cooldown on the exact strike turned every 5-point wall move into a fresh
# alert (2026-07-08: eight wall pushes in 24 minutes). Deduping by 25-point
# band keeps re-alerts for genuinely new levels only.
WALL_DEDUP_BAND_POINTS = 25.0


def wall_dedup_band(wall: float, band_points: float = WALL_DEDUP_BAND_POINTS) -> str:
    return f"band:{int(wall // band_points) * int(band_points)}"


def gamma_regime_state_path() -> str:
    data_root = os.getenv("MARKET_DATA_DATA_ROOT") or os.getenv("MAINTENANCE_DATA_ROOT") or "data"
    return os.getenv(
        "ALERT_GAMMA_REGIME_STATE_PATH",
        f"{data_root.rstrip('/')}/latest/gamma_regime_state.json",
    )


def load_gamma_regime_state(path: str | Path) -> dict[str, object]:
    try:
        payload = json.loads(Path(path).read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    return payload if isinstance(payload, dict) else {}


def gamma_regime_observation_stable(expiry: str, gamma_state: str, *, as_of: datetime) -> bool:
    """True when the persisted observation shows this gamma state has held for
    the hysteresis window. Read-only: observations are persisted separately so
    dry runs and tests do not mutate state."""
    hysteresis = env_float(
        "ALERT_GAMMA_REGIME_HYSTERESIS_SECONDS",
        float(runtime_value("alerts.gamma_regime_hysteresis_seconds")),
    )
    entry = load_gamma_regime_state(gamma_regime_state_path()).get(expiry)
    if not isinstance(entry, dict) or entry.get("state") != gamma_state:
        return False
    since = entry.get("since")
    if not isinstance(since, int | float):
        return False
    return as_of.timestamp() - float(since) >= hysteresis


def persist_gamma_regime_observations(options_map: OptionsMap, *, as_of: datetime) -> None:
    """Track when each expiry's gamma state was first observed; a state change
    resets the clock so 4-minute flip-flops never clear the hysteresis."""
    path = Path(gamma_regime_state_path())
    payload = load_gamma_regime_state(path)
    current_expiries = {expiry.expiry for expiry in options_map.expiries}
    payload = {key: value for key, value in payload.items() if key in current_expiries}
    for expiry in options_map.expiries:
        entry = payload.get(expiry.expiry)
        if not isinstance(entry, dict) or entry.get("state") != expiry.gamma_state:
            payload[expiry.expiry] = {"state": expiry.gamma_state, "since": as_of.timestamp()}
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        temp_path = path.with_suffix(f"{path.suffix}.tmp")
        temp_path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")
        temp_path.replace(path)
    except OSError:
        pass


def option_map_alerts(options_map: OptionsMap, *, window: AlertWindow) -> list[Alert]:
    alerts: list[Alert] = []
    underlier = options_map.underlier.price
    wall_threshold = max(10.0, underlier * 0.002 if underlier else 10.0)
    for expiry in options_map.expiries:
        if not option_coverage_is_fresh(expiry):
            alerts.append(option_freshness_alert(expiry, window=window))
            continue
        if expiry.gamma_state in OPTION_GAMMA_ALERT_STATES and gamma_regime_observation_stable(
            expiry.expiry,
            expiry.gamma_state,
            as_of=options_map.as_of,
        ):
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
                        dedup_group=wall_dedup_band(expiry.nearest_wall),
                    )
                )
    return alerts


def iv_surface_freshness_alert(surface: IvSurfaceSnapshot, *, now: datetime) -> Alert | None:
    max_age_seconds = env_float(
        "ALERT_MAX_IV_SURFACE_AGE_SECONDS",
        float(runtime_value("alerts.max_iv_surface_age_seconds")),
    )
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
            value=gate.get("basis_bps")
            if isinstance(gate.get("basis_bps"), (int, float))
            else None,
            threshold=gate.get("block_bps")
            if isinstance(gate.get("block_bps"), (int, float))
            else None,
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
    max_age_seconds = env_float(
        "ALERT_BROKER_STATE_MAX_AGE_SECONDS",
        float(runtime_value("alerts.broker_state_max_age_seconds")),
    )
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
    if "account standby connected" in reason:
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
        **previous,
        "ibkr_session_status": current_status,
        "ibkr_last_observed_status": current_status,
        "ibkr_checked_at": provider_state.checked_at.isoformat(),
        "updated_at": state.as_of.isoformat(),
    }


def persist_system_event_state(state: LatestState) -> None:
    state_path = system_event_state_path()
    previous = load_system_event_state(state_path)
    payload = dict(previous)
    provider_state = provider_state_for(state, Provider.IBKR)
    # Always track IBKR session edges so reconnect ops notices work even in
    # account-standby / no-position mode. Interrupt paging stays gated separately.
    if provider_state is not None:
        current_status = ibkr_session_status(provider_state, now=state.as_of)
        payload = build_system_event_state_payload(
            state,
            provider_state,
            current_status,
            payload,
        )
    failover_state = load_provider_failover_state(now=state.as_of)
    if failover_state is not None and failover_state.transition is not None:
        payload["provider_failover_transition_id"] = failover_state.transition.transition_id
        payload["provider_failover_mode"] = failover_state.mode.value
    if payload != previous:
        save_system_event_state(state_path, payload)


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
            title="IBKR broker session interrupted",
            detail=(
                "IBKR broker session is unavailable while positions or live execution require it"
                + (
                    " because another IBKR session appears to own market data."
                    if current_status == "competing_session"
                    else "."
                )
                + " Market-data fallback remains independent; account and execution safety require attention."
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
            title="IBKR broker session restored",
            detail="IBKR broker connectivity is available again for position or execution safety.",
            provider=Provider.IBKR.value,
            quality=current_status,
            research_only=False,
            source_gate="ibkr_session_state",
        )
    return None


def ibkr_gateway_login_alert(
    provider_state: ProviderState,
    *,
    previous_status: str | None,
    current_status: str,
) -> Alert | None:
    """Ops-visible reconnect notice when positions/live execution are not critical.

    Interruptions stay silent in standby; only Gateway/API coming back online pages.
    """
    if current_status != "available":
        return None
    if previous_status not in IBKR_INTERRUPTED_SESSION_STATUSES:
        return None
    standby = "account standby connected" in (provider_state.reason or "").lower()
    mode = "account standby (market data inactive)" if standby else "market-data session"
    return Alert(
        severity="high",
        kind="ibkr_session_login",
        instrument_id="index:SPX",
        title="IBKR Gateway/API reconnected",
        detail=(
            f"IBKR API connected again in {mode}. "
            "This is an ops notice for login/session visibility; it is not a trade signal."
        ),
        provider=Provider.IBKR.value,
        quality=current_status,
        research_only=False,
        source_gate="ibkr_session_state",
    )


def load_provider_failover_state(*, now: datetime) -> FailoverState | None:
    settings = ProviderFailoverSettings.from_env()
    raw = load_failover_control(settings.state_path)
    if not raw or raw.get("monitoring_active") is not True:
        return None
    updated_at = parse_timestamp(raw.get("updated_at"))
    if updated_at is None:
        return None
    state_age_seconds = (as_utc(now) - updated_at).total_seconds()
    if not 0 <= state_age_seconds <= settings.control_state_max_age_seconds:
        return None
    try:
        failover_state = FailoverState.from_dict(raw)
    except (KeyError, TypeError, ValueError):
        return None
    transition = failover_state.transition
    if transition is not None:
        transition_age_seconds = (as_utc(now) - transition.occurred_at).total_seconds()
        if not 0 <= transition_age_seconds <= settings.transition_alert_max_age_seconds:
            failover_state = replace(failover_state, transition=None)
    return failover_state


def provider_failover_event_alert(
    failover_state: FailoverState,
    *,
    previous_transition_id: str | None,
) -> Alert | None:
    transition = failover_state.transition
    if transition is None or transition.transition_id == previous_transition_id:
        return None
    if transition.mode == FailoverMode.IBKR_FALLBACK:
        return Alert(
            severity="high",
            kind="market_data_ibkr_fallback_activated",
            instrument_id="index:SPX",
            title="Schwab 异常，IBKR 备用行情已接管",
            detail=(
                "SPX/ES 直接行情已切换到 IBKR L1 备用通道；"
                "系统保持风控，但不会因为切换本身反复推送离线消息。"
            ),
            provider=Provider.IBKR.value,
            quality=failover_state.mode.value,
            research_only=False,
            source_gate="provider_failover_state",
            dedup_group=transition.transition_id,
        )
    if transition.mode == FailoverMode.BOTH_UNAVAILABLE:
        return Alert(
            severity="critical",
            kind="market_data_all_providers_unavailable",
            instrument_id="index:SPX",
            title="Schwab 与 IBKR 直接行情均不可用",
            detail="两个直接行情源均未通过健康门；禁止新开仓，只允许人工核对和已有仓位处置。",
            provider=Provider.INTERNAL.value,
            quality=failover_state.mode.value,
            research_only=False,
            source_gate="provider_failover_state",
            dedup_group=transition.transition_id,
        )
    if transition.mode == FailoverMode.SCHWAB_PRIMARY:
        if transition.previous_mode == FailoverMode.RECOVERY_PENDING:
            title = "Schwab 连续稳定，备用接管已取消"
            detail = "Schwab 在 IBKR 接管前恢复并连续通过健康门；系统继续使用主行情。"
        elif transition.previous_mode == FailoverMode.BOTH_UNAVAILABLE:
            title = "Schwab 连续稳定，主行情已恢复"
            detail = "Schwab 锚点连续通过健康门，双源不可用状态已解除。"
        else:
            title = "Schwab 连续稳定，主行情已恢复"
            detail = "Schwab SPX/ES 锚点连续通过健康门，系统已退出 IBKR 备用行情状态。"
        return Alert(
            severity="high",
            kind="market_data_schwab_restored",
            instrument_id="index:SPX",
            title=title,
            detail=detail,
            provider=Provider.SCHWAB.value,
            quality=failover_state.mode.value,
            research_only=False,
            source_gate="provider_failover_state",
            dedup_group=transition.transition_id,
        )
    return None


def ibkr_session_is_position_critical() -> bool:
    execution_mode = os.getenv(
        "IBKR_EXECUTION_MODE",
        str(runtime_value("ibkr_broker.execution_mode")),
    ).strip().lower()
    if execution_mode == "live":
        return True
    return has_open_spxw_positions()


def system_event_alerts(state: LatestState, *, persist: bool = True) -> list[Alert]:
    if not env_bool(
        "ALERT_SYSTEM_EVENTS_ENABLED",
        bool(runtime_value("alerts.system_events_enabled")),
    ):
        return []
    state_path = system_event_state_path()
    previous = load_system_event_state(state_path)
    alerts: list[Alert] = []
    failover_state = load_provider_failover_state(now=state.as_of)
    if failover_state is not None:
        failover_alert = provider_failover_event_alert(
            failover_state,
            previous_transition_id=(
                str(previous.get("provider_failover_transition_id"))
                if previous.get("provider_failover_transition_id")
                else None
            ),
        )
        if failover_alert is not None:
            alerts.append(failover_alert)

    provider_state = provider_state_for(state, Provider.IBKR)
    if provider_state is not None:
        current_status = ibkr_session_status(provider_state, now=state.as_of)
        previous_status = previous.get("ibkr_session_status")
        previous_status_s = str(previous_status) if previous_status else None
        if current_status not in IBKR_TRANSITIONAL_SESSION_STATUSES:
            if ibkr_session_is_position_critical():
                alert = ibkr_session_event_alert(
                    provider_state,
                    previous_status=previous_status_s,
                    current_status=current_status,
                )
            else:
                # Standby disconnect stays silent; reconnect/login becomes visible.
                alert = ibkr_gateway_login_alert(
                    provider_state,
                    previous_status=previous_status_s,
                    current_status=current_status,
                )
            if alert is not None:
                alerts.append(alert)

    if persist:
        persist_system_event_state(state)
    return alerts


def proxy_fallback_watch_alerts(
    state: LatestState,
    *,
    window: AlertWindow,
    market_context: dict[str, object] | None,
    options_map: OptionsMap | None = None,
) -> list[Alert]:
    if not env_bool(
        "ALERT_ALLOW_BROKER_UNAVAILABLE_PROXY_WATCH",
        bool(runtime_value("alerts.allow_broker_unavailable_proxy_watch")),
    ):
        return []
    gate = hyperliquid_proxy_gate(market_context)
    if gate.get("usable_for_alert") is True:
        return []
    # Keep an unanchored proxy move in the algorithmic context, but never make
    # it look like a directly actionable SPX alert.  Periodic research status
    # is the only human-facing path allowed without a live TradFi anchor.
    if str(gate.get("state") or "") != "unanchored_context_only":
        return []
    broker_down = ibkr_feed_unavailable_for_fallback(state)

    quote = find_best(state, "crypto_perp:xyz:SP500")
    if quote is None or not configured_quote_use_decision(quote, as_of=state.as_of).alert_allowed:
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
    if broker_down:
        detail = (
            "Broker SPX/ES feed is unavailable, likely because the trading session is in use. "
            "Proxy-only monitor moved enough to open the trading device and verify real SPX/SPXW "
            "quotes before any decision."
        )
    else:
        detail = (
            "No live SPX/ES anchor quotes (session closed or ES maintenance break); "
            "SP500 perp is the only live monitor and moved enough to notice. "
            "Verify real SPX/SPXW quotes before any decision."
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
            instrument_id=quote.instrument.canonical_id,
            title=f"SPX fallback monitor {direction} {move_bps:.1f} bps",
            detail=detail,
            provider=quote.provider.value,
            quality="degraded",
            value=move_bps,
            threshold=threshold,
            research_only=True,
            source_gate="hyperliquid_proxy_unanchored",
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
    shift_1h_threshold = env_float(
        "ALERT_IV_SURFACE_SHIFT_1H_THRESHOLD",
        float(runtime_value("alerts.iv_surface_shift_1h_threshold")),
    )
    atm_change_1h_threshold = env_float(
        "ALERT_IV_ATM_CHANGE_1H_THRESHOLD",
        float(runtime_value("alerts.iv_atm_change_1h_threshold")),
    )
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
        if (
            expiry.atm_iv_jump_5m is not None
            and abs(expiry.atm_iv_jump_5m) >= ATM_IV_JUMP_THRESHOLD
        ):
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
        skew_25d_threshold = env_float(
            "ALERT_SKEW_25D_THRESHOLD",
            float(runtime_value("alerts.skew_25d_threshold")),
        )
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
                    dedup_group=magnitude_bucket(
                        expiry.put_skew_steepening_5m, SKEW_STEEPENING_THRESHOLD
                    ),
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
                    dedup_group=magnitude_bucket(
                        expiry.iv_surface_shift_5m, SURFACE_SHIFT_THRESHOLD
                    ),
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
    persist_gamma_regime: bool = False,
) -> dict[str, object]:
    now = now or state.as_of
    window = active_window(now)
    window_payload = window.to_dict(now=now)
    options_map = build_options_map(state)
    if persist_gamma_regime:
        # Record observations before alert evaluation: a state seen for the
        # first time starts its hysteresis clock now and only alerts once it
        # has held for the configured window.
        persist_gamma_regime_observations(options_map, as_of=options_map.as_of)
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
    # Steven observe-only context: read-only note on selected alert kinds.
    if bool(runtime_value("steven.alert_context_enabled")):
        try:
            steven_state = load_steven_state_for_alerts(StorageSettings.from_env().data_root)
            alerts = annotate_alerts_with_steven_context(
                alerts,
                steven_state,
                as_of=state.as_of,
            )
        except Exception:  # noqa: BLE001 — never block alerts on context failure
            pass
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
    parser.add_argument(
        "--at", help="ISO timestamp. Naive timestamps are treated as Asia/Shanghai."
    )
    parser.add_argument("--json", action="store_true", help="Print JSON.")
    parser.add_argument("--notify", action="store_true", help="Send configured notifications.")
    parser.add_argument(
        "--no-notify", action="store_true", help="Disable notifications for this run."
    )
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
        persist_gamma_regime=True,
    )
    system_event_pending = any(
        isinstance(alert, dict)
        and alert.get("source_gate") in {"ibkr_session_state", "provider_failover_state"}
        for alert in payload.get("alerts", [])
    )
    movement_pending = any(
        isinstance(alert, dict) and alert.get("kind") == "price_move_from_close"
        for alert in payload.get("alerts", [])
    )
    notification_result = None
    if notification_settings.enabled:
        notification_result = notify_payload(payload, settings=notification_settings)
        reconcile_position_event_acknowledgements(notification_result.acknowledged_event_ids)
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
