from __future__ import annotations

import json
import os
from datetime import datetime
from pathlib import Path

from spx_spark.alert_engine.constants import (
    BAD_SURFACE_QUALITIES,
    BLOCKING_SURFACE_QUALITIES,
    DEGRADED_SURFACE_QUALITIES,
    OPTION_GAMMA_ALERT_STATES,
)
from spx_spark.alert_model import Alert, severity_for_priority
from spx_spark.alert_profile import AlertWindow
from spx_spark.config import env_bool, env_float
from spx_spark.iv_surface import IvSurfaceSnapshot
from spx_spark.options_map import OptionsMap
from spx_spark.settings import DEFAULT_ALERT_SETTINGS, AlertSettings


def option_coverage_is_fresh(
    expiry: object,
    *,
    settings: AlertSettings = DEFAULT_ALERT_SETTINGS,
) -> bool:
    coverage = getattr(expiry, "coverage", None)
    if coverage is None or coverage.total <= 0:
        return False
    min_live_ratio = env_float(
        "ALERT_MIN_OPTION_LIVE_RATIO",
        settings.min_option_live_ratio,
    )
    if coverage.live / coverage.total < min_live_ratio:
        return False
    max_age_ms = coverage.max_age_ms
    if max_age_ms is not None and max_age_ms > env_float(
        "ALERT_MAX_OPTION_QUOTE_AGE_MS",
        settings.max_option_quote_age_ms,
    ):
        return False
    if env_bool(
        "ALERT_REQUIRE_OPTION_QUOTE_TIMESTAMPS",
        settings.require_option_quote_timestamps,
    ):
        known_ratio = (coverage.total - coverage.unknown_age) / coverage.total
        if known_ratio < settings.min_known_option_timestamp_ratio:
            return False
    return True


def option_freshness_alert(
    expiry: object,
    *,
    window: AlertWindow,
    settings: AlertSettings = DEFAULT_ALERT_SETTINGS,
) -> Alert:
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
            settings.min_option_live_ratio,
        ),
    )


# Walls recompute every cycle; cooldown keys use WALL_DEDUP_BAND_POINTS from constants.


def wall_dedup_band(wall: float, band_points: float = 25.0) -> str:
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
        DEFAULT_ALERT_SETTINGS.gamma_regime_hysteresis_seconds,
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


def option_map_alerts(
    options_map: OptionsMap,
    *,
    window: AlertWindow,
    settings: AlertSettings = DEFAULT_ALERT_SETTINGS,
) -> list[Alert]:
    alerts: list[Alert] = []
    underlier = options_map.underlier.price
    wall_threshold = max(
        settings.wall_proximity_min_points,
        (
            underlier * settings.wall_proximity_underlier_fraction
            if underlier
            else settings.wall_proximity_min_points
        ),
    )
    for expiry in options_map.expiries:
        if not option_coverage_is_fresh(expiry, settings=settings):
            alerts.append(option_freshness_alert(expiry, window=window, settings=settings))
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
                        dedup_group=wall_dedup_band(
                            expiry.nearest_wall,
                            settings.wall_dedup_band_points,
                        ),
                    )
                )
    return alerts


def iv_surface_freshness_alert(surface: IvSurfaceSnapshot, *, now: datetime) -> Alert | None:
    max_age_seconds = env_float(
        "ALERT_MAX_IV_SURFACE_AGE_SECONDS",
        DEFAULT_ALERT_SETTINGS.max_iv_surface_age_seconds,
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
    degraded_threshold_multiplier: float = 1.5,
) -> str:
    base = severity_for_priority(window.priority)
    if degraded and abs(value) >= threshold * degraded_threshold_multiplier:
        return "high" if base in {"medium", "low", "info"} else base
    return base


def iv_surface_alerts(
    surface: IvSurfaceSnapshot,
    *,
    window: AlertWindow,
    history_1h: dict[str, object] | None = None,
    settings: AlertSettings = DEFAULT_ALERT_SETTINGS,
) -> list[Alert]:
    alerts: list[Alert] = []
    shift_1h_threshold = env_float(
        "ALERT_IV_SURFACE_SHIFT_1H_THRESHOLD",
        settings.iv_surface_shift_1h_threshold,
    )
    atm_change_1h_threshold = env_float(
        "ALERT_IV_ATM_CHANGE_1H_THRESHOLD",
        settings.iv_atm_change_1h_threshold,
    )
    if (
        surface.front_vs_next_atm_iv_gap is not None
        and abs(surface.front_vs_next_atm_iv_gap) >= settings.term_gap_threshold
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
                threshold=settings.term_gap_threshold,
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
            and abs(expiry.atm_iv_jump_5m) >= settings.atm_iv_jump_threshold
        ):
            alerts.append(
                Alert(
                    severity=iv_surface_movement_severity(
                        window,
                        value=expiry.atm_iv_jump_5m,
                        threshold=settings.atm_iv_jump_threshold,
                        degraded=degraded,
                        degraded_threshold_multiplier=settings.degraded_threshold_multiplier,
                    ),
                    kind="atm_iv_jump_5m",
                    instrument_id=instrument_id,
                    title=f"SPXW {expiry.expiry} ATM IV jump {expiry.atm_iv_jump_5m:.3f}",
                    detail=iv_surface_movement_detail(
                        f"ATM IV changed {expiry.atm_iv_jump_5m:.3f} since the previous surface snapshot.",
                        degraded=degraded,
                    ),
                    value=expiry.atm_iv_jump_5m,
                    threshold=settings.atm_iv_jump_threshold,
                    quality=expiry.surface_fit_quality if degraded else None,
                    source_gate="iv_surface",
                    dedup_group=magnitude_bucket(
                        expiry.atm_iv_jump_5m,
                        settings.atm_iv_jump_threshold,
                    ),
                )
            )
        skew_25d_threshold = env_float(
            "ALERT_SKEW_25D_THRESHOLD",
            settings.skew_25d_threshold,
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
                        degraded_threshold_multiplier=settings.degraded_threshold_multiplier,
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
            and expiry.put_skew_steepening_5m >= settings.skew_steepening_threshold
        ):
            alerts.append(
                Alert(
                    severity=iv_surface_movement_severity(
                        window,
                        value=expiry.put_skew_steepening_5m,
                        threshold=settings.skew_steepening_threshold,
                        degraded=degraded,
                        degraded_threshold_multiplier=settings.degraded_threshold_multiplier,
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
                    threshold=settings.skew_steepening_threshold,
                    quality=expiry.surface_fit_quality if degraded else None,
                    source_gate="iv_surface",
                    dedup_group=magnitude_bucket(
                        expiry.put_skew_steepening_5m,
                        settings.skew_steepening_threshold,
                    ),
                )
            )
        if (
            expiry.iv_surface_shift_5m is not None
            and abs(expiry.iv_surface_shift_5m) >= settings.surface_shift_threshold
        ):
            alerts.append(
                Alert(
                    severity=iv_surface_movement_severity(
                        window,
                        value=expiry.iv_surface_shift_5m,
                        threshold=settings.surface_shift_threshold,
                        degraded=degraded,
                        degraded_threshold_multiplier=settings.degraded_threshold_multiplier,
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
                    threshold=settings.surface_shift_threshold,
                    quality=expiry.surface_fit_quality if degraded else None,
                    source_gate="iv_surface",
                    dedup_group=magnitude_bucket(
                        expiry.iv_surface_shift_5m,
                        settings.surface_shift_threshold,
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

