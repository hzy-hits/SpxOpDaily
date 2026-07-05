from __future__ import annotations

import argparse
import json
from dataclasses import asdict, dataclass
from dataclasses import replace
from datetime import datetime

from spx_spark.alert_profile import AlertWindow, active_window, parse_at
from spx_spark.config import IvSurfaceSettings, NotificationSettings, StorageSettings
from spx_spark.iv_surface import IvSurfaceSnapshot, load_latest_snapshot
from spx_spark.marketdata import MarketDataQuality, Quote
from spx_spark.notifier import notify_payload
from spx_spark.options_map import OptionsMap, build_options_map
from spx_spark.storage import LatestState, LatestStateStore


BASELINE_INSTRUMENTS = (
    "index:SPX",
    "index:VIX",
    "index:VVIX",
    "index:SKEW",
    "equity:SPY",
    "equity:QQQ",
    "equity:IWM",
    "equity:DIA",
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
ATM_IV_JUMP_THRESHOLD = 0.03
SKEW_STEEPENING_THRESHOLD = 0.08
SURFACE_SHIFT_THRESHOLD = 0.03
TERM_GAP_THRESHOLD = 0.05


@dataclass(frozen=True)
class Alert:
    severity: str
    kind: str
    instrument_id: str | None
    title: str
    detail: str
    provider: str | None = None
    quality: str | None = None
    value: float | None = None
    threshold: float | None = None

    def to_dict(self) -> dict[str, object]:
        return asdict(self)


def severity_for_priority(priority: str) -> str:
    return {
        "critical": "critical",
        "high": "high",
        "elevated": "medium",
        "normal": "medium",
        "low": "low",
        "off": "info",
    }.get(priority, "medium")


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


def movement_alert(
    *,
    quote: Quote,
    window: AlertWindow,
    threshold_bps: float,
) -> Alert | None:
    move_bps = move_from_close_bps(quote)
    if move_bps is None or abs(move_bps) < threshold_bps:
        return None

    instrument_id = quote.instrument.canonical_id
    direction = "up" if move_bps > 0 else "down"
    return Alert(
        severity=severity_for_priority(window.priority),
        kind="price_move_from_close",
        instrument_id=instrument_id,
        title=f"{instrument_id} {direction} {move_bps:.1f} bps from close",
        detail=(
            f"{instrument_id} effective price moved {move_bps:.1f} bps from close "
            f"during {window.name}."
        ),
        provider=quote.provider.value,
        quality=quote.quality.value,
        value=move_bps,
        threshold=threshold_bps,
    )


def evaluate_alerts(
    state: LatestState,
    *,
    window: AlertWindow,
    options_map: OptionsMap | None = None,
    iv_surface: IvSurfaceSnapshot | None = None,
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

    threshold = MOVE_THRESHOLDS_BPS.get(window.priority, MOVE_THRESHOLDS_BPS["normal"])
    for instrument_id in BASELINE_INSTRUMENTS:
        quote = find_best(state, instrument_id)
        if quote is None or quote.quality in BAD_QUALITIES:
            continue
        alert = movement_alert(quote=quote, window=window, threshold_bps=threshold)
        if alert is not None:
            alerts.append(alert)

    alerts.extend(option_map_alerts(options_map or build_options_map(state), window=window))
    if iv_surface is not None:
        alerts.extend(iv_surface_alerts(iv_surface, window=window))
    return alerts


def option_map_alerts(options_map: OptionsMap, *, window: AlertWindow) -> list[Alert]:
    alerts: list[Alert] = []
    underlier = options_map.underlier.price
    wall_threshold = max(10.0, underlier * 0.002 if underlier else 10.0)
    for expiry in options_map.expiries:
        if expiry.gamma_state in OPTION_GAMMA_ALERT_STATES:
            alerts.append(
                Alert(
                    severity=severity_for_priority(window.priority),
                    kind="option_gamma_regime",
                    instrument_id=f"option_map:SPXW:{expiry.expiry}",
                    title=f"SPXW {expiry.expiry} {expiry.gamma_state}",
                    detail=(
                        f"SPXW {expiry.expiry} gamma state is {expiry.gamma_state}; "
                        f"zero gamma={expiry.zero_gamma}, net_gamma_ratio={expiry.net_gamma_ratio}."
                    ),
                    value=expiry.net_gamma_ratio,
                )
            )
        if expiry.nearest_wall is not None and expiry.nearest_wall_distance_points is not None:
            distance = abs(expiry.nearest_wall_distance_points)
            if distance <= wall_threshold:
                alerts.append(
                    Alert(
                        severity=severity_for_priority(window.priority),
                        kind="option_wall_proximity",
                        instrument_id=f"option_map:SPXW:{expiry.expiry}",
                        title=(
                            f"SPX near SPXW wall {expiry.nearest_wall:.0f} "
                            f"({expiry.nearest_wall_distance_points:+.1f} pts)"
                        ),
                        detail=(
                            f"Nearest SPXW wall for {expiry.expiry} is "
                            f"{expiry.nearest_wall:.0f}; threshold={wall_threshold:.1f} pts."
                        ),
                        value=expiry.nearest_wall_distance_points,
                        threshold=wall_threshold,
                    )
                )
    return alerts


def iv_surface_alerts(surface: IvSurfaceSnapshot, *, window: AlertWindow) -> list[Alert]:
    alerts: list[Alert] = []
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
            )
        )
    for expiry in surface.expiries:
        instrument_id = f"iv_surface:SPXW:{expiry.expiry}"
        if expiry.surface_fit_quality in BAD_SURFACE_QUALITIES:
            alerts.append(
                Alert(
                    severity="low" if window.priority in {"low", "off"} else "medium",
                    kind="iv_surface_degraded",
                    instrument_id=instrument_id,
                    title=f"SPXW {expiry.expiry} surface {expiry.surface_fit_quality}",
                    detail=(
                        f"SPXW {expiry.expiry} IV surface quality is "
                        f"{expiry.surface_fit_quality}; alerts should discount this expiry."
                    ),
                    quality=expiry.surface_fit_quality,
                )
            )
        if expiry.atm_iv_jump_5m is not None and abs(expiry.atm_iv_jump_5m) >= ATM_IV_JUMP_THRESHOLD:
            alerts.append(
                Alert(
                    severity=severity_for_priority(window.priority),
                    kind="atm_iv_jump_5m",
                    instrument_id=instrument_id,
                    title=f"SPXW {expiry.expiry} ATM IV jump {expiry.atm_iv_jump_5m:.3f}",
                    detail=f"ATM IV changed {expiry.atm_iv_jump_5m:.3f} since the previous surface snapshot.",
                    value=expiry.atm_iv_jump_5m,
                    threshold=ATM_IV_JUMP_THRESHOLD,
                )
            )
        if (
            expiry.put_skew_steepening_5m is not None
            and expiry.put_skew_steepening_5m >= SKEW_STEEPENING_THRESHOLD
        ):
            alerts.append(
                Alert(
                    severity=severity_for_priority(window.priority),
                    kind="put_skew_steepening_5m",
                    instrument_id=instrument_id,
                    title=f"SPXW {expiry.expiry} put skew steepening {expiry.put_skew_steepening_5m:.3f}",
                    detail=(
                        f"Put skew ratio increased {expiry.put_skew_steepening_5m:.3f} "
                        "since the previous surface snapshot."
                    ),
                    value=expiry.put_skew_steepening_5m,
                    threshold=SKEW_STEEPENING_THRESHOLD,
                )
            )
        if (
            expiry.iv_surface_shift_5m is not None
            and abs(expiry.iv_surface_shift_5m) >= SURFACE_SHIFT_THRESHOLD
        ):
            alerts.append(
                Alert(
                    severity=severity_for_priority(window.priority),
                    kind="iv_surface_shift_5m",
                    instrument_id=instrument_id,
                    title=f"SPXW {expiry.expiry} surface shift {expiry.iv_surface_shift_5m:.3f}",
                    detail=(
                        f"Average raw-grid IV shifted {expiry.iv_surface_shift_5m:.3f} "
                        "since the previous surface snapshot."
                    ),
                    value=expiry.iv_surface_shift_5m,
                    threshold=SURFACE_SHIFT_THRESHOLD,
                )
            )
    return alerts


def load_current_iv_surface() -> IvSurfaceSnapshot | None:
    try:
        return load_latest_snapshot(IvSurfaceSettings.from_env().latest_surface_path)
    except (OSError, ValueError, json.JSONDecodeError, KeyError):
        return None


def evaluate_payload(state: LatestState, *, now: datetime | None = None) -> dict[str, object]:
    now = now or state.as_of
    window = active_window(now)
    options_map = build_options_map(state)
    iv_surface = load_current_iv_surface()
    alerts = evaluate_alerts(
        state,
        window=window,
        options_map=options_map,
        iv_surface=iv_surface,
    )
    return {
        "created_at": datetime.now(tz=now.tzinfo).isoformat(),
        "as_of": state.as_of.isoformat(),
        "window": window.to_dict(now=now),
        "options_map": options_map.to_dict(),
        "iv_surface": iv_surface.to_dict() if iv_surface is not None else None,
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
    payload = evaluate_payload(state, now=now or state.as_of)
    notification_settings = NotificationSettings.from_env()
    if args.notify:
        notification_settings = replace(notification_settings, enabled=True)
    if args.no_notify:
        notification_settings = replace(notification_settings, enabled=False)
    if notification_settings.enabled:
        payload["notification"] = notify_payload(payload, settings=notification_settings).to_dict()
    if args.json:
        print(json.dumps(payload, indent=2, sort_keys=True))
    else:
        print_alerts(payload)
    return 0


def main() -> None:
    raise SystemExit(run())


if __name__ == "__main__":
    main()
