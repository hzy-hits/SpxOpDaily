from __future__ import annotations

import argparse
import json
import os
from dataclasses import asdict, dataclass
from dataclasses import replace
from datetime import datetime

from spx_spark.alert_profile import AlertWindow, active_window, parse_at
from spx_spark.config import IvSurfaceSettings, NotificationSettings, StorageSettings
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
    research_only: bool = False
    source_gate: str | None = None

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


def env_float(name: str, default: float) -> float:
    raw = os.getenv(name)
    if not raw:
        return default
    return float(raw)


def env_bool(name: str, default: bool) -> bool:
    raw = os.getenv(name)
    if not raw:
        return default
    return raw.lower() in {"1", "true", "yes", "on"}


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
    market_context: dict[str, object] | None = None,
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
        if instrument_id.startswith("crypto_perp:") and not hyperliquid_proxy_usable(market_context):
            continue
        quote = find_best(state, instrument_id)
        if quote is None or quote.quality in BAD_QUALITIES:
            continue
        alert = movement_alert(quote=quote, window=window, threshold_bps=threshold)
        if alert is not None:
            alerts.append(alert)

    alerts.extend(option_map_alerts(options_map or build_options_map(state), window=window))
    if iv_surface is not None:
        alerts.extend(iv_surface_alerts(iv_surface, window=window))
    alerts.extend(market_context_alerts(market_context))
    alerts.extend(proxy_fallback_watch_alerts(state, window=window, market_context=market_context))
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


def proxy_fallback_watch_alerts(
    state: LatestState,
    *,
    window: AlertWindow,
    market_context: dict[str, object] | None,
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
    threshold = env_float(
        "ALERT_PROXY_FALLBACK_MOVE_BPS",
        MOVE_THRESHOLDS_BPS.get(window.priority, MOVE_THRESHOLDS_BPS["normal"]),
    )
    if move_bps is None or abs(move_bps) < threshold:
        return []

    direction = "up" if move_bps > 0 else "down"
    return [
        Alert(
            severity=severity_for_priority(window.priority),
            kind="broker_unavailable_proxy_watch",
            instrument_id="index:SPX",
            title=f"SPX fallback monitor {direction} {move_bps:.1f} bps",
            detail=(
                "Broker SPX/ES feed is unavailable, likely because the trading session is in use. "
                "Proxy-only monitor moved enough to open the trading device and verify real SPX/SPXW "
                "quotes before any decision."
            ),
            provider=quote.provider.value,
            quality="degraded",
            value=move_bps,
            threshold=threshold,
            research_only=False,
            source_gate="broker_unavailable_fallback",
        )
    ]


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


def load_current_iv_surface(settings: IvSurfaceSettings | None = None) -> IvSurfaceSnapshot | None:
    settings = settings or IvSurfaceSettings.from_env()
    try:
        return load_latest_snapshot(settings.latest_surface_path)
    except (OSError, ValueError, json.JSONDecodeError, KeyError):
        return None


def evaluate_payload(state: LatestState, *, now: datetime | None = None) -> dict[str, object]:
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
        market_context=market_context,
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
