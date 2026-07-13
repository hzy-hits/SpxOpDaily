"""Runtime entrypoint for confirmed ES Globex trend transitions."""

from __future__ import annotations

import argparse
import json
from datetime import datetime, timedelta, timezone
from typing import Any
from zoneinfo import ZoneInfo

from spx_spark.alert_model import Alert
from spx_spark.application.globex_trend.machine import advance_trend_state
from spx_spark.application.globex_trend.models import REGIME_LABELS_CN
from spx_spark.application.globex_trend.state import (
    load_trend_state,
    locked_trend_state,
    save_trend_state,
    trend_state_path,
)
from spx_spark.config import NotificationSettings, StorageSettings
from spx_spark.market_calendar import DEFAULT_MARKET_CALENDAR
from spx_spark.marketdata import MarketDataQuality, Provider, Quote, as_utc
from spx_spark.notifier import notify_payload
from spx_spark.settings import load_app_settings
from spx_spark.settings.globex_trend import GlobexTrendSettings
from spx_spark.storage import LatestState, LatestStateStore


ET = ZoneInfo("America/New_York")
PROVIDER_PRIORITY = (Provider.SCHWAB, Provider.IBKR)


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run the ES Globex trend state machine.")
    parser.add_argument("--json", action="store_true")
    parser.add_argument("--no-notify", action="store_true")
    return parser.parse_args(argv)


def globex_session_id(now: datetime) -> str:
    local = as_utc(now).astimezone(ET)
    business_date = local.date() + timedelta(days=1) if local.hour >= 18 else local.date()
    return business_date.isoformat()


def select_live_es(
    state: LatestState,
    *,
    now: datetime,
    policy: GlobexTrendSettings,
) -> Quote | None:
    now = as_utc(now)
    provider_rank = {provider: index for index, provider in enumerate(PROVIDER_PRIORITY)}
    matches: list[Quote] = []
    for quote in state.quotes:
        if (
            quote.provider not in provider_rank
            or quote.instrument.canonical_id != "future:ES"
            or quote.effective_price is None
            or quote.quality is not MarketDataQuality.LIVE
        ):
            continue
        transport_at = as_utc(quote.last_update_at or quote.received_at)
        source_at = as_utc(
            quote.quote_time
            or quote.trade_time
            or quote.last_update_at
            or quote.received_at
        )
        if max(
            (now - transport_at).total_seconds(),
            (now - source_at).total_seconds(),
        ) <= policy.max_quote_age_seconds:
            matches.append(quote)
    if not matches:
        return None
    return max(
        matches,
        key=lambda quote: (
            as_utc(
                quote.quote_time
                or quote.trade_time
                or quote.last_update_at
                or quote.received_at
            ),
            -provider_rank[quote.provider],
        ),
    )


def alert_from_event(event: dict[str, Any]) -> Alert:
    metrics = event.get("metrics") if isinstance(event.get("metrics"), dict) else {}
    target = str(event["to_regime"])
    prior = str(event["from_regime"])
    direction = "偏空" if target == "bearish" else "偏多"
    detail = (
        f"ES Globex 趋势确认切换：{REGIME_LABELS_CN.get(prior, prior)} → "
        f"{REGIME_LABELS_CN.get(target, target)}，当前 {float(event['price']):.2f}。"
        f"15m {format_points(metrics.get('return_15m_points'))}，"
        f"60m {format_points(metrics.get('return_60m_points'))}，"
        f"180m {format_points(metrics.get('return_180m_points'))}；"
        f"距当前趋势腿高点 "
        f"{format_points(metrics.get('drawdown_from_regime_high_points'))}，"
        f"距当前趋势腿低点 "
        f"{format_points(metrics.get('rebound_from_regime_low_points'))}。"
        f"当前路径判断：{direction}；这是趋势状态切换，不是自动下单。"
    )
    return Alert(
        severity="high",
        kind="globex_trend_transition",
        instrument_id="future:ES",
        title=f"ES Globex {REGIME_LABELS_CN.get(target, target)}确认",
        detail=detail,
        provider=str(event.get("provider") or ""),
        quality="live",
        value=float(event["price"]),
        research_only=False,
        source_gate="globex_trend_machine",
        dedup_group=str(event["event_id"]),
        event_id=str(event["event_id"]),
        source_at=str(event.get("source_at") or event["at"]),
    )


def format_points(value: object) -> str:
    if not isinstance(value, int | float):
        return "-"
    return f"{float(value):+.1f}点"


def run(
    argv: list[str] | None = None,
    *,
    now: datetime | None = None,
) -> int:
    args = parse_args(argv)
    evaluation_now = as_utc(now or datetime.now(tz=timezone.utc))
    policy = load_app_settings().globex_trend
    storage = StorageSettings.from_env()
    output: dict[str, Any] = {"ok": True, "at": evaluation_now.isoformat()}
    if not policy.enabled:
        output["skipped_reason"] = "disabled"
    elif not DEFAULT_MARKET_CALENDAR.is_globex_open(evaluation_now):
        output["skipped_reason"] = "globex_closed"
    else:
        latest = LatestStateStore(storage).load(now=evaluation_now)
        quote = select_live_es(latest, now=evaluation_now, policy=policy)
        if quote is None:
            output["ok"] = False
            output["skipped_reason"] = "no_fresh_direct_es"
        else:
            path = trend_state_path(storage.data_root)
            source_at = as_utc(
                quote.quote_time
                or quote.trade_time
                or quote.last_update_at
                or quote.received_at
            )
            with locked_trend_state(path):
                state = load_trend_state(path)
                state, transition = advance_trend_state(
                    state,
                    session_id=globex_session_id(evaluation_now),
                    at=evaluation_now,
                    price=float(quote.effective_price),
                    provider=quote.provider.value,
                    source_at=source_at,
                    policy=policy,
                )
                pending = pending_event(state, now=evaluation_now, policy=policy)
                save_trend_state(path, state)
            output.update(
                {
                    "regime": state.get("regime"),
                    "candidate_regime": state.get("candidate_regime"),
                    "candidate_observations": state.get("candidate_observations"),
                    "metrics": state.get("metrics"),
                    "transition": transition,
                    "provider": quote.provider.value,
                }
            )
            if pending is not None and not args.no_notify:
                alert = alert_from_event(pending)
                payload = {
                    "created_at": evaluation_now.isoformat(),
                    "as_of": evaluation_now.isoformat(),
                    "alerts": [alert.to_dict()],
                    "alert_count": 1,
                    "globex_trend": state,
                }
                result = notify_payload(
                    payload,
                    settings=NotificationSettings.from_env(),
                    now=evaluation_now,
                    record_telemetry=False,
                )
                output["notification"] = result.to_dict()
                if alert.event_id in set(result.acknowledged_event_ids):
                    with locked_trend_state(path):
                        latest_state = load_trend_state(path)
                        if (
                            isinstance(latest_state.get("pending_event"), dict)
                            and latest_state["pending_event"].get("event_id") == alert.event_id
                        ):
                            latest_state["pending_event"] = None
                            save_trend_state(path, latest_state)
    if args.json:
        print(json.dumps(output, sort_keys=True))
    return 0 if output["ok"] else 1


def pending_event(
    state: dict[str, Any],
    *,
    now: datetime,
    policy: GlobexTrendSettings,
) -> dict[str, Any] | None:
    event = state.get("pending_event")
    if not isinstance(event, dict):
        return None
    try:
        created_at = datetime.fromisoformat(str(event["at"]))
    except (KeyError, ValueError):
        state["pending_event"] = None
        return None
    if (now - created_at).total_seconds() > policy.pending_event_ttl_seconds:
        state["pending_event"] = None
        return None
    return event


def main() -> None:
    raise SystemExit(run())


if __name__ == "__main__":
    main()
