"""Runtime entrypoint for confirmed ES Globex trend transitions."""

from __future__ import annotations

import argparse
import json
from dataclasses import dataclass
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
from spx_spark.macro_event_clock import macro_event_state
from spx_spark.market_calendar import DEFAULT_MARKET_CALENDAR
from spx_spark.marketdata import (
    FUTURE_TIMESTAMP_TOLERANCE_SECONDS,
    MarketDataQuality,
    Provider,
    Quote,
    as_utc,
    instrument_matches_id,
)
from spx_spark.notifier import notify_payload
from spx_spark.notifier.state import load_acknowledged_event_ids
from spx_spark.settings import load_app_settings
from spx_spark.settings.globex_trend import GlobexTrendSettings
from spx_spark.storage import LatestState, LatestStateStore


ET = ZoneInfo("America/New_York")
PROVIDER_PRIORITY = (Provider.SCHWAB, Provider.IBKR)


@dataclass(frozen=True)
class LiveEsObservation:
    quote: Quote
    price: float
    price_kind: str
    source_at: datetime

    @property
    def provider(self) -> Provider:
        return self.quote.provider


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run the ES Globex trend state machine.")
    parser.add_argument("--json", action="store_true")
    parser.add_argument(
        "--no-notify",
        action="store_true",
        help="Record trend transitions without sending human notifications.",
    )
    return parser.parse_args(argv)


def globex_session_id(now: datetime) -> str:
    local = as_utc(now).astimezone(ET)
    business_date = local.date() + timedelta(days=1) if local.hour >= 18 else local.date()
    return business_date.isoformat()


def trend_context_id(now: datetime) -> str:
    """Reset formal trend confirmation at SPX GTH and cash-session boundaries."""

    session_id = globex_session_id(now)
    if DEFAULT_MARKET_CALENDAR.is_rth_open(now):
        return f"{session_id}:rth"
    if DEFAULT_MARKET_CALENDAR.is_spx_gth_open(now):
        return f"{session_id}:gth"
    return f"{session_id}:globex"


def gth_advisory_allowed(
    now: datetime,
    macro_state: dict[str, Any],
) -> bool:
    return bool(
        DEFAULT_MARKET_CALENDAR.is_spx_gth_open(now)
        and macro_state.get("mode") == "normal"
        and macro_state.get("entry_allowed") is True
    )


def select_live_es(
    state: LatestState,
    *,
    now: datetime,
    policy: GlobexTrendSettings,
) -> LiveEsObservation | None:
    now = as_utc(now)
    provider_rank = {provider: index for index, provider in enumerate(PROVIDER_PRIORITY)}
    matches: list[LiveEsObservation] = []
    for quote in state.quotes:
        observation = _es_price_observation(quote)
        if (
            quote.provider not in provider_rank
            or not instrument_matches_id(quote.instrument, "future:ES")
            or observation is None
            or quote.quality is not MarketDataQuality.LIVE
        ):
            continue
        price, price_kind, source_at = observation
        transport_at = as_utc(quote.last_update_at or quote.received_at)
        transport_age = (now - transport_at).total_seconds()
        source_age = (now - source_at).total_seconds()
        if (
            -FUTURE_TIMESTAMP_TOLERANCE_SECONDS <= transport_age <= policy.max_quote_age_seconds
            and -FUTURE_TIMESTAMP_TOLERANCE_SECONDS <= source_age <= policy.max_quote_age_seconds
        ):
            matches.append(
                LiveEsObservation(
                    quote=quote,
                    price=price,
                    price_kind=price_kind,
                    source_at=source_at,
                )
            )
    if not matches:
        return None
    preferred = (
        Provider.IBKR
        if DEFAULT_MARKET_CALENDAR.is_spx_gth_open(now)
        else Provider.SCHWAB
        if DEFAULT_MARKET_CALENDAR.is_rth_open(now)
        else None
    )
    preferred_matches = [
        observation for observation in matches if observation.provider is preferred
    ]
    if preferred_matches:
        matches = preferred_matches
    return max(
        matches,
        key=lambda observation: (
            observation.source_at,
            -provider_rank[observation.provider],
        ),
    )


def _es_price_observation(
    quote: Quote,
) -> tuple[float, str, datetime] | None:
    if (
        quote.bid is not None
        and quote.mid is not None
        and quote.ask is not None
        and 0 < quote.bid <= quote.mid <= quote.ask
        and quote.quote_time is not None
    ):
        return float(quote.mid), "mid", as_utc(quote.quote_time)
    if quote.last is not None and quote.last > 0 and quote.trade_time is not None:
        return float(quote.last), "last", as_utc(quote.trade_time)
    return None


def alert_from_event(event: dict[str, Any]) -> Alert:
    if event.get("event_type") == "continuation":
        return continuation_alert_from_event(event)
    metrics = event.get("metrics") if isinstance(event.get("metrics"), dict) else {}
    target = str(event["to_regime"])
    prior = str(event["from_regime"])
    direction = "偏空" if target == "bearish" else "偏多"
    observed_at = as_utc(datetime.fromisoformat(str(event["at"])))
    session_label = "RTH" if DEFAULT_MARKET_CALENDAR.is_rth_open(observed_at) else "Globex"
    session_context = (
        "当前处于 SPX 现金交易时段，ES 是连续领先确认源；不得按夜盘薄流动性解释。"
        if session_label == "RTH"
        else "当前处于现金盘外，ES 路径用于 Globex 连续价格发现。"
    )
    detail = (
        f"ES {session_label} 趋势确认切换：{REGIME_LABELS_CN.get(prior, prior)} → "
        f"{REGIME_LABELS_CN.get(target, target)}，当前 {float(event['price']):.2f}。"
        f"15m {format_points(metrics.get('return_15m_points'))}，"
        f"60m {format_points(metrics.get('return_60m_points'))}，"
        f"180m {format_points(metrics.get('return_180m_points'))}；"
        f"距当前趋势腿高点 "
        f"{format_points(metrics.get('drawdown_from_regime_high_points'))}，"
        f"距当前趋势腿低点 "
        f"{format_points(metrics.get('rebound_from_regime_low_points'))}。"
        f"当前路径判断：{direction}；{session_context}这是趋势状态切换，不是自动下单。"
    )
    invalidated_advisory_id = str(event.get("invalidated_advisory_id") or "")
    if invalidated_advisory_id:
        detail += (
            f"前序 GTH 方向机会 {invalidated_advisory_id} 已失效；"
            "不得继续按原 Call/Put advisory 管理。"
        )
    return Alert(
        severity="high",
        kind=("gth_advisory_invalidated" if invalidated_advisory_id else "globex_trend_transition"),
        instrument_id="future:ES",
        title=f"ES {session_label} {REGIME_LABELS_CN.get(target, target)}确认",
        detail=detail,
        provider=str(event.get("provider") or ""),
        quality="live",
        value=float(event["price"]),
        # A raw ES regime flip is audit context.  It becomes human-visible only
        # when it explicitly invalidates an accepted advisory lifecycle.
        research_only=not bool(invalidated_advisory_id),
        source_gate="globex_trend_machine",
        dedup_group=str(event["event_id"]),
        event_id=str(event["event_id"]),
        source_at=str(event.get("source_at") or event["at"]),
        audit_context=(
            {
                "invalidated_advisory_id": invalidated_advisory_id,
                "advisory_lifecycle_action": "invalidated",
                "execution_eligible": False,
                "automatic_ordering": False,
            }
            if invalidated_advisory_id
            else None
        ),
    )


def continuation_alert_from_event(event: dict[str, Any]) -> Alert:
    metrics = event.get("metrics") if isinstance(event.get("metrics"), dict) else {}
    observed_at = as_utc(datetime.fromisoformat(str(event["at"])))
    session_label = "GTH" if DEFAULT_MARKET_CALENDAR.is_spx_gth_open(observed_at) else "Globex"
    regime = str(event["regime"])
    label = REGIME_LABELS_CN.get(regime, regime)
    extension = float(event["extension_points"])
    milestone = int(event["milestone_index"])
    option_right = str(event.get("option_right") or ("C" if regime == "bullish" else "P")).upper()
    option_label = "CALL" if option_right == "C" else "PUT"
    signal_stage = str(
        event.get("signal_stage")
        or ("entry_advisory" if milestone == 1 else "opportunity_management")
    )
    common = (
        f"ES {session_label} {label}延续 m{milestone}：自确认锚点 "
        f"{float(event['anchor_price']):.2f} 顺势延伸 {extension:.1f}点，"
        f"当前 {float(event['price']):.2f}；"
        f"15m {format_points(metrics.get('return_15m_points'))}，"
        f"60m {format_points(metrics.get('return_60m_points'))}。"
    )
    if signal_stage == "entry_advisory":
        kind = "gth_directional_advisory"
        title = f"SPX {session_label} {option_label} 机会 · m1"
        detail = (
            f"{common}方向提示：评估 {option_label} 机会。"
            "这是独立 GTH advisory；当前只由实时 ES 确认方向，"
            "未授权 exact-expiry SPXW 合约、入场限价或现金 SPX 坐标。"
            "EXECUTION_ELIGIBLE=NO，自动下单关闭。"
        )
        source_gate = "gth_directional_advisory_v1"
        dedup_group = str(event.get("advisory_id") or event["event_id"])
    else:
        kind = "gth_advisory_management"
        title = f"SPX {session_label} {option_label} 机会管理 · m2"
        detail = (
            f"{common}若已人工参与前序 {option_label} advisory，"
            "此处只用于止盈或抬高止损；未验证持仓时仅更新机会生命周期，"
            f"禁止把 m2 当作新的 {option_label} 入场。"
            "EXECUTION_ELIGIBLE=NO，自动下单关闭。"
        )
        source_gate = "gth_advisory_management_v1"
        dedup_group = str(event["event_id"])
    audit_context = {
        "advisory_contract_version": event.get("advisory_contract_version"),
        "advisory_id": event.get("advisory_id"),
        "signal_stage": signal_stage,
        "direction": event.get("direction"),
        "option_right": option_right,
        "parent_advisory_id": event.get("parent_advisory_id"),
        "signal_coordinate": event.get("signal_coordinate"),
        "option_coordinate_status": event.get("option_coordinate_status"),
        "quote_attachment_status": event.get("quote_attachment_status"),
        "contract_id": event.get("contract_id"),
        "entry_limit": event.get("entry_limit"),
        "execution_eligible": False,
        "automatic_ordering": False,
        "execution_block_reasons": event.get("execution_block_reasons"),
    }
    return Alert(
        severity="high",
        kind=kind,
        instrument_id="future:ES",
        title=title,
        detail=detail,
        provider=str(event.get("provider") or ""),
        quality="live",
        value=float(event["price"]),
        threshold=float(event.get("threshold_points") or extension),
        # Direction-only m1/m2 events have no exact option coordinate or entry
        # authority.  READY cards are emitted by the execution-contract lane.
        research_only=True,
        source_gate=source_gate,
        dedup_group=dedup_group,
        event_id=str(event["event_id"]),
        source_at=str(event.get("source_at") or event["at"]),
        audit_context=audit_context,
    )


def format_points(value: object) -> str:
    if not isinstance(value, int | float):
        return "-"
    return f"{float(value):+.1f}点"


def run(
    argv: list[str] | None = None,
    *,
    now: datetime | None = None,
    unavailable_is_error: bool = True,
) -> int:
    args = parse_args(argv)
    evaluation_now = as_utc(now or datetime.now(tz=timezone.utc))
    policy = load_app_settings().globex_trend
    storage = StorageSettings.from_env()
    notification_settings = NotificationSettings.from_env()
    output: dict[str, Any] = {"ok": True, "at": evaluation_now.isoformat()}
    if not policy.enabled:
        output["skipped_reason"] = "disabled"
    elif not DEFAULT_MARKET_CALENDAR.is_globex_open(evaluation_now):
        output["skipped_reason"] = "globex_closed"
    else:
        latest = LatestStateStore(storage).load(now=evaluation_now)
        observation = select_live_es(latest, now=evaluation_now, policy=policy)
        if observation is None:
            output["ok"] = False
            output["skipped_reason"] = "no_fresh_direct_es"
        else:
            path = trend_state_path(storage.data_root)
            macro_state = macro_event_state(evaluation_now)
            continuation_allowed = gth_advisory_allowed(evaluation_now, macro_state)
            with locked_trend_state(path):
                state = load_trend_state(path)
                reconcile_acknowledged_advisory(
                    state,
                    load_acknowledged_event_ids(notification_settings.state_path),
                    accepted_at=evaluation_now,
                )
                state, event = advance_trend_state(
                    state,
                    session_id=trend_context_id(evaluation_now),
                    at=evaluation_now,
                    price=observation.price,
                    provider=observation.provider.value,
                    source_at=observation.source_at,
                    policy=policy,
                    continuation_allowed=continuation_allowed,
                )
                pending = pending_event(state, now=evaluation_now, policy=policy)
                save_trend_state(path, state)
            output.update(
                {
                    "regime": state.get("regime"),
                    "candidate_regime": state.get("candidate_regime"),
                    "candidate_observations": state.get("candidate_observations"),
                    "metrics": state.get("metrics"),
                    "transition": (
                        event
                        if isinstance(event, dict) and event.get("event_type") != "continuation"
                        else None
                    ),
                    "continuation": (
                        event
                        if isinstance(event, dict) and event.get("event_type") == "continuation"
                        else None
                    ),
                    "provider": observation.provider.value,
                    "macro_event": macro_state,
                    "continuation_allowed": continuation_allowed,
                    "notification_policy": "direct_market_warning",
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
                    settings=notification_settings,
                    now=evaluation_now,
                    record_telemetry=False,
                )
                output["notification"] = result.to_dict()
                if alert.event_id in set(result.acknowledged_event_ids):
                    with locked_trend_state(path):
                        latest_state = load_trend_state(path)
                        latest_pending = latest_state.get("pending_event")
                        if (
                            isinstance(latest_pending, dict)
                            and latest_pending.get("event_id") == alert.event_id
                        ):
                            acknowledge_advisory_delivery(
                                latest_state,
                                latest_pending,
                                accepted_at=evaluation_now,
                            )
                            latest_state["pending_event"] = None
                            save_trend_state(path, latest_state)
    if args.json:
        print(json.dumps(output, sort_keys=True))
    if output["ok"] or (
        not unavailable_is_error
        and output.get("skipped_reason") == "no_fresh_direct_es"
    ):
        return 0
    return 1


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
        expire_pending_advisory(state, event)
        state["pending_event"] = None
        return None
    return event


def acknowledge_advisory_delivery(
    state: dict[str, Any],
    event: dict[str, Any],
    *,
    accepted_at: datetime,
) -> None:
    if event.get("signal_stage") != "entry_advisory":
        return
    advisory_id = str(event.get("advisory_id") or "")
    if advisory_id and state.get("pending_directional_advisory_id") == advisory_id:
        state["pending_directional_advisory_id"] = None
        state["active_directional_advisory_id"] = advisory_id
        state["active_directional_advisory_accepted_at"] = as_utc(accepted_at).isoformat()


def reconcile_acknowledged_advisory(
    state: dict[str, Any],
    acknowledged_event_ids: tuple[str, ...],
    *,
    accepted_at: datetime,
) -> bool:
    """Recover the advisory lifecycle after notification/state crash windows."""

    event = state.get("pending_event")
    if (
        not isinstance(event, dict)
        or event.get("signal_stage") != "entry_advisory"
        or str(event.get("event_id") or "") not in set(acknowledged_event_ids)
    ):
        return False
    acknowledge_advisory_delivery(state, event, accepted_at=accepted_at)
    if state.get("active_directional_advisory_id") != event.get("advisory_id"):
        return False
    state["pending_event"] = None
    return True


def expire_pending_advisory(
    state: dict[str, Any],
    event: dict[str, Any],
) -> None:
    advisory_id = str(event.get("advisory_id") or "")
    if (
        event.get("signal_stage") == "entry_advisory"
        and advisory_id
        and state.get("pending_directional_advisory_id") == advisory_id
    ):
        state["pending_directional_advisory_id"] = None
        state["continuation_suppressed_reason"] = "entry_advisory_delivery_expired"


def main() -> None:
    raise SystemExit(run())


if __name__ == "__main__":
    main()
