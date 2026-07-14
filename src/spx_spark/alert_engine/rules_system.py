from __future__ import annotations

import json
import os
from dataclasses import replace
from datetime import datetime
from pathlib import Path

from spx_spark.alert_engine.constants import (
    IBKR_INTERRUPTED_SESSION_STATUSES,
    IBKR_TRANSITIONAL_SESSION_STATUSES,
)
from spx_spark.alert_engine.rules_data import find_best
from spx_spark.alert_model import Alert, severity_for_priority
from spx_spark.alert_profile import AlertWindow
from spx_spark.config import env_bool, env_float
from spx_spark.marketdata import Provider, ProviderState, ProviderStatus, as_utc, parse_timestamp
from spx_spark.options_map import OptionsMap
from spx_spark.position_alerts import has_open_spxw_positions
from spx_spark.provider_failover import FailoverMode, FailoverState
from spx_spark.provider_failover_controller import (
    ProviderFailoverSettings,
    load_failover_control,
)
from spx_spark.settings import DEFAULT_ALERT_SETTINGS
from spx_spark.storage import LatestState, configured_quote_use_decision


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
        DEFAULT_ALERT_SETTINGS.broker_state_max_age_seconds,
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
    gth_option_gap = any(
        "GTH " in str(reason or "")
        for reason in (failover_state.last_schwab_reason, failover_state.last_ibkr_reason)
    )
    if transition.mode == FailoverMode.IBKR_FALLBACK:
        title = (
            "Schwab GTH SPXW 报价不足，IBKR 期权备用已接管"
            if gth_option_gap
            else "Schwab 异常，IBKR 备用行情已接管"
        )
        detail = (
            "Schwab 连接和 ES 锚可能仍正常，但 GTH SPXW 覆盖未通过定价门；"
            "系统已改用 IBKR 期权报价，交易闸门继续按期权覆盖判断。"
            if gth_option_gap
            else (
                "SPX/ES 直接行情已切换到 IBKR L1 备用通道；"
                "系统保持风控，但不会因为切换本身反复推送离线消息。"
            )
        )
        return Alert(
            severity="high",
            kind="market_data_ibkr_fallback_activated",
            instrument_id="index:SPX",
            title=title,
            detail=detail,
            provider=Provider.IBKR.value,
            quality=failover_state.mode.value,
            research_only=False,
            source_gate="provider_failover_state",
            dedup_group=transition.transition_id,
        )
    if transition.mode == FailoverMode.BOTH_UNAVAILABLE:
        title = (
            "Schwab/IBKR 的 GTH SPXW 报价均不可用"
            if gth_option_gap
            else "Schwab 与 IBKR 直接行情均不可用"
        )
        detail = (
            "ES 连续行情可能仍正常，但两个来源都未提供足够的新鲜 SPXW call/put 对；"
            "期权定价和新开仓闸门关闭，只允许核对已有仓位。"
            if gth_option_gap
            else "两个直接行情源均未通过健康门；禁止新开仓，只允许人工核对和已有仓位处置。"
        )
        return Alert(
            severity="critical",
            kind="market_data_all_providers_unavailable",
            instrument_id="index:SPX",
            title=title,
            detail=detail,
            provider=Provider.INTERNAL.value,
            quality=failover_state.mode.value,
            research_only=False,
            source_gate="provider_failover_state",
            dedup_group=transition.transition_id,
        )
    if transition.mode == FailoverMode.SCHWAB_PRIMARY:
        if transition.previous_mode == FailoverMode.RECOVERY_PENDING:
            # No provider switch or user-visible outage occurred. Persist the
            # transition for audit, but do not page a self-healed health probe.
            return None
        if transition.previous_mode == FailoverMode.BOTH_UNAVAILABLE:
            title = (
                "Schwab GTH SPXW 报价恢复"
                if gth_option_gap
                else "Schwab 连续稳定，主行情已恢复"
            )
            detail = (
                "Schwab SPXW call/put 覆盖连续通过定价门，新开仓闸门已恢复。"
                if gth_option_gap
                else "Schwab 锚点连续通过健康门，双源不可用状态已解除。"
            )
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
        DEFAULT_ALERT_SETTINGS.ibkr_execution_mode,
    ).strip().lower()
    if execution_mode == "live":
        return True
    return has_open_spxw_positions()


def system_event_alerts(state: LatestState, *, persist: bool = True) -> list[Alert]:
    if not env_bool(
        "ALERT_SYSTEM_EVENTS_ENABLED",
        DEFAULT_ALERT_SETTINGS.system_events_enabled,
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
    from spx_spark.alert_engine.constants import MOVE_THRESHOLDS_BPS
    from spx_spark.alert_engine.rules_price import (
        move_from_close_bps,
        movement_threshold_for_window,
    )

    if not env_bool(
        "ALERT_ALLOW_BROKER_UNAVAILABLE_PROXY_WATCH",
        DEFAULT_ALERT_SETTINGS.allow_broker_unavailable_proxy_watch,
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
