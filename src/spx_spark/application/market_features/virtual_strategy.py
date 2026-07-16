"""Broker-independent lifecycle for the system's own 0DTE strategy episode."""

from __future__ import annotations

import hashlib
import json
import os
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Mapping

from spx_spark.config import NotificationSettings, StorageSettings
from spx_spark.greek_reference import (
    calculate_contract_reference,
    inputs_from_quote,
    is_spxw_zero_dte,
)
from spx_spark.market_calendar import DEFAULT_MARKET_CALENDAR
from spx_spark.marketdata import OptionRight
from spx_spark.notifier.dispatcher import dispatch_notification
from spx_spark.notifier.model import CommandRunner, default_runner
from spx_spark.notifier.receipts import NotificationEnvelope
from spx_spark.settings.market_features import MarketFeatureSettings
from spx_spark.state_io import atomic_write_json_secure, exclusive_state_lock, read_json_object
from spx_spark.storage import LatestState, configured_quote_use_decision


def process_virtual_strategy(
    storage: StorageSettings,
    latest: LatestState,
    *,
    trade_intent: Mapping[str, object],
    gth_signal: Mapping[str, object],
    option_structure: Mapping[str, object],
    macro_event: Mapping[str, object],
    greek_decision: Mapping[str, object],
    now: datetime,
    policy: MarketFeatureSettings,
    notification: NotificationSettings | None = None,
    runner: CommandRunner = default_runner,
) -> dict[str, object]:
    """Open/update/close one virtual episode; never reads or writes broker positions."""

    now = _utc(now)
    if not policy.virtual_strategy_enabled:
        return {"status": "disabled", "notification_attempted": False}
    state_path = _state_path(storage)
    with exclusive_state_lock(state_path):
        state = read_json_object(state_path)
        active = dict(state.get("active") or {})
        consumed = set(str(item) for item in state.get("consumed_signal_ids") or [])
        if not active:
            active = _new_episode(
                latest,
                trade_intent=trade_intent,
                gth_signal=gth_signal,
                consumed=consumed,
                now=now,
                policy=policy,
            )
            if active:
                signal_id = str(active.get("source_signal_id") or "")
                if signal_id:
                    consumed.add(signal_id)
                _append_audit(storage, now, {"event": "virtual_opened", **active})
        if not active:
            state.update(
                {
                    "schema_version": 1,
                    "updated_at": now.isoformat(),
                    "active": None,
                    "consumed_signal_ids": sorted(consumed)[-200:],
                }
            )
            atomic_write_json_secure(state_path, state)
            return {"status": "observing", "notification_attempted": False}

        current = _contract_snapshot(latest, str(active.get("contract_id") or ""), now=now)
        exit_reason, action = _exit_decision(
            active,
            current,
            latest=latest,
            option_structure=option_structure,
            macro_event=macro_event,
            greek_decision=greek_decision,
            now=now,
            policy=policy,
        )
        active["last_observed_at"] = now.isoformat()
        if current:
            active["last"] = current
            entry_mid = _number(active.get("entry_mid"))
            current_mid = _number(current.get("mid"))
            if entry_mid and current_mid is not None:
                return_fraction = current_mid / entry_mid - 1.0
                active["mfe_fraction"] = max(
                    float(active.get("mfe_fraction", 0.0)), return_fraction
                )
                active["mae_fraction"] = min(
                    float(active.get("mae_fraction", 0.0)), return_fraction
                )
                _record_due_horizons(storage, active, current, now=now)
        if exit_reason is None:
            state.update(
                {
                    "schema_version": 1,
                    "updated_at": now.isoformat(),
                    "active": active,
                    "consumed_signal_ids": sorted(consumed)[-200:],
                }
            )
            atomic_write_json_secure(state_path, state)
            return {
                "status": "active",
                "episode_id": active.get("episode_id"),
                "contract_id": active.get("contract_id"),
                "notification_attempted": False,
            }

        closed = {
            **active,
            "status": "closed",
            "closed_at": now.isoformat(),
            "exit_reason": exit_reason,
            "exit_action": action,
            "exit_snapshot": current,
        }
        state.update(
            {
                "schema_version": 1,
                "updated_at": now.isoformat(),
                "active": None,
                "last_closed": closed,
                "consumed_signal_ids": sorted(consumed)[-200:],
            }
        )
        atomic_write_json_secure(state_path, state)
        _append_audit(storage, now, {"event": "virtual_closed", **closed})

    settings = notification or NotificationSettings.from_env()
    text = _render_exit(closed)
    result = dispatch_notification(
        settings,
        NotificationEnvelope(
            event_id=f"{closed['episode_id']}:{exit_reason}",
            source="virtual_strategy",
            kind="virtual_strategy_exit",
            lane="strategy_lifecycle",
            occurred_at=now,
        ),
        title="SPX VIRTUAL STRATEGY EXIT",
        text=text,
        friend=True,
        feishu_text=text,
        runner=runner,
        attempted_at=now,
    )
    return {
        "status": "closed",
        "episode_id": closed.get("episode_id"),
        "exit_reason": exit_reason,
        "exit_action": action,
        "notification_attempted": True,
        "notification_delivered": result.delivered,
        "sinks": [item.to_dict() for item in result.sinks],
    }


def _new_episode(
    latest: LatestState,
    *,
    trade_intent: Mapping[str, object],
    gth_signal: Mapping[str, object],
    consumed: set[str],
    now: datetime,
    policy: MarketFeatureSettings,
) -> dict[str, object]:
    if trade_intent.get("status") == "trade_ready":
        source_id = str(trade_intent.get("intent_id") or "")
        contract_id = str(trade_intent.get("contract_id") or "")
        if not source_id or source_id in consumed or not contract_id:
            return {}
        snapshot = _contract_snapshot(latest, contract_id, now=now)
        if not snapshot:
            return {}
        stop = _time(trade_intent.get("time_stop_at")) or now + timedelta(
            minutes=policy.trade_time_stop_minutes
        )
        return _episode(
            source_id=source_id,
            source_kind="trade_intent",
            direction=str(trade_intent.get("direction") or ""),
            contract_id=contract_id,
            snapshot=snapshot,
            now=now,
            stop=stop,
            invalidation_spx=_number(trade_intent.get("invalidation_spx")),
            target_spx=_number(trade_intent.get("target_spx")),
            invalidation_es=None,
        )
    if gth_signal.get("kind") != "gth_dip_reclaim_call":
        return {}
    if (
        str(gth_signal.get("session_date") or "")
        != DEFAULT_MARKET_CALENDAR.research_expiry(now).isoformat()
    ):
        return {}
    source_id = str(gth_signal.get("event_id") or "")
    if not source_id or source_id in consumed:
        return {}
    selected = _select_call(latest, now=now, policy=policy)
    if selected is None:
        return {}
    snapshot = _contract_snapshot(latest, selected.instrument.canonical_id, now=now)
    if not snapshot:
        return {}
    return _episode(
        source_id=source_id,
        source_kind="gth_dip_reclaim_call",
        direction="up",
        contract_id=selected.instrument.canonical_id,
        snapshot=snapshot,
        now=now,
        stop=now + timedelta(minutes=policy.virtual_gth_time_stop_minutes),
        invalidation_spx=None,
        target_spx=None,
        invalidation_es=_number(gth_signal.get("trough")),
    )


def _episode(
    *,
    source_id: str,
    source_kind: str,
    direction: str,
    contract_id: str,
    snapshot: Mapping[str, object],
    now: datetime,
    stop: datetime,
    invalidation_spx: float | None,
    target_spx: float | None,
    invalidation_es: float | None,
) -> dict[str, object]:
    if direction not in {"up", "down"}:
        return {}
    episode_id = "virtual:" + hashlib.sha256(f"{source_id}|{contract_id}".encode()).hexdigest()[:24]
    return {
        "schema_version": 1,
        "episode_id": episode_id,
        "status": "active",
        "source_signal_id": source_id,
        "source_kind": source_kind,
        "direction": direction,
        "contract_id": contract_id,
        "opened_at": now.isoformat(),
        "time_stop_at": stop.isoformat(),
        "entry_mid": snapshot.get("mid"),
        "entry_iv": snapshot.get("iv"),
        "entry_gamma": snapshot.get("gamma_per_point"),
        "entry_delta": snapshot.get("delta"),
        "invalidation_spx": invalidation_spx,
        "target_spx": target_spx,
        "invalidation_es": invalidation_es,
        "mfe_fraction": 0.0,
        "mae_fraction": 0.0,
        "automatic_ordering": False,
        "account_position_source": "none",
        "entry_basis": "decision_quote_snapshot",
        "execution_assumption": "none",
        "last": dict(snapshot),
    }


def _exit_decision(
    active: Mapping[str, object],
    current: Mapping[str, object],
    *,
    latest: LatestState,
    option_structure: Mapping[str, object],
    macro_event: Mapping[str, object],
    greek_decision: Mapping[str, object],
    now: datetime,
    policy: MarketFeatureSettings,
) -> tuple[str | None, str | None]:
    stop = _time(active.get("time_stop_at"))
    if stop is not None and now >= stop:
        return "time_stop", "exit"
    if not current:
        return None, None
    spx = _spx_reference(latest, current)
    es_quote = latest.best_quote("future:ES")
    es = _number(es_quote.effective_price) if es_quote is not None else None
    invalidation_spx = _number(active.get("invalidation_spx"))
    invalidation_es = _number(active.get("invalidation_es"))
    direction = str(active.get("direction") or "")
    if invalidation_spx is not None and spx is not None:
        invalidated = (direction == "up" and spx <= invalidation_spx) or (
            direction == "down" and spx >= invalidation_spx
        )
        if invalidated:
            return "strategy_invalidation", "exit"
    if invalidation_es is not None and es is not None and es <= invalidation_es:
        return "gth_dip_low_broken", "exit"
    entry_mid = _number(active.get("entry_mid"))
    mid = _number(current.get("mid"))
    if (
        entry_mid
        and mid is not None
        and mid / entry_mid - 1.0 >= policy.virtual_profit_take_fraction
    ):
        return "premium_profit_target", "take_profit_or_reduce"
    target_spx = _number(active.get("target_spx"))
    if target_spx is not None and spx is not None:
        target_reached = (direction == "up" and spx >= target_spx) or (
            direction == "down" and spx <= target_spx
        )
        if target_reached:
            return "underlier_target_reached", "take_profit"
    call_wall = _number(option_structure.get("call_wall"))
    if (
        active.get("source_kind") == "gth_dip_reclaim_call"
        and call_wall is not None
        and spx is not None
        and spx >= call_wall - policy.virtual_wall_touch_points
    ):
        return "call_wall_touched", "take_profit_or_reduce"
    quality = current.get("quality") if isinstance(current.get("quality"), Mapping) else {}
    greek_exit_allowed = bool(
        greek_decision.get("mode") == "decision_grade" and quality.get("status") == "ok"
    )
    delta = abs(_number(current.get("delta")) or 0.0)
    if greek_exit_allowed and delta >= policy.greek_delta_saturation:
        return "delta_saturated", "reduce"
    entry_iv = _number(active.get("entry_iv"))
    iv = _number(current.get("iv"))
    vanna = _number(current.get("vanna_delta_per_vol_point"))
    if (
        greek_exit_allowed
        and macro_event.get("mode") == "post_event"
        and entry_iv is not None
        and iv is not None
        and (entry_iv - iv) * 100.0 >= policy.virtual_iv_drop_vol_points
        and vanna is not None
        and vanna > 0
    ):
        return "post_event_iv_crush_vanna_drag", "take_profit_or_exit"
    entry_gamma = _number(active.get("entry_gamma"))
    gamma = _number(current.get("gamma_per_point"))
    color = _number(current.get("color_gamma_per_minute"))
    if (
        greek_exit_allowed
        and entry_gamma is not None
        and entry_gamma > 0
        and gamma is not None
        and gamma / entry_gamma <= policy.virtual_gamma_retention_fraction
        and color is not None
        and color < 0
    ):
        return "gamma_convexity_decayed", "exit"
    return None, None


def _record_due_horizons(
    storage: StorageSettings,
    active: dict[str, object],
    current: Mapping[str, object],
    *,
    now: datetime,
) -> None:
    opened = _time(active.get("opened_at"))
    if opened is None:
        return
    elapsed = (now - opened).total_seconds() / 60.0
    horizons = dict(active.get("horizon_outcomes") or {})
    entry_mid = _number(active.get("entry_mid"))
    current_mid = _number(current.get("mid"))
    for minutes in (5, 15, 30):
        key = str(minutes)
        if key in horizons or elapsed < minutes or not entry_mid or current_mid is None:
            continue
        row = {
            "horizon_minutes": minutes,
            "observed_at": now.isoformat(),
            "end_return_fraction": current_mid / entry_mid - 1.0,
            "mfe_fraction": active.get("mfe_fraction"),
            "mae_fraction": active.get("mae_fraction"),
            "delta": current.get("delta"),
            "gamma_per_point": current.get("gamma_per_point"),
            "color_gamma_per_minute": current.get("color_gamma_per_minute"),
            "speed_gamma_per_point": current.get("speed_gamma_per_point"),
            "theta_per_minute": current.get("theta_per_minute"),
            "vanna_delta_per_vol_point": current.get("vanna_delta_per_vol_point"),
        }
        horizons[key] = row
        _append_audit(
            storage,
            now,
            {
                "event": "virtual_horizon_outcome",
                "episode_id": active.get("episode_id"),
                "contract_id": active.get("contract_id"),
                **row,
            },
        )
    active["horizon_outcomes"] = horizons


def _select_call(latest: LatestState, *, now: datetime, policy: MarketFeatureSettings):
    rows = []
    for quote in latest.best_quotes:
        if quote.instrument.right is not OptionRight.CALL or not is_spxw_zero_dte(quote, as_of=now):
            continue
        decision = configured_quote_use_decision(quote, as_of=now)
        delta = (
            abs(quote.greeks.delta)
            if quote.greeks is not None and quote.greeks.delta is not None
            else None
        )
        if not decision.pricing_allowed or quote.mid is None or delta is None:
            continue
        if not policy.greek_target_delta_min <= delta <= policy.greek_target_delta_max:
            continue
        rows.append(quote)
    return (
        min(rows, key=lambda row: (abs(abs(row.greeks.delta) - 0.50), row.spread_bps or 1e9))
        if rows
        else None
    )


def _contract_snapshot(
    latest: LatestState, contract_id: str, *, now: datetime
) -> dict[str, object]:
    quote = latest.best_quote(contract_id)
    if quote is None or quote.mid is None:
        return {}
    inputs, _quality = inputs_from_quote(quote, as_of=now)
    if inputs is None:
        return {}
    reference = calculate_contract_reference(inputs)
    return {
        "at": now.isoformat(),
        "mid": quote.mid,
        "iv": inputs.iv,
        "underlier": inputs.spot,
        "delta": reference.delta,
        "gamma_per_point": reference.gamma_per_point,
        "color_gamma_per_minute": reference.color_gamma_per_minute,
        "speed_gamma_per_point": reference.speed_gamma_per_point,
        "theta_per_minute": reference.theta_per_minute,
        "vanna_delta_per_vol_point": reference.vanna_delta_per_vol_point,
        "quality": reference.quality.to_dict(),
    }


def _spx_reference(latest: LatestState, current: Mapping[str, object]) -> float | None:
    quote = latest.best_quote("index:SPX")
    if quote is not None:
        decision = configured_quote_use_decision(quote, as_of=latest.as_of)
        price = _number(quote.effective_price)
        if decision.pricing_allowed and price is not None:
            return price
    return _number(current.get("underlier"))


def _render_exit(closed: Mapping[str, object]) -> str:
    snapshot = (
        closed.get("exit_snapshot") if isinstance(closed.get("exit_snapshot"), Mapping) else {}
    )
    return "\n".join(
        (
            f"虚拟策略｜{closed.get('exit_action')}",
            f"主策略 `{closed.get('source_kind')}`，合约 `{closed.get('contract_id')}`。",
            f"原因：`{closed.get('exit_reason')}`。",
            f"虚拟入场 {_fmt(closed.get('entry_mid'))}，当前 {_fmt(snapshot.get('mid'))}，MFE {_pct(closed.get('mfe_fraction'))} / MAE {_pct(closed.get('mae_fraction'))}。",
            "这是显示报价路径的影子生命周期，不读取 IBKR 仓位、不假设成交，也不自动下单。",
        )
    )


def _append_audit(storage: StorageSettings, now: datetime, payload: Mapping[str, object]) -> None:
    path = (
        Path(storage.data_root)
        / "features"
        / "virtual_strategy"
        / f"date={now.date().isoformat()}"
        / "events.jsonl"
    )
    path.parent.mkdir(parents=True, exist_ok=True)
    descriptor = os.open(path, os.O_APPEND | os.O_CREAT | os.O_WRONLY, 0o600)
    try:
        os.write(
            descriptor,
            (json.dumps(dict(payload), ensure_ascii=False, sort_keys=True) + "\n").encode(),
        )
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def _state_path(storage: StorageSettings) -> Path:
    return Path(storage.data_root) / "latest" / "virtual_strategy_state.json"


def _number(value: object) -> float | None:
    return float(value) if isinstance(value, int | float) else None


def _time(value: object) -> datetime | None:
    if not isinstance(value, str):
        return None
    try:
        return _utc(datetime.fromisoformat(value))
    except ValueError:
        return None


def _fmt(value: object) -> str:
    return f"{float(value):.2f}" if isinstance(value, int | float) else "-"


def _pct(value: object) -> str:
    return f"{float(value):.1%}" if isinstance(value, int | float) else "-"


def _utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)
