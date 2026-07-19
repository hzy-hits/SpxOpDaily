"""Persistence, contract, and rendering helpers for virtual strategy episodes."""

from __future__ import annotations

import hashlib
import json
import math
import os
from datetime import datetime, time, timedelta, timezone
from pathlib import Path
from typing import Mapping

from spx_spark.config import StorageSettings
from spx_spark.greek_reference import calculate_contract_reference, inputs_from_quote
from spx_spark.market_calendar import DEFAULT_MARKET_CALENDAR, ET
from spx_spark.marketdata import InstrumentId
from spx_spark.settings.market_features import MarketFeatureSettings
from spx_spark.storage import LatestState, configured_quote_use_decision
from spx_spark.strategy_contract import (
    normalize_block_reasons,
    parse_aware_time,
    policy_version,
    strategy_event_fields,
)


def _render_exit(closed: Mapping[str, object]) -> str:
    snapshot = (
        closed.get("exit_snapshot") if isinstance(closed.get("exit_snapshot"), Mapping) else {}
    )
    contracts = str(closed.get("contract_id") or "-")
    if closed.get("position_type") == "call_debit_spread":
        contracts = f"{closed.get('long_contract_id')} / 卖 {closed.get('short_contract_id')}"
    return "\n".join(
        (
            f"虚拟策略｜{closed.get('exit_action')}",
            f"主策略 `{closed.get('source_kind')}`，组合 `{contracts}`。",
            f"原因：`{closed.get('exit_reason')}`。",
            f"虚拟入场 {_fmt(closed.get('entry_mid'))}，当前 {_fmt(snapshot.get('mid'))}，MFE {_pct(closed.get('mfe_fraction'))} / MAE {_pct(closed.get('mae_fraction'))}。",
            "这是显示报价路径的影子生命周期，不读取 IBKR 仓位、不假设成交，也不自动下单。",
        )
    )


def _event_contract(
    source: Mapping[str, object], *, block_reasons: tuple[str, ...] | list[str]
) -> dict[str, object]:
    raw_coordinate = source.get("coordinate")
    coordinate = dict(raw_coordinate) if isinstance(raw_coordinate, Mapping) else None
    version = str(source.get("policy_version") or "") or policy_version(
        "virtual_strategy_audit.v3",
        {"source_kind": source.get("source_kind") or "legacy"},
    )
    return strategy_event_fields(
        policy_version_value=version,
        valid_until=parse_aware_time(source.get("valid_until"))
        or parse_aware_time(source.get("time_stop_at")),
        coordinate=coordinate,
        block_reasons=block_reasons,
    )


def _record_entry_decision(
    storage: StorageSettings,
    decision: Mapping[str, object],
    *,
    entry_decisions: dict[str, dict[str, object]],
    now: datetime,
) -> None:
    key = str(decision.get("source_signal_id") or decision.get("decision_id") or "")
    if not key:
        return
    prior = dict(entry_decisions.get(key) or {})
    if prior.get("terminal") is True:
        return
    payload = dict(decision)
    reasons = normalize_block_reasons(payload.get("block_reasons") or [])
    if payload.get("terminal") is True and "signal_expired" in reasons:
        reasons = normalize_block_reasons([*(prior.get("last_block_reasons") or []), *reasons])
        payload["block_reasons"] = reasons
    signature_material = {
        "status": payload.get("status"),
        "terminal": payload.get("terminal"),
        "block_reasons": reasons,
    }
    signature = hashlib.sha256(
        json.dumps(signature_material, sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()[:24]
    if signature != prior.get("signature"):
        _append_audit(storage, now, payload)
    entry_decisions[key] = {
        "signature": signature,
        "last_block_reasons": reasons,
        "last_evaluated_at": payload.get("evaluated_at") or now.isoformat(),
        "terminal": payload.get("terminal") is True,
        "status": payload.get("status"),
        "decision_id": payload.get("decision_id"),
    }


def _trim_entry_decisions(
    rows: Mapping[str, Mapping[str, object]],
) -> dict[str, dict[str, object]]:
    ordered = sorted(
        ((str(key), dict(value)) for key, value in rows.items()),
        key=lambda item: str(item[1].get("last_evaluated_at") or ""),
    )
    return dict(ordered[-200:])


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
    if isinstance(value, bool) or not isinstance(value, int | float):
        return None
    parsed = float(value)
    return parsed if math.isfinite(parsed) else None


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


def _latest_created_at(latest: LatestState) -> str | None:
    created_at = getattr(latest, "created_at", None)
    return _utc(created_at).isoformat() if isinstance(created_at, datetime) else None


def _gth_signal_age_seconds(
    signal: Mapping[str, object],
    *,
    now: datetime,
    future_tolerance_seconds: float,
) -> float | None:
    """Return confirmation age only while the persisted signal is actionable."""

    now = _utc(now)
    confirmed_at = _time(signal.get("confirmed_at"))
    valid_until = _time(signal.get("valid_until"))
    if confirmed_at is None or valid_until is None or valid_until < confirmed_at:
        return None
    age = (now - confirmed_at).total_seconds()
    if age < -max(0.0, future_tolerance_seconds) or now >= valid_until:
        return None
    return age


def _should_replace_with_gth_spread(
    active: Mapping[str, object],
    gth_signal: Mapping[str, object],
) -> bool:
    """Let an exact GTH spread supersede a legacy single-leg shadow."""

    return bool(
        active
        and active.get("source_kind") == "gth_dip_reclaim_call"
        and active.get("position_type") != "call_debit_spread"
        and gth_signal.get("kind") == "gth_dip_reclaim_call"
        and isinstance(gth_signal.get("spread"), Mapping)
    )


def _gth_time_stop(now: datetime, *, policy: MarketFeatureSettings) -> datetime:
    """Return the current 0DTE expiry's DST-aware exit, never the next expiry."""

    now = _utc(now)
    clock = _exit_clock(policy.virtual_gth_exit_clock_et)
    expiry = DEFAULT_MARKET_CALENDAR.research_expiry(now)
    candidate = datetime.combine(expiry, clock, tzinfo=ET).astimezone(timezone.utc)
    if candidate <= now:
        return now
    backstop = now + timedelta(minutes=policy.virtual_gth_time_stop_minutes)
    return min(candidate, backstop)


def _exit_clock(value: object) -> time:
    try:
        parsed = time.fromisoformat(str(value))
    except ValueError:
        return time(9, 45)
    return time(parsed.hour, parsed.minute)


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
    source_contract: Mapping[str, object] | None = None,
    lifecycle_policy: object | None = None,
) -> dict[str, object]:
    if direction not in {"up", "down"}:
        return {}
    episode_id = "virtual:" + hashlib.sha256(
        f"{source_id}|{contract_id}".encode()
    ).hexdigest()[:24]
    source = dict(source_contract or {})
    raw_coordinate = source.get("coordinate")
    coordinate = dict(raw_coordinate) if isinstance(raw_coordinate, Mapping) else None
    lifecycle_policy_version = policy_version(
        "virtual_strategy_lifecycle.v3",
        {
            "source_kind": source_kind,
            "source_policy_version": source.get("policy_version"),
            "policy": lifecycle_policy,
        },
    )
    return {
        **strategy_event_fields(
            policy_version_value=lifecycle_policy_version,
            valid_until=stop,
            coordinate=coordinate,
            block_reasons=(),
        ),
        "episode_id": episode_id,
        "status": "active",
        "source_signal_id": source_id,
        "source_kind": source_kind,
        "source_schema_version": source.get("schema_version"),
        "source_policy_version": source.get("policy_version"),
        "source_valid_until": source.get("valid_until") or source.get("expires_at"),
        "session_id": source.get("session_id") or source.get("session_date"),
        "direction": direction,
        "contract_id": contract_id,
        "opened_at": now.isoformat(),
        "time_stop_at": stop.isoformat(),
        "entry_mid": snapshot.get("mid"),
        "entry_bid": snapshot.get("bid"),
        "entry_ask": snapshot.get("ask"),
        "entry_snapshot": dict(snapshot),
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
    mid = _number(current.get("mid"))
    if active.get("position_type") == "call_debit_spread":
        width = _number(active.get("spread_width_points"))
        if (
            width is not None
            and mid is not None
            and mid >= width * policy.virtual_gth_spread_saturation_fraction
        ):
            return "spread_value_saturation", "take_profit_or_exit"
    else:
        entry_mid = _number(active.get("entry_mid"))
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
                **_event_contract(active, block_reasons=()),
                **row,
            },
        )
    active["horizon_outcomes"] = horizons


def _gth_spread_contract_ids(
    spread: Mapping[str, object],
    *,
    session_date: str,
) -> tuple[str, str] | None:
    long_strike = _number(spread.get("long_strike"))
    short_strike = _number(spread.get("short_strike"))
    if (
        spread.get("right") != "C"
        or long_strike is None
        or short_strike is None
        or long_strike <= 0
        or short_strike <= long_strike
        or not (long_strike / 5.0).is_integer()
        or not (short_strike / 5.0).is_integer()
    ):
        return None
    expiry = session_date.replace("-", "")
    if len(expiry) != 8 or not expiry.isdigit():
        return None
    long_contract = InstrumentId.option(
        "SPX",
        expiry=expiry,
        strike=long_strike,
        right="C",
        trading_class="SPXW",
    ).canonical_id
    short_contract = InstrumentId.option(
        "SPX",
        expiry=expiry,
        strike=short_strike,
        right="C",
        trading_class="SPXW",
    ).canonical_id
    return long_contract, short_contract


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
    source_at = quote.quote_time or quote.trade_time
    transport_at = quote.last_update_at or quote.received_at
    return {
        "at": now.isoformat(),
        "mid": quote.mid,
        "bid": quote.bid,
        "ask": quote.ask,
        "provider": quote.provider.value,
        "source_at": source_at.isoformat() if source_at is not None else None,
        "transport_at": transport_at.isoformat(),
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
