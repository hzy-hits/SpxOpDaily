"""Pure ES-led 15/60 minute GTH dip-reclaim detector."""

from __future__ import annotations

import hashlib
from datetime import datetime, timedelta, timezone
from typing import Mapping

from spx_spark.alert_model import Alert
from spx_spark.marketdata import MarketDataQuality


GTH_DIP_RECLAIM_CALL_KIND = "gth_dip_reclaim_call"


def advance_gth_dip(
    previous: Mapping[str, object] | None,
    *,
    session_date: str,
    at: datetime,
    es: float,
    provider: str,
    expected_move_points: float | None,
    short_horizon_seconds: int,
    long_horizon_seconds: int,
    short_min_drawdown_points: float,
    long_min_drawdown_points: float,
    short_min_descent_seconds: int,
    long_min_descent_seconds: int,
    expected_move_fraction: float,
    reclaim_fraction: float,
    min_reclaim_points: float,
    confirm_samples: int,
    confirm_hold_seconds: int,
    session_warmup_seconds: int,
    max_signals_per_session: int,
    cooldown_seconds: int,
    entry_allowed: bool,
) -> tuple[dict[str, object], Alert | None, dict[str, object] | None]:
    """Advance one session state and emit at most one confirmed Call advisory."""

    now = _utc(at)
    state = dict(previous or {})
    if state.get("session_date") != session_date:
        state = {
            "schema_version": 1,
            "session_date": session_date,
            "samples": [],
            "first_sample_at": now.isoformat(),
            "signal_count": 0,
        }
    samples = [
        dict(item)
        for item in state.get("samples") or []
        if isinstance(item, Mapping)
        and (_time(item.get("at")) or now) >= now - timedelta(seconds=long_horizon_seconds)
    ]
    if not samples or _time(samples[-1].get("at")) != now:
        samples.append({"at": now.isoformat(), "es": float(es), "provider": provider})
    state["samples"] = samples
    state["updated_at"] = now.isoformat()
    first_sample_at = _time(state.get("first_sample_at")) or now
    if (now - first_sample_at).total_seconds() < session_warmup_seconds:
        state["status"] = "session_warmup"
        return state, None, None
    if int(state.get("signal_count") or 0) >= max_signals_per_session:
        state["status"] = "session_signal_limit"
        return state, None, None

    last_signal_at = _time((state.get("last_signal") or {}).get("confirmed_at")) if isinstance(state.get("last_signal"), Mapping) else None
    if last_signal_at is not None and (now - last_signal_at).total_seconds() < cooldown_seconds:
        state["status"] = "cooldown"
        return state, None, None

    candidates = []
    adaptive = (expected_move_points or 0.0) * expected_move_fraction
    for horizon, fixed_floor, min_descent in (
        (short_horizon_seconds, short_min_drawdown_points, short_min_descent_seconds),
        (long_horizon_seconds, long_min_drawdown_points, long_min_descent_seconds),
    ):
        window = [row for row in samples if (_time(row.get("at")) or now) >= now - timedelta(seconds=horizon)]
        candidate = _dip_candidate(
            window,
            horizon_seconds=horizon,
            drawdown_floor=max(fixed_floor, adaptive),
            reclaim_fraction=reclaim_fraction,
            min_reclaim_points=min_reclaim_points,
            min_descent_seconds=min_descent,
        )
        if candidate:
            candidates.append(candidate)
    if not candidates:
        state["pending"] = None
        state["status"] = "observing"
        return state, None, None

    chosen = max(candidates, key=lambda row: (float(row["drawdown_points"]), int(row["horizon_seconds"])))
    token = "|".join(
        (
            session_date,
            str(chosen["horizon_seconds"]),
            str(chosen["peak_at"]),
            str(chosen["trough_at"]),
        )
    )
    event_id = "gth-dip:" + hashlib.sha256(token.encode()).hexdigest()[:24]
    prior_signal = state.get("last_signal") if isinstance(state.get("last_signal"), Mapping) else {}
    if prior_signal.get("event_id") == event_id:
        state["status"] = "already_confirmed"
        state["pending"] = None
        return state, None, None
    prior_pending = state.get("pending") if isinstance(state.get("pending"), Mapping) else {}
    same_pending = prior_pending.get("event_id") == event_id
    count = int(prior_pending.get("confirm_count") or 0) + 1 if same_pending else 1
    confirm_started_at = (
        prior_pending.get("confirm_started_at") if same_pending else now.isoformat()
    )
    pending = {
        **chosen,
        "event_id": event_id,
        "confirm_count": count,
        "confirm_started_at": confirm_started_at,
        "provider": provider,
    }
    state["pending"] = pending
    state["status"] = "confirming" if entry_allowed else "suppressed_pre_event"
    confirm_started = _time(confirm_started_at) or now
    if (
        count < confirm_samples
        or (now - confirm_started).total_seconds() < confirm_hold_seconds
        or not entry_allowed
    ):
        return state, None, None

    signal = {
        **pending,
        "kind": GTH_DIP_RECLAIM_CALL_KIND,
        "session_date": session_date,
        "direction": "up",
        "confirmed_at": now.isoformat(),
        "es": float(es),
        "expected_move_points": expected_move_points,
        "automatic_ordering": False,
    }
    state["last_signal"] = signal
    state["signal_count"] = int(state.get("signal_count") or 0) + 1
    state["pending"] = None
    state["status"] = "confirmed"
    alert = Alert(
        severity="high",
        kind=GTH_DIP_RECLAIM_CALL_KIND,
        instrument_id="future:ES",
        title=f"ES GTH dip reclaim confirmed ({int(chosen['horizon_seconds']) // 60}m)",
        detail=(
            f"ES 从 {float(chosen['peak']):.2f} 缓跌至 {float(chosen['trough']):.2f}，"
            f"回升至 {float(es):.2f}；回撤 {float(chosen['drawdown_points']):.2f} 点、"
            f"已收复 {float(chosen['recovery_fraction']):.0%}。这是 0DTE Call 主策略候选，"
            "不要求夜盘直接 SPX；等待可执行 SPXW 报价，反向跌破低点即失效，不自动下单。"
        ),
        provider=provider,
        quality=MarketDataQuality.LIVE.value,
        value=float(chosen["recovery_points"]),
        threshold=float(chosen["required_recovery_points"]),
        source_gate="es_gth_15_60m_dip_reclaim_confirmed",
        dedup_group=f"{event_id}:gth-dip-reclaim",
        event_id=event_id,
        source_at=now.isoformat(),
    )
    return state, alert, signal


def mark_gth_delivery(
    state: Mapping[str, object], *, event_id: str, at: datetime
) -> dict[str, object]:
    result = dict(state)
    signal = dict(result.get("last_signal") or {})
    if signal.get("event_id") == event_id:
        signal["delivered_at"] = _utc(at).isoformat()
        result["last_signal"] = signal
    return result


def _dip_candidate(
    rows: list[dict[str, object]],
    *,
    horizon_seconds: int,
    drawdown_floor: float,
    reclaim_fraction: float,
    min_reclaim_points: float,
    min_descent_seconds: int,
) -> dict[str, object] | None:
    if len(rows) < 3:
        return None
    peak_index = max(range(len(rows) - 1), key=lambda index: float(rows[index]["es"]))
    trough_index = min(
        range(peak_index + 1, len(rows)), key=lambda index: float(rows[index]["es"])
    )
    if trough_index >= len(rows) - 1:
        return None
    peak = float(rows[peak_index]["es"])
    trough = float(rows[trough_index]["es"])
    current = float(rows[-1]["es"])
    drawdown = peak - trough
    peak_at = _time(rows[peak_index].get("at"))
    trough_at = _time(rows[trough_index].get("at"))
    if (
        peak_at is None
        or trough_at is None
        or (trough_at - peak_at).total_seconds() < min_descent_seconds
    ):
        return None
    recovery = current - trough
    required = max(drawdown * reclaim_fraction, min_reclaim_points)
    if drawdown < drawdown_floor or recovery < required:
        return None
    return {
        "horizon_seconds": horizon_seconds,
        "peak": peak,
        "peak_at": rows[peak_index]["at"],
        "trough": trough,
        "trough_at": rows[trough_index]["at"],
        "drawdown_points": drawdown,
        "drawdown_threshold_points": drawdown_floor,
        "recovery_points": recovery,
        "required_recovery_points": required,
        "recovery_fraction": recovery / drawdown,
    }


def _time(value: object) -> datetime | None:
    if not isinstance(value, str):
        return None
    try:
        return _utc(datetime.fromisoformat(value))
    except ValueError:
        return None


def _utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)
