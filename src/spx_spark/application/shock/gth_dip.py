"""Pure ES-led 15/60 minute GTH dip-reclaim detector."""

from __future__ import annotations

import hashlib
import math
from datetime import date, datetime, time, timedelta, timezone
from typing import Mapping
from zoneinfo import ZoneInfo

from spx_spark.alert_model import Alert
from spx_spark.market_calendar import ET
from spx_spark.marketdata import MarketDataQuality
from spx_spark.strategy_contract import policy_version, strategy_event_fields


GTH_DIP_RECLAIM_CALL_KIND = "gth_dip_reclaim_call"
BEIJING = ZoneInfo("Asia/Shanghai")


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
    delivery_retry_seconds: int = 30,
    signal_expiry_seconds: int = 600,
    structure_levels: Mapping[str, float] | None = None,
    es_spx_basis: float | None = None,
    spread_min_width_points: float = 15.0,
    spread_max_width_points: float = 75.0,
    spread_default_width_points: float = 50.0,
    exit_clock_et: str = "09:45",
    entry_quality: Mapping[str, object] | None = None,
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
    previous_provider = str(samples[-1].get("provider") or "") if samples else ""
    enqueued = not samples or _time(samples[-1].get("at")) != now
    provider_changed = bool(
        enqueued and previous_provider and previous_provider != provider
    )
    if enqueued:
        samples.append({"at": now.isoformat(), "es": float(es), "provider": provider})
    state["samples"] = samples
    state["updated_at"] = now.isoformat()
    if provider_changed:
        # A confirmation may not span providers: their source timestamps and
        # price bases are not interchangeable. Raw observations remain for
        # the horizon detector, but eligibility must start again.
        state["pending"] = None

    # Redelivery mirrors the RTH shock path: re-emit an unacknowledged signal
    # on the retry interval (same event_id, idempotent downstream) until the
    # service records delivered_at or the signal ages out.
    raw_signal = state.get("last_signal")
    last_signal = dict(raw_signal) if isinstance(raw_signal, Mapping) else None
    if last_signal is not None and not last_signal.get("delivered_at"):
        confirmed_at = _time(last_signal.get("confirmed_at"))
        valid_until = _time(last_signal.get("valid_until"))
        attempt_at = _time(last_signal.get("last_delivery_attempt_at"))
        expired = confirmed_at is None or (
            valid_until is not None and now >= valid_until
        ) or (
            valid_until is None
            and (now - confirmed_at).total_seconds() > signal_expiry_seconds
        )
        due = attempt_at is None or (
            now - attempt_at
        ).total_seconds() >= delivery_retry_seconds
        if not expired and due:
            last_signal["last_delivery_attempt_at"] = now.isoformat()
            state["last_signal"] = last_signal
            state["status"] = "delivery_retry"
            retry_signal = {**last_signal, "delivery_retry": True}
            return state, _signal_alert(last_signal), retry_signal

    first_sample_at = _time(state.get("first_sample_at")) or now
    if (now - first_sample_at).total_seconds() < session_warmup_seconds:
        state["status"] = "session_warmup"
        return state, None, None
    if int(state.get("signal_count") or 0) >= max_signals_per_session:
        state["status"] = "session_signal_limit"
        return state, None, None

    last_signal_at = _time(last_signal.get("confirmed_at")) if last_signal is not None else None
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
    prior_signal = last_signal or {}
    if prior_signal.get("event_id") == event_id:
        state["status"] = "already_confirmed"
        state["pending"] = None
        return state, None, None
    if not entry_allowed:
        # Suppressed time is not confirmation time. Keep the observations for
        # research, but never bank count/hold duration that can fire as soon
        # as the suppression clears.
        state["pending"] = None
        state["status"] = "suppressed_pre_event"
        return state, None, None
    prior_pending = state.get("pending") if isinstance(state.get("pending"), Mapping) else {}
    same_pending = prior_pending.get("event_id") == event_id
    if same_pending:
        count = int(prior_pending.get("confirm_count") or 0) + (1 if enqueued else 0)
    else:
        count = 1
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
    state["status"] = "confirming"
    confirm_started = _time(confirm_started_at) or now
    if (
        count < confirm_samples
        or (now - confirm_started).total_seconds() < confirm_hold_seconds
    ):
        return state, None, None

    detector_policy_version = policy_version(
        "gth_dip_reclaim.v3",
        {
            "short_horizon_seconds": short_horizon_seconds,
            "long_horizon_seconds": long_horizon_seconds,
            "short_min_drawdown_points": short_min_drawdown_points,
            "long_min_drawdown_points": long_min_drawdown_points,
            "short_min_descent_seconds": short_min_descent_seconds,
            "long_min_descent_seconds": long_min_descent_seconds,
            "expected_move_fraction": expected_move_fraction,
            "reclaim_fraction": reclaim_fraction,
            "min_reclaim_points": min_reclaim_points,
            "confirm_samples": confirm_samples,
            "confirm_hold_seconds": confirm_hold_seconds,
            "session_warmup_seconds": session_warmup_seconds,
            "max_signals_per_session": max_signals_per_session,
            "cooldown_seconds": cooldown_seconds,
            "signal_expiry_seconds": signal_expiry_seconds,
            "spread_min_width_points": spread_min_width_points,
            "spread_max_width_points": spread_max_width_points,
            "spread_default_width_points": spread_default_width_points,
            "exit_clock_et": exit_clock_et,
        },
    )
    valid_until = now + timedelta(seconds=signal_expiry_seconds)
    signal = {
        **pending,
        **strategy_event_fields(
            policy_version_value=detector_policy_version,
            valid_until=valid_until,
            coordinate={
                "kind": "raw_es",
                "instrument_id": "future:ES",
                "observed_value": float(es),
                "target_value": float(pending["trough"])
                + float(pending["required_recovery_points"]),
                "spx_observed_value": None,
                "basis_points": 0.0,
                "as_of": now,
                "provider": provider,
            },
            block_reasons=(),
        ),
        "kind": GTH_DIP_RECLAIM_CALL_KIND,
        "session_date": session_date,
        "direction": "up",
        "confirmed_at": now.isoformat(),
        "last_delivery_attempt_at": now.isoformat(),
        "es": float(es),
        "expected_move_points": expected_move_points,
        "automatic_ordering": False,
        "entry_quality": _frozen_entry_quality(entry_quality),
        "spread": _spread_structure(
            at=now,
            session_date=session_date,
            es=float(es),
            trough=float(pending["trough"]),
            expected_move_points=expected_move_points,
            structure_levels=structure_levels,
            es_spx_basis=es_spx_basis,
            min_width_points=spread_min_width_points,
            max_width_points=spread_max_width_points,
            default_width_points=spread_default_width_points,
            exit_clock_et=exit_clock_et,
        ),
    }
    state["last_signal"] = signal
    state["signal_count"] = int(state.get("signal_count") or 0) + 1
    state["pending"] = None
    state["status"] = "confirmed"
    return state, _signal_alert(signal), signal


def _frozen_entry_quality(value: Mapping[str, object] | None) -> dict[str, object]:
    """Persist the point-in-time shadow verdict; redelivery never recomputes it."""

    if value is not None:
        return dict(value)
    return {
        "mode": "shadow",
        "policy_version": "gth_trend_alignment_shadow_v1",
        "verdict": "blocked",
        "block_reasons": ["trend_context_unavailable"],
        "features": {},
    }


def mark_gth_delivery(
    state: Mapping[str, object], *, event_id: str, at: datetime
) -> dict[str, object]:
    result = dict(state)
    signal = dict(result.get("last_signal") or {})
    if signal.get("event_id") == event_id:
        signal["delivered_at"] = _utc(at).isoformat()
        result["last_signal"] = signal
    return result


def _signal_alert(signal: Mapping[str, object]) -> Alert:
    """Rebuild the confirmed-signal alert so a redelivery stays identical."""

    event_id = str(signal["event_id"])
    desk_view = (
        f"Desk View：ES 自 {float(signal['peak']):.2f} 回落至 {float(signal['trough']):.2f} 后"
        f"回升至 {float(signal['es']):.2f}，回撤 {float(signal['drawdown_points']):.2f} 点并收复"
        f" {float(signal['recovery_fraction']):.0%}；Call 方向进入执行评估。"
    )
    spread = signal.get("spread")
    if isinstance(spread, Mapping):
        detail = (
            desk_view
            + f"Execution：买 SPXW 0DTE {int(spread['long_strike'])}C / 卖 "
            + f"{int(spread['short_strike'])}C（宽 {int(spread['width_points'])} 点，借记价差埋伏）；"
            + f"出场窗口：{spread['exit_window_note']}，"
            + f"最迟 {spread['exit_by_utc']} UTC 离场。"
            + f"Risk：ES 跌破 {float(spread['invalidation_es']):.2f} 即撤销；自动下单关闭，数量人工定。"
        )
    else:
        detail = (
            desk_view
            + "Execution：仅在新鲜 SPXW NBBO 通过门控后建立 TradeReady。"
            + "Risk：ES 跌破本次低点即撤销 Call 判断；自动下单关闭。"
        )
    return Alert(
        severity="high",
        kind=GTH_DIP_RECLAIM_CALL_KIND,
        instrument_id="future:ES",
        title=f"SPX 0DTE | CALL RECLAIM ({int(signal['horizon_seconds']) // 60}m)",
        detail=detail,
        provider=str(signal["provider"]),
        quality=MarketDataQuality.LIVE.value,
        value=float(signal["recovery_points"]),
        threshold=float(signal["required_recovery_points"]),
        source_gate="es_gth_15_60m_dip_reclaim_confirmed",
        dedup_group=f"{event_id}:gth-dip-reclaim",
        event_id=event_id,
        source_at=str(signal["confirmed_at"]),
    )


def _spread_structure(
    *,
    at: datetime,
    session_date: str,
    es: float,
    trough: float,
    expected_move_points: float | None,
    structure_levels: Mapping[str, float] | None,
    es_spx_basis: float | None,
    min_width_points: float,
    max_width_points: float,
    default_width_points: float,
    exit_clock_et: str,
) -> dict[str, object] | None:
    """Debit-spread 埋伏单 skeleton: wall-anchored short strike, EM then default fallback."""

    basis = _finite_number(es_spx_basis)
    exit_context = _gth_exit_context(session_date, exit_clock_et=exit_clock_et)
    if basis is None or exit_context is None or _utc(at) >= exit_context["exit_at"]:
        return None
    spx_equiv = es - basis
    long_strike = _round_strike(spx_equiv)
    levels = structure_levels or {}
    walls = sorted(
        (_round_strike(value), kind)
        for kind in ("flip_high", "call_wall")
        if (value := _finite_number(levels.get(kind))) is not None and value > 0
    )
    short_strike: int | None = None
    target_wall: float | None = None
    target_wall_kind: str | None = None
    anchor = "structure_wall"
    for wall, kind in walls:
        if wall <= long_strike:
            continue
        width = wall - long_strike
        if width < min_width_points:
            continue
        target_wall = float(wall)
        target_wall_kind = kind
        short_strike = wall if width <= max_width_points else long_strike + int(max_width_points)
        break
    if short_strike is None:
        expected_move = _finite_number(expected_move_points)
        if expected_move is not None and expected_move > 0:
            anchor = "expected_move"
            em_width = _round_strike(0.5 * expected_move)
            width = int(min(max(em_width, min_width_points), max_width_points))
        else:
            anchor = "default"
            width = int(default_width_points)
        short_strike = long_strike + width
    return {
        "right": "C",
        "es_spx_basis_used": basis,
        "spx_equiv": spx_equiv,
        "long_strike": long_strike,
        "short_strike": short_strike,
        "width_points": short_strike - long_strike,
        "target_wall": target_wall,
        "target_wall_kind": target_wall_kind,
        "anchor": anchor,
        "invalidation_es": float(trough),
        "expiry_date": session_date,
        "exit_window_note": exit_context["window_note"],
        "exit_clock_et": exit_clock_et,
        "exit_at": exit_context["exit_at"].isoformat(),
        "exit_by_utc": exit_context["exit_at"].strftime("%H:%M"),
        "quantity_policy": "operator_selected",
    }


def _gth_exit_context(
    session_date: str,
    *,
    exit_clock_et: str,
) -> dict[str, object] | None:
    """Resolve one expiry-day ET window into DST-aware UTC and Beijing clocks."""

    try:
        expiry = date.fromisoformat(session_date)
        clock = time.fromisoformat(exit_clock_et)
    except (TypeError, ValueError):
        return None
    if clock.tzinfo is not None or clock.second or clock.microsecond:
        return None
    start_local = datetime.combine(expiry, time(4, 30), tzinfo=ET)
    exit_local = datetime.combine(expiry, clock, tzinfo=ET)
    if exit_local <= start_local:
        return None
    start_beijing = start_local.astimezone(BEIJING)
    exit_beijing = exit_local.astimezone(BEIJING)
    return {
        "exit_at": exit_local.astimezone(timezone.utc),
        "window_note": (
            f"美东 {start_local:%H:%M}–{exit_local:%H:%M}"
            f"（北京 {start_beijing:%H:%M}–{exit_beijing:%H:%M}）分批止盈"
        ),
    }


def _round_strike(value: float) -> int:
    """Round to the nearest 5-point SPX strike, ties away from zero."""

    scaled = value / 5.0
    rounded = math.floor(scaled + 0.5) if scaled >= 0 else -math.floor(-scaled + 0.5)
    return int(rounded * 5)


def _finite_number(value: object) -> float | None:
    if isinstance(value, bool) or not isinstance(value, int | float):
        return None
    parsed = float(value)
    return parsed if math.isfinite(parsed) else None


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
