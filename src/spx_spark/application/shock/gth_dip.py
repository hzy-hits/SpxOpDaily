"""Pure ES-led 15/60 minute GTH dip-reclaim detector."""

from __future__ import annotations

import hashlib
import math
from datetime import date, datetime, time, timedelta, timezone
from typing import Mapping
from zoneinfo import ZoneInfo

from spx_spark.alert_model import Alert
from spx_spark.application.shock.gth_path_history import (
    PATH_SAMPLE_SECONDS,
    advance_path_history as _advance_path_history,
    path_window_summary as _path_window_summary,
    window_rows as _window_rows,
)
from spx_spark.application.shock.gth_provider_continuity import (
    evaluate_provider_switch,
)
from spx_spark.market_calendar import ET
from spx_spark.marketdata import MarketDataQuality
from spx_spark.strategy_contract import policy_version, strategy_event_fields


GTH_DIP_RECLAIM_CALL_KIND = "gth_dip_reclaim_call"
BEIJING = ZoneInfo("Asia/Shanghai")
PATH_HISTORY_GRACE_SECONDS = 60
PATH_DECISION_SAMPLE_SECONDS = 300
PATH_DECISION_MAX_GAP_SECONDS = 600.0
PATH_RANK_SEMANTICS = "empirical_cdf_midrank_not_probability"
PATH_RANK_METHOD = "causal_non_overlapping_session_windows.v1"


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
    provider_switch_hold_seconds: int = 30,
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
    if state.get("session_date") == session_date:
        prior_updated_at = _time(state.get("updated_at"))
        if prior_updated_at is not None and now < prior_updated_at:
            state["status"] = "non_monotonic_sample_ignored"
            state["last_ignored_sample_at"] = now.isoformat()
            state["ignored_sample_count"] = int(state.get("ignored_sample_count") or 0) + 1
            return state, None, None
    if state.get("session_date") != session_date:
        state = {
            "schema_version": 1,
            "session_date": session_date,
            "samples": [],
            "first_sample_at": now.isoformat(),
            "continuous_started_at": _sample_at(now).isoformat(),
            "continuous_provider": provider,
            "path_history": {},
            "path_history_progress": {},
            "signal_count": 0,
        }
    samples = _normalized_samples(state.get("samples"), at=now)
    if samples:
        # Persisted pre-v4 state may already contain mixed-provider history.
        # Keep only the latest provider-continuous suffix before computing any
        # peak/trough geometry.
        suffix_provider = str(samples[-1].get("provider") or "")
        suffix: list[dict[str, object]] = []
        for item in reversed(samples):
            if str(item.get("provider") or "") != suffix_provider:
                break
            suffix.append(item)
        samples = list(reversed(suffix))
    previous_provider = (
        str(samples[-1].get("provider") or "")
        if samples
        else str(state.get("continuous_provider") or "")
    )
    provider_decision = evaluate_provider_switch(
        state.get("provider_switch_candidate"),
        active_provider=previous_provider,
        incoming_provider=provider,
        at=now,
        hold_seconds=provider_switch_hold_seconds,
    )
    state["provider_switch_candidate"] = provider_decision["candidate"]
    if provider_decision["status"] == "holding":
        state["provider_changed"] = False
        state["status"] = "provider_switch_hysteresis"
        state["last_ignored_provider"] = provider
        state["last_ignored_provider_sample_at"] = now.isoformat()
        state["ignored_provider_sample_count"] = (
            int(state.get("ignored_provider_sample_count") or 0) + 1
        )
        return state, None, None
    provider_changed = provider_decision["switch"] is True
    if provider_changed:
        # Provider price bases and timestamps are not interchangeable.  A
        # switch starts a new continuous observation window; retaining the old
        # provider's peak/trough would let the new provider confirm a mixed-
        # coordinate event.  This also catches equal-timestamp failovers.
        samples = []
        state["pending"] = None
        state["path_history"] = {}
        state["path_history_progress"] = {}
        state["continuous_started_at"] = _sample_at(now).isoformat()
    elif state.get("continuous_provider") and state.get("continuous_provider") != provider:
        # A persisted compact history belongs to exactly one provider price
        # coordinate even when its raw suffix was already pruned.
        state["path_history"] = {}
        state["path_history_progress"] = {}
        state["continuous_started_at"] = _sample_at(now).isoformat()
    sample_at = _sample_at(now)
    enqueued = not samples or _time(samples[-1].get("at")) != sample_at
    current_sample = {
        "at": sample_at.isoformat(),
        "es": float(es),
        "provider": provider,
    }
    if enqueued:
        samples.append(current_sample)
    else:
        # One deterministic observation per five-second bucket prevents a
        # faster polling loop from manufacturing confidence.  The latest real
        # price replaces the bucket value without advancing confirmation.
        samples[-1] = current_sample
    continuous_started_at = _time(state.get("continuous_started_at"))
    if continuous_started_at is None:
        continuous_started_at = (_time(samples[0].get("at")) if samples else None) or sample_at
    if provider_changed:
        continuous_started_at = sample_at
    state["continuous_started_at"] = continuous_started_at.isoformat()
    state["continuous_provider"] = provider
    horizons = tuple(sorted({short_horizon_seconds, long_horizon_seconds}))
    history, progress = _advance_path_history(
        state.get("path_history"),
        state.get("path_history_progress"),
        samples=samples,
        continuous_started_at=continuous_started_at,
        now=now,
        horizons=horizons,
    )
    state["path_history"] = history
    state["path_history_progress"] = progress
    retention_seconds = max(horizons) + PATH_HISTORY_GRACE_SECONDS
    samples = [
        row
        for row in samples
        if (_time(row.get("at")) or now) >= now - timedelta(seconds=retention_seconds)
    ]
    max_raw_samples = min(
        10_000,
        math.ceil(retention_seconds / PATH_SAMPLE_SECONDS) + 4,
    )
    samples = samples[-max_raw_samples:]
    state["samples"] = samples
    state["updated_at"] = now.isoformat()
    state["provider_changed"] = provider_changed
    state["path_sampling_seconds"] = PATH_SAMPLE_SECONDS
    state["path_rank_semantics"] = PATH_RANK_SEMANTICS
    state["path_rank_method"] = PATH_RANK_METHOD
    state["legacy_session_warmup_seconds"] = session_warmup_seconds

    readiness: dict[str, dict[str, object]] = {}
    current_windows: dict[int, list[dict[str, object]]] = {}
    for horizon in horizons:
        window_start = now - timedelta(seconds=horizon)
        window = _window_rows(
            samples,
            window_start=window_start,
            window_end=now,
        )
        current_windows[horizon] = window
        summary = _path_window_summary(
            window,
            window_start=window_start,
            window_end=now,
            require_full_window=True,
        )
        ready = summary is not None and continuous_started_at <= window_start
        observed_span = _observed_span_seconds(window)
        expected_sample_count = math.floor(horizon / PATH_SAMPLE_SECONDS) + 1
        coverage_ratio = min(1.0, len(window) / expected_sample_count)
        max_sample_gap_seconds = (
            float(summary["max_sample_gap_seconds"])
            if summary is not None
            else _max_sample_gap_seconds(window)
        )
        minimum_decision_samples = max(
            3,
            math.floor(horizon / PATH_DECISION_SAMPLE_SECONDS) + 1,
        )
        decision_usable = bool(
            ready
            and len(window) >= minimum_decision_samples
            and max_sample_gap_seconds <= PATH_DECISION_MAX_GAP_SECONDS
        )
        rank = (
            _path_rank_summary(
                summary,
                history.get(str(horizon)),
                current_window_start=window_start,
                horizon_seconds=horizon,
            )
            if ready and summary is not None
            else None
        )
        readiness[str(horizon)] = {
            "horizon_seconds": horizon,
            "ready": ready,
            "status": "ready" if ready else "collecting_full_window",
            "observed_span_seconds": observed_span,
            "seconds_until_ready": max(0.0, horizon - observed_span),
            "sample_count": len(window),
            "expected_sample_count": expected_sample_count,
            "coverage_ratio": coverage_ratio,
            "max_sample_gap_seconds": max_sample_gap_seconds,
            "sampling_quality": _sampling_quality(
                ready=ready,
                coverage_ratio=coverage_ratio,
                max_sample_gap_seconds=max_sample_gap_seconds,
            ),
            "minimum_decision_samples": minimum_decision_samples,
            "decision_usable": decision_usable,
            "path_rank": rank,
        }
    state["horizon_readiness"] = readiness

    # Redelivery mirrors the RTH shock path: re-emit an unacknowledged signal
    # on the retry interval (same event_id, idempotent downstream) until the
    # service records delivered_at or the signal ages out.
    raw_signal = state.get("last_signal")
    last_signal = dict(raw_signal) if isinstance(raw_signal, Mapping) else None
    if last_signal is not None and not last_signal.get("delivered_at"):
        confirmed_at = _time(last_signal.get("confirmed_at"))
        valid_until = _time(last_signal.get("valid_until"))
        attempt_at = _time(last_signal.get("last_delivery_attempt_at"))
        expired = (
            confirmed_at is None
            or (valid_until is not None and now >= valid_until)
            or (
                valid_until is None and (now - confirmed_at).total_seconds() > signal_expiry_seconds
            )
        )
        due = attempt_at is None or (now - attempt_at).total_seconds() >= delivery_retry_seconds
        if not expired and due:
            last_signal["last_delivery_attempt_at"] = now.isoformat()
            state["last_signal"] = last_signal
            state["status"] = "delivery_retry"
            retry_signal = {**last_signal, "delivery_retry": True}
            return state, _signal_alert(last_signal), retry_signal

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
        horizon_state = readiness[str(horizon)]
        if not horizon_state["decision_usable"]:
            continue
        window = current_windows[horizon]
        candidate = _dip_candidate(
            window,
            horizon_seconds=horizon,
            drawdown_floor=max(fixed_floor, adaptive),
            reclaim_fraction=reclaim_fraction,
            min_reclaim_points=min_reclaim_points,
            min_descent_seconds=min_descent,
        )
        if candidate:
            candidate["path_rank"] = horizon_state["path_rank"]
            candidates.append(candidate)
    if not candidates:
        state["pending"] = None
        state["status"] = (
            "observing" if any(item["ready"] for item in readiness.values()) else "session_warmup"
        )
        return state, None, None

    chosen = max(
        candidates,
        key=lambda row: (
            float(row["drawdown_points"]),
            int(row["horizon_seconds"]),
        ),
    )
    token = "|".join(
        (
            session_date,
            str(chosen["horizon_seconds"]),
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
    detector_policy_version = policy_version(
        "gth_dip_reclaim.v4",
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
            "warmup_semantics": "independent_full_horizon_window.v1",
            "path_sampling_seconds": PATH_SAMPLE_SECONDS,
            "path_rank_semantics": PATH_RANK_SEMANTICS,
            "path_rank_method": PATH_RANK_METHOD,
            "path_decision_minimum_cadence_seconds": PATH_DECISION_SAMPLE_SECONDS,
            "path_decision_max_gap_seconds": PATH_DECISION_MAX_GAP_SECONDS,
            "event_identity_semantics": "session_horizon_trough.v1",
            "max_signals_per_session": max_signals_per_session,
            "cooldown_seconds": cooldown_seconds,
            "signal_expiry_seconds": signal_expiry_seconds,
            "provider_switch_hold_seconds": provider_switch_hold_seconds,
            "entry_quality_policy_version": str((entry_quality or {}).get("policy_version") or ""),
            "spread_selection_semantics": "first_valid_pending_frozen.v1",
            "spread_min_width_points": spread_min_width_points,
            "spread_max_width_points": spread_max_width_points,
            "spread_default_width_points": spread_default_width_points,
            "exit_clock_et": exit_clock_et,
        },
    )
    prior_pending = state.get("pending") if isinstance(state.get("pending"), Mapping) else {}
    spread_policy_version = policy_version(
        "gth_spread_selection.v1",
        {
            "min_width_points": spread_min_width_points,
            "max_width_points": spread_max_width_points,
            "default_width_points": spread_default_width_points,
            "exit_clock_et": exit_clock_et,
        },
    )
    prior_spread_valid = _spread_matches_policy(
        prior_pending.get("spread"),
        at=now,
        session_date=session_date,
        expected_trough=float(chosen["trough"]),
        min_width_points=spread_min_width_points,
        max_width_points=spread_max_width_points,
        default_width_points=spread_default_width_points,
        exit_clock_et=exit_clock_et,
    )
    same_event = (
        prior_pending.get("event_id") == event_id
        and prior_pending.get("provider") == provider
        and prior_pending.get("policy_version") == detector_policy_version
        and prior_pending.get("spread_policy_version") == spread_policy_version
    )
    same_pending = same_event and prior_spread_valid
    frozen_spread = (
        dict(prior_pending["spread"])
        if same_pending and isinstance(prior_pending.get("spread"), Mapping)
        else None
    )
    if frozen_spread is None:
        # Freeze the exact legs as soon as this event first has enough
        # coordinate evidence.  Confirmation must use these same legs so the
        # collector can prewarm the contracts while the detector is pending.
        frozen_spread = _spread_structure(
            at=now,
            session_date=session_date,
            es=float(es),
            trough=float(chosen["trough"]),
            expected_move_points=expected_move_points,
            structure_levels=structure_levels,
            es_spx_basis=es_spx_basis,
            min_width_points=spread_min_width_points,
            max_width_points=spread_max_width_points,
            default_width_points=spread_default_width_points,
            exit_clock_et=exit_clock_et,
        )
    spread_ready = frozen_spread is not None
    if spread_ready:
        count = (
            int(prior_pending.get("confirm_count") or 0) + (1 if enqueued else 0)
            if same_pending
            else 1
        )
        confirm_started_at = (
            prior_pending.get("confirm_started_at") if same_pending else now.isoformat()
        )
    else:
        # Preserve the event candidate so confirmation can begin when stable
        # SPX coordinates arrive, but do not bank count or hold time beforehand.
        count = 0
        confirm_started_at = None
    pending = {
        **chosen,
        **strategy_event_fields(
            policy_version_value=detector_policy_version,
            valid_until=None,
            coordinate={
                "kind": "raw_es",
                "instrument_id": "future:ES",
                "observed_value": float(es),
                "target_value": float(chosen["trough"]) + float(chosen["required_recovery_points"]),
                "spx_observed_value": None,
                "basis_points": 0.0,
                "as_of": now,
                "provider": provider,
            },
            block_reasons=(),
        ),
        "event_id": event_id,
        "session_date": session_date,
        "confirm_count": count,
        "confirm_started_at": confirm_started_at,
        "provider": provider,
        "spread_policy_version": spread_policy_version,
        "spread": frozen_spread,
        "automatic_ordering": False,
    }
    state["pending"] = pending
    if not spread_ready:
        state["status"] = "spread_inputs_unavailable"
        return state, None, None
    state["status"] = "confirming"
    confirm_started = _time(confirm_started_at) or now
    if count < confirm_samples or (now - confirm_started).total_seconds() < confirm_hold_seconds:
        return state, None, None

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
        "spread": dict(frozen_spread) if frozen_spread is not None else None,
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
        "mode": "decision_grade",
        "policy_version": "gth_trend_alignment_live_v2",
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
    entry_quality = signal.get("entry_quality")
    entry_quality = entry_quality if isinstance(entry_quality, Mapping) else {}
    quality_passed = bool(
        entry_quality.get("mode") == "decision_grade"
        and entry_quality.get("verdict") == "pass"
        and not entry_quality.get("block_reasons")
    )
    desk_view = (
        f"Desk View：ES 自 {float(signal['peak']):.2f} 回落至 {float(signal['trough']):.2f} 后"
        f"回升至 {float(signal['es']):.2f}，回撤 {float(signal['drawdown_points']):.2f} 点并收复"
        f" {float(signal['recovery_fraction']):.0%}。"
    )
    rank = signal.get("path_rank")
    rank = rank if isinstance(rank, Mapping) else {}
    rank_n = int(rank.get("effective_reference_windows") or 0)
    position_rank = _finite_number(rank.get("position_percentile"))
    drawdown_rank = _finite_number(rank.get("drawdown_rank_percentile"))
    recovery_rank = _finite_number(rank.get("recovery_rank_percentile"))
    if position_rank is not None:
        rank_detail = f"窗内位置rank {position_rank:.0f}%"
        if drawdown_rank is not None and recovery_rank is not None:
            rank_detail += (
                f"，回撤/收复历史rank {drawdown_rank:.0f}%/{recovery_rank:.0f}%（有效n={rank_n}）"
            )
        desk_view += rank_detail + "；rank仅为因果经验排序，不是概率。"
    if quality_passed:
        detail = (
            desk_view
            + "方向门已通过；正在核验精确 SPXW 双腿、parity、到期最大赔付比和报价时效。"
            + "只有随后绿色 MANUAL READY 卡可操作；本事件卡本身不得下单。"
        )
    else:
        reasons = ",".join(str(item) for item in entry_quality.get("block_reasons") or ())
        detail = (
            desk_view
            + "方向门未通过，仅记录形态，不生成合约或操作指令。"
            + (f"阻断：{reasons}。" if reasons else "")
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


def _spread_matches_policy(
    value: object,
    *,
    at: datetime,
    session_date: str,
    expected_trough: float,
    min_width_points: float,
    max_width_points: float,
    default_width_points: float,
    exit_clock_et: str,
) -> bool:
    if not isinstance(value, Mapping):
        return False
    long_strike = _finite_number(value.get("long_strike"))
    short_strike = _finite_number(value.get("short_strike"))
    width = _finite_number(value.get("width_points"))
    invalidation = _finite_number(value.get("invalidation_es"))
    basis = _finite_number(value.get("es_spx_basis_used"))
    spx_equiv = _finite_number(value.get("spx_equiv"))
    target_wall = _finite_number(value.get("target_wall"))
    target_wall_kind = value.get("target_wall_kind")
    expected_exit = _gth_exit_context(session_date, exit_clock_et=exit_clock_et)
    raw_exit_at = value.get("exit_at")
    try:
        exit_at = datetime.fromisoformat(raw_exit_at) if isinstance(raw_exit_at, str) else None
    except ValueError:
        exit_at = None
    anchor = value.get("anchor")
    return bool(
        value.get("right") == "C"
        and value.get("expiry_date") == session_date
        and long_strike is not None
        and short_strike is not None
        and long_strike > 0
        and short_strike > long_strike
        and long_strike.is_integer()
        and short_strike.is_integer()
        and int(long_strike) % 5 == 0
        and int(short_strike) % 5 == 0
        and width is not None
        and width == short_strike - long_strike
        and min_width_points <= width <= max_width_points
        and anchor in {"structure_wall", "expected_move", "default"}
        and (anchor != "default" or width == int(default_width_points))
        and (
            anchor != "structure_wall"
            or (
                target_wall is not None
                and target_wall_kind in {"flip_high", "call_wall"}
                and target_wall > long_strike
                and target_wall - long_strike >= min_width_points
                and short_strike == min(target_wall, long_strike + int(max_width_points))
            )
        )
        and (anchor == "structure_wall" or (target_wall is None and target_wall_kind is None))
        and invalidation == expected_trough
        and basis is not None
        and spx_equiv is not None
        and _round_strike(spx_equiv) == long_strike
        and value.get("exit_clock_et") == exit_clock_et
        and expected_exit is not None
        and value.get("exit_window_note") == expected_exit["window_note"]
        and value.get("exit_by_utc") == expected_exit["exit_at"].strftime("%H:%M")
        and value.get("quantity_policy") == "operator_selected"
        and exit_at is not None
        and exit_at.tzinfo is not None
        and exit_at.astimezone(timezone.utc) == expected_exit["exit_at"]
        and _utc(at) < exit_at.astimezone(timezone.utc)
    )


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


def _sample_at(value: datetime) -> datetime:
    """Normalize polling to one deterministic five-second observation bucket."""

    current = _utc(value)
    epoch = math.floor(current.timestamp() / PATH_SAMPLE_SECONDS) * PATH_SAMPLE_SECONDS
    return datetime.fromtimestamp(epoch, tz=timezone.utc)


def _normalized_samples(value: object, *, at: datetime) -> list[dict[str, object]]:
    """Migrate persisted raw observations into ordered five-second buckets."""

    if not isinstance(value, list):
        return []
    buckets: dict[datetime, dict[str, object]] = {}
    for item in value:
        if not isinstance(item, Mapping):
            continue
        observed_at = _time(item.get("at"))
        price = _finite_number(item.get("es"))
        provider = str(item.get("provider") or "")
        if observed_at is None or observed_at > at or price is None or not provider:
            continue
        bucket_at = _sample_at(observed_at)
        buckets[bucket_at] = {
            "at": bucket_at.isoformat(),
            "es": price,
            "provider": provider,
        }
    return [buckets[key] for key in sorted(buckets)]


def _path_rank_summary(
    current: Mapping[str, object],
    raw_history: object,
    *,
    current_window_start: datetime,
    horizon_seconds: int,
) -> dict[str, object]:
    """Rank the current path only against completed, earlier session windows."""

    history = raw_history if isinstance(raw_history, list) else []
    references = [
        item
        for item in history
        if isinstance(item, Mapping)
        and (_time(item.get("window_ended_at")) or current_window_start) <= current_window_start
    ]
    drawdown = _finite_number(current.get("drawdown_points")) or 0.0
    recovery = _finite_number(current.get("recovery_points")) or 0.0
    rally = _finite_number(current.get("rally_points")) or 0.0
    pullback = _finite_number(current.get("pullback_points")) or 0.0
    reference_drawdowns = [
        value
        for item in references
        if (value := _finite_number(item.get("drawdown_points"))) is not None
    ]
    reference_recoveries = [
        value
        for item in references
        if (value := _finite_number(item.get("recovery_points"))) is not None
    ]
    reference_rallies = [
        value
        for item in references
        if (value := _finite_number(item.get("rally_points"))) is not None
    ]
    reference_pullbacks = [
        value
        for item in references
        if (value := _finite_number(item.get("pullback_points"))) is not None
    ]
    effective_n = min(
        len(reference_drawdowns),
        len(reference_recoveries),
        len(reference_rallies),
        len(reference_pullbacks),
    )
    sample_count = int(current.get("sample_count") or 0)
    expected_sample_count = math.floor(horizon_seconds / PATH_SAMPLE_SECONDS) + 1
    coverage_ratio = min(1.0, sample_count / expected_sample_count)
    max_sample_gap_seconds = _finite_number(current.get("max_sample_gap_seconds")) or 0.0
    rank_status = (
        "unavailable" if effective_n == 0 else "small_sample" if effective_n < 5 else "descriptive"
    )
    return {
        "horizon_seconds": horizon_seconds,
        "rank_semantics": PATH_RANK_SEMANTICS,
        "reference_method": PATH_RANK_METHOD,
        "reference_overlap": False,
        "position_sampling_seconds": PATH_SAMPLE_SECONDS,
        "position_sample_count": sample_count,
        "expected_sample_count": expected_sample_count,
        "coverage_ratio": coverage_ratio,
        "max_sample_gap_seconds": max_sample_gap_seconds,
        "sampling_quality": _sampling_quality(
            ready=True,
            coverage_ratio=coverage_ratio,
            max_sample_gap_seconds=max_sample_gap_seconds,
        ),
        "position_percentile": _finite_number(current.get("position_percentile")),
        "drawdown_points": drawdown,
        "drawdown_rank_percentile": _empirical_rank(drawdown, reference_drawdowns),
        "recovery_points": recovery,
        "recovery_rank_percentile": _empirical_rank(recovery, reference_recoveries),
        "rally_points": rally,
        "rally_rank_percentile": _empirical_rank(rally, reference_rallies),
        "pullback_points": pullback,
        "pullback_rank_percentile": _empirical_rank(pullback, reference_pullbacks),
        "effective_reference_windows": effective_n,
        "rank_status": rank_status,
    }


def _empirical_rank(value: float, references: list[float]) -> float | None:
    if not references:
        return None
    less = sum(reference < value for reference in references)
    equal = sum(reference == value for reference in references)
    return 100.0 * (less + 0.5 * equal) / len(references)


def _observed_span_seconds(rows: list[dict[str, object]]) -> float:
    if len(rows) < 2:
        return 0.0
    first = _time(rows[0].get("at"))
    last = _time(rows[-1].get("at"))
    if first is None or last is None:
        return 0.0
    return max(0.0, (last - first).total_seconds())


def _max_sample_gap_seconds(rows: list[dict[str, object]]) -> float:
    timestamps = [observed_at for row in rows if (observed_at := _time(row.get("at"))) is not None]
    return max(
        ((right - left).total_seconds() for left, right in zip(timestamps, timestamps[1:])),
        default=0.0,
    )


def _sampling_quality(
    *,
    ready: bool,
    coverage_ratio: float,
    max_sample_gap_seconds: float,
) -> str:
    if not ready:
        return "collecting"
    if coverage_ratio >= 0.8 and max_sample_gap_seconds <= 30.0:
        return "dense"
    return "usable_sparse"


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
    trough_index = min(range(peak_index + 1, len(rows)), key=lambda index: float(rows[index]["es"]))
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
