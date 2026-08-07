"""Causal Q/P advisory artifact; candidate authority remains in the selector."""

from __future__ import annotations

import hashlib
import json
import math
import threading
from collections.abc import Callable, Mapping
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

from spx_spark.analytics.greeks.black_scholes import d1, normal_cdf
from spx_spark.application.market_features.models import FrameQuality, OptionStructureFrame
from spx_spark.application.market_features.physical_followthrough import (
    FEATURE_SET_VERSION as PHYSICAL_FEATURE_SET_VERSION,
)
from spx_spark.application.market_features.physical_followthrough import (
    MODEL_VERSION as PHYSICAL_MODEL_VERSION,
)
from spx_spark.application.market_features.physical_followthrough import (
    PhysicalFollowThroughEstimate,
    estimate_physical_followthrough,
)
from spx_spark.domain.strategy_distribution_forecast import (
    CalibrationStatus,
    EstimateQuality,
    EstimateStatus,
    ForecastQuality,
    ForecastSession,
    ProbabilityEstimate,
    ProbabilityEventDefinition,
    ProbabilityEventKind,
    ProbabilityMeasure,
    ShadowAction,
    ShadowDecision,
    StrategyDistributionForecast,
)
from spx_spark.market_calendar import DEFAULT_MARKET_CALENDAR
from spx_spark.marketdata import (
    FUTURE_TIMESTAMP_TOLERANCE_SECONDS,
    MarketDataQuality,
    Quote,
    as_utc,
    quality_from_market_data_type,
)
from spx_spark.settings.strategy_distribution import StrategyDistributionSettings
from spx_spark.state_io import (
    append_jsonl_secure,
    atomic_write_json_secure,
    exclusive_state_lock,
    read_json_object,
)
from spx_spark.storage import LatestState


MODEL_VERSION = "strategy_distribution_q_nearest_neighbor.v2"
FEATURE_SET_VERSION = "action_spx_option_frame_level_neighbours.v2"
POLICY_VERSION = "formal_no_trade_proxy_only.v1"
Q_METHOD_VERSION = "risk_neutral_atm_nd2_short_horizon_proxy.v1"
LATEST_FILENAME = "strategy_distribution_forecast.json"
AUDIT_DIRECTORY = "strategy_distribution_forecast"
AUDIT_FILENAME = "forecasts.jsonl"


PhysicalEstimator = Callable[..., PhysicalFollowThroughEstimate]
_PHYSICAL_CACHE: dict[tuple[object, ...], tuple[datetime, PhysicalFollowThroughEstimate]] = {}
_PHYSICAL_CACHE_LOCK = threading.Lock()


def latest_strategy_distribution_forecast_path(data_root: str | Path) -> Path:
    return Path(data_root).expanduser() / "latest" / LATEST_FILENAME


def strategy_distribution_forecast_audit_path(
    data_root: str | Path,
    trading_date: date,
) -> Path:
    return (
        Path(data_root).expanduser()
        / "features"
        / AUDIT_DIRECTORY
        / f"date={trading_date.isoformat()}"
        / AUDIT_FILENAME
    )


def clear_strategy_distribution_forecast_cache() -> None:
    """Clear the process-local physical baseline cache (primarily for tests)."""

    with _PHYSICAL_CACHE_LOCK:
        _PHYSICAL_CACHE.clear()


def process_strategy_distribution_forecast(
    *,
    data_root: str | Path,
    action_state: LatestState,
    option_frame: OptionStructureFrame | Mapping[str, object],
    raw_level_decision: Mapping[str, object],
    now: datetime,
    settings: StrategyDistributionSettings,
    trading_date: date | None = None,
    physical_estimator: PhysicalEstimator = estimate_physical_followthrough,
) -> dict[str, object]:
    """Build and durably publish one causal, advisory-only forecast artifact."""

    now_utc = _aware_utc(now, "strategy distribution now")
    if type(action_state) is not LatestState:
        raise ValueError("strategy distribution action_state must be LatestState")
    if not isinstance(option_frame, OptionStructureFrame | Mapping):
        raise ValueError("strategy distribution option_frame must be typed or a mapping")
    if not isinstance(raw_level_decision, Mapping):
        raise ValueError("strategy distribution raw_level_decision must be a mapping")
    if type(settings) is not StrategyDistributionSettings:
        raise ValueError("strategy distribution settings must be typed")
    resolved_date = trading_date or DEFAULT_MARKET_CALENDAR.research_expiry(now_utc)
    if type(resolved_date) is not date:
        raise ValueError("strategy distribution trading_date must be a date")

    document = build_strategy_distribution_forecast(
        data_root=data_root,
        action_state=action_state,
        option_frame=option_frame,
        raw_level_decision=raw_level_decision,
        now=now_utc,
        trading_date=resolved_date,
        settings=settings,
        physical_estimator=physical_estimator,
    )
    payload = document.to_dict()
    _persist_strategy_distribution_forecast(
        payload,
        data_root=data_root,
        trading_date=resolved_date,
        append_interval_seconds=settings.append_interval_seconds,
    )
    return payload


def build_strategy_distribution_forecast(
    *,
    data_root: str | Path,
    action_state: LatestState,
    option_frame: OptionStructureFrame | Mapping[str, object],
    raw_level_decision: Mapping[str, object],
    now: datetime,
    trading_date: date,
    settings: StrategyDistributionSettings,
    physical_estimator: PhysicalEstimator = estimate_physical_followthrough,
) -> StrategyDistributionForecast:
    """Build one typed document without mutating provider or strategy state."""

    now_utc = _aware_utc(now, "strategy distribution now")
    direction, direction_reasons = _direction(raw_level_decision, now=now_utc)
    spot, spot_quote, spot_reasons = _action_spot(
        action_state,
        now=now_utc,
        maximum_age_seconds=settings.projection_ttl_seconds,
    )
    event = _directional_event(
        raw_level_decision,
        direction=direction,
        spot=spot,
        now=now_utc,
        horizon_seconds=settings.horizon_seconds,
        trading_date=trading_date,
    )

    if not settings.enabled:
        event = None
        q_event = _unavailable_probability(
            ProbabilityMeasure.RISK_NEUTRAL,
            event=None,
            reasons=("strategy_distribution_disabled",),
        )
        p_event = _unavailable_probability(
            ProbabilityMeasure.PHYSICAL,
            event=None,
            reasons=("strategy_distribution_disabled",),
            physical_counts=(0, 0),
        )
    elif event is None:
        unavailable_reasons = tuple(
            sorted(set((*direction_reasons, *spot_reasons, "directional_event_unavailable")))
        )
        q_event = _unavailable_probability(
            ProbabilityMeasure.RISK_NEUTRAL,
            event=None,
            reasons=unavailable_reasons,
        )
        p_event = _unavailable_probability(
            ProbabilityMeasure.PHYSICAL,
            event=None,
            reasons=unavailable_reasons,
            physical_counts=(0, 0),
        )
    else:
        assert direction is not None and spot is not None
        q_event = _risk_neutral_probability(
            event,
            direction=direction,
            spot=spot,
            option_frame=option_frame,
            now=now_utc,
            trading_date=trading_date,
            settings=settings,
        )
        estimate = _physical_estimate(
            data_root=data_root,
            now=now_utc,
            trading_date=trading_date,
            direction=direction,
            thesis=_thesis(raw_level_decision),
            level_kind=str(raw_level_decision.get("level_kind") or "unknown"),
            settings=settings,
            estimator=physical_estimator,
        )
        p_event = _physical_probability(
            event,
            estimate=estimate,
            trading_date=trading_date,
        )

    quality_reasons = _quality_reasons(
        q_event,
        p_event,
        direction_reasons=direction_reasons,
        spot_reasons=spot_reasons,
        disabled=not settings.enabled,
    )
    quality = (
        ForecastQuality.UNAVAILABLE
        if event is None
        or (
            q_event.status is EstimateStatus.UNAVAILABLE
            and p_event.status is EstimateStatus.UNAVAILABLE
        )
        else ForecastQuality.DEGRADED
    )
    decision_reasons = tuple(
        sorted(
            set(
                (
                    "actual_fill_probability_unavailable",
                    "net_pnl_distribution_unavailable",
                    "strategy_candidate_unavailable",
                    *quality_reasons,
                )
            )
        )
    )
    source_snapshot_id = _source_snapshot_id(
        action_state,
        option_frame,
        raw_level_decision,
        spot_quote=spot_quote,
    )
    document_id = _document_id(
        source_snapshot_id=source_snapshot_id,
        available_at=now_utc,
        q_event=q_event,
        p_event=p_event,
    )
    observed_through = _observed_through(
        action_state,
        option_frame,
        raw_level_decision,
        now=now_utc,
    )
    session = (
        ForecastSession.RTH if DEFAULT_MARKET_CALENDAR.is_rth_open(now_utc) else ForecastSession.GTH
    )
    return StrategyDistributionForecast(
        document_id=document_id,
        source_snapshot_id=source_snapshot_id,
        trading_date_et=trading_date,
        session=session,
        observed_through=observed_through,
        available_at=now_utc,
        valid_until=now_utc + timedelta(seconds=settings.projection_ttl_seconds),
        model_version=MODEL_VERSION,
        feature_set_version=(f"{FEATURE_SET_VERSION}+{PHYSICAL_FEATURE_SET_VERSION}"),
        calibration_status=CalibrationStatus.UNCALIBRATED,
        calibration_version=None,
        policy_version=f"{POLICY_VERSION}:{settings.horizon_seconds}s",
        q_event=q_event,
        p_event=p_event,
        strategy_candidates=(),
        shadow_decision=ShadowDecision(
            action=ShadowAction.NO_TRADE,
            selected_candidate_id=None,
            score_threshold=0.0,
            reason_codes=decision_reasons,
        ),
        quality=quality,
        quality_reason_codes=quality_reasons,
    )


def _direction(
    raw_level_decision: Mapping[str, object],
    *,
    now: datetime,
) -> tuple[str | None, tuple[str, ...]]:
    direction = str(raw_level_decision.get("direction") or "").strip().lower()
    reasons: list[str] = []
    if str(raw_level_decision.get("phase") or "").strip().lower() != "confirmed":
        reasons.append("level_phase_not_confirmed")
        direction = None
    if direction not in {"up", "down"}:
        reasons.append("level_direction_unavailable")
        direction = None
    updated_at = _timestamp(
        raw_level_decision.get("updated_at") or raw_level_decision.get("phase_at")
    )
    if (
        updated_at is not None
        and (updated_at - now).total_seconds() > FUTURE_TIMESTAMP_TOLERANCE_SECONDS
    ):
        reasons.append("level_decision_future_timestamp")
        direction = None
    expires_at = _timestamp(raw_level_decision.get("expires_at"))
    if expires_at is not None and expires_at <= now:
        reasons.append("level_decision_expired")
        direction = None
    return direction, tuple(sorted(set(reasons)))


def _action_spot(
    state: LatestState,
    *,
    now: datetime,
    maximum_age_seconds: float,
) -> tuple[float | None, Quote | None, tuple[str, ...]]:
    quote = state.best_quote("index:SPX")
    if quote is None:
        return None, None, ("action_spx_quote_unavailable",)
    reasons: list[str] = []
    if quote.quality is not MarketDataQuality.LIVE:
        reasons.append("action_spx_quote_not_live")
    feed_quality = quality_from_market_data_type(quote.market_data_type)
    if feed_quality is not None and feed_quality is not MarketDataQuality.LIVE:
        reasons.append("action_spx_feed_not_live")
    value = _finite(quote.effective_price)
    if value is None or value <= 0.0:
        reasons.append("action_spx_price_unavailable")
    source_at = quote.quote_time or quote.trade_time or quote.received_at
    try:
        source_at_utc = _aware_utc(source_at, "action SPX source timestamp")
    except ValueError:
        reasons.append("action_spx_source_timestamp_invalid")
    else:
        age = (now - source_at_utc).total_seconds()
        if age < -FUTURE_TIMESTAMP_TOLERANCE_SECONDS:
            reasons.append("action_spx_quote_future_timestamp")
        elif age > maximum_age_seconds:
            reasons.append("action_spx_quote_stale")
    if reasons:
        return None, quote, tuple(sorted(set(reasons)))
    assert value is not None
    return value, quote, ()


def _directional_event(
    raw_level_decision: Mapping[str, object],
    *,
    direction: str | None,
    spot: float | None,
    now: datetime,
    horizon_seconds: int,
    trading_date: date,
) -> ProbabilityEventDefinition | None:
    if direction not in {"up", "down"} or spot is None:
        return None
    source_event = str(raw_level_decision.get("event_id") or "background").strip()
    target_at = now + timedelta(seconds=horizon_seconds)
    identity = _hash(
        {
            "trading_date": trading_date.isoformat(),
            "source_event": source_event,
            "direction": direction,
            "horizon_seconds": horizon_seconds,
            "reference_level": round(spot, 6),
            "target_at": target_at.isoformat(),
        }
    )
    if direction == "up":
        return ProbabilityEventDefinition(
            event_id=f"directional-event:{identity[:24]}",
            kind=ProbabilityEventKind.TERMINAL_ABOVE,
            target_at=target_at,
            lower_level=spot,
        )
    return ProbabilityEventDefinition(
        event_id=f"directional-event:{identity[:24]}",
        kind=ProbabilityEventKind.TERMINAL_BELOW,
        target_at=target_at,
        upper_level=spot,
    )


def _risk_neutral_probability(
    event: ProbabilityEventDefinition,
    *,
    direction: str,
    spot: float,
    option_frame: OptionStructureFrame | Mapping[str, object],
    now: datetime,
    trading_date: date,
    settings: StrategyDistributionSettings,
) -> ProbabilityEstimate:
    reasons: list[str] = []
    frame_quality = _frame_quality(option_frame)
    if frame_quality is FrameQuality.UNAVAILABLE:
        reasons.append("option_frame_unavailable")
    frame_at = _frame_timestamp(option_frame)
    if frame_at is None:
        reasons.append("option_frame_timestamp_unavailable")
    else:
        age = (now - frame_at).total_seconds()
        if age < -FUTURE_TIMESTAMP_TOLERANCE_SECONDS:
            reasons.append("option_frame_future_timestamp")
        elif age > settings.projection_ttl_seconds:
            reasons.append("option_frame_stale")
    expiry = _expiry_date(_frame_value(option_frame, "front_expiry"))
    if expiry is None:
        reasons.append("front_0dte_expiry_unavailable")
    elif expiry != trading_date:
        reasons.append("front_0dte_expiry_mismatch")
    volatility = _frame_value(option_frame, "volatility")
    atm_iv = _finite(volatility.get("atm_iv_0dte")) if isinstance(volatility, Mapping) else None
    if atm_iv is None or atm_iv <= 0.0:
        reasons.append("atm_iv_0dte_unavailable")
    if reasons or atm_iv is None:
        return _unavailable_probability(
            ProbabilityMeasure.RISK_NEUTRAL,
            event=event,
            reasons=tuple(sorted(set(reasons or ("atm_nd2_proxy_unavailable",)))),
            method_version=Q_METHOD_VERSION,
        )

    tau_years = settings.horizon_seconds / (365.0 * 24.0 * 60.0 * 60.0)
    try:
        d2_value = d1(spot, spot, atm_iv, tau_years) - atm_iv * math.sqrt(tau_years)
        probability = normal_cdf(d2_value) if direction == "up" else normal_cdf(-d2_value)
    except (OverflowError, ValueError, ZeroDivisionError):
        probability = None
    if probability is None or not math.isfinite(probability):
        return _unavailable_probability(
            ProbabilityMeasure.RISK_NEUTRAL,
            event=event,
            reasons=("atm_nd2_proxy_calculation_unavailable",),
            method_version=Q_METHOD_VERSION,
        )
    proxy_reasons = ["risk_neutral_atm_nd2_proxy", "risk_neutral_not_physical"]
    if frame_quality is FrameQuality.DEGRADED:
        proxy_reasons.append("option_frame_degraded")
    return ProbabilityEstimate(
        measure=ProbabilityMeasure.RISK_NEUTRAL,
        event=event,
        status=EstimateStatus.AVAILABLE,
        quality=EstimateQuality.DEGRADED,
        probability=round(max(0.0, min(1.0, probability)), 6),
        method_version=Q_METHOD_VERSION,
        reason_codes=tuple(sorted(set(proxy_reasons))),
    )


def _physical_estimate(
    *,
    data_root: str | Path,
    now: datetime,
    trading_date: date,
    direction: str,
    thesis: str | None,
    level_kind: str,
    settings: StrategyDistributionSettings,
    estimator: PhysicalEstimator,
) -> PhysicalFollowThroughEstimate:
    key = (
        str(Path(data_root).expanduser().resolve()),
        trading_date,
        direction,
        thesis,
        level_kind,
        settings.horizon_seconds,
        settings.window_days,
        settings.minimum_physical_samples,
        settings.beta_prior_alpha,
        settings.beta_prior_beta,
        id(estimator),
    )
    with _PHYSICAL_CACHE_LOCK:
        cached = _PHYSICAL_CACHE.get(key)
        if cached is not None:
            cached_at, estimate = cached
            age = (now - cached_at).total_seconds()
            if 0.0 <= age < settings.refresh_seconds:
                return estimate
    estimate = estimator(
        Path(data_root).expanduser() / "features",
        now=now,
        trading_date=trading_date,
        horizon_seconds=settings.horizon_seconds,
        window_days=settings.window_days,
        minimum_samples=settings.minimum_physical_samples,
        prior_alpha=settings.beta_prior_alpha,
        prior_beta=settings.beta_prior_beta,
        direction=direction,
        thesis=thesis,
        level_kind=level_kind,
    )
    if type(estimate) is not PhysicalFollowThroughEstimate:
        raise ValueError("physical estimator must return PhysicalFollowThroughEstimate")
    with _PHYSICAL_CACHE_LOCK:
        _PHYSICAL_CACHE[key] = (now, estimate)
    return estimate


def _physical_probability(
    event: ProbabilityEventDefinition,
    *,
    estimate: PhysicalFollowThroughEstimate,
    trading_date: date,
) -> ProbabilityEstimate:
    probability = _finite(estimate.probability)
    interval_low = _finite(estimate.interval_low)
    interval_high = _finite(estimate.interval_high)
    causal_training = (
        estimate.trained_through_date is not None and estimate.trained_through_date < trading_date
    )
    evidence_complete = (
        probability is not None
        and interval_low is not None
        and interval_high is not None
        and estimate.sample_count > 0
        and estimate.session_count > 0
        and causal_training
    )
    reasons = set(estimate.reason_codes)
    reasons.update(
        {
            "physical_probability_beta_baseline",
            "physical_probability_uncalibrated",
        }
    )
    if not causal_training and estimate.trained_through_date is not None:
        reasons.add("physical_training_date_not_prior")
    if not evidence_complete:
        reasons.add("physical_probability_unavailable")
        return _unavailable_probability(
            ProbabilityMeasure.PHYSICAL,
            event=event,
            reasons=tuple(sorted(reasons)),
            method_version=estimate.model_version or PHYSICAL_MODEL_VERSION,
            physical_counts=(
                max(estimate.sample_count, 0),
                max(estimate.session_count, 0),
            ),
            effective_sample_count=max(estimate.effective_sample_count, 0.0),
            historical_sessions=estimate.historical_sessions,
        )
    assert probability is not None
    assert interval_low is not None and interval_high is not None
    assert estimate.trained_through_date is not None
    return ProbabilityEstimate(
        measure=ProbabilityMeasure.PHYSICAL,
        event=event,
        status=EstimateStatus.AVAILABLE,
        quality=EstimateQuality.DEGRADED,
        probability=probability,
        method_version=estimate.model_version or PHYSICAL_MODEL_VERSION,
        reason_codes=tuple(sorted(reasons)),
        sample_count=estimate.sample_count,
        session_count=estimate.session_count,
        interval_low=interval_low,
        interval_high=interval_high,
        trained_through_date=estimate.trained_through_date,
        effective_sample_count=estimate.effective_sample_count,
        historical_sessions=estimate.historical_sessions,
    )


def _unavailable_probability(
    measure: ProbabilityMeasure,
    *,
    event: ProbabilityEventDefinition | None,
    reasons: tuple[str, ...],
    method_version: str | None = None,
    physical_counts: tuple[int, int] | None = None,
    effective_sample_count: float | None = None,
    historical_sessions: tuple[str, ...] = (),
) -> ProbabilityEstimate:
    sample_count: int | None = None
    session_count: int | None = None
    if measure is ProbabilityMeasure.PHYSICAL:
        sample_count, session_count = physical_counts or (0, 0)
    return ProbabilityEstimate(
        measure=measure,
        event=event,
        status=EstimateStatus.UNAVAILABLE,
        quality=EstimateQuality.UNAVAILABLE,
        probability=None,
        method_version=method_version,
        reason_codes=tuple(sorted(set(reasons))),
        sample_count=sample_count,
        session_count=session_count,
        effective_sample_count=(effective_sample_count if measure is ProbabilityMeasure.PHYSICAL else None),
        historical_sessions=(historical_sessions if measure is ProbabilityMeasure.PHYSICAL else ()),
    )


def _quality_reasons(
    q_event: ProbabilityEstimate,
    p_event: ProbabilityEstimate,
    *,
    direction_reasons: tuple[str, ...],
    spot_reasons: tuple[str, ...],
    disabled: bool,
) -> tuple[str, ...]:
    reasons = {
        "actual_fill_probability_unavailable",
        "net_pnl_distribution_unavailable",
        "strategy_candidates_unavailable",
        *direction_reasons,
        *spot_reasons,
    }
    if disabled:
        reasons.add("strategy_distribution_disabled")
    if q_event.status is EstimateStatus.UNAVAILABLE:
        reasons.add("q_probability_unavailable")
    if p_event.status is EstimateStatus.UNAVAILABLE:
        reasons.add("p_probability_unavailable")
    return tuple(sorted(reasons))


def _persist_strategy_distribution_forecast(
    payload: Mapping[str, object],
    *,
    data_root: str | Path,
    trading_date: date,
    append_interval_seconds: float,
) -> None:
    latest_path = latest_strategy_distribution_forecast_path(data_root)
    audit_path = strategy_distribution_forecast_audit_path(data_root, trading_date)
    incoming_at = _timestamp(payload.get("available_at"))
    if incoming_at is None:
        raise ValueError("strategy distribution payload available_at is invalid")
    with exclusive_state_lock(latest_path):
        current = read_json_object(latest_path)
        current_at = _timestamp(current.get("available_at"))
        if current_at is not None and current_at > incoming_at:
            return
        if current_at == incoming_at and current:
            if current.get("document_id") != payload.get("document_id"):
                raise ValueError("strategy distribution available_at collision")
            return
        last_audit = _latest_jsonl_record(audit_path)
        if _should_append(
            payload,
            previous=last_audit,
            append_interval_seconds=append_interval_seconds,
        ):
            append_jsonl_secure(audit_path, payload)
        atomic_write_json_secure(latest_path, payload)


def _should_append(
    payload: Mapping[str, object],
    *,
    previous: Mapping[str, object] | None,
    append_interval_seconds: float,
) -> bool:
    if not previous:
        return True
    if _semantic_fingerprint(payload) != _semantic_fingerprint(previous):
        return True
    current_at = _timestamp(payload.get("available_at"))
    previous_at = _timestamp(previous.get("available_at"))
    if current_at is None or previous_at is None:
        return True
    return (current_at - previous_at).total_seconds() >= append_interval_seconds


def _semantic_fingerprint(payload: Mapping[str, object]) -> str:
    q_event = payload.get("q_event") if isinstance(payload.get("q_event"), Mapping) else {}
    p_event = payload.get("p_event") if isinstance(payload.get("p_event"), Mapping) else {}
    event = q_event.get("event") if isinstance(q_event.get("event"), Mapping) else {}
    semantic = {
        "trading_date_et": payload.get("trading_date_et"),
        "session": payload.get("session"),
        "event_kind": event.get("kind"),
        "q": _probability_semantics(q_event),
        "p": _probability_semantics(p_event),
        "shadow_decision": payload.get("shadow_decision"),
        "quality": payload.get("quality"),
        "quality_reason_codes": payload.get("quality_reason_codes"),
        "automatic_ordering": payload.get("automatic_ordering"),
    }
    return _hash(semantic)


def _probability_semantics(payload: Mapping[str, object]) -> dict[str, object]:
    probability = _finite(payload.get("probability"))
    return {
        "status": payload.get("status"),
        "quality": payload.get("quality"),
        "probability": round(probability, 4) if probability is not None else None,
        "method_version": payload.get("method_version"),
        "reason_codes": payload.get("reason_codes"),
        "sample_count": payload.get("sample_count"),
        "session_count": payload.get("session_count"),
        "interval_low": payload.get("interval_low"),
        "interval_high": payload.get("interval_high"),
        "trained_through_date": payload.get("trained_through_date"),
    }


def _latest_jsonl_record(path: Path) -> dict[str, object] | None:
    try:
        with path.open("rb") as handle:
            handle.seek(0, 2)
            end = handle.tell()
            if end <= 0:
                return None
            handle.seek(max(0, end - 131_072))
            lines = handle.read().decode("utf-8", errors="ignore").splitlines()
    except OSError:
        return None
    for line in reversed(lines):
        try:
            row = json.loads(line)
        except json.JSONDecodeError:
            continue
        if isinstance(row, dict):
            return row
    return None


def _source_snapshot_id(
    state: LatestState,
    option_frame: OptionStructureFrame | Mapping[str, object],
    raw_level_decision: Mapping[str, object],
    *,
    spot_quote: Quote | None,
) -> str:
    identity = {
        "action_state_as_of": state.as_of.isoformat(),
        "spot_provider": spot_quote.provider.value if spot_quote else None,
        "spot_source_at": (
            (spot_quote.quote_time or spot_quote.trade_time or spot_quote.received_at).isoformat()
            if spot_quote
            else None
        ),
        "option_frame_id": _frame_value(option_frame, "frame_id"),
        "option_frame_as_of": _iso_value(_frame_value(option_frame, "as_of")),
        "level_event_id": raw_level_decision.get("event_id"),
        "level_direction": raw_level_decision.get("direction"),
        "level_updated_at": raw_level_decision.get("updated_at")
        or raw_level_decision.get("phase_at"),
    }
    return f"strategy-input:{_hash(identity)[:24]}"


def _document_id(
    *,
    source_snapshot_id: str,
    available_at: datetime,
    q_event: ProbabilityEstimate,
    p_event: ProbabilityEstimate,
) -> str:
    identity = {
        "source_snapshot_id": source_snapshot_id,
        "available_at": available_at.isoformat(),
        "q_event": q_event.to_dict(),
        "p_event": p_event.to_dict(),
    }
    return f"strategy-distribution:{_hash(identity)[:24]}"


def _observed_through(
    state: LatestState,
    option_frame: OptionStructureFrame | Mapping[str, object],
    raw_level_decision: Mapping[str, object],
    *,
    now: datetime,
) -> datetime:
    candidates = [state.as_of, state.created_at]
    frame_at = _frame_timestamp(option_frame)
    if frame_at is not None:
        candidates.append(frame_at)
    decision_at = _timestamp(
        raw_level_decision.get("updated_at") or raw_level_decision.get("phase_at")
    )
    if decision_at is not None:
        candidates.append(decision_at)
    valid = [as_utc(value) for value in candidates if as_utc(value) <= now]
    return max(valid, default=now)


def _thesis(raw_level_decision: Mapping[str, object]) -> str | None:
    value = str(raw_level_decision.get("thesis") or "").strip().lower()
    return value or None


def _frame_value(
    frame: OptionStructureFrame | Mapping[str, object],
    name: str,
) -> object:
    return getattr(frame, name) if isinstance(frame, OptionStructureFrame) else frame.get(name)


def _frame_quality(
    frame: OptionStructureFrame | Mapping[str, object],
) -> FrameQuality:
    value = _frame_value(frame, "quality")
    if type(value) is FrameQuality:
        return value
    try:
        return FrameQuality(str(value))
    except ValueError:
        return FrameQuality.UNAVAILABLE


def _frame_timestamp(
    frame: OptionStructureFrame | Mapping[str, object],
) -> datetime | None:
    value = _frame_value(frame, "as_of")
    if type(value) is datetime:
        try:
            return _aware_utc(value, "option frame as_of")
        except ValueError:
            return None
    return _timestamp(value)


def _expiry_date(value: object) -> date | None:
    if type(value) is date:
        return value
    if not isinstance(value, str):
        return None
    text = value.strip()
    for pattern in ("%Y%m%d", "%Y-%m-%d"):
        try:
            return datetime.strptime(text, pattern).date()
        except ValueError:
            continue
    return None


def _timestamp(value: object) -> datetime | None:
    if type(value) is datetime:
        try:
            return _aware_utc(value, "timestamp")
        except ValueError:
            return None
    if not isinstance(value, str) or not value.strip():
        return None
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        return None
    return parsed.astimezone(timezone.utc)


def _aware_utc(value: datetime, name: str) -> datetime:
    if type(value) is not datetime or value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{name} must be timezone-aware")
    return value.astimezone(timezone.utc)


def _finite(value: object) -> float | None:
    if isinstance(value, bool) or not isinstance(value, int | float):
        return None
    parsed = float(value)
    return parsed if math.isfinite(parsed) else None


def _iso_value(value: object) -> object:
    return value.isoformat() if type(value) is datetime else value


def _hash(value: object) -> str:
    rendered = json.dumps(
        value,
        allow_nan=False,
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
        default=str,
    )
    return hashlib.sha256(rendered.encode()).hexdigest()


__all__ = [
    "FEATURE_SET_VERSION",
    "LATEST_FILENAME",
    "MODEL_VERSION",
    "POLICY_VERSION",
    "Q_METHOD_VERSION",
    "build_strategy_distribution_forecast",
    "clear_strategy_distribution_forecast_cache",
    "latest_strategy_distribution_forecast_path",
    "process_strategy_distribution_forecast",
    "strategy_distribution_forecast_audit_path",
]
