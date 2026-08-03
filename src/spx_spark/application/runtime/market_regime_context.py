"""Build the strict advisory research-context document for the regime worker."""

from __future__ import annotations

import hashlib
import json
import math
from collections.abc import Mapping, Sequence
from datetime import date, datetime, timezone

from spx_spark.domain.research_context import (
    CASH_INDEX_ORDER,
    CashIndex,
    CloseLocationBucket,
    CloseLocationDistribution,
    CrossIndexFrame,
    FilteredRegimePosterior,
    ForecastDistribution,
    ForecastStatus,
    ForecastTarget,
    IndexObservation,
    IndexPriceKind,
    ObservationStatus,
    PriorRthContextReference,
    QuantileBand,
    ResearchContextDocument,
    ResearchContextStatus,
    ResearchDataQuality,
    SpxRangeForecast,
)
from spx_spark.market_calendar import DEFAULT_MARKET_CALENDAR


UTC = timezone.utc
DEFAULT_SOURCE_SKEW_SECONDS = 5.0
RISK_NEUTRAL_HEURISTIC_VERSION = "risk-neutral-close-advisory-heuristic:v1"
INTRADAY_EXTREME_MODEL_VERSION = "intraday-extreme-em-bootstrap:v1"
CLOSE_LOCATION_METHOD_VERSION = "filtered-bootstrap-regime-to-range-thirds:v1"


def build_research_context_document(
    *,
    signal: Mapping[str, object],
    market: Mapping[str, object],
    prior_rth_context: Mapping[str, object],
    available_at: datetime,
    regime_feature_set_version: str,
    cross_index_feature_set_version: str,
    model_version: str,
    state_names: Sequence[str],
    hmm_adjusted_model_version: str,
    hmm_close_shift_fraction: float,
    p10_z: float,
) -> dict[str, object]:
    available_at = _aware_utc(available_at, "available_at")
    fingerprint = str(signal.get("input_fingerprint") or "")
    suffix = fingerprint.removeprefix("sha256:")[:24] or _canonical_hash(signal)[7:31]
    trading_date = _trading_date(signal, available_at)
    frame = _cross_index_frame(
        market,
        trading_date=trading_date,
        available_at=available_at,
        feature_set_version=cross_index_feature_set_version,
        lineage_suffix=suffix,
    )
    prior = _prior_rth_reference(
        prior_rth_context,
        trading_date=trading_date,
        available_at=available_at,
        lineage_suffix=suffix,
    )
    regime, regime_reasons = _filtered_regime(
        signal,
        frame_id=frame.frame_id,
        trading_date=trading_date,
        available_at=available_at,
        feature_set_version=regime_feature_set_version,
        model_version=model_version,
        state_names=state_names,
        lineage_suffix=suffix,
    )
    forecasts = _forecasts(
        signal,
        regime=regime,
        trading_date=trading_date,
        available_at=available_at,
        lineage_suffix=suffix,
        hmm_adjusted_model_version=hmm_adjusted_model_version,
        hmm_close_shift_fraction=hmm_close_shift_fraction,
        p10_z=p10_z,
    )
    close_location = _close_location(
        regime=regime,
        forecasts=forecasts,
        observed_through=_observed_through(signal, available_at),
        available_at=available_at,
        target_at=forecasts[0].target_at,
    )
    return ResearchContextDocument(
        document_id=f"research-context:{suffix}",
        generated_at=available_at,
        cross_index_frame=frame,
        prior_rth_context=prior,
        regime=regime,
        regime_reason_codes=regime_reasons,
        forecasts=forecasts,
        close_location=close_location,
    ).to_dict()


def _cross_index_frame(
    market: Mapping[str, object],
    *,
    trading_date: date,
    available_at: datetime,
    feature_set_version: str,
    lineage_suffix: str,
) -> CrossIndexFrame:
    market_as_of = _parse_at(market.get("as_of"))
    observed_through = (
        market_as_of if market_as_of is not None and market_as_of <= available_at else available_at
    )
    cross_asset = _mapping(market.get("cross_asset"))
    cash_index = _mapping(cross_asset.get("cash_index"))
    raw_observations = _mapping(cash_index.get("observations"))
    cash_session_open = cash_index.get("cash_session_open") is True
    observations = tuple(
        _index_observation(
            instrument,
            _mapping(raw_observations.get(instrument.value)),
            cash_session_open=cash_session_open,
            observed_through=observed_through,
            available_at=available_at,
            lineage_suffix=lineage_suffix,
        )
        for instrument in CASH_INDEX_ORDER
    )
    skew_limit = _number(cash_index.get("source_skew_limit_seconds"))
    return CrossIndexFrame(
        frame_id=str(market.get("frame_id") or f"market-frame:{lineage_suffix}"),
        trading_date_et=trading_date,
        observed_through=observed_through,
        available_at=available_at,
        observations=observations,
        feature_set_version=feature_set_version,
        source_skew_limit_seconds=(
            skew_limit
            if skew_limit is not None and skew_limit > 0.0
            else DEFAULT_SOURCE_SKEW_SECONDS
        ),
    )


def _index_observation(
    instrument: CashIndex,
    raw: Mapping[str, object],
    *,
    cash_session_open: bool,
    observed_through: datetime,
    available_at: datetime,
    lineage_suffix: str,
) -> IndexObservation:
    source_at = _parse_at(raw.get("source_at"))
    price = _number(raw.get("price"))
    reference_close = _number(raw.get("reference_close"))
    provider = str(raw.get("provider") or "")
    quality = _quality(raw.get("quality"))
    price_kind = _price_kind(raw.get("price_kind"))
    reason = None
    if not cash_session_open:
        reason = "cash_index_cash_session_closed"
    elif raw.get("status") != "available":
        reason = str(raw.get("missing_reason") or "fresh_cash_index_quote_unavailable")
    elif source_at is None:
        reason = "cash_index_source_timestamp_unavailable"
    elif source_at > observed_through or source_at > available_at:
        reason = "cash_index_source_timestamp_from_future"
    elif price is None or price <= 0.0:
        reason = "cash_index_price_unavailable"
    elif not provider:
        reason = "cash_index_provider_unavailable"
    elif quality is None or quality is ResearchDataQuality.MISSING:
        reason = "cash_index_quality_unavailable"
    elif price_kind is None:
        reason = "cash_index_price_kind_unavailable"
    lineage_id = f"cash-index:{instrument.value}:{lineage_suffix}"
    if reason is not None:
        return IndexObservation(
            instrument=instrument,
            status=ObservationStatus.MISSING,
            quality=ResearchDataQuality.MISSING,
            available_at=available_at,
            lineage_id=lineage_id,
            missing_reason=reason,
        )
    assert source_at is not None and price is not None and quality is not None
    assert price_kind is not None
    return IndexObservation(
        instrument=instrument,
        status=ObservationStatus.AVAILABLE,
        quality=quality,
        available_at=available_at,
        lineage_id=lineage_id,
        price=price,
        reference_close=reference_close,
        price_kind=price_kind,
        provider=provider,
        source_as_of=source_at,
    )


def _prior_rth_reference(
    raw: Mapping[str, object],
    *,
    trading_date: date,
    available_at: datetime,
    lineage_suffix: str,
) -> PriorRthContextReference:
    expected_session = DEFAULT_MARKET_CALENDAR.previous_trading_day(trading_date)
    parsed_for_date = _parse_date(raw.get("for_trading_date"))
    parsed_session = _parse_date(raw.get("session_date"))
    source_as_of = _parse_at(raw.get("as_of"))
    reasons = list(_string_values(raw.get("reasons")))
    schema_ok = raw.get("schema_version") == "prior_rth_context.v2"
    date_ok = parsed_for_date == trading_date and parsed_session == expected_session
    time_ok = source_as_of is not None and source_as_of <= available_at
    raw_status = str(raw.get("status") or "unavailable")
    status = (
        ResearchContextStatus(raw_status)
        if raw_status in {value.value for value in ResearchContextStatus}
        else ResearchContextStatus.UNAVAILABLE
    )
    if not schema_ok:
        reasons.append("prior_rth_context_v2_unavailable")
        status = ResearchContextStatus.UNAVAILABLE
    if not date_ok:
        reasons.append("prior_rth_context_trading_date_mismatch")
        status = ResearchContextStatus.UNAVAILABLE
    if not time_ok:
        reasons.append("prior_rth_context_time_invalid")
        status = ResearchContextStatus.UNAVAILABLE
    cross_index = _mapping(raw.get("cross_index"))
    returns = _mapping(cross_index.get("return_bps")) if schema_ok else {}
    return_bps = tuple(
        (instrument, _number_signed(returns.get(instrument.value)))
        for instrument in CASH_INDEX_ORDER
    )
    available_return_count = sum(value is not None for _instrument, value in return_bps)
    if status is ResearchContextStatus.READY and available_return_count < len(CASH_INDEX_ORDER):
        reasons.append("prior_rth_returns_incomplete")
        status = (
            ResearchContextStatus.PARTIAL
            if available_return_count
            else ResearchContextStatus.UNAVAILABLE
        )
    if status is ResearchContextStatus.PARTIAL:
        if available_return_count == 0:
            reasons.append("prior_rth_returns_unavailable")
            status = ResearchContextStatus.UNAVAILABLE
        elif not reasons:
            reasons.append("prior_rth_context_partial")
    if status is ResearchContextStatus.UNAVAILABLE and not reasons:
        reasons.append("prior_rth_context_unavailable")
    resolved_source = source_as_of if time_ok and source_as_of is not None else available_at
    return PriorRthContextReference(
        context_id=f"prior-rth:{lineage_suffix}",
        status=status,
        for_trading_date=trading_date,
        session_date=expected_session,
        source_as_of=resolved_source,
        available_at=available_at,
        return_bps=return_bps,
        reason_codes=tuple(sorted(set(reasons))),
    )


def _filtered_regime(
    signal: Mapping[str, object],
    *,
    frame_id: str,
    trading_date: date,
    available_at: datetime,
    feature_set_version: str,
    model_version: str,
    state_names: Sequence[str],
    lineage_suffix: str,
) -> tuple[FilteredRegimePosterior | None, tuple[str, ...]]:
    raw = _mapping(signal.get("regime"))
    reasons = list(_string_values(raw.get("reasons")))
    observed_through = _parse_at(raw.get("as_of"))
    probabilities = _mapping(raw.get("posterior"))
    ordered = tuple((state, _probability(probabilities.get(state))) for state in state_names)
    valid = (
        raw.get("status") == "available"
        and observed_through is not None
        and observed_through <= available_at
        and all(probability is not None for _state, probability in ordered)
        and int(raw.get("observation_count") or 0) > 0
    )
    if not valid:
        if not reasons:
            reasons.append("filtered_bootstrap_regime_unavailable")
        return None, tuple(sorted(set(reasons)))
    assert observed_through is not None
    posterior = FilteredRegimePosterior(
        signal_id=f"regime:{lineage_suffix}",
        frame_id=frame_id,
        model_version=model_version,
        feature_set_version=feature_set_version,
        sequence_id=trading_date.isoformat(),
        trading_date_et=trading_date,
        observed_through=observed_through,
        available_at=available_at,
        update_index=int(raw["observation_count"]),
        probabilities=tuple((state, float(probability)) for state, probability in ordered),
    )
    return posterior, tuple(sorted(set(reasons)))


def _forecasts(
    signal: Mapping[str, object],
    *,
    regime: FilteredRegimePosterior | None,
    trading_date: date,
    available_at: datetime,
    lineage_suffix: str,
    hmm_adjusted_model_version: str,
    hmm_close_shift_fraction: float,
    p10_z: float,
) -> tuple[SpxRangeForecast, ...]:
    ranges = _mapping(signal.get("today_range"))
    close_values = _mapping(ranges.get("close"))
    target_at = _parse_at(close_values.get("target_at"))
    session = DEFAULT_MARKET_CALENDAR.session(trading_date)
    if target_at is None and session is not None:
        target_at = session.close_at.astimezone(UTC)
    target_at = target_at or available_at
    observed_through = _observed_through(signal, available_at)
    close = _close_forecast(
        close_values,
        regime=regime,
        observed_through=observed_through,
        available_at=available_at,
        target_at=target_at,
        lineage_suffix=lineage_suffix,
        hmm_adjusted_model_version=hmm_adjusted_model_version,
        hmm_close_shift_fraction=hmm_close_shift_fraction,
        p10_z=p10_z,
    )
    high = _intraday_extreme_forecast(
        _mapping(ranges.get("high")),
        target=ForecastTarget.SESSION_HIGH,
        observed_through=observed_through,
        available_at=available_at,
        target_at=target_at,
        lineage_suffix=lineage_suffix,
    )
    low = _intraday_extreme_forecast(
        _mapping(ranges.get("low")),
        target=ForecastTarget.SESSION_LOW,
        observed_through=observed_through,
        available_at=available_at,
        target_at=target_at,
        lineage_suffix=lineage_suffix,
    )
    return close, high, low


def _intraday_extreme_forecast(
    values: Mapping[str, object],
    *,
    target: ForecastTarget,
    observed_through: datetime,
    available_at: datetime,
    target_at: datetime,
    lineage_suffix: str,
) -> SpxRangeForecast:
    p10, p50, p90 = (
        _number(values.get("p10")),
        _number(values.get("p50")),
        _number(values.get("p90")),
    )
    valid = (
        values.get("status") == "available"
        and p10 is not None
        and p50 is not None
        and p90 is not None
        and p10 < p50 < p90
        and target_at > available_at
    )
    if not valid:
        reason = (
            "rth_close_target_elapsed"
            if target_at <= available_at
            else str(values.get("reason") or f"{target.value}_model_unavailable")
        )
        return _unavailable_forecast(
            target,
            reason,
            observed_through=observed_through,
            available_at=available_at,
            target_at=target_at,
            lineage_suffix=lineage_suffix,
        )
    assert p10 is not None and p50 is not None and p90 is not None
    return SpxRangeForecast(
        forecast_id=f"forecast:{target.value}:{lineage_suffix}",
        target=target,
        status=ForecastStatus.AVAILABLE,
        observed_through=observed_through,
        available_at=available_at,
        target_at=target_at,
        reason_codes=("expected_move_extreme_mapping_unvalidated",),
        distribution=ForecastDistribution.EXPERIMENTAL_HEURISTIC,
        quantiles=QuantileBand(p10=p10, p50=p50, p90=p90),
        model_version=str(values.get("model_version") or INTRADAY_EXTREME_MODEL_VERSION),
    )


def _close_location(
    *,
    regime: FilteredRegimePosterior | None,
    forecasts: tuple[SpxRangeForecast, ...],
    observed_through: datetime,
    available_at: datetime,
    target_at: datetime,
) -> CloseLocationDistribution:
    forecast_by_target = {forecast.target: forecast for forecast in forecasts}
    required = (
        forecast_by_target[ForecastTarget.RTH_CLOSE],
        forecast_by_target[ForecastTarget.SESSION_HIGH],
        forecast_by_target[ForecastTarget.SESSION_LOW],
    )
    if target_at <= available_at:
        reason = "rth_close_target_elapsed"
    elif regime is None:
        reason = "filtered_bootstrap_regime_unavailable"
    elif any(forecast.status is ForecastStatus.UNAVAILABLE for forecast in required):
        reason = "close_high_low_forecasts_required"
    else:
        probability_by_state = dict(regime.probabilities)
        raw = (
            probability_by_state.get("state_00", 0.0),
            probability_by_state.get("state_01", 0.0),
            probability_by_state.get("state_02", 0.0),
        )
        total = math.fsum(raw)
        if total > 0.0:
            return CloseLocationDistribution(
                status=ForecastStatus.AVAILABLE,
                observed_through=observed_through,
                available_at=available_at,
                target_at=target_at,
                reason_codes=("latent_state_location_mapping_unvalidated",),
                probabilities=tuple(
                    (bucket, probability / total)
                    for bucket, probability in zip(
                        (
                            CloseLocationBucket.LOWER_THIRD,
                            CloseLocationBucket.MIDDLE_THIRD,
                            CloseLocationBucket.UPPER_THIRD,
                        ),
                        raw,
                        strict=True,
                    )
                ),
                method_version=CLOSE_LOCATION_METHOD_VERSION,
                distribution=ForecastDistribution.EXPERIMENTAL_HEURISTIC,
            )
        reason = "filtered_bootstrap_probabilities_invalid"
    return CloseLocationDistribution(
        status=ForecastStatus.UNAVAILABLE,
        observed_through=observed_through,
        available_at=available_at,
        target_at=target_at,
        reason_codes=(reason,),
    )


def _close_forecast(
    values: Mapping[str, object],
    *,
    regime: FilteredRegimePosterior | None,
    observed_through: datetime,
    available_at: datetime,
    target_at: datetime,
    lineage_suffix: str,
    hmm_adjusted_model_version: str,
    hmm_close_shift_fraction: float,
    p10_z: float,
) -> SpxRangeForecast:
    p10, p50, p90 = (
        _number(values.get("p10")),
        _number(values.get("p50")),
        _number(values.get("p90")),
    )
    valid = (
        values.get("status") == "available"
        and p10 is not None
        and p50 is not None
        and p90 is not None
        and p10 < p50 < p90
        and target_at > available_at
    )
    if not valid:
        reason = (
            "rth_close_target_elapsed"
            if target_at <= available_at
            else str(values.get("reason") or "fresh_risk_neutral_density_unavailable")
        )
        return _unavailable_forecast(
            ForecastTarget.RTH_CLOSE,
            reason,
            observed_through=observed_through,
            available_at=available_at,
            target_at=target_at,
            lineage_suffix=lineage_suffix,
        )
    assert p10 is not None and p50 is not None and p90 is not None
    shift = 0.0
    model_version = RISK_NEUTRAL_HEURISTIC_VERSION
    reasons = ["risk_neutral_source_not_physical"]
    if regime is not None:
        probability_by_state = dict(regime.probabilities)
        expected_move = _number(values.get("expected_move_points"))
        if expected_move is None:
            expected_move = (p90 - p10) / (2.0 * p10_z)
        shift = (
            hmm_close_shift_fraction
            * expected_move
            * (
                probability_by_state.get("state_02", 0.0)
                - probability_by_state.get("state_00", 0.0)
            )
        )
        model_version = hmm_adjusted_model_version
        reasons.append("fixed_bootstrap_hmm_shift_unvalidated")
    return SpxRangeForecast(
        forecast_id=f"forecast:rth-close:{lineage_suffix}",
        target=ForecastTarget.RTH_CLOSE,
        status=ForecastStatus.AVAILABLE,
        observed_through=observed_through,
        available_at=available_at,
        target_at=target_at,
        reason_codes=tuple(sorted(reasons)),
        distribution=ForecastDistribution.EXPERIMENTAL_HEURISTIC,
        quantiles=QuantileBand(p10=p10 + shift, p50=p50 + shift, p90=p90 + shift),
        model_version=model_version,
    )


def _unavailable_forecast(
    target: ForecastTarget,
    reason: str,
    *,
    observed_through: datetime,
    available_at: datetime,
    target_at: datetime,
    lineage_suffix: str,
) -> SpxRangeForecast:
    return SpxRangeForecast(
        forecast_id=f"forecast:{target.value}:{lineage_suffix}",
        target=target,
        status=ForecastStatus.UNAVAILABLE,
        observed_through=observed_through,
        available_at=available_at,
        target_at=target_at,
        reason_codes=(reason,),
    )


def _trading_date(signal: Mapping[str, object], available_at: datetime) -> date:
    parsed = _parse_date(signal.get("session_date"))
    return (
        parsed
        or DEFAULT_MARKET_CALENDAR.spx_session_date_for(available_at, retain_completed=True)
        or available_at.date()
    )


def _quality(value: object) -> ResearchDataQuality | None:
    try:
        return ResearchDataQuality(str(value))
    except ValueError:
        return None


def _price_kind(value: object) -> IndexPriceKind | None:
    try:
        return IndexPriceKind(str(value))
    except ValueError:
        return None


def _parse_date(value: object) -> date | None:
    if not isinstance(value, str):
        return None
    try:
        return date.fromisoformat(value)
    except ValueError:
        return None


def _parse_at(value: object) -> datetime | None:
    if not isinstance(value, str):
        return None
    try:
        parsed = datetime.fromisoformat(value)
    except ValueError:
        return None
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        return None
    return parsed.astimezone(UTC)


def _aware_utc(value: datetime, name: str) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{name} must be timezone-aware")
    return value.astimezone(UTC)


def _observed_through(signal: Mapping[str, object], available_at: datetime) -> datetime:
    observed_through = _parse_at(signal.get("as_of"))
    if observed_through is None or observed_through > available_at:
        return available_at
    return observed_through


def _mapping(value: object) -> dict[str, object]:
    return dict(value) if isinstance(value, Mapping) else {}


def _string_values(value: object) -> tuple[str, ...]:
    if not isinstance(value, Sequence) or isinstance(value, str | bytes):
        return ()
    return tuple(item for item in value if isinstance(item, str) and item)


def _number(value: object) -> float | None:
    if isinstance(value, bool) or not isinstance(value, int | float):
        return None
    parsed = float(value)
    return parsed if math.isfinite(parsed) and parsed > 0.0 else None


def _probability(value: object) -> float | None:
    parsed = _number_signed(value)
    return parsed if parsed is not None and 0.0 <= parsed <= 1.0 else None


def _number_signed(value: object) -> float | None:
    if isinstance(value, bool) or not isinstance(value, int | float):
        return None
    parsed = float(value)
    return parsed if math.isfinite(parsed) else None


def _canonical_hash(value: object) -> str:
    encoded = json.dumps(value, allow_nan=False, separators=(",", ":"), sort_keys=True).encode()
    return "sha256:" + hashlib.sha256(encoded).hexdigest()


__all__ = ["build_research_context_document"]
