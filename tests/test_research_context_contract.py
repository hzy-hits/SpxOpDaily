from __future__ import annotations

from dataclasses import replace
from datetime import date, datetime, timedelta, timezone

import pytest

from spx_spark.domain.research_context import (
    CASH_INDEX_ORDER,
    ActionAuthority,
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


UTC = timezone.utc
OBSERVED = datetime(2026, 8, 3, 15, 0, tzinfo=UTC)
AVAILABLE = OBSERVED + timedelta(seconds=1)
TARGET = datetime(2026, 8, 3, 20, 0, tzinfo=UTC)


def _observation(index: int) -> IndexObservation:
    instrument = CASH_INDEX_ORDER[index]
    price = (6_300.0, 23_000.0, 44_000.0, 2_250.0)[index]
    return IndexObservation(
        instrument=instrument,
        status=ObservationStatus.AVAILABLE,
        quality=ResearchDataQuality.LIVE,
        price=price,
        reference_close=price - 10.0,
        price_kind=IndexPriceKind.LAST,
        provider="schwab",
        source_as_of=OBSERVED - timedelta(milliseconds=index * 250),
        available_at=AVAILABLE,
        lineage_id=f"quote:{instrument.value}:1",
    )


def _frame(*, observations: tuple[IndexObservation, ...] | None = None) -> CrossIndexFrame:
    return CrossIndexFrame(
        frame_id="cross-index:2026-08-03:1500",
        trading_date_et=date(2026, 8, 3),
        observed_through=OBSERVED,
        available_at=AVAILABLE,
        observations=observations or tuple(_observation(index) for index in range(4)),
        feature_set_version="cash-index-rth:v1",
        source_skew_limit_seconds=5.0,
    )


def _unavailable(target: ForecastTarget) -> SpxRangeForecast:
    return SpxRangeForecast(
        forecast_id=f"forecast:{target.value}:1",
        target=target,
        status=ForecastStatus.UNAVAILABLE,
        observed_through=OBSERVED,
        available_at=AVAILABLE,
        target_at=TARGET,
        reason_codes=("physical_model_unavailable",),
    )


def _prior_rth() -> PriorRthContextReference:
    return PriorRthContextReference(
        context_id="prior-rth:2026-07-31",
        status=ResearchContextStatus.READY,
        for_trading_date=date(2026, 8, 3),
        session_date=date(2026, 7, 31),
        source_as_of=OBSERVED - timedelta(days=2),
        available_at=OBSERVED - timedelta(days=2) + timedelta(seconds=1),
        return_bps=tuple(
            (instrument, float(index)) for index, instrument in enumerate(CASH_INDEX_ORDER)
        ),
        reason_codes=(),
    )


def _close_location() -> CloseLocationDistribution:
    return CloseLocationDistribution(
        status=ForecastStatus.AVAILABLE,
        observed_through=OBSERVED,
        available_at=AVAILABLE,
        target_at=TARGET,
        reason_codes=("fixed_bootstrap_mapping_unvalidated",),
        probabilities=(
            (CloseLocationBucket.LOWER_THIRD, 0.2),
            (CloseLocationBucket.MIDDLE_THIRD, 0.5),
            (CloseLocationBucket.UPPER_THIRD, 0.3),
        ),
        method_version="filtered-regime-location-map:v1",
        distribution=ForecastDistribution.EXPERIMENTAL_HEURISTIC,
    )


def _unavailable_close_location() -> CloseLocationDistribution:
    return CloseLocationDistribution(
        status=ForecastStatus.UNAVAILABLE,
        observed_through=OBSERVED,
        available_at=AVAILABLE,
        target_at=TARGET,
        reason_codes=("close_high_low_forecasts_required",),
    )


def test_cross_index_frame_requires_all_indices_with_explicit_missing_observation() -> None:
    missing_rut = IndexObservation(
        instrument=CASH_INDEX_ORDER[-1],
        status=ObservationStatus.MISSING,
        quality=ResearchDataQuality.MISSING,
        available_at=AVAILABLE,
        lineage_id="missing:index:RUT:1",
        missing_reason="fresh_quote_unavailable",
    )
    frame = _frame(observations=(*tuple(_observation(index) for index in range(3)), missing_rut))

    assert frame.status == "degraded"
    assert frame.missing_instruments == ("index:RUT",)
    assert frame.to_dict()["observations"][-1]["missing_reason"] == "fresh_quote_unavailable"

    with pytest.raises(ValueError, match="exactly once"):
        _frame(observations=tuple(_observation(index) for index in range(3)))


def test_observation_rejects_future_source_and_missing_values_hidden_as_live() -> None:
    with pytest.raises(ValueError, match="source_as_of is after available_at"):
        replace(_observation(0), source_as_of=AVAILABLE + timedelta(seconds=1))
    with pytest.raises(ValueError, match="available observation price missing"):
        replace(_observation(1), price=None)


def test_frame_reports_source_skew_without_claiming_readiness() -> None:
    observations = list(_observation(index) for index in range(4))
    observations[-1] = replace(
        observations[-1],
        source_as_of=OBSERVED - timedelta(seconds=7),
    )
    frame = _frame(observations=tuple(observations))

    assert frame.source_skew_seconds == pytest.approx(7.0)
    assert frame.status == "degraded"


def test_hmm_contract_is_filtered_advisory_and_bootstrap_unvalidated() -> None:
    posterior = FilteredRegimePosterior(
        signal_id="regime:1",
        frame_id=_frame().frame_id,
        model_version="fixed-bootstrap:sha256:1",
        feature_set_version="cash-index-rth:v1",
        sequence_id="2026-08-03",
        trading_date_et=date(2026, 8, 3),
        observed_through=OBSERVED,
        available_at=AVAILABLE,
        update_index=1,
        probabilities=(("state_00", 0.2), ("state_01", 0.5), ("state_02", 0.3)),
    )

    payload = posterior.to_dict()
    assert payload["inference"] == "filtered"
    assert payload["parameter_mode"] == "fixed_bootstrap"
    assert payload["evidence_status"] == "bootstrap_unvalidated"
    assert payload["use_scope"] == "advisory"
    assert payload["trained_through_date"] is None

    with pytest.raises(ValueError, match="sum to one"):
        replace(
            posterior,
            probabilities=(("state_00", 0.2), ("state_01", 0.2), ("state_02", 0.2)),
        )


def test_prior_rth_reference_requires_v2_causal_dates_and_four_index_returns() -> None:
    prior = _prior_rth()
    assert prior.to_dict()["schema_version"] == "prior_rth_context.v2"
    assert list(prior.to_dict()["return_bps"]) == [
        "index:SPX",
        "index:NDX",
        "index:DJI",
        "index:RUT",
    ]
    with pytest.raises(ValueError, match="must precede"):
        replace(prior, session_date=prior.for_trading_date)
    with pytest.raises(ValueError, match="ready prior-RTH returns incomplete"):
        replace(
            prior,
            return_bps=(*prior.return_bps[:-1], (CASH_INDEX_ORDER[-1], None)),
        )


def test_forecast_unavailable_is_explicit_and_cannot_claim_distribution() -> None:
    unavailable = _unavailable(ForecastTarget.RTH_CLOSE)
    assert unavailable.to_dict()["status"] == "unavailable"
    assert unavailable.to_dict()["reason_codes"] == ["physical_model_unavailable"]

    with pytest.raises(ValueError, match="requires reason_codes"):
        replace(unavailable, reason_codes=())
    with pytest.raises(ValueError, match="cannot claim"):
        replace(
            unavailable,
            distribution=ForecastDistribution.EXPERIMENTAL_HEURISTIC,
        )


def test_available_forecast_preserves_distribution_semantics_and_quantile_order() -> None:
    forecast = SpxRangeForecast(
        forecast_id="forecast:rth-close:1",
        target=ForecastTarget.RTH_CLOSE,
        status=ForecastStatus.AVAILABLE,
        observed_through=OBSERVED,
        available_at=AVAILABLE,
        target_at=TARGET,
        reason_codes=(),
        distribution=ForecastDistribution.RISK_NEUTRAL,
        quantiles=QuantileBand(p10=6_250.0, p50=6_300.0, p90=6_350.0),
        model_version="risk-neutral-density:v1",
    )

    assert forecast.to_dict()["distribution"] == "risk_neutral"
    with pytest.raises(ValueError, match="strictly ordered"):
        replace(forecast, quantiles=QuantileBand(p10=6_300.0, p50=6_300.0, p90=6_350.0))


def test_close_location_uses_explicit_thirds_and_normalized_heuristic_probabilities() -> None:
    location = _close_location()
    assert location.to_dict()["bucket_definition"] == (
        "thirds_of_projected_session_low_to_high_range"
    )
    assert sum(location.to_dict()["probabilities"].values()) == pytest.approx(1.0)
    with pytest.raises(ValueError, match="sum to one"):
        replace(
            location,
            probabilities=(
                (CloseLocationBucket.LOWER_THIRD, 0.2),
                (CloseLocationBucket.MIDDLE_THIRD, 0.2),
                (CloseLocationBucket.UPPER_THIRD, 0.2),
            ),
        )


def test_document_requires_explicit_close_high_low_and_cannot_gain_authority() -> None:
    forecasts = tuple(_unavailable(target) for target in ForecastTarget)
    document = ResearchContextDocument(
        document_id="research:1",
        generated_at=AVAILABLE,
        cross_index_frame=_frame(),
        prior_rth_context=_prior_rth(),
        regime=None,
        regime_reason_codes=("bootstrap_signal_unavailable",),
        forecasts=forecasts,
        close_location=_unavailable_close_location(),
    )

    payload = document.to_dict()
    assert payload["automatic_ordering"] is False
    assert payload["action_authority"] == "none"
    assert payload["evidence_status"] == "bootstrap_unvalidated"
    assert [row["status"] for row in payload["forecasts"]] == [
        "unavailable",
        "unavailable",
        "unavailable",
    ]

    with pytest.raises(ValueError, match="close/high/low"):
        replace(document, forecasts=forecasts[:1])
    with pytest.raises(ValueError, match="action authority"):
        replace(document, action_authority=ActionAuthority.NONE.value)  # type: ignore[arg-type]
    with pytest.raises(ValueError, match="requires close/high/low"):
        replace(document, close_location=_close_location())
