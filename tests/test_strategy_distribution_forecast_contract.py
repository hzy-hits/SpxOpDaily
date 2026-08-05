from __future__ import annotations

from dataclasses import replace
from datetime import date, datetime, timedelta, timezone

import pytest

from spx_spark.domain.strategy_distribution_forecast import (
    CalibrationStatus,
    CandidateDirection,
    CandidateScore,
    EstimateQuality,
    EstimateStatus,
    ExecutionEstimate,
    ForecastQuality,
    ForecastSession,
    NetPnlEstimate,
    ProbabilityEstimate,
    ProbabilityEventDefinition,
    ProbabilityEventKind,
    ProbabilityMeasure,
    ShadowAction,
    ShadowDecision,
    StrategyCandidate,
    StrategyDistributionForecast,
)


UTC = timezone.utc
TRADING_DATE = date(2026, 8, 5)
OBSERVED = datetime(2026, 8, 5, 14, 30, tzinfo=UTC)
AVAILABLE = OBSERVED + timedelta(seconds=1)
VALID_UNTIL = AVAILABLE + timedelta(seconds=90)
TARGET_AT = datetime(2026, 8, 5, 20, 0, tzinfo=UTC)


def _event(
    *,
    event_id: str = "event:spx-close-above-7760",
    lower_level: float = 7_760.0,
) -> ProbabilityEventDefinition:
    return ProbabilityEventDefinition(
        event_id=event_id,
        kind=ProbabilityEventKind.TERMINAL_ABOVE,
        target_at=TARGET_AT,
        lower_level=lower_level,
    )


def _probability(
    measure: ProbabilityMeasure,
    *,
    event: ProbabilityEventDefinition | None = None,
    probability: float = 0.40,
) -> ProbabilityEstimate:
    physical = measure is ProbabilityMeasure.PHYSICAL
    return ProbabilityEstimate(
        measure=measure,
        event=event or _event(),
        status=EstimateStatus.AVAILABLE,
        quality=EstimateQuality.READY,
        probability=probability,
        method_version=(
            "q-density:bid-ask-constrained:v1"
            if measure is ProbabilityMeasure.RISK_NEUTRAL
            else "p-terminal-offset:v1"
        ),
        reason_codes=(),
        sample_count=40 if physical else None,
        session_count=8 if physical else None,
        interval_low=0.30 if physical else None,
        interval_high=0.50 if physical else None,
        trained_through_date=date(2026, 8, 4) if physical else None,
    )


def _unavailable_probability(measure: ProbabilityMeasure) -> ProbabilityEstimate:
    physical = measure is ProbabilityMeasure.PHYSICAL
    return ProbabilityEstimate(
        measure=measure,
        event=_event(),
        status=EstimateStatus.UNAVAILABLE,
        quality=EstimateQuality.UNAVAILABLE,
        probability=None,
        method_version=None,
        reason_codes=(
            "physical_probability_unavailable"
            if measure is ProbabilityMeasure.PHYSICAL
            else "risk_neutral_probability_unavailable",
        ),
        sample_count=0 if physical else None,
        session_count=0 if physical else None,
    )


def _execution(*, available: bool = True) -> ExecutionEstimate:
    if not available:
        return ExecutionEstimate(
            status=EstimateStatus.UNAVAILABLE,
            limit_debit_points=2.10,
            wait_horizon_seconds=10,
            quote_reach_probability=None,
            model_version=None,
            reason_codes=("quote_reach_model_unavailable",),
        )
    return ExecutionEstimate(
        status=EstimateStatus.AVAILABLE,
        limit_debit_points=2.10,
        wait_horizon_seconds=10,
        quote_reach_probability=0.62,
        model_version="displayed-quote-reach:logistic:v1",
        reason_codes=(),
    )


def _net_pnl(*, available: bool = True) -> NetPnlEstimate:
    if not available:
        return NetPnlEstimate(
            status=EstimateStatus.UNAVAILABLE,
            expected_net_pnl=None,
            p10_net_pnl=None,
            p50_net_pnl=None,
            p90_net_pnl=None,
            tail_loss_p10=None,
            model_version=None,
            reason_codes=("net_pnl_model_unavailable",),
        )
    return NetPnlEstimate(
        status=EstimateStatus.AVAILABLE,
        expected_net_pnl=75.0,
        p10_net_pnl=-80.0,
        p50_net_pnl=65.0,
        p90_net_pnl=280.0,
        tail_loss_p10=120.0,
        model_version="net-pnl-quantile:v1",
        reason_codes=(),
    )


def _score(*, available: bool = True) -> CandidateScore:
    if not available:
        return CandidateScore(
            status=EstimateStatus.UNAVAILABLE,
            tail_risk_penalty=None,
            model_uncertainty_penalty=None,
            liquidity_risk_penalty=None,
            total=None,
            reason_codes=("candidate_score_unavailable",),
        )
    return CandidateScore(
        status=EstimateStatus.AVAILABLE,
        tail_risk_penalty=20.0,
        model_uncertainty_penalty=10.0,
        liquidity_risk_penalty=5.0,
        total=40.0,
        reason_codes=(),
    )


def _candidate(
    *,
    candidate_id: str = "candidate:call-7750-7760",
    execution: ExecutionEstimate | None = None,
    net_pnl: NetPnlEstimate | None = None,
    score: CandidateScore | None = None,
) -> StrategyCandidate:
    return StrategyCandidate(
        candidate_id=candidate_id,
        probability_event_id=_event().event_id,
        direction=CandidateDirection.CALL_VERTICAL_10,
        expiry=TRADING_DATE,
        long_contract_id="SPXW:2026-08-05:C:7750",
        short_contract_id="SPXW:2026-08-05:C:7760",
        long_strike=7_750.0,
        short_strike=7_760.0,
        execution=execution or _execution(),
        net_pnl=net_pnl or _net_pnl(),
        score=score or _score(),
        reason_codes=(),
    )


def _no_trade_decision(
    *, reasons: tuple[str, ...] = ("model_inputs_unavailable",)
) -> ShadowDecision:
    return ShadowDecision(
        action=ShadowAction.NO_TRADE,
        selected_candidate_id=None,
        score_threshold=0.0,
        reason_codes=reasons,
    )


def _document(
    *,
    q_event: ProbabilityEstimate | None = None,
    p_event: ProbabilityEstimate | None = None,
    candidates: tuple[StrategyCandidate, ...] = (),
    decision: ShadowDecision | None = None,
    quality: ForecastQuality = ForecastQuality.UNAVAILABLE,
    quality_reasons: tuple[str, ...] = ("distribution_inputs_unavailable",),
) -> StrategyDistributionForecast:
    return StrategyDistributionForecast(
        document_id="strategy-distribution:2026-08-05:143000:1",
        source_snapshot_id="analytical-option-snapshot:2026-08-05:143000",
        trading_date_et=TRADING_DATE,
        session=ForecastSession.RTH,
        observed_through=OBSERVED,
        available_at=AVAILABLE,
        valid_until=VALID_UNTIL,
        model_version="strategy-distribution:v1",
        feature_set_version="decision-snapshot:v1",
        calibration_status=CalibrationStatus.UNCALIBRATED,
        calibration_version=None,
        policy_version="fixed10-shadow:v1",
        q_event=q_event or _unavailable_probability(ProbabilityMeasure.RISK_NEUTRAL),
        p_event=p_event or _unavailable_probability(ProbabilityMeasure.PHYSICAL),
        strategy_candidates=candidates,
        shadow_decision=decision or _no_trade_decision(),
        quality=quality,
        quality_reason_codes=quality_reasons,
    )


def _manual_document() -> StrategyDistributionForecast:
    candidate = _candidate()
    return _document(
        q_event=_probability(ProbabilityMeasure.RISK_NEUTRAL, probability=0.28),
        p_event=_probability(ProbabilityMeasure.PHYSICAL, probability=0.39),
        candidates=(candidate,),
        decision=ShadowDecision(
            action=ShadowAction.MANUAL_CANDIDATE,
            selected_candidate_id=candidate.candidate_id,
            score_threshold=0.0,
            reason_codes=(),
        ),
        quality=ForecastQuality.READY,
        quality_reasons=(),
    )


def test_unavailable_probabilities_and_empty_candidates_are_formal_no_trade() -> None:
    document = _document()

    payload = document.to_dict()
    assert payload["schema_version"] == "strategy_distribution_forecast.v1"
    assert payload["q_event"]["status"] == "unavailable"
    assert payload["q_event"]["probability"] is None
    assert payload["p_event"]["status"] == "unavailable"
    assert payload["p_event"]["probability"] is None
    assert payload["strategy_candidates"] == []
    assert payload["shadow_decision"] == {
        "action": "no_trade",
        "selected_candidate_id": None,
        "score_threshold": 0.0,
        "reason_codes": ["model_inputs_unavailable"],
    }
    assert payload["evidence_status"] == "research_unvalidated"
    assert payload["action_authority"] == "none"
    assert payload["automatic_ordering"] is False


def test_missing_direction_can_use_null_event_only_for_unavailable_q_and_p() -> None:
    q_event = replace(
        _unavailable_probability(ProbabilityMeasure.RISK_NEUTRAL),
        event=None,
        reason_codes=("direction_unavailable",),
    )
    p_event = replace(
        _unavailable_probability(ProbabilityMeasure.PHYSICAL),
        event=None,
        reason_codes=("direction_unavailable",),
    )
    document = _document(q_event=q_event, p_event=p_event)

    assert document.to_dict()["q_event"]["event"] is None
    assert document.to_dict()["p_event"]["event"] is None
    with pytest.raises(ValueError, match="event is missing"):
        replace(_probability(ProbabilityMeasure.RISK_NEUTRAL), event=None)


def test_physical_probability_carries_prior_day_reliability_evidence() -> None:
    estimate = _probability(ProbabilityMeasure.PHYSICAL, probability=0.39)
    payload = estimate.to_dict()

    assert payload["sample_count"] == 40
    assert payload["session_count"] == 8
    assert payload["interval_low"] == pytest.approx(0.30)
    assert payload["interval_high"] == pytest.approx(0.50)
    assert payload["trained_through_date"] == "2026-08-04"

    with pytest.raises(ValueError, match="requires sample_count"):
        replace(estimate, sample_count=None)
    with pytest.raises(ValueError, match="cannot exceed"):
        replace(estimate, session_count=41)
    with pytest.raises(ValueError, match="inside its interval"):
        replace(estimate, interval_high=0.38)
    with pytest.raises(ValueError, match="must precede"):
        replace(_manual_document(), p_event=replace(estimate, trained_through_date=TRADING_DATE))

    q_event = _probability(ProbabilityMeasure.RISK_NEUTRAL)
    with pytest.raises(ValueError, match="cannot claim physical sample evidence"):
        replace(q_event, sample_count=1)


def test_candidate_with_unavailable_net_pnl_remains_a_valid_no_trade_observation() -> None:
    candidate = _candidate(net_pnl=_net_pnl(available=False), score=_score(available=False))
    document = _document(
        q_event=_probability(ProbabilityMeasure.RISK_NEUTRAL, probability=0.28),
        p_event=_probability(ProbabilityMeasure.PHYSICAL, probability=0.39),
        candidates=(candidate,),
        decision=_no_trade_decision(reasons=("net_pnl_model_unavailable",)),
        quality=ForecastQuality.DEGRADED,
        quality_reasons=("net_pnl_model_unavailable",),
    )

    candidate_payload = document.to_dict()["strategy_candidates"][0]
    assert candidate_payload["net_pnl"]["status"] == "unavailable"
    assert candidate_payload["net_pnl"]["p10_net_pnl"] is None
    assert candidate_payload["score"]["status"] == "unavailable"
    assert candidate_payload["execution"]["actual_fill_probability"] is None
    assert document.shadow_decision.action is ShadowAction.NO_TRADE


def test_manual_candidate_preserves_q_p_execution_pnl_and_score_semantics() -> None:
    document = _manual_document()

    payload = document.to_dict()
    candidate = payload["strategy_candidates"][0]
    assert payload["q_event"]["measure"] == "risk_neutral"
    assert payload["q_event"]["probability"] == pytest.approx(0.28)
    assert payload["p_event"]["measure"] == "physical"
    assert payload["p_event"]["probability"] == pytest.approx(0.39)
    assert candidate["execution"]["execution_semantics"] == "displayed_quote_reach_proxy"
    assert candidate["execution"]["actual_fill_probability"] is None
    assert candidate["net_pnl"]["basis"] == "displayed_quote_reach_proxy"
    assert candidate["net_pnl"]["unit"] == "usd_per_one_spread"
    assert candidate["net_pnl"]["p10_net_pnl"] == pytest.approx(-80.0)
    assert candidate["score"]["total"] == pytest.approx(40.0)
    assert payload["shadow_decision"]["action"] == "manual_candidate"
    assert payload["automatic_ordering"] is False


def test_q_and_p_must_use_typed_measures_and_the_exact_same_event() -> None:
    q_event = _probability(ProbabilityMeasure.RISK_NEUTRAL)
    mismatched_p = _probability(
        ProbabilityMeasure.PHYSICAL,
        event=_event(event_id="event:other", lower_level=7_770.0),
    )
    with pytest.raises(ValueError, match="same event"):
        _document(q_event=q_event, p_event=mismatched_p)

    wrong_q = _probability(ProbabilityMeasure.PHYSICAL)
    with pytest.raises(ValueError, match="risk-neutral"):
        _document(q_event=wrong_q, p_event=_probability(ProbabilityMeasure.PHYSICAL))


def test_probability_status_controls_nullable_value_quality_and_reasons() -> None:
    unavailable = _unavailable_probability(ProbabilityMeasure.RISK_NEUTRAL)
    with pytest.raises(ValueError, match="must be null"):
        replace(unavailable, probability=0.3)
    with pytest.raises(ValueError, match="requires reason_codes"):
        replace(unavailable, reason_codes=())

    available = _probability(ProbabilityMeasure.RISK_NEUTRAL)
    with pytest.raises(ValueError, match="value is missing"):
        replace(available, probability=None)
    with pytest.raises(ValueError, match=r"within \[0, 1\]"):
        replace(available, probability=1.01)
    with pytest.raises(ValueError, match="finite"):
        replace(available, probability=float("nan"))
    with pytest.raises(ValueError, match="degraded probability requires"):
        replace(available, quality=EstimateQuality.DEGRADED)


def test_execution_is_explicit_quote_reach_proxy_and_never_actual_fill() -> None:
    execution = _execution()
    assert execution.to_dict()["actual_fill_probability"] is None

    with pytest.raises(ValueError, match="must remain null"):
        replace(execution, actual_fill_probability=0.50)
    with pytest.raises(ValueError, match="within"):
        replace(execution, quote_reach_probability=-0.01)

    unavailable = _execution(available=False)
    with pytest.raises(ValueError, match="must be null"):
        replace(unavailable, quote_reach_probability=0.2)


def test_signed_net_pnl_requires_complete_ordered_quantiles() -> None:
    estimate = _net_pnl()
    assert estimate.p10_net_pnl == pytest.approx(-80.0)

    with pytest.raises(ValueError, match="nondecreasing"):
        replace(estimate, p10_net_pnl=100.0)
    with pytest.raises(ValueError, match="finite"):
        replace(estimate, expected_net_pnl=float("inf"))
    with pytest.raises(ValueError, match="non-negative"):
        replace(estimate, tail_loss_p10=-1.0)

    unavailable = _net_pnl(available=False)
    with pytest.raises(ValueError, match="fields must be null"):
        replace(unavailable, p50_net_pnl=0.0)


def test_candidate_score_must_reconcile_with_expected_net_pnl() -> None:
    score = _score()
    _candidate(score=score)

    with pytest.raises(ValueError, match="does not reconcile"):
        _candidate(score=replace(score, total=41.0))
    with pytest.raises(ValueError, match="non-negative"):
        replace(score, liquidity_risk_penalty=-1.0)
    with pytest.raises(ValueError, match="requires reason_codes"):
        replace(_score(available=False), reason_codes=())


def test_candidate_requires_exact_fixed_ten_point_vertical_and_executable_debit() -> None:
    candidate = _candidate()

    with pytest.raises(ValueError, match="exact 10-point"):
        replace(candidate, short_strike=7_765.0)
    with pytest.raises(ValueError, match="exact contract ids must differ"):
        replace(candidate, short_contract_id=candidate.long_contract_id)
    with pytest.raises(ValueError, match="below vertical width"):
        replace(candidate, execution=replace(candidate.execution, limit_debit_points=10.0))
    with pytest.raises(ValueError, match="exact 10-point"):
        replace(candidate, direction=CandidateDirection.PUT_VERTICAL_10)


def test_reason_codes_are_unique_sorted_at_every_boundary() -> None:
    with pytest.raises(ValueError, match="unique and sorted"):
        _no_trade_decision(reasons=("z_reason", "a_reason"))
    with pytest.raises(ValueError, match="unique and sorted"):
        replace(
            _unavailable_probability(ProbabilityMeasure.PHYSICAL),
            reason_codes=("same", "same"),
        )
    with pytest.raises(ValueError, match="unique and sorted"):
        _document(quality_reasons=("z_reason", "a_reason"))


def test_manual_candidate_requires_ready_complete_selected_candidate_above_threshold() -> None:
    document = _manual_document()

    with pytest.raises(ValueError, match="requires READY"):
        replace(
            document,
            quality=ForecastQuality.DEGRADED,
            quality_reason_codes=("research_only",),
        )
    with pytest.raises(ValueError, match="not in strategy_candidates"):
        replace(
            document,
            shadow_decision=replace(
                document.shadow_decision,
                selected_candidate_id="candidate:missing",
            ),
        )
    with pytest.raises(ValueError, match="must exceed threshold"):
        replace(
            document,
            shadow_decision=replace(document.shadow_decision, score_threshold=40.0),
        )

    unavailable_candidate = _candidate(
        net_pnl=_net_pnl(available=False),
        score=_score(available=False),
    )
    with pytest.raises(ValueError, match="requires available execution"):
        replace(document, strategy_candidates=(unavailable_candidate,))


def test_document_rejects_time_calibration_schema_and_safety_drift() -> None:
    document = _document()

    with pytest.raises(ValueError, match="after available_at"):
        replace(document, observed_through=AVAILABLE + timedelta(seconds=1))
    with pytest.raises(ValueError, match="must be after available_at"):
        replace(document, valid_until=AVAILABLE)
    with pytest.raises(ValueError, match="cannot claim calibration_version"):
        replace(document, calibration_version="calibration:v1")
    with pytest.raises(ValueError, match="schema_version drift"):
        replace(document, schema_version="strategy_distribution_forecast.v2")
    with pytest.raises(ValueError, match="cannot enable automatic ordering"):
        replace(document, automatic_ordering=True)


def test_candidate_set_is_bounded_canonical_and_zero_dte() -> None:
    first = _candidate(candidate_id="candidate:a")
    second = _candidate(candidate_id="candidate:b")
    document = _document(
        q_event=_probability(ProbabilityMeasure.RISK_NEUTRAL),
        p_event=_probability(ProbabilityMeasure.PHYSICAL),
        candidates=(first, second),
        decision=_no_trade_decision(reasons=("negative_net_utility",)),
        quality=ForecastQuality.READY,
        quality_reasons=(),
    )
    assert len(document.strategy_candidates) == 2

    with pytest.raises(ValueError, match="unique and sorted"):
        replace(document, strategy_candidates=(second, first))
    with pytest.raises(ValueError, match="bounded contract"):
        replace(
            document, strategy_candidates=(first, second, _candidate(candidate_id="candidate:c"))
        )
    with pytest.raises(ValueError, match="expiry must match"):
        replace(
            document,
            strategy_candidates=(replace(first, expiry=TRADING_DATE + timedelta(days=1)),),
        )
    with pytest.raises(ValueError, match="different probability event"):
        replace(
            document,
            strategy_candidates=(replace(first, probability_event_id="event:other"),),
        )
