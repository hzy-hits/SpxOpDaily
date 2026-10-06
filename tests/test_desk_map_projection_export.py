from __future__ import annotations

import copy
import json
from datetime import date, datetime, timezone, timedelta
from pathlib import Path
from types import SimpleNamespace

import pytest

from spx_spark.application.order_map.desk_projection_export import (
    _research_advisory_summary,
    build_desk_map_wire,
    persist_desk_map_projection,
    rust_report_owner_enabled,
    refresh_desk_on_data_recovery,
)
from spx_spark.config import StorageSettings
from spx_spark.analytics.options.strategy_payoff import (
    IRON_CONDOR_MANAGEMENT_POLICY, RTH_IRON_CONDOR_MANAGEMENT_POLICY,
)
from spx_spark.domain.research_context import (
    CASH_INDEX_ORDER,
    CloseLocationDistribution,
    CrossIndexFrame,
    ForecastStatus,
    ForecastTarget,
    IndexObservation,
    ObservationStatus,
    PriorRthContextReference,
    ResearchContextDocument,
    ResearchContextStatus,
    ResearchDataQuality,
    SpxRangeForecast,
)


def _storage(root: Path) -> StorageSettings:
    return StorageSettings(
        data_root=str(root),
        latest_state_path=str(root / "latest" / "state.json"),
        raw_file_name="raw.jsonl",
        include_raw_payload=False,
        latest_stale_after_seconds=30,
        slow_index_stale_after_seconds=30,
        slow_index_labels=frozenset(),
    )


def _recovery_payload(now, session, widths=()):
    payload = _payload()
    payload["expiry"] = "20260805"
    payload["as_of"] = now.isoformat()
    payload["option_structure_frame"]["exposure"] = {"oi_quality": "schwab_unverified"}
    policy = IRON_CONDOR_MANAGEMENT_POLICY if session == "gth" else RTH_IRON_CONDOR_MANAGEMENT_POLICY
    payload["strategy_decision"] = {
        "decision_id": "decision:recovery-test", "decision_at": now.isoformat(),
        "decision_type": "NO_TRADE", "action_authority": "none", "automatic_ordering": False,
        "data_quality": {"status": "ready", "market": "ready", "options": "ready", "l1": "ready"},
        "iron_condor_map": {"width_comparisons": [{
            "status": "ready", "wing_width": width, "session_mode": session,
            "strikes": [7400, 7400 + width, 7600, 7600 + width],
            "quote": {"status": "ready", "credit": 2.5, "max_quote_age_seconds": 2},
            "path_distribution": {"status": "estimated_uncalibrated", "n_sessions": 7,
                "net_profit_rate": .7, "tp_before_stop_rate": .6, "stop_loss_rate": .2,
                "management_policy_version": policy.policy_version, "hard_exit_et": policy.hard_exit_et},
        } for width in widths]},
    }
    return payload


@pytest.mark.parametrize("hour,session,slot", [(11, "gth", "2026-08-05:gth:07:00"), (14, "rth", "2026-08-05:10:00")])
def test_recovery_refreshes_current_map_between_slots_and_suppresses_flapping(tmp_path, monkeypatch, hour, session, slot):
    from spx_spark.application.order_map import service

    monkeypatch.setenv("SPX_RUST_REPORT_OWNER", "true")
    storage = _storage(tmp_path)
    now = datetime(2026, 8, 5, hour, 7, tzinfo=timezone.utc)
    before = now - timedelta(minutes=7)
    original = persist_desk_map_projection(_recovery_payload(before, session), [], now=before,
                                          trading_date="2026-08-05", storage=storage)
    current = _recovery_payload(now, session, (10,))
    monkeypatch.setattr(service, "build_order_payload_with_retry", lambda *a, **kw: copy.deepcopy(current))
    result = refresh_desk_on_data_recovery(storage, current, now=now)
    assert result["published"]
    wire = json.loads((tmp_path / "latest/desk_map_projection.json").read_text())
    assert wire["recovery_of"] == original["projection_id"]
    assert wire["source_slot"] == slot
    assert datetime.fromisoformat(wire["available_at"].replace("Z", "+00:00")) >= now
    assert datetime.fromisoformat(wire["valid_until"].replace("Z", "+00:00")) >= now + timedelta(minutes=20)
    assert "oi_verified" not in wire["recovery_seen"]  # OI need not recover with IC.
    assert wire["automatic_ordering"] is False
    assert set(result["recovered"]) == {"iron_condor_10_quotes", "iron_condor_10_probability"}
    assert not refresh_desk_on_data_recovery(storage, current, now=now)["published"]
    # One width temporarily disappears while another arrives; a return to the
    # already announced width cannot generate an endless recovery loop.
    current = _recovery_payload(now, session, (20,))
    assert refresh_desk_on_data_recovery(storage, current, now=now)["published"]
    current = _recovery_payload(now, session, (10,))
    assert not refresh_desk_on_data_recovery(storage, current, now=now)["published"]

    # A :15 audit refresh is not sent to the user; it cannot consume a recovery.
    audit_at = now.replace(minute=15)
    current = _recovery_payload(audit_at, session, ())
    persist_desk_map_projection(current, [], now=audit_at, trading_date="2026-08-05", storage=storage)
    assert not refresh_desk_on_data_recovery(storage, current, now=audit_at)["published"]


@pytest.mark.parametrize("minute", [15, 38])
def test_audit_or_delayed_timer_does_not_consume_newly_available_data(tmp_path, monkeypatch, minute):
    from spx_spark.application.order_map import service

    monkeypatch.setenv("SPX_RUST_REPORT_OWNER", "true")
    storage = _storage(tmp_path)
    now = datetime(2026, 8, 5, 11, 0, tzinfo=timezone.utc)
    persist_desk_map_projection(_recovery_payload(now, "gth"), [], now=now,
                               trading_date="2026-08-05", storage=storage)
    now += timedelta(minutes=minute)
    current = _recovery_payload(now, "gth", (10,))
    audit = persist_desk_map_projection(current, [], now=now,
                                       trading_date="2026-08-05", storage=storage)
    assert "iron_condor_10_quotes" not in audit["recovery_seen"]
    monkeypatch.setattr(service, "build_order_payload_with_retry", lambda *a, **kw: copy.deepcopy(current))
    assert refresh_desk_on_data_recovery(storage, current, now=now)["published"]


def test_recovery_does_not_renew_stale_or_future_decisions_or_old_quotes(tmp_path, monkeypatch):
    from spx_spark.application.order_map.desk_projection_export import available_desk_data

    monkeypatch.setenv("SPX_RUST_REPORT_OWNER", "true")
    now = datetime(2026, 8, 5, 14, 7, tzinfo=timezone.utc)
    storage = _storage(tmp_path)
    persist_desk_map_projection(_recovery_payload(now, "rth"), [], now=now,
                               trading_date="2026-08-05", storage=storage)
    for offset in (-31, 1):
        current = _recovery_payload(now + timedelta(seconds=offset), "rth", (10,))
        assert not refresh_desk_on_data_recovery(storage, current, now=now)["published"]
    current = _recovery_payload(now - timedelta(seconds=10), "rth", (10,))
    current["strategy_decision"]["iron_condor_map"]["width_comparisons"][0]["quote"]["max_quote_age_seconds"] = 10
    assert "iron_condor_10_quotes" not in available_desk_data(current, now=now)


def test_recovery_cannot_overwrite_a_newer_published_map(tmp_path):
    storage = _storage(tmp_path)
    now = datetime(2026, 8, 5, 14, 7, tzinfo=timezone.utc)
    original = persist_desk_map_projection(_recovery_payload(now, "rth"), [], now=now,
                                          trading_date="2026-08-05", storage=storage)
    newer = persist_desk_map_projection(_recovery_payload(now + timedelta(seconds=2), "rth"), [],
        now=now + timedelta(seconds=2), trading_date="2026-08-05", storage=storage)
    actual = persist_desk_map_projection(_recovery_payload(now, "rth", (10,)), [],
        now=now, published_at=now + timedelta(seconds=3), trading_date="2026-08-05", storage=storage,
        recovery_of=original["projection_id"], recovery_seen={"iron_condor_10_quotes"})
    assert actual["projection_id"] == newer["projection_id"]


def _payload() -> dict[str, object]:
    return {
        "expiry": "20260804",
        "underlier": {"price": 7500.0, "source": "index:SPX"},
        "es_last": 7510.0,
        "flip_zone": [7490.0, 7510.0],
        "level_decision": {
            "event_id": "level-event-1",
            "phase": "confirmed",
            "direction": "up",
            "thesis": "breakout",
            "level_kind": "flip_high",
            "level": 7510.0,
            "levels": {
                "put_wall": 7450.0,
                "flip_low": 7490.0,
                "flip_high": 7510.0,
                "call_wall": 7550.0,
            },
        },
        "minute_market_frame": {"quality": "ready"},
        "option_structure_frame": {"quality": "ready", "l1": {"quality": "ready"}},
        "warnings": [],
    }


def _valid_research_context() -> dict[str, object]:
    observed = datetime(2026, 8, 4, 13, 59, tzinfo=timezone.utc)
    available = datetime(2026, 8, 4, 13, 59, 30, tzinfo=timezone.utc)
    target = datetime(2026, 8, 4, 20, 0, tzinfo=timezone.utc)
    frame = CrossIndexFrame(
        frame_id="market:2026-08-04:1359",
        trading_date_et=date(2026, 8, 4),
        observed_through=observed,
        available_at=available,
        observations=tuple(
            IndexObservation(
                instrument=instrument,
                status=ObservationStatus.MISSING,
                quality=ResearchDataQuality.MISSING,
                available_at=available,
                lineage_id=f"missing:{instrument.value}:fixture",
                missing_reason="quote_unavailable",
            )
            for instrument in CASH_INDEX_ORDER
        ),
        feature_set_version="cash-index-rth:test",
        source_skew_limit_seconds=5.0,
    )
    prior = PriorRthContextReference(
        context_id="prior-rth:fixture",
        status=ResearchContextStatus.UNAVAILABLE,
        for_trading_date=date(2026, 8, 4),
        session_date=date(2026, 8, 3),
        source_as_of=observed,
        available_at=available,
        return_bps=tuple((instrument, None) for instrument in CASH_INDEX_ORDER),
        reason_codes=("prior_rth_context_unavailable",),
    )
    forecasts = tuple(
        SpxRangeForecast(
            forecast_id=f"forecast:{forecast_target.value}:fixture",
            target=forecast_target,
            status=ForecastStatus.UNAVAILABLE,
            observed_through=observed,
            available_at=available,
            target_at=target,
            reason_codes=("model_output_unavailable",),
        )
        for forecast_target in ForecastTarget
    )
    close_location = CloseLocationDistribution(
        status=ForecastStatus.UNAVAILABLE,
        observed_through=observed,
        available_at=available,
        target_at=target,
        reason_codes=("model_output_unavailable",),
    )
    return ResearchContextDocument(
        document_id="research-context:123",
        generated_at=datetime(2026, 8, 4, 13, 59, 59, tzinfo=timezone.utc),
        cross_index_frame=frame,
        prior_rth_context=prior,
        regime=None,
        regime_reason_codes=("model_output_unavailable",),
        forecasts=forecasts,
        close_location=close_location,
    ).to_dict()


def _valid_strategy_distribution_forecast() -> dict[str, object]:
    event = {
        "event_id": "level-event-1:terminal-above:300s",
        "kind": "terminal_above",
        "target_at": "2026-08-04T14:05:00+00:00",
        "lower_level": 7500.0,
        "upper_level": None,
    }
    return {
        "schema_version": "strategy_distribution_forecast.v1",
        "document_id": "strategy-distribution:fixture",
        "source_snapshot_id": "snapshot:fixture",
        "trading_date_et": "2026-08-04",
        "session": "rth",
        "observed_through": "2026-08-04T14:00:00+00:00",
        "available_at": "2026-08-04T14:00:00+00:00",
        "valid_until": "2026-08-04T14:01:30+00:00",
        "model_version": "strategy-distribution:v1",
        "feature_set_version": "confirmed-level:v1",
        "calibration_status": "uncalibrated",
        "calibration_version": None,
        "policy_version": "fixed10-shadow:v1",
        "evidence_status": "research_unvalidated",
        "q_event": {
            "measure": "risk_neutral",
            "event": event,
            "status": "available",
            "quality": "degraded",
            "probability": 0.49,
            "method_version": "short-horizon-atm-nd2-proxy:v1",
            "reason_codes": ["risk_neutral_density_not_yet_available"],
            "sample_count": None,
            "session_count": None,
            "interval_low": None,
            "interval_high": None,
            "trained_through_date": None,
        },
        "p_event": {
            "measure": "physical",
            "event": event,
            "status": "available",
            "quality": "degraded",
            "probability": 0.62,
            "method_version": "physical-followthrough-beta-binomial:v1",
            "reason_codes": ["research_unvalidated"],
            "sample_count": 98,
            "session_count": 14,
            "interval_low": 0.52,
            "interval_high": 0.71,
            "trained_through_date": "2026-08-03",
        },
        "strategy_candidates": [],
        "shadow_decision": {
            "action": "no_trade",
            "selected_candidate_id": None,
            "score_threshold": 0.0,
            "reason_codes": ["net_pnl_labels_unavailable"],
        },
        "quality": "degraded",
        "quality_reason_codes": ["net_pnl_labels_unavailable"],
        "action_authority": "none",
        "automatic_ordering": False,
    }


def test_projection_is_versioned_complete_and_never_truncates_changes(tmp_path: Path) -> None:
    storage = _storage(tmp_path)
    long_change = "结构变化" * 3000
    payload = _payload()
    payload["strategy_decision"] = {"schema_version": "strategy_decision.v1"}
    wire = build_desk_map_wire(
        payload,
        [long_change],
        now=datetime(2026, 8, 4, 14, 0, tzinfo=timezone.utc),
        trading_date="2026-08-04",
        storage=storage,
    )

    assert wire["schema_version"] == "desk_map_projection.v1"
    assert wire["session"] == "rth"
    assert wire["action_authority"] == "none"
    assert wire["automatic_ordering"] is False
    assert "strategy_decision" not in wire
    assert wire["research_context"] is None
    assert long_change in wire["message"]["structure"]
    assert set(wire["message"]) == {
        "title",
        "desk_view",
        "location",
        "structure",
        "primary_path",
        "alternative_path",
        "targets",
        "execution",
        "data_quality",
    }
    assert "方向来源" in wire["message"]["primary_path"]
    assert "Gamma职责" in wire["message"]["structure"]
    assert "dealer sign unknown" in wire["message"]["structure"]


def test_projection_keeps_raw_surface_shape_out_of_human_desk_view(tmp_path: Path) -> None:
    payload = _payload()
    payload["strategy_decision"] = {
        "market_facts": {
            "structure": {
                "strike_differential_context": {
                    "feature_version": "strike_differential_context.v1",
                    "status": "ready",
                    "references": [
                        {
                            "center": 7500.0,
                            "labels": ["atm"],
                            "observations": [
                                {
                                    "scale_points": 5.0,
                                    "quality": "degraded_low_snr",
                                    "strike_d2": 0.02,
                                    "strike_d3": 0.001,
                                    "strike_d4": 0.0,
                                    "d2_snr": 0.4,
                                    "d3_snr": 0.4,
                                    "d4_snr": 0.0,
                                }
                            ],
                        }
                    ],
                }
            }
        }
    }

    wire = build_desk_map_wire(
        payload,
        [],
        now=datetime(2026, 8, 4, 14, 0, tzinfo=timezone.utc),
        trading_date="2026-08-04",
        storage=_storage(tmp_path),
    )

    desk_view = wire["message"]["desk_view"]
    assert "曲面形状" not in desk_view
    assert "SNR" not in desk_view
    assert payload["strategy_decision"]["market_facts"]["structure"]["strike_differential_context"]["status"] == "ready"


def test_gamma_or_iv_change_updates_the_projection_fingerprint(tmp_path: Path) -> None:
    first_payload = _payload()
    first_payload["option_structure_frame"] = {
        "quality": "ready",
        "structure": {
            "gamma_state": "positive_gamma_pin",
            "gex_quality": "open_interest_gex",
            "net_gamma_ratio": 0.61,
            "zero_gamma": 7495.0,
            "put_wall": 7450.0,
            "call_wall": 7550.0,
            "flip_zone": [7490.0, 7510.0],
        },
        "volatility": {"atm_iv_change_15m": 0.01},
        "exposure": {"oi_quality": "ibkr_ok"},
        "l1": {"quality": "ready"},
    }
    second_payload = copy.deepcopy(first_payload)
    second_payload["option_structure_frame"]["structure"][  # type: ignore[index]
        "gamma_state"
    ] = "negative_gamma_acceleration"
    second_payload["option_structure_frame"]["volatility"][  # type: ignore[index]
        "atm_iv_change_15m"
    ] = 0.02

    first = build_desk_map_wire(
        first_payload,
        [],
        now=datetime(2026, 8, 4, 14, 0, tzinfo=timezone.utc),
        trading_date="2026-08-04",
        storage=_storage(tmp_path),
    )
    second = build_desk_map_wire(
        second_payload,
        [],
        now=datetime(2026, 8, 4, 14, 0, tzinfo=timezone.utc),
        trading_date="2026-08-04",
        storage=_storage(tmp_path),
    )

    assert first["structure_fingerprint"] != second["structure_fingerprint"]
    assert first["projection_id"] != second["projection_id"]


def test_projection_write_is_atomic_and_private(tmp_path: Path) -> None:
    storage = _storage(tmp_path)
    wire = persist_desk_map_projection(
        _payload(),
        [],
        now=datetime(2026, 8, 4, 14, 0, tzinfo=timezone.utc),
        trading_date="2026-08-04",
        storage=storage,
    )
    path = tmp_path / "latest" / "desk_map_projection.json"
    assert json.loads(path.read_text(encoding="utf-8"))["projection_id"] == wire["projection_id"]
    assert path.stat().st_mode & 0o777 == 0o600


def test_projection_embeds_one_atomic_advisory_research_context(tmp_path: Path) -> None:
    storage = _storage(tmp_path)
    context = _valid_research_context()
    path = tmp_path / "latest" / "experimental_research_signals.json"
    path.parent.mkdir(parents=True)
    path.write_text(json.dumps(context), encoding="utf-8")

    wire = build_desk_map_wire(
        _payload(),
        [],
        now=datetime(2026, 8, 4, 14, 0, tzinfo=timezone.utc),
        published_at=datetime(2026, 8, 4, 14, 2, tzinfo=timezone.utc),
        trading_date="2026-08-04",
        storage=storage,
    )

    assert wire["available_at"] == "2026-08-04T14:02:00Z"
    assert wire["research_context_document_id"] == "research-context:123"
    assert wire["research_context"] == context


def test_projection_shows_bounded_fresh_p_vs_q_evidence_without_promoting_it(
    tmp_path: Path,
) -> None:
    storage = _storage(tmp_path)
    forecast = _valid_strategy_distribution_forecast()
    forecast["observed_through"] = "2026-08-04T13:59:53+00:00"
    path = tmp_path / "latest" / "strategy_distribution_forecast.json"
    path.parent.mkdir(parents=True)
    path.write_text(json.dumps(forecast), encoding="utf-8")

    wire = build_desk_map_wire(
        _payload(),
        [],
        now=datetime(2026, 8, 4, 14, 0, tzinfo=timezone.utc),
        published_at=datetime(2026, 8, 4, 14, 0, 30, tzinfo=timezone.utc),
        trading_date="2026-08-04",
        storage=storage,
    )

    desk_view = wire["message"]["desk_view"]
    # Uncalibrated P/Q must not appear on the operator Desk View.
    assert "P/Q研究" not in desk_view
    assert "P−Q" not in desk_view
    assert wire["action_authority"] == "none"
    assert wire["automatic_ordering"] is False


def test_stale_probability_artifact_is_disclosed_but_does_not_degrade_execution(
    tmp_path: Path,
) -> None:
    storage = _storage(tmp_path)
    forecast = _valid_strategy_distribution_forecast()
    path = tmp_path / "latest" / "strategy_distribution_forecast.json"
    path.parent.mkdir(parents=True)
    path.write_text(json.dumps(forecast), encoding="utf-8")

    wire = build_desk_map_wire(
        _payload(),
        [],
        now=datetime(2026, 8, 4, 14, 2, tzinfo=timezone.utc),
        published_at=datetime(2026, 8, 4, 14, 2, tzinfo=timezone.utc),
        trading_date="2026-08-04",
        storage=storage,
    )

    assert "P/Q研究" not in wire["message"]["desk_view"]
    assert "P/Q实验" not in wire["message"]["data_quality"]
    assert "概率帧已过期" not in wire["quality_reasons"]


def test_unavailable_p_and_q_are_one_formal_no_trade_line_not_a_diagnostic_dump(
    tmp_path: Path,
) -> None:
    storage = _storage(tmp_path)
    forecast = _valid_strategy_distribution_forecast()
    for key in ("q_event", "p_event"):
        estimate = forecast[key]
        assert isinstance(estimate, dict)
        estimate.update(
            {
                "event": None,
                "status": "unavailable",
                "quality": "unavailable",
                "probability": None,
                "method_version": None,
                "reason_codes": ["model_input_unavailable"],
                "interval_low": None,
                "interval_high": None,
                "trained_through_date": None,
            }
        )
    forecast["p_event"]["sample_count"] = 0  # type: ignore[index]
    forecast["p_event"]["session_count"] = 0  # type: ignore[index]
    path = tmp_path / "latest" / "strategy_distribution_forecast.json"
    path.parent.mkdir(parents=True)
    path.write_text(json.dumps(forecast), encoding="utf-8")

    wire = build_desk_map_wire(
        _payload(),
        [],
        now=datetime(2026, 8, 4, 14, 0, tzinfo=timezone.utc),
        published_at=datetime(2026, 8, 4, 14, 0, 30, tzinfo=timezone.utc),
        trading_date="2026-08-04",
        storage=storage,
    )

    assert "P/Q研究" not in wire["message"]["desk_view"]
    assert "model_input_unavailable" not in wire["message"]["desk_view"]


def test_invalid_nested_research_is_omitted_and_disclosed_without_poisoning_desk(
    tmp_path: Path,
) -> None:
    storage = _storage(tmp_path)
    context = _valid_research_context()
    context["forecasts"] = []
    path = tmp_path / "latest" / "experimental_research_signals.json"
    path.parent.mkdir(parents=True)
    path.write_text(json.dumps(context), encoding="utf-8")

    wire = build_desk_map_wire(
        _payload(),
        [],
        now=datetime(2026, 8, 4, 14, 0, tzinfo=timezone.utc),
        published_at=datetime(2026, 8, 4, 14, 2, tzinfo=timezone.utc),
        trading_date="2026-08-04",
        storage=storage,
    )

    assert wire["research_context_document_id"] is None
    assert wire["research_context"] is None
    assert "research_context_contract_invalid" not in wire["quality_reasons"]
    assert "研究层" not in wire["message"]["data_quality"]
    assert "research_context_contract_invalid" not in wire["message"]["data_quality"]


def test_gth_projection_floors_source_slot_to_et_quarter_hour(tmp_path: Path) -> None:
    storage = _storage(tmp_path)

    wire = build_desk_map_wire(
        _payload(),
        [],
        now=datetime(2026, 8, 4, 0, 15, 45, tzinfo=timezone.utc),
        trading_date="2026-08-04",
        storage=storage,
    )

    assert wire["session"] == "gth"
    assert wire["source_slot"] == "2026-08-04:gth:20:15"


def test_gth_projection_does_not_relabel_prior_date_research_as_current(tmp_path: Path) -> None:
    storage = _storage(tmp_path)
    context = {
        "schema_version": "research_context.v2",
        "document_id": "research-context:prior-rth",
        "generated_at": "2026-08-04T19:59:00Z",
        "action_authority": "none",
        "automatic_ordering": False,
        "cross_index_frame": {"trading_date_et": "2026-08-04"},
    }
    path = tmp_path / "latest" / "experimental_research_signals.json"
    path.parent.mkdir(parents=True)
    path.write_text(json.dumps(context), encoding="utf-8")

    wire = build_desk_map_wire(
        _payload(),
        [],
        now=datetime(2026, 8, 5, 0, 30, tzinfo=timezone.utc),
        trading_date="2026-08-05",
        storage=storage,
    )

    assert wire["session"] == "gth"
    assert wire["source_slot"] == "2026-08-05:gth:20:30"
    assert wire["research_context_document_id"] is None
    assert wire["research_context"] is None


def test_gth_projection_does_not_embed_rth_research_context(tmp_path: Path) -> None:
    storage = _storage(tmp_path)
    context = _valid_research_context()
    context["document_id"] = "research-context:gth-same-date"
    context["generated_at"] = "2026-08-05T00:29:59+00:00"
    context["cross_index_frame"]["trading_date_et"] = "2026-08-05"  # type: ignore[index]
    context["prior_rth_context"]["for_trading_date"] = "2026-08-05"  # type: ignore[index]
    context["regime_reason_codes"] = ["filtered_bootstrap_regime_unavailable"]
    for forecast in context["forecasts"]:  # type: ignore[union-attr]
        forecast["target_at"] = "2026-08-05T20:00:00+00:00"
    context["close_location"]["target_at"] = "2026-08-05T20:00:00+00:00"  # type: ignore[index]
    path = tmp_path / "latest" / "experimental_research_signals.json"
    path.parent.mkdir(parents=True)
    path.write_text(json.dumps(context), encoding="utf-8")

    wire = build_desk_map_wire(
        _payload(),
        [],
        now=datetime(2026, 8, 5, 0, 30, tzinfo=timezone.utc),
        published_at=datetime(2026, 8, 5, 0, 30, tzinfo=timezone.utc),
        trading_date="2026-08-05",
        storage=storage,
    )

    assert wire["session"] == "gth"
    assert wire["research_context_document_id"] is None
    assert wire["research_context"] is None
    assert "HMM" not in wire["message"]["desk_view"]
    assert "RTH收盘" not in wire["message"]["desk_view"]
    assert "未校准研究观点" not in wire["message"]["desk_view"]
    assert wire["action_authority"] == "none"
    assert wire["automatic_ordering"] is False


def test_research_summary_keeps_uncalibrated_hmm_visible_without_action_authority() -> None:
    summary = _research_advisory_summary(
        {
            "regime": {
                "posterior": [
                    {"state_id": "state_00", "probability": 0.05},
                    {"state_id": "state_01", "probability": 0.90},
                    {"state_id": "state_02", "probability": 0.05},
                ]
            },
            "forecasts": [
                {
                    "target": "rth_close",
                    "status": "available",
                    "quantiles": {"p10": 7700.123, "p50": 7750.456, "p90": 7800.789},
                }
            ],
            "close_location": {
                "status": "available",
                "probabilities": {
                    "lower_third": 0.05,
                    "middle_third": 0.90,
                    "upper_third": 0.05,
                },
                "reason_codes": ["latent_state_location_mapping_unvalidated"],
            },
            "prior_rth_context": {"status": "partial"},
            "regime_reason_codes": [],
        },
        session="gth",
    )

    assert summary is not None
    assert "基线=区间/中位收盘" in summary
    assert "可靠性=低" in summary
    assert "HMM映射后的主导收盘桶模型权重 90%" in summary
    assert "HMM state_01" not in summary
    assert "潜状态到收盘位置的映射未验证" in summary
    assert "RTH收盘启发区间 7700.1/7750.5/7800.8" in summary
    assert "不改结论" in summary


def test_gth_research_summary_does_not_claim_an_unavailable_prior_rth_input() -> None:
    summary = _research_advisory_summary(
        {
            "regime": {
                "posterior": [
                    {"state_id": "state_00", "probability": 0.05},
                    {"state_id": "state_01", "probability": 0.90},
                    {"state_id": "state_02", "probability": 0.05},
                ]
            },
            "regime_reason_codes": ["prior_rth_component_unavailable"],
            "prior_rth_context": {"status": "partial"},
            "forecasts": [],
            "close_location": {"status": "unavailable", "probabilities": {}},
        },
        session="gth",
    )

    assert summary is not None
    assert "夜盘ES为主（前日RTH不可用）" in summary
    assert "主要限制=前日RTH上下文不可用" in summary
    assert "前日RTH+夜盘ES" not in summary


def test_rust_owner_persists_projection_and_never_enqueues_python_outbox(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    import spx_spark.application.order_map.service as service

    captured: dict[str, object] = {}
    monkeypatch.setenv("SPX_RUST_REPORT_OWNER", "true")
    monkeypatch.setattr(
        service.StorageSettings,
        "from_env",
        classmethod(lambda cls: _storage(tmp_path)),
    )
    monkeypatch.setattr(service, "load_order_map_state", lambda _path: {})
    monkeypatch.setattr(
        service, "build_order_payload_with_retry", lambda *_args, **_kwargs: _payload()
    )
    monkeypatch.setattr(service, "_payload_is_thin", lambda _payload: False)
    monkeypatch.setattr(service, "_status_fingerprint", lambda _payload: {"stage": "confirmed"})
    monkeypatch.setattr(service, "_status_material_changes", lambda *_args: ["old-writer-change"])
    monkeypatch.setattr(service, "render_status_template", lambda *_args: "template")

    def persist(*_args: object, **kwargs: object) -> dict[str, str]:
        captured.update(kwargs)
        captured["changes"] = _args[1]
        return {
            "projection_id": "desk-map:test",
            "source_slot": "2026-08-04:10:00",
        }

    monkeypatch.setattr(service, "persist_desk_map_projection", persist)
    monkeypatch.setattr(service, "persist_order_map_pricing_audit", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(
        service,
        "enqueue_order_map_status",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(
            AssertionError("Python outbox must not be called after Rust owner cutover")
        ),
    )

    result = service.run_status(
        SimpleNamespace(force=True, dry_run=False),
        now=datetime(2026, 8, 4, 14, 0, tzinfo=timezone.utc),
        state_path=str(tmp_path / "order-map-state.json"),
        trading_date="2026-08-04",
    )

    assert result == 0
    assert captured["changes"] == []
    assert isinstance(captured["published_at"], datetime)


def test_rust_owner_preserves_independent_position_safety_lane(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    import spx_spark.application.order_map.service as service

    delivered: dict[str, object] = {}
    now = datetime(2026, 8, 4, 20, 30, tzinfo=timezone.utc)
    monkeypatch.setenv("SPX_RUST_REPORT_OWNER", "true")
    monkeypatch.setattr(service, "within_status_window", lambda _now: True)
    monkeypatch.setattr(
        service.StorageSettings,
        "from_env",
        classmethod(lambda cls: _storage(tmp_path)),
    )
    monkeypatch.setattr(service, "load_order_map_state", lambda _path: {})
    monkeypatch.setattr(
        service, "build_order_payload_with_retry", lambda *_args, **_kwargs: _payload()
    )
    monkeypatch.setattr(service, "_payload_is_thin", lambda _payload: False)
    monkeypatch.setattr(service, "_status_fingerprint", lambda _payload: {"stage": "confirmed"})
    monkeypatch.setattr(service, "_status_material_changes", lambda *_args: [])
    monkeypatch.setattr(service, "render_status_template", lambda *_args: "template")
    monkeypatch.setattr(
        service,
        "persist_desk_map_projection",
        lambda *_args, **_kwargs: {
            "projection_id": "desk-map:test",
            "source_slot": "2026-08-04:gth:20:30",
        },
    )
    monkeypatch.setattr(service, "_has_open_position_risk", lambda _settings: True)
    monkeypatch.setattr(
        service, "_status_delivery_reason", lambda *_args, **_kwargs: "open_position_risk"
    )
    monkeypatch.setattr(
        service,
        "order_map_status_semantic",
        lambda **_kwargs: SimpleNamespace(
            event_id="position-safety:test",
            lane="position_safety",
            occurred_at=now,
            expires_at=None,
            slot_key="2026-08-04:gth:20:30",
        ),
    )
    monkeypatch.setattr(service, "render_operator_status_brief", lambda *_args: "safety")
    monkeypatch.setattr(service, "render_feishu_delivery_text", lambda *_args: "safety")
    monkeypatch.setattr(service.NotificationSettings, "from_env", classmethod(lambda cls: object()))
    monkeypatch.setattr(service, "notification_event_exists", lambda *_args: False)

    def enqueue(*_args: object, **kwargs: object) -> dict[str, object]:
        delivered.update(kwargs)
        return {"accepted": True}

    monkeypatch.setattr(service, "enqueue_order_map_status", enqueue)
    monkeypatch.setattr(service, "persist_zero_dte_greeks_reference", lambda *_args: None)
    monkeypatch.setattr(service, "mark_sent", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(service, "record_push", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(service, "persist_order_map_pricing_audit", lambda *_args, **_kwargs: None)

    result = service.run_status(
        SimpleNamespace(force=False, dry_run=False),
        now=now,
        state_path=str(tmp_path / "order-map-state.json"),
        trading_date="2026-08-04",
    )

    assert result == 0
    assert delivered["delivery_reason"] == "open_position_risk"


def test_projection_closes_unknown_direction_and_thesis_values(tmp_path: Path) -> None:
    payload = _payload()
    payload["level_decision"] = {
        **payload["level_decision"],  # type: ignore[dict-item]
        "direction": "sideways",
        "thesis": "guess",
    }
    payload["regime_decision"] = {"direction": "bullish"}
    wire = build_desk_map_wire(
        payload,
        [],
        now=datetime(2026, 8, 4, 14, 0, tzinfo=timezone.utc),
        trading_date="2026-08-04",
        storage=_storage(tmp_path),
    )

    assert wire["direction"] == "none"
    assert wire["thesis"] == "none"


def test_trade_ready_without_closed_semantics_is_degraded_not_ready(tmp_path: Path) -> None:
    payload = _payload()
    payload["trade_intent"] = {
        "status": "trade_ready",
        "event_id": "level-event-1",
        "intent_id": "intent:closed-semantics",
        "contract_id": "option:SPX:SPXW:20260804:7510:C",
    }
    payload["plan_candidates"] = [
        {
            "intent_id": "intent:closed-semantics",
            "contract_id": "option:SPX:SPXW:20260804:7510:C",
        }
    ]
    payload["level_decision"] = {
        **payload["level_decision"],  # type: ignore[dict-item]
        "direction": "unknown",
        "thesis": "unknown",
    }
    wire = build_desk_map_wire(
        payload,
        [],
        now=datetime(2026, 8, 4, 14, 0, tzinfo=timezone.utc),
        trading_date="2026-08-04",
        storage=_storage(tmp_path),
    )

    assert wire["stage"] == "paused"
    assert wire["quality"] == "degraded"
    assert wire["quality_reasons"] == [
        "ready_direction_missing",
        "ready_thesis_missing",
    ]


def test_rust_report_owner_switch_rejects_typos(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("SPX_RUST_REPORT_OWNER", "true")
    assert rust_report_owner_enabled() is True
    monkeypatch.setenv("SPX_RUST_REPORT_OWNER", "flase")
    with pytest.raises(ValueError, match="must be a boolean"):
        rust_report_owner_enabled()


@pytest.mark.parametrize("mode", ["rth", "gth"])
@pytest.mark.parametrize("wide_available", [True, False])
def test_both_condor_widths_survive_desk_export_with_separate_quotes_and_evidence(tmp_path, mode, wide_available):
    from spx_spark.analytics.options.strategy_payoff import management_policy_for_candidate
    payload = _payload()
    payload["session_mode"] = mode
    payload["level_decision"]["session_mode"] = mode
    policy = management_policy_for_candidate({"strategy_type": "IRON_CONDOR", "session_mode": mode})
    comparisons = []
    for width, credit, rate in ((10, 2.4, .7), (20, 4.8, .8)):
        comparisons.append({
            "status": "ready", "strategy_type": "IRON_CONDOR", "session_mode": mode,
            "expiry": "20260804", "provider": "ibkr" if mode == "gth" else "schwab",
            "wing_width": width, "short_abs_delta": .2,
            "strikes": [7460 - width, 7460, 7540, 7540 + width], "quote": {"credit": credit},
            "path_distribution": {"status": "insufficient_sample", "n_sessions": 11,
                "net_profit_rate": rate, "tp_before_stop_rate": rate, "stop_loss_rate": 1 - rate,
                "hard_exit_et": policy.hard_exit_et, "management_policy_version": policy.policy_version,
                "probability_as_of": "2026-08-04T14:00:00+00:00", "entry_credit_basis": credit},
        })
    if not wide_available:
        comparisons[1] = {"wing_width": 20, "status": "unavailable", "reason": "iron_condor_delta_quotes_unavailable"}
    payload["strategy_decision"] = {"action_authority": "manual", "decision_type": "IRON_CONDOR",
        "candidate": dict(comparisons[0]), "market_facts": {"session": {"mode": mode}},
        "iron_condor_map": {"width_comparisons": comparisons}}
    wire = build_desk_map_wire(payload, [], now=datetime(2026, 8, 4, 14, 0, tzinfo=timezone.utc),
        trading_date="2026-08-04", storage=_storage(tmp_path))
    text = wire["message"]["desk_view"]
    ten, twenty = text.split("20Δ/20点翼", 1)
    assert "20Δ/10点翼" in ten and "人工候选" in ten
    assert "买7450P/卖7460P/卖7540C/买7550C" in ten
    assert "贷记2.40" in ten and "盈利 70%" in ten
    assert "结构扫描" in twenty and "人工候选" not in twenty
    if wide_available:
        assert "买7440P/卖7460P/卖7540C/买7560C" in twenty
        assert "贷记4.80" in twenty and "盈利 80%" in twenty
    else:
        assert "概率暂不可估" in twenty and "历史模拟" not in twenty
        assert "买7440P" not in twenty and "贷记4.80" not in twenty
        assert "盈利 70%" not in twenty
    audit = json.loads((Path(__file__).resolve().parents[1] / "docs/condor-evidence-audit-2026-09-23.json").read_text())
    for part, reference in zip((ten, twenty), audit["cash_recheck"]["summaries"][:2]):
        assert f'{reference["win_rate"]:.1%}' in part
        assert f'{reference["mean_usd"]:+.2f}' in part
        assert f'{reference["total_usd"]:+,.0f}' in part
        assert f'{reference["complete"]}/37' in part
    assert "RTH固定10:00" in text and "2026-07-06至09-04" in text
    assert len(text.encode("utf-8")) <= 4096
    # Both comparisons also remain visible when there is no authorized candidate.
    payload["strategy_decision"].update(candidate=None, action_authority="none", decision_type="NO_TRADE")
    wire = build_desk_map_wire(payload, [], now=datetime(2026, 8, 4, 14, 0, tzinfo=timezone.utc),
        trading_date="2026-08-04", storage=_storage(tmp_path))
    assert "20Δ/10点翼" in wire["message"]["desk_view"] and "20Δ/20点翼" in wire["message"]["desk_view"]
