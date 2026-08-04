from __future__ import annotations

import json
from datetime import date, datetime, timezone
from pathlib import Path
from types import SimpleNamespace

import pytest

from spx_spark.application.order_map.desk_projection_export import (
    build_desk_map_wire,
    persist_desk_map_projection,
    rust_report_owner_enabled,
)
from spx_spark.config import StorageSettings
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


def test_projection_is_versioned_complete_and_never_truncates_changes(tmp_path: Path) -> None:
    storage = _storage(tmp_path)
    long_change = "结构变化" * 3000
    wire = build_desk_map_wire(
        _payload(),
        [long_change],
        now=datetime(2026, 8, 4, 14, 0, tzinfo=timezone.utc),
        trading_date="2026-08-04",
        storage=storage,
    )

    assert wire["schema_version"] == "desk_map_projection.v1"
    assert wire["session"] == "rth"
    assert wire["action_authority"] == "none"
    assert wire["automatic_ordering"] is False
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
    assert "research_context_contract_invalid" in wire["quality_reasons"]
    assert "research_context_contract_invalid" in wire["message"]["data_quality"]


def test_gth_projection_does_not_relabel_prior_rth_research_as_current(tmp_path: Path) -> None:
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
    payload["trade_intent"] = {"status": "trade_ready"}
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
