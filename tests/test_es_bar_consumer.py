from __future__ import annotations

import inspect
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

from spx_spark.application.market_features import service
from spx_spark.application.market_features.es_bar_consumer import (
    evaluate_es_bar_consumer_readiness,
    load_consumable_es_bars,
)
from spx_spark.application.market_features.es_bar_state import SCHEMA_VERSION
from spx_spark.application.runtime import es_bar_sampler
from spx_spark.config import StorageSettings
from spx_spark.state_io import atomic_write_json_secure


UTC = timezone.utc
NOW = datetime(2026, 7, 30, 14, 0, tzinfo=UTC)


def _storage(tmp_path: Path) -> StorageSettings:
    return StorageSettings(
        data_root=str(tmp_path),
        latest_state_path=str(tmp_path / "latest" / "state.json"),
        raw_file_name="quotes.jsonl",
        include_raw_payload=False,
        latest_stale_after_seconds=90.0,
        slow_index_stale_after_seconds=180.0,
        slow_index_labels=frozenset(),
    )


def _state(*, source_at: datetime, writer: str) -> dict[str, object]:
    bar_start = source_at.replace(second=0, microsecond=0) - timedelta(minutes=10)
    return {
        "schema_version": SCHEMA_VERSION,
        "interval_seconds": 300,
        "updated_at": source_at.isoformat(),
        "last_source_at": source_at.isoformat(),
        "last_provider": "ibkr",
        "writer_instance_id": writer,
        "contract_identity": "ES:202609",
        "current_bar": {},
        "closed_bars": [
            {
                "bar_start": bar_start.isoformat(),
                "bar_end": (bar_start + timedelta(minutes=5)).isoformat(),
                "interval_seconds": 300,
                "open": 7400.0,
                "high": 7402.0,
                "low": 7399.0,
                "close": 7401.0,
                "quality": "ok",
                "gap_before": False,
                "segment": "rth",
                "trading_date_et": "2026-07-30",
                "contract_identity": "ES:202609",
                "contract_identity_ambiguous": False,
            }
        ],
        "rth_ma_history": [],
        "diagnostics": {
            "canonical_writer": "es_bar_sampler",
            "writer_instance_id": writer,
        },
    }


def _lease(*, source_at: datetime, writer: str) -> dict[str, object]:
    return {
        "schema_version": es_bar_sampler.LEASE_SCHEMA_VERSION,
        "task": es_bar_sampler.TASK_NAME,
        "event": "cycle_finished",
        "ok": True,
        "liveness_ok": True,
        "data_healthy": True,
        "sla_ok": True,
        "writer_has_accepted": True,
        "writer_instance_id": writer,
        "finished_at": source_at.isoformat(),
        "last_accepted_at": source_at.isoformat(),
        "last_accepted_source_at": source_at.isoformat(),
    }


def _publish(
    storage: StorageSettings,
    *,
    lease: dict[str, object],
    state: dict[str, object],
) -> None:
    atomic_write_json_secure(es_bar_sampler.lease_path(storage), lease)
    atomic_write_json_secure(es_bar_sampler.canonical_state_path(storage), state)


def test_fresh_matching_lease_exposes_canonical_bars(tmp_path: Path) -> None:
    storage = _storage(tmp_path)
    source_at = NOW - timedelta(seconds=1)
    _publish(
        storage,
        lease=_lease(source_at=source_at, writer="writer-current"),
        state=_state(source_at=source_at, writer="writer-current"),
    )

    bars, readiness = load_consumable_es_bars(storage, now=NOW)

    assert readiness["ready"] is True
    assert readiness["status"] == "ready"
    assert readiness["reasons"] == []
    assert len(bars) == 1
    assert bars[0]["close"] == 7401.0


def test_stale_lease_and_last_accepted_source_hide_old_bars(tmp_path: Path) -> None:
    storage = _storage(tmp_path)
    source_at = NOW - timedelta(seconds=16)
    _publish(
        storage,
        lease=_lease(source_at=source_at, writer="writer-old"),
        state=_state(source_at=source_at, writer="writer-old"),
    )

    bars, readiness = load_consumable_es_bars(
        storage,
        now=NOW,
        max_age_seconds=15.0,
    )

    assert bars == []
    assert readiness["ready"] is False
    assert readiness["status"] == "stale"
    assert {
        "lease_stale",
        "last_accept_stale",
        "accepted_source_stale",
        "state_source_stale",
    }.issubset(readiness["reasons"])


def test_writer_instance_mismatch_hides_other_writer_bars(tmp_path: Path) -> None:
    storage = _storage(tmp_path)
    source_at = NOW - timedelta(seconds=1)
    _publish(
        storage,
        lease=_lease(source_at=source_at, writer="writer-new"),
        state=_state(source_at=source_at, writer="writer-old"),
    )

    bars, readiness = load_consumable_es_bars(storage, now=NOW)

    assert bars == []
    assert readiness["ready"] is False
    assert readiness["status"] == "unavailable"
    assert "writer_instance_mismatch" in readiness["reasons"]


def test_missing_lease_is_unavailable_not_mislabeled_stale() -> None:
    readiness = evaluate_es_bar_consumer_readiness(
        lease={},
        state={},
        now=NOW,
    )

    assert readiness["ready"] is False
    assert readiness["status"] == "unavailable"
    assert "lease_finished_at_missing" in readiness["reasons"]
    assert "accepted_source_at_missing" in readiness["reasons"]


def test_accepted_source_marker_mismatch_hides_bars(tmp_path: Path) -> None:
    storage = _storage(tmp_path)
    accepted_at = NOW - timedelta(seconds=1)
    state_source_at = NOW - timedelta(seconds=2)
    _publish(
        storage,
        lease=_lease(source_at=accepted_at, writer="writer-current"),
        state=_state(source_at=state_source_at, writer="writer-current"),
    )

    bars, readiness = load_consumable_es_bars(storage, now=NOW)

    assert bars == []
    assert readiness["ready"] is False
    assert "accepted_source_marker_mismatch" in readiness["reasons"]


@pytest.mark.parametrize(
    ("field", "reason"),
    (
        ("data_healthy", "lease_data_not_healthy"),
        ("sla_ok", "lease_cycle_sla_failed"),
    ),
)
def test_lease_health_flags_fail_closed(
    tmp_path: Path,
    field: str,
    reason: str,
) -> None:
    source_at = NOW - timedelta(seconds=1)
    lease = _lease(source_at=source_at, writer="writer-current")
    lease[field] = False

    readiness = evaluate_es_bar_consumer_readiness(
        lease=lease,
        state=_state(source_at=source_at, writer="writer-current"),
        now=NOW,
    )

    assert readiness["ready"] is False
    assert reason in readiness["reasons"]


def test_unhealthy_sampler_only_fences_rth_state_inputs() -> None:
    original = {
        "schema_version": "market_state_5m_inputs.v2",
        "status": "ready",
        "available_count": 8,
        "required_count": 8,
        "missing": [],
        "values": {
            "price_vs_vwap": "ABOVE_CONFIRMED",
            "vwap_slope": 0.5,
            "opening_range_state": "ABOVE_ORH_CONFIRMED",
            "market_structure": "HH_HL",
            "efficiency_ratio": 0.8,
            "vwap_cross_count": 0,
            "same_time_range_ratio": 1.2,
            "breadth_above_vwap": 0.7,
        },
        "diagnostics": {"bar_count_all": 20},
    }
    readiness = {
        "ready": False,
        "status": "stale",
        "reasons": ["accepted_source_stale"],
    }

    fenced = service._fence_rth_market_state_inputs(
        original,
        es_bar_consumer=readiness,
    )

    assert fenced["status"] == "unavailable"
    assert fenced["available_count"] == 0
    assert all(value is None for value in fenced["values"].values())
    assert fenced["diagnostics"]["es_bar_consumer"] == readiness

    market_state = service._fence_rth_market_state(
        {
            "state": "TREND_UP",
            "market_state": "TREND_UP",
            "status": "ready",
            "classification_tier": "complete",
        },
        es_bar_consumer=readiness,
    )
    assert market_state["state"] == "UNCERTAIN"
    assert market_state["status"] == "unavailable"
    assert market_state["classification_tier"] == "unavailable"
    assert market_state["reasons"] == [
        "es_bar_consumer_stale",
        "es_bar_consumer:accepted_source_stale",
    ]

    source = inspect.getsource(service.run)
    assert source.index("load_consumable_es_bars") < source.index("build_minute_market_frame")
    assert source.index("build_minute_market_frame") < source.index("evaluate_trade_intent")
    assert source.index("evaluate_trade_intent") < source.index("_fence_rth_trade_intent_authority")
    assert source.index("_fence_rth_trade_intent_authority") < source.index(
        "advance_trade_candidate"
    )
    assert source.count("_fence_rth_trade_intent_authority(") == 1
    assert "process_gth_manual_candidate(" not in source
    assert source.index("_fence_rth_trade_intent_authority") < source.index(
        "process_gth_level_manual_candidate"
    )


@pytest.mark.parametrize("status", ("trade_ready", "shadow_ready"))
def test_unhealthy_sampler_removes_rth_ready_authority(status: str) -> None:
    intent = {
        "status": status,
        "strategy_lane": "long_0dte_rth_upside_breakout_pilot",
        "execution_eligible": status == "trade_ready",
        "quote_observation_eligible": status == "shadow_ready",
        "block_reasons": ["existing_reason"],
    }
    readiness = {
        "ready": False,
        "status": "stale",
        "reasons": ["accepted_source_stale"],
    }

    fenced = service._fence_rth_trade_intent_authority(
        intent,
        es_bar_consumer=readiness,
    )

    assert fenced["status"] == "blocked"
    assert fenced["execution_eligible"] is False
    assert fenced["quote_observation_eligible"] is False
    assert fenced["rth_trade_ready_authority"] is False
    assert fenced["es_bar_consumer"] == readiness
    assert fenced["block_reasons"] == [
        "existing_reason",
        "rth_es_bar_consumer_not_ready",
        "es_bar_consumer_stale",
        "es_bar_consumer:accepted_source_stale",
    ]


def test_ready_sampler_preserves_rth_ready_intent() -> None:
    intent = {
        "status": "trade_ready",
        "execution_eligible": True,
        "block_reasons": [],
    }

    fenced = service._fence_rth_trade_intent_authority(
        intent,
        es_bar_consumer={"ready": True, "status": "ready", "reasons": []},
    )

    assert fenced == intent
