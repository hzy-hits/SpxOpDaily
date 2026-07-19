from __future__ import annotations

import json
from dataclasses import replace
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

from spx_spark.application.order_map import level_decision_shadow as shadow_service
from spx_spark.application.order_map.level_decision_machine import LevelObservation
from spx_spark.application.order_map.level_decision_shadow import (
    _structure_session_age,
    load_level_decision_shadow,
    run_level_decision_shadow,
)
from spx_spark.settings.level_decision import LevelDecisionPolicy


NOW = datetime(2026, 7, 13, 14, 30, tzinfo=timezone.utc)


def test_frozen_structure_ttl_counts_trading_sessions() -> None:
    structure = {
        "session_date": "2026-07-10",
        "observed_at": "2026-07-10T19:00:00+00:00",
    }

    assert _structure_session_age(structure, now=NOW) == 1
    assert (
        _structure_session_age(
            structure,
            now=datetime(2026, 7, 14, 14, 30, tzinfo=timezone.utc),
        )
        == 2
    )


def test_shadow_persists_mutually_exclusive_state_and_transition_audit(
    tmp_path, monkeypatch
) -> None:
    storage = SimpleNamespace(data_root=str(tmp_path))
    current = {
        "observation": LevelObservation(
            at=NOW,
            spot=95.0,
            es=5000.0,
            levels={"put_wall": 100.0, "call_wall": 120.0},
            quality_ok=True,
            session_date="2026-07-13",
        )
    }
    monkeypatch.setattr(
        "spx_spark.application.order_map.level_decision_shadow._observation",
        lambda *_args, **_kwargs: current["observation"],
    )

    result = run_level_decision_shadow(storage, SimpleNamespace(), now=NOW)
    assert result["phase"] == "approaching"
    assert result["level_kind"] == "put_wall"
    assert result["actionable"] is False

    current["observation"] = LevelObservation(
        at=NOW + timedelta(seconds=5),
        spot=99.0,
        es=5000.0,
        levels={"put_wall": 100.0, "call_wall": 120.0},
        quality_ok=True,
        session_date="2026-07-13",
    )
    result = run_level_decision_shadow(storage, SimpleNamespace(), now=NOW + timedelta(seconds=5))
    assert result["phase"] == "testing"
    persisted = load_level_decision_shadow(storage)
    assert persisted["phase"] == "testing"

    audit = tmp_path / "features" / "level_decision_audit" / "date=2026-07-13" / "transitions.jsonl"
    rows = [json.loads(line) for line in audit.read_text().splitlines()]
    assert [row["current_phase"] for row in rows] == ["approaching", "testing"]
    assert len({row["record_key"] for row in rows}) == 2


def test_outside_rth_advances_when_es_globex_observation_is_usable(tmp_path, monkeypatch) -> None:
    storage = SimpleNamespace(data_root=str(tmp_path))
    at = datetime(2026, 7, 13, 8, 0, tzinfo=timezone.utc)
    monkeypatch.setattr(
        "spx_spark.application.order_map.level_decision_shadow._observation",
        lambda *_args, **_kwargs: LevelObservation(
            at=at,
            spot=99.0,
            es=145.0,
            levels={"put_wall": 100.0},
            quality_ok=True,
            session_date="2026-07-13",
            spot_source="es_basis_adjusted:46.0",
            level_source="frozen_oi_gex",
        ),
    )
    result = run_level_decision_shadow(
        storage,
        SimpleNamespace(),
        now=at,
    )
    assert result["status"] == "updated"
    assert result["phase"] == "testing"
    assert result["spot_source"] == "es_basis_adjusted:46.0"
    assert (tmp_path / "latest" / "level_decision_shadow_state.json").exists()


def test_transition_is_audited_without_human_delivery(tmp_path, monkeypatch) -> None:
    storage = SimpleNamespace(data_root=str(tmp_path))
    monkeypatch.setattr(
        "spx_spark.application.order_map.level_decision_shadow._observation",
        lambda *_args, **_kwargs: LevelObservation(
            at=NOW,
            spot=99.0,
            es=145.0,
            levels={"put_wall": 100.0},
            quality_ok=True,
            session_date="2026-07-13",
            spot_source="es_basis_adjusted:46.0000",
            level_source="frozen_last_rth_oi_gex",
        ),
    )

    result = run_level_decision_shadow(storage, SimpleNamespace(), now=NOW)

    assert result["delivery"]["delivered"] is False
    assert result["delivery"]["delivery_gate"] == "trade_intent_required"
    assert result["spot_source"] == "es_basis_adjusted:46.0000"


def test_confirmed_shadow_emits_deduplicated_30_second_outcome(tmp_path, monkeypatch) -> None:
    storage = SimpleNamespace(data_root=str(tmp_path))
    current: dict[str, LevelObservation] = {}
    monkeypatch.setattr(
        "spx_spark.application.order_map.level_decision_shadow._observation",
        lambda *_args, **_kwargs: current["value"],
    )
    path = (
        (0, 95.0, 5000.0),
        (5, 99.0, 5000.0),
        (10, 96.0, 4999.0),
        (31, 95.0, 4997.0),
        (40, 99.0, 4998.0),
        (45, 95.0, 4996.0),
        (56, 94.0, 4994.0),
        (86, 91.0, 4990.0),
    )
    result = None
    for seconds, spot, es in path:
        at = NOW + timedelta(seconds=seconds)
        current["value"] = LevelObservation(
            at=at,
            spot=spot,
            es=es,
            levels={"put_wall": 100.0, "call_wall": 120.0},
            quality_ok=True,
            session_date="2026-07-13",
        )
        result = run_level_decision_shadow(storage, SimpleNamespace(), now=at)
    assert result is not None
    assert result["completed_outcomes"] == 1

    outcomes = (
        tmp_path / "features" / "level_decision_outcomes" / "date=2026-07-13" / "outcomes.jsonl"
    )
    rows = [json.loads(line) for line in outcomes.read_text().splitlines()]
    assert len(rows) == 1
    assert rows[0]["horizon_seconds"] == 30
    assert rows[0]["attribution"] == "follow_through"


def test_operator_override_confirms_level_but_still_requires_trade_intent(
    tmp_path, monkeypatch
) -> None:
    storage = SimpleNamespace(data_root=str(tmp_path))
    current: dict[str, LevelObservation] = {}
    monkeypatch.setattr(
        "spx_spark.application.order_map.level_decision_shadow._observation",
        lambda *_args, **_kwargs: current["value"],
    )

    policy = replace(
        LevelDecisionPolicy(),
        formal_signal_enabled=True,
        notify_transitions=False,
    )
    result = None
    for seconds, spot, es in (
        (0, 95.0, 5000.0),
        (5, 99.0, 5000.0),
        (10, 96.0, 4999.0),
        (31, 95.0, 4997.0),
        (40, 99.0, 4998.0),
        (45, 95.0, 4996.0),
        (56, 94.0, 4994.0),
    ):
        at = NOW + timedelta(seconds=seconds)
        current["value"] = LevelObservation(
            at=at,
            spot=spot,
            es=es,
            levels={"put_wall": 100.0, "call_wall": 120.0},
            quality_ok=True,
            session_date="2026-07-13",
        )
        result = run_level_decision_shadow(
            storage,
            SimpleNamespace(),
            now=at,
            policy=policy,
        )

    assert result is not None
    assert result["phase"] == "confirmed"
    assert result["formal_signal"] is True
    assert result["level_path_confirmed"] is True
    assert result["actionable"] is False
    assert result["delivery"]["delivered"] is False
    assert result["delivery"]["delivery_gate"] == "trade_intent_required"


def test_confirmed_level_path_sends_one_non_executable_warning(tmp_path, monkeypatch) -> None:
    storage = SimpleNamespace(data_root=str(tmp_path))
    current: dict[str, LevelObservation] = {}
    enqueued: list[tuple[object, str]] = []
    monkeypatch.setattr(
        shadow_service,
        "_observation",
        lambda *_args, **_kwargs: current["value"],
    )
    monkeypatch.setattr(
        shadow_service,
        "NotificationSettings",
        SimpleNamespace(
            from_env=lambda: SimpleNamespace(
                enabled=True,
                feishu_enabled=True,
                bark_enabled=False,
                bark_friend_enabled=False,
            )
        ),
    )

    def fake_enqueue(_settings, envelope, **kwargs):
        enqueued.append((envelope, kwargs["text"]))
        return SimpleNamespace(
            targets=("feishu",),
            accepted=True,
            inserted=True,
            duplicate=False,
            queued_for_recovery=True,
            delivered=False,
        )

    monkeypatch.setattr(shadow_service, "enqueue_notification", fake_enqueue)
    policy = replace(
        LevelDecisionPolicy(),
        formal_signal_enabled=True,
        notify_transitions=True,
    )
    result = None
    for seconds, spot, es in (
        (0, 95.0, 5000.0),
        (5, 99.0, 5000.0),
        (10, 96.0, 4999.0),
        (31, 95.0, 4997.0),
        (40, 99.0, 4998.0),
        (45, 95.0, 4996.0),
        (56, 94.0, 4994.0),
    ):
        at = NOW + timedelta(seconds=seconds)
        current["value"] = LevelObservation(
            at=at,
            spot=spot,
            es=es,
            levels={"put_wall": 100.0, "call_wall": 120.0},
            quality_ok=True,
            session_date="2026-07-13",
            spx_spot=spot,
        )
        result = run_level_decision_shadow(
            storage,
            None,
            now=at,
            policy=policy,
            notifications_enabled=True,
        )

    assert result is not None
    assert result["phase"] == "confirmed"
    assert result["delivery"]["accepted"] is True
    assert result["delivery"]["queued"] is True
    assert result["delivery"]["delivered"] is False
    assert len(enqueued) == 1
    envelope, text = enqueued[0]
    assert envelope.kind == "level_path_confirmed"
    assert envelope.event_id.endswith(":confirmed")
    assert "等待 TRADE READY" in text
    assert "本提醒不连接真实订单" in text
