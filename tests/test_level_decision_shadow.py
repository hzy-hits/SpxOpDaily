from __future__ import annotations

import json
from dataclasses import replace
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

from spx_spark.application.order_map import level_decision_shadow as shadow_service
from spx_spark.application.order_map.level_decision_machine import (
    LevelObservation,
    LevelPhase,
    advance_level_decision,
)
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


def test_pending_structure_gate_applies_only_before_a_new_arm() -> None:
    terminal = {
        LevelPhase.FAR,
        LevelPhase.INVALIDATED,
        LevelPhase.EXPIRED,
    }

    assert shadow_service._structure_pending_blocks_new_arm(
        None,
        structure_change_pending=True,
    )
    for phase in LevelPhase:
        assert (
            shadow_service._structure_pending_blocks_new_arm(
                {"phase": phase.value},
                structure_change_pending=True,
            )
            is (phase in terminal)
        )
        assert not shadow_service._structure_pending_blocks_new_arm(
            {"phase": phase.value},
            structure_change_pending=False,
        )


def test_pending_structure_keeps_active_lifecycle_on_frozen_stable_levels(
    tmp_path, monkeypatch
) -> None:
    storage = SimpleNamespace(data_root=str(tmp_path))
    stable = _stable_structure(NOW, put_wall=100.0, call_wall=120.0)
    armed = advance_level_decision(
        None,
        _level_observation(NOW, spot=95.0, levels=stable["levels"]),
    )
    _write_shadow_state(tmp_path, decision=armed.state, stable=stable)
    monkeypatch.setattr(
        shadow_service,
        "_live_structure",
        lambda *_args, **_kwargs: _live_structure(
            NOW + timedelta(seconds=5),
            put_wall=110.0,
            call_wall=130.0,
        ),
    )
    seen: dict[str, object] = {}

    def fake_observation(_storage, _tick, *, now, frozen_structure, **kwargs):
        blocks_arm = kwargs["structure_pending_blocks_new_arm"]
        levels = shadow_service._structure_levels(frozen_structure)
        seen.update({"blocks_arm": blocks_arm, "levels": levels})
        return _level_observation(
            now,
            spot=96.0,
            levels=levels,
            arm_allowed=not blocks_arm,
            arm_block_reason=(
                "structure_change_pending_new_arm_blocked" if blocks_arm else None
            ),
        )

    monkeypatch.setattr(shadow_service, "_observation", fake_observation)

    result = run_level_decision_shadow(
        storage,
        SimpleNamespace(),
        now=NOW + timedelta(seconds=5),
    )

    assert result["phase"] == LevelPhase.TESTING.value
    assert result["reason"] == "entered_test_zone"
    assert result["structure_change_pending"] is True
    assert result["new_arm_blocked"] is False
    assert result["quality_ok"] is True
    assert result["quality_reason"] is None
    assert seen == {
        "blocks_arm": False,
        "levels": {"put_wall": 100.0, "call_wall": 120.0},
    }
    health = (
        tmp_path
        / "features"
        / "level_decision_health"
        / "date=2026-07-13"
        / "samples.jsonl"
    )
    sample = json.loads(health.read_text().splitlines()[-1])
    assert sample["structure_change_pending"] is True
    assert sample["new_arm_blocked"] is False
    assert sample["levels"]["put_wall"] == 100.0
    audit = (
        tmp_path
        / "features"
        / "level_decision_audit"
        / "date=2026-07-13"
        / "transitions.jsonl"
    )
    transition = json.loads(audit.read_text().splitlines()[-1])
    assert transition["structure_change_pending"] is True
    assert transition["new_arm_blocked"] is False


def test_pending_structure_blocks_new_arm_until_promotion(tmp_path, monkeypatch) -> None:
    storage = SimpleNamespace(data_root=str(tmp_path))
    stable = _stable_structure(NOW, put_wall=100.0, call_wall=120.0)
    _write_shadow_state(tmp_path, decision=None, stable=stable)
    monkeypatch.setattr(
        shadow_service,
        "_live_structure",
        lambda *_args, **_kwargs: _live_structure(
            NOW + timedelta(seconds=5),
            put_wall=110.0,
            call_wall=130.0,
        ),
    )

    def fake_observation(_storage, _tick, *, now, frozen_structure, **kwargs):
        blocks_arm = kwargs["structure_pending_blocks_new_arm"]
        return _level_observation(
            now,
            spot=100.0,
            levels=shadow_service._structure_levels(frozen_structure),
            arm_allowed=not blocks_arm,
            arm_block_reason=(
                "structure_change_pending_new_arm_blocked" if blocks_arm else None
            ),
        )

    monkeypatch.setattr(shadow_service, "_observation", fake_observation)

    result = run_level_decision_shadow(
        storage,
        SimpleNamespace(),
        now=NOW + timedelta(seconds=5),
    )

    assert result["phase"] == LevelPhase.FAR.value
    assert result["event_id"] is None
    assert result["structure_change_pending"] is True
    assert result["new_arm_blocked"] is True
    assert result["quality_ok"] is True
    assert result["quality_reason"] is None


def test_promoted_structure_still_runs_machine_drift_validation(
    tmp_path, monkeypatch
) -> None:
    storage = SimpleNamespace(data_root=str(tmp_path))
    stable = _stable_structure(NOW, put_wall=100.0, call_wall=120.0)
    armed = advance_level_decision(
        None,
        _level_observation(NOW, spot=95.0, levels=stable["levels"]),
    )
    bucket = int(NOW.timestamp()) // 900
    candidate = {
        **_live_structure(
            NOW - timedelta(seconds=900),
            put_wall=110.0,
            call_wall=130.0,
        ),
        "levels": {"put_wall": 110.0, "call_wall": 130.0},
        "samples": [
            {
                "bucket": bucket - 1,
                "levels": {"put_wall": 110.0, "call_wall": 130.0},
                "at": (NOW - timedelta(seconds=900)).isoformat(),
            }
        ],
        "confirmation_count": 1,
        "required_confirmations": 2,
    }
    _write_shadow_state(
        tmp_path,
        decision=armed.state,
        stable=stable,
        candidate=candidate,
        last_bucket=bucket - 1,
    )
    monkeypatch.setattr(
        shadow_service,
        "_live_structure",
        lambda *_args, **_kwargs: _live_structure(
            NOW + timedelta(seconds=5),
            put_wall=110.0,
            call_wall=130.0,
        ),
    )

    def fake_observation(_storage, _tick, *, now, frozen_structure, **kwargs):
        blocks_arm = kwargs["structure_pending_blocks_new_arm"]
        return _level_observation(
            now,
            spot=96.0,
            levels=shadow_service._structure_levels(frozen_structure),
            arm_allowed=not blocks_arm,
            arm_block_reason=(
                "structure_change_pending_new_arm_blocked" if blocks_arm else None
            ),
        )

    monkeypatch.setattr(shadow_service, "_observation", fake_observation)

    result = run_level_decision_shadow(
        storage,
        SimpleNamespace(),
        now=NOW + timedelta(seconds=5),
    )

    assert result["phase"] == LevelPhase.INVALIDATED.value
    assert result["reason"] == "structure_drift"
    assert result["structure_change_pending"] is False
    assert result["new_arm_blocked"] is False
    assert result["levels"]["put_wall"] == 110.0


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
            spx_levels={"put_wall": 100.0, "call_wall": 120.0},
            trigger_coordinate_kind="official_spx",
            trigger_instrument_id="index:SPX",
            trigger_basis_points=4905.0,
            spx_spot=95.0,
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
        spx_levels={"put_wall": 100.0, "call_wall": 120.0},
        trigger_coordinate_kind="official_spx",
        trigger_instrument_id="index:SPX",
        trigger_basis_points=4901.0,
        spx_spot=99.0,
    )
    result = run_level_decision_shadow(storage, SimpleNamespace(), now=NOW + timedelta(seconds=5))
    assert result["phase"] == "testing"
    persisted = load_level_decision_shadow(storage)
    assert persisted["phase"] == "testing"

    audit = tmp_path / "features" / "level_decision_audit" / "date=2026-07-13" / "transitions.jsonl"
    rows = [json.loads(line) for line in audit.read_text().splitlines()]
    assert [row["current_phase"] for row in rows] == ["approaching", "testing"]
    assert len({row["record_key"] for row in rows}) == 2

    health = (
        tmp_path
        / "features"
        / "level_decision_health"
        / "date=2026-07-13"
        / "samples.jsonl"
    )
    samples = [json.loads(line) for line in health.read_text().splitlines()]
    latest = samples[-1]
    assert latest["schema_version"] == 2
    assert latest["spot"] == 99.0
    assert latest["es"] == 5000.0
    assert latest["levels"]["put_wall"] == 100.0
    assert latest["spx_levels"]["call_wall"] == 120.0
    assert latest["trigger_coordinate_kind"] == "official_spx"
    assert latest["trigger_instrument_id"] == "index:SPX"
    assert latest["machine_settings"]["accept_hold_seconds"] == 20.0


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


def _stable_structure(
    at: datetime,
    *,
    put_wall: float,
    call_wall: float,
) -> dict[str, object]:
    return {
        "levels": {"put_wall": put_wall, "call_wall": call_wall},
        "expiry": "20260713",
        "source": "stable_15m_oi_gex",
        "observed_at": at.isoformat(),
        "session_date": "2026-07-13",
        "promoted_at": at.isoformat(),
        "last_confirmed_at": at.isoformat(),
        "confirmation_count": 1,
    }


def _live_structure(
    at: datetime,
    *,
    put_wall: float,
    call_wall: float,
) -> dict[str, object]:
    return {
        "levels": {"put_wall": put_wall, "call_wall": call_wall},
        "expiry": "20260713",
        "source": "live_oi_gex",
        "observed_at": at.isoformat(),
        "session_date": "2026-07-13",
    }


def _level_observation(
    at: datetime,
    *,
    spot: float,
    levels: dict[str, float],
    quality_ok: bool = True,
    quality_reason: str | None = None,
    arm_allowed: bool = True,
    arm_block_reason: str | None = None,
) -> LevelObservation:
    return LevelObservation(
        at=at,
        spot=spot,
        es=5000.0,
        levels=levels,
        quality_ok=quality_ok,
        quality_reason=quality_reason,
        session_date="2026-07-13",
        spx_levels=levels,
        trigger_coordinate_kind="official_spx",
        trigger_instrument_id="index:SPX",
        trigger_basis_points=4905.0,
        spx_spot=spot,
        arm_allowed=arm_allowed,
        arm_block_reason=arm_block_reason,
    )


def _write_shadow_state(
    tmp_path,
    *,
    decision: dict[str, object] | None,
    stable: dict[str, object],
    candidate: dict[str, object] | None = None,
    last_bucket: int | None = None,
) -> None:
    path = tmp_path / "latest" / "level_decision_shadow_state.json"
    path.parent.mkdir(parents=True)
    payload: dict[str, object] = {
        "schema_version": 1,
        "outcomes": {},
        "structure": stable,
        "structure_stability": {
            "schema_version": 1,
            "stable": stable,
            "candidate": candidate,
            "last_bucket": (
                last_bucket if last_bucket is not None else int(NOW.timestamp()) // 900
            ),
        },
    }
    if decision is not None:
        payload["decision"] = decision
    path.write_text(json.dumps(payload), encoding="utf-8")
