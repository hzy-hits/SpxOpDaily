from __future__ import annotations

import json
from dataclasses import replace
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

import pytest

from spx_spark.application.order_map import level_decision_shadow as shadow_service
from spx_spark.application.order_map import level_transition_delivery as transition_delivery
from spx_spark.application.order_map.level_decision_machine import (
    LevelObservation,
    LevelPhase,
    LevelTransition,
    advance_level_decision,
)
from spx_spark.application.order_map.level_decision_shadow import (
    _structure_session_age,
    load_level_decision_shadow,
    run_level_decision_shadow,
)
from spx_spark.application.order_map.trigger_coordinates import (
    TriggerCoordinate,
    TriggerCoordinateKind,
)
from spx_spark.settings.level_decision import LevelDecisionPolicy, LevelDecisionSession


NOW = datetime(2026, 7, 13, 14, 30, tzinfo=timezone.utc)


@pytest.mark.parametrize(
    ("at", "expected"),
    (
        ("2026-07-12T20:14:59-04:00", LevelDecisionSession.CLOSED),
        ("2026-07-12T20:15:00-04:00", LevelDecisionSession.GTH),
        ("2026-07-13T09:24:59-04:00", LevelDecisionSession.GTH),
        ("2026-07-13T09:25:00-04:00", LevelDecisionSession.CLOSED),
        ("2026-07-13T09:30:00-04:00", LevelDecisionSession.RTH),
        ("2026-07-13T17:30:00-04:00", LevelDecisionSession.CLOSED),
    ),
)
def test_level_decision_session_uses_strict_spx_windows(
    at: str,
    expected: LevelDecisionSession,
) -> None:
    assert shadow_service._level_decision_session(datetime.fromisoformat(at)) is expected


def test_machine_settings_select_session_specific_timeout() -> None:
    policy = LevelDecisionPolicy(
        phase_timeout_seconds=90.0,
        gth_phase_timeout_seconds=300.0,
    )

    assert (
        shadow_service._machine_settings(
            policy,
            session=LevelDecisionSession.GTH,
        ).phase_timeout_seconds
        == 300.0
    )
    for session in (LevelDecisionSession.RTH, LevelDecisionSession.CLOSED):
        assert (
            shadow_service._machine_settings(
                policy,
                session=session,
            ).phase_timeout_seconds
            == 90.0
        )


def test_public_projection_preserves_reentry_generation() -> None:
    projected = shadow_service._public_state(
        {
            "phase": "confirmed",
            "event_id": "level:generation-five",
            "reentry_generation": 5,
            "thesis": "breakout",
            "direction": "up",
        },
        formal_signal_enabled=True,
    )

    assert projected["event_id"] == "level:generation-five"
    assert projected["reentry_generation"] == 5


def test_public_projection_exposes_top_level_trigger_basis_points() -> None:
    projected = shadow_service._public_state(
        {
            "phase": "armed",
            "thesis": "support",
            "direction": "up",
            "level": 6820.0,
            "spx_level": 6820.0,
            "trigger_coordinate_kind": "es_equivalent",
            "trigger_instrument_id": "future:ES",
            "trigger_basis_points": 48.5,
        },
        formal_signal_enabled=False,
        latest_observation={
            "spot": 6870.0,
            "es": 6870.0,
            "spx_spot": 6821.5,
            "trigger_coordinate_kind": "es_equivalent",
            "trigger_instrument_id": "future:ES",
            "trigger_basis_points": 48.5,
            "session_mode": "gth",
            "quality_ok": True,
        },
    )

    assert projected["trigger_basis_points"] == pytest.approx(48.5)
    assert projected["es_basis_points"] == pytest.approx(48.5)
    assert projected["trigger_coordinate"]["basis_points"] == pytest.approx(48.5)


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
        assert shadow_service._structure_pending_blocks_new_arm(
            {"phase": phase.value},
            structure_change_pending=True,
        ) is (phase in terminal)
        assert not shadow_service._structure_pending_blocks_new_arm(
            {"phase": phase.value},
            structure_change_pending=False,
        )


def test_rth_observation_releases_es_latch_when_official_spx_recovers(
    tmp_path, monkeypatch
) -> None:
    storage = SimpleNamespace(data_root=str(tmp_path))
    state = SimpleNamespace(best_quote=lambda instrument_id: object())

    class Store:
        def __init__(self, _storage) -> None:
            raise AssertionError("preloaded state must bypass LatestStateStore")

        def load(self, *, now):
            raise AssertionError("preloaded state must bypass LatestStateStore")

    monkeypatch.setattr(shadow_service, "LatestStateStore", Store)
    monkeypatch.setattr(
        shadow_service,
        "actionable_live_price",
        lambda *_args, **_kwargs: 7420.0,
    )
    monkeypatch.setattr(shadow_service, "_qualified_es_basis", lambda *_args, **_kwargs: 40.0)
    monkeypatch.setattr(
        shadow_service,
        "build_options_map",
        lambda _state: pytest.fail("prebuilt options map must be reused"),
    )
    monkeypatch.setattr(
        shadow_service,
        "resolve_trigger_coordinate",
        lambda *_args, **_kwargs: TriggerCoordinate(
            kind=TriggerCoordinateKind.OFFICIAL_SPX,
            instrument_id="index:SPX",
            observed_value=7380.0,
            spx_observed_value=7380.0,
            basis_points=None,
            source="index:SPX",
            as_of=NOW,
            reason="rth_official_spx",
        ),
    )

    result = shadow_service._observation(
        storage,
        SimpleNamespace(),
        now=NOW,
        session_date="2026-07-13",
        session_mode="rth",
        frozen_structure=_stable_structure(NOW, put_wall=7375.0, call_wall=7450.0),
        latest_state=state,
        options_map=SimpleNamespace(),
        active_decision={
            "phase": LevelPhase.BREAK_PENDING.value,
            "trigger_coordinate_kind": "es_equivalent",
            "trigger_instrument_id": "future:ES",
            "trigger_basis_points": 40.0,
        },
    )

    assert result.quality_ok is True
    assert result.trigger_coordinate_kind == "official_spx"
    assert result.trigger_instrument_id == "index:SPX"
    assert result.spot == 7380.0
    assert result.spx_spot == 7380.0
    assert result.levels == {"put_wall": 7375.0, "call_wall": 7450.0}
    assert result.trigger_basis_points == 40.0
    assert result.spot_source == "index:SPX"


def test_rth_observation_releases_chain_latch_when_official_spx_recovers(
    tmp_path, monkeypatch
) -> None:
    storage = SimpleNamespace(data_root=str(tmp_path))
    state = SimpleNamespace(best_quote=lambda instrument_id: object())

    class Store:
        def __init__(self, _storage) -> None:
            pass

        def load(self, *, now):
            assert now == NOW
            return state

    monkeypatch.setattr(shadow_service, "LatestStateStore", Store)
    monkeypatch.setattr(
        shadow_service,
        "actionable_live_price",
        lambda *_args, **_kwargs: 7420.0,
    )
    monkeypatch.setattr(shadow_service, "_qualified_es_basis", lambda *_args, **_kwargs: 40.0)
    monkeypatch.setattr(shadow_service, "build_options_map", lambda _state: None)
    monkeypatch.setattr(
        shadow_service,
        "resolve_trigger_coordinate",
        lambda *_args, **_kwargs: TriggerCoordinate(
            kind=TriggerCoordinateKind.OFFICIAL_SPX,
            instrument_id="index:SPX",
            observed_value=7380.0,
            spx_observed_value=7380.0,
            basis_points=None,
            source="index:SPX",
            as_of=NOW,
            reason="rth_official_spx",
        ),
    )

    result = shadow_service._observation(
        storage,
        SimpleNamespace(),
        now=NOW,
        session_date="2026-07-13",
        session_mode="rth",
        frozen_structure=_stable_structure(NOW, put_wall=7375.0, call_wall=7450.0),
        active_decision={
            "phase": LevelPhase.BREAK_PENDING.value,
            "session_mode": "rth",
            "trigger_coordinate_kind": "chain_implied_spx",
            "trigger_instrument_id": "synthetic:SPXW_PARITY",
            "trigger_basis_points": 40.0,
        },
    )

    assert result.quality_ok is True
    assert result.trigger_coordinate_kind == "official_spx"
    assert result.trigger_instrument_id == "index:SPX"
    assert result.spot == 7380.0
    assert result.spx_spot == 7380.0
    assert result.levels == {"put_wall": 7375.0, "call_wall": 7450.0}
    assert result.trigger_basis_points == 40.0
    assert result.spot_source == "index:SPX"


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
            arm_block_reason=("structure_change_pending_new_arm_blocked" if blocks_arm else None),
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
    health = tmp_path / "features" / "level_decision_health" / "date=2026-07-13" / "samples.jsonl"
    sample = json.loads(health.read_text().splitlines()[-1])
    assert sample["structure_change_pending"] is True
    assert sample["new_arm_blocked"] is False
    assert sample["levels"]["put_wall"] == 100.0
    audit = tmp_path / "features" / "level_decision_audit" / "date=2026-07-13" / "transitions.jsonl"
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
            arm_block_reason=("structure_change_pending_new_arm_blocked" if blocks_arm else None),
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


def test_direct_live_structure_refreshes_walls_without_realtime_engine_tick(
    tmp_path, monkeypatch
) -> None:
    storage = SimpleNamespace(data_root=str(tmp_path))
    stable = _stable_structure(NOW, put_wall=100.0, call_wall=120.0)
    _write_shadow_state(tmp_path, decision=None, stable=stable)

    def fake_observation(_storage, _tick, *, now, frozen_structure, **kwargs):
        blocks_arm = kwargs["structure_pending_blocks_new_arm"]
        return _level_observation(
            now,
            spot=100.0,
            levels=shadow_service._structure_levels(frozen_structure),
            arm_allowed=not blocks_arm,
            arm_block_reason=("structure_change_pending_new_arm_blocked" if blocks_arm else None),
        )

    monkeypatch.setattr(shadow_service, "_observation", fake_observation)
    monkeypatch.setattr(
        shadow_service,
        "_live_structure",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(
            AssertionError("tick-derived structure must not be used")
        ),
    )

    result = run_level_decision_shadow(
        storage,
        None,
        now=NOW + timedelta(seconds=5),
        live_structure=_live_structure(
            NOW + timedelta(seconds=5),
            put_wall=110.0,
            call_wall=130.0,
        ),
    )

    assert result["structure_change_pending"] is True
    assert result["structure_candidate"]["levels"] == {
        "put_wall": 110.0,
        "call_wall": 130.0,
    }
    assert result["new_arm_blocked"] is True


def test_promoted_structure_still_runs_machine_drift_validation(tmp_path, monkeypatch) -> None:
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
            arm_block_reason=("structure_change_pending_new_arm_blocked" if blocks_arm else None),
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

    health = tmp_path / "features" / "level_decision_health" / "date=2026-07-13" / "samples.jsonl"
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
    assert result["delivery"]["delivery_gate"] == "independent_trade_ready_required"
    assert result["spot_source"] == "es_basis_adjusted:46.0000"


def test_confirmed_shadow_emits_deduplicated_30_second_outcome(tmp_path, monkeypatch) -> None:
    storage = SimpleNamespace(data_root=str(tmp_path))
    current: dict[str, LevelObservation] = {}
    monkeypatch.setattr(
        "spx_spark.application.order_map.level_decision_shadow._observation",
        lambda *_args, **_kwargs: current["value"],
    )
    path = (
        (0, 107.0, 5000.0),
        (5, 101.0, 5000.0),
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
            trigger_coordinate_kind="official_spx",
            trigger_instrument_id="index:SPX",
            trigger_basis_points=4905.0,
            spx_spot=spot,
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
    persisted = json.loads(
        (tmp_path / "latest" / "level_decision_shadow_state.json").read_text()
    )
    observation = next(iter(persisted["outcomes"]["observations"].values()))
    assert observation["trigger_coordinate_kind"] == "official_spx"
    assert observation["trigger_instrument_id"] == "index:SPX"
    assert observation["latched_basis_points"] == 4905.0
    assert all(sample["trigger_coordinate_kind"] == "official_spx" for sample in observation["samples"])


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
        (0, 107.0, 5000.0),
        (5, 101.0, 5000.0),
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
    assert result["delivery"]["delivery_gate"] == "independent_trade_ready_required"


def test_meaningful_level_path_transitions_send_idempotent_non_executable_cards(
    tmp_path, monkeypatch
) -> None:
    storage = SimpleNamespace(data_root=str(tmp_path))
    current: dict[str, LevelObservation] = {}
    enqueued: list[tuple[object, str]] = []
    monkeypatch.setattr(
        shadow_service,
        "_observation",
        lambda *_args, **_kwargs: current["value"],
    )
    monkeypatch.setattr(
        transition_delivery,
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

    monkeypatch.setattr(transition_delivery, "enqueue_notification", fake_enqueue)
    policy = replace(
        LevelDecisionPolicy(),
        formal_signal_enabled=True,
        notify_transitions=True,
    )
    result = None
    for seconds, spot, es in (
        (0, 107.0, 5000.0),
        (5, 101.0, 5000.0),
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
    assert [envelope.kind for envelope, _text in enqueued] == [
        "level_setup_transition",
    ]
    assert [envelope.event_id.rsplit(":", 1)[-1] for envelope, _text in enqueued] == [
        "confirmed",
    ]
    assert enqueued[0][0].expires_at == NOW + timedelta(seconds=56, minutes=15)
    confirmed_text = enqueued[-1][1]
    assert "State  RETEST → CONFIRMED" in confirmed_text
    assert "继续等待合约、NBBO、R/R 与时效门控" in confirmed_text
    assert "独立 MANUAL READY" in confirmed_text


def test_level_transition_notification_retries_after_state_commit_enqueue_crash(
    tmp_path, monkeypatch
) -> None:
    storage = SimpleNamespace(data_root=str(tmp_path))
    current: dict[str, LevelObservation] = {}
    monkeypatch.setattr(
        shadow_service,
        "_observation",
        lambda *_args, **_kwargs: current["value"],
    )
    monkeypatch.setattr(
        transition_delivery,
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
    attempts = 0

    def enqueue(_settings, _envelope, **_kwargs):
        nonlocal attempts
        attempts += 1
        if attempts == 1:
            raise RuntimeError("simulated state-to-outbox crash")
        return SimpleNamespace(
            targets=("feishu",),
            accepted=True,
            inserted=True,
            duplicate=False,
            queued_for_recovery=True,
            delivered=False,
            outcome="pending",
        )

    monkeypatch.setattr(transition_delivery, "enqueue_notification", enqueue)
    policy = replace(
        LevelDecisionPolicy(),
        formal_signal_enabled=True,
        notify_transitions=True,
    )
    current["value"] = _level_observation(
        NOW,
        spot=70.0,
        levels={"put_wall": 100.0, "call_wall": 120.0},
    )
    run_level_decision_shadow(
        storage,
        None,
        now=NOW,
        policy=policy,
        notifications_enabled=True,
    )
    failed = None
    for seconds, spot, es in (
        (5, 101.0, 5000.0),
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
        failed = run_level_decision_shadow(
            storage,
            None,
            now=at,
            policy=policy,
            notifications_enabled=True,
        )
    assert failed is not None
    state_path = tmp_path / "latest" / "level_decision_shadow_state.json"
    after_failure = json.loads(state_path.read_text(encoding="utf-8"))

    assert failed["delivery"]["reason"] == "delivery_error:RuntimeError"
    assert len(after_failure["pending_notifications"]) == 1

    recovered = run_level_decision_shadow(
        storage,
        None,
        now=NOW + timedelta(seconds=57),
        policy=policy,
        notifications_enabled=True,
    )
    after_recovery = json.loads(state_path.read_text(encoding="utf-8"))

    assert recovered["changed"] is False
    assert recovered["delivery"]["accepted"] is True
    assert attempts == 2
    assert after_recovery["pending_notifications"] == []
    assert after_recovery["accepted_notification_event_ids"] == [
        failed["delivery"]["notification_event_id"]
    ]


def test_level_transition_notification_replays_idempotently_after_ack_crash(
    tmp_path, monkeypatch
) -> None:
    state_path = tmp_path / "latest" / "level_decision_shadow_state.json"
    state_path.parent.mkdir(parents=True)
    event_id = "level-path:setup:approaching"
    state_path.write_text(
        json.dumps(
            {
                "schema_version": 1,
                "pending_notifications": [
                    {
                        "event_id": event_id,
                        "source": "level_decision",
                        "kind": "level_setup_transition",
                        "lane": "market_warning",
                        "occurred_at": NOW.isoformat(),
                        "title": "SPX SETUP TRANSITION",
                        "text": "frozen setup payload",
                        "friend": True,
                        "feishu_text": "frozen setup payload",
                        "enqueued_at": NOW.isoformat(),
                    }
                ],
            }
        ),
        encoding="utf-8",
    )
    monkeypatch.setattr(
        transition_delivery,
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
    attempts: list[str] = []

    def enqueue(_settings, envelope, **_kwargs):
        attempts.append(envelope.event_id)
        return SimpleNamespace(
            targets=("feishu",),
            accepted=True,
            inserted=len(attempts) == 1,
            duplicate=len(attempts) > 1,
            queued_for_recovery=True,
            delivered=False,
            outcome="pending",
        )

    real_atomic_write = transition_delivery.atomic_write_json_secure
    monkeypatch.setattr(
        transition_delivery,
        "atomic_write_json_secure",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(
            RuntimeError("simulated outbox-to-state ack crash")
        ),
    )
    with pytest.raises(RuntimeError, match="ack crash"):
        transition_delivery.flush_pending_level_transition_notifications(
            state_path,
            now=NOW,
            enqueue=enqueue,
        )
    assert json.loads(state_path.read_text(encoding="utf-8"))["pending_notifications"]

    monkeypatch.setattr(
        transition_delivery,
        "atomic_write_json_secure",
        real_atomic_write,
    )
    recovered = transition_delivery.flush_pending_level_transition_notifications(
        state_path,
        now=NOW + timedelta(seconds=1),
        enqueue=enqueue,
    )
    persisted = json.loads(state_path.read_text(encoding="utf-8"))

    assert attempts == [event_id, event_id]
    assert recovered["accepted"] is True
    assert recovered["duplicate"] is True
    assert persisted["pending_notifications"] == []
    assert persisted["accepted_notification_event_ids"] == [event_id]


def test_level_transition_quiet_window_is_suppressed_not_delivered(
    tmp_path, monkeypatch
) -> None:
    storage = SimpleNamespace(data_root=str(tmp_path))
    current: dict[str, LevelObservation] = {}
    monkeypatch.setattr(
        shadow_service,
        "_observation",
        lambda *_args, **_kwargs: current["value"],
    )
    monkeypatch.setattr(
        transition_delivery,
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
    monkeypatch.setattr(
        transition_delivery,
        "enqueue_notification",
        lambda *_args, **_kwargs: SimpleNamespace(
            targets=("quiet_window_policy",),
            accepted=True,
            inserted=False,
            duplicate=False,
            queued_for_recovery=False,
            delivered=True,
            outcome="quiet_window_suppressed",
        ),
    )
    policy = replace(
        LevelDecisionPolicy(),
        formal_signal_enabled=True,
        notify_transitions=True,
    )
    current["value"] = _level_observation(
        NOW,
        spot=70.0,
        levels={"put_wall": 100.0, "call_wall": 120.0},
    )
    run_level_decision_shadow(
        storage,
        None,
        now=NOW,
        policy=policy,
        notifications_enabled=True,
    )
    result = None
    for seconds, spot, es in (
        (5, 101.0, 5000.0),
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
    assert result["delivery"]["accepted"] is True
    assert result["delivery"]["delivered"] is False
    assert result["delivery"]["suppressed"] is True
    assert result["delivery"]["outcome"] == "quiet_window_suppressed"


def _stable_structure(
    at: datetime,
    *,
    put_wall: float,
    call_wall: float,
) -> dict[str, object]:
    return {
        "levels": {"put_wall": put_wall, "call_wall": call_wall},
        "expiry": "20260713",
        "source": "stable_intraday_oi_gex",
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

def _notify_settings(monkeypatch) -> None:
    monkeypatch.setattr(
        transition_delivery,
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


def _terminal_transition(previous: LevelPhase, *, thesis: str) -> LevelTransition:
    return LevelTransition(
        previous_phase=previous,
        current_phase=LevelPhase.INVALIDATED,
        state={
            "event_id": "level:prearm",
            "level_kind": "call_wall",
            "level": 7775.0,
            "spx_level": 7775.0,
            "thesis": thesis,
            "reentry_generation": 0,
            "quality_status": "ready",
            "trigger_coordinate_kind": "chain_implied_spx",
            "transition_count": 2,
        },
        changed=True,
        reason="structure_drift",
    )


def _terminal_observation() -> LevelObservation:
    return LevelObservation(
        at=NOW,
        spot=7757.2,
        es=7781.0,
        levels={"call_wall": 7800.0},
        quality_ok=True,
        spx_spot=7757.2,
        trigger_coordinate_kind="chain_implied_spx",
    )


@pytest.mark.parametrize("previous", [LevelPhase.APPROACHING, LevelPhase.TESTING])
def test_prearm_terminal_transition_is_audit_only(previous, monkeypatch) -> None:
    _notify_settings(monkeypatch)
    result, intent = transition_delivery.prepare_level_transition_delivery(
        _terminal_transition(previous, thesis="none"),
        _terminal_observation(),
        now=NOW,
        notify_transitions=True,
        formal_signal_enabled=True,
        notifications_enabled=True,
    )
    assert intent is None
    assert result is not None
    assert result["reason"] == "audit_only_prearm_terminal"


def test_pathful_invalidation_still_pushes_and_explains_reason(monkeypatch) -> None:
    _notify_settings(monkeypatch)
    result, intent = transition_delivery.prepare_level_transition_delivery(
        _terminal_transition(LevelPhase.REJECT_PENDING, thesis="fade"),
        _terminal_observation(),
        now=NOW,
        notify_transitions=True,
        formal_signal_enabled=True,
        notifications_enabled=True,
    )
    assert result is not None
    assert intent is not None
    text = str(intent["text"])
    assert "Reason  structure_drift" in text
    assert "Drift  冻结 Call Wall 7775.00 → 实时 7800.00（+25.00pt）" in text
