from __future__ import annotations

from copy import deepcopy
from datetime import timedelta
import json

import pytest

from spx_spark.application.market_features import service
from spx_spark.application.order_map import desk_projection_export
from spx_spark.config import StorageSettings
from spx_spark.infrastructure.operational_db import read_strategy_decisions
from spx_spark.settings import load_app_settings
from tests.test_operational_db import NOW, _candidate, _no_trade


@pytest.fixture
def cycle(tmp_path, monkeypatch, migrate_operational_database):
    database = migrate_operational_database(tmp_path)
    storage = StorageSettings(
        data_root=str(tmp_path),
        latest_state_path=str(tmp_path / "latest" / "state.json"),
        raw_file_name="quotes.jsonl",
        include_raw_payload=False,
        latest_stale_after_seconds=90.0,
        slow_index_stale_after_seconds=180.0,
        slow_index_labels=frozenset(),
    )
    app = load_app_settings()
    emitted = []
    frames = []
    selected = _candidate()
    selector_inputs = []

    def select(payload, latest, now, **kwargs):
        selector_inputs.append(deepcopy(payload))
        return deepcopy(selected)

    def enqueue(decision, *, now, storage_settings, action_clock):
        # Delivery must never observe an uncommitted or different decision.
        assert decision in read_strategy_decisions(database_path=database)
        assert json.loads((tmp_path / "latest" / "strategy_decision.json").read_text()) == decision
        emitted.append((deepcopy(decision), now))
        return {"accepted": True}

    monkeypatch.setattr(service, "build_strategy_decision", select)
    monkeypatch.setattr(service, "enqueue_strategy_decision", enqueue)
    monkeypatch.setattr(service, "enqueue_pin_stable_watch", lambda *a, **kw: {"accepted": False})
    monkeypatch.setattr(desk_projection_export, "refresh_desk_on_data_recovery", lambda *a, **kw: {})
    for name in (
        "run_level_decision_shadow",
        "process_trade_intent",
        "process_strategy_distribution_forecast",
        "process_gamma_prearm_plan",
        "process_gth_level_manual_candidate",
        "evaluate_gth_manual_candidate",
        "process_virtual_strategy",
        "observe_due_strategy_outcomes",
        "build_zero_dte_greeks_reference",
        "_process_spring_gamma_v3_shadow",
    ):
        monkeypatch.setattr(service, name, lambda *a, **kw: {})

    def run(*, now=NOW, action_now=None, cache=None):
        return service.run(
            ["--json"], now=now, action_clock=lambda: action_now or now,
            app_settings=app, storage_settings=storage, state_cache=cache,
            on_frames=lambda market, options: frames.append((market, options)),
        )

    return run, selected, emitted, frames, selector_inputs, database


@pytest.mark.parametrize("decision", [_candidate(), _no_trade()], ids=["candidate", "no-trade"])
def test_cycle_preserves_formal_payload_through_commit_and_export(
    cycle, tmp_path, capsys, decision,
):
    run, selected, emitted, frames, inputs, database = cycle
    selected.clear()
    selected.update(decision)
    action_now = NOW + timedelta(seconds=3)

    assert run(action_now=action_now) == 0

    assert read_strategy_decisions(database_path=database) == (decision,)
    assert json.loads((tmp_path / "latest" / "strategy_decision.json").read_text()) == decision
    assert emitted == [(decision, action_now)]
    assert len(frames) == 1
    assert inputs[0]["candidates"] == []
    assert decision["execution"]["automatic_ordering"] is False
    summary = json.loads(capsys.readouterr().out.splitlines()[-1])
    assert summary["strategy_decision_id"] == decision["decision_id"]
    assert summary["strategy_persistence"]["ok"] is True
    assert summary["market_quality"] == frames[0][0]["quality"]
    assert summary["option_quality"] == frames[0][1]["quality"]


def test_cycle_does_not_publish_or_deliver_when_commit_fails(cycle, monkeypatch, tmp_path, capsys):
    run, _, emitted, frames, _, database = cycle

    def fail(*args, **kwargs):
        raise OSError("database unavailable")

    monkeypatch.setattr(service, "persist_strategy_decision", fail)
    assert run() == 0

    assert read_strategy_decisions(database_path=database) == ()
    assert not (tmp_path / "latest" / "strategy_decision.json").exists()
    assert emitted == []
    assert len(frames) == 1
    summary = json.loads(capsys.readouterr().out.splitlines()[-1])
    assert summary["strategy_persistence"]["ok"] is False


@pytest.mark.parametrize("failure", ["build_zero_dte_greeks_reference", "_process_spring_gamma_v3_shadow"])
def test_research_failure_cannot_change_the_committed_decision(cycle, monkeypatch, failure, tmp_path):
    run, selected, emitted, frames, _, database = cycle
    research_observations = []

    def fail(*args, **kwargs):
        research_observations.append(read_strategy_decisions(database_path=database))
        raise ValueError("research unavailable")

    monkeypatch.setattr(service, failure, fail)
    assert run() == 0

    assert research_observations == [(selected,)]
    assert read_strategy_decisions(database_path=database) == (selected,)
    assert json.loads((tmp_path / "latest" / "strategy_decision.json").read_text()) == selected
    assert emitted == [(selected, NOW)]
    assert len(frames) == 1


def test_cycle_reuses_research_cache_within_minute_but_refreshes_next_minute(cycle, monkeypatch):
    run, _, _, frames, _, _ = cycle
    computations = []

    def compute(latest, **kwargs):
        computations.append(latest.as_of)
        return {}

    monkeypatch.setattr(service, "build_zero_dte_greeks_reference", compute)
    cache = {}
    assert run(cache=cache) == 0
    assert run(now=NOW + timedelta(seconds=5), cache=cache) == 0
    assert run(now=NOW + timedelta(minutes=1), cache=cache) == 0

    assert computations == [NOW, NOW + timedelta(minutes=1)]
    assert len(frames) == 3
