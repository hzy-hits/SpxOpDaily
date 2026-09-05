from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

from spx_spark.application.market_features.gamma_prearm_plan import (
    evaluate_gamma_prearm_plan,
    process_gamma_prearm_plan,
)
from spx_spark.application.order_map.level_trigger_repricing import REPRICING_PHASES


NOW = datetime(2026, 7, 30, 12, 0, tzinfo=timezone.utc)




def test_approaching_gamma_level_builds_two_sided_research_plan() -> None:
    plan = evaluate_gamma_prearm_plan(
        _repricing(),
        _level_decision(),
        now=NOW,
        spring_gamma=_spring_gamma(),
        prior_session=_prior_session(),
        gth_position_fraction=0.05,
    )

    assert "approaching" in REPRICING_PHASES
    assert plan["status"] == "prearm_ready"
    assert plan["execution_eligible"] is False
    assert plan["automatic_ordering"] is False
    assert plan["level"] == 7375.0
    assert [item["side"] for item in plan["paths"]] == ["CALL", "PUT"]


def test_process_keeps_prearm_audit_only_and_clears_legacy_pending(tmp_path) -> None:
    rth = datetime(2026, 7, 30, 15, 30, tzinfo=timezone.utc)
    repricing, level = _break_pending_inputs(as_of=rth)
    latest = tmp_path / "latest"
    latest.mkdir()
    (latest / "gamma_prearm_plan_state.json").write_text(
        json.dumps({"pending_notifications": [{"event_id": "legacy-prearm"}]}),
        encoding="utf-8",
    )

    result = process_gamma_prearm_plan(
        SimpleNamespace(data_root=str(tmp_path)), repricing, level, now=rth
    )
    state = json.loads((latest / "gamma_prearm_plan_state.json").read_text())

    assert result["status"] == "prearm_ready"
    assert result["notification_attempted"] is False
    assert result["notification_outcome"] == "unified_strategy_decision_owned"
    assert state["pending_notifications"] == []


def test_prearm_plan_requires_fresh_supported_repricing() -> None:
    stale = evaluate_gamma_prearm_plan(
        _repricing(as_of=NOW - timedelta(minutes=2)), _level_decision(), now=NOW
    )
    testing = evaluate_gamma_prearm_plan(
        {**_repricing(), "phase": "testing"},
        {**_level_decision(), "phase": "testing"},
        now=NOW,
    )

    assert stale["block_reasons"] == ["approach_repricing_stale"]
    assert testing["block_reasons"] == ["level_not_approaching"]


def _break_pending_inputs(*, as_of: datetime) -> tuple[dict[str, object], dict[str, object]]:
    level = {
        **_level_decision(),
        "phase": "break_pending",
        "thesis": "breakout",
        "direction": "down",
    }
    repricing = {
        **_repricing(as_of=as_of),
        "phase": "break_pending",
        "path_geometries": {
            "level_breakout_put": {"target_spx": 7365.0, "feasible": True}
        },
    }
    repricing["candidates"][0]["execution_bid"] = 12.1
    repricing["candidates"][0]["execution_ask"] = 12.3
    return repricing, level


def _level_decision(event_id: str = "level:flip-low-approach") -> dict[str, object]:
    return {
        "phase": "approaching",
        "event_id": event_id,
        "level_kind": "flip_low",
        "level": 7375.0,
    }


def _repricing(
    *, event_id: str = "level:flip-low-approach", as_of: datetime = NOW
) -> dict[str, object]:
    return {
        "status": "repriced",
        "phase": "approaching",
        "event_id": event_id,
        "as_of": as_of.isoformat(),
        "expiry": "20260730",
        "level_kind": "flip_low",
        "spx_level": 7375.0,
        "pricing_spot": 7390.0,
        "trigger_coordinate": {
            "kind": "chain_implied_spx",
            "observed_value": 7390.0,
            "target_value": 7375.0,
        },
        "touch_time_estimate": {"base_minutes": 12.0},
        "gamma_context": {"state": "positive_gamma_pin"},
        "candidates": [
            {
                "play": "level_breakout_put",
                "right": "P",
                "contract_id": "option:SPX:SPXW:20260730:7375:P",
                "execution_quote_status": "executable",
                "projection_range_low": 12.2,
                "projected_mid": 12.5,
                "projection_range_high": 12.8,
                "execution_quote_provider": "ibkr",
            },
            {
                "play": "level_fade_call",
                "right": "C",
                "contract_id": "option:SPX:SPXW:20260730:7375:C",
                "execution_quote_status": "executable",
                "projection_range_low": 13.1,
                "projected_mid": 13.4,
                "projection_range_high": 13.7,
                "execution_quote_provider": "ibkr",
            },
        ],
    }


def _spring_gamma() -> dict[str, object]:
    return {
        "status": "ready",
        "as_of": NOW.isoformat(),
        "expiry": "20260730",
        "actionable": False,
        "automatic_ordering": False,
        "action_authority": "none",
        "direction": {"decision": "up", "composite_score": 0.67},
    }


def _prior_session() -> dict[str, object]:
    return {
        "status": "ready",
        "session_date": "2026-07-29",
        "return_fraction": -0.0152,
        "close_location_fraction": 0.02,
        "tail_return_fraction": -0.004,
        "shock_direction": "down",
        "close_zone": "lower",
        "path_class": "shock_down_close_low",
    }
