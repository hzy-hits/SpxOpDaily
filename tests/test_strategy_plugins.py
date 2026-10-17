from __future__ import annotations

import json
from datetime import datetime, timezone

import pytest

from spx_spark.application.order_map.strategy_plugins import (
    StrategyRegistry,
    run_shadow_strategies,
)
from spx_spark.domain.strategy import GateResult, StrategyCandidate


class _EchoStrategy:
    strategy_id = "echo"
    version = "1"

    def evaluate(self, facts):
        return (
            StrategyCandidate(
                strategy_id=self.strategy_id,
                version=self.version,
                structure="CALL_DEBIT_VERTICAL",
                direction="UP",
                evidence={"session_date": facts.get("session_date")},
            ),
        )

    def gates(self, candidate, facts):
        return (GateResult("shadow_gate", True),)

    def manage(self, candidate):
        return {}

    def explain(self, candidate):
        return {}


def test_shadow_registry_records_candidates_without_authority(tmp_path) -> None:
    registry = StrategyRegistry()
    registry.register(_EchoStrategy())
    with pytest.raises(ValueError):
        registry.register(_EchoStrategy())

    candidates = run_shadow_strategies(
        tmp_path,
        {"session_date": "2026-10-16"},
        now=datetime(2026, 10, 16, 14, 0, tzinfo=timezone.utc),
        registry=registry,
    )
    assert candidates[0]["shadow_only"] is True
    assert candidates[0]["authority"] == "none"
    assert candidates[0]["gates"] == [
        {"gate": "shadow_gate", "passed": True, "reason": None}
    ]

    payload = json.loads((tmp_path / "latest" / "strategy_shadow.json").read_text())
    assert payload["authority"] == "none"
    assert payload["candidates"][0]["strategy_id"] == "echo"
