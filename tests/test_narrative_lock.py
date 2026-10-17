from __future__ import annotations

from spx_spark.notifier.llm_writer import narrative_numbers_locked


def test_narrative_numbers_must_come_from_the_decision() -> None:
    decision = {"candidate": {"targets": [7500.0], "entry": 3.25}}
    assert narrative_numbers_locked("target 7500, entry 3.25", decision)
    assert not narrative_numbers_locked("target 7510, entry 3.25", decision)
