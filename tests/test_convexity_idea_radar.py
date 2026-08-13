from datetime import datetime, timezone

from spx_spark.application.order_map.convexity_idea_presentation import (
    compact_convexity_idea_radar,
    render_convexity_idea_radar_lines,
)
from spx_spark.application.order_map.convexity_idea_radar import (
    _hypothesis,
    build_convexity_idea_radar,
)
from spx_spark.notifier import llm_writer


NOW = datetime(2026, 8, 6, 15, 0, tzinfo=timezone.utc)


def _payload() -> dict[str, object]:
    return {
        "underlier": {"price": 7710.0, "source": "index:SPX"},
        "level_decision": {
            "levels": {"put_wall": 7690.0, "flip_low": 7700.0,
                       "flip_high": 7715.0, "call_wall": 7730.0},
        },
        "option_structure_frame": {
            "as_of": NOW.isoformat(), "quality": "ready",
            "structure": {"put_wall": 7690.0, "flip_zone": [7700.0, 7715.0],
                          "call_wall": 7730.0},
            "density": {"p10": 7680.0, "median": 7710.0, "p90": 7740.0},
            "volatility": {}, "l1": {"quality": "ready"},
        },
        "candidates": [],
    }


def _strategy_decision() -> dict[str, object]:
    return {
        "decision_id": "strategy:idea-memo",
        "decision_at": NOW.isoformat(),
        "action_authority": "manual",
        "probability_evidence": {"q": 0.52, "n_raw": 12, "n_effective": 7.0},
        "candidate": {
            "opportunity_id": "strategy-opportunity:test",
            "setup_kind": "TREND_PULLBACK",
            "direction": "UP",
            "trigger_level": 7705.0,
            "target_spx": 7730.0,
            "invalidation_spx": 7705.0,
            "opportunity_valid_until": NOW.isoformat(),
            "long": {"contract_id": "option:SPX:SPXW:20260806:7710:C"},
            "short": {"contract_id": "option:SPX:SPXW:20260806:7720:C"},
            "quote": {"bid": 2.8, "ask": 3.0},
            "utility": {"event_probability": 0.61, "utility": 0.12},
        },
    }


def test_radar_has_competing_hypotheses_and_no_fixed_opportunity_board() -> None:
    radar = build_convexity_idea_radar(_payload(), now=NOW)
    assert radar["mode"] == "deterministic_competing_hypotheses"
    assert "opportunity_board" not in radar
    assert len(radar["hypotheses"]) == 4
    assert all(row["action_authority"] == "none" for row in radar["hypotheses"])


def test_supporting_facts_are_bound_to_existing_fact_references() -> None:
    row = _hypothesis(
        scenario="lower_rejection_call",
        boundary={"status": "available", "side": "lower", "name": "put_wall", "level": 7690.0},
        right="C", direction="up", required_path="REJECTED→RETEST→CONFIRMED",
        falsifier="lower_boundary_accepted_below",
        option_evidence={"edge_status": "not_observed"}, idea_generation_allowed=True,
    )
    assert row["supporting_facts"] == [{"ref": "boundary_tests.lower.level", "value": 7690.0}]
    assert row["falsifiers"] == ["lower_boundary_accepted_below"]
    assert row["origin"] == "deterministic_fallback"


def test_compact_and_render_views_cannot_restore_retired_ranking() -> None:
    radar = build_convexity_idea_radar(_payload(), now=NOW)
    compact = compact_convexity_idea_radar(radar)
    assert compact is not None and "opportunity_board" not in compact
    lines = render_convexity_idea_radar_lines({"convexity_idea_radar": radar})
    assert lines and all("PRIORITY=" not in line for line in lines)


def test_llm_critic_accepts_only_existing_supporting_fact_refs(monkeypatch) -> None:
    radar = build_convexity_idea_radar(_payload(), now=NOW)
    deterministic = radar["hypotheses"][0]
    reference = deterministic["supporting_facts"][0]["ref"]
    response = {"hypotheses": [{"kind": "lower_rejection_call",
                                 "supporting_fact_refs": [reference],
                                 "contradictions": [],
                                 "falsifiers": deterministic["falsifiers"],
                                 "eligible_expressions": ["vertical", "no_trade"]}]}
    monkeypatch.setattr(llm_writer, "call_llm_writer",
                        lambda *args, **kwargs: (__import__("json").dumps(response), None))
    critic, error = llm_writer.call_hypothesis_critic(radar)
    assert error is None and critic == response

    response["hypotheses"][0]["supporting_fact_refs"] = ["invented.price"]
    critic, error = llm_writer.call_hypothesis_critic(radar)
    assert critic is None and error == "hypothesis_fact_or_expression_validation_failed"

    response["hypotheses"][0]["supporting_fact_refs"] = [reference]
    response["hypotheses"][0]["falsifiers"] = ["invented_falsifier"]
    critic, error = llm_writer.call_hypothesis_critic(radar)
    assert critic is None and error == "hypothesis_fact_or_expression_validation_failed"


def test_llm_writer_uses_openai_compatible_json_mode(monkeypatch) -> None:
    calls: dict[str, object] = {}

    class Completions:
        def create(self, **kwargs):
            calls["create"] = kwargs
            message = type("Message", (), {"content": '{"hypotheses":[]}'})()
            choice = type("Choice", (), {"message": message})()
            return type("Response", (), {"choices": [choice]})()

    class Client:
        def __init__(self, **kwargs):
            calls["client"] = kwargs
            self.chat = type("Chat", (), {"completions": Completions()})()

    monkeypatch.setenv("DEEPSEEK_API_KEY", "test-key")
    monkeypatch.setattr(llm_writer, "OpenAI", Client)
    settings = llm_writer.LlmWriterSettings(
        enabled=True,
        model="deepseek-v4-flash",
        url="https://api.deepseek.com/v1/chat/completions",
        env_file="",
        timeout_seconds=12.0,
        max_tokens=4096,
        provider_order=("deepseek",),
    )

    text, error = llm_writer.call_llm_writer(
        "json input", settings=settings, json_mode=True
    )

    assert error is None and text == '{"hypotheses":[]}'
    assert calls["client"] == {
        "api_key": "test-key",
        "base_url": "https://api.deepseek.com",
        "timeout": 12.0,
    }
    assert calls["create"]["response_format"] == {"type": "json_object"}


def test_strategy_idea_memo_accepts_payload_fact_references_only(monkeypatch) -> None:
    calls: dict[str, object] = {}
    memo = {
        "thesis": "Focus on 7705.0 reclaim before leaning toward 7730.0.",
        "falsification": ["Lose 7705.0 after refresh."],
        "watch_levels": [7705.0, 7730.0],
        "risks": ["Synthetic BBO can widen near option:SPX:SPXW:20260806:7710:C."],
    }
    settings = llm_writer.LlmWriterSettings(
        enabled=True,
        model="deepseek-v4-flash",
        url="https://api.deepseek.com/v1/chat/completions",
        env_file="",
        timeout_seconds=12.0,
        max_tokens=4096,
        provider_order=("deepseek",),
    )

    def fake_call(prompt, *, system, settings, json_mode):
        calls["prompt"] = prompt
        calls["system"] = system
        calls["timeout_seconds"] = settings.timeout_seconds
        calls["json_mode"] = json_mode
        return (__import__("json").dumps(memo), None)

    monkeypatch.setattr(llm_writer, "call_llm_writer", fake_call)
    result, error = llm_writer.call_strategy_idea_memo(_strategy_decision(), settings=settings)

    assert error is None and result == memo
    assert calls["timeout_seconds"] == 12.0
    assert calls["json_mode"] is True
    assert "不得编造合约、价格或概率" in str(calls["system"])
    assert "market_facts" not in str(calls["prompt"])


def test_strategy_idea_memo_rejects_watch_levels_outside_payload(monkeypatch) -> None:
    memo = {
        "thesis": "Watch 7705.0 first.",
        "falsification": ["Lose 7705.0 after refresh."],
        "watch_levels": [9999.0],
        "risks": ["Synthetic BBO can widen."],
    }
    monkeypatch.setattr(
        llm_writer,
        "call_llm_writer",
        lambda *args, **kwargs: (__import__("json").dumps(memo), None),
    )

    result, error = llm_writer.call_strategy_idea_memo(_strategy_decision())

    assert result is None
    assert error == "strategy_idea_memo_validation_failed"


def test_strategy_idea_memo_rejects_banned_terms(monkeypatch) -> None:
    memo = {
        "thesis": "Use a market order at 7705.0.",
        "falsification": ["Lose 7705.0 after refresh."],
        "watch_levels": [7705.0],
        "risks": ["Synthetic BBO can widen."],
    }
    monkeypatch.setattr(
        llm_writer,
        "call_llm_writer",
        lambda *args, **kwargs: (__import__("json").dumps(memo), None),
    )

    result, error = llm_writer.call_strategy_idea_memo(_strategy_decision())

    assert result is None
    assert error == "strategy_idea_memo_validation_failed"


def test_strategy_idea_memo_rejects_overlong_thesis_and_falsification(monkeypatch) -> None:
    memo = {
        "thesis": "A" * 601,
        "falsification": [],
        "watch_levels": [7705.0],
        "risks": ["Synthetic BBO can widen."],
    }
    monkeypatch.setattr(
        llm_writer,
        "call_llm_writer",
        lambda *args, **kwargs: (__import__("json").dumps(memo), None),
    )

    result, error = llm_writer.call_strategy_idea_memo(_strategy_decision())

    assert result is None
    assert error == "strategy_idea_memo_validation_failed"


def test_strategy_idea_memo_rejects_non_json(monkeypatch) -> None:
    monkeypatch.setattr(llm_writer, "call_llm_writer", lambda *args, **kwargs: ("not-json", None))

    result, error = llm_writer.call_strategy_idea_memo(_strategy_decision())

    assert result is None
    assert error is not None and error.startswith("invalid_strategy_idea_memo_json:")


def test_strategy_idea_memo_rejects_banned_term_variants(monkeypatch) -> None:
    memo = {
        "thesis": "Prefer a market-order fill at 7705.0.",
        "falsification": ["Lose 7705.0 after refresh."],
        "watch_levels": [7705.0],
        "risks": ["Synthetic BBO can widen."],
    }
    monkeypatch.setattr(
        llm_writer,
        "call_llm_writer",
        lambda *args, **kwargs: (__import__("json").dumps(memo), None),
    )
    result, error = llm_writer.call_strategy_idea_memo(_strategy_decision())
    assert result is None
    assert error == "strategy_idea_memo_validation_failed"


def test_strategy_idea_memo_rejects_foreign_ticker(monkeypatch) -> None:
    memo = {
        "thesis": "Rotate into SPY near 7705.0.",
        "falsification": ["Lose 7705.0 after refresh."],
        "watch_levels": [7705.0],
        "risks": ["Synthetic BBO can widen."],
    }
    monkeypatch.setattr(
        llm_writer,
        "call_llm_writer",
        lambda *args, **kwargs: (__import__("json").dumps(memo), None),
    )
    result, error = llm_writer.call_strategy_idea_memo(_strategy_decision())
    assert result is None
    assert error == "strategy_idea_memo_validation_failed"
