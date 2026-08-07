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
