from spx_spark.application.market_features.greek_decision import build_greek_decision
from spx_spark.settings.market_features import MarketFeatureSettings


def reference(*, status: str = "ok", ratio: float = 0.8) -> dict[str, object]:
    return {
        "status": status,
        "coverage": {"usable_ratio": ratio, "oi_ratio": ratio},
        "aggregate": {"quality": "ok"},
        "contracts": [
            {
                "contract_id": "option:SPX:SPXW:test",
                "delta": 0.50,
                "gamma_per_point": 0.02,
                "speed_gamma_per_point": 0.001,
                "color_gamma_per_minute": -0.0001,
                "theta_per_minute": -0.02,
                "vanna_delta_per_vol_point": 0.01,
                "quality": {"status": "ok"},
                "scenarios": [
                    {"name": "clock_plus_15m", "reference_price": 8.0},
                    {"name": "iv_down_3vol", "reference_price": 5.0},
                ],
            }
        ],
    }


def test_greeks_adjust_contract_confidence_but_have_no_direction_authority() -> None:
    result = build_greek_decision(
        reference(),
        [{"contract_id": "option:SPX:SPXW:test", "current_mid": 10.0}],
        macro_event={"mode": "post_event"},
        policy=MarketFeatureSettings(),
    )
    assert result["mode"] == "decision_grade"
    assert result["direction_authority"] == "none"
    score = result["contract_scores"]["option:SPX:SPXW:test"]
    assert "post_event_vanna_turns_iv_crush_into_delta_drag" in score["reasons"]


def test_greeks_fall_back_to_explanation_when_coverage_is_low() -> None:
    result = build_greek_decision(
        reference(status="degraded", ratio=0.4),
        [{"contract_id": "option:SPX:SPXW:test", "current_mid": 10.0}],
        macro_event={"mode": "normal"},
        policy=MarketFeatureSettings(),
    )
    assert result["mode"] == "explanation_only"
    assert result["contract_scores"]["option:SPX:SPXW:test"]["confidence_adjustment"] == 0
