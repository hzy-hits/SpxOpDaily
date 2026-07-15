from __future__ import annotations

from datetime import datetime, timezone

from spx_spark.application.order_map.guidance import (
    GuidanceAction,
    build_decision_guidance,
)
from spx_spark.application.order_map.prompts import (
    render_feishu_delivery_text,
    render_status_template,
)


NOW = datetime(2026, 7, 15, 14, 0, tzinfo=timezone.utc)


def _payload() -> dict[str, object]:
    return {
        "expiry": "20260715",
        "underlier": {"price": 7558.0, "source": "index:SPX"},
        "es_last": 7603.0,
        "flip_zone": [7560.0, 7565.0],
        "regime_decision": {
            "mode": "trending",
            "direction": "down",
            "trend_score": 70.0,
            "mean_reversion_score": 45.0,
        },
        "level_decision": {
            "phase": "far",
            "quality_ok": True,
            "snapshot_consistent": True,
            "levels": {
                "put_wall": 7550.0,
                "flip_low": 7560.0,
                "flip_high": 7565.0,
                "call_wall": 7600.0,
            },
        },
        "trade_intent": {"status": "observing"},
        "plan_candidates": [],
        "candidates": [
            {"play": "put_wall_bounce_call", "level": 7550.0},
            {"play": "call_wall_fade_put", "level": 7600.0},
        ],
        "warnings": [],
    }


def test_guidance_turns_regime_into_directional_wait_conditions() -> None:
    guidance = build_decision_guidance(_payload())

    assert guidance.action is GuidanceAction.WAIT_FOR_TRIGGER
    assert guidance.bias == "趋势偏空"
    assert guidance.action_text == "当前不进场；等待价格进入关键位测试"
    assert "SPX 7560 下方保持" in guidance.trigger_text
    assert "SPX 收回 7565" in guidance.invalidation_text


def test_guidance_translates_joined_quality_failures() -> None:
    payload = _payload()
    payload["level_decision"] = {
        "phase": "far",
        "quality_ok": False,
        "snapshot_consistent": True,
        "quality_reason": "es_not_live;spx_price_unavailable;key_levels_unavailable",
    }

    guidance = build_decision_guidance(payload)

    assert guidance.action is GuidanceAction.PAUSED
    assert "ES 行情不满足实时门槛" in guidance.action_text
    assert "SPX 触发坐标不可用" in guidance.action_text
    assert "Put Wall、Flip 或 Call Wall 不完整" in guidance.action_text


def test_guidance_emits_one_trade_ready_plan() -> None:
    payload = _payload()
    payload["trade_intent"] = {"status": "trade_ready"}
    payload["plan_candidates"] = [
        {
            "strike": 7550.0,
            "right": "P",
            "level": 7560.0,
            "invalidation_spx": 7565.0,
            "target_spx": 7545.0,
        }
    ]

    guidance = build_decision_guidance(payload)

    assert guidance.action is GuidanceAction.TRADE_READY
    assert "SPXW 7550P" in guidance.action_text
    assert guidance.trigger_text == "SPX 7560 已确认触发"
    assert guidance.invalidation_text == "SPX 7565 失效；目标 7545"


def test_status_first_screen_is_guidance_and_far_delivery_stays_compact() -> None:
    payload = _payload()
    rendered = render_status_template(payload, [], NOW)

    assert "判断  趋势偏空（趋势 70 / 回归 45）　未通过执行门控" in rendered
    assert "动作  当前不进场；等待价格进入关键位测试" in rendered
    assert "确认  SPX 7560 下方保持且状态机 CONFIRMED 后才评估 Put" in rendered
    assert "证伪  SPX 收回 7565 且 ES 量价不再同向时，偏空判断取消" in rendered

    delivered = render_feishu_delivery_text(payload, [], NOW, rendered)
    assert delivered == rendered
    assert "## Greeks 与波动" not in delivered
