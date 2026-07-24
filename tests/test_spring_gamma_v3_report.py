from __future__ import annotations

import json
from copy import deepcopy
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

from spx_spark.application.order_map.guidance import (
    STATUS_BRIEF_SYSTEM_PROMPT,
    build_decision_guidance,
)
from spx_spark.application.order_map.pricing_audit import build_pricing_audit_record
from spx_spark.application.order_map.prompts import (
    _status_writer_payload,
    render_status_template,
)
from spx_spark.application.order_map.service import _status_fingerprint
from spx_spark.application.order_map.spring_gamma_presentation import (
    SPRING_GAMMA_V3_SHADOW_SYSTEM_RULE,
    render_research_only_template,
)
from spx_spark.application.order_map.spring_gamma_projection import (
    attach_spring_gamma_v3_shadow,
)


NOW = datetime(2026, 7, 24, 14, 15, tzinfo=timezone.utc)


def _attach(
    payload: dict[str, object],
    data_root,
    *,
    report_enabled: bool,
) -> None:
    attach_spring_gamma_v3_shadow(
        payload,
        data_root,
        settings=SimpleNamespace(
            report_enabled=report_enabled,
            prediction_interval_seconds=60,
        ),
        now=NOW,
    )


def _shadow(
    *,
    status: str = "ready",
    decision: str = "up",
    score: float = 0.6666,
    wall_probability: float | None = 0.23456,
) -> dict[str, object]:
    payload: dict[str, object] = {
        "schema_version": "spring_gamma_v3_shadow.v1",
        "model_version": "spring_gamma_v3_es_only_shadow.v1",
        "prediction_id": f"prediction-{status}-{decision}",
        "input_fingerprint": f"input-{status}-{decision}",
        "as_of": NOW.isoformat(),
        "session_id": "2026-07-24",
        "session": "rth",
        "expiry": "20260724",
        "trading_date": "2026-07-24",
        "status": status,
        "mode": "shadow",
        "direction_authority": "none",
        "action_authority": "none",
        "actionable": False,
        "automatic_ordering": False,
        "calibration_status": "uncalibrated_shadow",
        "direction": {
            "decision": decision,
            "diagnostic_es_direction": decision,
            "composite_score": score,
            "p_up": 0.71234,
            "p_down": 0.28766,
        },
        "abstain": status == "abstain",
        "abstain_reasons": (
            ["greek_frame_stale", "pair_ratio_below_minimum"]
            if status == "abstain"
            else []
        ),
    }
    if wall_probability is not None:
        payload["wall_probability"] = wall_probability
    return payload


def _production_payload() -> dict[str, object]:
    return {
        "expiry": "20260724",
        "underlier": {"price": 7558.0, "source": "index:SPX"},
        "es_last": 7603.0,
        "expected_move_points": 35.0,
        "flip_zone": [7560.0, 7565.0],
        "gamma_state": "zero_gamma_transition",
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
        "session_phase": {"name": "us_open_hour", "name_cn": "美盘开盘首小时"},
        "warnings": [],
    }


def test_shadow_loader_is_report_flagged_and_fail_closed(tmp_path) -> None:
    latest = tmp_path / "latest"
    latest.mkdir()
    shadow = _shadow()
    (latest / "spring_gamma_v3_shadow.json").write_text(
        json.dumps(shadow),
        encoding="utf-8",
    )

    report_identity: dict[str, object] = {
        "expiry": "20260724",
        "trading_date": "2026-07-24",
        "minute_market_frame": {
            "session_id": "2026-07-24",
            "diagnostics": {"segment": "rth"},
        },
    }
    disabled = {
        **report_identity,
        "spring_gamma_v3_shadow": {"status": "stale"},
    }
    _attach(
        disabled,
        tmp_path,
        report_enabled=False,
    )
    assert "spring_gamma_v3_shadow" not in disabled

    enabled = dict(report_identity)
    _attach(
        enabled,
        tmp_path,
        report_enabled=True,
    )
    assert enabled["spring_gamma_v3_shadow"] == shadow

    invalid = {**shadow, "direction_authority": "production"}
    (latest / "spring_gamma_v3_shadow.json").write_text(
        json.dumps(invalid),
        encoding="utf-8",
    )
    rejected = dict(report_identity)
    _attach(
        rejected,
        tmp_path,
        report_enabled=True,
    )
    assert "spring_gamma_v3_shadow" not in rejected

    for stale_or_crossed in (
        {**shadow, "expiry": "20260727"},
        {**shadow, "session_id": "2026-07-23"},
        {**shadow, "session": "gth"},
        {**shadow, "as_of": (NOW - timedelta(seconds=121)).isoformat()},
        {**shadow, "as_of": (NOW + timedelta(microseconds=1)).isoformat()},
    ):
        (latest / "spring_gamma_v3_shadow.json").write_text(
            json.dumps(stale_or_crossed),
            encoding="utf-8",
        )
        skipped = dict(report_identity)
        _attach(
            skipped,
            tmp_path,
            report_enabled=True,
        )
        assert "spring_gamma_v3_shadow" not in skipped

    (latest / "spring_gamma_v3_shadow.json").write_text(
        json.dumps(shadow),
        encoding="utf-8",
    )
    unknown_segment = {
        **report_identity,
        "minute_market_frame": {
            "session_id": "2026-07-24",
            "diagnostics": {"segment": "maintenance"},
        },
    }
    _attach(
        unknown_segment,
        tmp_path,
        report_enabled=True,
    )
    assert "spring_gamma_v3_shadow" not in unknown_segment


def test_ready_and_abstain_shadow_lines_are_deterministic_and_two_decimal() -> None:
    ready = _production_payload()
    ready["spring_gamma_v3_shadow"] = _shadow()

    rth = render_status_template(ready, [], NOW)
    expected = (
        "Spring Gamma v3 Shadow  READY · 方向诊断 偏多 · 方向分数 0.67 · "
        "墙触达概率 0.23；方向分数未校准；墙触达概率为风险中性启发式；"
        "无方向/执行权限"
    )
    assert rth.count("Spring Gamma v3 Shadow") == 1
    assert expected in rth
    assert "0.6666" not in rth
    assert "0.23456" not in rth

    research = {
        **ready,
        "research_only": True,
        "beijing_time": "22:15",
        "research_reference": {"price": 7603.0, "source": "future:ES"},
        "pricing_reference": {"gate_state": "missing"},
        "spring_gamma_v3_shadow": _shadow(
            status="abstain",
            decision="abstain",
            score=-0.1251,
            wall_probability=None,
        ),
    }
    gth = render_research_only_template(research)
    assert gth.count("Spring Gamma v3 Shadow") == 1
    assert (
        "Spring Gamma v3 Shadow  ABSTAIN · 方向诊断 弃权 · 方向分数 -0.13 · "
        "首要原因 greek_frame_stale；方向分数未校准；"
        "墙触达概率为风险中性启发式；无方向/执行权限"
    ) in gth
    assert gth.index("Spring Gamma v3 Shadow") < gth.index("执行限制:")


def test_nested_wall_probability_selects_nearest_directional_target() -> None:
    payload = _production_payload()
    shadow = _shadow(wall_probability=None)
    shadow["wall_probability"] = {
        "path": {"underlier": 7558.0},
        "stable_levels": {
            "put_wall": 7550.0,
            "flip_high": 7565.0,
            "call_wall": 7600.0,
        },
        "wall_probabilities": {
            "30m": {
                "flip_high": {
                    "status": "available",
                    "level": 7565.0,
                    "touch_probability_2x_reflection": 0.56789,
                }
            },
            "15m": {
                "put_wall": {
                    "status": "available",
                    "level": 7550.0,
                    "touch_probability_2x_reflection": 0.98765,
                },
                "flip_high": {
                    "status": "available",
                    "level": 7565.0,
                    "touch_probability_2x_reflection": 0.45678,
                },
                "call_wall": {
                    "status": "available",
                    "level": 7600.0,
                    "touch_probability_2x_reflection": 0.34567,
                },
            },
        },
    }
    payload["spring_gamma_v3_shadow"] = shadow

    rendered = render_status_template(payload, [], NOW)

    assert "墙触达概率 0.46（15m Flip High）" in rendered
    assert "0.98765" not in rendered
    assert "0.45678" not in rendered


def test_gth_abstain_uses_partial_wall_contract_upstream_direction_only() -> None:
    payload = _production_payload()
    shadow = _shadow(
        status="abstain",
        decision="abstain",
        wall_probability=None,
    )
    shadow["direction"]["diagnostic_es_direction"] = "up"
    shadow["wall_probability"] = {
        "status": "abstain",
        "probability_status": "partial",
        "direction": "up",
        "path": {"underlier": 7558.0},
        "stable_levels": {
            "put_wall": 7550.0,
            "flip_high": 7565.0,
            "call_wall": 7600.0,
        },
        "wall_probabilities": {
            "15m": {
                "put_wall": {
                    "status": "available",
                    "level": 7550.0,
                    "touch_probability_2x_reflection": 0.98765,
                },
                "flip_high": {
                    "status": "available",
                    "level": 7565.0,
                    "touch_probability_2x_reflection": 0.45678,
                },
            }
        },
    }
    payload["spring_gamma_v3_shadow"] = shadow

    rendered = render_status_template(payload, [], NOW)
    compact = _status_writer_payload(payload)["spring_gamma_v3_shadow"]

    assert "Shadow  ABSTAIN · 方向诊断 弃权" in rendered
    assert "原始 ES 诊断 偏多（仅诊断）" in rendered
    assert "墙触达概率 0.46（15m Flip High）" in rendered
    assert compact["direction"]["decision"] == "abstain"
    assert compact["direction"]["diagnostic_es_direction"] == "up"
    assert compact["wall_probability"] == 0.46
    assert compact["direction_authority"] == "none"
    assert compact["action_authority"] == "none"

    shadow["wall_probability"]["probability_status"] = "unavailable"
    unavailable = render_status_template(payload, [], NOW)
    unavailable_compact = _status_writer_payload(payload)["spring_gamma_v3_shadow"]
    assert "墙触达概率 0.46" not in unavailable
    assert "wall_probability" not in unavailable_compact


def test_opposite_shadow_cannot_change_production_guidance_or_fingerprint() -> None:
    bullish = _production_payload()
    bearish = deepcopy(bullish)
    bullish["spring_gamma_v3_shadow"] = _shadow(decision="up", score=0.91)
    bearish["spring_gamma_v3_shadow"] = _shadow(decision="down", score=-0.91)

    assert build_decision_guidance(bullish) == build_decision_guidance(bearish)
    assert _status_fingerprint(bullish) == _status_fingerprint(bearish)
    assert bullish["candidates"] == bearish["candidates"]
    assert bullish["plan_candidates"] == bearish["plan_candidates"]


def test_writer_and_pricing_audit_keep_bounded_non_authoritative_shadow() -> None:
    payload = _production_payload()
    shadow = _shadow()
    shadow["large_diagnostic_blob"] = {"rows": list(range(1000))}
    shadow["abstain_reasons"] = [f"reason_{index}" for index in range(10)]
    payload["spring_gamma_v3_shadow"] = shadow

    compact = _status_writer_payload(payload)["spring_gamma_v3_shadow"]
    assert compact["direction"]["composite_score"] == 0.67
    assert compact["direction"]["p_up"] == 0.71
    assert compact["wall_probability"] == 0.23
    assert compact["direction_authority"] == "none"
    assert compact["action_authority"] == "none"
    assert compact["actionable"] is False
    assert compact["automatic_ordering"] is False
    assert compact["abstain_reasons"] == [f"reason_{index}" for index in range(5)]
    assert "large_diagnostic_blob" not in compact
    assert "不得据此修改生产 guidance、候选、裁决、限价或下单动作" in (
        SPRING_GAMMA_V3_SHADOW_SYSTEM_RULE
    )
    assert "不得据此修改生产 guidance、候选、裁决、限价或下单动作" in (
        STATUS_BRIEF_SYSTEM_PROMPT
    )

    audit = build_pricing_audit_record(
        payload,
        generated_at=NOW,
        report_kind="status",
        template="template",
        delivered_text="delivered",
        writer="template",
        delivered_ok=True,
    )
    assert audit["spring_gamma_v3_shadow"] == shadow
