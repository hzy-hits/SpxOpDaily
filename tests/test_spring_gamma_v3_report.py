from __future__ import annotations

import json
from copy import deepcopy
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from zoneinfo import ZoneInfo

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
    build_spring_gamma_v3_state_window,
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
            ["greek_frame_stale", "pair_ratio_below_minimum"] if status == "abstain" else []
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

    for stale_or_crossed, reason in (
        ({**shadow, "expiry": "20260727"}, "expiry_mismatch"),
        ({**shadow, "session_id": "2026-07-23"}, "session_id_mismatch"),
        ({**shadow, "session": "gth"}, "session_kind_mismatch"),
        (
            {**shadow, "as_of": (NOW - timedelta(seconds=121)).isoformat()},
            "projection_stale",
        ),
        (
            {
                **shadow,
                "as_of": (NOW + timedelta(seconds=5, microseconds=1)).isoformat(),
            },
            "projection_future_beyond_tolerance",
        ),
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
        assert skipped["spring_gamma_v3_projection_diagnostic"]["status"] == "rejected"
        assert skipped["spring_gamma_v3_projection_diagnostic"]["reason"] == reason

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


def test_future_clock_tolerance_and_durable_rth_state_window_are_auditable(
    tmp_path,
) -> None:
    latest = tmp_path / "latest"
    latest.mkdir()

    def state_prediction(
        at: datetime,
        state: str,
        prediction_id: str,
    ) -> dict[str, object]:
        prediction = _shadow()
        prediction.update(
            {
                "as_of": at.isoformat(),
                "prediction_id": prediction_id,
                "input_fingerprint": f"input-{prediction_id}",
                "rth_market_state": {
                    "schema_version": "market_state_5m.v1",
                    "rule_version": "market_state_5m_eight_variable_rules.v2",
                    "as_of": at.isoformat(),
                    "state": state,
                    "status": "ready" if state != "UNCERTAIN" else "uncertain",
                    "D": 8 if state == "TREND_UP" else 0,
                    "Q": {
                        "quality": "high",
                        "efficiency_ratio": 0.72 if state == "TREND_UP" else 0.12,
                        "vwap_cross_count": 0 if state == "TREND_UP" else 3,
                    },
                    "V": {"state": "normal", "same_time_range_ratio": 1.0},
                    "input_availability": {
                        "required_count": 8,
                        "available_count": 8,
                        "complete": True,
                    },
                    "action_authority": "none",
                    "actionable": False,
                },
            }
        )
        return prediction

    predictions = [
        state_prediction(NOW - timedelta(minutes=14), "LOW_VOL_RANGE", "p-1"),
        state_prediction(NOW - timedelta(minutes=8), "LOW_VOL_RANGE", "p-2"),
        state_prediction(NOW - timedelta(minutes=4), "TREND_UP", "p-3"),
    ]
    future = state_prediction(
        NOW + timedelta(seconds=2, milliseconds=750),
        "TREND_UP",
        "p-4",
    )
    raw = tmp_path / "features" / "spring_gamma_v3" / "date=2026-07-24" / "predictions.jsonl"
    raw.parent.mkdir(parents=True)
    raw.write_text(
        "".join(json.dumps(row) + "\n" for row in predictions),
        encoding="utf-8",
    )
    (latest / "spring_gamma_v3_shadow.json").write_text(
        json.dumps(future),
        encoding="utf-8",
    )
    payload: dict[str, object] = {
        "expiry": "20260724",
        "trading_date": "2026-07-24",
        "minute_market_frame": {
            "session_id": "2026-07-24",
            "diagnostics": {"segment": "rth"},
        },
    }

    _attach(payload, tmp_path, report_enabled=True)

    assert payload["spring_gamma_v3_shadow"] == future
    assert payload["spring_gamma_v3_projection_diagnostic"]["status"] == "attached"
    assert (
        payload["spring_gamma_v3_projection_diagnostic"]["reason"]
        == "projection_future_within_tolerance"
    )
    window = payload["spring_gamma_v3_state_window"]
    assert window == {
        "schema_version": "spring_gamma_v3_state_window.v1",
        "session_id": "2026-07-24",
        "session": "rth",
        "expiry": "20260724",
        "window_start": (NOW - timedelta(minutes=15)).isoformat(),
        "window_end": NOW.isoformat(),
        "window_minutes": 15,
        "sample_count": 4,
        "states": ["TREND_UP", "LOW_VOL_RANGE"],
        "counts": {"TREND_UP": 2, "LOW_VOL_RANGE": 2},
        "five_minute_slot_count": 4,
        "five_minute_slot_counts": {"TREND_UP": 2, "LOW_VOL_RANGE": 2},
        "latest_state": "TREND_UP",
        "latest_state_as_of": future["as_of"],
        "future_tolerance_seconds": 5.0,
        "max_future_skew_seconds": 2.75,
        "source": "durable_spring_gamma_v3_predictions",
        "action_authority": "none",
        "actionable": False,
    }

    rendered = render_status_template(payload, [], NOW)
    assert (
        "RTH状态15m  TREND_UP 2样本/2档 · LOW_VOL_RANGE 2样本/2档 · "
        "最新 TREND_UP · 覆盖 4样本/4档　只读"
    ) in rendered
    audit = build_pricing_audit_record(
        payload,
        generated_at=NOW,
        report_kind="status",
        template=rendered,
        delivered_text=rendered,
        writer="template",
        delivered_ok=True,
    )
    assert audit["spring_gamma_v3_state_window"] == window
    assert audit["spring_gamma_v3_projection_diagnostic"]["reason"] == (
        "projection_future_within_tolerance"
    )


def test_report_uses_latest_durable_causal_shadow_when_worker_clock_advances(
    tmp_path,
) -> None:
    latest = tmp_path / "latest"
    latest.mkdir()
    causal = {
        **_shadow(),
        "as_of": (NOW - timedelta(seconds=12)).isoformat(),
        "prediction_id": "causal-before-report-clock",
        "input_fingerprint": "causal-before-report-clock",
    }
    future = {
        **_shadow(),
        "as_of": (NOW + timedelta(seconds=45)).isoformat(),
        "prediction_id": "worker-advanced-after-report-start",
        "input_fingerprint": "worker-advanced-after-report-start",
    }
    raw = tmp_path / "features" / "spring_gamma_v3" / "date=2026-07-24" / "predictions.jsonl"
    raw.parent.mkdir(parents=True)
    raw.write_text(json.dumps(causal) + "\n", encoding="utf-8")
    (latest / "spring_gamma_v3_shadow.json").write_text(
        json.dumps(future),
        encoding="utf-8",
    )
    payload: dict[str, object] = {
        "expiry": "20260724",
        "trading_date": "2026-07-24",
        "minute_market_frame": {
            "session_id": "2026-07-24",
            "diagnostics": {"segment": "rth"},
        },
    }

    _attach(payload, tmp_path, report_enabled=True)

    assert payload["spring_gamma_v3_shadow"] == causal
    diagnostic = payload["spring_gamma_v3_projection_diagnostic"]
    assert diagnostic["status"] == "attached"
    assert diagnostic["reason"] == "projection_durable_causal_fallback"
    assert diagnostic["selection_source"] == "durable_causal_fallback"
    assert diagnostic["shadow_as_of"] == causal["as_of"]
    assert diagnostic["latest_shadow_as_of"] == future["as_of"]
    assert diagnostic["age_seconds"] == 12.0


def test_durable_causal_shadow_never_selects_archive_row_after_report_clock(
    tmp_path,
) -> None:
    latest = tmp_path / "latest"
    latest.mkdir()
    causal = {
        **_shadow(),
        "as_of": (NOW - timedelta(seconds=12)).isoformat(),
        "prediction_id": "causal-before-report-clock",
        "input_fingerprint": "causal-before-report-clock",
    }
    future_archive = {
        **_shadow(),
        "as_of": (NOW + timedelta(seconds=2)).isoformat(),
        "prediction_id": "future-within-general-clock-tolerance",
        "input_fingerprint": "future-within-general-clock-tolerance",
    }
    future_latest = {
        **_shadow(),
        "as_of": (NOW + timedelta(seconds=45)).isoformat(),
        "prediction_id": "worker-advanced-after-report-start",
        "input_fingerprint": "worker-advanced-after-report-start",
    }
    raw = tmp_path / "features" / "spring_gamma_v3" / "date=2026-07-24" / "predictions.jsonl"
    raw.parent.mkdir(parents=True)
    raw.write_text(
        json.dumps(causal) + "\n" + json.dumps(future_archive) + "\n",
        encoding="utf-8",
    )
    (latest / "spring_gamma_v3_shadow.json").write_text(
        json.dumps(future_latest),
        encoding="utf-8",
    )
    payload: dict[str, object] = {
        "expiry": "20260724",
        "trading_date": "2026-07-24",
        "minute_market_frame": {
            "session_id": "2026-07-24",
            "diagnostics": {"segment": "rth"},
        },
    }

    _attach(payload, tmp_path, report_enabled=True)

    assert payload["spring_gamma_v3_shadow"] == causal
    diagnostic = payload["spring_gamma_v3_projection_diagnostic"]
    assert diagnostic["selection_source"] == "durable_causal_fallback"
    assert diagnostic["shadow_as_of"] == causal["as_of"]


def test_future_latest_still_rejects_when_no_identity_matched_causal_shadow(
    tmp_path,
) -> None:
    latest = tmp_path / "latest"
    latest.mkdir()
    wrong_expiry = {
        **_shadow(),
        "as_of": (NOW - timedelta(seconds=12)).isoformat(),
        "expiry": "20260727",
        "prediction_id": "wrong-expiry",
        "input_fingerprint": "wrong-expiry",
    }
    future = {
        **_shadow(),
        "as_of": (NOW + timedelta(seconds=45)).isoformat(),
        "prediction_id": "worker-advanced-after-report-start",
        "input_fingerprint": "worker-advanced-after-report-start",
    }
    raw = tmp_path / "features" / "spring_gamma_v3" / "date=2026-07-24" / "predictions.jsonl"
    raw.parent.mkdir(parents=True)
    raw.write_text(json.dumps(wrong_expiry) + "\n", encoding="utf-8")
    (latest / "spring_gamma_v3_shadow.json").write_text(
        json.dumps(future),
        encoding="utf-8",
    )
    payload: dict[str, object] = {
        "expiry": "20260724",
        "trading_date": "2026-07-24",
        "minute_market_frame": {
            "session_id": "2026-07-24",
            "diagnostics": {"segment": "rth"},
        },
    }

    _attach(payload, tmp_path, report_enabled=True)

    assert "spring_gamma_v3_shadow" not in payload
    diagnostic = payload["spring_gamma_v3_projection_diagnostic"]
    assert diagnostic["status"] == "rejected"
    assert diagnostic["reason"] == "projection_future_beyond_tolerance"


def test_current_shadow_retains_recent_causal_path_as_read_only_fallback(
    tmp_path,
) -> None:
    latest = tmp_path / "latest"
    latest.mkdir()
    prior = {
        **_shadow(),
        "as_of": (NOW - timedelta(minutes=4)).isoformat(),
        "prediction_id": "prior-path",
        "input_fingerprint": "prior-path",
        "rth_market_state": {
            "schema_version": "market_state_5m.v1",
            "input_lineage": {
                "diagnostics": {
                    "rolling_path_percentiles": {
                        "status": "provisional",
                        "confidence": "medium",
                        "input_quality": "degraded",
                        "sample_count": 13,
                        "latest_bar_end": (NOW - timedelta(minutes=5)).isoformat(),
                        "dip": {"shrunk_percentile": 0.76},
                        "rally": {"shrunk_percentile": 0.29},
                        "signed_path_bias": -0.47,
                        "action_authority": "none",
                    }
                }
            },
            "action_authority": "none",
            "actionable": False,
        },
    }
    current = {
        **_shadow(status="abstain", decision="abstain"),
        "as_of": NOW.isoformat(),
        "prediction_id": "current-path-unavailable",
        "input_fingerprint": "current-path-unavailable",
        "rth_market_state": {
            "schema_version": "market_state_5m.v1",
            "input_lineage": {
                "diagnostics": {
                    "rolling_path_percentiles": {
                        "status": "warming",
                        "confidence": "unavailable",
                        "sample_count": 0,
                        "reason": "rolling_path_requires_six_contiguous_observed_bars",
                        "action_authority": "none",
                    }
                }
            },
            "action_authority": "none",
            "actionable": False,
        },
    }
    raw = tmp_path / "features" / "spring_gamma_v3" / "date=2026-07-24" / "predictions.jsonl"
    raw.parent.mkdir(parents=True)
    raw.write_text(json.dumps(prior) + "\n", encoding="utf-8")
    (latest / "spring_gamma_v3_shadow.json").write_text(
        json.dumps(current),
        encoding="utf-8",
    )
    payload: dict[str, object] = {
        "expiry": "20260724",
        "trading_date": "2026-07-24",
        "minute_market_frame": {
            "session_id": "2026-07-24",
            "diagnostics": {"segment": "rth"},
        },
    }

    _attach(payload, tmp_path, report_enabled=True)

    assert payload["spring_gamma_v3_shadow"] == current
    fallback = payload["spring_gamma_v3_path_fallback"]
    assert fallback["schema_version"] == "spring_gamma_v3_path_fallback.v1"
    assert fallback["action_authority"] == "none"
    assert fallback["actionable"] is False
    assert fallback["automatic_ordering"] is False
    path = fallback["rolling_path_percentiles"]
    assert path["confidence"] == "low"
    assert path["input_quality"] == "stale_fallback"
    assert path["source_input_quality"] == "degraded"
    assert path["source_shadow_lag_seconds"] == 240.0
    assert path["source_bar_lag_seconds"] == 300.0
    assert path["source_lag_seconds"] == 300.0
    assert path["source_latest_bar_end"] == (NOW - timedelta(minutes=5)).isoformat()
    assert path["signed_path_bias"] == -0.47


def test_path_fallback_rejects_recent_shadow_with_stale_embedded_bar(
    tmp_path,
) -> None:
    latest = tmp_path / "latest"
    latest.mkdir()
    prior = {
        **_shadow(),
        "as_of": (NOW - timedelta(minutes=1)).isoformat(),
        "prediction_id": "recent-shadow-stale-bar",
        "input_fingerprint": "recent-shadow-stale-bar",
        "rth_market_state": {
            "schema_version": "market_state_5m.v1",
            "input_lineage": {
                "diagnostics": {
                    "rolling_path_percentiles": {
                        "status": "provisional",
                        "confidence": "medium",
                        "input_quality": "strict",
                        "sample_count": 13,
                        "latest_bar_end": (NOW - timedelta(minutes=20)).isoformat(),
                        "dip": {"shrunk_percentile": 0.76},
                        "rally": {"shrunk_percentile": 0.29},
                        "action_authority": "none",
                    }
                }
            },
            "action_authority": "none",
            "actionable": False,
        },
    }
    current = {
        **_shadow(status="abstain", decision="abstain"),
        "rth_market_state": {
            "schema_version": "market_state_5m.v1",
            "input_lineage": {
                "diagnostics": {
                    "rolling_path_percentiles": {
                        "status": "warming",
                        "sample_count": 0,
                        "action_authority": "none",
                    }
                }
            },
            "action_authority": "none",
            "actionable": False,
        },
    }
    raw = tmp_path / "features" / "spring_gamma_v3" / "date=2026-07-24" / "predictions.jsonl"
    raw.parent.mkdir(parents=True)
    raw.write_text(json.dumps(prior) + "\n", encoding="utf-8")
    (latest / "spring_gamma_v3_shadow.json").write_text(
        json.dumps(current),
        encoding="utf-8",
    )
    payload: dict[str, object] = {
        "expiry": "20260724",
        "trading_date": "2026-07-24",
        "minute_market_frame": {
            "session_id": "2026-07-24",
            "diagnostics": {"segment": "rth"},
        },
    }

    _attach(payload, tmp_path, report_enabled=True)

    assert "spring_gamma_v3_path_fallback" not in payload


def test_path_fallback_expires_after_fifteen_minutes(tmp_path) -> None:
    latest = tmp_path / "latest"
    latest.mkdir()
    prior = {
        **_shadow(),
        "as_of": (NOW - timedelta(minutes=15, seconds=1)).isoformat(),
        "prediction_id": "stale-prior-path",
        "input_fingerprint": "stale-prior-path",
        "rth_market_state": {
            "schema_version": "market_state_5m.v1",
            "input_lineage": {
                "diagnostics": {
                    "rolling_path_percentiles": {
                        "status": "provisional",
                        "sample_count": 13,
                        "latest_bar_end": (
                            NOW - timedelta(minutes=20)
                        ).isoformat(),
                        "dip": {"shrunk_percentile": 0.76},
                        "rally": {"shrunk_percentile": 0.29},
                        "action_authority": "none",
                    }
                }
            },
            "action_authority": "none",
            "actionable": False,
        },
    }
    current = {
        **_shadow(status="abstain", decision="abstain"),
        "rth_market_state": {
            "schema_version": "market_state_5m.v1",
            "input_lineage": {
                "diagnostics": {
                    "rolling_path_percentiles": {
                        "status": "warming",
                        "sample_count": 0,
                        "action_authority": "none",
                    }
                }
            },
            "action_authority": "none",
            "actionable": False,
        },
    }
    raw = tmp_path / "features" / "spring_gamma_v3" / "date=2026-07-24" / "predictions.jsonl"
    raw.parent.mkdir(parents=True)
    raw.write_text(json.dumps(prior) + "\n", encoding="utf-8")
    (latest / "spring_gamma_v3_shadow.json").write_text(
        json.dumps(current),
        encoding="utf-8",
    )
    payload: dict[str, object] = {
        "expiry": "20260724",
        "trading_date": "2026-07-24",
        "minute_market_frame": {
            "session_id": "2026-07-24",
            "diagnostics": {"segment": "rth"},
        },
    }

    _attach(payload, tmp_path, report_enabled=True)

    assert "spring_gamma_v3_path_fallback" not in payload


def test_state_window_uses_precise_causal_boundary_not_minute_bucket(
    tmp_path,
) -> None:
    et = ZoneInfo("America/New_York")
    report_now = datetime(2026, 7, 24, 10, 45, 8, tzinfo=et)
    window_start = report_now - timedelta(minutes=15)

    def prediction(
        at: datetime,
        state: str,
        prediction_id: str,
    ) -> dict[str, object]:
        row = _shadow()
        row.update(
            {
                "as_of": at.isoformat(),
                "prediction_id": prediction_id,
                "input_fingerprint": f"input-{prediction_id}",
                "rth_market_state": {
                    "schema_version": "market_state_5m.v1",
                    "state": state,
                    "action_authority": "none",
                    "actionable": False,
                },
            }
        )
        return row

    rows = [
        prediction(
            window_start - timedelta(milliseconds=1),
            "UNCERTAIN",
            "before-window",
        ),
        # 10:30:30 ET is causally inside a 10:45:08 report window even though
        # it shares the start minute's bucket.
        prediction(
            window_start + timedelta(seconds=22),
            "LOW_VOL_RANGE",
            "inside-start-minute",
        ),
        # Content fingerprints can remain unchanged across real minute ticks;
        # the as-of clock, not prediction_id alone, distinguishes observations.
        prediction(
            report_now - timedelta(minutes=2),
            "LOW_VOL_RANGE",
            "inside-end",
        ),
        prediction(report_now - timedelta(seconds=1), "TREND_UP", "inside-end"),
        prediction(report_now - timedelta(seconds=1), "TREND_UP", "inside-end"),
        prediction(
            report_now + timedelta(seconds=6),
            "LOW_VOL_RANGE",
            "future-beyond-tolerance",
        ),
    ]
    raw = tmp_path / "features" / "spring_gamma_v3" / "date=2026-07-24" / "predictions.jsonl"
    raw.parent.mkdir(parents=True)
    raw.write_text(
        "".join(json.dumps(row) + "\n" for row in rows),
        encoding="utf-8",
    )
    future_latest = prediction(
        report_now + timedelta(seconds=2),
        "TREND_UP",
        "future-within-tolerance",
    )

    window = build_spring_gamma_v3_state_window(
        tmp_path,
        now=report_now,
        session_id="2026-07-24",
        expiry="20260724",
        latest_candidate=future_latest,
    )

    assert window["window_start"] == window_start.isoformat()
    assert window["window_end"] == report_now.isoformat()
    assert window["sample_count"] == 4
    assert window["counts"] == {"TREND_UP": 2, "LOW_VOL_RANGE": 2}
    assert window["latest_state"] == "TREND_UP"
    assert window["max_future_skew_seconds"] == 2.0


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


def test_rth_eight_feature_state_renders_state_to_expression_path() -> None:
    payload = _production_payload()
    shadow = _shadow()
    shadow["rth_market_state"] = {
        "schema_version": "market_state_5m.v1",
        "rule_version": "market_state_5m_eight_variable_rules.v2",
        "state": "TREND_UP",
        "status": "ready",
        "D": 8,
        "Q": {
            "quality": "high",
            "efficiency_ratio": 0.72,
            "vwap_cross_count": 0,
        },
        "V": {"state": "high", "same_time_range_ratio": 1.35},
        "input_availability": {
            "required_count": 8,
            "available_count": 8,
            "complete": True,
        },
        "input_lineage": {
            "values": {"breadth_above_vwap": 0.68},
            "diagnostics": {
                "moving_averages": {
                    "status": "ready",
                    "timeframe": "5m",
                    "session": "rth",
                    "price": 7603.0,
                    "sma20": 7595.0,
                    "sma50": 7580.0,
                    "sma200": 7550.0,
                    "atr_5m": 10.0,
                    "distance_to_sma20_points": 8.0,
                    "distance_to_sma50_points": 23.0,
                    "distance_to_sma200_points": 53.0,
                    "distance_to_sma50_atr": 2.3,
                    "distance_to_sma200_atr": 5.3,
                    "ma50_slope_3_atr": 0.31,
                    "ma50_slope_6_atr": 0.48,
                    "ma200_slope_3_atr": 0.04,
                    "ma200_slope_6_atr": 0.08,
                    "ma50_ma200_spread_points": 30.0,
                    "ma50_ma200_spread_atr": 3.0,
                    "spread_change_3_atr": 0.27,
                    "cross_direction": "golden",
                    "bars_since_cross": 27,
                    "cross_persistent_2_bars": True,
                    "cross_fresh": False,
                    "regime_state": "TREND_EXTENDED",
                    "regime_direction": "up",
                    "same_direction_convexity": "do_not_chase",
                    "relation": "bullish_stack",
                    "contract_identity": "ES:202609",
                    "spx_equivalent_sma20": 7550.0,
                    "spx_equivalent_sma50": 7535.0,
                    "spx_equivalent_sma200": 7505.0,
                    "spx_projection_near_line": False,
                    "spx_projection_near_line_tolerance_points": 4.25,
                    "projection_method": (
                        "es_sma_minus_synchronized_current_basis_not_cash_spx_sma"
                    ),
                    "action_authority": "none",
                }
            },
        },
        "action_authority": "none",
        "actionable": False,
    }
    shadow["option_overlay"] = {
        "status": "ready",
        "reasons": [],
        "market_state_independent": True,
    }
    payload["spring_gamma_v3_shadow"] = shadow

    rendered = render_status_template(payload, [], NOW)
    compact = _status_writer_payload(payload)["spring_gamma_v3_shadow"]

    assert (
        "RTH状态 Shadow  TREND_UP · D +8.00/10 · ER 0.72 · VWAP穿越 0 · "
        "Range 1.35x · 宽度 68.00% · 数据 8/8　只读"
    ) in rendered
    assert (
        "ES 5m均线  P/MA20/MA50/MA200 7603.00/7595.00/7580.00/7550.00 · "
        "bullish_stack · SPX基差投影 MA20/50/200 7550.00/7535.00/7505.00"
    ) in rendered
    assert "MA50/200 TREND_EXTENDED/up · 同向凸性 do_not_chase" in rendered
    assert "间距 3.00 ATR/3根Δ 0.27" in rendered
    assert "交叉 golden/27根/持续2根是/新鲜否 · 禁止追同向凸性" in rendered
    assert "非自身历史均线；均线不生成方向/入场" in rendered
    assert "状态路径  等待位置：VWAP/ORH与上涨腿回撤区（本层未计算回撤比例）" in rendered
    assert "状态路径  触发确认：仅记录外部level lifecycle确认" in rendered
    assert "状态路径  期权结构：方向映射Call；具体价差仅以独立实时双腿Shadow为准" in rendered
    assert compact["rth_market_state"]["state"] == "TREND_UP"
    assert compact["rth_market_state"]["breadth_above_vwap"] == 0.68
    assert compact["rth_market_state"]["moving_averages"]["sma20"] == 7595.0
    assert compact["rth_market_state"]["moving_averages"]["spx_equivalent_sma50"] == 7535.0
    assert compact["rth_market_state"]["moving_averages"]["sma200"] == 7550.0
    assert (
        compact["rth_market_state"]["moving_averages"]["same_direction_convexity"]
        == "do_not_chase"
    )
    assert compact["option_overlay"]["market_state_independent"] is True
    assert compact["action_authority"] == "none"


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


def test_nested_wall_probability_cannot_cross_1300_hard_exit() -> None:
    payload = _production_payload()
    shadow = _shadow(wall_probability=None)
    shadow["as_of"] = datetime(
        2026,
        7,
        24,
        12,
        59,
        tzinfo=ZoneInfo("America/New_York"),
    ).isoformat()
    shadow["wall_probability"] = {
        "path": {"underlier": 7558.0},
        "stable_levels": {"flip_high": 7565.0},
        "wall_probabilities": {
            horizon: {
                "flip_high": {
                    "status": "available",
                    "level": 7565.0,
                    "touch_probability_2x_reflection": probability,
                }
            }
            for horizon, probability in (
                ("15m", 0.45),
                ("30m", 0.55),
                ("60m", 0.65),
            )
        },
    }
    payload["spring_gamma_v3_shadow"] = shadow

    rendered = render_status_template(
        payload,
        [],
        datetime(2026, 7, 24, 12, 59, tzinfo=ZoneInfo("America/New_York")),
    )
    compact = _status_writer_payload(payload)["spring_gamma_v3_shadow"]

    assert "墙触达概率 0." not in rendered
    assert "wall_probability" not in compact


def test_nested_wall_probability_allows_exit_at_exactly_1300() -> None:
    payload = _production_payload()
    shadow = _shadow(wall_probability=None)
    shadow["as_of"] = datetime(
        2026,
        7,
        24,
        12,
        30,
        tzinfo=ZoneInfo("America/New_York"),
    ).isoformat()
    shadow["wall_probability"] = {
        "path": {"underlier": 7558.0},
        "stable_levels": {"flip_high": 7565.0},
        "wall_probabilities": {
            "30m": {
                "flip_high": {
                    "status": "available",
                    "level": 7565.0,
                    "touch_probability_2x_reflection": 0.46,
                }
            },
            "60m": {
                "flip_high": {
                    "status": "available",
                    "level": 7565.0,
                    "touch_probability_2x_reflection": 0.76,
                }
            },
        },
    }
    payload["spring_gamma_v3_shadow"] = shadow

    rendered = render_status_template(
        payload,
        [],
        datetime(2026, 7, 24, 12, 30, tzinfo=ZoneInfo("America/New_York")),
    )
    compact = _status_writer_payload(payload)["spring_gamma_v3_shadow"]

    assert "墙触达概率 0.46（30m Flip High）" in rendered
    assert compact["wall_probability"] == 0.46
    assert compact["wall_probability_horizon"] == "30m"


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
    assert "不得据此修改生产 guidance、候选、裁决、限价或下单动作" in (STATUS_BRIEF_SYSTEM_PROMPT)

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
