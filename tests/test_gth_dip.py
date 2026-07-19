from datetime import datetime, timedelta, timezone

import pytest

from spx_spark.application.shock.gth_dip import (
    _gth_exit_context,
    _signal_alert,
    _spread_structure,
    advance_gth_dip,
    mark_gth_delivery,
)
from spx_spark.application.shock.service import (
    _gth_spread_inputs,
    _gth_trend_entry_quality,
    _virtual_strategy_blocks_gth,
)


NOW = datetime(2026, 7, 14, 3, 0, tzinfo=timezone.utc)


def advance(
    state,
    minute: int,
    es: float,
    *,
    allowed: bool = True,
    seconds: int = 0,
    retry_seconds: int = 30,
    expiry_seconds: int = 600,
    **extra,
):
    extra.setdefault("es_spx_basis", 45.0)
    return advance_gth_dip(
        state,
        session_date="2026-07-14",
        at=NOW + timedelta(minutes=minute, seconds=seconds),
        es=es,
        provider="schwab",
        expected_move_points=80,
        short_horizon_seconds=900,
        long_horizon_seconds=3600,
        short_min_drawdown_points=8,
        long_min_drawdown_points=12,
        short_min_descent_seconds=0,
        long_min_descent_seconds=0,
        expected_move_fraction=0.10,
        reclaim_fraction=0.35,
        min_reclaim_points=4,
        confirm_samples=2,
        confirm_hold_seconds=0,
        session_warmup_seconds=0,
        max_signals_per_session=3,
        cooldown_seconds=900,
        entry_allowed=allowed,
        delivery_retry_seconds=retry_seconds,
        signal_expiry_seconds=expiry_seconds,
        **extra,
    )


def test_slow_es_dip_reclaim_confirms_without_spx() -> None:
    state = None
    for minute, es in ((0, 7560), (5, 7554), (10, 7546), (12, 7551)):
        state, alert, signal = advance(state, minute, es)
    assert alert is None
    state, alert, signal = advance(state, 13, 7552)
    assert alert is not None
    assert alert.kind == "gth_dip_reclaim_call"
    assert alert.title == "SPX 0DTE | CALL RECLAIM (60m)"
    assert "Desk View" in alert.detail
    assert "Execution" in alert.detail
    assert "Risk" in alert.detail
    assert signal["direction"] == "up"
    assert signal["drawdown_points"] == 14
    assert signal["schema_version"] == 3
    assert str(signal["policy_version"]).startswith("gth_dip_reclaim.v3+sha256:")
    assert signal["valid_until"] == (NOW + timedelta(minutes=23)).isoformat()
    assert signal["coordinate"]["kind"] == "raw_es"
    assert signal["coordinate"]["instrument_id"] == "future:ES"
    assert signal["block_reasons"] == []
    assert signal["entry_quality"]["mode"] == "shadow"


def test_macro_pre_event_suppresses_confirmation_but_keeps_observation() -> None:
    state = None
    for minute, es in ((0, 7560), (5, 7554), (10, 7546), (12, 7551), (13, 7552)):
        state, alert, signal = advance(state, minute, es, allowed=False)
    assert alert is None
    assert signal is None
    assert state["status"] == "suppressed_pre_event"
    assert state["pending"] is None


def test_gth_trend_quality_is_shadow_only_and_point_in_time() -> None:
    result = _gth_trend_entry_quality(
        {
            "session_id": "2026-07-14:globex",
            "updated_at": (NOW - timedelta(seconds=30)).isoformat(),
            "regime": "bullish",
            "metrics": {
                "return_15m_points": 3.0,
                "return_60m_points": -4.0,
            },
        },
        session_date="2026-07-14",
        at=NOW,
        max_age_seconds=90.0,
    )
    assert result["mode"] == "shadow"
    assert result["verdict"] == "pass"
    assert result["features"]["return_15m_points"] == 3.0


@pytest.mark.parametrize(
    ("session_id", "updated_at", "regime", "reason"),
    (
        (
            "2026-07-13:globex",
            NOW.isoformat(),
            "bullish",
            "trend_session_mismatch",
        ),
        (
            "2026-07-14:globex",
            (NOW - timedelta(seconds=91)).isoformat(),
            "bullish",
            "trend_context_stale",
        ),
        (
            "2026-07-14:globex",
            NOW.isoformat(),
            "bearish",
            "trend_not_bullish",
        ),
    ),
)
def test_gth_trend_quality_blocks_bad_context_in_shadow(
    session_id: str,
    updated_at: str,
    regime: str,
    reason: str,
) -> None:
    result = _gth_trend_entry_quality(
        {"session_id": session_id, "updated_at": updated_at, "regime": regime},
        session_date="2026-07-14",
        at=NOW,
        max_age_seconds=90.0,
    )
    assert result["mode"] == "shadow"
    assert result["verdict"] == "blocked"
    assert reason in result["block_reasons"]


def test_suppression_clear_requires_a_fresh_confirmation() -> None:
    state = None
    for minute, es in ((0, 7560), (5, 7554), (10, 7546), (12, 7551), (13, 7552)):
        state, alert, signal = advance(state, minute, es, allowed=False)
    assert alert is None
    assert state["pending"] is None

    state, alert, signal = advance(state, 14, 7553, allowed=True)
    assert alert is None
    assert state["pending"]["confirm_count"] == 1
    state, alert, signal = advance(state, 15, 7554, allowed=True)
    assert alert is not None


def test_provider_switch_resets_pending_confirmation() -> None:
    state = None
    for minute, es in ((0, 7560), (5, 7554), (10, 7546), (12, 7551)):
        state, alert, signal = advance(state, minute, es)
    assert state["pending"]["confirm_count"] == 1

    state, alert, signal = advance_gth_dip(
        state,
        session_date="2026-07-14",
        at=NOW + timedelta(minutes=13),
        es=7552,
        provider="ibkr",
        expected_move_points=80,
        short_horizon_seconds=900,
        long_horizon_seconds=3600,
        short_min_drawdown_points=8,
        long_min_drawdown_points=12,
        short_min_descent_seconds=0,
        long_min_descent_seconds=0,
        expected_move_fraction=0.10,
        reclaim_fraction=0.35,
        min_reclaim_points=4,
        confirm_samples=2,
        confirm_hold_seconds=0,
        session_warmup_seconds=0,
        max_signals_per_session=3,
        cooldown_seconds=900,
        entry_allowed=True,
        es_spx_basis=45.0,
    )
    assert alert is None
    assert signal is None
    assert state["pending"]["confirm_count"] == 1


def confirmed_signal_state():
    state = None
    for minute, es in ((0, 7560), (5, 7554), (10, 7546), (12, 7551), (13, 7552)):
        state, alert, signal = advance(state, minute, es)
    assert alert is not None
    return state, alert


def test_undelivered_signal_redelivers_after_retry_interval() -> None:
    state, alert = confirmed_signal_state()

    state, early, early_signal = advance(state, 13, 7552, seconds=29)
    assert early is None
    assert early_signal is None

    state, retry, retry_signal = advance(state, 13, 7552, seconds=31)
    assert retry is not None
    assert retry.event_id == alert.event_id
    assert retry.dedup_group == alert.dedup_group
    assert retry.title == alert.title
    assert retry.detail == alert.detail
    assert retry.source_at == alert.source_at
    assert retry_signal["delivery_retry"] is True
    assert state["status"] == "delivery_retry"
    assert state["last_signal"]["last_delivery_attempt_at"] == (
        NOW + timedelta(minutes=13, seconds=31)
    ).isoformat()


def test_delivery_ack_stops_redelivery() -> None:
    state, alert = confirmed_signal_state()
    state = mark_gth_delivery(
        state,
        event_id=str(alert.event_id),
        at=NOW + timedelta(minutes=13),
    )
    state, retry, retry_signal = advance(state, 13, 7552, seconds=45)
    assert retry is None
    assert retry_signal is None


def test_redelivery_stops_after_signal_expiry() -> None:
    state = None
    for minute, es in ((0, 7560), (5, 7554), (10, 7546), (12, 7551), (13, 7552)):
        state, alert, signal = advance(state, minute, es, expiry_seconds=60)
    assert alert is not None

    state, retry, _ = advance(state, 13, 7552, seconds=45, expiry_seconds=60)
    assert retry is not None

    # 75s after confirmation the signal is too old to retry, even when due.
    state, late, late_signal = advance(state, 14, 7553, seconds=15, expiry_seconds=60)
    assert late is None
    assert late_signal is None


def test_redelivery_treats_valid_until_as_exclusive_boundary() -> None:
    state = None
    for minute, es in ((0, 7560), (5, 7554), (10, 7546), (12, 7551), (13, 7552)):
        state, alert, _signal = advance(state, minute, es, expiry_seconds=60)
    assert alert is not None

    state, retry, retry_signal = advance(state, 14, 7553, expiry_seconds=60)

    assert retry is None
    assert retry_signal is None


def test_confirm_count_requires_fresh_samples() -> None:
    state = None
    for minute, es in ((0, 7560), (5, 7554), (10, 7546), (12, 7551)):
        state, alert, signal = advance(state, minute, es)
    assert state["pending"]["confirm_count"] == 1

    # A repeated poll with the same timestamp enqueues no new sample.
    state, alert, signal = advance(state, 12, 7551)
    assert alert is None
    assert state["pending"]["confirm_count"] == 1

    state, alert, signal = advance(state, 13, 7552)
    assert alert is not None


def spread(**overrides):
    kwargs = {
        "at": NOW,
        "session_date": "2026-07-14",
        "es": 7552.0,
        "trough": 7546.0,
        "expected_move_points": 80.0,
        "structure_levels": None,
        "es_spx_basis": 45.0,
        "min_width_points": 15.0,
        "max_width_points": 75.0,
        "default_width_points": 50.0,
        "exit_clock_et": "09:45",
    }
    kwargs.update(overrides)
    return _spread_structure(**kwargs)


def test_spread_requires_qualified_basis_and_rounds_strikes() -> None:
    result = spread()
    assert result["es_spx_basis_used"] == 45.0
    assert result["spx_equiv"] == 7507.0
    assert result["long_strike"] == 7505

    # Ties round away from zero.
    assert spread(es=7552.5)["long_strike"] == 7510
    assert spread(es_spx_basis=40.0)["spx_equiv"] == 7512.0
    assert spread(es_spx_basis=None) is None


def test_spread_anchors_short_strike_to_nearest_wall() -> None:
    result = spread(structure_levels={"flip_high": 7532.0, "call_wall": 7561.0})
    assert result["short_strike"] == 7530
    assert result["width_points"] == 25
    assert result["anchor"] == "structure_wall"
    assert result["target_wall"] == 7530.0
    assert result["target_wall_kind"] == "flip_high"


def test_spread_skips_wall_tighter_than_min_width() -> None:
    result = spread(structure_levels={"flip_high": 7512.0, "call_wall": 7532.0})
    assert result["short_strike"] == 7530
    assert result["target_wall_kind"] == "call_wall"


def test_spread_caps_far_wall_at_max_width() -> None:
    result = spread(structure_levels={"flip_high": 7623.0})
    assert result["short_strike"] == 7580
    assert result["width_points"] == 75
    assert result["anchor"] == "structure_wall"
    assert result["target_wall"] == 7625.0


def test_spread_expected_move_fallback_ignores_put_wall() -> None:
    result = spread(structure_levels={"put_wall": 7400.0, "flip_low": 7450.0})
    assert result["anchor"] == "expected_move"
    assert result["short_strike"] == 7545
    assert result["width_points"] == 40
    assert result["target_wall"] is None
    assert result["target_wall_kind"] is None


def test_spread_expected_move_width_clamped_to_band() -> None:
    assert spread(expected_move_points=20.0)["width_points"] == 15
    assert spread(expected_move_points=400.0)["width_points"] == 75


def test_spread_default_fallback_and_static_fields() -> None:
    result = spread(expected_move_points=None)
    assert result["anchor"] == "default"
    assert result["short_strike"] == 7555
    assert result["width_points"] == 50
    assert result["right"] == "C"
    assert result["invalidation_es"] == 7546.0
    assert result["expiry_date"] == "2026-07-14"
    assert result["exit_window_note"] == "美东 04:30–09:45（北京 16:30–21:45）分批止盈"
    assert result["exit_at"] == "2026-07-14T13:45:00+00:00"
    assert result["exit_by_utc"] == "13:45"
    assert result["quantity_policy"] == "operator_selected"


def test_signal_payload_carries_spread_and_redelivery_is_identical() -> None:
    state, alert = confirmed_signal_state()
    spread_block = state["last_signal"]["spread"]
    assert spread_block["right"] == "C"
    assert spread_block["anchor"] == "expected_move"
    assert spread_block["long_strike"] == 7505
    assert spread_block["short_strike"] == 7545
    assert spread_block["width_points"] == 40
    assert spread_block["invalidation_es"] == 7546.0
    assert spread_block["exit_by_utc"] == "13:45"

    state, retry, retry_signal = advance(state, 13, 7552, seconds=31)
    assert retry is not None
    assert retry.detail == alert.detail
    assert retry_signal["delivery_retry"] is True


def test_signal_spread_anchors_to_structure_wall() -> None:
    levels = {"flip_high": 7532.0, "call_wall": 7561.0, "put_wall": 7400.0}
    state = None
    for minute, es in ((0, 7560), (5, 7554), (10, 7546), (12, 7551), (13, 7552)):
        state, alert, signal = advance(
            state, minute, es, structure_levels=levels, es_spx_basis=45.0
        )
    assert alert is not None
    assert signal["spread"]["anchor"] == "structure_wall"
    assert signal["spread"]["long_strike"] == 7505
    assert signal["spread"]["short_strike"] == 7530
    assert signal["spread"]["target_wall_kind"] == "flip_high"
    assert "7530C" in alert.detail


def test_alert_renders_spread_strikes_and_exit_window() -> None:
    _, alert = confirmed_signal_state()
    assert "买 SPXW 0DTE 7505C / 卖 7545C" in alert.detail
    assert "宽 40 点" in alert.detail
    assert "出场窗口：美东 04:30–09:45（北京 16:30–21:45）分批止盈" in alert.detail
    assert "最迟 13:45 UTC 离场" in alert.detail
    assert "Risk：ES 跌破 7546.00 即撤销；自动下单关闭，数量人工定。" in alert.detail
    assert len(alert.detail) <= 600


def test_signal_alert_without_spread_keeps_legacy_text() -> None:
    state, _ = confirmed_signal_state()
    signal = {key: value for key, value in state["last_signal"].items() if key != "spread"}
    alert = _signal_alert(signal)
    assert "仅在新鲜 SPXW NBBO 通过门控后建立 TradeReady" in alert.detail
    assert "借记价差埋伏" not in alert.detail


def test_winter_exit_context_uses_dst_for_utc_and_beijing() -> None:
    result = _gth_exit_context("2026-12-15", exit_clock_et="09:45")
    assert result is not None
    assert result["exit_at"] == datetime(2026, 12, 15, 14, 45, tzinfo=timezone.utc)
    assert result["window_note"] == "美东 04:30–09:45（北京 17:30–22:45）分批止盈"


def test_spread_is_suppressed_at_or_after_expiry_exit() -> None:
    assert spread(at=datetime(2026, 7, 14, 13, 45, tzinfo=timezone.utc)) is None


def qualified_level_shadow(*, at: datetime = NOW) -> dict[str, object]:
    return {
        "updated_at": at.isoformat(),
        "structure": {
            "session_date": "2026-07-14",
            "expiry": "2026-07-14",
            "last_confirmed_at": at.isoformat(),
            "levels": {"flip_high": 7530.0, "call_wall": 7560.0},
        },
        "latest_observation": {
            "quality_ok": True,
            "trigger_basis_points": 45.0,
        },
    }


def test_gth_spread_inputs_require_same_session_fresh_quality() -> None:
    levels, basis = _gth_spread_inputs(
        qualified_level_shadow(),
        session_date="2026-07-14",
        at=NOW,
        max_age_seconds=90.0,
    )
    assert levels == {"flip_high": 7530.0, "call_wall": 7560.0}
    assert basis == 45.0


@pytest.mark.parametrize("failure", ("stale", "wrong_session", "bad_quality", "no_basis"))
def test_gth_spread_inputs_fail_closed(failure: str) -> None:
    payload = qualified_level_shadow()
    if failure == "stale":
        payload["structure"]["last_confirmed_at"] = (NOW - timedelta(seconds=91)).isoformat()
    elif failure == "wrong_session":
        payload["structure"]["session_date"] = "2026-07-13"
    elif failure == "bad_quality":
        payload["latest_observation"]["quality_ok"] = False
    else:
        payload["latest_observation"]["trigger_basis_points"] = None

    assert _gth_spread_inputs(
        payload,
        session_date="2026-07-14",
        at=NOW,
        max_age_seconds=90.0,
    ) == (None, None)


def test_only_existing_two_leg_shadow_suppresses_gth() -> None:
    assert not _virtual_strategy_blocks_gth(
        {"source_kind": "gth_dip_reclaim_call", "contract_id": "legacy-call"}
    )
    assert _virtual_strategy_blocks_gth({"position_type": "call_debit_spread"})
