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
from spx_spark.ibkr.quote_demand import select_gth_quote_demand


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
    warmup_seconds: int = 0,
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
        session_warmup_seconds=warmup_seconds,
        max_signals_per_session=3,
        cooldown_seconds=900,
        entry_allowed=allowed,
        delivery_retry_seconds=retry_seconds,
        signal_expiry_seconds=expiry_seconds,
        **extra,
    )


def test_slow_es_dip_reclaim_confirms_without_spx() -> None:
    state = None
    for minute, es in ((0, 7560), (5, 7554), (10, 7546), (15, 7551)):
        state, alert, signal = advance(state, minute, es)
    assert alert is None
    state, alert, signal = advance(state, 16, 7552)
    assert alert is not None
    assert alert.kind == "gth_dip_reclaim_call"
    assert alert.title == "SPX 0DTE | CALL RECLAIM (15m)"
    assert "Desk View" in alert.detail
    assert "仅记录形态" in alert.detail
    assert "Execution" not in alert.detail
    assert signal["direction"] == "up"
    assert signal["drawdown_points"] == 14
    assert signal["schema_version"] == 3
    assert str(signal["policy_version"]).startswith("gth_dip_reclaim.v4+sha256:")
    assert signal["valid_until"] == (NOW + timedelta(minutes=26)).isoformat()
    assert signal["coordinate"]["kind"] == "raw_es"
    assert signal["coordinate"]["instrument_id"] == "future:ES"
    assert signal["block_reasons"] == []
    assert signal["entry_quality"]["mode"] == "decision_grade"


def test_legacy_one_hour_warmup_does_not_block_full_15m_window() -> None:
    state = None
    for minute, es in ((0, 7560), (5, 7554), (10, 7546), (15, 7551)):
        state, alert, signal = advance(
            state,
            minute,
            es,
            warmup_seconds=3600,
        )

    assert alert is None and signal is None
    assert state["legacy_session_warmup_seconds"] == 3600
    assert state["horizon_readiness"]["900"]["ready"] is True
    assert state["horizon_readiness"]["3600"]["ready"] is False
    assert state["status"] == "confirming"


def test_15m_and_60m_readiness_are_independent_full_windows() -> None:
    state = None
    for minute in (0, 15):
        state, _, _ = advance(
            state,
            minute,
            7500 + minute,
            warmup_seconds=3600,
        )
    assert state["horizon_readiness"]["900"]["ready"] is True
    assert state["horizon_readiness"]["3600"]["ready"] is False

    for minute in (30, 45, 60):
        state, _, _ = advance(
            state,
            minute,
            7500 + minute,
            warmup_seconds=3600,
        )
    assert state["horizon_readiness"]["900"]["ready"] is True
    assert state["horizon_readiness"]["3600"]["ready"] is True


def test_path_ranks_are_causal_non_overlapping_and_symmetric() -> None:
    state = None
    path = (
        (0, 100),
        (5, 90),
        (15, 95),
        (20, 94),
        (25, 92),
        (30, 93),
        (35, 110),
        (40, 100),
        (45, 105),
    )
    for minute, es in path:
        state, _, _ = advance(state, minute, es)

    rank = state["horizon_readiness"]["900"]["path_rank"]
    assert rank["rank_semantics"] == "empirical_cdf_midrank_not_probability"
    assert rank["reference_method"] == "causal_non_overlapping_session_windows.v1"
    assert rank["reference_overlap"] is False
    # The compact history already contains the 30–45 window, but the current
    # path may only compare with the two windows ending by its 30m start.
    assert len(state["path_history"]["900"]) == 3
    assert rank["effective_reference_windows"] == 2
    assert rank["position_percentile"] == 62.5
    assert rank["drawdown_rank_percentile"] == 75.0
    assert rank["recovery_rank_percentile"] == 75.0
    assert rank["rally_rank_percentile"] == 100.0
    assert rank["pullback_rank_percentile"] == 100.0
    assert rank["rank_status"] == "small_sample"


def test_flat_path_midrank_is_neutral_not_artificially_bullish() -> None:
    state = None
    for minute in (0, 15, 30):
        state, _, _ = advance(state, minute, 7500)

    rank = state["horizon_readiness"]["900"]["path_rank"]
    assert rank["effective_reference_windows"] == 1
    assert rank["position_percentile"] == 50.0
    assert rank["drawdown_rank_percentile"] == 50.0
    assert rank["recovery_rank_percentile"] == 50.0
    assert rank["rally_rank_percentile"] == 50.0
    assert rank["pullback_rank_percentile"] == 50.0


def test_five_second_bucket_dedup_does_not_inflate_sample_count() -> None:
    state, _, _ = advance(None, 0, 7500, seconds=1)
    state, _, _ = advance(state, 0, 7501, seconds=4)

    assert len(state["samples"]) == 1
    assert state["samples"][0] == {
        "at": NOW.isoformat(),
        "es": 7501.0,
        "provider": "schwab",
    }
    assert state["horizon_readiness"]["900"]["sample_count"] == 1


def test_non_monotonic_source_tick_is_ignored_without_truncating_history() -> None:
    state, _, _ = advance(None, 0, 7500)
    state, _, _ = advance(state, 1, 7502)
    samples = list(state["samples"])
    updated_at = state["updated_at"]

    state, alert, signal = advance(state, 0, 7490, seconds=30)

    assert state["status"] == "non_monotonic_sample_ignored"
    assert state["last_ignored_sample_at"] == (NOW + timedelta(seconds=30)).isoformat()
    assert state["ignored_sample_count"] == 1
    assert state["samples"] == samples
    assert state["updated_at"] == updated_at
    assert alert is None
    assert signal is None


def test_sparse_interior_samples_are_visible_but_do_not_reset_readiness() -> None:
    state, _, _ = advance(None, 0, 7500, warmup_seconds=3600)
    state, _, _ = advance(state, 15, 7501, warmup_seconds=3600)

    readiness = state["horizon_readiness"]["900"]
    assert readiness["ready"] is True
    assert readiness["sample_count"] == 2
    assert readiness["expected_sample_count"] == 181
    assert readiness["coverage_ratio"] == pytest.approx(2 / 181)
    assert readiness["max_sample_gap_seconds"] == 900.0
    assert readiness["sampling_quality"] == "usable_sparse"
    assert readiness["decision_usable"] is False


def test_extremely_sparse_full_window_cannot_trigger_formal_dip_signal() -> None:
    state, _, _ = advance(None, 0, 7560)
    state, _, _ = advance(state, 10, 7546)
    state, alert, signal = advance(state, 15, 7552)

    readiness = state["horizon_readiness"]["900"]
    assert readiness["ready"] is True
    assert readiness["decision_usable"] is False
    assert state["status"] == "observing"
    assert state["pending"] is None
    assert alert is None
    assert signal is None


def test_legacy_state_samples_restore_15m_readiness_without_new_warmup() -> None:
    legacy = {
        "schema_version": 1,
        "session_date": "2026-07-14",
        "first_sample_at": NOW.isoformat(),
        "signal_count": 0,
        "samples": [
            {"at": NOW.isoformat(), "es": 7500.0, "provider": "schwab"},
            {
                "at": (NOW + timedelta(minutes=15)).isoformat(),
                "es": 7501.0,
                "provider": "schwab",
            },
        ],
    }

    state, _, _ = advance(legacy, 15, 7501, warmup_seconds=3600)

    assert state["continuous_started_at"] == NOW.isoformat()
    assert state["horizon_readiness"]["900"]["ready"] is True


def test_state_keeps_bounded_raw_window_and_compact_full_session_history() -> None:
    state = None
    for minute in range(126):
        state, _, _ = advance(state, minute, 7500 + (minute % 7))

    assert len(state["samples"]) == 62
    assert len(state["path_history"]["900"]) == 8
    assert len(state["path_history"]["3600"]) == 2
    assert all(len(rows) <= 1_000 for rows in state["path_history"].values())


def test_macro_pre_event_suppresses_confirmation_but_keeps_observation() -> None:
    state = None
    for minute, es in ((0, 7560), (5, 7554), (10, 7546), (15, 7551), (16, 7552)):
        state, alert, signal = advance(state, minute, es, allowed=False)
    assert alert is None
    assert signal is None
    assert state["status"] == "suppressed_pre_event"
    assert state["pending"] is None


def test_gth_trend_quality_is_decision_grade_and_point_in_time() -> None:
    result = _gth_trend_entry_quality(
        {
            "session_id": "2026-07-14:gth",
            "updated_at": (NOW - timedelta(seconds=30)).isoformat(),
            "regime": "bullish",
            "metrics": {
                "return_15m_points": 3.0,
                "return_60m_points": 4.0,
            },
        },
        session_date="2026-07-14",
        at=NOW,
        max_age_seconds=90.0,
    )
    assert result["mode"] == "decision_grade"
    assert result["verdict"] == "pass"
    assert result["features"]["return_15m_points"] == 3.0
    assert result["features"]["expected_session_id"] == "2026-07-14:gth"


def test_gth_trend_quality_blocks_the_july_29_bad_call_context() -> None:
    result = _gth_trend_entry_quality(
        {
            "session_id": "2026-07-14:gth",
            "updated_at": (NOW - timedelta(seconds=19)).isoformat(),
            "regime": "bullish",
            "metrics": {
                "return_15m_points": 3.375,
                "return_60m_points": -10.125,
                "return_180m_points": None,
            },
        },
        session_date="2026-07-14",
        at=NOW,
        max_age_seconds=90.0,
    )

    assert result["mode"] == "decision_grade"
    assert result["verdict"] == "blocked"
    assert "trend_60m_not_positive" in result["block_reasons"]


@pytest.mark.parametrize(
    ("session_id", "updated_at", "regime", "reason"),
    (
        (
            "2026-07-13:gth",
            NOW.isoformat(),
            "bullish",
            "trend_session_mismatch",
        ),
        (
            "2026-07-14:gth",
            (NOW - timedelta(seconds=91)).isoformat(),
            "bullish",
            "trend_context_stale",
        ),
        (
            "2026-07-14:gth",
            NOW.isoformat(),
            "bearish",
            "trend_not_bullish",
        ),
    ),
)
def test_gth_trend_quality_blocks_bad_context(
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
    assert result["mode"] == "decision_grade"
    assert result["verdict"] == "blocked"
    assert reason in result["block_reasons"]


def test_suppression_clear_requires_a_fresh_confirmation() -> None:
    state = None
    for minute, es in ((0, 7560), (5, 7554), (10, 7546), (15, 7551), (16, 7552)):
        state, alert, signal = advance(state, minute, es, allowed=False)
    assert alert is None
    assert state["pending"] is None

    state, alert, signal = advance(state, 17, 7553, allowed=True)
    assert alert is None
    assert state["pending"]["confirm_count"] == 1
    state, alert, signal = advance(state, 18, 7554, allowed=True)
    assert alert is not None


def test_provider_switch_resets_pending_confirmation() -> None:
    state = None
    for minute, es in ((0, 7560), (5, 7554), (10, 7546), (15, 7551)):
        state, alert, signal = advance(state, minute, es)
    assert state["pending"]["confirm_count"] == 1

    state, alert, signal = advance_gth_dip(
        state,
        session_date="2026-07-14",
        at=NOW + timedelta(minutes=16),
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
    assert state["pending"] is None
    assert state["samples"] == [
        {
            "at": (NOW + timedelta(minutes=16)).isoformat(),
            "es": 7552.0,
            "provider": "ibkr",
        }
    ]
    assert state["first_sample_at"] == NOW.isoformat()
    assert state["provider_changed"] is True
    assert state["continuous_started_at"] == (NOW + timedelta(minutes=16)).isoformat()
    assert state["path_history"] == {"900": [], "3600": []}
    assert state["horizon_readiness"]["900"]["ready"] is False


def test_equal_timestamp_provider_switch_cannot_relabel_pending() -> None:
    state = None
    for minute, es in ((0, 7560), (5, 7554), (10, 7546), (15, 7551)):
        state, _alert, _signal = advance(state, minute, es)
    assert state["pending"]["provider"] == "schwab"

    state, alert, signal = advance_gth_dip(
        state,
        session_date="2026-07-14",
        at=NOW + timedelta(minutes=15),
        es=7551,
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
    assert state["provider_changed"] is True
    assert state["pending"] is None
    assert {row["provider"] for row in state["samples"]} == {"ibkr"}


def test_persisted_mixed_provider_history_uses_only_contiguous_suffix() -> None:
    state = {
        "schema_version": 1,
        "session_date": "2026-07-14",
        "first_sample_at": NOW.isoformat(),
        "signal_count": 0,
        "samples": [
            {"at": NOW.isoformat(), "es": 7560.0, "provider": "schwab"},
            {
                "at": (NOW + timedelta(minutes=5)).isoformat(),
                "es": 7540.0,
                "provider": "schwab",
            },
            {
                "at": (NOW + timedelta(minutes=10)).isoformat(),
                "es": 7550.0,
                "provider": "ibkr",
            },
        ],
    }

    state, alert, signal = advance_gth_dip(
        state,
        session_date="2026-07-14",
        at=NOW + timedelta(minutes=11),
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

    assert alert is None and signal is None
    assert {row["provider"] for row in state["samples"]} == {"ibkr"}
    assert state["pending"] is None


def test_spread_policy_change_restarts_confirmation_and_refreezes_legs() -> None:
    state = None
    levels = {"call_wall": 7580.0}
    for minute, es in ((0, 7560), (5, 7554), (10, 7546), (15, 7551)):
        state, alert, signal = advance(
            state,
            minute,
            es,
            structure_levels=levels,
            spread_max_width_points=75.0,
        )
    assert alert is None and signal is None
    assert state["pending"]["spread"]["width_points"] == 75
    old_policy = state["pending"]["spread_policy_version"]

    state, alert, signal = advance(
        state,
        16,
        7552,
        structure_levels=levels,
        spread_max_width_points=50.0,
    )

    assert alert is None and signal is None
    assert state["pending"]["confirm_count"] == 1
    assert state["pending"]["spread"]["width_points"] == 50
    assert state["pending"]["spread_policy_version"] != old_policy


def test_corrupt_frozen_spread_restarts_instead_of_crashing_confirmation() -> None:
    state = None
    for minute, es in ((0, 7560), (5, 7554), (10, 7546), (15, 7551)):
        state, _alert, _signal = advance(state, minute, es)
    state["pending"]["spread"] = {
        "right": "C",
        "expiry_date": "2026-07-14",
        "long_strike": 7505,
        "short_strike": 7555,
    }

    state, alert, signal = advance(state, 16, 7552)

    assert alert is None and signal is None
    assert state["pending"]["confirm_count"] == 1
    assert state["pending"]["spread"]["width_points"] == 40


def confirmed_signal_state():
    state = None
    entry_quality = {
        "mode": "decision_grade",
        "policy_version": "gth_trend_alignment_live_v2",
        "verdict": "pass",
        "block_reasons": [],
        "features": {
            "session_id": "2026-07-14:gth",
            "return_15m_points": 3.0,
            "return_60m_points": 4.0,
        },
    }
    for minute, es in ((0, 7560), (5, 7554), (10, 7546), (15, 7551), (16, 7552)):
        state, alert, signal = advance(
            state,
            minute,
            es,
            entry_quality=entry_quality,
        )
    assert alert is not None
    return state, alert


def test_undelivered_signal_redelivers_after_retry_interval() -> None:
    state, alert = confirmed_signal_state()

    state, early, early_signal = advance(state, 16, 7552, seconds=29)
    assert early is None
    assert early_signal is None

    state, retry, retry_signal = advance(state, 16, 7552, seconds=31)
    assert retry is not None
    assert retry.event_id == alert.event_id
    assert retry.dedup_group == alert.dedup_group
    assert retry.title == alert.title
    assert retry.detail == alert.detail
    assert retry.source_at == alert.source_at
    assert retry_signal["delivery_retry"] is True
    assert state["status"] == "delivery_retry"
    assert (
        state["last_signal"]["last_delivery_attempt_at"]
        == (NOW + timedelta(minutes=16, seconds=31)).isoformat()
    )


def test_delivery_ack_stops_redelivery() -> None:
    state, alert = confirmed_signal_state()
    state = mark_gth_delivery(
        state,
        event_id=str(alert.event_id),
        at=NOW + timedelta(minutes=16),
    )
    state, retry, retry_signal = advance(state, 16, 7552, seconds=45)
    assert retry is None
    assert retry_signal is None


def test_redelivery_stops_after_signal_expiry() -> None:
    state = None
    for minute, es in ((0, 7560), (5, 7554), (10, 7546), (15, 7551), (16, 7552)):
        state, alert, signal = advance(state, minute, es, expiry_seconds=60)
    assert alert is not None

    state, retry, _ = advance(state, 16, 7552, seconds=45, expiry_seconds=60)
    assert retry is not None

    # 75s after confirmation the signal is too old to retry, even when due.
    state, late, late_signal = advance(state, 17, 7553, seconds=15, expiry_seconds=60)
    assert late is None
    assert late_signal is None


def test_redelivery_treats_valid_until_as_exclusive_boundary() -> None:
    state = None
    for minute, es in ((0, 7560), (5, 7554), (10, 7546), (15, 7551), (16, 7552)):
        state, alert, _signal = advance(state, minute, es, expiry_seconds=60)
    assert alert is not None

    state, retry, retry_signal = advance(state, 17, 7553, expiry_seconds=60)

    assert retry is None
    assert retry_signal is None


def test_confirm_count_requires_fresh_samples() -> None:
    state = None
    for minute, es in ((0, 7560), (5, 7554), (10, 7546), (15, 7551)):
        state, alert, signal = advance(state, minute, es)
    assert state["pending"]["confirm_count"] == 1

    # A repeated poll with the same timestamp enqueues no new sample.
    state, alert, signal = advance(state, 15, 7551)
    assert alert is None
    assert state["pending"]["confirm_count"] == 1

    state, alert, signal = advance(state, 16, 7552)
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

    state, retry, retry_signal = advance(state, 16, 7552, seconds=31)
    assert retry is not None
    assert retry.detail == alert.detail
    assert retry_signal["delivery_retry"] is True


def test_pending_freezes_exact_spread_and_confirmation_reuses_it() -> None:
    state = None
    for minute, es in ((0, 7560), (5, 7554), (10, 7546)):
        state, _, _ = advance(state, minute, es)
    state, alert, _ = advance(
        state,
        15,
        7551,
        structure_levels={"flip_high": 7530.0},
        es_spx_basis=45.0,
    )
    assert alert is None
    pending_spread = dict(state["pending"]["spread"])
    assert (pending_spread["long_strike"], pending_spread["short_strike"]) == (7505, 7530)

    state, alert, signal = advance(
        state,
        16,
        7557,
        structure_levels={"call_wall": 7580.0},
        es_spx_basis=35.0,
    )
    assert alert is not None
    assert signal["spread"] == pending_spread


def test_real_pending_and_confirmed_state_publish_exact_leg_demand() -> None:
    state = None
    for minute, es in ((0, 7560), (5, 7554), (10, 7546), (15, 7551)):
        state, alert, signal = advance(state, minute, es)
    assert alert is None and signal is None

    pending_demand, reason = select_gth_quote_demand(
        at=NOW + timedelta(minutes=15),
        session_date="2026-07-14",
        provider="schwab",
        gth_state=state,
        virtual_active=None,
    )
    assert reason == "selected"
    assert pending_demand is not None and pending_demand.status == "pending"

    state, alert, signal = advance(state, 16, 7552)
    assert alert is not None and signal is not None
    confirmed_demand, reason = select_gth_quote_demand(
        at=NOW + timedelta(minutes=16),
        session_date="2026-07-14",
        provider="schwab",
        gth_state=state,
        virtual_active=None,
    )
    assert reason == "selected"
    assert confirmed_demand is not None and confirmed_demand.status == "confirmed"
    assert confirmed_demand.demand_id == pending_demand.demand_id
    assert confirmed_demand.legs == pending_demand.legs


def test_pending_freezes_spread_when_coordinates_first_become_available() -> None:
    state = None
    for minute, es in ((0, 7560), (5, 7554), (10, 7546)):
        state, _, _ = advance(state, minute, es, es_spx_basis=None)
    state, alert, _ = advance(state, 15, 7551, es_spx_basis=None)
    assert alert is None
    assert state["pending"]["spread"] is None

    state, alert, signal = advance(state, 16, 7552, es_spx_basis=45.0)
    assert alert is None and signal is None
    assert state["pending"]["confirm_count"] == 1
    pending_spread = dict(state["pending"]["spread"])

    state, alert, signal = advance(state, 17, 7553, es_spx_basis=45.0)
    assert alert is not None
    assert signal["spread"] == pending_spread
    assert signal["spread"]["long_strike"] == 7505


def test_signal_spread_anchors_to_structure_wall() -> None:
    levels = {"flip_high": 7532.0, "call_wall": 7561.0, "put_wall": 7400.0}
    state = None
    for minute, es in ((0, 7560), (5, 7554), (10, 7546), (15, 7551), (16, 7552)):
        state, alert, signal = advance(
            state, minute, es, structure_levels=levels, es_spx_basis=45.0
        )
    assert alert is not None
    assert signal["spread"]["anchor"] == "structure_wall"
    assert signal["spread"]["long_strike"] == 7505
    assert signal["spread"]["short_strike"] == 7530
    assert signal["spread"]["target_wall_kind"] == "flip_high"
    assert "7530C" not in alert.detail
    assert "不生成合约或操作指令" in alert.detail


def test_source_alert_waits_for_green_card_without_leaking_execution_legs() -> None:
    _, alert = confirmed_signal_state()
    assert "方向门已通过" in alert.detail
    assert "只有随后绿色 MANUAL READY 卡可操作" in alert.detail
    assert "7505C" not in alert.detail
    assert "7545C" not in alert.detail
    assert "Execution" not in alert.detail
    assert len(alert.detail) <= 600


def test_signal_alert_without_spread_still_waits_for_green_card() -> None:
    state, _ = confirmed_signal_state()
    signal = {key: value for key, value in state["last_signal"].items() if key != "spread"}
    alert = _signal_alert(signal)
    assert "只有随后绿色 MANUAL READY 卡可操作" in alert.detail
    assert "借记价差埋伏" not in alert.detail


def test_winter_exit_context_uses_dst_for_utc_and_beijing() -> None:
    result = _gth_exit_context("2026-12-15", exit_clock_et="09:45")
    assert result is not None
    assert result["exit_at"] == datetime(2026, 12, 15, 14, 45, tzinfo=timezone.utc)
    assert result["window_note"] == "美东 04:30–09:45（北京 17:30–22:45）分批止盈"


def test_spread_is_suppressed_at_or_after_expiry_exit() -> None:
    assert spread(at=datetime(2026, 7, 14, 13, 45, tzinfo=timezone.utc)) is None


def qualified_level_shadow(
    *,
    at: datetime = NOW,
    expiry: str = "20260714",
) -> dict[str, object]:
    return {
        "updated_at": at.isoformat(),
        "structure": {
            "session_date": "2026-07-14",
            "expiry": expiry,
            "last_confirmed_at": at.isoformat(),
            "levels": {"flip_high": 7530.0, "call_wall": 7560.0},
        },
        "latest_observation": {
            "quality_ok": True,
            "trigger_basis_points": 45.0,
        },
    }


@pytest.mark.parametrize("expiry", ("20260714", "2026-07-14"))
def test_gth_spread_inputs_require_same_session_fresh_quality(expiry: str) -> None:
    levels, basis = _gth_spread_inputs(
        qualified_level_shadow(expiry=expiry),
        session_date="2026-07-14",
        at=NOW,
        max_age_seconds=90.0,
    )
    assert levels == {"flip_high": 7530.0, "call_wall": 7560.0}
    assert basis == 45.0


@pytest.mark.parametrize(
    "failure",
    (
        "stale",
        "wrong_session",
        "wrong_expiry",
        "malformed_expiry",
        "bad_quality",
        "no_basis",
    ),
)
def test_gth_spread_inputs_fail_closed(failure: str) -> None:
    payload = qualified_level_shadow()
    if failure == "stale":
        payload["structure"]["last_confirmed_at"] = (NOW - timedelta(seconds=91)).isoformat()
    elif failure == "wrong_session":
        payload["structure"]["session_date"] = "2026-07-13"
    elif failure == "wrong_expiry":
        payload["structure"]["expiry"] = "20260715"
    elif failure == "malformed_expiry":
        payload["structure"]["expiry"] = "not-an-expiry"
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


def test_missing_spread_inputs_do_not_claim_confirmation_progress() -> None:
    state = None
    for minute, es in ((0, 7560), (5, 7554), (10, 7546), (15, 7551)):
        state, alert, signal = advance(state, minute, es, es_spx_basis=None)

    assert alert is None
    assert signal is None
    assert state["status"] == "spread_inputs_unavailable"
    assert state["pending"]["confirm_count"] == 0
    assert state["pending"]["confirm_started_at"] is None
    assert state["pending"]["spread"] is None


def test_only_existing_two_leg_shadow_suppresses_gth() -> None:
    assert not _virtual_strategy_blocks_gth(
        {"source_kind": "gth_dip_reclaim_call", "contract_id": "legacy-call"}
    )
    assert _virtual_strategy_blocks_gth({"position_type": "call_debit_spread"})
