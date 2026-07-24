from __future__ import annotations

import math
from datetime import datetime, timedelta
from statistics import NormalDist
from zoneinfo import ZoneInfo

import pytest

from spx_spark.application.market_features.wall_probability import (
    POLICY_STATUS,
    PROBABILITY_SEMANTICS,
    build_wall_probability_tenor_shadow,
)
from spx_spark.marketdata import (
    InstrumentId,
    MarketDataQuality,
    OptionGreeks,
    Provider,
    Quote,
)


ET = ZoneInfo("America/New_York")
FRONT = "20260723"
NEXT = "20260724"
SPOT = 7500.0
STRIKES = (7450.0, 7495.0, 7505.0, 7550.0)


def make_quote(
    *,
    expiry: str,
    strike: float,
    right: str,
    now: datetime,
    iv: float | None = 0.20,
    bid: float | None = 1.00,
    ask: float | None = 1.20,
    quality: MarketDataQuality = MarketDataQuality.LIVE,
) -> Quote:
    if right == "C":
        delta = 0.70 if strike < SPOT else 0.30
    else:
        delta = -0.30 if strike < SPOT else -0.70
    return Quote(
        instrument=InstrumentId.option(
            "SPX",
            expiry=expiry,
            strike=strike,
            right=right,
            trading_class="SPXW",
        ),
        provider=Provider.IBKR,
        provider_symbol=f"SPXW:{expiry}:{strike}:{right}",
        received_at=now,
        quality=quality,
        bid=bid,
        ask=ask,
        quote_time=now,
        greeks=OptionGreeks(
            implied_vol=iv,
            delta=delta,
            gamma=0.003,
            theta=-1.0,
            vega=0.3,
            model="test",
        ),
    )


def complete_quotes(
    expiry: str,
    *,
    now: datetime,
    iv: float | None = 0.20,
    bid: float | None = 1.00,
    ask: float | None = 1.20,
) -> list[Quote]:
    return [
        make_quote(
            expiry=expiry,
            strike=strike,
            right=right,
            now=now,
            iv=iv,
            bid=bid,
            ask=ask,
        )
        for strike in STRIKES
        for right in ("C", "P")
    ]


def inputs(
    now: datetime,
    *,
    front: str = FRONT,
    next_expiry: str = NEXT,
) -> tuple[dict[str, object], dict[str, list[Quote]], dict[str, object]]:
    options_map: dict[str, object] = {
        "created_at": now.isoformat(),
        "as_of": now.isoformat(),
        "underlier": {"price": SPOT, "source": "index:SPX"},
        "expiries": [
            {
                "expiry": front,
                "atm_iv": 0.22,
                "expected_move_points": 35.0,
                "expected_move_pct": 35.0 / SPOT,
                "coverage": {
                    "total": 8,
                    "live": 8,
                    "with_bid_ask": 8,
                    "with_iv": 8,
                },
            },
            {
                "expiry": next_expiry,
                "atm_iv": 0.18,
                "expected_move_points": 55.0,
                "expected_move_pct": 55.0 / SPOT,
                "coverage": {
                    "total": 8,
                    "live": 8,
                    "with_bid_ask": 8,
                    "with_iv": 8,
                },
            },
        ],
        "warnings": [],
    }
    grouped = {
        front: complete_quotes(front, now=now),
        next_expiry: complete_quotes(next_expiry, now=now),
    }
    option_frame: dict[str, object] = {
        "schema_version": 1,
        "frame_id": "options:test",
        "as_of": now.isoformat(),
        "quality": "ready",
        "front_expiry": front,
        "next_expiry": next_expiry,
        "structure": {
            "underlier": SPOT,
            "put_wall": 7450.0,
            "flip_zone": [7495.0, 7505.0],
            "zero_gamma": 7500.0,
            "call_wall": 7550.0,
            "frozen": False,
            "source": "live_options_map",
        },
        "volatility": {
            "atm_iv_0dte": 0.22,
            "atm_iv_1dte": 0.18,
            "expected_move_points_0dte": 35.0,
            "term_gap": 0.04,
        },
        "concentration": {},
        "density": {},
        "diagnostics": {},
    }
    return options_map, grouped, option_frame


def build(
    now: datetime,
    *,
    direction: str = "up",
    options_map: dict[str, object] | None = None,
    grouped: dict[str, list[Quote]] | None = None,
    option_frame: dict[str, object] | None = None,
) -> dict[str, object]:
    default_options, default_grouped, default_frame = inputs(now)
    return build_wall_probability_tenor_shadow(
        options_map=options_map or default_options,
        grouped_quotes=grouped if grouped is not None else default_grouped,
        option_frame=option_frame or default_frame,
        direction=direction,
        now=now,
    )


@pytest.mark.parametrize(
    ("hour", "minute", "expected_summary", "expected_by_horizon"),
    (
        (11, 30, "1DTE", {"15m": "1DTE", "30m": "1DTE", "60m": "1DTE"}),
        (12, 30, "mixed", {"15m": "1DTE", "30m": "1DTE", "60m": "0DTE"}),
        (12, 59, "0DTE", {"15m": "0DTE", "30m": "0DTE", "60m": "0DTE"}),
        (13, 0, "0DTE", {"15m": "0DTE", "30m": "0DTE", "60m": "0DTE"}),
        (15, 30, "0DTE", {"15m": "0DTE", "30m": "0DTE", "60m": "1DTE"}),
    ),
)
def test_tenor_prior_uses_each_horizons_planned_exit(
    hour: int,
    minute: int,
    expected_summary: str,
    expected_by_horizon: dict[str, str],
) -> None:
    now = datetime(2026, 7, 23, hour, minute, tzinfo=ET)

    result = build(now)

    assert result["status"] == "ready"
    assert result["direction_authority"] == "none"
    assert result["action_authority"] == "none"
    assert result["actionable"] is False
    assert result["policy_status"] == POLICY_STATUS
    tenor = result["tenor_shadow"]
    assert tenor["preferred_tenor"] == expected_summary
    assert {
        key: row["preferred_tenor"]
        for key, row in tenor["by_horizon"].items()
    } == (
        expected_by_horizon
        if not (hour == 15 and minute == 30)
        else {"15m": "0DTE", "30m": "0DTE", "60m": "0DTE"}
    )
    assert {
        key: row["selected_tenor"]
        for key, row in tenor["by_horizon"].items()
    } == expected_by_horizon
    # Even when 1DTE is the expression, the wall path remains exact 0DTE.
    assert result["path"]["tenor"] == "0DTE"
    assert result["path"]["expiry"] == FRONT


def test_frozen_front_structure_abstains() -> None:
    now = datetime(2026, 7, 23, 12, 0, tzinfo=ET)
    options_map, grouped, frame = inputs(now)
    frame["structure"] = {**frame["structure"], "frozen": True}

    result = build(
        now, options_map=options_map, grouped=grouped, option_frame=frame
    )

    assert result["status"] == "abstain"
    assert result["probability_status"] == "unavailable"
    assert result["wall_probabilities"] == {}
    assert "front_structure_frozen" in result["abstain_reasons"]
    assert result["tenor_shadow"]["selected_tenor"] is None


def test_real_iv_is_required_and_delta_fallback_is_not_published() -> None:
    now = datetime(2026, 7, 23, 14, 0, tzinfo=ET)
    options_map, grouped, frame = inputs(now)
    grouped[FRONT] = complete_quotes(FRONT, now=now, iv=None)

    result = build(
        now, options_map=options_map, grouped=grouped, option_frame=frame
    )

    assert result["status"] == "abstain"
    assert result["probability_status"] == "unavailable"
    assert all(
        row["status"] == "unavailable"
        for horizon in result["wall_probabilities"].values()
        for row in horizon.values()
    )
    assert "front_live_bid_ask_iv_coverage_insufficient" in result["abstain_reasons"]
    assert "real_iv_required_delta_fallback_rejected" in result["abstain_reasons"]
    assert result["probability_semantics"] == PROBABILITY_SEMANTICS
    assert result["tenor_shadow"]["eligibility"]["0DTE"]["iv_coverage_ratio"] == 0.0


def test_one_sided_quotes_are_not_live_bid_ask_coverage() -> None:
    now = datetime(2026, 7, 23, 14, 0, tzinfo=ET)
    options_map, grouped, frame = inputs(now)
    grouped[FRONT] = complete_quotes(FRONT, now=now, ask=None)

    result = build(
        now, options_map=options_map, grouped=grouped, option_frame=frame
    )

    assert result["status"] == "abstain"
    eligibility = result["tenor_shadow"]["eligibility"]["0DTE"]
    assert eligibility["live_bid_ask_count"] == 0
    assert "live_bid_ask_coverage_insufficient" in eligibility["reasons"]
    assert result["action"] == "none"


def test_call_only_front_chain_cannot_publish_all_wall_probabilities() -> None:
    now = datetime(2026, 7, 23, 14, 0, tzinfo=ET)
    options_map, grouped, frame = inputs(now)
    grouped[FRONT] = [
        quote for quote in grouped[FRONT] if quote.instrument.right.value == "C"
    ]

    result = build(
        now, options_map=options_map, grouped=grouped, option_frame=frame
    )

    assert result["status"] == "abstain"
    assert result["tenor_shadow"]["eligibility"]["0DTE"]["eligible"] is True
    assert "real_iv_anchor_unavailable" in result["abstain_reasons"]
    assert all(
        row["status"] == "unavailable"
        for row in result["horizon_status"].values()
    )


def test_unavailable_1dte_falls_back_to_0dte_before_cutoff() -> None:
    now = datetime(2026, 7, 23, 12, 30, tzinfo=ET)
    options_map, grouped, frame = inputs(now)
    grouped.pop(NEXT)

    result = build(
        now, options_map=options_map, grouped=grouped, option_frame=frame
    )

    assert result["status"] == "ready"
    tenor = result["tenor_shadow"]
    assert tenor["preferred_tenor"] == "mixed"
    assert tenor["selected_tenor"] == "0DTE"
    assert tenor["fallback_used"] is True
    assert tenor["eligibility"]["1DTE"]["eligible"] is False
    assert "preferred_tenor_unavailable_fallback_used" in result["warnings"]


def test_wall_probabilities_are_bounded_nd2_and_reflection_values() -> None:
    now = datetime(2026, 7, 23, 13, 15, tzinfo=ET)

    result = build(now, direction="down")

    assert result["status"] == "ready"
    probabilities = result["wall_probabilities"]
    assert set(probabilities) == {"15m", "30m", "60m"}
    assert all(
        set(rows) == {"put_wall", "flip_low", "flip_high", "call_wall"}
        for rows in probabilities.values()
    )
    for rows in probabilities.values():
        for row in rows.values():
            terminal = row["terminal_beyond_probability"]
            touch = row["touch_probability_2x_reflection"]
            assert 0.0 <= terminal <= 1.0
            assert 0.0 <= touch <= 1.0
            assert touch == pytest.approx(min(1.0, 2.0 * terminal))
            assert row["prob_close_beyond"] == terminal
            assert row["prob_touch"] == touch
            assert row["source_iv"] == pytest.approx(0.20)
            assert row["method"] == "risk_neutral_nd2_and_2x_reflection"
            assert row["probability_semantics"] == PROBABILITY_SEMANTICS
            assert (
                row["touch_probability_semantics"]
                == "zero_drift_2x_terminal_reflection_heuristic_not_calibrated_or_physical"
            )
            assert row["delta_role"] == "anchor_selection_only_not_probability"
            assert row["delta_fallback_allowed"] is False

    call_wall = probabilities["15m"]["call_wall"]
    tau = 15.0 / (365.0 * 24.0 * 60.0)
    d2 = (
        math.log(SPOT / 7550.0) - 0.5 * 0.20**2 * tau
    ) / (0.20 * math.sqrt(tau))
    assert call_wall["terminal_beyond_probability"] == pytest.approx(
        NormalDist().cdf(d2)
    )

    targets = result["directional_targets"]
    assert all(target["status"] == "available" for target in targets.values())
    assert all(target["level_name"] == "flip_low" for target in targets.values())
    assert all(target["distance_points"] == 5.0 for target in targets.values())
    assert all(
        target["direction_source"] == "upstream_input_no_probability_inference"
        for target in targets.values()
    )
    assert all(target["usable_scope"] == "shadow_diagnostic_only" for target in targets.values())
    assert all(target["execution_usable"] is False for target in targets.values())
    assert all(target["action_authority"] == "none" for target in targets.values())


def test_upstream_up_direction_selects_nearest_upper_flip_without_inference() -> None:
    now = datetime(2026, 7, 23, 11, 30, tzinfo=ET)

    result = build(now, direction="up")

    assert result["direction"] == "up"
    assert result["direction_authority"] == "none"
    assert all(
        target["level_name"] == "flip_high"
        and target["distance_points"] == 5.0
        and target["status"] == "available"
        for target in result["directional_targets"].values()
    )


def test_tenor_market_snapshot_contains_term_structure_and_coverage() -> None:
    now = datetime(2026, 7, 23, 11, 30, tzinfo=ET)

    result = build(now)

    market = result["tenor_shadow"]["market"]
    assert market["0DTE"]["atm_iv"] == pytest.approx(0.22)
    assert market["0DTE"]["expected_move_points"] == pytest.approx(35.0)
    assert market["1DTE"]["atm_iv"] == pytest.approx(0.18)
    assert market["1DTE"]["expected_move_points"] == pytest.approx(55.0)
    assert market["term_gap_0dte_minus_1dte"] == pytest.approx(0.04)
    assert market["0DTE"]["coverage"]["directional_live_bid_ask_ratio"] == 1.0
    assert market["1DTE"]["coverage"]["directional_iv_ratio"] == 1.0
    assert market["0DTE"]["coverage"]["map_with_iv"] == 8


def test_late_rth_expiry_gate_is_horizon_local() -> None:
    now = datetime(2026, 7, 23, 15, 30, tzinfo=ET)

    result = build(now)

    assert result["status"] == "ready"
    assert result["available_horizons"] == ["15m", "30m"]
    assert result["horizon_status"]["15m"]["status"] == "available"
    assert result["horizon_status"]["30m"]["status"] == "available"
    assert result["horizon_status"]["60m"]["status"] == "unavailable"
    assert (
        "holding_window_crosses_expiry"
        in result["horizon_status"]["60m"]["reasons"]
    )
    assert result["tenor_shadow"]["by_horizon"]["60m"]["selected_tenor"] == "1DTE"
    assert result["tenor_shadow"]["by_horizon"]["60m"]["fallback_used"] is True
    assert all(
        row["status"] == "unavailable"
        and row["reason"] == "holding_window_crosses_expiry"
        for row in result["wall_probabilities"]["60m"].values()
    )
    assert result["directional_targets"]["60m"]["status"] == "unavailable"


def test_no_horizon_ready_after_front_expiry_window_abstains_locally() -> None:
    now = datetime(2026, 7, 23, 15, 50, tzinfo=ET)

    result = build(now)

    assert result["status"] == "abstain"
    assert result["available_horizons"] == []
    assert "no_horizon_with_wall_probability_and_tenor" in result["abstain_reasons"]
    assert all(
        row["status"] == "unavailable"
        and "holding_window_crosses_expiry" in row["reasons"]
        for row in result["horizon_status"].values()
    )
    assert result["wall_probabilities"]


def test_early_close_uses_calendar_session_close() -> None:
    now = datetime(2026, 11, 27, 12, 15, tzinfo=ET)
    options_map, grouped, frame = inputs(
        now,
        front="20261127",
        next_expiry="20261130",
    )

    result = build_wall_probability_tenor_shadow(
        options_map=options_map,
        grouped_quotes=grouped,
        option_frame=frame,
        direction="up",
        now=now,
    )

    assert result["status"] == "ready"
    assert result["available_horizons"] == ["15m", "30m"]
    assert (
        result["horizon_status"]["60m"]["reasons"]
        == ["holding_window_crosses_expiry"]
    )
    assert result["tenor_shadow"]["by_horizon"]["60m"]["selected_tenor"] == "1DTE"


def test_front_expiry_close_boundary_is_inclusive_per_horizon() -> None:
    now = datetime(2026, 7, 23, 15, 45, tzinfo=ET)

    result = build(now)

    assert result["status"] == "ready"
    assert result["available_horizons"] == ["15m"]
    assert result["horizon_status"]["15m"]["status"] == "available"
    assert all(
        row["planned_exit_at"].endswith("16:00:00-04:00")
        and row["holding_window_valid"] is True
        for row in result["wall_probabilities"]["15m"].values()
    )
    assert all(
        row["status"] == "unavailable"
        and "holding_window_crosses_expiry" in row["reasons"]
        for key, row in result["horizon_status"].items()
        if key != "15m"
    )


def test_rth_freshness_boundary_is_inclusive_at_15_seconds() -> None:
    now = datetime(2026, 7, 23, 14, 0, tzinfo=ET)
    observed_at = now - timedelta(seconds=15)
    options_map, grouped, frame = inputs(now)
    options_map["as_of"] = observed_at.isoformat()
    frame["as_of"] = observed_at.isoformat()
    grouped[FRONT] = complete_quotes(FRONT, now=observed_at)

    result = build(
        now,
        options_map=options_map,
        grouped=grouped,
        option_frame=frame,
    )

    assert result["status"] == "ready"
    assert result["session"] == "rth"
    assert result["path"]["maximum_live_quote_age_seconds"] == 15.0
    assert result["input_freshness"]["maximum_age_seconds"] == 15.0
    assert result["probability_status"] == "ready"


@pytest.mark.parametrize(
    ("age_seconds", "expected_probability_status"),
    ((90, "ready"), (91, "unavailable")),
)
def test_gth_retains_only_90_second_wall_probability_diagnostics(
    age_seconds: int,
    expected_probability_status: str,
) -> None:
    now = datetime(2026, 7, 22, 20, 30, tzinfo=ET)
    quote_at = now - timedelta(seconds=age_seconds)
    options_map, grouped, frame = inputs(now)
    grouped[FRONT] = complete_quotes(FRONT, now=quote_at)

    result = build(
        now,
        options_map=options_map,
        grouped=grouped,
        option_frame=frame,
    )

    assert result["session"] == "gth"
    assert result["status"] == "abstain"
    assert result["actionable"] is False
    assert result["action_authority"] == "none"
    assert result["probability_semantics"] == PROBABILITY_SEMANTICS
    assert result["probability_status"] == expected_probability_status
    assert result["path"]["maximum_live_quote_age_seconds"] == 90.0
    assert "rth_required_for_tenor_prior" in result["abstain_reasons"]
    assert result["tenor_shadow"]["preferred_tenor"] is None
    assert result["tenor_shadow"]["selected_tenor"] is None
    assert all(
        row["selected_tenor"] is None
        and row["selection_reason"] == "rth_tenor_prior_unavailable"
        for row in result["tenor_shadow"]["by_horizon"].values()
    )
    if age_seconds == 90:
        assert result["probability_available_horizons"] == [
            "15m",
            "30m",
            "60m",
        ]
        assert result["wall_probabilities"]
    else:
        assert result["probability_available_horizons"] == []


@pytest.mark.parametrize("offset_seconds", (-16, 6))
def test_live_label_cannot_bypass_quote_timestamp_freshness(
    offset_seconds: int,
) -> None:
    now = datetime(2026, 7, 23, 14, 0, tzinfo=ET)
    options_map, grouped, frame = inputs(now)
    quote_at = now + timedelta(seconds=offset_seconds)
    grouped[FRONT] = complete_quotes(FRONT, now=quote_at)

    result = build(
        now,
        options_map=options_map,
        grouped=grouped,
        option_frame=frame,
    )

    eligibility = result["tenor_shadow"]["eligibility"]["0DTE"]
    assert result["status"] == "abstain"
    assert eligibility["quality_live_bid_ask_count"] == 4
    assert eligibility["live_bid_ask_count"] == 0
    assert "quote_freshness_insufficient" in eligibility["reasons"]
    assert result["probability_status"] == "unavailable"
    assert result["wall_probabilities"]


@pytest.mark.parametrize(
    ("input_name", "expected_reason"),
    (
        ("options_map", "options_map_stale"),
        ("option_frame", "option_frame_stale"),
    ),
)
def test_stale_input_frame_cannot_publish_current_probabilities(
    input_name: str,
    expected_reason: str,
) -> None:
    now = datetime(2026, 7, 23, 14, 0, tzinfo=ET)
    options_map, grouped, frame = inputs(now)
    stale_at = (now - timedelta(seconds=16)).isoformat()
    if input_name == "options_map":
        options_map["as_of"] = stale_at
    else:
        frame["as_of"] = stale_at

    result = build(
        now,
        options_map=options_map,
        grouped=grouped,
        option_frame=frame,
    )

    assert result["status"] == "abstain"
    assert expected_reason in result["abstain_reasons"]
    assert result["wall_probabilities"] == {}


def test_non_next_trading_expiry_cannot_masquerade_as_1dte() -> None:
    now = datetime(2026, 7, 23, 11, 30, tzinfo=ET)
    wrong_next = "20260727"
    options_map, grouped, frame = inputs(now, next_expiry=wrong_next)

    result = build(
        now,
        options_map=options_map,
        grouped=grouped,
        option_frame=frame,
    )

    one_dte = result["tenor_shadow"]["eligibility"]["1DTE"]
    market = result["tenor_shadow"]["market"]["1DTE"]
    assert result["status"] == "ready"
    assert one_dte["expected_expiry"] == NEXT
    assert one_dte["observed_expiry"] == wrong_next
    assert one_dte["expiry_contract_valid"] is False
    assert "1dte_exact_expiry_mismatch" in one_dte["reasons"]
    assert all(
        row["selected_tenor"] == "0DTE"
        for row in result["tenor_shadow"]["by_horizon"].values()
    )
    assert market["expiry"] is None
    assert market["atm_iv"] is None
    assert market["expected_move_points"] is None
    assert set(market["coverage"].values()) == {None}


@pytest.mark.parametrize(
    ("case", "expected_reason"),
    (
        ("direction", "direction_abstain"),
        ("wall", "stable_wall_incomplete"),
        ("chain", "front_chain_unavailable"),
    ),
)
def test_missing_direction_wall_or_chain_abstains(
    case: str, expected_reason: str
) -> None:
    now = datetime(2026, 7, 23, 14, 0, tzinfo=ET)
    options_map, grouped, frame = inputs(now)
    direction = "up"
    if case == "direction":
        direction = "abstain"
    elif case == "wall":
        structure = dict(frame["structure"])
        structure.pop("call_wall")
        frame["structure"] = structure
    else:
        grouped.pop(FRONT)

    result = build(
        now,
        direction=direction,
        options_map=options_map,
        grouped=grouped,
        option_frame=frame,
    )

    assert result["status"] == "abstain"
    assert expected_reason in result["abstain_reasons"]
    if case == "direction":
        assert result["probability_status"] == "ready"
        assert result["wall_probabilities"]
    else:
        assert result["wall_probabilities"] == {}
    assert result["direction_authority"] == "none"
    assert result["action_authority"] == "none"
