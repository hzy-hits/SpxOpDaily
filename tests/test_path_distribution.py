from __future__ import annotations

import json
from datetime import date, datetime, time, timedelta, timezone
from pathlib import Path
from zoneinfo import ZoneInfo

from spx_spark.application.market_features.physical_followthrough import (
    load_iron_condor_clearing_paths,
    load_physical_spot_paths,
)
from spx_spark.application.order_map.path_distribution import (
    attach_iron_condor_path_distribution,
    estimate_iron_condor_clearing_distribution,
    estimate_path_distribution,
    load_joint_surface_paths,
    path_distribution_desk_text,
)
from spx_spark.settings.strategy_distribution import StrategyDistributionSettings

NEW_YORK = ZoneInfo("America/New_York")
RTH_NOW = datetime(2026, 8, 6, 14, 30, tzinfo=timezone.utc)
GTH_NOW = datetime(2026, 8, 6, 4, 30, tzinfo=timezone.utc)
EXPIRY = "20260806"


def _write_session(root: Path, day: str, *, start_et: time, prices: list[float]) -> None:
    path = root / "features" / "spx_standardized_samples" / f"date={day}" / "events.jsonl"
    path.parent.mkdir(parents=True, exist_ok=True)
    rows: list[str] = []
    start = datetime.combine(date.fromisoformat(day), start_et, tzinfo=NEW_YORK)
    for offset, price in enumerate(prices):
        minute = start + timedelta(minutes=offset)
        rows.append(
            json.dumps(
                {
                    "status": "selected",
                    "minute": minute.isoformat(),
                    "selected": {"price": price},
                }
            )
        )
    path.write_text("\n".join(rows) + "\n", encoding="utf-8")


def _write_surface_session(
    root: Path,
    day: str,
    *,
    start_et: time,
    count: int,
    atm_step: float = 0.0002,
    created_at: datetime | None = None,
) -> None:
    session_day = date.fromisoformat(day)
    start = datetime.combine(session_day, start_et, tzinfo=NEW_YORK)
    by_path: dict[Path, list[str]] = {}
    for index in range(count):
        as_of = start + timedelta(minutes=5 * index)
        expiry = day.replace("-", "")
        row = {
            "created_at": (created_at or as_of).astimezone(timezone.utc).isoformat(),
            "as_of": as_of.astimezone(timezone.utc).isoformat(),
            "underlier_price": 7750.0 + 0.1 * index,
            "underlier_source": "index:SPX",
            "front_expiry": expiry,
            "next_expiry": None,
            "front_vs_next_atm_iv_gap": None,
            "warnings": [],
            "expiries": [
                {
                    "expiry": expiry,
                    "atm_iv": 0.16 + atm_step * index,
                    "put_skew_ratio": 1.15 + 0.001 * index,
                    "call_skew_ratio": 1.05 - 0.0005 * index,
                    "put_skew_25d": 0.02 + 0.0001 * index,
                    "call_skew_25d": -0.005 - 0.00005 * index,
                    "surface_fit_quality": "raw_grid",
                    "wide_quote_surface_degraded": False,
                    "option_count": 80,
                    "iv_coverage_ratio": 1.0,
                    "gamma_coverage_ratio": 1.0,
                    "warnings": [],
                }
            ],
        }
        utc = as_of.astimezone(timezone.utc)
        path = (
            root
            / "features"
            / "iv_surface"
            / f"date={day}"
            / f"hour={utc:%H}"
            / "snapshots.jsonl"
        )
        by_path.setdefault(path, []).append(json.dumps(row))
    for path, lines in by_path.items():
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def _facts(*, now: datetime, spot: float = 7750.0, session_date: str = "2026-08-06") -> dict[str, object]:
    close = datetime.combine(date.fromisoformat(session_date), time(16, 0), tzinfo=NEW_YORK)
    minutes = max(int((close - now.astimezone(NEW_YORK)).total_seconds() // 60), 1)
    return {
        "session_date": session_date,
        "minutes_to_close": minutes,
        "spot": {"spx": spot},
        "volatility": {"expected_move_points": 18.0},
    }


def _leg(strike: float, right: str, bid: float, ask: float) -> dict[str, object]:
    return {
        "contract_id": f"option:SPX:SPXW:{EXPIRY}:{strike:g}:{right}",
        "strike": strike,
        "right": right,
        "bid": bid,
        "ask": ask,
        "implied_vol": 0.16,
        "source_at": RTH_NOW.isoformat(),
    }


def _call_vertical() -> dict[str, object]:
    long_leg = _leg(7750.0, "C", 1.4, 1.6)
    short_leg = _leg(7760.0, "C", 0.9, 1.1)
    return {
        "strategy_type": "CALL_DEBIT_VERTICAL",
        "setup_kind": "GTH_WIDTH_SCAN",
        "right": "C",
        "direction": "UP",
        "long": long_leg,
        "short": short_leg,
        "quote": {"status": "ready", "bid": 0.3, "ask": 0.7},
        "economics": {
            "width_points": 10.0,
            "max_loss_points": 0.7,
            "max_gain_points": 9.3,
        },
        "invalidation_spx": 7735.0,
        "selection_score": 1.25,
    }


def test_same_clock_paths_exclude_current_session_and_are_stable(tmp_path: Path) -> None:
    prices = [7700.0 + offset * 0.2 for offset in range(80)]
    _write_session(tmp_path, "2026-08-04", start_et=time(10, 0), prices=prices)
    _write_session(tmp_path, "2026-08-05", start_et=time(10, 0), prices=[value + 5 for value in prices])
    _write_session(tmp_path, "2026-08-06", start_et=time(10, 0), prices=[value + 50 for value in prices])

    first, mode = load_physical_spot_paths(
        tmp_path / "features",
        now=RTH_NOW,
        trading_date=date(2026, 8, 6),
        window_days=35,
        horizon_minutes=20,
        minimum_same_clock=30,
    )
    second, _ = load_physical_spot_paths(
        tmp_path / "features",
        now=RTH_NOW,
        trading_date=date(2026, 8, 6),
        window_days=35,
        horizon_minutes=20,
        minimum_same_clock=30,
    )

    assert mode == "same_clock"
    assert first
    assert {row.session_date for row in first} == {date(2026, 8, 4), date(2026, 8, 5)}
    assert [row.prices for row in first] == [row.prices for row in second]
    assert all(len(row.prices) == 21 for row in first)


def test_gth_falls_back_to_rth_session_shapes(tmp_path: Path) -> None:
    prices = [7700.0 + offset * 0.15 for offset in range(80)]
    _write_session(tmp_path, "2026-08-05", start_et=time(10, 0), prices=prices)

    rows, mode = load_physical_spot_paths(
        tmp_path / "features",
        now=GTH_NOW,
        trading_date=date(2026, 8, 6),
        window_days=35,
        horizon_minutes=20,
        minimum_same_clock=30,
    )

    assert mode == "session_shape_fallback"
    assert len(rows) >= 50
    assert all(row.session_date == date(2026, 8, 5) for row in rows)


def _full_rth_prices(start: float, step: float) -> list[float]:
    return [start + offset * step for offset in range(391)]


def test_winner_path_distribution_is_ordered_and_does_not_change_score(tmp_path: Path) -> None:
    _write_session(tmp_path, "2026-08-04", start_et=time(9, 30), prices=_full_rth_prices(7750.0, 0.8))
    _write_session(tmp_path, "2026-08-05", start_et=time(9, 30), prices=_full_rth_prices(7750.0, -0.8))

    candidate = _call_vertical()
    distribution = estimate_path_distribution(
        candidate,
        _facts(now=RTH_NOW),
        data_root=tmp_path,
        probability_settings=StrategyDistributionSettings(),
        now=RTH_NOW,
    )

    assert distribution["status"] == "estimated_uncalibrated"
    assert distribution["evidence_status"] == "research_unvalidated"
    assert distribution["n_sessions"] == 2
    assert distribution["n_paths"] >= 30
    assert distribution["p10_pnl_points"] <= distribution["p50_pnl_points"] <= distribution["p90_pnl_points"]
    assert candidate["selection_score"] == 1.25
    assert distribution["method"] == "physical_path_management_policy.v3"
    assert distribution["risk_objective"]["status"] == "available"
    assert distribution["risk_objective"]["authority"] == "advisory_only"
    assert distribution["pnl_histogram"]
    assert distribution["horizon_minutes"] > 20
    assert "invalidation_not_protective" not in distribution["reason_codes"]
    text = path_distribution_desk_text(distribution)
    assert text is not None
    assert text.startswith("最迟15:45ET 路径 P10/P50/P90 $")


def test_joint_surface_replay_is_primary_and_keeps_sticky_baseline(tmp_path: Path) -> None:
    _write_surface_session(tmp_path, "2026-08-04", start_et=time(10, 30), count=64)
    _write_surface_session(
        tmp_path, "2026-08-05", start_et=time(10, 30), count=64, atm_step=-0.00015
    )

    distribution = estimate_path_distribution(
        _call_vertical(),
        _facts(now=RTH_NOW),
        data_root=tmp_path,
        probability_settings=StrategyDistributionSettings(),
        now=RTH_NOW,
    )

    assert distribution["method"] == "joint_spot_surface_management_policy.v1"
    assert distribution["surface_coordinate"].startswith("historical_dynamic_25d")
    assert distribution["surface_cadence_seconds"] == 300
    assert distribution["n_sessions"] == 2
    assert distribution["sticky_iv_baseline"]["method"] == "sticky_iv_same_spot_paths.v1"
    assert "joint_spot_atm_skew_curvature_replay" in distribution["reason_codes"]


def test_joint_surface_loader_rejects_snapshots_available_after_decision(tmp_path: Path) -> None:
    _write_surface_session(tmp_path, "2026-08-04", start_et=time(10, 30), count=6)
    _write_surface_session(
        tmp_path,
        "2026-08-05",
        start_et=time(10, 30),
        count=6,
        created_at=RTH_NOW + timedelta(minutes=1),
    )

    rows, mode = load_joint_surface_paths(
        _facts(now=RTH_NOW),
        data_root=tmp_path,
        probability_settings=StrategyDistributionSettings(),
        now=RTH_NOW,
        horizon_minutes=20,
    )

    assert mode == "same_session_clock_5m"
    assert [row.session_date for row in rows] == [date(2026, 8, 4)]


def test_trigger_level_is_not_counted_as_a_protective_stop(tmp_path: Path) -> None:
    _write_session(
        tmp_path, "2026-08-05", start_et=time(9, 30), prices=_full_rth_prices(7750.0, 0.1)
    )
    candidate = _call_vertical()
    candidate["invalidation_spx"] = 7800.0
    distribution = estimate_path_distribution(
        candidate,
        _facts(now=RTH_NOW, spot=7750.0),
        data_root=tmp_path,
        probability_settings=StrategyDistributionSettings(),
        now=RTH_NOW,
    )
    assert distribution["hit_invalidation_rate"] is None
    assert "invalidation_not_protective" in distribution["reason_codes"]


def _call_butterfly() -> dict[str, object]:
    legs = [
        _leg(7740.0, "C", 11.2, 11.4),
        _leg(7750.0, "C", 6.2, 6.4),
        _leg(7760.0, "C", 2.6, 2.8),
    ]
    return {
        "strategy_type": "CALL_BUTTERFLY",
        "setup_kind": "STABLE_PIN",
        "right": "C",
        "direction": "NEUTRAL",
        "center": 7750.0,
        "width": 10.0,
        "legs": legs,
        "quote": {"status": "ready", "bid": 0.4, "ask": 0.8},
        "economics": {
            "width_points": 10.0,
            "max_loss_points": 0.8,
            "max_gain_points": 9.2,
        },
        "invalidation_spx": [7740.0, 7760.0],
    }


def test_butterfly_uses_three_leg_path_and_pin_management_policy(tmp_path: Path) -> None:
    _write_session(
        tmp_path, "2026-08-04", start_et=time(9, 30), prices=_full_rth_prices(7750.0, 0.03)
    )
    _write_session(
        tmp_path, "2026-08-05", start_et=time(9, 30), prices=_full_rth_prices(7750.0, -0.03)
    )
    distribution = estimate_path_distribution(
        _call_butterfly(),
        _facts(now=RTH_NOW),
        data_root=tmp_path,
        probability_settings=StrategyDistributionSettings(),
        now=RTH_NOW,
    )

    assert distribution["status"] == "estimated_uncalibrated"
    assert distribution["method"] == "physical_path_management_policy.v3"
    assert distribution["management_policy_version"] == "management_policy.pin_butterfly.hold_1545.v1"
    assert distribution["premium_stop_rate"] == 0.0
    assert distribution["pnl_histogram"]
    assert distribution["risk_objective"]["status"] == "available"


def test_missing_data_root_returns_unavailable_without_raising() -> None:
    distribution = estimate_path_distribution(
        _call_vertical(),
        _facts(now=RTH_NOW),
        data_root=None,
        probability_settings=None,
        now=RTH_NOW,
    )
    assert distribution["status"] == "unavailable"
    assert "physical_spot_paths_unavailable" in distribution["reason_codes"]


def _write_open_to_clear(root: Path, day: str, *, open_px: float, clear_px: float) -> None:
    n = 181
    prices = [open_px + (clear_px - open_px) * index / (n - 1) for index in range(n)]
    _write_session(root, day, start_et=time(9, 30), prices=prices)


def _iron_condor(*, session_mode: str | None = None) -> dict[str, object]:
    put_long = _leg(7680.0, "P", 1.1, 1.3)
    put_short = _leg(7690.0, "P", 1.6, 1.8)
    call_short = _leg(7810.0, "C", 1.6, 1.8)
    call_long = _leg(7820.0, "C", 1.1, 1.3)
    candidate: dict[str, object] = {
        "strategy_type": "IRON_CONDOR",
        "setup_kind": "IRON_CONDOR_DELTA",
        "legs": [put_long, put_short, call_short, call_long],
        "put_short": {"strike": 7690.0, "delta": -0.20},
        "call_short": {"strike": 7810.0, "delta": 0.20},
        "quote": {"status": "ready", "bid": 0.6, "ask": 1.0, "credit": 0.8},
        "economics": {
            "width_points": 10.0,
            "max_loss_points": 9.2,
            "max_gain_points": 0.8,
        },
        "invalidation_spx": [7690.0, 7810.0],
    }
    if session_mode is not None:
        candidate["session_mode"] = session_mode
    return candidate


def test_gth_iron_condor_clearing_paths_are_one_overnight_session(tmp_path: Path) -> None:
    _write_open_to_clear(tmp_path, "2026-08-04", open_px=7700.0, clear_px=7702.0)
    _write_open_to_clear(tmp_path, "2026-08-05", open_px=7703.0, clear_px=7705.0)

    rows, mode = load_iron_condor_clearing_paths(
        tmp_path / "features",
        now=GTH_NOW,
        trading_date=date(2026, 8, 6),
        window_days=35,
    )

    assert mode == "overnight_gap_and_rth_to_clear"
    assert len(rows) == 1
    assert rows[0].session_date == date(2026, 8, 5)
    assert rows[0].overnight_gap == 1.0
    assert len(rows[0].prices) == 181


def test_iron_condor_does_not_reuse_twenty_minute_debit_policy() -> None:
    distribution = estimate_path_distribution(
        _iron_condor(),
        _facts(now=GTH_NOW, spot=7750.0),
        data_root=None,
        probability_settings=None,
        now=GTH_NOW,
    )

    assert distribution["status"] == "unavailable"
    assert "iron_condor_uses_clearing_overlay" in distribution["reason_codes"]
    assert distribution["p10_pnl_points"] is None
    assert distribution["p50_pnl_points"] is None
    assert distribution["p90_pnl_points"] is None


def test_iron_condor_path_holds_to_1230_et_not_twenty_minutes(tmp_path: Path) -> None:
    _write_open_to_clear(tmp_path, "2026-08-04", open_px=7750.0, clear_px=7751.0)
    _write_open_to_clear(tmp_path, "2026-08-05", open_px=7751.0, clear_px=7752.0)

    distribution = estimate_iron_condor_clearing_distribution(
        _iron_condor(),
        _facts(now=GTH_NOW, spot=7750.0),
        data_root=tmp_path,
        probability_settings=StrategyDistributionSettings(),
        now=GTH_NOW,
    )

    assert distribution["method"] == "physical_path_iron_condor_clear_1230.v1"
    assert distribution["hard_exit_et"] == "12:30"
    assert distribution["time_stop_rate"] == 0.0
    assert distribution["median_hold_minutes"] > 20
    assert distribution["p10_pnl_points"] <= distribution["p50_pnl_points"] <= distribution["p90_pnl_points"]
    assert distribution["pnl_histogram"]
    assert distribution["risk_objective"]["status"] == "available"
    assert distribution["risk_objective"]["automatic_ordering"] is False
    text = path_distribution_desk_text(distribution)
    assert text is not None
    assert text.startswith("GTH旧研究·次日12:30ET前 路径 P10/P50/P90 $")
    assert text.endswith("样本不足，仅研究")


def test_gth_iron_condor_uses_joint_surface_path_and_sticky_comparison(
    tmp_path: Path,
) -> None:
    _write_surface_session(tmp_path, "2026-08-04", start_et=time(0, 30), count=145)
    _write_surface_session(
        tmp_path, "2026-08-05", start_et=time(0, 30), count=145, atm_step=-0.0001
    )

    distribution = estimate_iron_condor_clearing_distribution(
        _iron_condor(),
        _facts(now=GTH_NOW, spot=7750.0),
        data_root=tmp_path,
        probability_settings=StrategyDistributionSettings(),
        now=GTH_NOW,
    )

    assert distribution["method"] == "joint_spot_surface_iron_condor_clear_1230.v1"
    assert distribution["n_sessions"] == 2
    assert distribution["surface_cadence_seconds"] == 300
    assert distribution["sticky_iv_baseline"]["method"] == "sticky_iv_same_spot_paths.v1"
    assert distribution["hard_exit_et"] == "12:30"


def test_rth_iron_condor_map_uses_tp50_sl200_hold1545_policy(tmp_path: Path) -> None:
    _write_session(
        tmp_path,
        "2026-08-04",
        start_et=time(9, 30),
        prices=_full_rth_prices(7750.0, 0.01),
    )
    _write_session(
        tmp_path,
        "2026-08-05",
        start_et=time(9, 30),
        prices=_full_rth_prices(7750.0, -0.01),
    )
    structure = {**_iron_condor(session_mode="rth"), "status": "ready"}

    result = attach_iron_condor_path_distribution(
        structure,
        _facts(now=RTH_NOW),
        data_root=tmp_path,
        probability_settings=StrategyDistributionSettings(),
        now=RTH_NOW,
    )

    distribution = result["path_distribution"]
    assert distribution["method"] == "physical_path_management_policy.v3"
    assert distribution["management_policy_version"] == (
        "management_policy.iron_condor.tp50_sl200_hold1545.v2"
    )
    assert distribution["hard_exit_et"] == "15:45"
    assert distribution["premium_stop_rate"] == 0.0
    assert "stop_loss_rate" in distribution
    text = path_distribution_desk_text(distribution)
    assert text is not None
    assert text.startswith("RTH 0.5C止盈/3C止损·最迟15:45ET 路径 P10/P50/P90 $")


def test_rth_iron_condor_joint_surface_replay_keeps_credit_policy(tmp_path: Path) -> None:
    _write_surface_session(tmp_path, "2026-08-04", start_et=time(10, 30), count=64)
    _write_surface_session(
        tmp_path,
        "2026-08-05",
        start_et=time(10, 30),
        count=64,
        atm_step=-0.0001,
    )
    structure = {**_iron_condor(session_mode="rth"), "status": "ready"}

    result = attach_iron_condor_path_distribution(
        structure,
        _facts(now=RTH_NOW),
        data_root=tmp_path,
        probability_settings=StrategyDistributionSettings(),
        now=RTH_NOW,
    )

    distribution = result["path_distribution"]
    assert distribution["method"] == "joint_spot_surface_management_policy.v1"
    assert distribution["management_policy_version"] == (
        "management_policy.iron_condor.tp50_sl200_hold1545.v2"
    )
    assert distribution["hard_exit_et"] == "15:45"
    assert distribution["premium_stop_rate"] == 0.0
    assert "stop_loss_rate" in distribution
