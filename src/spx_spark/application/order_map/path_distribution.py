"""Advisory path-forward PnL for ranked winners and the iron-condor map.

Replays completed prior-session 1-minute SPX windows through sticky-IV
Black-Scholes marks and the frozen management policy. The distribution is
display-only: it does not veto, re-rank, or authorize orders.
"""

from __future__ import annotations

import math
from collections.abc import Mapping, Sequence
from datetime import date, datetime, time, timedelta, timezone
from pathlib import Path
from statistics import median
from time import perf_counter
from typing import Any
from zoneinfo import ZoneInfo

import numpy as np
from scipy.special import ndtr

from spx_spark.analytics.greeks.black_scholes import bs_price, intrinsic_value
from spx_spark.analytics.options.pricing import time_to_expiry_years
from spx_spark.analytics.options.strategy_payoff import (
    DEFAULT_MANAGEMENT_POLICY,
    IRON_CONDOR_MANAGEMENT_POLICY,
    PolicyMark,
    policy_mark_horizon_end,
    simulate_management_policy,
)
from spx_spark.application.market_features.physical_followthrough import (
    ClearingSpotPath,
    PhysicalSpotPath,
    RTH_OPEN_MINUTE,
    load_iron_condor_clearing_paths,
    load_physical_spot_paths,
)
from spx_spark.application.order_map.strategy_regime import StrategyPolicy
from spx_spark.settings.strategy_distribution import StrategyDistributionSettings

METHOD = "physical_path_management_policy.v2"
IRON_CONDOR_CLEARING_METHOD = "physical_path_iron_condor_clear_1230.v1"
SUPPORTED_VERTICALS = {"CALL_DEBIT_VERTICAL", "PUT_DEBIT_VERTICAL"}
IRON_CONDOR_TYPE = "IRON_CONDOR"
MAX_PATHS = 4000
NEW_YORK = ZoneInfo("America/New_York")


def attach_path_distribution(
    candidate: Mapping[str, Any],
    facts: Mapping[str, Any],
    *,
    data_root: str | Path | None,
    probability_settings: StrategyDistributionSettings | None,
    now: datetime,
    policy: StrategyPolicy | None = None,
    paths: tuple[PhysicalSpotPath, ...] | None = None,
    clock_mode: str | None = None,
) -> dict[str, Any]:
    """Copy ``candidate`` and hang the advisory distribution on ``edge``."""

    distribution = estimate_path_distribution(
        candidate,
        facts,
        data_root=data_root,
        probability_settings=probability_settings,
        now=now,
        policy=policy,
        paths=paths,
        clock_mode=clock_mode,
    )
    edge = dict(_map(candidate.get("edge")))
    edge["path_distribution"] = distribution
    return {**dict(candidate), "edge": edge}


def attach_iron_condor_path_distribution(
    structure: Mapping[str, Any],
    facts: Mapping[str, Any],
    *,
    data_root: str | Path | None,
    probability_settings: StrategyDistributionSettings | None,
    now: datetime,
    policy: StrategyPolicy | None = None,
    paths: tuple[PhysicalSpotPath, ...] | None = None,
    clock_mode: str | None = None,
) -> dict[str, Any]:
    """Hang the 12:30 ET clearing-window distribution on the iron-condor map."""

    del policy, paths, clock_mode
    result = dict(structure)
    if str(structure.get("status") or "") != "ready":
        result["path_distribution"] = _unavailable("iron_condor_not_ready")
        return result
    result["path_distribution"] = estimate_iron_condor_clearing_distribution(
        _iron_condor_as_candidate(structure),
        facts,
        data_root=data_root,
        probability_settings=probability_settings,
        now=now,
    )
    return result


def load_decision_spot_paths(
    facts: Mapping[str, Any],
    *,
    data_root: str | Path | None,
    probability_settings: StrategyDistributionSettings | None,
    now: datetime,
    policy: StrategyPolicy | None = None,
) -> tuple[tuple[PhysicalSpotPath, ...], str]:
    """Load the shared path library once per strategy decision."""

    del policy
    if data_root is None:
        return (), "unavailable"
    session_date = _session_date(facts.get("session_date"))
    if session_date is None:
        return (), "unavailable"
    settings = probability_settings or StrategyDistributionSettings()
    horizon_end = policy_mark_horizon_end(
        _utc(now),
        DEFAULT_MANAGEMENT_POLICY,
        session_date=session_date,
    )
    horizon = int((horizon_end - _utc(now)).total_seconds() // 60)
    if horizon <= 0:
        return (), "unavailable"
    try:
        return load_physical_spot_paths(
            Path(data_root).expanduser() / "features",
            now=_utc(now),
            trading_date=session_date,
            window_days=settings.window_days,
            horizon_minutes=horizon,
            minimum_same_clock=settings.minimum_physical_samples,
            max_paths=MAX_PATHS,
        )
    except ValueError:
        return (), "unavailable"


def estimate_path_distribution(
    candidate: Mapping[str, Any],
    facts: Mapping[str, Any],
    *,
    data_root: str | Path | None,
    probability_settings: StrategyDistributionSettings | None,
    now: datetime,
    policy: StrategyPolicy | None = None,
    paths: tuple[PhysicalSpotPath, ...] | None = None,
    clock_mode: str | None = None,
) -> dict[str, Any]:
    """Return P10/P50/P90 management-policy PnL for one structure."""

    del policy
    started = perf_counter()
    strategy_type = str(candidate.get("strategy_type") or "")
    if strategy_type.endswith("_BUTTERFLY"):
        return _unavailable("butterfly_path_not_in_v1")
    if strategy_type == IRON_CONDOR_TYPE:
        return _unavailable("iron_condor_uses_clearing_overlay")
    if strategy_type not in SUPPORTED_VERTICALS:
        return _unavailable("unsupported_strategy_type")

    spot = _number(_map(facts.get("spot")).get("spx"))
    if spot is None or spot <= 0:
        return _unavailable("spx_price_unavailable")
    legs = _structure_legs(candidate)
    if not legs:
        return _unavailable("path_legs_unavailable")
    quote = _map(candidate.get("quote"))
    entry = _entry_level(candidate, quote)
    if entry is None or entry <= 0:
        return _unavailable("path_entry_unavailable")
    expiry = _expiry_from_legs(legs)
    if not expiry:
        return _unavailable("vertical_expiry_unavailable")

    now_utc = _utc(now)
    tau0 = time_to_expiry_years(expiry, as_of=now_utc)
    priced_legs = _sticky_legs(legs, spot=spot, tau_years=tau0)
    if priced_legs is None:
        return _unavailable("path_iv_unavailable")

    loaded_mode = clock_mode
    if paths is None:
        paths, loaded_mode = load_decision_spot_paths(
            facts,
            data_root=data_root,
            probability_settings=probability_settings,
            now=now_utc,
        )
    if not paths:
        return _unavailable("physical_spot_paths_unavailable")

    remaining_em = _number(_map(facts.get("volatility")).get("expected_move_points"))
    minutes_to_close = _number(facts.get("minutes_to_close"))
    horizon = len(paths[0].prices) - 1
    scale, scale_reason = _path_scale(
        paths,
        remaining_em=remaining_em,
        minutes_to_close=minutes_to_close,
        horizon_minutes=horizon,
    )
    credit = strategy_type == IRON_CONDOR_TYPE
    close_seed = _number(quote.get("ask")) if credit else _number(quote.get("bid"))
    if close_seed is None:
        return _unavailable("path_mark_seed_unavailable")
    model0 = _model_mid(priced_legs, spot=spot, tau_years=tau0)
    combo_bids = _combo_bid_matrix(
        paths,
        legs=priced_legs,
        expiry=expiry,
        now=now_utc,
        spot=spot,
        scale=scale,
        model0=model0,
        close_seed=close_seed,
        entry_credit=entry if credit else None,
    )
    invalidation, invalidation_reason = _invalidation_touch(
        candidate, credit=credit, spot=spot
    )

    pnls: list[float] = []
    hold_minutes: list[float] = []
    hit_invalidation = 0
    tp_before_stop = 0
    premium_stops = 0
    for index in range(len(paths)):
        projected = tuple(float(value) for value in combo_bids["spots"][index])
        if invalidation is not None and invalidation(projected):
            hit_invalidation += 1
        marks = [
            PolicyMark(at=now_utc + timedelta(minutes=offset), combo_bid=float(bid))
            for offset, bid in enumerate(combo_bids["bids"][index])
        ]
        label = simulate_management_policy(
            marks,
            entry_ask=entry,
            leg_count=len(priced_legs),
            entry_at=now_utc,
            policy=DEFAULT_MANAGEMENT_POLICY,
        )
        pnls.append(label.policy_pnl_points)
        if label.exit_at is not None:
            hold_minutes.append((label.exit_at - now_utc).total_seconds() / 60.0)
        if label.tp_before_stop:
            tp_before_stop += 1
        if label.exit_reason == "premium_stop":
            premium_stops += 1

    count = len(pnls)
    p10, p50, p90 = _percentiles(pnls, (10.0, 50.0, 90.0))
    sessions = sorted({row.session_date.isoformat() for row in paths})
    settings = probability_settings or StrategyDistributionSettings()
    status = (
        "estimated_uncalibrated"
        if count >= settings.minimum_physical_samples
        else "insufficient_sample"
    )
    reasons = [
        "research_unvalidated",
        "not_fill_probability",
        "sticky_iv_conservative_mark",
        METHOD,
    ]
    if loaded_mode == "session_shape_fallback":
        reasons.append("gth_or_sparse_clock_uses_rth_shapes")
    if scale_reason:
        reasons.append(scale_reason)
    if invalidation_reason:
        reasons.append(invalidation_reason)
    if status == "insufficient_sample":
        reasons.append("physical_sample_below_minimum")
    hit_rate = (
        None
        if invalidation is None
        else round(hit_invalidation / count, 4)
    )
    return {
        "status": status,
        "method": METHOD,
        "evidence_status": "research_unvalidated",
        "p10_pnl_points": p10,
        "p50_pnl_points": p50,
        "p90_pnl_points": p90,
        "p10_net_pnl": _dollars(p10),
        "p50_net_pnl": _dollars(p50),
        "p90_net_pnl": _dollars(p90),
        "hit_invalidation_rate": hit_rate,
        "tp_before_stop_rate": round(tp_before_stop / count, 4),
        "premium_stop_rate": round(premium_stops / count, 4),
        "median_hold_minutes": round(median(hold_minutes), 3) if hold_minutes else None,
        "n_paths": count,
        "n_sessions": len(sessions),
        "n_same_clock": sum(1 for row in paths if row.same_clock),
        "historical_sessions": sessions,
        "clock_mode": loaded_mode or "unavailable",
        "horizon_minutes": horizon,
        "scale": round(scale, 6),
        "remaining_expected_move": remaining_em,
        "compute_ms": round((perf_counter() - started) * 1000.0, 1),
        "reason_codes": reasons,
    }


def estimate_iron_condor_clearing_distribution(
    candidate: Mapping[str, Any],
    facts: Mapping[str, Any],
    *,
    data_root: str | Path | None,
    probability_settings: StrategyDistributionSettings | None,
    now: datetime,
) -> dict[str, Any]:
    """Hold a GTH/RTH iron condor to the 12:00–13:00 ET clearing window."""

    started = perf_counter()
    if str(candidate.get("strategy_type") or "") != IRON_CONDOR_TYPE:
        return _unavailable("unsupported_strategy_type")
    spot = _number(_map(facts.get("spot")).get("spx"))
    if spot is None or spot <= 0:
        return _unavailable("spx_price_unavailable")
    legs = _structure_legs(candidate)
    if not legs:
        return _unavailable("path_legs_unavailable")
    quote = _map(candidate.get("quote"))
    entry = _entry_level(candidate, quote)
    if entry is None or entry <= 0:
        return _unavailable("path_entry_unavailable")
    expiry = _expiry_from_legs(legs)
    if not expiry:
        return _unavailable("vertical_expiry_unavailable")
    session_date = _session_date(facts.get("session_date"))
    if session_date is None:
        return _unavailable("session_date_unavailable")
    if data_root is None:
        return _unavailable("physical_spot_paths_unavailable")

    now_utc = _utc(now)
    tau0 = time_to_expiry_years(expiry, as_of=now_utc)
    priced_legs = _sticky_legs(legs, spot=spot, tau_years=tau0)
    if priced_legs is None:
        return _unavailable("path_iv_unavailable")
    close_seed = _number(quote.get("ask"))
    if close_seed is None:
        return _unavailable("path_mark_seed_unavailable")

    settings = probability_settings or StrategyDistributionSettings()
    try:
        paths, loaded_mode = load_iron_condor_clearing_paths(
            Path(data_root).expanduser() / "features",
            now=now_utc,
            trading_date=session_date,
            window_days=settings.window_days,
        )
    except ValueError:
        return _unavailable("physical_spot_paths_unavailable")
    if loaded_mode == "past_clearing_window":
        return _unavailable("past_iron_condor_clearing_window")
    if not paths:
        return _unavailable("physical_spot_paths_unavailable")

    clocks, combo_bids = _clearing_combo_bids(
        paths,
        legs=priced_legs,
        expiry=expiry,
        now=now_utc,
        spot=spot,
        session_date=session_date,
        close_seed=close_seed,
        entry_credit=entry,
    )
    invalidation, invalidation_reason = _invalidation_touch(
        candidate, credit=True, spot=spot
    )
    pnls: list[float] = []
    hold_minutes: list[float] = []
    hit_invalidation = 0
    tp_before_stop = 0
    premium_stops = 0
    hard_closes = 0
    time_stops = 0
    for index in range(len(paths)):
        projected = tuple(float(value) for value in combo_bids["spots"][index])
        if invalidation is not None and invalidation(projected):
            hit_invalidation += 1
        marks = [
            PolicyMark(at=clock, combo_bid=float(bid))
            for clock, bid in zip(clocks, combo_bids["bids"][index], strict=True)
        ]
        label = simulate_management_policy(
            marks,
            entry_ask=entry,
            leg_count=len(priced_legs),
            entry_at=now_utc,
            policy=IRON_CONDOR_MANAGEMENT_POLICY,
            session_date=session_date,
        )
        pnls.append(label.policy_pnl_points)
        if label.exit_at is not None:
            hold_minutes.append((label.exit_at - now_utc).total_seconds() / 60.0)
        if label.tp_before_stop:
            tp_before_stop += 1
        if label.exit_reason == "premium_stop":
            premium_stops += 1
        elif label.exit_reason == "hard_close":
            hard_closes += 1
        elif label.exit_reason == "time_stop":
            time_stops += 1

    count = len(pnls)
    p10, p50, p90 = _percentiles(pnls, (10.0, 50.0, 90.0))
    sessions = sorted({row.session_date.isoformat() for row in paths})
    status = (
        "estimated_uncalibrated"
        if count >= settings.minimum_physical_samples
        else "insufficient_sample"
    )
    reasons = [
        "research_unvalidated",
        "not_fill_probability",
        "sticky_iv_conservative_mark",
        IRON_CONDOR_CLEARING_METHOD,
        "unscaled_clearing_session_paths",
        loaded_mode,
    ]
    if invalidation_reason:
        reasons.append(invalidation_reason)
    if status == "insufficient_sample":
        reasons.append("physical_sample_below_minimum")
    hit_rate = None if invalidation is None else round(hit_invalidation / count, 4)
    return {
        "status": status,
        "method": IRON_CONDOR_CLEARING_METHOD,
        "evidence_status": "research_unvalidated",
        "p10_pnl_points": p10,
        "p50_pnl_points": p50,
        "p90_pnl_points": p90,
        "p10_net_pnl": _dollars(p10),
        "p50_net_pnl": _dollars(p50),
        "p90_net_pnl": _dollars(p90),
        "hit_invalidation_rate": hit_rate,
        "tp_before_stop_rate": round(tp_before_stop / count, 4),
        "premium_stop_rate": round(premium_stops / count, 4),
        "hard_close_rate": round(hard_closes / count, 4),
        "time_stop_rate": round(time_stops / count, 4),
        "median_hold_minutes": round(median(hold_minutes), 3) if hold_minutes else None,
        "n_paths": count,
        "n_sessions": len(sessions),
        "n_same_clock": 0,
        "historical_sessions": sessions,
        "clock_mode": loaded_mode,
        "horizon_minutes": max(int((clocks[-1] - now_utc).total_seconds() / 60.0), 0),
        "scale": 1.0,
        "remaining_expected_move": _number(_map(facts.get("volatility")).get("expected_move_points")),
        "compute_ms": round((perf_counter() - started) * 1000.0, 1),
        "reason_codes": reasons,
        "management_policy_version": IRON_CONDOR_MANAGEMENT_POLICY.policy_version,
        "hard_exit_et": IRON_CONDOR_MANAGEMENT_POLICY.hard_exit_et,
    }


def _clearing_combo_bids(
    paths: Sequence[ClearingSpotPath],
    *,
    legs: Sequence[Mapping[str, Any]],
    expiry: str,
    now: datetime,
    spot: float,
    session_date: date,
    close_seed: float,
    entry_credit: float,
) -> tuple[list[datetime], dict[str, np.ndarray]]:
    morning = np.asarray([path.prices for path in paths], dtype=float)
    gaps = np.asarray([path.overnight_gap for path in paths], dtype=float)
    start_minute = paths[0].start_minute
    include_gth = start_minute <= RTH_OPEN_MINUTE and now.astimezone(NEW_YORK).date() <= session_date
    clocks = _clearing_clocks(
        now,
        session_date,
        start_minute=start_minute,
        n_prices=morning.shape[1],
        include_gth_now=include_gth,
    )
    relative = morning - morning[:, :1]
    open_spots = spot + gaps
    if len(clocks) == morning.shape[1] + 1:
        spots = np.concatenate(
            (
                np.full((len(paths), 1), spot, dtype=float),
                open_spots[:, None] + relative,
            ),
            axis=1,
        )
    else:
        spots = open_spots[:, None] + relative
    tau0 = time_to_expiry_years(expiry, as_of=now)
    model0 = _model_mid(legs, spot=spot, tau_years=tau0)
    model = np.zeros((len(paths), len(clocks)), dtype=float)
    for offset, clock in enumerate(clocks):
        tau = time_to_expiry_years(expiry, as_of=clock)
        column = spots[:, offset]
        for leg in legs:
            model[:, offset] += float(leg["quantity"]) * _bs_price_np(
                column,
                float(leg["strike"]),
                float(leg["implied_vol"]),
                tau,
                str(leg["right"]),
            )
    close_mark = np.maximum(close_seed + (model - model0), 0.0)
    bids = np.maximum(2.0 * entry_credit - close_mark, 0.0)
    return clocks, {"spots": spots, "bids": bids}


def _clearing_clocks(
    now: datetime,
    session_date: date,
    *,
    start_minute: int,
    n_prices: int,
    include_gth_now: bool,
) -> list[datetime]:
    start_local = datetime.combine(
        session_date,
        time(hour=start_minute // 60, minute=start_minute % 60),
        tzinfo=NEW_YORK,
    )
    morning = [
        (start_local + timedelta(minutes=offset)).astimezone(timezone.utc)
        for offset in range(n_prices)
    ]
    now_utc = _utc(now)
    if include_gth_now and now_utc < morning[0]:
        return [now_utc, *morning]
    return morning


def path_distribution_desk_text(distribution: Mapping[str, Any] | None) -> str | None:
    """Compact P10/P50/P90 line for Desk Map and trade cards."""

    if not isinstance(distribution, Mapping):
        return None
    if distribution.get("status") not in {"estimated_uncalibrated", "insufficient_sample"}:
        return None
    p10, p50, p90 = (distribution.get(key) for key in ("p10_net_pnl", "p50_net_pnl", "p90_net_pnl"))
    if not all(isinstance(value, int | float) and not isinstance(value, bool) for value in (p10, p50, p90)):
        return None
    n_paths = distribution.get("n_paths")
    sample = f" n={int(n_paths)}" if isinstance(n_paths, int | float) else ""
    prefix = (
        "持有至12:30ET "
        if distribution.get("method") == IRON_CONDOR_CLEARING_METHOD
        else "持有至15:45ET "
        if distribution.get("method") == METHOD
        else ""
    )
    return f"{prefix}路径 P10/P50/P90 ${float(p10):.0f}/${float(p50):.0f}/${float(p90):.0f}{sample}"


def _structure_legs(candidate: Mapping[str, Any]) -> list[dict[str, Any]]:
    raw = candidate.get("legs")
    if isinstance(raw, Sequence) and not isinstance(raw, (str, bytes)) and raw:
        rows = [dict(_map(item)) for item in raw]
        if all(row for row in rows):
            return rows
    long_leg, short_leg = _map(candidate.get("long")), _map(candidate.get("short"))
    if long_leg and short_leg:
        return [dict(long_leg), dict(short_leg)]
    return []


def _iron_condor_as_candidate(structure: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "strategy_type": IRON_CONDOR_TYPE,
        "setup_kind": structure.get("setup_kind"),
        "quote": dict(_map(structure.get("quote"))),
        "economics": dict(_map(structure.get("economics"))),
        "legs": [dict(_map(item)) for item in structure.get("legs") or ()],
        "put_short": dict(_map(structure.get("put_short"))),
        "call_short": dict(_map(structure.get("call_short"))),
        "invalidation_spx": [
            _number(_map(structure.get("put_short")).get("strike")),
            _number(_map(structure.get("call_short")).get("strike")),
        ],
    }


def _entry_level(candidate: Mapping[str, Any], quote: Mapping[str, Any]) -> float | None:
    if str(candidate.get("strategy_type") or "") == IRON_CONDOR_TYPE:
        return _number(quote.get("credit")) or _number(quote.get("bid"))
    return _number(quote.get("ask"))


def _sticky_legs(
    legs: Sequence[Mapping[str, Any]], *, spot: float, tau_years: float
) -> tuple[dict[str, Any], ...] | None:
    priced: list[dict[str, Any]] = []
    for index, leg in enumerate(legs):
        strike = _number(leg.get("strike"))
        right = str(leg.get("right") or "").upper()
        bid = _number(leg.get("bid"))
        ask = _number(leg.get("ask"))
        if strike is None or strike <= 0 or right not in {"C", "P"} or bid is None or ask is None:
            return None
        mid = 0.5 * (bid + ask)
        implied = _number(leg.get("implied_vol"))
        if implied is None or implied <= 0:
            implied = _invert_iv(spot, strike, tau_years, right, mid)
        if implied is None:
            return None
        quantity = _leg_quantity(leg, index=index, total=len(legs))
        priced.append(
            {
                "strike": strike,
                "right": right,
                "implied_vol": implied,
                "quantity": quantity,
            }
        )
    return tuple(priced)


def _leg_quantity(leg: Mapping[str, Any], *, index: int, total: int) -> int:
    explicit = leg.get("quantity")
    if isinstance(explicit, int) and not isinstance(explicit, bool):
        return explicit
    if total == 2:
        return 1 if index == 0 else -1
    if total == 4:
        return (1, -1, -1, 1)[index]
    return 1


def _invert_iv(
    spot: float, strike: float, tau_years: float, right: str, price: float
) -> float | None:
    if price <= 0 or spot <= 0 or strike <= 0:
        return None
    floor = intrinsic_value(spot, strike, right)
    if price <= floor + 0.01 or tau_years <= 0:
        return 0.0
    low, high = 1e-4, 5.0
    for _ in range(40):
        mid = 0.5 * (low + high)
        model = bs_price(spot, strike, mid, tau_years, right)
        if model < price:
            low = mid
        else:
            high = mid
    return 0.5 * (low + high)


def _model_mid(legs: Sequence[Mapping[str, Any]], *, spot: float, tau_years: float) -> float:
    return sum(
        float(leg["quantity"])
        * bs_price(spot, float(leg["strike"]), float(leg["implied_vol"]), tau_years, str(leg["right"]))
        for leg in legs
    )


def _combo_bid_matrix(
    paths: Sequence[PhysicalSpotPath],
    *,
    legs: Sequence[Mapping[str, Any]],
    expiry: str,
    now: datetime,
    spot: float,
    scale: float,
    model0: float,
    close_seed: float,
    entry_credit: float | None,
) -> dict[str, np.ndarray]:
    horizon = len(paths[0].prices)
    origins = np.asarray([path.prices[0] for path in paths], dtype=float)
    raw = np.asarray([path.prices for path in paths], dtype=float)
    spots = spot + scale * (raw - origins[:, None])
    model = np.zeros((len(paths), horizon), dtype=float)
    for offset in range(horizon):
        tau = time_to_expiry_years(expiry, as_of=now + timedelta(minutes=offset))
        column = spots[:, offset]
        for leg in legs:
            model[:, offset] += float(leg["quantity"]) * _bs_price_np(
                column,
                float(leg["strike"]),
                float(leg["implied_vol"]),
                tau,
                str(leg["right"]),
            )
    close_mark = np.maximum(close_seed + (model - model0), 0.0)
    if entry_credit is not None:
        bids = np.maximum(2.0 * entry_credit - close_mark, 0.0)
    else:
        bids = close_mark
    return {"spots": spots, "bids": bids}


def _bs_price_np(
    spot: np.ndarray, strike: float, iv: float, tau: float, right: str
) -> np.ndarray:
    if right == "C":
        intrinsic = np.maximum(spot - strike, 0.0)
    else:
        intrinsic = np.maximum(strike - spot, 0.0)
    if tau <= 0.0 or iv <= 0.0:
        return intrinsic
    safe = np.maximum(spot, 1e-12)
    root_t = math.sqrt(tau)
    d1_value = (np.log(safe / strike) + 0.5 * iv * iv * tau) / (iv * root_t)
    d2_value = d1_value - iv * root_t
    cdf1 = ndtr(d1_value)
    cdf2 = ndtr(d2_value)
    if right == "C":
        model = safe * cdf1 - strike * cdf2
    else:
        model = strike * (1.0 - cdf2) - safe * (1.0 - cdf1)
    return np.maximum(intrinsic, model)


def _path_scale(
    paths: Sequence[PhysicalSpotPath],
    *,
    remaining_em: float | None,
    minutes_to_close: float | None,
    horizon_minutes: int,
) -> tuple[float, str | None]:
    moves = [(row.prices[-1] - row.prices[0]) for row in paths]
    hist_rms = (sum(value * value for value in moves) / len(moves)) ** 0.5 if moves else 0.0
    if remaining_em is None or remaining_em <= 0:
        return 1.0, "remaining_em_unavailable_unscaled"
    if hist_rms <= 1e-6:
        return 1.0, "historical_path_move_degenerate"
    remaining_minutes = max(float(minutes_to_close or horizon_minutes), float(horizon_minutes))
    target = remaining_em * (float(horizon_minutes) / remaining_minutes) ** 0.5
    return target / hist_rms, None


def _invalidation_touch(
    candidate: Mapping[str, Any], *, credit: bool, spot: float
) -> tuple[Any, str | None]:
    raw = candidate.get("invalidation_spx")
    if credit and isinstance(raw, Sequence) and not isinstance(raw, (str, bytes)):
        low = _number(raw[0] if len(raw) > 0 else None)
        high = _number(raw[1] if len(raw) > 1 else None)
        if low is None or high is None or not low < spot < high:
            return None, "invalidation_not_protective"

        def _iron(spots: Sequence[float], *, lower=low, upper=high) -> bool:
            return any(value <= lower or value >= upper for value in spots)

        return _iron, None
    level = _number(raw)
    right = str(candidate.get("right") or "").upper()
    if level is None:
        return None, "invalidation_unavailable"
    if right == "P":
        if level <= spot:
            return None, "invalidation_not_protective"

        def _put(spots: Sequence[float], *, stop=level) -> bool:
            return any(value >= stop for value in spots)

        return _put, None
    if level >= spot:
        return None, "invalidation_not_protective"

    def _call(spots: Sequence[float], *, stop=level) -> bool:
        return any(value <= stop for value in spots)

    return _call, None


def _percentiles(values: Sequence[float], points: Sequence[float]) -> tuple[float | None, ...]:
    if not values:
        return tuple(None for _ in points)
    ordered = sorted(values)
    last = len(ordered) - 1
    result: list[float] = []
    for point in points:
        rank = (point / 100.0) * last
        lower = int(rank)
        upper = min(lower + 1, last)
        weight = rank - lower
        interpolated = ordered[lower] * (1.0 - weight) + ordered[upper] * weight
        result.append(round(interpolated, 6))
    return tuple(result)


def _expiry_from_legs(legs: Sequence[Mapping[str, Any]]) -> str | None:
    for leg in legs:
        parts = str(leg.get("contract_id") or "").split(":")
        if len(parts) >= 6:
            return parts[-3]
    return None


def _session_date(value: object) -> date | None:
    if isinstance(value, date) and not isinstance(value, datetime):
        return value
    if not isinstance(value, str) or not value:
        return None
    try:
        return date.fromisoformat(value)
    except ValueError:
        return None


def _unavailable(reason: str) -> dict[str, Any]:
    return {
        "status": "unavailable",
        "method": METHOD,
        "evidence_status": "research_unvalidated",
        "p10_pnl_points": None,
        "p50_pnl_points": None,
        "p90_pnl_points": None,
        "p10_net_pnl": None,
        "p50_net_pnl": None,
        "p90_net_pnl": None,
        "n_paths": 0,
        "n_sessions": 0,
        "reason_codes": ["research_unvalidated", reason],
    }


def _dollars(points: float | None) -> float | None:
    return None if points is None else round(points * 100.0, 2)


def _map(value: object) -> Mapping[str, Any]:
    return value if isinstance(value, Mapping) else {}


def _number(value: object) -> float | None:
    if isinstance(value, bool) or not isinstance(value, int | float):
        return None
    parsed = float(value)
    return parsed if math.isfinite(parsed) else None


def _utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        raise ValueError("path-distribution now must be timezone-aware")
    return value.astimezone(timezone.utc)


__all__ = [
    "IRON_CONDOR_CLEARING_METHOD",
    "METHOD",
    "attach_iron_condor_path_distribution",
    "attach_path_distribution",
    "estimate_iron_condor_clearing_distribution",
    "estimate_path_distribution",
    "load_decision_spot_paths",
    "path_distribution_desk_text",
]
