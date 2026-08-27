"""Causal joint SPX and IV-surface path replay for defined-risk structures."""

from __future__ import annotations

import json
import math
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import date, datetime, time, timedelta, timezone
from functools import lru_cache
from pathlib import Path
from statistics import median
from time import perf_counter
from typing import Any
from zoneinfo import ZoneInfo

import numpy as np
from scipy.special import ndtr

from spx_spark.analytics.options.pricing import time_to_expiry_years
from spx_spark.analytics.options.strategy_payoff import (
    IRON_CONDOR_MANAGEMENT_POLICY,
    ManagementPolicy,
    PolicyMark,
    risk_adjusted_cvar_objective,
    simulate_management_policy,
)
from spx_spark.iv_surface import IvSurfaceExpiry, IvSurfaceSnapshot, snapshot_from_dict
from spx_spark.market_calendar import DEFAULT_MARKET_CALENDAR
from spx_spark.settings.strategy_distribution import StrategyDistributionSettings

JOINT_SURFACE_METHOD = "joint_spot_surface_management_policy.v1"
JOINT_SURFACE_CLEARING_METHOD = "joint_spot_surface_iron_condor_clear_1230.v1"
JOINT_SURFACE_COORDINATE = "historical_dynamic_25d_to_entry_frozen_strikes.v1"
PHYSICAL_METHOD = "physical_path_management_policy.v3"
PHYSICAL_CLEARING_METHOD = "physical_path_iron_condor_clear_1230.v1"
SURFACE_CADENCE_MINUTES = 5
MAX_PATHS = 4000
NEW_YORK = ZoneInfo("America/New_York")


@dataclass(frozen=True)
class JointSurfacePath:
    session_date: date
    prices: tuple[float, ...]
    atm: tuple[float, ...]
    put_skew: tuple[float, ...]
    call_skew: tuple[float, ...]
    put_fly: tuple[float, ...]
    call_fly: tuple[float, ...]
    degraded_points: int
    same_clock: bool = True


def path_distribution_desk_text(distribution: Mapping[str, Any] | None) -> str | None:
    """Compact P10/P50/P90 line for Desk Map and trade cards."""

    if not isinstance(distribution, Mapping):
        return None
    if distribution.get("status") not in {"estimated_uncalibrated", "insufficient_sample"}:
        return None
    values = tuple(distribution.get(key) for key in ("p10_net_pnl", "p50_net_pnl", "p90_net_pnl"))
    if not all(isinstance(value, int | float) and not isinstance(value, bool) for value in values):
        return None
    n_paths = distribution.get("n_paths")
    sample = f" n={int(n_paths)}" if isinstance(n_paths, int | float) else ""
    method = distribution.get("method")
    prefix = (
        "持有至12:30ET "
        if method in {PHYSICAL_CLEARING_METHOD, JOINT_SURFACE_CLEARING_METHOD}
        else "持有至15:45ET "
        if method in {PHYSICAL_METHOD, JOINT_SURFACE_METHOD}
        else ""
    )
    p10, p50, p90 = (float(value) for value in values)
    return f"{prefix}路径 P10/P50/P90 ${p10:.0f}/${p50:.0f}/${p90:.0f}{sample}"


def load_joint_surface_paths(
    facts: Mapping[str, Any],
    *,
    data_root: str | Path | None,
    probability_settings: StrategyDistributionSettings | None,
    now: datetime,
    horizon_minutes: int,
) -> tuple[tuple[JointSurfacePath, ...], str]:
    """Load prior-session five-minute spot/surface paths without future leakage."""

    if data_root is None or horizon_minutes <= 0:
        return (), "unavailable"
    trading_date = _session_date(facts.get("session_date"))
    if trading_date is None:
        return (), "unavailable"
    now_utc = _utc(now)
    current_window = DEFAULT_MARKET_CALENDAR.spx_session_window(trading_date)
    if current_window is None:
        return (), "unavailable"
    start_offset = int(
        (now_utc.astimezone(NEW_YORK) - current_window.session_start).total_seconds()
        // 60
    )
    if start_offset < 0:
        return (), "unavailable"
    steps = max(int(math.ceil(horizon_minutes / SURFACE_CADENCE_MINUTES)), 1)
    targets = tuple(
        start_offset + index * SURFACE_CADENCE_MINUTES for index in range(steps + 1)
    )
    settings = probability_settings or StrategyDistributionSettings()
    root = Path(data_root).expanduser() / "features" / "iv_surface"
    grouped: dict[
        date, list[tuple[int, int, IvSurfaceSnapshot, IvSurfaceExpiry]]
    ] = {}
    day = trading_date - timedelta(days=settings.window_days + 4)
    while day < trading_date:
        for path in sorted((root / f"date={day.isoformat()}").glob("hour=*/*.jsonl")):
            try:
                snapshots = _surface_file_snapshots(str(path), path.stat().st_mtime_ns)
            except OSError:
                continue
            for snapshot in snapshots:
                as_of, created_at = _utc(snapshot.as_of), _utc(snapshot.created_at)
                if as_of >= now_utc or created_at >= now_utc:
                    continue
                session_day = DEFAULT_MARKET_CALENDAR.spx_session_date_for(as_of)
                if session_day is None or session_day >= trading_date:
                    continue
                window = DEFAULT_MARKET_CALENDAR.spx_session_window(session_day)
                expiry = _front_surface(snapshot)
                if window is None or expiry is None or _surface_coordinate(snapshot, expiry) is None:
                    continue
                offset = int(
                    (as_of.astimezone(NEW_YORK) - window.session_start).total_seconds()
                    // 60
                )
                available_offset = int(
                    (created_at.astimezone(NEW_YORK) - window.session_start).total_seconds()
                    // 60
                )
                grouped.setdefault(session_day, []).append(
                    (offset, available_offset, snapshot, expiry)
                )
        day += timedelta(days=1)

    paths: list[JointSurfacePath] = []
    for session_day, rows in sorted(grouped.items()):
        rows.sort(key=lambda item: (item[0], item[1], item[2].as_of))
        selected = [_surface_row_at(rows, target=target) for target in targets]
        if any(item is None for item in selected):
            continue
        resolved = [item for item in selected if item is not None]
        coordinates = [item[0] for item in resolved]
        paths.append(
            JointSurfacePath(
                session_date=session_day,
                prices=tuple(item[0] for item in coordinates),
                atm=tuple(item[1] for item in coordinates),
                put_skew=tuple(item[2] for item in coordinates),
                call_skew=tuple(item[3] for item in coordinates),
                put_fly=tuple(item[4] for item in coordinates),
                call_fly=tuple(item[5] for item in coordinates),
                degraded_points=sum(item[1] for item in resolved),
            )
        )
    return tuple(paths[-MAX_PATHS:]), "same_session_clock_5m" if paths else "unavailable"


def estimate_joint_debit_distribution(
    candidate: Mapping[str, Any],
    facts: Mapping[str, Any],
    *,
    data_root: str | Path | None,
    probability_settings: StrategyDistributionSettings | None,
    now: datetime,
    horizon_minutes: int,
    spot: float,
    expiry: str,
    priced_legs: Sequence[Mapping[str, Any]],
    quote: Mapping[str, Any],
    entry: float,
    close_seed: float,
    policy: ManagementPolicy,
    started: float,
) -> dict[str, Any] | None:
    paths, mode = load_joint_surface_paths(
        facts,
        data_root=data_root,
        probability_settings=probability_settings,
        now=now,
        horizon_minutes=horizon_minutes,
    )
    if not paths:
        return None
    scale, scale_reason = _path_scale(paths, facts=facts, horizon_minutes=horizon_minutes)
    model0 = _model_mid(priced_legs, spot=spot, expiry=expiry, now=now)
    joint = _joint_combo_bid_matrix(
        paths,
        legs=priced_legs,
        candidate=candidate,
        facts=facts,
        expiry=expiry,
        now=now,
        spot=spot,
        scale=scale,
        model0=model0,
        close_seed=close_seed,
        entry_credit=None,
    )
    sticky = _sticky_combo_bid_matrix(
        paths,
        legs=priced_legs,
        expiry=expiry,
        now=now,
        spot=spot,
        scale=scale,
        model0=model0,
        close_seed=close_seed,
        entry_credit=None,
    )
    invalidation, invalidation_reason = _invalidation_touch(candidate, credit=False, spot=spot)
    simulation = _simulate(
        joint,
        paths=paths,
        entry=entry,
        leg_count=len(priced_legs),
        now=now,
        policy=policy,
        invalidation=invalidation,
    )
    return _distribution(
        simulation,
        candidate=candidate,
        quote=quote,
        paths=paths,
        settings=probability_settings or StrategyDistributionSettings(),
        method=JOINT_SURFACE_METHOD,
        mode=mode,
        scale=scale,
        scale_reason=scale_reason,
        invalidation_reason=invalidation_reason,
        horizon_minutes=(len(paths[0].prices) - 1) * SURFACE_CADENCE_MINUTES,
        remaining_em=_number(_map(facts.get("volatility")).get("expected_move_points")),
        policy=policy,
        started=started,
        sticky=sticky,
        entry=entry,
        leg_count=len(priced_legs),
        now=now,
        session_date=None,
    )


def estimate_joint_iron_condor_distribution(
    candidate: Mapping[str, Any],
    facts: Mapping[str, Any],
    *,
    data_root: str | Path | None,
    probability_settings: StrategyDistributionSettings | None,
    now: datetime,
    session_date: date,
    spot: float,
    expiry: str,
    priced_legs: Sequence[Mapping[str, Any]],
    quote: Mapping[str, Any],
    entry: float,
    close_seed: float,
    started: float,
) -> dict[str, Any] | None:
    target = datetime.combine(session_date, time(12, 30), tzinfo=NEW_YORK).astimezone(
        timezone.utc
    )
    horizon = int((target - now).total_seconds() // 60)
    if horizon <= 0:
        return None
    paths, mode = load_joint_surface_paths(
        facts,
        data_root=data_root,
        probability_settings=probability_settings,
        now=now,
        horizon_minutes=horizon,
    )
    if not paths:
        return None
    scale, scale_reason = _path_scale(paths, facts=facts, horizon_minutes=horizon)
    model0 = _model_mid(priced_legs, spot=spot, expiry=expiry, now=now)
    joint = _joint_combo_bid_matrix(
        paths,
        legs=priced_legs,
        candidate=candidate,
        facts=facts,
        expiry=expiry,
        now=now,
        spot=spot,
        scale=scale,
        model0=model0,
        close_seed=close_seed,
        entry_credit=entry,
    )
    sticky = _sticky_combo_bid_matrix(
        paths,
        legs=priced_legs,
        expiry=expiry,
        now=now,
        spot=spot,
        scale=scale,
        model0=model0,
        close_seed=close_seed,
        entry_credit=entry,
    )
    invalidation, invalidation_reason = _invalidation_touch(candidate, credit=True, spot=spot)
    policy = IRON_CONDOR_MANAGEMENT_POLICY
    simulation = _simulate(
        joint,
        paths=paths,
        entry=entry,
        leg_count=len(priced_legs),
        now=now,
        policy=policy,
        invalidation=invalidation,
        session_date=session_date,
    )
    return _distribution(
        simulation,
        candidate=candidate,
        quote=quote,
        paths=paths,
        settings=probability_settings or StrategyDistributionSettings(),
        method=JOINT_SURFACE_CLEARING_METHOD,
        mode=mode,
        scale=scale,
        scale_reason=scale_reason,
        invalidation_reason=invalidation_reason,
        horizon_minutes=(len(paths[0].prices) - 1) * SURFACE_CADENCE_MINUTES,
        remaining_em=_number(_map(facts.get("volatility")).get("expected_move_points")),
        policy=policy,
        started=started,
        sticky=sticky,
        entry=entry,
        leg_count=len(priced_legs),
        now=now,
        session_date=session_date,
    )


@lru_cache(maxsize=2048)
def _surface_file_snapshots(path: str, mtime_ns: int) -> tuple[IvSurfaceSnapshot, ...]:
    del mtime_ns
    rows: list[IvSurfaceSnapshot] = []
    for line in Path(path).read_text(encoding="utf-8").splitlines():
        if not line.strip():
            continue
        try:
            rows.append(snapshot_from_dict(json.loads(line)))
        except (json.JSONDecodeError, KeyError, TypeError, ValueError):
            continue
    return tuple(rows)


def _front_surface(snapshot: IvSurfaceSnapshot) -> IvSurfaceExpiry | None:
    return next(
        (item for item in snapshot.expiries if item.expiry == snapshot.front_expiry),
        snapshot.expiries[0] if snapshot.expiries else None,
    )


def _surface_row_at(
    rows: Sequence[tuple[int, int, IvSurfaceSnapshot, IvSurfaceExpiry]], *, target: int
) -> tuple[tuple[float, float, float, float, float, float], bool] | None:
    eligible = [item for item in rows if item[0] <= target and item[1] <= target]
    if not eligible:
        return None
    latest = max(eligible, key=lambda item: (item[0], item[1]))
    age = target - latest[0]
    coordinate = _surface_coordinate(latest[2], latest[3])
    if age < 0 or age > 30 or coordinate is None:
        return None
    return coordinate, latest[3].surface_fit_quality != "raw_grid" or age > 4


def _surface_coordinate(
    snapshot: IvSurfaceSnapshot, expiry: IvSurfaceExpiry
) -> tuple[float, float, float, float, float, float] | None:
    values = tuple(
        _number(value)
        for value in (
            snapshot.underlier_price,
            expiry.atm_iv,
            expiry.put_skew_25d,
            expiry.call_skew_25d,
            expiry.put_skew_ratio,
            expiry.call_skew_ratio,
        )
    )
    if any(value is None for value in values):
        return None
    spot, atm, put_skew, call_skew, put_ratio, call_ratio = (
        float(value) for value in values if value is not None
    )
    if spot <= 0 or atm <= 0:
        return None
    return (
        spot,
        atm,
        put_skew,
        call_skew,
        atm * (put_ratio - 1.0) - put_skew,
        atm * (call_ratio - 1.0) - call_skew,
    )


def _joint_combo_bid_matrix(
    paths: Sequence[JointSurfacePath],
    *,
    legs: Sequence[Mapping[str, Any]],
    candidate: Mapping[str, Any],
    facts: Mapping[str, Any],
    expiry: str,
    now: datetime,
    spot: float,
    scale: float,
    model0: float,
    close_seed: float,
    entry_credit: float | None,
) -> dict[str, np.ndarray]:
    raw_spots = np.asarray([path.prices for path in paths], dtype=float)
    origins = raw_spots[:, :1]
    spots = spot + scale * (raw_spots - origins)
    coordinates = {
        "atm": np.asarray([path.atm for path in paths], dtype=float),
        "put_skew": np.asarray([path.put_skew for path in paths], dtype=float),
        "call_skew": np.asarray([path.call_skew for path in paths], dtype=float),
        "put_fly": np.asarray([path.put_fly for path in paths], dtype=float),
        "call_fly": np.asarray([path.call_fly for path in paths], dtype=float),
    }
    shocks = {name: values - values[:, :1] for name, values in coordinates.items()}
    surface_scale = _surface_scale(candidate, facts, spot=spot, legs=legs)
    taus = np.asarray(
        [
            time_to_expiry_years(
                expiry,
                as_of=now + timedelta(minutes=offset * SURFACE_CADENCE_MINUTES),
            )
            for offset in range(spots.shape[1])
        ],
        dtype=float,
    )[None, :]
    model = np.zeros_like(spots)
    for leg in legs:
        strike = float(leg["strike"])
        put = min(max((spot - strike) / surface_scale, 0.0), 1.0)
        call = min(max((strike - spot) / surface_scale, 0.0), 1.0)
        iv = np.maximum(
            float(leg["implied_vol"])
            + shocks["atm"]
            + put * shocks["put_skew"]
            + call * shocks["call_skew"]
            + put * put * shocks["put_fly"]
            + call * call * shocks["call_fly"],
            1e-4,
        )
        model += float(leg["quantity"]) * _bs_price_np(
            spots, strike, iv, taus, str(leg["right"])
        )
    return _to_combo_bid(model, model0=model0, close_seed=close_seed, entry_credit=entry_credit, spots=spots)


def _sticky_combo_bid_matrix(
    paths: Sequence[JointSurfacePath],
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
    raw = np.asarray([path.prices for path in paths], dtype=float)
    spots = spot + scale * (raw - raw[:, :1])
    taus = np.asarray(
        [
            time_to_expiry_years(
                expiry,
                as_of=now + timedelta(minutes=offset * SURFACE_CADENCE_MINUTES),
            )
            for offset in range(spots.shape[1])
        ],
        dtype=float,
    )[None, :]
    model = np.zeros_like(spots)
    for leg in legs:
        model += float(leg["quantity"]) * _bs_price_np(
            spots,
            float(leg["strike"]),
            np.asarray(float(leg["implied_vol"])),
            taus,
            str(leg["right"]),
        )
    return _to_combo_bid(model, model0=model0, close_seed=close_seed, entry_credit=entry_credit, spots=spots)


def _to_combo_bid(
    model: np.ndarray,
    *,
    model0: float,
    close_seed: float,
    entry_credit: float | None,
    spots: np.ndarray,
) -> dict[str, np.ndarray]:
    close_mark = np.maximum(close_seed + (model - model0), 0.0)
    bids = (
        np.maximum(2.0 * entry_credit - close_mark, 0.0)
        if entry_credit is not None
        else close_mark
    )
    return {"spots": spots, "bids": bids}


def _simulate(
    combo: Mapping[str, np.ndarray],
    *,
    paths: Sequence[JointSurfacePath],
    entry: float,
    leg_count: int,
    now: datetime,
    policy: ManagementPolicy,
    invalidation: Any,
    session_date: date | None = None,
) -> dict[str, Any]:
    pnls: list[float] = []
    holds: list[float] = []
    counters = {"invalidation": 0, "tp": 0, "premium_stop": 0, "hard_close": 0, "time_stop": 0}
    for index, _path in enumerate(paths):
        projected = tuple(float(value) for value in combo["spots"][index])
        if invalidation is not None and invalidation(projected):
            counters["invalidation"] += 1
        marks = [
            PolicyMark(now + timedelta(minutes=offset * SURFACE_CADENCE_MINUTES), float(bid))
            for offset, bid in enumerate(combo["bids"][index])
        ]
        label = simulate_management_policy(
            marks,
            entry_ask=entry,
            leg_count=leg_count,
            entry_at=now,
            policy=policy,
            session_date=session_date,
        )
        pnls.append(label.policy_pnl_points)
        if label.exit_at is not None:
            holds.append((label.exit_at - now).total_seconds() / 60.0)
        counters["tp"] += int(label.tp_before_stop)
        if label.exit_reason in counters:
            counters[label.exit_reason] += 1
    return {"pnls": pnls, "holds": holds, "counters": counters}


def _distribution(
    simulation: Mapping[str, Any],
    *,
    candidate: Mapping[str, Any],
    quote: Mapping[str, Any],
    paths: Sequence[JointSurfacePath],
    settings: StrategyDistributionSettings,
    method: str,
    mode: str,
    scale: float,
    scale_reason: str | None,
    invalidation_reason: str | None,
    horizon_minutes: int,
    remaining_em: float | None,
    policy: ManagementPolicy,
    started: float,
    sticky: Mapping[str, np.ndarray],
    entry: float,
    leg_count: int,
    now: datetime,
    session_date: date | None,
) -> dict[str, Any]:
    pnls = list(simulation["pnls"])
    counters, count = simulation["counters"], len(pnls)
    sessions = sorted(path.session_date.isoformat() for path in paths)
    p10, p50, p90 = _percentiles(pnls, (10.0, 50.0, 90.0))
    status = "estimated_uncalibrated" if count >= settings.minimum_physical_samples else "insufficient_sample"
    reasons = [
        "research_unvalidated",
        "not_fill_probability",
        method,
        "joint_spot_atm_skew_curvature_replay",
        "surface_causal_carry_max_30m",
        "surface_wide_quotes_may_be_degraded",
        mode,
    ]
    reasons.extend(reason for reason in (scale_reason, invalidation_reason) if reason)
    if status == "insufficient_sample":
        reasons.append("physical_sample_below_minimum")
    objective = _risk_objective(pnls, candidate=candidate, quote=quote, session_count=len(sessions))
    result = {
        "status": status,
        "method": method,
        "evidence_status": "research_unvalidated",
        "p10_pnl_points": p10,
        "p50_pnl_points": p50,
        "p90_pnl_points": p90,
        "p10_net_pnl": _dollars(p10),
        "p50_net_pnl": _dollars(p50),
        "p90_net_pnl": _dollars(p90),
        "pnl_histogram": _pnl_histogram(pnls),
        "risk_objective": objective,
        "hit_invalidation_rate": (
            None
            if invalidation_reason is not None
            else round(counters["invalidation"] / count, 4)
        ),
        "tp_before_stop_rate": round(counters["tp"] / count, 4),
        "premium_stop_rate": round(counters["premium_stop"] / count, 4),
        "hard_close_rate": round(counters["hard_close"] / count, 4),
        "time_stop_rate": round(counters["time_stop"] / count, 4),
        "median_hold_minutes": round(median(simulation["holds"]), 3) if simulation["holds"] else None,
        "n_paths": count,
        "n_sessions": len(sessions),
        "n_same_clock": count,
        "historical_sessions": sessions,
        "clock_mode": mode,
        "horizon_minutes": horizon_minutes,
        "scale": round(scale, 6),
        "remaining_expected_move": remaining_em,
        "compute_ms": round((perf_counter() - started) * 1000.0, 1),
        "reason_codes": reasons,
        "management_policy_version": policy.policy_version,
        "hard_exit_et": policy.hard_exit_et,
        "surface_coordinate": JOINT_SURFACE_COORDINATE,
        "surface_cadence_seconds": SURFACE_CADENCE_MINUTES * 60,
        "surface_degraded_fraction": round(
            sum(path.degraded_points for path in paths) / sum(len(path.prices) for path in paths), 4
        ),
        "sticky_iv_baseline": _baseline(
            sticky,
            entry=entry,
            leg_count=leg_count,
            now=now,
            policy=policy,
            candidate=candidate,
            quote=quote,
            session_count=len(sessions),
            session_date=session_date,
        ),
    }
    return result


def _baseline(
    combo: Mapping[str, np.ndarray],
    *,
    entry: float,
    leg_count: int,
    now: datetime,
    policy: ManagementPolicy,
    candidate: Mapping[str, Any],
    quote: Mapping[str, Any],
    session_count: int,
    session_date: date | None,
) -> dict[str, Any]:
    pnls = _simulate(
        combo,
        paths=tuple(_BaselinePath() for _ in combo["bids"]),
        entry=entry,
        leg_count=leg_count,
        now=now,
        policy=policy,
        invalidation=None,
        session_date=session_date,
    )["pnls"]
    p10, p50, p90 = _percentiles(pnls, (10.0, 50.0, 90.0))
    return {
        "method": "sticky_iv_same_spot_paths.v1",
        "p10_net_pnl": _dollars(p10),
        "p50_net_pnl": _dollars(p50),
        "p90_net_pnl": _dollars(p90),
        "risk_objective": _risk_objective(
            pnls, candidate=candidate, quote=quote, session_count=session_count
        ),
    }


@dataclass(frozen=True)
class _BaselinePath:
    session_date: date = date.min


def _model_mid(
    legs: Sequence[Mapping[str, Any]], *, spot: float, expiry: str, now: datetime
) -> float:
    tau = time_to_expiry_years(expiry, as_of=now)
    return sum(
        float(leg["quantity"])
        * float(_bs_price_np(
            np.asarray([spot]),
            float(leg["strike"]),
            np.asarray([float(leg["implied_vol"])]),
            tau,
            str(leg["right"]),
        )[0])
        for leg in legs
    )


def _bs_price_np(
    spot: np.ndarray,
    strike: float,
    iv: np.ndarray,
    tau: float | np.ndarray,
    right: str,
) -> np.ndarray:
    intrinsic = np.maximum(spot - strike, 0.0) if right == "C" else np.maximum(strike - spot, 0.0)
    safe, safe_iv = np.maximum(spot, 1e-12), np.maximum(iv, 1e-6)
    tau_values = np.asarray(tau, dtype=float)
    root_t = np.sqrt(np.maximum(tau_values, 1e-16))
    d1 = (
        np.log(safe / strike) + 0.5 * safe_iv * safe_iv * tau_values
    ) / (safe_iv * root_t)
    d2 = d1 - safe_iv * root_t
    if right == "C":
        model = safe * ndtr(d1) - strike * ndtr(d2)
    else:
        model = strike * (1.0 - ndtr(d2)) - safe * (1.0 - ndtr(d1))
    return np.where(tau_values <= 0.0, intrinsic, np.maximum(intrinsic, model))


def _surface_scale(
    candidate: Mapping[str, Any],
    facts: Mapping[str, Any],
    *,
    spot: float,
    legs: Sequence[Mapping[str, Any]],
) -> float:
    expected = _number(_map(facts.get("volatility")).get("expected_move_points"))
    width = _number(_map(candidate.get("economics")).get("width_points"))
    farthest = max(abs(float(leg["strike"]) - spot) for leg in legs)
    return max(expected or 0.0, width or 0.0, farthest, 5.0)


def _path_scale(
    paths: Sequence[JointSurfacePath],
    *,
    facts: Mapping[str, Any],
    horizon_minutes: int,
) -> tuple[float, str | None]:
    moves = [path.prices[-1] - path.prices[0] for path in paths]
    rms = (sum(value * value for value in moves) / len(moves)) ** 0.5
    expected = _number(_map(facts.get("volatility")).get("expected_move_points"))
    if expected is None or expected <= 0:
        return 1.0, "remaining_em_unavailable_unscaled"
    if rms <= 1e-6:
        return 1.0, "historical_path_move_degenerate"
    minutes_to_close = _number(facts.get("minutes_to_close"))
    remaining = max(float(minutes_to_close or horizon_minutes), float(horizon_minutes))
    return expected * (horizon_minutes / remaining) ** 0.5 / rms, None


def _invalidation_touch(candidate: Mapping[str, Any], *, credit: bool, spot: float):
    raw = candidate.get("invalidation_spx")
    if isinstance(raw, Sequence) and not isinstance(raw, (str, bytes)):
        low = _number(raw[0] if len(raw) > 0 else None)
        high = _number(raw[1] if len(raw) > 1 else None)
        if low is None or high is None or not low < spot < high:
            return None, "invalidation_not_protective"
        return lambda spots: any(value <= low or value >= high for value in spots), None
    level = _number(raw)
    if level is None:
        return None, "invalidation_unavailable"
    right = str(candidate.get("right") or "").upper()
    if right == "P":
        if level <= spot:
            return None, "invalidation_not_protective"
        return lambda spots: any(value >= level for value in spots), None
    if level >= spot:
        return None, "invalidation_not_protective"
    return lambda spots: any(value <= level for value in spots), None


def _risk_objective(
    pnls: Sequence[float],
    *,
    candidate: Mapping[str, Any],
    quote: Mapping[str, Any],
    session_count: int,
) -> dict[str, Any]:
    max_loss = _number(_map(candidate.get("economics")).get("max_loss_points"))
    bid, ask = _number(quote.get("bid")), _number(quote.get("ask"))
    if max_loss is None or max_loss <= 0 or bid is None or ask is None or ask < bid:
        return {
            "status": "unavailable",
            "authority": "advisory_only",
            "evidence_status": "research_unvalidated",
            "automatic_ordering": False,
            "reason_codes": ["risk_objective_inputs_unavailable"],
        }
    return {
        "status": "available",
        **risk_adjusted_cvar_objective(
            pnls,
            max_loss_points=max_loss,
            quote_width_points=ask - bid,
            session_count=session_count,
        ),
        "reason_codes": [],
    }


def _pnl_histogram(values: Sequence[float]) -> list[dict[str, float | int]]:
    low, high = min(values), max(values)
    if math.isclose(low, high, abs_tol=1e-12):
        half = max(abs(low) * 0.005, 0.005)
        return [{
            "lower_pnl_points": round(low - half, 6),
            "upper_pnl_points": round(high + half, 6),
            "lower_net_pnl": round((low - half) * 100, 2),
            "upper_net_pnl": round((high + half) * 100, 2),
            "probability": 1.0,
            "count": len(values),
        }]
    counts, edges = np.histogram(values, bins=min(12, max(5, math.ceil(math.sqrt(len(values))))))
    return [
        {
            "lower_pnl_points": round(float(edges[index]), 6),
            "upper_pnl_points": round(float(edges[index + 1]), 6),
            "lower_net_pnl": round(float(edges[index]) * 100, 2),
            "upper_net_pnl": round(float(edges[index + 1]) * 100, 2),
            "probability": round(int(count) / len(values), 6),
            "count": int(count),
        }
        for index, count in enumerate(counts)
        if count > 0
    ]


def _percentiles(values: Sequence[float], points: Sequence[float]) -> tuple[float, ...]:
    return tuple(round(float(value), 6) for value in np.percentile(values, points))


def _session_date(value: object) -> date | None:
    try:
        return date.fromisoformat(str(value))
    except ValueError:
        return None


def _dollars(value: float) -> float:
    return round(value * 100.0, 2)


def _map(value: object) -> Mapping[str, Any]:
    return value if isinstance(value, Mapping) else {}


def _number(value: object) -> float | None:
    if isinstance(value, bool) or not isinstance(value, int | float):
        return None
    parsed = float(value)
    return parsed if math.isfinite(parsed) else None


def _utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        raise ValueError("surface path replay time must be timezone-aware")
    return value.astimezone(timezone.utc)


__all__ = [
    "JOINT_SURFACE_CLEARING_METHOD",
    "JOINT_SURFACE_METHOD",
    "estimate_joint_debit_distribution",
    "estimate_joint_iron_condor_distribution",
    "load_joint_surface_paths",
    "path_distribution_desk_text",
]
