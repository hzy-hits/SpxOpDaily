"""Causal, exact-BBO structure-selection audit for strategy policy v45."""

from __future__ import annotations

import argparse
import json
import math
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from pathlib import Path
from statistics import mean
from typing import Any

import duckdb
import numpy as np
from scipy.stats import spearmanr

from spx_spark.analytics.options.strategy_payoff import (
    DEFAULT_MANAGEMENT_POLICY,
    butterfly_economics,
    conservative_butterfly_bbo,
    conservative_iron_condor_bbo,
    conservative_vertical_bbo,
    iron_condor_economics,
    vertical_economics,
)
from spx_spark.analytics.options.surface_attribution import attribute_candidate_surface


POLICY_VERSION = "strategy_policy.bootstrap.v45"
SESSIONS = (
    "2026-08-05",
    "2026-08-06",
    "2026-08-07",
    "2026-08-10",
    "2026-08-11",
    "2026-08-12",
    "2026-08-13",
    "2026-08-14",
    "2026-08-17",
    "2026-08-18",
    "2026-08-19",
    "2026-08-20",
    "2026-08-21",
)
MAX_AGE_SECONDS = 15.0
MAX_SKEW_SECONDS = 2.0
FEE_DOLLARS_PER_LEG_PER_SIDE = DEFAULT_MANAGEMENT_POLICY.fees_per_leg_per_side
VERTICAL_ENTRY_TIMES = ((14, 0), (15, 0), (16, 0), (17, 0), (18, 0))
VERTICAL_HOLD_MINUTES = 30
IRON_CONDOR_HOLD_MINUTES = 60
BUTTERFLY_ENTRY_TIME = (19, 0)
BUTTERFLY_EXIT_TIME = (19, 55)


def _utc(day: str, hour: int, minute: int) -> datetime:
    year, month, day_number = map(int, day.split("-"))
    return datetime(year, month, day_number, hour, minute, tzinfo=timezone.utc)


def _finite(value: object) -> float | None:
    if isinstance(value, bool) or not isinstance(value, int | float):
        return None
    parsed = float(value)
    return parsed if math.isfinite(parsed) else None


def _snapshot(
    connection: duckdb.DuckDBPyConnection,
    data_root: Path,
    *,
    day: str,
    at: datetime,
) -> dict[str, Any]:
    files = []
    for hour in {at.hour, (at - timedelta(minutes=1)).hour}:
        path = (
            data_root
            / "lake"
            / "quotes"
            / "schema=v1"
            / f"date={day}"
            / "provider=schwab"
            / f"hour={hour:02d}"
            / "quotes.parquet"
        )
        if path.exists():
            files.append(str(path))
    if not files:
        return {"status": "unavailable", "reason": "partition_missing"}
    rows = connection.execute(
        """
        SELECT instrument_id, instrument_type, strike, "right", bid, ask,
               last, mid, mark, implied_vol, delta, source_at, received_at
        FROM read_parquet(?)
        WHERE received_at <= ? AND received_at >= ?
          AND source_at IS NOT NULL AND source_at <= ? AND quality = 'live'
        QUALIFY row_number() OVER (
            PARTITION BY instrument_id ORDER BY received_at DESC
        ) = 1
        """,
        [files, at, at - timedelta(seconds=30), at],
    ).fetchall()
    spot = None
    chain: dict[tuple[float, str], dict[str, Any]] = {}
    invalid_bbo_rows = 0
    expiry_token = day.replace("-", "")
    for (
        instrument_id,
        instrument_type,
        strike,
        right,
        bid,
        ask,
        last,
        mid,
        mark,
        implied_vol,
        delta,
        source_at,
        received_at,
    ) in rows:
        source_utc = source_at.astimezone(timezone.utc) if source_at is not None else None
        received_utc = received_at.astimezone(timezone.utc)
        if instrument_id == "index:SPX":
            spot = next(
                (_finite(value) for value in (last, mid, mark) if _finite(value) is not None),
                None,
            )
        if (
            instrument_type != "option"
            or expiry_token not in str(instrument_id)
            or strike is None
            or right not in {"C", "P"}
        ):
            continue
        parsed_bid, parsed_ask = _finite(bid), _finite(ask)
        if (
            parsed_bid is None
            or parsed_ask is None
            or parsed_bid < 0
            or parsed_ask <= 0
            or parsed_ask < parsed_bid
        ):
            invalid_bbo_rows += 1
        chain[(float(strike), str(right))] = {
            "contract_id": str(instrument_id),
            "strike": float(strike),
            "right": str(right),
            "bid": parsed_bid,
            "ask": parsed_ask,
            "implied_vol": _finite(implied_vol),
            "delta": _finite(delta),
            "source_at": source_utc.isoformat() if source_utc is not None else None,
            "received_at": received_utc.isoformat(),
            "provider": "schwab",
        }
    iv_count = sum(leg.get("implied_vol") is not None for leg in chain.values())
    return {
        "status": "ready" if spot is not None and chain else "unavailable",
        "spot": spot,
        "chain": chain,
        "option_count": len(chain),
        "iv_count": iv_count,
        "iv_ratio": iv_count / len(chain) if chain else 0.0,
        "future_source_rows_used": 0,
        "invalid_bbo_rows": invalid_bbo_rows,
    }


def _surface_facts(snapshot: dict[str, Any]) -> dict[str, Any]:
    spot = float(snapshot["spot"])
    chain = snapshot["chain"]
    strikes = sorted({strike for strike, _ in chain})
    atm = min(strikes, key=lambda strike: abs(strike - spot))
    atm_call, atm_put = chain.get((atm, "C"), {}), chain.get((atm, "P"), {})
    call_mid = _mid(atm_call)
    put_mid = _mid(atm_put)
    expected_move = (
        0.85 * (call_mid + put_mid)
        if call_mid is not None and put_mid is not None
        else max(abs(strikes[-1] - strikes[0]) / 8.0, 5.0)
    )
    atm_ivs = [
        value
        for value in (_finite(atm_call.get("implied_vol")), _finite(atm_put.get("implied_vol")))
        if value is not None
    ]
    atm_iv = mean(atm_ivs) if atm_ivs else None
    put_25 = _nearest_delta(chain, right="P", target=0.25, at_or_below=False)
    call_25 = _nearest_delta(chain, right="C", target=0.25, at_or_below=False)
    return {
        "spot": {"spx": spot},
        "volatility": {
            "expected_move_points": expected_move,
            "atm_iv_0dte": atm_iv,
            "put_skew_25d_0dte": _difference(
                _finite((put_25 or {}).get("implied_vol")), atm_iv
            ),
            "call_skew_25d_0dte": _difference(
                _finite((call_25 or {}).get("implied_vol")), atm_iv
            ),
        },
    }


def _difference(left: float | None, right: float | None) -> float | None:
    return left - right if left is not None and right is not None else None


def _mid(leg: dict[str, Any]) -> float | None:
    bid, ask = _finite(leg.get("bid")), _finite(leg.get("ask"))
    return 0.5 * (bid + ask) if bid is not None and ask is not None else None


def _nearest_delta(
    chain: dict[tuple[float, str], dict[str, Any]],
    *,
    right: str,
    target: float,
    at_or_below: bool,
) -> dict[str, Any] | None:
    rows = [
        leg
        for leg in chain.values()
        if leg["right"] == right
        and (delta := _finite(leg.get("delta"))) is not None
        and (not at_or_below or 0.05 <= abs(delta) <= target)
    ]
    return min(rows, key=lambda leg: abs(abs(float(leg["delta"])) - target)) if rows else None


def _candidate(
    *,
    day: str,
    family: str,
    group: str,
    legs: list[dict[str, Any]],
    quote: dict[str, Any],
    economics: dict[str, float],
    facts: dict[str, Any],
    entry_at: datetime,
    base_score: float,
) -> dict[str, Any]:
    strategy_type = {
        "vertical:C": "CALL_DEBIT_VERTICAL",
        "vertical:P": "PUT_DEBIT_VERTICAL",
        "butterfly:C": "CALL_BUTTERFLY",
        "butterfly:P": "PUT_BUTTERFLY",
        "iron_condor:IC": "IRON_CONDOR",
    }[f"{family}:{'IC' if family == 'iron_condor' else legs[0]['right']}"]
    payload = {
        "strategy_type": strategy_type,
        "expiry": day.replace("-", ""),
        "legs": legs,
        "economics": economics,
    }
    attribution = attribute_candidate_surface(
        payload,
        facts,
        now=entry_at,
        bump_vol_points=1.0,
        modifier_cap=0.05,
    )
    modifier = min(float(attribution.get("decision_modifier") or 0.0), 0.0)
    strikes = tuple(float(leg["strike"]) for leg in legs)
    return {
        "day": day,
        "entry_at": entry_at.isoformat(),
        "family": family,
        "group": group,
        "strategy_type": strategy_type,
        "strikes": strikes,
        "legs": legs,
        "entry_quote": quote,
        "economics": economics,
        "surface_attribution": attribution,
        "surface_available": attribution.get("status") == "ready",
        "surface_risk_ratio": (
            float(attribution["surface_risk_points"])
            / float(economics["max_loss_points"])
            if attribution.get("surface_risk_points") is not None
            and economics.get("max_loss_points", 0) > 0
            else None
        ),
        "surface_modifier": modifier,
        "modifier_capped": math.isclose(modifier, -0.05, abs_tol=1e-9),
        "base_score": base_score,
        "adjusted_score": base_score + modifier,
    }


def _vertical_candidates(
    day: str, snapshot: dict[str, Any], entry_at: datetime
) -> list[dict[str, Any]]:
    chain, facts = snapshot["chain"], _surface_facts(snapshot)
    rows = []
    for right in ("C", "P"):
        long_leg = _nearest_delta(chain, right=right, target=0.60, at_or_below=False)
        if long_leg is None:
            continue
        sign = 1.0 if right == "C" else -1.0
        for width in (5.0, 10.0, 15.0, 20.0):
            short_leg = chain.get((float(long_leg["strike"]) + sign * width, right))
            if short_leg is None:
                continue
            quote = conservative_vertical_bbo(
                long_leg,
                short_leg,
                now=entry_at,
                max_quote_age_seconds=MAX_AGE_SECONDS,
                max_source_skew_seconds=MAX_SKEW_SECONDS,
            )
            if quote.get("status") != "ready":
                continue
            try:
                economics = vertical_economics(
                    long_strike=float(long_leg["strike"]),
                    short_strike=float(short_leg["strike"]),
                    net_debit=float(quote["ask"]),
                    right=right,
                )
            except ValueError:
                continue
            if economics["debit_fraction_of_width"] > 0.45:
                continue
            spread = float(quote["ask"]) - float(quote["bid"])
            base = economics["max_gain_points"] / economics["max_loss_points"]
            base -= 0.05 * spread / economics["max_loss_points"]
            rows.append(
                _candidate(
                    day=day,
                    family="vertical",
                    group=f"{day}:{entry_at:%H%M}:vertical:{right}",
                    legs=[long_leg, short_leg],
                    quote=quote,
                    economics=economics,
                    facts=facts,
                    entry_at=entry_at,
                    base_score=base,
                )
            )
    return rows


def _butterfly_candidates(
    day: str, snapshot: dict[str, Any], entry_at: datetime
) -> list[dict[str, Any]]:
    spot, chain, facts = float(snapshot["spot"]), snapshot["chain"], _surface_facts(snapshot)
    strikes = sorted({strike for strike, _ in chain})
    center = min(strikes, key=lambda strike: abs(strike - spot))
    rows = []
    for right in ("C", "P"):
        for width in (10.0, 15.0, 20.0):
            legs = [
                chain.get((center - width, right)),
                chain.get((center, right)),
                chain.get((center + width, right)),
            ]
            if any(leg is None for leg in legs):
                continue
            exact_legs = [dict(leg) for leg in legs if leg is not None]
            quote = conservative_butterfly_bbo(
                *exact_legs,
                now=entry_at,
                max_quote_age_seconds=MAX_AGE_SECONDS,
                max_source_skew_seconds=MAX_SKEW_SECONDS,
            )
            if quote.get("status") != "ready":
                continue
            try:
                economics = butterfly_economics(
                    center=center, width=width, net_debit=float(quote["ask"])
                )
            except ValueError:
                continue
            if economics["debit_fraction_of_width"] > 0.45:
                continue
            spread = float(quote["ask"]) - float(quote["bid"])
            base = min(
                economics["max_gain_points"] / economics["max_loss_points"], 3.0
            ) * 0.05
            base -= 0.01 * width / 5.0
            base -= 0.02 * spread / economics["max_loss_points"]
            rows.append(
                _candidate(
                    day=day,
                    family="butterfly",
                    group=f"{day}:{entry_at:%H%M}:butterfly:{right}",
                    legs=exact_legs,
                    quote=quote,
                    economics=economics,
                    facts=facts,
                    entry_at=entry_at,
                    base_score=base,
                )
            )
    return rows


def _iron_condor_candidates(
    day: str, snapshot: dict[str, Any], entry_at: datetime
) -> list[dict[str, Any]]:
    spot, chain, facts = float(snapshot["spot"]), snapshot["chain"], _surface_facts(snapshot)
    rows = []
    for target in (0.20, 0.15, 0.10, 0.05):
        put_short = _nearest_delta(chain, right="P", target=target, at_or_below=True)
        call_short = _nearest_delta(chain, right="C", target=target, at_or_below=True)
        if put_short is None or call_short is None:
            continue
        put_long = chain.get((float(put_short["strike"]) - 10.0, "P"))
        call_long = chain.get((float(call_short["strike"]) + 10.0, "C"))
        if put_long is None or call_long is None:
            continue
        if not float(put_short["strike"]) < spot < float(call_short["strike"]):
            continue
        legs = [put_long, put_short, call_short, call_long]
        quote = conservative_iron_condor_bbo(
            *legs,
            now=entry_at,
            max_quote_age_seconds=MAX_AGE_SECONDS,
            max_source_skew_seconds=MAX_SKEW_SECONDS,
        )
        if quote.get("status") != "ready":
            continue
        try:
            economics = iron_condor_economics(
                put_long=float(put_long["strike"]),
                put_short=float(put_short["strike"]),
                call_short=float(call_short["strike"]),
                call_long=float(call_long["strike"]),
                net_credit=float(quote["credit"]),
            )
        except ValueError:
            continue
        if not 0.15 <= economics["credit_fraction_of_width"] <= 0.55:
            continue
        spread = float(quote["ask"]) - float(quote["bid"])
        base = economics["max_gain_points"] / economics["max_loss_points"]
        base -= 0.05 * spread / economics["max_loss_points"]
        rows.append(
            _candidate(
                day=day,
                family="iron_condor",
                group=f"{day}:{entry_at:%H%M}:iron_condor",
                legs=legs,
                quote=quote,
                economics=economics,
                facts=facts,
                entry_at=entry_at,
                base_score=base,
            )
        )
    return rows


def _exit_pnl(
    candidate: dict[str, Any], snapshot: dict[str, Any], exit_at: datetime
) -> float | None:
    chain = snapshot.get("chain", {})
    exit_legs = [chain.get((float(leg["strike"]), str(leg["right"]))) for leg in candidate["legs"]]
    if any(leg is None for leg in exit_legs):
        return None
    legs = [dict(leg) for leg in exit_legs if leg is not None]
    family = candidate["family"]
    if family == "vertical":
        quote = conservative_vertical_bbo(
            *legs,
            now=exit_at,
            max_quote_age_seconds=MAX_AGE_SECONDS,
            max_source_skew_seconds=MAX_SKEW_SECONDS,
        )
        pnl = float(quote["bid"]) - float(candidate["entry_quote"]["ask"]) if quote.get("status") == "ready" else None
    elif family == "butterfly":
        quote = conservative_butterfly_bbo(
            *legs,
            now=exit_at,
            max_quote_age_seconds=MAX_AGE_SECONDS,
            max_source_skew_seconds=MAX_SKEW_SECONDS,
        )
        pnl = float(quote["bid"]) - float(candidate["entry_quote"]["ask"]) if quote.get("status") == "ready" else None
    else:
        quote = conservative_iron_condor_bbo(
            *legs,
            now=exit_at,
            max_quote_age_seconds=MAX_AGE_SECONDS,
            max_source_skew_seconds=MAX_SKEW_SECONDS,
        )
        pnl = float(candidate["entry_quote"]["bid"]) - float(quote["ask"]) if quote.get("status") == "ready" else None
    if pnl is None:
        return None
    fees_points = FEE_DOLLARS_PER_LEG_PER_SIDE * len(legs) * 2.0 / 100.0
    return pnl - fees_points


def _cvar10(values: list[float]) -> float | None:
    if not values:
        return None
    ordered = sorted(values)
    return mean(ordered[: max(1, math.ceil(len(ordered) * 0.10))])


def _bootstrap_session_mean(
    session_deltas: dict[str, float], *, seed: int = 45, draws: int = 10_000
) -> list[float] | None:
    if not session_deltas:
        return None
    values = np.asarray(list(session_deltas.values()), dtype=float)
    rng = np.random.default_rng(seed)
    means = rng.choice(values, size=(draws, len(values)), replace=True).mean(axis=1)
    return [float(np.quantile(means, 0.025)), float(np.quantile(means, 0.975))]


def _family_summary(rows: list[dict[str, Any]]) -> dict[str, Any]:
    deltas = [float(row["delta_pnl_points"]) for row in rows]
    session_deltas = {
        day: sum(float(row["delta_pnl_points"]) for row in rows if row["day"] == day)
        for day in sorted({str(row["day"]) for row in rows})
    }
    baseline_pnls = [float(row["baseline_pnl_points"]) for row in rows]
    v45_pnls = [float(row["v45_pnl_points"]) for row in rows]
    return {
        "comparison_groups": len(rows),
        "changed_groups": sum(bool(row["changed"]) for row in rows),
        "baseline_total_pnl_points": sum(baseline_pnls),
        "v45_total_pnl_points": sum(v45_pnls),
        "delta_total_pnl_points": sum(deltas),
        "delta_mean_pnl_points": mean(deltas) if deltas else None,
        "delta_session_bootstrap_95": _bootstrap_session_mean(session_deltas),
        "baseline_cvar10_points": _cvar10(baseline_pnls),
        "v45_cvar10_points": _cvar10(v45_pnls),
    }


def _experimental_selector_summary(
    comparisons: list[dict[str, Any]], *, selector: str
) -> dict[str, Any]:
    pnl_key = f"{selector}_pnl_points"
    strikes_key = f"{selector}_strikes"
    rows = [row for row in comparisons if row.get(pnl_key) is not None]
    deltas = [float(row[pnl_key]) - float(row["baseline_pnl_points"]) for row in rows]
    session_deltas = {
        day: sum(
            float(row[pnl_key]) - float(row["baseline_pnl_points"])
            for row in rows
            if row["day"] == day
        )
        for day in sorted({str(row["day"]) for row in rows})
    }
    return {
        "comparison_groups": len(rows),
        "changed_groups": sum(row[strikes_key] != row["baseline_strikes"] for row in rows),
        "delta_total_pnl_points": sum(deltas),
        "delta_mean_pnl_points": mean(deltas) if deltas else None,
        "delta_session_bootstrap_95": _bootstrap_session_mean(session_deltas),
        "selector_cvar10_points": _cvar10([float(row[pnl_key]) for row in rows]),
        "baseline_cvar10_points": _cvar10(
            [float(row["baseline_pnl_points"]) for row in rows]
        ),
    }


def run(data_root: Path) -> dict[str, Any]:
    connection = duckdb.connect()
    snapshots: dict[tuple[str, datetime], dict[str, Any]] = {}
    candidates: list[dict[str, Any]] = []
    for day in SESSIONS:
        entry_times = [_utc(day, hour, minute) for hour, minute in VERTICAL_ENTRY_TIMES]
        required_times = {
            *entry_times,
            *(at + timedelta(minutes=VERTICAL_HOLD_MINUTES) for at in entry_times),
            *(at + timedelta(minutes=IRON_CONDOR_HOLD_MINUTES) for at in entry_times),
            _utc(day, *BUTTERFLY_ENTRY_TIME),
            _utc(day, *BUTTERFLY_EXIT_TIME),
        }
        for at in sorted(required_times):
            snapshots[(day, at)] = _snapshot(connection, data_root, day=day, at=at)
        for entry_at in entry_times:
            entry = snapshots[(day, entry_at)]
            if entry.get("status") != "ready":
                continue
            for candidate in _vertical_candidates(day, entry, entry_at):
                candidate["exit_at"] = (
                    entry_at + timedelta(minutes=VERTICAL_HOLD_MINUTES)
                ).isoformat()
                candidates.append(candidate)
            for candidate in _iron_condor_candidates(day, entry, entry_at):
                candidate["exit_at"] = (
                    entry_at + timedelta(minutes=IRON_CONDOR_HOLD_MINUTES)
                ).isoformat()
                candidates.append(candidate)
        butterfly_entry = _utc(day, *BUTTERFLY_ENTRY_TIME)
        butterfly = snapshots[(day, butterfly_entry)]
        if butterfly.get("status") == "ready":
            for candidate in _butterfly_candidates(day, butterfly, butterfly_entry):
                candidate["exit_at"] = _utc(day, *BUTTERFLY_EXIT_TIME).isoformat()
                candidates.append(candidate)

    for candidate in candidates:
        exit_at = datetime.fromisoformat(str(candidate["exit_at"]))
        candidate["pnl_points"] = _exit_pnl(
            candidate,
            snapshots[(candidate["day"], exit_at)],
            exit_at,
        )
        candidate["return_on_max_loss"] = (
            candidate["pnl_points"] / candidate["economics"]["max_loss_points"]
            if candidate["pnl_points"] is not None
            else None
        )

    grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for candidate in candidates:
        grouped[candidate["group"]].append(candidate)
    comparisons = []
    for group, variants in sorted(grouped.items()):
        if len(variants) < 2:
            continue
        base_ranked = sorted(
            variants, key=lambda row: (row["base_score"], row["strikes"]), reverse=True
        )
        baseline = max(variants, key=lambda row: (row["base_score"], row["strikes"]))
        v45 = max(variants, key=lambda row: (row["adjusted_score"], row["strikes"]))
        surface_ready_variants = [row for row in variants if row["surface_available"]]
        surface_first = max(
            surface_ready_variants,
            key=lambda row: (row["surface_modifier"], row["base_score"]),
        )
        guard_variants = [
            row
            for row in surface_ready_variants
            if float(row["base_score"]) >= float(baseline["base_score"]) - 0.10
        ]
        guard_010 = max(
            guard_variants,
            key=lambda row: (row["surface_modifier"], row["base_score"]),
        )
        if baseline["pnl_points"] is None or v45["pnl_points"] is None:
            continue
        comparisons.append(
            {
                "day": baseline["day"],
                "family": baseline["family"],
                "group": group,
                "variant_count": len(variants),
                "changed": baseline["strikes"] != v45["strikes"],
                "surface_discriminating": (
                    max(float(row["surface_modifier"]) for row in variants)
                    > min(float(row["surface_modifier"]) for row in variants)
                ),
                "modifier_range": (
                    max(float(row["surface_modifier"]) for row in variants)
                    - min(float(row["surface_modifier"]) for row in variants)
                ),
                "base_gap_to_runner_up": (
                    float(base_ranked[0]["base_score"])
                    - float(base_ranked[1]["base_score"])
                ),
                "baseline_surface_modifier": baseline["surface_modifier"],
                "v45_surface_modifier": v45["surface_modifier"],
                "baseline_strikes": baseline["strikes"],
                "v45_strikes": v45["strikes"],
                "baseline_pnl_points": baseline["pnl_points"],
                "v45_pnl_points": v45["pnl_points"],
                "delta_pnl_points": v45["pnl_points"] - baseline["pnl_points"],
                "baseline_return_on_max_loss": baseline["return_on_max_loss"],
                "v45_return_on_max_loss": v45["return_on_max_loss"],
                "surface_first_strikes": surface_first["strikes"],
                "surface_first_pnl_points": surface_first["pnl_points"],
                "guard_010_strikes": guard_010["strikes"],
                "guard_010_pnl_points": guard_010["pnl_points"],
            }
        )

    families = ("vertical", "butterfly", "iron_condor")
    family_results = {
        family: _family_summary(
            [row for row in comparisons if row["family"] == family]
        )
        for family in families
    }
    validation_days = set(SESSIONS[-5:])
    period_results = {
        "development_2026-08-05_to_14": {
            family: _family_summary(
                [
                    row
                    for row in comparisons
                    if row["family"] == family and row["day"] not in validation_days
                ]
            )
            for family in families
        },
        "validation_2026-08-17_to_21": {
            family: _family_summary(
                [
                    row
                    for row in comparisons
                    if row["family"] == family and row["day"] in validation_days
                ]
            )
            for family in families
        },
    }
    experimental_selectors = {
        selector: _experimental_selector_summary(comparisons, selector=selector)
        for selector in ("surface_first", "guard_010")
    }

    candidate_returns = [
        (float(row["surface_risk_ratio"]), float(row["return_on_max_loss"]))
        for row in candidates
        if row["surface_risk_ratio"] is not None and row["return_on_max_loss"] is not None
    ]
    correlation = spearmanr(*zip(*candidate_returns, strict=True)) if candidate_returns else None
    ready_snapshots = [row for row in snapshots.values() if row.get("status") == "ready"]
    surface_ready = [row for row in candidates if row["surface_available"]]
    comparable_groups = len(comparisons)
    return {
        "schema_version": "surface_risk_v45_backtest.v1",
        "policy_version": POLICY_VERSION,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "question": "Does the v45 negative-only surface modifier improve conditional structure selection?",
        "scope": {
            "sessions": list(SESSIONS),
            "session_count": len(SESSIONS),
            "timezone": "UTC anchors; 11:00/11:30/12:30/15:00/15:55 America/New_York",
            "entry_contract": "latest received_at<=decision_at and source_at<=decision_at; live Schwab exact legs; source age<=15s; leg skew<=2s",
            "vertical": "Hourly 10:00-14:00 ET 60-delta long; 5/10/15/20-wide; exit +30m",
            "butterfly": "15:00 ET spot-nearest center; 10/15/20-wide Call/Put; exit 15:55 ET",
            "iron_condor": "Hourly 10:00-14:00 ET 5/10/15/20-delta shorts; 10-wide wings; exit +60m",
            "fees": "round trip per-leg fees included",
        },
        "data_quality": {
            "requested_snapshots": len(snapshots),
            "ready_snapshots": len(ready_snapshots),
            "mean_option_count": mean(row["option_count"] for row in ready_snapshots)
            if ready_snapshots
            else None,
            "mean_iv_ratio": mean(row["iv_ratio"] for row in ready_snapshots)
            if ready_snapshots
            else None,
            "future_source_rows_used": sum(
                row["future_source_rows_used"] for row in ready_snapshots
            ),
            "invalid_bbo_rows": sum(row["invalid_bbo_rows"] for row in ready_snapshots),
            "candidate_count": len(candidates),
            "candidate_surface_ready": len(surface_ready),
            "candidate_surface_ready_ratio": len(surface_ready) / len(candidates)
            if candidates
            else None,
            "modifier_cap_hit_count": sum(row["modifier_capped"] for row in surface_ready),
            "modifier_cap_hit_ratio": sum(row["modifier_capped"] for row in surface_ready)
            / len(surface_ready)
            if surface_ready
            else None,
            "candidate_exit_ready": sum(row["pnl_points"] is not None for row in candidates),
            "candidate_exit_ready_ratio": sum(row["pnl_points"] is not None for row in candidates)
            / len(candidates)
            if candidates
            else None,
            "comparable_groups": comparable_groups,
            "modifier_discriminating_groups": sum(
                bool(row["surface_discriminating"]) for row in comparisons
            ),
        },
        "family_results": family_results,
        "period_results": period_results,
        "experimental_selectors": experimental_selectors,
        "candidate_surface_risk_vs_return_spearman": {
            "n": len(candidate_returns),
            "rho": float(correlation.statistic) if correlation is not None else None,
            "p_value": float(correlation.pvalue) if correlation is not None else None,
        },
        "comparisons": comparisons,
        "limitations": [
            "Conditional structure-selector audit; it does not test direction or entry timing alpha.",
            "Fixed clocks and finite candidate grids avoid search, but do not reproduce every production setup.",
            "Thirteen sessions are too few to authorize positive surface-premium harvesting or the IC gate.",
            "Current v45 uses a one-vol adverse sensitivity penalty, not a learned Ravagli premium forecast.",
            "The hourly audit increases conditional structure comparisons; it does not imply five independent directional trades per day.",
        ],
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--data-root", type=Path, default=Path("/srv/data/spx-spark/data"))
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    result = run(args.data_root)
    rendered = json.dumps(result, ensure_ascii=False, indent=2, sort_keys=True)
    if args.output is not None:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(rendered + "\n", encoding="utf-8")
    print(rendered)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
