"""Causal multi-session SPXW vertical and butterfly research from close distributions.

The close distribution is the only forecast input. Existing strategy decisions,
walls, GEX, HMM states, and production candidate ranks are intentionally excluded.
Every option structure uses same-day SPXW exact BBO observed by decision time and
is evaluated against the last causal RTH SPX observation as a settlement proxy.
"""

from __future__ import annotations

from collections import defaultdict
from datetime import date, datetime, timedelta, timezone
from html import escape
import json
import math
from pathlib import Path
import runpy
from typing import Any, Mapping, Sequence

import duckdb
import numpy as np

from spx_spark.analytics.options.strategy_payoff import (
    butterfly_payoff,
    conservative_butterfly_bbo,
    conservative_vertical_bbo,
    risk_adjusted_cvar_objective,
    vertical_payoff,
)
from spx_spark.market_calendar import DEFAULT_MARKET_CALENDAR


REPO_ROOT = next(
    path for path in (Path.cwd(), *Path.cwd().parents) if (path / "src/spx_spark").is_dir()
)
DISTRIBUTION_SCRIPT = REPO_ROOT / "docs/notebooks/spx-close-convergence-monte-carlo-2026-08-22.py"
QUOTE_ROOT = Path("/srv/data/spx-spark/data/lake/quotes/schema=v1")
OUTPUT_ROOT = REPO_ROOT / "docs/research"
ARTIFACT_STEM = "spx-close-distribution-option-structures-2026-08-22"
UTC = timezone.utc

VERTICAL_WIDTH = 10.0
BUTTERFLY_WIDTHS = (10.0, 15.0, 20.0)
MAX_DEBIT_FRACTION = 0.45
MAX_QUOTE_AGE_SECONDS = 15.0
MAX_SOURCE_SKEW_SECONDS = 2.0
FEES_PER_CONTRACT_PER_SIDE_USD = 1.32
FIRST_TRADE_HORIZONS = (120, 90, 60, 45, 30, 20, 15)
PREQUENTIAL_META_TRAIN_SESSIONS = 7
BOOTSTRAP_ITERATIONS = 2_000
RNG_SEED = 20260822


def _json_safe(value: object) -> object:
    if isinstance(value, Mapping):
        return {str(key): _json_safe(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_json_safe(item) for item in value]
    if isinstance(value, np.ndarray):
        return [_json_safe(item) for item in value.tolist()]
    if isinstance(value, np.integer):
        return int(value)
    if isinstance(value, np.floating):
        result = float(value)
        return result if math.isfinite(result) else None
    if isinstance(value, date | datetime):
        return value.isoformat()
    if isinstance(value, float) and not math.isfinite(value):
        return None
    return value


def _distribution_predictions() -> tuple[list[dict[str, Any]], int]:
    module = runpy.run_path(str(DISTRIBUTION_SCRIPT))
    sessions = module["load_sessions"]()
    samples = module["build_samples"](sessions)
    predictions = module["expanding_predictions"](
        samples,
        sessions,
        include_settlement_draws=True,
    )
    selected = [row for row in predictions if row["method"] == "online_pool"]
    if not selected or any("settlement_draws" not in row for row in selected):
        raise RuntimeError("online-pool settlement draws unavailable")
    return selected, int(module["MIN_TRAIN_SESSIONS"])


def _quote_files(session_date: date) -> list[str]:
    root = QUOTE_ROOT / f"date={session_date.isoformat()}" / "provider=schwab"
    return [str(path) for path in sorted(root.glob("hour=*/quotes.parquet"))]


def load_option_snapshots(
    predictions: Sequence[Mapping[str, Any]],
) -> tuple[dict[tuple[str, int, str, float], dict[str, Any]], list[dict[str, Any]]]:
    by_day: dict[str, list[Mapping[str, Any]]] = defaultdict(list)
    for row in predictions:
        by_day[str(row["session_date"])].append(row)
    snapshots: dict[tuple[str, int, str, float], dict[str, Any]] = {}
    coverage: list[dict[str, Any]] = []
    connection = duckdb.connect()
    try:
        for session_text, rows in sorted(by_day.items()):
            session_date = date.fromisoformat(session_text)
            files = _quote_files(session_date)
            if not files:
                coverage.extend(
                    {
                        "session_date": session_text,
                        "horizon_minutes": int(row["horizon_minutes"]),
                        "contracts": 0,
                    }
                    for row in rows
                )
                continue
            connection.execute(
                "CREATE OR REPLACE TEMP TABLE decisions(decision_id INTEGER, decision_at TIMESTAMPTZ)"
            )
            ordered = sorted(rows, key=lambda row: int(row["horizon_minutes"]), reverse=True)
            connection.executemany(
                "INSERT INTO decisions VALUES (?, ?)",
                [
                    (index, datetime.fromisoformat(str(row["decision_at"])).astimezone(UTC))
                    for index, row in enumerate(ordered)
                ],
            )
            query = """
            SELECT
              decision_id,
              instrument_id,
              strike,
              "right",
              bid,
              ask,
              source_at,
              received_at
            FROM decisions d
            JOIN read_parquet(?, union_by_name=true) q
              ON q.received_at <= d.decision_at
             AND q.received_at >= d.decision_at - INTERVAL 15 SECOND
            WHERE q.provider = 'schwab'
              AND q.quality = 'live'
              AND lower(coalesce(q.market_data_type, 'live')) IN ('live', '1')
              AND q.instrument_type = 'option'
              AND q.underlier = 'SPX'
              AND q.expiry = ?
              AND q.bid >= 0
              AND q.ask > 0
              AND q.ask >= q.bid
              AND q.source_at IS NOT NULL
              AND q.source_at >= q.received_at - INTERVAL 30 SECOND
              AND q.source_at <= q.received_at + INTERVAL 5 SECOND
            QUALIFY row_number() OVER (
              PARTITION BY decision_id, instrument_id ORDER BY received_at DESC
            ) = 1
            ORDER BY decision_id, strike, "right"
            """
            raw = connection.execute(query, [files, session_date]).fetchall()
            counts: dict[int, int] = defaultdict(int)
            for decision_id, instrument, strike, right, bid, ask, source_at, received_at in raw:
                row = ordered[int(decision_id)]
                horizon = int(row["horizon_minutes"])
                counts[int(decision_id)] += 1
                snapshots[(session_text, horizon, str(right), float(strike))] = {
                    "instrument_id": str(instrument),
                    "strike": float(strike),
                    "right": str(right),
                    "bid": float(bid),
                    "ask": float(ask),
                    "provider": "schwab",
                    "source_at": source_at.astimezone(UTC).isoformat(),
                    "received_at": received_at.astimezone(UTC).isoformat(),
                }
            coverage.extend(
                {
                    "session_date": session_text,
                    "horizon_minutes": int(row["horizon_minutes"]),
                    "contracts": counts[index],
                }
                for index, row in enumerate(ordered)
            )
    finally:
        connection.close()
    return snapshots, coverage


def _quote(
    snapshots: Mapping[tuple[str, int, str, float], Mapping[str, Any]],
    row: Mapping[str, Any],
    right: str,
    strike: float,
) -> Mapping[str, Any] | None:
    return snapshots.get(
        (str(row["session_date"]), int(row["horizon_minutes"]), right, float(strike))
    )


def _fees_points(contract_count: int) -> float:
    return FEES_PER_CONTRACT_PER_SIDE_USD * contract_count * 2.0 / 100.0


def _modal_center(draws: np.ndarray) -> tuple[float, float]:
    q10, median, q90 = (float(value) for value in np.quantile(draws, (0.1, 0.5, 0.9)))
    lower = 5.0 * math.floor((q10 - 2.5) / 5.0)
    upper = 5.0 * math.ceil((q90 + 2.5) / 5.0)
    centers = np.arange(lower, upper + 5.0, 5.0)
    probabilities = np.asarray([np.mean(np.abs(draws - center) <= 2.5) for center in centers])
    order = sorted(
        range(len(centers)),
        key=lambda index: (-float(probabilities[index]), abs(float(centers[index]) - median)),
    )
    selected = order[0]
    return float(centers[selected]), float(probabilities[selected])


def _candidate_economics(
    pnl_points: np.ndarray,
    *,
    debit: float,
    quote_width: float,
    contract_count: int,
    training_sessions: int,
) -> dict[str, Any]:
    fees_points = _fees_points(contract_count)
    net_paths = pnl_points - fees_points
    risk = risk_adjusted_cvar_objective(
        net_paths,
        max_loss_points=debit + fees_points,
        quote_width_points=quote_width,
        session_count=training_sessions,
    )
    return {
        "fees_points": fees_points,
        "predicted_mean_pnl_usd": float(np.mean(net_paths) * 100.0),
        "predicted_profit_probability": float(np.mean(net_paths > 0.0)),
        "predicted_p10_pnl_usd": float(np.quantile(net_paths, 0.10) * 100.0),
        "predicted_p50_pnl_usd": float(np.quantile(net_paths, 0.50) * 100.0),
        "predicted_p90_pnl_usd": float(np.quantile(net_paths, 0.90) * 100.0),
        "risk_objective": risk,
    }


def _vertical_candidate(
    row: Mapping[str, Any],
    snapshots: Mapping[tuple[str, int, str, float], Mapping[str, Any]],
    *,
    right: str,
    training_sessions: int,
) -> tuple[dict[str, Any] | None, str | None]:
    spot = float(row["current_spx"])
    if right == "C":
        long_strike = 5.0 * math.ceil(spot / 5.0)
        short_strike = long_strike + VERTICAL_WIDTH
    else:
        long_strike = 5.0 * math.floor(spot / 5.0)
        short_strike = long_strike - VERTICAL_WIDTH
    long_leg = _quote(snapshots, row, right, long_strike)
    short_leg = _quote(snapshots, row, right, short_strike)
    if long_leg is None or short_leg is None:
        return None, "vertical_leg_missing"
    now = datetime.fromisoformat(str(row["decision_at"])).astimezone(UTC)
    bbo = conservative_vertical_bbo(
        long_leg,
        short_leg,
        now=now,
        max_quote_age_seconds=MAX_QUOTE_AGE_SECONDS,
        max_source_skew_seconds=MAX_SOURCE_SKEW_SECONDS,
    )
    if bbo.get("status") != "ready":
        return None, ";".join(str(reason) for reason in bbo.get("reasons") or ())
    debit = float(bbo["ask"])
    debit_fraction = debit / VERTICAL_WIDTH
    if not 0.0 < debit < VERTICAL_WIDTH:
        return None, "vertical_debit_invalid"
    if debit_fraction > MAX_DEBIT_FRACTION:
        return None, "vertical_debit_fraction_exceeded"
    draws = np.asarray(row["settlement_draws"], dtype=float)
    payoff = np.asarray(
        [
            vertical_payoff(
                settlement,
                long_strike=long_strike,
                short_strike=short_strike,
                net_debit=debit,
                right=right,
            )
            for settlement in draws
        ]
    )
    economics = _candidate_economics(
        payoff,
        debit=debit,
        quote_width=float(bbo["ask"]) - float(bbo["bid"]),
        contract_count=2,
        training_sessions=training_sessions,
    )
    actual_points = (
        vertical_payoff(
            float(row["close_spx"]),
            long_strike=long_strike,
            short_strike=short_strike,
            net_debit=debit,
            right=right,
        )
        - economics["fees_points"]
    )
    return {
        "family": "vertical",
        "right": right,
        "direction": "UP" if right == "C" else "DOWN",
        "strikes": [long_strike, short_strike],
        "width": VERTICAL_WIDTH,
        "net_debit": debit,
        "debit_fraction": debit_fraction,
        "combo_bid": float(bbo["bid"]),
        "combo_ask": float(bbo["ask"]),
        "quote_source_times": list(bbo["source_times"]),
        "source_skew_seconds": float(bbo["source_skew_seconds"]),
        "max_quote_age_seconds": float(bbo["max_quote_age_seconds"]),
        "actual_pnl_usd": actual_points * 100.0,
        **economics,
    }, None


def _butterfly_candidate(
    row: Mapping[str, Any],
    snapshots: Mapping[tuple[str, int, str, float], Mapping[str, Any]],
    *,
    right: str,
    center: float,
    width: float,
    center_probability: float,
    training_sessions: int,
) -> tuple[dict[str, Any] | None, str | None]:
    strikes = (center - width, center, center + width)
    legs = tuple(_quote(snapshots, row, right, strike) for strike in strikes)
    if any(leg is None for leg in legs):
        return None, "butterfly_leg_missing"
    now = datetime.fromisoformat(str(row["decision_at"])).astimezone(UTC)
    bbo = conservative_butterfly_bbo(
        *legs,
        now=now,
        max_quote_age_seconds=MAX_QUOTE_AGE_SECONDS,
        max_source_skew_seconds=MAX_SOURCE_SKEW_SECONDS,
    )
    if bbo.get("status") != "ready":
        return None, ";".join(str(reason) for reason in bbo.get("reasons") or ())
    debit = float(bbo["ask"])
    debit_fraction = debit / width
    if not 0.0 < debit < width:
        return None, "butterfly_debit_invalid"
    if debit_fraction > MAX_DEBIT_FRACTION:
        return None, "butterfly_debit_fraction_exceeded"
    draws = np.asarray(row["settlement_draws"], dtype=float)
    payoff = np.asarray(
        [
            butterfly_payoff(settlement, center=center, width=width, net_debit=debit)
            for settlement in draws
        ]
    )
    economics = _candidate_economics(
        payoff,
        debit=debit,
        quote_width=float(bbo["ask"]) - float(bbo["bid"]),
        contract_count=4,
        training_sessions=training_sessions,
    )
    actual_points = (
        butterfly_payoff(float(row["close_spx"]), center=center, width=width, net_debit=debit)
        - economics["fees_points"]
    )
    return {
        "family": "butterfly",
        "right": right,
        "direction": "TARGET_CONCENTRATED",
        "strikes": list(strikes),
        "center": center,
        "center_probability": center_probability,
        "width": width,
        "net_debit": debit,
        "debit_fraction": debit_fraction,
        "combo_bid": float(bbo["bid"]),
        "combo_ask": float(bbo["ask"]),
        "quote_source_times": list(bbo["source_times"]),
        "source_skew_seconds": float(bbo["source_skew_seconds"]),
        "max_quote_age_seconds": float(bbo["max_quote_age_seconds"]),
        "actual_pnl_usd": actual_points * 100.0,
        **economics,
    }, None


def enumerate_decisions(
    predictions: Sequence[Mapping[str, Any]],
    snapshots: Mapping[tuple[str, int, str, float], Mapping[str, Any]],
    *,
    minimum_training_sessions: int,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    decisions: list[dict[str, Any]] = []
    candidates: list[dict[str, Any]] = []
    ordered_dates = sorted({str(row["session_date"]) for row in predictions})
    date_position = {session: index for index, session in enumerate(ordered_dates)}
    for row in predictions:
        session_text = str(row["session_date"])
        training_sessions = minimum_training_sessions + date_position[session_text]
        center, center_probability = _modal_center(np.asarray(row["settlement_draws"], dtype=float))
        rejection_counts: dict[str, int] = defaultdict(int)
        local_candidates: list[dict[str, Any]] = []
        for right in ("C", "P"):
            candidate, reason = _vertical_candidate(
                row, snapshots, right=right, training_sessions=training_sessions
            )
            if candidate is None:
                rejection_counts[str(reason)] += 1
            else:
                local_candidates.append(candidate)
            for width in BUTTERFLY_WIDTHS:
                candidate, reason = _butterfly_candidate(
                    row,
                    snapshots,
                    right=right,
                    center=center,
                    width=width,
                    center_probability=center_probability,
                    training_sessions=training_sessions,
                )
                if candidate is None:
                    rejection_counts[str(reason)] += 1
                else:
                    local_candidates.append(candidate)
        base = {
            "session_date": session_text,
            "decision_at": str(row["decision_at"]),
            "horizon_minutes": int(row["horizon_minutes"]),
            "decision_spx": float(row["current_spx"]),
            "settlement_proxy_spx": float(row["close_spx"]),
            "training_sessions": training_sessions,
            "predicted_p10_close": float(row["current_spx"]) + float(row["q10_move"]),
            "predicted_p50_close": float(row["current_spx"]) + float(row["q50_move"]),
            "predicted_p90_close": float(row["current_spx"]) + float(row["q90_move"]),
            "modal_center": center,
            "modal_center_probability": center_probability,
        }
        enriched: list[dict[str, Any]] = []
        for candidate in local_candidates:
            item = base | candidate
            item["candidate_id"] = (
                f"{session_text}:{int(row['horizon_minutes'])}:{candidate['family']}:"
                f"{candidate['right']}:{'-'.join(f'{strike:.0f}' for strike in candidate['strikes'])}"
            )
            enriched.append(item)
            candidates.append(item)
        selections: dict[str, dict[str, Any] | None] = {}
        best_priced: dict[str, dict[str, Any] | None] = {}
        for family in ("vertical", "butterfly"):
            family_rows = [item for item in enriched if item["family"] == family]
            best = max(
                family_rows,
                key=lambda item: float(item["risk_objective"]["objective_points"]),
                default=None,
            )
            best_priced[family] = best
            selections[family] = (
                best
                if best is not None and best["risk_objective"]["shadow_choice"] == "STRUCTURE"
                else None
            )
        eligible = [item for item in selections.values() if item is not None]
        combined = max(
            eligible,
            key=lambda item: float(item["risk_objective"]["objective_points"]),
            default=None,
        )
        priced = [item for item in best_priced.values() if item is not None]
        best_priced_combined = max(
            priced,
            key=lambda item: float(item["risk_objective"]["objective_points"]),
            default=None,
        )
        decisions.append(
            base
            | {
                "priced_candidates": len(enriched),
                "rejections": dict(sorted(rejection_counts.items())),
                "selected_vertical": selections["vertical"],
                "selected_butterfly": selections["butterfly"],
                "selected_combined": combined,
                "best_priced_vertical": best_priced["vertical"],
                "best_priced_butterfly": best_priced["butterfly"],
                "best_priced_combined": best_priced_combined,
            }
        )
    return decisions, candidates


def _session_bootstrap_mean(
    rows: Sequence[Mapping[str, Any]],
    *,
    seed: int,
) -> tuple[float | None, float | None]:
    by_session: dict[str, list[float]] = defaultdict(list)
    for row in rows:
        by_session[str(row["session_date"])].append(float(row["decision_pnl_usd"]))
    sessions = sorted(by_session)
    if len(sessions) < 2:
        return None, None
    rng = np.random.default_rng(seed)
    draws = np.empty(BOOTSTRAP_ITERATIONS)
    for index in range(BOOTSTRAP_ITERATIONS):
        sampled = rng.choice(sessions, size=len(sessions), replace=True)
        values = [value for session in sampled for value in by_session[str(session)]]
        draws[index] = float(np.mean(values))
    return float(np.quantile(draws, 0.025)), float(np.quantile(draws, 0.975))


def selection_metrics(
    decisions: Sequence[Mapping[str, Any]],
    *,
    selection_key: str,
    label: str,
    horizon: int | None = None,
) -> dict[str, Any]:
    requested = [
        row for row in decisions if horizon is None or int(row["horizon_minutes"]) == horizon
    ]
    evaluation: list[dict[str, Any]] = []
    trades: list[Mapping[str, Any]] = []
    for row in requested:
        selected = row.get(selection_key)
        pnl = float(selected["actual_pnl_usd"]) if isinstance(selected, Mapping) else 0.0
        evaluation.append({"session_date": row["session_date"], "decision_pnl_usd": pnl})
        if isinstance(selected, Mapping):
            trades.append(selected)
    pnl_values = np.asarray([float(row["actual_pnl_usd"]) for row in trades], dtype=float)
    predicted = np.asarray([float(row["predicted_mean_pnl_usd"]) for row in trades], dtype=float)
    ci = _session_bootstrap_mean(
        evaluation,
        seed=RNG_SEED + (horizon or 0) + sum(ord(char) for char in selection_key),
    )
    return {
        "label": label,
        "horizon_minutes": horizon,
        "decisions": len(requested),
        "sessions": len({row["session_date"] for row in requested}),
        "trades": len(trades),
        "trade_rate": len(trades) / len(requested) if requested else None,
        "mean_pnl_per_decision_usd": (
            float(sum(row["decision_pnl_usd"] for row in evaluation) / len(evaluation))
            if evaluation
            else None
        ),
        "session_bootstrap_95_mean_pnl_per_decision_usd": list(ci),
        "mean_pnl_per_trade_usd": float(np.mean(pnl_values)) if len(pnl_values) else None,
        "median_pnl_per_trade_usd": float(np.median(pnl_values)) if len(pnl_values) else None,
        "win_rate": float(np.mean(pnl_values > 0.0)) if len(pnl_values) else None,
        "total_pnl_usd": float(np.sum(pnl_values)) if len(pnl_values) else 0.0,
        "mean_predicted_minus_actual_usd": (
            float(np.mean(predicted - pnl_values)) if len(pnl_values) else None
        ),
    }


def _first_trade_policy(decisions: Sequence[Mapping[str, Any]]) -> list[dict[str, Any]]:
    by_day_horizon = {
        (str(row["session_date"]), int(row["horizon_minutes"])): row for row in decisions
    }
    output: list[dict[str, Any]] = []
    for session_text in sorted({str(row["session_date"]) for row in decisions}):
        selected_row: Mapping[str, Any] | None = None
        for horizon in FIRST_TRADE_HORIZONS:
            row = by_day_horizon.get((session_text, horizon))
            if row is not None and isinstance(row.get("selected_combined"), Mapping):
                selected_row = row
                break
        if selected_row is None:
            fallback = next(row for row in decisions if str(row["session_date"]) == session_text)
            output.append(
                {
                    **fallback,
                    "horizon_minutes": None,
                    "selected_first_trade": None,
                }
            )
        else:
            output.append(
                {
                    **selected_row,
                    "selected_first_trade": selected_row["selected_combined"],
                }
            )
    return output


def _prequential_structure_policy(
    decisions: Sequence[Mapping[str, Any]],
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    """Choose family/horizon using prior realized sessions, then evaluate the next day.

    This deliberately ignores the positive risk-objective gate so the research can
    distinguish a weak structure from an overly conservative gate. It remains a
    diagnostic policy and never authorizes a production candidate.
    """

    dates = sorted({str(row["session_date"]) for row in decisions})
    by_key = {(str(row["session_date"]), int(row["horizon_minutes"])): row for row in decisions}
    horizons = sorted({int(row["horizon_minutes"]) for row in decisions}, reverse=True)
    choices = [(family, horizon) for family in ("vertical", "butterfly") for horizon in horizons]
    evaluation: list[dict[str, Any]] = []
    audit: list[dict[str, Any]] = []
    for date_index in range(PREQUENTIAL_META_TRAIN_SESSIONS, len(dates)):
        session_text = dates[date_index]
        training_dates = dates[:date_index]
        ranked: list[tuple[float, str, int]] = []
        for family, horizon in choices:
            pnl = []
            for training_date in training_dates:
                row = by_key[(training_date, horizon)]
                candidate = row.get(f"best_priced_{family}")
                pnl.append(
                    float(candidate["actual_pnl_usd"]) if isinstance(candidate, Mapping) else 0.0
                )
            ranked.append((float(np.mean(pnl)), family, horizon))
        training_mean, family, horizon = max(ranked, key=lambda item: item[0])
        current = by_key[(session_text, horizon)]
        candidate = current.get(f"best_priced_{family}")
        evaluation.append(
            {
                **current,
                "selected_prequential": candidate,
                "chosen_family": family,
                "chosen_horizon_minutes": horizon,
                "prior_mean_pnl_per_decision_usd": training_mean,
            }
        )
        audit.append(
            {
                "session_date": session_text,
                "chosen_family": family,
                "chosen_horizon_minutes": horizon,
                "prior_sessions": len(training_dates),
                "prior_mean_pnl_per_decision_usd": training_mean,
                "candidate_id": (
                    str(candidate["candidate_id"]) if isinstance(candidate, Mapping) else None
                ),
                "actual_pnl_usd": (
                    float(candidate["actual_pnl_usd"]) if isinstance(candidate, Mapping) else 0.0
                ),
            }
        )
    return evaluation, audit


def _candidate_payoff_usd(
    candidate: Mapping[str, Any],
    *,
    settlement: float,
    extra_entry_slippage_points: float,
) -> float:
    strikes = [float(value) for value in candidate["strikes"]]
    if candidate["family"] == "vertical":
        long_strike, short_strike = strikes
        if candidate["right"] == "C":
            intrinsic = max(settlement - long_strike, 0.0) - max(settlement - short_strike, 0.0)
        else:
            intrinsic = max(long_strike - settlement, 0.0) - max(short_strike - settlement, 0.0)
    else:
        lower, center, upper = strikes
        intrinsic = (
            max(settlement - lower, 0.0)
            - 2.0 * max(settlement - center, 0.0)
            + max(settlement - upper, 0.0)
        )
    return (
        intrinsic
        - float(candidate["net_debit"])
        - extra_entry_slippage_points
        - float(candidate["fees_points"])
    ) * 100.0


def _butterfly_1555_exit_policy(
    decisions: Sequence[Mapping[str, Any]],
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    """Value the frozen 60-minute Butterfly at exact 15:55 ET combo bid."""

    evaluation: list[dict[str, Any]] = []
    audit: list[dict[str, Any]] = []
    connection = duckdb.connect()
    try:
        for row in decisions:
            if int(row["horizon_minutes"]) != 60:
                continue
            candidate = row.get("best_priced_butterfly")
            if not isinstance(candidate, Mapping):
                evaluation.append({**row, "selected_1555_exit": None})
                audit.append(
                    {
                        "session_date": str(row["session_date"]),
                        "candidate_id": None,
                        "status": "entry_unavailable",
                        "actual_pnl_usd": 0.0,
                    }
                )
                continue
            session_date = date.fromisoformat(str(row["session_date"]))
            session = DEFAULT_MARKET_CALENDAR.session(session_date)
            files = _quote_files(session_date)
            if session is None or not files:
                evaluation.append({**row, "selected_1555_exit": None})
                audit.append(
                    {
                        "session_date": session_date.isoformat(),
                        "candidate_id": candidate["candidate_id"],
                        "status": "exit_quote_unavailable",
                        "actual_pnl_usd": 0.0,
                    }
                )
                continue
            exit_at = session.close_at.astimezone(UTC) - timedelta(minutes=5)
            strikes = [float(value) for value in candidate["strikes"]]
            right = str(candidate["right"])
            rows = connection.execute(
                """
                SELECT strike, "right", bid, ask, source_at, received_at
                FROM read_parquet(?, union_by_name=true)
                WHERE provider = 'schwab'
                  AND quality = 'live'
                  AND lower(coalesce(market_data_type, 'live')) IN ('live', '1')
                  AND instrument_type = 'option'
                  AND underlier = 'SPX'
                  AND expiry = ?
                  AND strike IN (?, ?, ?)
                  AND "right" = ?
                  AND bid >= 0
                  AND ask > 0
                  AND ask >= bid
                  AND source_at IS NOT NULL
                  AND source_at >= received_at - INTERVAL 30 SECOND
                  AND source_at <= received_at + INTERVAL 5 SECOND
                  AND received_at <= ?
                  AND received_at >= ? - INTERVAL 15 SECOND
                QUALIFY row_number() OVER (
                  PARTITION BY strike, "right" ORDER BY received_at DESC
                ) = 1
                ORDER BY strike
                """,
                [
                    files,
                    session_date,
                    *strikes,
                    right,
                    exit_at,
                    exit_at,
                ],
            ).fetchall()
            quotes = {
                float(strike): {
                    "bid": float(bid),
                    "ask": float(ask),
                    "source_at": source_at.astimezone(UTC),
                    "received_at": received_at.astimezone(UTC),
                }
                for strike, _right, bid, ask, source_at, received_at in rows
            }
            if any(strike not in quotes for strike in strikes):
                evaluation.append({**row, "selected_1555_exit": None})
                audit.append(
                    {
                        "session_date": session_date.isoformat(),
                        "candidate_id": candidate["candidate_id"],
                        "status": "exit_leg_missing",
                        "actual_pnl_usd": 0.0,
                    }
                )
                continue
            source_times = [quotes[strike]["source_at"] for strike in strikes]
            max_age = max((exit_at - value).total_seconds() for value in source_times)
            source_skew = (max(source_times) - min(source_times)).total_seconds()
            if max_age > MAX_QUOTE_AGE_SECONDS or source_skew > MAX_SOURCE_SKEW_SECONDS:
                evaluation.append({**row, "selected_1555_exit": None})
                audit.append(
                    {
                        "session_date": session_date.isoformat(),
                        "candidate_id": candidate["candidate_id"],
                        "status": "exit_quote_integrity_failed",
                        "max_quote_age_seconds": max_age,
                        "source_skew_seconds": source_skew,
                        "actual_pnl_usd": 0.0,
                    }
                )
                continue
            lower, body, upper = (quotes[strike] for strike in strikes)
            combo_bid = max(
                float(lower["bid"]) - 2.0 * float(body["ask"]) + float(upper["bid"]),
                0.0,
            )
            pnl_usd = (
                combo_bid - float(candidate["net_debit"]) - float(candidate["fees_points"])
            ) * 100.0
            selected = {**candidate, "actual_pnl_usd": pnl_usd}
            evaluation.append({**row, "selected_1555_exit": selected})
            audit.append(
                {
                    "session_date": session_date.isoformat(),
                    "candidate_id": candidate["candidate_id"],
                    "status": "priced",
                    "exit_at": exit_at.isoformat(),
                    "entry_combo_ask": candidate["net_debit"],
                    "exit_combo_bid": combo_bid,
                    "max_quote_age_seconds": max_age,
                    "source_skew_seconds": source_skew,
                    "actual_pnl_usd": pnl_usd,
                }
            )
    finally:
        connection.close()
    return evaluation, audit


def _prequential_stress(rows: Sequence[Mapping[str, Any]]) -> list[dict[str, Any]]:
    scenarios = ((0.0, 0.0), (0.5, 0.10), (1.0, 0.25), (2.0, 0.50))
    output: list[dict[str, Any]] = []
    for settlement_error, extra_slippage in scenarios:
        evaluation: list[dict[str, Any]] = []
        trades = 0
        for row in rows:
            candidate = row.get("selected_prequential")
            if not isinstance(candidate, Mapping):
                pnl = 0.0
            else:
                trades += 1
                settlement = float(candidate["settlement_proxy_spx"])
                pnl = min(
                    _candidate_payoff_usd(
                        candidate,
                        settlement=settlement + offset,
                        extra_entry_slippage_points=extra_slippage,
                    )
                    for offset in (-settlement_error, settlement_error)
                )
            evaluation.append({"session_date": str(row["session_date"]), "decision_pnl_usd": pnl})
        ci = _session_bootstrap_mean(
            evaluation,
            seed=RNG_SEED + round(settlement_error * 100) + round(extra_slippage * 1_000),
        )
        output.append(
            {
                "settlement_error_points": settlement_error,
                "extra_entry_slippage_points": extra_slippage,
                "decisions": len(evaluation),
                "trades": trades,
                "mean_pnl_per_decision_usd": float(
                    np.mean([row["decision_pnl_usd"] for row in evaluation])
                ),
                "total_pnl_usd": float(sum(row["decision_pnl_usd"] for row in evaluation)),
                "session_bootstrap_95_mean_pnl_per_decision_usd": list(ci),
            }
        )
    return output


def _candidate_type_metrics(candidates: Sequence[Mapping[str, Any]]) -> list[dict[str, Any]]:
    groups: dict[tuple[str, str, float], list[Mapping[str, Any]]] = defaultdict(list)
    for row in candidates:
        groups[(str(row["family"]), str(row["right"]), float(row["width"]))].append(row)
    output: list[dict[str, Any]] = []
    for (family, right, width), rows in sorted(groups.items()):
        pnl = np.asarray([float(row["actual_pnl_usd"]) for row in rows])
        output.append(
            {
                "family": family,
                "right": right,
                "width": width,
                "priced_candidates": len(rows),
                "sessions": len({row["session_date"] for row in rows}),
                "mean_actual_pnl_usd": float(np.mean(pnl)),
                "win_rate": float(np.mean(pnl > 0.0)),
                "mean_predicted_pnl_usd": float(
                    np.mean([float(row["predicted_mean_pnl_usd"]) for row in rows])
                ),
            }
        )
    return output


def _table_html(rows: Sequence[Mapping[str, Any]], columns: Sequence[str]) -> str:
    head = "".join(f"<th>{escape(column)}</th>" for column in columns)
    body: list[str] = []
    for row in rows:
        cells: list[str] = []
        for column in columns:
            value = row.get(column)
            if value is None:
                text = "—"
            elif isinstance(value, float):
                text = f"{value:.3f}"
            else:
                text = str(value)
            cells.append(f"<td>{escape(text)}</td>")
        body.append(f"<tr>{''.join(cells)}</tr>")
    return f"<table><thead><tr>{head}</tr></thead><tbody>{''.join(body)}</tbody></table>"


def _write_outputs(artifact: Mapping[str, Any]) -> tuple[Path, Path]:
    OUTPUT_ROOT.mkdir(parents=True, exist_ok=True)
    json_path = OUTPUT_ROOT / f"{ARTIFACT_STEM}.json"
    html_path = OUTPUT_ROOT / f"{ARTIFACT_STEM}.html"
    json_path.write_text(
        json.dumps(_json_safe(artifact), ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    html = f"""<!doctype html><html lang="zh-CN"><head><meta charset="utf-8">
<title>SPX close distribution option structures</title>
<style>body{{font-family:system-ui,sans-serif;max-width:1180px;margin:32px auto;color:#172033}}table{{border-collapse:collapse;width:100%;margin:12px 0 28px}}th,td{{border:1px solid #dbe2ea;padding:7px;text-align:right}}th:first-child,td:first-child{{text-align:left}}</style></head><body>
<h1>SPX 收盘分布 → SPXW 结构多日回测</h1>
<p><strong>结论：</strong>{escape(str(artifact["conclusion"]["status"]))} — {escape(str(artifact["conclusion"]["summary"]))}</p>
<p>严格前向 online-pool 收盘分布；同日 SPXW exact BBO；买腿 ask、卖腿 bid；到期 payoff 使用最后可观察 RTH SPX 作为 settlement proxy。</p>
<h2>固定时点：Combined</h2>{_table_html(artifact["fixed_horizon_combined"], ("horizon_minutes", "decisions", "trades", "trade_rate", "mean_pnl_per_decision_usd", "mean_pnl_per_trade_usd", "win_rate", "total_pnl_usd", "session_bootstrap_95_mean_pnl_per_decision_usd"))}
<h2>固定时点：Vertical</h2>{_table_html(artifact["fixed_horizon_vertical"], ("horizon_minutes", "decisions", "trades", "mean_pnl_per_decision_usd", "mean_pnl_per_trade_usd", "win_rate", "total_pnl_usd"))}
<h2>固定时点：Butterfly</h2>{_table_html(artifact["fixed_horizon_butterfly"], ("horizon_minutes", "decisions", "trades", "mean_pnl_per_decision_usd", "mean_pnl_per_trade_usd", "win_rate", "total_pnl_usd"))}
<h2>每日首次正目标候选</h2>{_table_html([artifact["first_trade_policy"]], ("decisions", "trades", "trade_rate", "mean_pnl_per_decision_usd", "mean_pnl_per_trade_usd", "win_rate", "total_pnl_usd", "session_bootstrap_95_mean_pnl_per_decision_usd"))}
<h2>前向元策略（研究对照，忽略正目标门）</h2>{_table_html([artifact["prequential_structure_policy"]], ("decisions", "trades", "trade_rate", "mean_pnl_per_decision_usd", "mean_pnl_per_trade_usd", "win_rate", "total_pnl_usd", "session_bootstrap_95_mean_pnl_per_decision_usd"))}
<h2>前向元策略压力测试</h2>{_table_html(artifact["prequential_stress"], ("settlement_error_points", "extra_entry_slippage_points", "decisions", "trades", "mean_pnl_per_decision_usd", "total_pnl_usd", "session_bootstrap_95_mean_pnl_per_decision_usd"))}
<h2>固定时点最佳可定价 Butterfly（研究对照）</h2>{_table_html(artifact["best_priced_fixed_horizon_butterfly"], ("horizon_minutes", "decisions", "trades", "mean_pnl_per_decision_usd", "mean_pnl_per_trade_usd", "win_rate", "total_pnl_usd", "session_bootstrap_95_mean_pnl_per_decision_usd"))}
<h2>生产退出：15:55 ET exact combo bid</h2>{_table_html([artifact["butterfly_1555_exit_policy"]], ("decisions", "trades", "trade_rate", "mean_pnl_per_decision_usd", "mean_pnl_per_trade_usd", "win_rate", "total_pnl_usd", "session_bootstrap_95_mean_pnl_per_decision_usd"))}
<h2>所有可定价结构（不按模型门控）</h2>{_table_html(artifact["candidate_type_metrics"], ("family", "right", "width", "priced_candidates", "sessions", "mean_actual_pnl_usd", "win_rate", "mean_predicted_pnl_usd"))}
<h2>限制</h2><ul>{"".join(f"<li>{escape(item)}</li>" for item in artifact["limitations"])}</ul>
</body></html>"""
    html_path.write_text(html, encoding="utf-8")
    return json_path, html_path


def run_analysis(*, write_outputs: bool = True) -> dict[str, Any]:
    predictions, minimum_training_sessions = _distribution_predictions()
    snapshots, quote_coverage = load_option_snapshots(predictions)
    decisions, candidates = enumerate_decisions(
        predictions,
        snapshots,
        minimum_training_sessions=minimum_training_sessions,
    )
    horizons = sorted({int(row["horizon_minutes"]) for row in decisions}, reverse=True)
    combined = [
        selection_metrics(
            decisions,
            selection_key="selected_combined",
            label="combined",
            horizon=horizon,
        )
        for horizon in horizons
    ]
    vertical = [
        selection_metrics(
            decisions,
            selection_key="selected_vertical",
            label="vertical",
            horizon=horizon,
        )
        for horizon in horizons
    ]
    butterfly = [
        selection_metrics(
            decisions,
            selection_key="selected_butterfly",
            label="butterfly",
            horizon=horizon,
        )
        for horizon in horizons
    ]
    first_trade_rows = _first_trade_policy(decisions)
    first_trade = selection_metrics(
        first_trade_rows,
        selection_key="selected_first_trade",
        label="first_positive_objective_120_to_15",
    )
    best_priced_vertical = [
        selection_metrics(
            decisions,
            selection_key="best_priced_vertical",
            label="best_priced_vertical_diagnostic",
            horizon=horizon,
        )
        for horizon in horizons
    ]
    best_priced_butterfly = [
        selection_metrics(
            decisions,
            selection_key="best_priced_butterfly",
            label="best_priced_butterfly_diagnostic",
            horizon=horizon,
        )
        for horizon in horizons
    ]
    prequential_rows, prequential_audit = _prequential_structure_policy(decisions)
    prequential = selection_metrics(
        prequential_rows,
        selection_key="selected_prequential",
        label="prior_session_selected_family_horizon_diagnostic",
    )
    prequential_stress = _prequential_stress(prequential_rows)
    exit_rows, exit_audit = _butterfly_1555_exit_policy(decisions)
    exit_policy = selection_metrics(
        exit_rows,
        selection_key="selected_1555_exit",
        label="best_priced_butterfly_60m_exit_1555_exact_bid",
    )
    best_fixed = max(
        combined,
        key=lambda row: float(row["mean_pnl_per_decision_usd"] or -math.inf),
    )
    ci = best_fixed["session_bootstrap_95_mean_pnl_per_decision_usd"]
    validated = (
        best_fixed["trades"] >= 8
        and ci[0] is not None
        and float(ci[0]) > 0.0
        and first_trade["trades"] >= 8
        and first_trade["session_bootstrap_95_mean_pnl_per_decision_usd"][0] is not None
        and float(first_trade["session_bootstrap_95_mean_pnl_per_decision_usd"][0]) > 0.0
    )
    prequential_ci = prequential["session_bootstrap_95_mean_pnl_per_decision_usd"]
    promising = (
        prequential["trades"] >= 6
        and prequential_ci[0] is not None
        and float(prequential_ci[0]) > 0.0
    )
    exit_ci = exit_policy["session_bootstrap_95_mean_pnl_per_decision_usd"]
    exit_supported = (
        exit_policy["trades"] >= 8 and exit_ci[0] is not None and float(exit_ci[0]) > 0.0
    )
    conclusion = {
        "status": "manual_production_integration_authorized_forward_unvalidated",
        "summary": (
            "The user authorized a manual-only production lane after the frozen 60-minute Butterfly also retained a positive session-bootstrap lower bound at the exact 15:55 ET combo bid. It remains forward-unvalidated and cannot place orders."
        ),
        "positive_objective_gate_validated": validated,
        "prequential_diagnostic_promising": promising,
        "exact_1555_exit_supported": exit_supported,
        "production_eligible": exit_supported,
        "evidence_status": "forward_unvalidated_user_override",
        "action_authority": "MANUAL_ONLY",
        "automatic_ordering": False,
    }
    rejection_totals: dict[str, int] = defaultdict(int)
    for row in decisions:
        for reason, count in row["rejections"].items():
            rejection_totals[str(reason)] += int(count)
    artifact: dict[str, Any] = {
        "artifact_version": "spx_close_distribution_option_structures.v2",
        "created_at": datetime.now(UTC).isoformat(),
        "contract": {
            "distribution_model": "spx_close_convergence_mc.v2 online_pool",
            "session_count": len({row["session_date"] for row in decisions}),
            "decision_horizons_minutes": horizons,
            "vertical_width": VERTICAL_WIDTH,
            "butterfly_widths": list(BUTTERFLY_WIDTHS),
            "max_debit_fraction": MAX_DEBIT_FRACTION,
            "max_quote_age_seconds": MAX_QUOTE_AGE_SECONDS,
            "max_source_skew_seconds": MAX_SOURCE_SKEW_SECONDS,
            "entry_execution": "long ask minus short bid; no mid",
            "production_horizon_minutes": 60,
            "production_exit": "15:55 ET exact conservative combo bid",
            "production_setup": "CLOSE_CONVERGENCE_60M Butterfly manual-only",
            "settlement": "last causal RTH SPX observation proxy",
            "fees_per_contract_per_side_usd": FEES_PER_CONTRACT_PER_SIDE_USD,
            "first_trade_horizons": list(FIRST_TRADE_HORIZONS),
            "prequential_meta_train_sessions": PREQUENTIAL_META_TRAIN_SESSIONS,
        },
        "data_profile": {
            "distribution_decisions": len(predictions),
            "sessions": len({row["session_date"] for row in predictions}),
            "option_snapshot_contracts": len(snapshots),
            "decision_snapshots_with_contracts": sum(
                int(row["contracts"] > 0) for row in quote_coverage
            ),
            "decision_snapshots": len(quote_coverage),
            "priced_candidates": len(candidates),
            "rejections": dict(sorted(rejection_totals.items())),
        },
        "fixed_horizon_combined": combined,
        "fixed_horizon_vertical": vertical,
        "fixed_horizon_butterfly": butterfly,
        "first_trade_policy": first_trade,
        "best_priced_fixed_horizon_vertical": best_priced_vertical,
        "best_priced_fixed_horizon_butterfly": best_priced_butterfly,
        "butterfly_1555_exit_policy": exit_policy,
        "butterfly_1555_exit_audit": exit_audit,
        "prequential_structure_policy": prequential,
        "prequential_structure_audit": prequential_audit,
        "prequential_stress": prequential_stress,
        "candidate_type_metrics": _candidate_type_metrics(candidates),
        "quote_coverage": quote_coverage,
        "decisions": decisions,
        "candidates": candidates,
        "conclusion": conclusion,
        "limitations": [
            "Only 14 independent OOS sessions are available; the ten horizons within one day are correlated and are never counted as independent sessions.",
            "The last causal RTH SPX quote is a settlement proxy; a small official settlement difference can materially change payoff near a strike.",
            "The production exit is valued at one exact 15:55 ET snapshot; intrahour mark-to-market drawdown and fill probability remain unmeasured.",
            "The best fixed horizon is reported descriptively after comparing ten horizons; it is not an untouched validation choice.",
            "The prequential meta-policy uses only prior realized sessions, but it bypasses the positive risk-objective gate and has only seven evaluation sessions.",
            "Butterfly center is the online-pool modal 5-point bucket, not dealer inventory, GEX, wall, or OI evidence.",
            "The user-authorized lane is manual-only and forward-unvalidated; automatic ordering remains disabled.",
        ],
    }
    if write_outputs:
        json_path, html_path = _write_outputs(artifact)
        artifact["output_paths"] = {"json": str(json_path), "html": str(html_path)}
    return artifact


if __name__ == "__main__":
    result = run_analysis(write_outputs=True)
    print(
        json.dumps(
            _json_safe(
                {
                    "data_profile": result["data_profile"],
                    "fixed_horizon_combined": result["fixed_horizon_combined"],
                    "first_trade_policy": result["first_trade_policy"],
                    "conclusion": result["conclusion"],
                    "output_paths": result["output_paths"],
                }
            ),
            ensure_ascii=False,
            indent=2,
        )
    )
