"""Causal RTH SPX close-distribution exploration from raw quote updates.

This research artifact is intentionally independent of production strategy
decisions, candidates, policy versions, GEX direction labels, and execution
rules.  It predicts the last observable RTH SPX value from raw SPX/ES paths.
"""

from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass
from datetime import date, datetime, timezone
from html import escape
import json
import math
import os
from pathlib import Path
from typing import Any, Mapping, Sequence

import duckdb
import numpy as np
from scipy.stats import spearmanr
from sklearn.linear_model import Ridge

from spx_spark.market_calendar import DEFAULT_MARKET_CALENDAR


REPO_ROOT = next(
    (path for path in (Path.cwd(), *Path.cwd().parents) if (path / "src/spx_spark").is_dir()),
    None,
)
if REPO_ROOT is None:
    raise RuntimeError("Run from the spx-spark repository")

QUOTE_ROOT = Path(
    os.environ.get("SPX_SPARK_QUOTE_ROOT", "/srv/data/spx-spark/data/lake/quotes/schema=v1")
)
OUTPUT_ROOT = Path(
    os.environ.get("SPX_SPARK_RESEARCH_OUTPUT_ROOT", str(REPO_ROOT / "docs/research"))
)
START_DATE = date(2026, 7, 13)
END_DATE = date(2026, 8, 21)
HORIZONS_MINUTES = (180, 120, 90, 60, 45, 30, 20, 15, 10, 5)
MIN_TRAIN_SESSIONS = 15
MIN_SESSION_COVERAGE = 0.95
MONTE_CARLO_DRAWS = 2_000
PIN_HALF_WIDTH = 2.5
RNG_SEED = 20260822
ALPHA = 0.20
STUDENT_T_DEGREES_OF_FREEDOM = 5.0
FUNCTIONAL_RANK = 2
FUNCTIONAL_RIDGE_ALPHA = 10.0
ONLINE_POOL_SHRINKAGE = 0.20
ARTIFACT_STEM = "spx-close-convergence-monte-carlo-2026-08-22"
MODEL_NAMES = (
    "spot_zero",
    "empirical",
    "rv_scaled",
    "seasonal_student_t",
    "whole_path_analog",
    "functional_ridge",
    "online_pool",
)


@dataclass(frozen=True)
class SessionPath:
    session_date: date
    epoch_seconds: np.ndarray
    spx: np.ndarray
    es: np.ndarray
    spx_coverage: float
    es_coverage: float
    raw_rows: int


@dataclass(frozen=True)
class SampleSet:
    X: np.ndarray
    feature_names: tuple[str, ...]
    session_dates: np.ndarray
    decision_epoch: np.ndarray
    horizons: np.ndarray
    current_spx: np.ndarray
    close_spx: np.ndarray
    target_move: np.ndarray
    rv15: np.ndarray


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


def _available_session_dates() -> list[date]:
    available: list[date] = []
    for provider_path in QUOTE_ROOT.glob("date=2026-*/provider=schwab"):
        try:
            value = date.fromisoformat(provider_path.parent.name.removeprefix("date="))
        except ValueError:
            continue
        if START_DATE <= value <= END_DATE and DEFAULT_MARKET_CALENDAR.session(value) is not None:
            available.append(value)
    return sorted(set(available))


def _minute_bucket_sql() -> str:
    return """
    WITH filtered AS (
      SELECT
        received_at,
        source_at,
        instrument_id,
        effective_price
      FROM read_parquet(?, hive_partitioning=true, union_by_name=true)
      WHERE provider = 'schwab'
        AND instrument_id IN ('index:SPX', 'future:ES')
        AND quality = 'live'
        AND lower(coalesce(market_data_type, 'live')) IN ('live', '1')
        AND effective_price > 0
        AND source_at IS NOT NULL
        AND source_at >= received_at - INTERVAL 30 SECOND
        AND source_at <= received_at + INTERVAL 5 SECOND
        AND received_at > ?
        AND received_at <= ?
    )
    SELECT
      to_timestamp((floor(epoch(received_at) / 60) + 1) * 60) AS bucket_end,
      instrument_id,
      arg_max(effective_price, received_at) AS price,
      count(*) AS quote_count
    FROM filtered
    GROUP BY 1, 2
    ORDER BY 1, 2
    """


def _forward_fill(
    observed_epoch: np.ndarray,
    observed_price: np.ndarray,
    timeline: np.ndarray,
    *,
    max_age_seconds: int = 90,
) -> np.ndarray:
    output = np.full(len(timeline), np.nan)
    if len(observed_epoch) == 0:
        return output
    positions = np.searchsorted(observed_epoch, timeline, side="right") - 1
    valid = positions >= 0
    ages = np.full(len(timeline), np.inf)
    ages[valid] = timeline[valid] - observed_epoch[positions[valid]]
    fresh = valid & (ages <= max_age_seconds)
    output[fresh] = observed_price[positions[fresh]]
    return output


def load_session_path(session_date: date) -> SessionPath | None:
    session = DEFAULT_MARKET_CALENDAR.session(session_date)
    if session is None:
        return None
    source_path = (
        QUOTE_ROOT
        / f"date={session_date.isoformat()}"
        / "provider=schwab"
        / "hour=*"
        / "quotes.parquet"
    )
    if not any(source_path.parent.parent.glob("hour=*/quotes.parquet")):
        return None
    connection = duckdb.connect()
    try:
        rows = connection.execute(
            _minute_bucket_sql(),
            [str(source_path), session.open_at.astimezone(timezone.utc), session.close_at.astimezone(timezone.utc)],
        ).fetchall()
    finally:
        connection.close()
    timeline = np.arange(
        int(session.open_at.timestamp()) + 60,
        int(session.close_at.timestamp()) + 1,
        60,
        dtype=np.int64,
    )
    grouped: dict[str, list[tuple[int, float, int]]] = defaultdict(list)
    for bucket_end, instrument_id, price, quote_count in rows:
        grouped[str(instrument_id)].append(
            (int(bucket_end.timestamp()), float(price), int(quote_count))
        )
    instruments: dict[str, np.ndarray] = {}
    for instrument_id in ("index:SPX", "future:ES"):
        values = grouped[instrument_id]
        observed_epoch = np.asarray([row[0] for row in values], dtype=np.int64)
        observed_price = np.asarray([row[1] for row in values], dtype=float)
        instruments[instrument_id] = _forward_fill(observed_epoch, observed_price, timeline)
    return SessionPath(
        session_date=session_date,
        epoch_seconds=timeline,
        spx=instruments["index:SPX"],
        es=instruments["future:ES"],
        spx_coverage=float(np.mean(np.isfinite(instruments["index:SPX"]))),
        es_coverage=float(np.mean(np.isfinite(instruments["future:ES"]))),
        raw_rows=sum(row[2] for values in grouped.values() for row in values),
    )


def load_sessions() -> list[SessionPath]:
    sessions: list[SessionPath] = []
    for session_date in _available_session_dates():
        path = load_session_path(session_date)
        if path is None:
            continue
        if path.spx_coverage >= MIN_SESSION_COVERAGE and path.es_coverage >= MIN_SESSION_COVERAGE:
            sessions.append(path)
    return sessions


def _window(values: np.ndarray, index: int, minutes: int) -> np.ndarray:
    start = max(0, index - minutes)
    return values[start : index + 1]


def _return(values: np.ndarray, index: int, minutes: int) -> float:
    if index - minutes < 0:
        return math.nan
    current = values[index]
    previous = values[index - minutes]
    return float(current - previous) if np.isfinite(current) and np.isfinite(previous) else math.nan


def _realized_volatility(values: np.ndarray, index: int, minutes: int) -> float:
    observed = _window(values, index, minutes)
    observed = observed[np.isfinite(observed)]
    if len(observed) < max(3, int(minutes * 0.8)):
        return math.nan
    return float(np.sqrt(np.sum(np.diff(observed) ** 2)))


def _range(values: np.ndarray, index: int, minutes: int) -> float:
    observed = _window(values, index, minutes)
    observed = observed[np.isfinite(observed)]
    return float(np.max(observed) - np.min(observed)) if len(observed) else math.nan


def _sample_features(path: SessionPath, index: int, horizon: int) -> tuple[list[float], tuple[str, ...]]:
    spx = path.spx
    es = path.es
    elapsed = index + 1
    observed = spx[: index + 1]
    observed = observed[np.isfinite(observed)]
    session_low = float(np.min(observed))
    session_high = float(np.max(observed))
    session_range = max(session_high - session_low, 1e-6)
    current = float(spx[index])
    names: list[str] = []
    values: list[float] = []

    def add(name: str, value: float) -> None:
        names.append(name)
        values.append(float(value))

    add("minutes_to_close", horizon)
    add("sqrt_minutes_to_close", math.sqrt(horizon))
    for minutes in (5, 15, 30, 60):
        add(f"spx_return_{minutes}m", _return(spx, index, minutes))
        add(f"spx_rv_{minutes}m", _realized_volatility(spx, index, minutes))
        add(f"spx_range_{minutes}m", _range(spx, index, minutes))
        add(f"es_return_{minutes}m", _return(es, index, minutes))
    for minutes in (5, 15, 60):
        add(
            f"es_minus_spx_return_{minutes}m",
            _return(es, index, minutes) - _return(spx, index, minutes),
        )
    add("distance_from_open", current - float(spx[0]))
    add("distance_from_session_mean", current - float(np.mean(observed)))
    add("session_range_so_far", session_range)
    add("session_range_position", (current - session_low) / session_range - 0.5)
    add("elapsed_minutes", elapsed)
    add("rv15_per_sqrt_minute", _realized_volatility(spx, index, 15) / math.sqrt(15))
    return values, tuple(names)


def build_samples(sessions: Sequence[SessionPath]) -> SampleSet:
    features: list[list[float]] = []
    feature_names: tuple[str, ...] | None = None
    session_dates: list[date] = []
    decision_epoch: list[int] = []
    horizons: list[int] = []
    current_spx: list[float] = []
    close_spx: list[float] = []
    target_move: list[float] = []
    rv15: list[float] = []
    for path in sessions:
        if not np.isfinite(path.spx[-1]):
            continue
        for horizon in HORIZONS_MINUTES:
            decision_time = path.epoch_seconds[-1] - horizon * 60
            matches = np.flatnonzero(path.epoch_seconds == decision_time)
            if len(matches) != 1:
                continue
            index = int(matches[0])
            if index < 60 or not np.isfinite(path.spx[index]):
                continue
            row, names = _sample_features(path, index, horizon)
            if feature_names is None:
                feature_names = names
            elif feature_names != names:
                raise RuntimeError("feature schema drift")
            observed_rv15 = _realized_volatility(path.spx, index, 15)
            if not math.isfinite(observed_rv15) or observed_rv15 <= 0:
                continue
            features.append(row)
            session_dates.append(path.session_date)
            decision_epoch.append(int(decision_time))
            horizons.append(horizon)
            current_spx.append(float(path.spx[index]))
            close_spx.append(float(path.spx[-1]))
            target_move.append(float(path.spx[-1] - path.spx[index]))
            rv15.append(observed_rv15)
    if feature_names is None:
        raise RuntimeError("no close-distribution samples")
    return SampleSet(
        X=np.asarray(features, dtype=float),
        feature_names=feature_names,
        session_dates=np.asarray(session_dates, dtype=object),
        decision_epoch=np.asarray(decision_epoch, dtype=np.int64),
        horizons=np.asarray(horizons, dtype=np.int64),
        current_spx=np.asarray(current_spx, dtype=float),
        close_spx=np.asarray(close_spx, dtype=float),
        target_move=np.asarray(target_move, dtype=float),
        rv15=np.asarray(rv15, dtype=float),
    )


def _finite_curve(values: np.ndarray) -> np.ndarray:
    observed = np.flatnonzero(np.isfinite(values))
    if len(observed) < 2:
        raise RuntimeError("path has fewer than two finite observations")
    return np.interp(np.arange(len(values)), observed, values[observed])


def _path_scale(path: SessionPath, index: int) -> float:
    curve = _finite_curve(path.spx[: index + 1])
    start = max(0, len(curve) - 16)
    increments = np.diff(curve[start:])
    scale = float(np.sqrt(np.sum(increments**2)) / math.sqrt(max(len(increments), 1)))
    return max(scale, 0.05)


def _future_path(path: SessionPath, index: int, horizon: int) -> np.ndarray:
    segment = _finite_curve(path.spx[index : index + horizon + 1])
    if len(segment) != horizon + 1:
        raise RuntimeError("future path length mismatch")
    return segment[1:] - segment[0]


def _neighbor_weights(
    train_X: np.ndarray,
    current_X: np.ndarray,
) -> tuple[np.ndarray, np.ndarray, float]:
    medians = np.nanmedian(train_X, axis=0)
    q25 = np.nanquantile(train_X, 0.25, axis=0)
    q75 = np.nanquantile(train_X, 0.75, axis=0)
    scale = np.where(q75 - q25 > 1e-6, q75 - q25, 1.0)
    normalized_train = (np.where(np.isfinite(train_X), train_X, medians) - medians) / scale
    normalized_current = (np.where(np.isfinite(current_X), current_X, medians) - medians) / scale
    distances = np.sqrt(np.mean((normalized_train - normalized_current) ** 2, axis=1))
    neighbor_count = min(len(train_X), max(8, min(15, len(train_X) // 2 + 2)))
    selected = np.argsort(distances)[:neighbor_count]
    selected_distance = distances[selected]
    temperature = max(float(np.median(selected_distance)), 0.25)
    weights = np.exp(-selected_distance / temperature)
    weights /= np.sum(weights)
    effective_paths = float(1.0 / np.sum(weights**2))
    return selected, weights, effective_paths


def _whole_path_analog_paths(
    train_paths: Sequence[SessionPath],
    train_X: np.ndarray,
    current_X: np.ndarray,
    *,
    index: int,
    horizon: int,
    current_scale: float,
    seed: int,
) -> tuple[np.ndarray, float]:
    selected, weights, effective_paths = _neighbor_weights(train_X, current_X)
    normalized_paths = np.asarray(
        [
            _future_path(train_paths[position], index, horizon)
            / _path_scale(train_paths[position], index)
            for position in selected
        ],
        dtype=float,
    )
    rng = np.random.default_rng(seed)
    sampled = rng.choice(len(selected), size=MONTE_CARLO_DRAWS, replace=True, p=weights)
    return normalized_paths[sampled] * current_scale, effective_paths


def _seasonal_student_t_paths(
    train_paths: Sequence[SessionPath],
    *,
    index: int,
    horizon: int,
    current_scale: float,
    seed: int,
) -> np.ndarray:
    increments = np.asarray(
        [np.diff(_finite_curve(path.spx)) for path in train_paths],
        dtype=float,
    )
    seasonal_scale = 1.4826 * np.median(np.abs(increments), axis=0)
    positive = seasonal_scale[seasonal_scale > 1e-6]
    fallback = float(np.median(positive)) if len(positive) else current_scale
    seasonal_scale = np.where(seasonal_scale > 1e-6, seasonal_scale, fallback)
    recent_start = max(0, index - 15)
    recent_seasonal = float(np.sqrt(np.mean(seasonal_scale[recent_start:index] ** 2)))
    volatility_multiplier = float(np.clip(current_scale / max(recent_seasonal, 0.05), 0.5, 2.0))
    future_scale = seasonal_scale[index : index + horizon] * volatility_multiplier
    rng = np.random.default_rng(seed)
    standardized = rng.standard_t(
        STUDENT_T_DEGREES_OF_FREEDOM,
        size=(MONTE_CARLO_DRAWS, horizon),
    ) * math.sqrt(
        (STUDENT_T_DEGREES_OF_FREEDOM - 2.0) / STUDENT_T_DEGREES_OF_FREEDOM
    )
    return np.cumsum(standardized * future_scale, axis=1)


def _functional_prefix_suffix_paths(
    train_paths: Sequence[SessionPath],
    current_path: SessionPath,
    *,
    index: int,
    horizon: int,
    current_scale: float,
    seed: int,
) -> tuple[np.ndarray, int]:
    prefix_positions = np.unique(np.append(np.arange(0, index + 1, 15), index))
    training_prefixes: list[np.ndarray] = []
    training_futures: list[np.ndarray] = []
    for path in train_paths:
        prefix = _finite_curve(path.spx[: index + 1])
        scale = _path_scale(path, index)
        training_prefixes.append((prefix[prefix_positions] - prefix[0]) / scale)
        training_futures.append(_future_path(path, index, horizon) / scale)
    prefix_matrix = np.asarray(training_prefixes, dtype=float)
    future_matrix = np.asarray(training_futures, dtype=float)
    prefix_center = np.mean(prefix_matrix, axis=0)
    centered = prefix_matrix - prefix_center
    _left, _singular, right = np.linalg.svd(centered, full_matrices=False)
    rank = max(1, min(FUNCTIONAL_RANK, len(train_paths) - 2, right.shape[0]))
    basis = right[:rank].T
    training_scores = centered @ basis
    current_prefix = _finite_curve(current_path.spx[: index + 1])
    current_normalized = (current_prefix[prefix_positions] - current_prefix[0]) / current_scale
    current_scores = (current_normalized - prefix_center) @ basis
    model = Ridge(alpha=FUNCTIONAL_RIDGE_ALPHA)
    model.fit(training_scores, future_matrix)
    predicted_path = model.predict(current_scores.reshape(1, -1))[0]
    residual_paths = future_matrix - model.predict(training_scores)
    degrees_of_freedom = max(len(train_paths) - rank - 1, 1)
    residual_paths *= math.sqrt(len(train_paths) / degrees_of_freedom)
    rng = np.random.default_rng(seed)
    sampled = rng.choice(len(residual_paths), size=MONTE_CARLO_DRAWS, replace=True)
    return (predicted_path + residual_paths[sampled]) * current_scale, rank


def _online_pool_weights(
    loss_history: Mapping[tuple[str, int], Sequence[float]],
    *,
    horizon: int,
) -> dict[str, float]:
    methods = ("seasonal_student_t", "whole_path_analog", "functional_ridge")
    histories = [loss_history.get((method, horizon), ()) for method in methods]
    if min((len(values) for values in histories), default=0) < 3:
        return {method: 1.0 / len(methods) for method in methods}
    mean_losses = np.asarray([float(np.mean(values)) for values in histories])
    temperature = max(float(np.median(mean_losses)), 1.0)
    raw = np.exp(-(mean_losses - np.min(mean_losses)) / temperature)
    raw /= np.sum(raw)
    shrunk = (1.0 - ONLINE_POOL_SHRINKAGE) * raw + ONLINE_POOL_SHRINKAGE / len(methods)
    return {method: float(weight) for method, weight in zip(methods, shrunk, strict=True)}


def _pool_paths(
    paths_by_method: Mapping[str, np.ndarray],
    weights: Mapping[str, float],
    *,
    seed: int,
) -> np.ndarray:
    methods = tuple(weights)
    probabilities = np.asarray([weights[method] for method in methods], dtype=float)
    probabilities /= np.sum(probabilities)
    rng = np.random.default_rng(seed)
    chosen_methods = rng.choice(len(methods), size=MONTE_CARLO_DRAWS, p=probabilities)
    output = np.empty_like(next(iter(paths_by_method.values())))
    for method_index, method in enumerate(methods):
        positions = np.flatnonzero(chosen_methods == method_index)
        if not len(positions):
            continue
        source = paths_by_method[method]
        sampled = rng.choice(len(source), size=len(positions), replace=True)
        output[positions] = source[sampled]
    return output


def _touch_probabilities(paths: np.ndarray, current: float) -> dict[str, float]:
    center = 5.0 * math.floor(current / 5.0 + 0.5)
    absolute_paths = current + paths
    minimum = np.minimum(current, np.min(absolute_paths, axis=1))
    maximum = np.maximum(current, np.max(absolute_paths, axis=1))
    return {
        f"{level:.0f}": float(np.mean((minimum <= level) & (maximum >= level)))
        for level in (center - 10.0, center - 5.0, center, center + 5.0, center + 10.0)
    }


def _distribution_metrics(draws: np.ndarray, actual: float) -> tuple[float, float, float, float]:
    q10, q50, q90 = (float(value) for value in np.quantile(draws, (0.1, 0.5, 0.9)))
    ordered = np.sort(draws)
    count = len(ordered)
    coefficients = 2 * np.arange(1, count + 1) - count - 1
    half_pairwise = float(np.sum(coefficients * ordered) / (count * count))
    crps = float(np.mean(np.abs(draws - actual)) - half_pairwise)
    return q10, q50, q90, crps


def _pin_probability(draws: np.ndarray, current: float) -> tuple[float, float, float]:
    strike = 5.0 * math.floor(current / 5.0 + 0.5)
    settlements = current + draws
    probability = float(np.mean(np.abs(settlements - strike) <= PIN_HALF_WIDTH))
    return strike, probability, float(PIN_HALF_WIDTH)


def expanding_predictions(
    samples: SampleSet,
    sessions: Sequence[SessionPath],
    *,
    include_settlement_draws: bool = False,
) -> list[dict[str, Any]]:
    unique_dates = sorted(set(samples.session_dates.tolist()))
    paths_by_date = {path.session_date: path for path in sessions}
    predictions: list[dict[str, Any]] = []
    loss_history: dict[tuple[str, int], list[float]] = defaultdict(list)
    distance_features = tuple(
        index
        for index, name in enumerate(samples.feature_names)
        if name
        in {
            "spx_return_5m",
            "spx_return_15m",
            "spx_return_60m",
            "spx_rv_15m",
            "spx_rv_60m",
            "spx_range_60m",
            "es_return_5m",
            "es_return_15m",
            "es_return_60m",
            "es_minus_spx_return_15m",
            "distance_from_open",
            "session_range_position",
            "session_range_so_far",
        }
    )
    for test_position in range(MIN_TRAIN_SESSIONS, len(unique_dates)):
        test_date = unique_dates[test_position]
        train_mask = samples.session_dates < test_date
        train_dates = samples.session_dates[train_mask]
        if len(train_dates) == 0 or max(train_dates) >= test_date:
            raise RuntimeError("session-held-out split is not causal")
        test_indices = np.flatnonzero(samples.session_dates == test_date)
        for index in test_indices:
            horizon = int(samples.horizons[index])
            same_horizon = train_mask & (samples.horizons == horizon)
            training_indices = np.flatnonzero(same_horizon)
            train_moves = samples.target_move[same_horizon]
            train_rv = samples.rv15[same_horizon]
            if len(train_moves) < MIN_TRAIN_SESSIONS:
                continue
            current_rv = float(samples.rv15[index])
            current = float(samples.current_spx[index])
            actual = float(samples.target_move[index])
            current_path = paths_by_date[test_date]
            path_index = len(current_path.spx) - horizon - 1
            if int(current_path.epoch_seconds[path_index]) != int(samples.decision_epoch[index]):
                raise RuntimeError("sample and path decision timestamps are misaligned")
            training_paths = [paths_by_date[samples.session_dates[item]] for item in training_indices]
            current_scale = _path_scale(current_path, path_index)
            seed = RNG_SEED + int(test_date.strftime("%Y%m%d")) + horizon
            empirical_draws = np.resize(train_moves, MONTE_CARLO_DRAWS)
            rv_draws = np.resize(
                train_moves * np.clip(current_rv / np.maximum(train_rv, 1e-6), 0.5, 2.0),
                MONTE_CARLO_DRAWS,
            )
            seasonal_paths = _seasonal_student_t_paths(
                training_paths,
                index=path_index,
                horizon=horizon,
                current_scale=current_scale,
                seed=seed + 101,
            )
            analog_paths, effective_paths = _whole_path_analog_paths(
                training_paths,
                samples.X[same_horizon][:, distance_features],
                samples.X[index, distance_features],
                index=path_index,
                horizon=horizon,
                current_scale=current_scale,
                seed=seed + 202,
            )
            functional_paths, functional_rank = _functional_prefix_suffix_paths(
                training_paths,
                current_path,
                index=path_index,
                horizon=horizon,
                current_scale=current_scale,
                seed=seed + 303,
            )
            candidate_paths = {
                "seasonal_student_t": seasonal_paths,
                "whole_path_analog": analog_paths,
                "functional_ridge": functional_paths,
            }
            online_weights = _online_pool_weights(loss_history, horizon=horizon)
            online_paths = _pool_paths(candidate_paths, online_weights, seed=seed + 404)
            path_models = candidate_paths | {"online_pool": online_paths}
            pin_strike = 5.0 * math.floor(current / 5.0 + 0.5)
            base = {
                "session_date": test_date.isoformat(),
                "decision_at": datetime.fromtimestamp(
                    int(samples.decision_epoch[index]), timezone.utc
                ).isoformat(),
                "horizon_minutes": horizon,
                "current_spx": current,
                "close_spx": float(samples.close_spx[index]),
                "actual_move": actual,
                "pin_strike": pin_strike,
                "pin_actual": float(
                    abs(float(samples.close_spx[index]) - pin_strike) <= PIN_HALF_WIDTH
                ),
            }
            for method, draws in (
                ("empirical", empirical_draws),
                ("rv_scaled", rv_draws),
            ):
                q10, q50, q90, crps = _distribution_metrics(draws, actual)
                _strike, pin_probability, _half_width = _pin_probability(draws, current)
                predictions.append(
                    base
                    | {
                        "method": method,
                        "q10_move": q10,
                        "q50_move": q50,
                        "q90_move": q90,
                        "crps": crps,
                        "pin_probability": pin_probability,
                    }
                )
            current_losses: dict[str, float] = {}
            for method, paths in path_models.items():
                draws = paths[:, -1]
                q10, q50, q90, crps = _distribution_metrics(draws, actual)
                _strike, pin_probability, _half_width = _pin_probability(draws, current)
                row = base | {
                    "method": method,
                    "q10_move": q10,
                    "q50_move": q50,
                    "q90_move": q90,
                    "crps": crps,
                    "pin_probability": pin_probability,
                }
                if method == "whole_path_analog":
                    row["effective_analog_paths"] = effective_paths
                elif method == "functional_ridge":
                    row["functional_rank"] = functional_rank
                elif method == "online_pool":
                    row["online_weights"] = online_weights
                    row["touch_probabilities"] = _touch_probabilities(paths, current)
                    if include_settlement_draws:
                        row["settlement_draws"] = current + draws
                predictions.append(row)
                current_losses[method] = crps
            predictions.append(
                base
                | {
                    "method": "spot_zero",
                    "q10_move": None,
                    "q50_move": 0.0,
                    "q90_move": None,
                    "crps": None,
                    "pin_probability": None,
                }
            )
            for method, loss in current_losses.items():
                loss_history[(method, horizon)].append(loss)
    return predictions


def add_prequential_conformal(predictions: list[dict[str, Any]]) -> None:
    history: dict[tuple[str, int], list[float]] = defaultdict(list)
    ordered = sorted(
        predictions,
        key=lambda row: (row["session_date"], row["horizon_minutes"], row["method"]),
    )
    for row in ordered:
        lower = row.get("q10_move")
        upper = row.get("q90_move")
        if lower is None or upper is None:
            row["conformal_q10_move"] = None
            row["conformal_q90_move"] = None
            continue
        key = (str(row["method"]), int(row["horizon_minutes"]))
        scores = history[key]
        if len(scores) >= 5:
            rank = min(len(scores) - 1, math.ceil((len(scores) + 1) * (1 - ALPHA)) - 1)
            adjustment = float(np.sort(scores)[rank])
            row["conformal_q10_move"] = float(lower) - adjustment
            row["conformal_q90_move"] = float(upper) + adjustment
        else:
            row["conformal_q10_move"] = None
            row["conformal_q90_move"] = None
        actual = float(row["actual_move"])
        scores.append(max(float(lower) - actual, actual - float(upper), 0.0))


def _metric_rows(predictions: Sequence[Mapping[str, Any]]) -> list[dict[str, Any]]:
    grouped: dict[str, list[Mapping[str, Any]]] = defaultdict(list)
    for row in predictions:
        grouped[str(row["method"])].append(row)
    output: list[dict[str, Any]] = []
    for method, rows in sorted(grouped.items()):
        absolute_errors = [abs(float(row["actual_move"]) - float(row["q50_move"])) for row in rows]
        intervals = [row for row in rows if row.get("q10_move") is not None]
        conformal = [row for row in rows if row.get("conformal_q10_move") is not None]
        crps = [float(row["crps"]) for row in rows if row.get("crps") is not None]
        pin = [row for row in rows if row.get("pin_probability") is not None]
        output.append(
            {
                "method": method,
                "n": len(rows),
                "session_count": len({row["session_date"] for row in rows}),
                "mean_absolute_error_points": float(np.mean(absolute_errors)),
                "mean_bias_points": float(
                    np.mean([float(row["q50_move"]) - float(row["actual_move"]) for row in rows])
                ),
                "raw_80_coverage": (
                    float(
                        np.mean(
                            [
                                float(row["q10_move"])
                                <= float(row["actual_move"])
                                <= float(row["q90_move"])
                                for row in intervals
                            ]
                        )
                    )
                    if intervals
                    else None
                ),
                "raw_mean_width_points": (
                    float(
                        np.mean(
                            [float(row["q90_move"]) - float(row["q10_move"]) for row in intervals]
                        )
                    )
                    if intervals
                    else None
                ),
                "conformal_n": len(conformal),
                "conformal_80_coverage": (
                    float(
                        np.mean(
                            [
                                float(row["conformal_q10_move"])
                                <= float(row["actual_move"])
                                <= float(row["conformal_q90_move"])
                                for row in conformal
                            ]
                        )
                    )
                    if conformal
                    else None
                ),
                "conformal_mean_width_points": (
                    float(
                        np.mean(
                            [
                                float(row["conformal_q90_move"])
                                - float(row["conformal_q10_move"])
                                for row in conformal
                            ]
                        )
                    )
                    if conformal
                    else None
                ),
                "crps_points": float(np.mean(crps)) if crps else None,
                "pin_brier": (
                    float(
                        np.mean(
                            [
                                (float(row["pin_probability"]) - float(row["pin_actual"])) ** 2
                                for row in pin
                            ]
                        )
                    )
                    if pin
                    else None
                ),
            }
        )
    return output


def _horizon_rows(
    predictions: Sequence[Mapping[str, Any]], *, method: str = "online_pool"
) -> list[dict[str, Any]]:
    output: list[dict[str, Any]] = []
    for horizon in HORIZONS_MINUTES:
        rows = [
            row
            for row in predictions
            if row["method"] == method and int(row["horizon_minutes"]) == horizon
        ]
        if not rows:
            continue
        output.append(
            {
                "horizon_minutes": horizon,
                "n": len(rows),
                "mean_absolute_error_points": float(
                    np.mean(
                        [abs(float(row["q50_move"]) - float(row["actual_move"])) for row in rows]
                    )
                ),
                "raw_80_coverage": float(
                    np.mean(
                        [
                            float(row["q10_move"])
                            <= float(row["actual_move"])
                            <= float(row["q90_move"])
                            for row in rows
                        ]
                    )
                ),
                "mean_width_points": float(
                    np.mean(
                        [float(row["q90_move"]) - float(row["q10_move"]) for row in rows]
                    )
                ),
                "crps_points": float(np.mean([float(row["crps"]) for row in rows])),
                "pin_brier": float(
                    np.mean(
                        [
                            (float(row["pin_probability"]) - float(row["pin_actual"])) ** 2
                            for row in rows
                        ]
                    )
                ),
            }
        )
    return output


def _paired_session_bootstrap(
    predictions: Sequence[Mapping[str, Any]],
    *,
    challenger: str,
    baseline: str,
    metric: str,
    iterations: int = 2_000,
) -> dict[str, Any]:
    by_key = {
        (str(row["session_date"]), int(row["horizon_minutes"]), str(row["method"])): row
        for row in predictions
    }
    pairs: list[tuple[str, float]] = []
    for session_date, horizon, method in list(by_key):
        if method != challenger:
            continue
        challenger_row = by_key[(session_date, horizon, challenger)]
        baseline_row = by_key.get((session_date, horizon, baseline))
        if baseline_row is None:
            continue
        if metric == "mean_absolute_error":
            challenger_value = abs(
                float(challenger_row["q50_move"]) - float(challenger_row["actual_move"])
            )
            baseline_value = abs(
                float(baseline_row["q50_move"]) - float(baseline_row["actual_move"])
            )
        elif metric == "crps":
            challenger_value = float(challenger_row["crps"])
            baseline_value = float(baseline_row["crps"])
        elif metric == "pin_brier":
            challenger_value = (
                float(challenger_row["pin_probability"]) - float(challenger_row["pin_actual"])
            ) ** 2
            baseline_value = (
                float(baseline_row["pin_probability"]) - float(baseline_row["pin_actual"])
            ) ** 2
        else:
            raise ValueError(metric)
        pairs.append((session_date, challenger_value - baseline_value))
    sessions = sorted({session_date for session_date, _value in pairs})
    values_by_session = {
        session_date: np.asarray(
            [value for observed, value in pairs if observed == session_date], dtype=float
        )
        for session_date in sessions
    }
    observed = float(np.mean([value for _session, value in pairs]))
    rng = np.random.default_rng(RNG_SEED + len(metric))
    draws = np.empty(iterations)
    for index in range(iterations):
        sampled = rng.choice(sessions, size=len(sessions), replace=True)
        draws[index] = float(
            np.mean(np.concatenate([values_by_session[str(session)] for session in sampled]))
        )
    return {
        "challenger": challenger,
        "baseline": baseline,
        "metric": metric,
        "difference_challenger_minus_baseline": observed,
        "session_cluster_95_interval": [
            float(np.quantile(draws, 0.025)),
            float(np.quantile(draws, 0.975)),
        ],
        "probability_challenger_better": float(np.mean(draws < 0.0)),
        "sessions": len(sessions),
    }


def _latest_session_rows(predictions: Sequence[Mapping[str, Any]]) -> list[dict[str, Any]]:
    latest = max(str(row["session_date"]) for row in predictions)
    return [
        {
            "horizon_minutes": int(row["horizon_minutes"]),
            "decision_spx": float(row["current_spx"]),
            "p10_close": float(row["current_spx"]) + float(row["q10_move"]),
            "p50_close": float(row["current_spx"]) + float(row["q50_move"]),
            "p90_close": float(row["current_spx"]) + float(row["q90_move"]),
            "observed_close": float(row["close_spx"]),
            "nearest_5pt_pin": float(row["pin_strike"]),
            "pin_probability": float(row["pin_probability"]),
            "pin_actual": bool(row["pin_actual"]),
        }
        for row in sorted(
            (
                item
                for item in predictions
                if item["session_date"] == latest and item["method"] == "online_pool"
            ),
            key=lambda item: int(item["horizon_minutes"]),
            reverse=True,
        )
    ]


def _latest_touch_rows(predictions: Sequence[Mapping[str, Any]]) -> list[dict[str, Any]]:
    latest = max(str(row["session_date"]) for row in predictions)
    output: list[dict[str, Any]] = []
    selected = sorted(
        (
            row
            for row in predictions
            if row["session_date"] == latest and row["method"] == "online_pool"
        ),
        key=lambda row: int(row["horizon_minutes"]),
        reverse=True,
    )
    for row in selected:
        for level, probability in row.get("touch_probabilities", {}).items():
            output.append(
                {
                    "horizon_minutes": int(row["horizon_minutes"]),
                    "decision_spx": float(row["current_spx"]),
                    "level": float(level),
                    "touch_probability": float(probability),
                }
            )
    return output


def _model_diagnostics(predictions: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    online_rows = [row for row in predictions if row["method"] == "online_pool"]
    analog_rows = [row for row in predictions if row["method"] == "whole_path_analog"]
    first_dates = sorted({str(row["session_date"]) for row in online_rows})[:3]
    initial_equal = all(
        all(abs(float(weight) - 1.0 / 3.0) < 1e-12 for weight in row["online_weights"].values())
        for row in online_rows
        if str(row["session_date"]) in first_dates
    )
    latest = max(str(row["session_date"]) for row in online_rows)
    latest_weights = [
        {
            "horizon_minutes": int(row["horizon_minutes"]),
            **{f"weight_{method}": float(weight) for method, weight in row["online_weights"].items()},
        }
        for row in sorted(
            (row for row in online_rows if row["session_date"] == latest),
            key=lambda row: int(row["horizon_minutes"]),
            reverse=True,
        )
    ]
    effective = np.asarray([float(row["effective_analog_paths"]) for row in analog_rows])
    return {
        "initial_three_oos_sessions_use_equal_pool_weights": initial_equal,
        "minimum_effective_analog_paths": float(np.min(effective)),
        "median_effective_analog_paths": float(np.median(effective)),
        "functional_ranks_observed": sorted(
            {
                int(row["functional_rank"])
                for row in predictions
                if row["method"] == "functional_ridge"
            }
        ),
        "latest_online_weights": latest_weights,
    }


def _convergence_svg(horizon_rows: Sequence[Mapping[str, Any]]) -> str:
    width, height = 920, 380
    left, right, top, bottom = 75.0, 890.0, 45.0, 315.0
    ordered = sorted(horizon_rows, key=lambda row: int(row["horizon_minutes"]))
    x_values = np.asarray([float(row["horizon_minutes"]) for row in ordered])
    y_values = np.asarray([float(row["mean_width_points"]) for row in ordered])
    x_max = max(float(np.max(x_values)), 1.0)
    y_max = max(float(np.max(y_values)) * 1.1, 1.0)
    points: list[str] = []
    circles: list[str] = []
    for horizon, interval_width in zip(x_values, y_values, strict=True):
        x = left + horizon / x_max * (right - left)
        y = bottom - interval_width / y_max * (bottom - top)
        points.append(f"{x:.1f},{y:.1f}")
        circles.append(
            f'<circle cx="{x:.1f}" cy="{y:.1f}" r="5" fill="#7C3AED"/>'
            f'<text x="{x:.1f}" y="{y - 10:.1f}" text-anchor="middle" font-size="13">{interval_width:.1f}</text>'
        )
    return "".join(
        [
            f'<svg xmlns="http://www.w3.org/2000/svg" width="{width}" height="{height}" viewBox="0 0 {width} {height}">',
            '<rect width="100%" height="100%" fill="white"/>',
            '<text x="30" y="26" font-family="sans-serif" font-size="20" font-weight="700">Online pool: mean P10–P90 width</text>',
            f'<line x1="{left}" y1="{bottom}" x2="{right}" y2="{bottom}" stroke="#94A3B8"/>',
            f'<line x1="{left}" y1="{top}" x2="{left}" y2="{bottom}" stroke="#94A3B8"/>',
            f'<polyline points="{" ".join(points)}" fill="none" stroke="#7C3AED" stroke-width="3"/>',
            *circles,
            f'<text x="{(left + right) / 2:.1f}" y="360" text-anchor="middle" font-family="sans-serif" font-size="15">Minutes to RTH close</text>',
            '<text x="18" y="190" transform="rotate(-90 18 190)" text-anchor="middle" font-family="sans-serif" font-size="15">Interval width (SPX points)</text>',
            '</svg>',
        ]
    )


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
                text = f"{value:.4f}"
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
    conclusion = artifact["conclusion"]
    html = f"""<!doctype html>
<html lang="zh-CN"><head><meta charset="utf-8"><title>SPX close convergence MC</title>
<style>body{{font-family:system-ui,sans-serif;max-width:1100px;margin:32px auto;color:#172033}}table{{border-collapse:collapse;width:100%;margin:12px 0 28px}}th,td{{border:1px solid #dbe2ea;padding:8px;text-align:right}}th:first-child,td:first-child{{text-align:left}}.bad{{color:#b42318}}.good{{color:#067647}}code{{background:#f2f4f7;padding:2px 5px}}</style></head>
<body><h1>SPX RTH 收盘收敛区间探索</h1>
<p><strong>结论：</strong>{escape(str(conclusion['status']))} — {escape(str(conclusion['summary']))}</p>
<p>原始 Schwab SPX/ES 一分钟因果桶；每个测试日仅使用更早交易日训练。Action authority: NONE。</p>
{artifact['convergence_svg']}
<h2>总体 OOS 指标</h2>{_table_html(artifact['overall_metrics'], ('method','n','session_count','mean_absolute_error_points','raw_80_coverage','raw_mean_width_points','conformal_80_coverage','crps_points','pin_brier'))}
<h2>Online pool 按剩余时间</h2>{_table_html(artifact['horizon_metrics'], ('horizon_minutes','n','mean_absolute_error_points','raw_80_coverage','mean_width_points','crps_points','pin_brier'))}
<h2>配对 session bootstrap</h2>{_table_html(artifact['paired_bootstrap'], ('metric','difference_challenger_minus_baseline','session_cluster_95_interval','probability_challenger_better','sessions'))}
<h2>最近留出日</h2>{_table_html(artifact['latest_session'], ('horizon_minutes','decision_spx','p10_close','p50_close','p90_close','observed_close','nearest_5pt_pin','pin_probability','pin_actual'))}
<h2>最近留出日关键位触及概率</h2>{_table_html(artifact['latest_touch_probabilities'], ('horizon_minutes','decision_spx','level','touch_probability'))}
<h2>最近留出日在线权重</h2>{_table_html(artifact['model_diagnostics']['latest_online_weights'], ('horizon_minutes','weight_seasonal_student_t','weight_whole_path_analog','weight_functional_ridge'))}
<h2>限制</h2><ul>{''.join(f'<li>{escape(item)}</li>' for item in artifact['limitations'])}</ul>
</body></html>"""
    html_path.write_text(html, encoding="utf-8")
    return json_path, html_path


def run_analysis(*, write_outputs: bool = True) -> dict[str, Any]:
    sessions = load_sessions()
    if len(sessions) <= MIN_TRAIN_SESSIONS:
        raise RuntimeError(f"insufficient sessions: {len(sessions)}")
    samples = build_samples(sessions)
    predictions = expanding_predictions(samples, sessions)
    add_prequential_conformal(predictions)
    overall_metrics = _metric_rows(predictions)
    horizon_metrics = _horizon_rows(predictions)
    paired = [
        _paired_session_bootstrap(
            predictions,
            challenger="online_pool",
            baseline=baseline,
            metric=metric,
        )
        for baseline in ("rv_scaled", "seasonal_student_t")
        for metric in ("mean_absolute_error", "crps", "pin_brier")
    ]
    width_rank = spearmanr(
        [row["horizon_minutes"] for row in horizon_metrics],
        [row["mean_width_points"] for row in horizon_metrics],
    )
    by_method = {row["method"]: row for row in overall_metrics}
    required_edges = [
        row for row in paired if row["metric"] in {"mean_absolute_error", "crps"}
    ]
    calibrated = by_method["online_pool"].get("raw_80_coverage")
    validated = (
        all(row["session_cluster_95_interval"][1] < 0.0 for row in required_edges)
        and calibrated is not None
        and 0.72 <= float(calibrated) <= 0.88
    )
    conclusion = {
        "status": "forecast_edge_candidate" if validated else "not_yet_validated",
        "summary": (
            "The causal online pool beats both fixed baselines on MAE and CRPS with session-cluster uncertainty while retaining usable raw coverage."
            if validated
            else "The close distribution is measurable, but the four-model comparison has not cleared all paired OOS and calibration gates."
        ),
        "production_eligible": False,
        "action_authority": "NONE",
    }
    artifact: dict[str, Any] = {
        "artifact_version": "spx_close_convergence_mc.v2",
        "created_at": datetime.now(timezone.utc).isoformat(),
        "source": {
            "quote_root": str(QUOTE_ROOT),
            "provider": "schwab",
            "instruments": ["index:SPX", "future:ES"],
            "grain": "one-minute causal quote-update buckets",
            "target": "last observable SPX at or before the exchange-local RTH close",
            "available_at_rule": "source_at <= received_at + 5s; decision features use bucket_end <= decision_at",
        },
        "contract": {
            "date_range": [START_DATE.isoformat(), END_DATE.isoformat()],
            "horizons_minutes": list(HORIZONS_MINUTES),
            "minimum_training_sessions": MIN_TRAIN_SESSIONS,
            "minimum_session_coverage": MIN_SESSION_COVERAGE,
            "monte_carlo_draws": MONTE_CARLO_DRAWS,
            "pin_half_width_points": PIN_HALF_WIDTH,
            "pin_event": "close finishes within +/-2.5 points of the 5-point strike nearest SPX at decision time",
            "models": list(MODEL_NAMES),
            "student_t_degrees_of_freedom": STUDENT_T_DEGREES_OF_FREEDOM,
            "functional_rank_cap": FUNCTIONAL_RANK,
            "functional_ridge_alpha": FUNCTIONAL_RIDGE_ALPHA,
            "online_pool_shrinkage": ONLINE_POOL_SHRINKAGE,
            "strategy_inputs_used": False,
        },
        "data_profile": {
            "complete_sessions": len(sessions),
            "first_session": sessions[0].session_date.isoformat(),
            "last_session": sessions[-1].session_date.isoformat(),
            "median_spx_coverage": float(np.median([item.spx_coverage for item in sessions])),
            "median_es_coverage": float(np.median([item.es_coverage for item in sessions])),
            "samples": len(samples.target_move),
            "oos_sessions": len(set(row["session_date"] for row in predictions)),
            "oos_rows_per_method": len(predictions) // len(MODEL_NAMES),
        },
        "leakage_checks": {
            "split_unit": "exchange_session",
            "training_dates_strictly_precede_test_date": True,
            "random_intraday_split_used": False,
            "production_strategy_inputs_used": False,
            "target_close_in_feature_matrix": False,
        },
        "overall_metrics": overall_metrics,
        "horizon_metrics": horizon_metrics,
        "paired_bootstrap": paired,
        "convergence": {
            "spearman_horizon_vs_interval_width": float(width_rank.statistic),
            "p_value_descriptive_only": float(width_rank.pvalue),
        },
        "latest_session": _latest_session_rows(predictions),
        "latest_touch_probabilities": _latest_touch_rows(predictions),
        "model_diagnostics": _model_diagnostics(predictions),
        "conclusion": conclusion,
        "limitations": [
            "Only about six weeks of complete sessions are available; Monte Carlo draws resample a small number of independent days and do not create new information.",
            "The target is the last causal RTH quote, not the subsequently published official settlement value.",
            "No SPXW exact-leg BBO or option payoff is used, so forecast skill is not yet a tradable edge.",
            "Wall/GEX/Q variables are intentionally excluded in v2 to keep the raw-price baseline independent of production strategy versions.",
            "The online pool uses only prior held-out CRPS, but 14 OOS sessions leave its weights noisy and heavily shrunk.",
            "Touch probability means the sampled path crosses a level; it does not infer dealer inventory or resting liquidity.",
            "Pin Brier comparisons cover multiple models and metrics without a multiplicity correction; the isolated improvement is exploratory.",
            "Coverage is evaluated by held-out session; intraday rows are never randomly split.",
        ],
    }
    artifact["convergence_svg"] = _convergence_svg(horizon_metrics)
    if write_outputs:
        json_path, html_path = _write_outputs(artifact)
        artifact["output_paths"] = {"json": str(json_path), "html": str(html_path)}
    return artifact


if __name__ == "__main__":
    result = run_analysis(write_outputs=True)
    print(json.dumps(_json_safe({
        "data_profile": result["data_profile"],
        "overall_metrics": result["overall_metrics"],
        "paired_bootstrap": result["paired_bootstrap"],
        "convergence": result["convergence"],
        "conclusion": result["conclusion"],
        "output_paths": result["output_paths"],
    }), ensure_ascii=False, indent=2))
