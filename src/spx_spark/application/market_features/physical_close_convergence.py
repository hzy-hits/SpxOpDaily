"""Causal 60-minute physical SPX close-convergence distribution."""

from __future__ import annotations

import json
import math
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Mapping

import duckdb
import numpy as np
from sklearn.linear_model import Ridge

from spx_spark.market_calendar import DEFAULT_MARKET_CALENDAR

CLOSE_CONVERGENCE_MODEL_VERSION = "physical_close_online_pool_60m.v1"
CLOSE_CONVERGENCE_FEATURE_VERSION = "raw_spx_es_prefix_suffix_60m.v1"
CLOSE_CONVERGENCE_HORIZON_MINUTES = 60
CLOSE_CONVERGENCE_MIN_TRAINING_SESSIONS = 15
CLOSE_CONVERGENCE_MONTE_CARLO_DRAWS = 2_000
CLOSE_CONVERGENCE_QUANTILE_COUNT = 51
_CLOSE_CONVERGENCE_WINDOW_DAYS = 45
_CLOSE_CONVERGENCE_WINDOW_SECONDS = 180
_CLOSE_STUDENT_T_DEGREES_OF_FREEDOM = 5.0
_CLOSE_FUNCTIONAL_RANK = 2
_CLOSE_FUNCTIONAL_RIDGE_ALPHA = 10.0
_CLOSE_ONLINE_POOL_SHRINKAGE = 0.20
_CLOSE_RNG_SEED = 20260822
_CLOSE_MIN_COVERAGE = 0.95



@dataclass(frozen=True, slots=True)
class PhysicalCloseConvergenceEstimate:
    """Causal physical SPX close distribution at the frozen 60-minute clock."""

    status: str
    as_of: datetime
    target_at: datetime
    horizon_minutes: int
    center: float | None
    center_probability: float | None
    q10: float | None
    q50: float | None
    q90: float | None
    settlement_quantiles: tuple[float, ...]
    training_sessions: int
    trained_through_date: date | None
    online_weights: tuple[tuple[str, float], ...]
    reason_codes: tuple[str, ...]
    model_version: str = CLOSE_CONVERGENCE_MODEL_VERSION
    feature_version: str = CLOSE_CONVERGENCE_FEATURE_VERSION

    def to_dict(self) -> dict[str, object]:
        return {
            "status": self.status,
            "as_of": self.as_of.isoformat(),
            "target_at": self.target_at.isoformat(),
            "horizon_minutes": self.horizon_minutes,
            "center": self.center,
            "center_probability": self.center_probability,
            "q10": self.q10,
            "q50": self.q50,
            "q90": self.q90,
            "settlement_quantiles": list(self.settlement_quantiles),
            "training_sessions": self.training_sessions,
            "trained_through_date": (
                self.trained_through_date.isoformat()
                if self.trained_through_date is not None
                else None
            ),
            "online_weights": dict(self.online_weights),
            "reason_codes": list(self.reason_codes),
            "model_version": self.model_version,
            "feature_version": self.feature_version,
            "evidence_status": "forward_unvalidated_user_override",
            "action_authority": "none",
            "automatic_ordering": False,
        }


_CLOSE_READY_CACHE: dict[
    tuple[str, date, datetime], PhysicalCloseConvergenceEstimate
] = {}



@dataclass(frozen=True, slots=True)
class _CloseSessionPath:
    session_date: date
    epoch_seconds: np.ndarray
    spx: np.ndarray
    es: np.ndarray
    spx_coverage: float
    es_coverage: float


def estimate_physical_close_convergence(
    data_root: str | Path,
    *,
    now: datetime,
    trading_date: date,
) -> PhysicalCloseConvergenceEstimate:
    """Estimate the 16:00 SPX distribution from raw causal SPX/ES paths.

    The estimator is deliberately clock-frozen. It can become ready only in
    the first three minutes after 15:00 ET, and every training session is
    strictly earlier than ``trading_date``. Production receives 51 terminal
    quantiles rather than the full Monte Carlo draw matrix.
    """

    now_utc = now.astimezone(timezone.utc)
    session = DEFAULT_MARKET_CALENDAR.session(trading_date)
    target_at = session.close_at if session is not None else now_utc
    anchor = target_at - timedelta(minutes=CLOSE_CONVERGENCE_HORIZON_MINUTES)
    if session is None:
        return _close_unavailable(
            now=now_utc,
            target_at=target_at,
            reason="close_convergence_session_unavailable",
        )
    if not anchor <= now_utc < anchor + timedelta(seconds=_CLOSE_CONVERGENCE_WINDOW_SECONDS):
        return _close_unavailable(
            now=now_utc,
            target_at=target_at,
            reason="close_convergence_clock_closed",
        )
    cache_key = (str(Path(data_root).expanduser().resolve()), trading_date, anchor)
    if cached := _CLOSE_READY_CACHE.get(cache_key):
        return cached

    quote_root = Path(data_root).expanduser() / "lake" / "quotes" / "schema=v1"
    earliest = trading_date - timedelta(days=_CLOSE_CONVERGENCE_WINDOW_DAYS)
    prior_dates: list[date] = []
    for provider_path in quote_root.glob("date=*/provider=schwab"):
        try:
            observed_date = date.fromisoformat(provider_path.parent.name.removeprefix("date="))
        except ValueError:
            continue
        if (
            earliest <= observed_date < trading_date
            and DEFAULT_MARKET_CALENDAR.session(observed_date) is not None
        ):
            prior_dates.append(observed_date)
    prior_dates = sorted(set(prior_dates))
    if len(prior_dates) < CLOSE_CONVERGENCE_MIN_TRAINING_SESSIONS:
        return _close_unavailable(
            now=anchor,
            target_at=target_at,
            reason="close_convergence_training_sessions_insufficient",
            training_sessions=len(prior_dates),
            trained_through=max(prior_dates, default=None),
        )

    historical: list[_CloseSessionPath] = []
    current: _CloseSessionPath | None = None
    try:
        connection = duckdb.connect()
        try:
            for session_date in prior_dates:
                historical_session = DEFAULT_MARKET_CALENDAR.session(session_date)
                if historical_session is None:
                    continue
                path = _load_close_session_path(
                    connection,
                    quote_root=quote_root,
                    session_date=session_date,
                    available_at=historical_session.close_at,
                    complete=True,
                )
                if path is not None:
                    historical.append(path)
            current = _load_close_session_path(
                connection,
                quote_root=quote_root,
                session_date=trading_date,
                available_at=anchor,
                complete=False,
                allow_partial=True,
            )
        finally:
            connection.close()
    except (duckdb.Error, OSError, ValueError):
        return _close_unavailable(
            now=anchor,
            target_at=target_at,
            reason="close_convergence_quote_lake_unavailable",
            training_sessions=len(historical),
            trained_through=(historical[-1].session_date if historical else None),
        )
    current = _merge_close_current_state(
        current,
        data_root=Path(data_root).expanduser(),
        trading_date=trading_date,
        available_at=anchor,
    )
    if current is None or len(historical) < CLOSE_CONVERGENCE_MIN_TRAINING_SESSIONS:
        return _close_unavailable(
            now=anchor,
            target_at=target_at,
            reason=(
                "close_convergence_current_path_unavailable"
                if current is None
                else "close_convergence_training_coverage_insufficient"
            ),
            training_sessions=len(historical),
            trained_through=(historical[-1].session_date if historical else None),
        )

    try:
        settlement_draws, weights = _close_online_pool_distribution(
            historical,
            current,
        )
    except (ValueError, RuntimeError, np.linalg.LinAlgError):
        return _close_unavailable(
            now=anchor,
            target_at=target_at,
            reason="close_convergence_model_unavailable",
            training_sessions=len(historical),
            trained_through=historical[-1].session_date,
        )
    q10, q50, q90 = (float(value) for value in np.quantile(settlement_draws, (0.10, 0.50, 0.90)))
    center, center_probability = _close_modal_center(
        settlement_draws,
        q10=q10,
        median=q50,
        q90=q90,
    )
    probability_grid = np.linspace(
        0.5 / CLOSE_CONVERGENCE_QUANTILE_COUNT,
        1.0 - 0.5 / CLOSE_CONVERGENCE_QUANTILE_COUNT,
        CLOSE_CONVERGENCE_QUANTILE_COUNT,
    )
    quantiles = tuple(
        round(float(value), 6) for value in np.quantile(settlement_draws, probability_grid)
    )
    result = PhysicalCloseConvergenceEstimate(
        status="ready",
        as_of=anchor,
        target_at=target_at,
        horizon_minutes=CLOSE_CONVERGENCE_HORIZON_MINUTES,
        center=round(center, 4),
        center_probability=round(center_probability, 6),
        q10=round(q10, 6),
        q50=round(q50, 6),
        q90=round(q90, 6),
        settlement_quantiles=quantiles,
        training_sessions=len(historical),
        trained_through_date=historical[-1].session_date,
        online_weights=tuple(sorted(weights.items())),
        reason_codes=(),
    )
    if len(_CLOSE_READY_CACHE) >= 8:
        _CLOSE_READY_CACHE.pop(next(iter(_CLOSE_READY_CACHE)))
    _CLOSE_READY_CACHE[cache_key] = result
    return result


def _close_unavailable(
    *,
    now: datetime,
    target_at: datetime,
    reason: str,
    training_sessions: int = 0,
    trained_through: date | None = None,
) -> PhysicalCloseConvergenceEstimate:
    return PhysicalCloseConvergenceEstimate(
        status="unavailable",
        as_of=now.astimezone(timezone.utc),
        target_at=target_at.astimezone(timezone.utc),
        horizon_minutes=CLOSE_CONVERGENCE_HORIZON_MINUTES,
        center=None,
        center_probability=None,
        q10=None,
        q50=None,
        q90=None,
        settlement_quantiles=(),
        training_sessions=training_sessions,
        trained_through_date=trained_through,
        online_weights=(),
        reason_codes=(reason,),
    )


def _load_close_session_path(
    connection: duckdb.DuckDBPyConnection,
    *,
    quote_root: Path,
    session_date: date,
    available_at: datetime,
    complete: bool,
    allow_partial: bool = False,
) -> _CloseSessionPath | None:
    session = DEFAULT_MARKET_CALENDAR.session(session_date)
    if session is None:
        return None
    day_root = quote_root / f"date={session_date.isoformat()}" / "provider=schwab"
    files = [str(path) for path in sorted(day_root.glob("hour=*/quotes.parquet"))]
    if not files:
        return None
    rows = connection.execute(
        """
        WITH filtered AS (
          SELECT received_at, source_at, instrument_id, effective_price
          FROM read_parquet(?, union_by_name=true)
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
          arg_max(effective_price, received_at) AS price
        FROM filtered
        GROUP BY 1, 2
        ORDER BY 1, 2
        """,
        [
            files,
            session.open_at.astimezone(timezone.utc),
            available_at.astimezone(timezone.utc),
        ],
    ).fetchall()
    timeline = np.arange(
        int(session.open_at.timestamp()) + 60,
        int(session.close_at.timestamp()) + 1,
        60,
        dtype=np.int64,
    )
    grouped: dict[str, list[tuple[int, float]]] = {
        "index:SPX": [],
        "future:ES": [],
    }
    for bucket_end, instrument_id, price in rows:
        grouped[str(instrument_id)].append((int(bucket_end.timestamp()), float(price)))
    instruments: dict[str, np.ndarray] = {}
    coverage: dict[str, float] = {}
    decision_index = len(timeline) - CLOSE_CONVERGENCE_HORIZON_MINUTES - 1
    coverage_end = len(timeline) if complete else decision_index + 1
    for instrument_id in ("index:SPX", "future:ES"):
        observed = grouped[instrument_id]
        values = _close_forward_fill(
            np.asarray([row[0] for row in observed], dtype=np.int64),
            np.asarray([row[1] for row in observed], dtype=float),
            timeline,
        )
        instruments[instrument_id] = values
        coverage[instrument_id] = float(np.mean(np.isfinite(values[:coverage_end])))
    if not allow_partial and min(coverage.values()) < _CLOSE_MIN_COVERAGE:
        return None
    if complete and not np.isfinite(instruments["index:SPX"][-1]):
        return None
    if not allow_partial and not all(
        np.isfinite(instruments[instrument_id][decision_index])
        for instrument_id in ("index:SPX", "future:ES")
    ):
        return None
    return _CloseSessionPath(
        session_date=session_date,
        epoch_seconds=timeline,
        spx=instruments["index:SPX"],
        es=instruments["future:ES"],
        spx_coverage=coverage["index:SPX"],
        es_coverage=coverage["future:ES"],
    )


def _merge_close_current_state(
    base: _CloseSessionPath | None,
    *,
    data_root: Path,
    trading_date: date,
    available_at: datetime,
) -> _CloseSessionPath | None:
    """Overlay the live in-process samples on the last compacted Parquet prefix."""

    session = DEFAULT_MARKET_CALENDAR.session(trading_date)
    if session is None:
        return None
    if base is None:
        timeline = np.arange(
            int(session.open_at.timestamp()) + 60,
            int(session.close_at.timestamp()) + 1,
            60,
            dtype=np.int64,
        )
        spx = np.full(len(timeline), np.nan)
        es = np.full(len(timeline), np.nan)
    else:
        timeline = base.epoch_seconds
        spx = base.spx.copy()
        es = base.es.copy()
    state_path = data_root / "latest" / "market_feature_state.json"
    try:
        payload = json.loads(state_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        payload = {}
    samples = payload.get("market_samples") if isinstance(payload, Mapping) else None
    if isinstance(samples, list):
        for raw_sample in samples:
            sample = raw_sample if isinstance(raw_sample, Mapping) else {}
            received_at = _timestamp(sample.get("at"))
            if (
                received_at is None
                or received_at <= session.open_at.astimezone(timezone.utc)
                or received_at > available_at.astimezone(timezone.utc)
            ):
                continue
            bucket_end = (math.floor(received_at.timestamp() / 60.0) + 1) * 60
            position = int(np.searchsorted(timeline, bucket_end))
            if position >= len(timeline) or int(timeline[position]) != bucket_end:
                continue
            instruments = sample.get("instruments")
            spx_row = (
                instruments.get("index:SPX")
                if isinstance(instruments, Mapping)
                else None
            )
            es_by_provider = sample.get("es_by_provider")
            es_row = (
                es_by_provider.get("schwab")
                if isinstance(es_by_provider, Mapping)
                else None
            )
            spx_value = _close_live_state_price(spx_row, received_at=received_at)
            es_value = _close_live_state_price(es_row, received_at=received_at)
            if spx_value is not None and not np.isfinite(spx[position]):
                spx[position] = spx_value
            if es_value is not None and not np.isfinite(es[position]):
                es[position] = es_value
    decision_index = len(timeline) - CLOSE_CONVERGENCE_HORIZON_MINUTES - 1
    coverage_end = decision_index + 1
    spx_coverage = float(np.mean(np.isfinite(spx[:coverage_end])))
    es_coverage = float(np.mean(np.isfinite(es[:coverage_end])))
    if min(spx_coverage, es_coverage) < _CLOSE_MIN_COVERAGE:
        return None
    if not np.isfinite(spx[decision_index]) or not np.isfinite(es[decision_index]):
        return None
    return _CloseSessionPath(
        session_date=trading_date,
        epoch_seconds=timeline,
        spx=spx,
        es=es,
        spx_coverage=spx_coverage,
        es_coverage=es_coverage,
    )


def _close_live_state_price(
    value: object,
    *,
    received_at: datetime,
) -> float | None:
    row = value if isinstance(value, Mapping) else {}
    if row.get("provider") != "schwab" or row.get("quality") != "live":
        return None
    price = _finite(row.get("price"))
    source_at = _timestamp(row.get("source_at"))
    if price is None or price <= 0.0 or source_at is None:
        return None
    source_lag = (received_at - source_at).total_seconds()
    if not -5.0 <= source_lag <= 30.0:
        return None
    return price


def _close_forward_fill(
    observed_epoch: np.ndarray,
    observed_price: np.ndarray,
    timeline: np.ndarray,
) -> np.ndarray:
    output = np.full(len(timeline), np.nan)
    if len(observed_epoch) == 0:
        return output
    positions = np.searchsorted(observed_epoch, timeline, side="right") - 1
    valid = positions >= 0
    ages = np.full(len(timeline), np.inf)
    ages[valid] = timeline[valid] - observed_epoch[positions[valid]]
    fresh = valid & (ages <= 90)
    output[fresh] = observed_price[positions[fresh]]
    return output


def _close_online_pool_distribution(
    historical: list[_CloseSessionPath],
    current: _CloseSessionPath,
) -> tuple[np.ndarray, dict[str, float]]:
    index = len(current.spx) - CLOSE_CONVERGENCE_HORIZON_MINUTES - 1
    loss_history: dict[str, list[float]] = {
        "seasonal_student_t": [],
        "whole_path_analog": [],
        "functional_ridge": [],
    }
    for test_position in range(
        CLOSE_CONVERGENCE_MIN_TRAINING_SESSIONS,
        len(historical),
    ):
        test = historical[test_position]
        models = _close_candidate_path_models(
            historical[:test_position],
            test,
            index=index,
        )
        actual = float(test.spx[-1] - test.spx[index])
        for method, paths in models.items():
            loss_history[method].append(_close_crps(np.asarray(paths[:, -1], dtype=float), actual))
    weights = _close_online_weights(loss_history)
    models = _close_candidate_path_models(historical, current, index=index)
    seed = (
        _CLOSE_RNG_SEED
        + int(current.session_date.strftime("%Y%m%d"))
        + CLOSE_CONVERGENCE_HORIZON_MINUTES
        + 404
    )
    pooled = _close_pool_paths(models, weights, seed=seed)
    return float(current.spx[index]) + pooled[:, -1], weights


def _close_candidate_path_models(
    training: list[_CloseSessionPath],
    current: _CloseSessionPath,
    *,
    index: int,
) -> dict[str, np.ndarray]:
    train_x = np.asarray(
        [_close_feature_vector(path, index=index) for path in training],
        dtype=float,
    )
    current_x = _close_feature_vector(current, index=index)
    current_scale = _close_path_scale(current, index=index)
    seed = (
        _CLOSE_RNG_SEED
        + int(current.session_date.strftime("%Y%m%d"))
        + CLOSE_CONVERGENCE_HORIZON_MINUTES
    )
    return {
        "seasonal_student_t": _close_seasonal_paths(
            training,
            index=index,
            current_scale=current_scale,
            seed=seed + 101,
        ),
        "whole_path_analog": _close_analog_paths(
            training,
            train_x,
            current_x,
            index=index,
            current_scale=current_scale,
            seed=seed + 202,
        ),
        "functional_ridge": _close_functional_paths(
            training,
            current,
            index=index,
            current_scale=current_scale,
            seed=seed + 303,
        ),
    }


def _close_feature_vector(path: _CloseSessionPath, *, index: int) -> np.ndarray:
    spx = path.spx
    es = path.es
    observed = spx[: index + 1]
    observed = observed[np.isfinite(observed)]
    low = float(np.min(observed))
    high = float(np.max(observed))
    session_range = max(high - low, 1e-6)
    current = float(spx[index])
    # Keep this projection byte-for-byte equivalent to the frozen research
    # distance-feature order. Adding descriptive features here silently changes
    # the nearest-neighbour cohort and therefore requires a new model version.
    values = [
        _close_return(spx, index=index, minutes=5),
        _close_return(es, index=index, minutes=5),
        _close_return(spx, index=index, minutes=15),
        _close_realized_volatility(spx, index=index, minutes=15),
        _close_return(es, index=index, minutes=15),
        _close_return(spx, index=index, minutes=60),
        _close_realized_volatility(spx, index=index, minutes=60),
        _close_range(spx, index=index, minutes=60),
        _close_return(es, index=index, minutes=60),
        _close_return(es, index=index, minutes=15) - _close_return(spx, index=index, minutes=15),
        current - float(spx[0]),
        session_range,
        (current - low) / session_range - 0.5,
    ]
    return np.asarray(values, dtype=float)


def _close_return(values: np.ndarray, *, index: int, minutes: int) -> float:
    if index - minutes < 0:
        return math.nan
    current = values[index]
    previous = values[index - minutes]
    if not np.isfinite(current) or not np.isfinite(previous):
        return math.nan
    return float(current - previous)


def _close_realized_volatility(
    values: np.ndarray,
    *,
    index: int,
    minutes: int,
) -> float:
    observed = values[max(0, index - minutes) : index + 1]
    observed = observed[np.isfinite(observed)]
    if len(observed) < max(3, int(minutes * 0.8)):
        return math.nan
    return float(np.sqrt(np.sum(np.diff(observed) ** 2)))


def _close_range(values: np.ndarray, *, index: int, minutes: int) -> float:
    observed = values[max(0, index - minutes) : index + 1]
    observed = observed[np.isfinite(observed)]
    return float(np.max(observed) - np.min(observed)) if len(observed) else math.nan


def _close_finite_curve(values: np.ndarray) -> np.ndarray:
    observed = np.flatnonzero(np.isfinite(values))
    if len(observed) < 2:
        raise RuntimeError("close convergence path has fewer than two observations")
    return np.interp(np.arange(len(values)), observed, values[observed])


def _close_path_scale(path: _CloseSessionPath, *, index: int) -> float:
    curve = _close_finite_curve(path.spx[: index + 1])
    increments = np.diff(curve[max(0, len(curve) - 16) :])
    scale = float(np.sqrt(np.sum(increments**2)) / math.sqrt(max(len(increments), 1)))
    return max(scale, 0.05)


def _close_future_path(path: _CloseSessionPath, *, index: int) -> np.ndarray:
    segment = _close_finite_curve(path.spx[index : index + CLOSE_CONVERGENCE_HORIZON_MINUTES + 1])
    if len(segment) != CLOSE_CONVERGENCE_HORIZON_MINUTES + 1:
        raise RuntimeError("close convergence future path length mismatch")
    return segment[1:] - segment[0]


def _close_neighbor_weights(
    train_x: np.ndarray,
    current_x: np.ndarray,
) -> tuple[np.ndarray, np.ndarray]:
    medians = np.nanmedian(train_x, axis=0)
    q25 = np.nanquantile(train_x, 0.25, axis=0)
    q75 = np.nanquantile(train_x, 0.75, axis=0)
    scale = np.where(q75 - q25 > 1e-6, q75 - q25, 1.0)
    normalized_train = (np.where(np.isfinite(train_x), train_x, medians) - medians) / scale
    normalized_current = (np.where(np.isfinite(current_x), current_x, medians) - medians) / scale
    distances = np.sqrt(np.mean((normalized_train - normalized_current) ** 2, axis=1))
    neighbor_count = min(
        len(train_x),
        max(8, min(15, len(train_x) // 2 + 2)),
    )
    selected = np.argsort(distances)[:neighbor_count]
    selected_distance = distances[selected]
    temperature = max(float(np.median(selected_distance)), 0.25)
    weights = np.exp(-selected_distance / temperature)
    weights /= np.sum(weights)
    return selected, weights


def _close_analog_paths(
    training: list[_CloseSessionPath],
    train_x: np.ndarray,
    current_x: np.ndarray,
    *,
    index: int,
    current_scale: float,
    seed: int,
) -> np.ndarray:
    selected, weights = _close_neighbor_weights(train_x, current_x)
    normalized = np.asarray(
        [
            _close_future_path(training[position], index=index)
            / _close_path_scale(training[position], index=index)
            for position in selected
        ],
        dtype=float,
    )
    rng = np.random.default_rng(seed)
    sampled = rng.choice(
        len(selected),
        size=CLOSE_CONVERGENCE_MONTE_CARLO_DRAWS,
        replace=True,
        p=weights,
    )
    return normalized[sampled] * current_scale


def _close_seasonal_paths(
    training: list[_CloseSessionPath],
    *,
    index: int,
    current_scale: float,
    seed: int,
) -> np.ndarray:
    increments = np.asarray(
        [np.diff(_close_finite_curve(path.spx)) for path in training],
        dtype=float,
    )
    seasonal_scale = 1.4826 * np.median(np.abs(increments), axis=0)
    positive = seasonal_scale[seasonal_scale > 1e-6]
    fallback = float(np.median(positive)) if len(positive) else current_scale
    seasonal_scale = np.where(seasonal_scale > 1e-6, seasonal_scale, fallback)
    recent_start = max(0, index - 15)
    recent = float(np.sqrt(np.mean(seasonal_scale[recent_start:index] ** 2)))
    multiplier = float(np.clip(current_scale / max(recent, 0.05), 0.5, 2.0))
    future_scale = seasonal_scale[index : index + CLOSE_CONVERGENCE_HORIZON_MINUTES] * multiplier
    rng = np.random.default_rng(seed)
    standardized = rng.standard_t(
        _CLOSE_STUDENT_T_DEGREES_OF_FREEDOM,
        size=(
            CLOSE_CONVERGENCE_MONTE_CARLO_DRAWS,
            CLOSE_CONVERGENCE_HORIZON_MINUTES,
        ),
    ) * math.sqrt((_CLOSE_STUDENT_T_DEGREES_OF_FREEDOM - 2.0) / _CLOSE_STUDENT_T_DEGREES_OF_FREEDOM)
    return np.cumsum(standardized * future_scale, axis=1)


def _close_functional_paths(
    training: list[_CloseSessionPath],
    current: _CloseSessionPath,
    *,
    index: int,
    current_scale: float,
    seed: int,
) -> np.ndarray:
    prefix_positions = np.unique(np.append(np.arange(0, index + 1, 15), index))
    prefixes: list[np.ndarray] = []
    futures: list[np.ndarray] = []
    for path in training:
        prefix = _close_finite_curve(path.spx[: index + 1])
        scale = _close_path_scale(path, index=index)
        prefixes.append((prefix[prefix_positions] - prefix[0]) / scale)
        futures.append(_close_future_path(path, index=index) / scale)
    prefix_matrix = np.asarray(prefixes, dtype=float)
    future_matrix = np.asarray(futures, dtype=float)
    prefix_center = np.mean(prefix_matrix, axis=0)
    centered = prefix_matrix - prefix_center
    _left, _singular, right = np.linalg.svd(centered, full_matrices=False)
    rank = max(
        1,
        min(
            _CLOSE_FUNCTIONAL_RANK,
            len(training) - 2,
            right.shape[0],
        ),
    )
    basis = right[:rank].T
    training_scores = centered @ basis
    current_prefix = _close_finite_curve(current.spx[: index + 1])
    current_normalized = (current_prefix[prefix_positions] - current_prefix[0]) / current_scale
    current_scores = (current_normalized - prefix_center) @ basis
    model = Ridge(alpha=_CLOSE_FUNCTIONAL_RIDGE_ALPHA)
    model.fit(training_scores, future_matrix)
    predicted_path = model.predict(current_scores.reshape(1, -1))[0]
    residual_paths = future_matrix - model.predict(training_scores)
    degrees_of_freedom = max(len(training) - rank - 1, 1)
    residual_paths *= math.sqrt(len(training) / degrees_of_freedom)
    rng = np.random.default_rng(seed)
    sampled = rng.choice(
        len(residual_paths),
        size=CLOSE_CONVERGENCE_MONTE_CARLO_DRAWS,
        replace=True,
    )
    return (predicted_path + residual_paths[sampled]) * current_scale


def _close_crps(draws: np.ndarray, actual: float) -> float:
    ordered = np.sort(draws)
    count = len(ordered)
    coefficients = 2 * np.arange(1, count + 1) - count - 1
    half_pairwise = float(np.sum(coefficients * ordered) / (count * count))
    return float(np.mean(np.abs(draws - actual)) - half_pairwise)


def _close_online_weights(
    loss_history: Mapping[str, list[float]],
) -> dict[str, float]:
    methods = (
        "seasonal_student_t",
        "whole_path_analog",
        "functional_ridge",
    )
    histories = [loss_history.get(method, ()) for method in methods]
    if min((len(values) for values in histories), default=0) < 3:
        return {method: 1.0 / len(methods) for method in methods}
    losses = np.asarray([float(np.mean(values)) for values in histories])
    temperature = max(float(np.median(losses)), 1.0)
    raw = np.exp(-(losses - np.min(losses)) / temperature)
    raw /= np.sum(raw)
    shrunk = (1.0 - _CLOSE_ONLINE_POOL_SHRINKAGE) * raw + _CLOSE_ONLINE_POOL_SHRINKAGE / len(
        methods
    )
    return {method: float(weight) for method, weight in zip(methods, shrunk, strict=True)}


def _close_pool_paths(
    paths_by_method: Mapping[str, np.ndarray],
    weights: Mapping[str, float],
    *,
    seed: int,
) -> np.ndarray:
    methods = tuple(weights)
    probabilities = np.asarray([weights[method] for method in methods])
    probabilities /= np.sum(probabilities)
    rng = np.random.default_rng(seed)
    chosen = rng.choice(
        len(methods),
        size=CLOSE_CONVERGENCE_MONTE_CARLO_DRAWS,
        p=probabilities,
    )
    output = np.empty_like(next(iter(paths_by_method.values())))
    for method_index, method in enumerate(methods):
        positions = np.flatnonzero(chosen == method_index)
        if not len(positions):
            continue
        source = paths_by_method[method]
        sampled = rng.choice(len(source), size=len(positions), replace=True)
        output[positions] = source[sampled]
    return output


def _close_modal_center(
    draws: np.ndarray,
    *,
    q10: float,
    median: float,
    q90: float,
) -> tuple[float, float]:
    lower = 5.0 * math.floor((q10 - 2.5) / 5.0)
    upper = 5.0 * math.ceil((q90 + 2.5) / 5.0)
    centers = np.arange(lower, upper + 5.0, 5.0)
    probabilities = np.asarray([np.mean(np.abs(draws - center) <= 2.5) for center in centers])
    selected = min(
        range(len(centers)),
        key=lambda index: (
            -float(probabilities[index]),
            abs(float(centers[index]) - median),
        ),
    )
    return float(centers[selected]), float(probabilities[selected])




def _timestamp(value: object) -> datetime | None:
    if not isinstance(value, str):
        return None
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        return None
    return parsed.astimezone(timezone.utc)


def _finite(value: object) -> float | None:
    if isinstance(value, bool) or not isinstance(value, int | float):
        return None
    parsed = float(value)
    return parsed if math.isfinite(parsed) else None


__all__ = [
    "CLOSE_CONVERGENCE_FEATURE_VERSION",
    "CLOSE_CONVERGENCE_HORIZON_MINUTES",
    "CLOSE_CONVERGENCE_MODEL_VERSION",
    "PhysicalCloseConvergenceEstimate",
    "estimate_physical_close_convergence",
]
