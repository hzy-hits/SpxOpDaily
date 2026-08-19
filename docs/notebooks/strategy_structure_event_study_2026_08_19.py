"""Causal RTH event study for raw SPX path plus option structure.

This research artifact is intentionally independent of production strategy
decisions and policy versions.  It reads only point-in-time market/structure
data and evaluates session-held-out labels.
"""

from __future__ import annotations

import bisect
import json
import math
from collections import defaultdict
from datetime import date, datetime, time, timedelta, timezone
from pathlib import Path
from typing import Any, Iterable, Mapping, Sequence
from zoneinfo import ZoneInfo

import duckdb
import nbformat
import numpy as np
from nbclient import NotebookClient
from sklearn.impute import SimpleImputer
from sklearn.cluster import KMeans
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import adjusted_rand_score, brier_score_loss, log_loss, roc_auc_score
from sklearn.mixture import GaussianMixture
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import StandardScaler


REPO_ROOT = next(
    path
    for path in (Path.cwd(), *Path.cwd().parents)
    if (path / "src/spx_spark").is_dir()
)
DATA_ROOT = Path("/srv/data/spx-spark/data")
QUOTE_ROOT = DATA_ROOT / "lake/quotes/schema=v1"
SURFACE_ROOT = DATA_ROOT / "features/iv_surface"
GREEK_ROOT = DATA_ROOT / "features/spxw_0dte_greeks_reference"
START = date(2026, 7, 13)
END = date(2026, 8, 18)
ET = ZoneInfo("America/New_York")
UTC = timezone.utc
MIN_TRAIN_SESSIONS = 10
SOURCE_PATH = Path(__file__).resolve()
NOTEBOOK_PATH = SOURCE_PATH.with_name("strategy-structure-event-study-2026-08-19.ipynb")
ARTIFACT_PATH = REPO_ROOT / "docs/research/strategy-structure-event-study-2026-08-19.json"

PATH_FEATURES = (
    "return_1m_scale",
    "return_5m_scale",
    "return_15m_scale",
    "range_15m_scale",
    "range_position_15m",
)
STRUCTURE_FEATURES = (
    "zero_gamma_distance_scale",
    "call_wall_distance_scale",
    "put_wall_distance_scale",
    "expected_move_scale",
    "atm_iv",
    "atm_iv_jump_5m",
    "put_skew_25d",
    "put_skew_change_5m",
    "call_skew_25d",
    "smile_curvature",
    "net_gamma_ratio_proxy",
    "vix1d_vix_ratio",
    "vix1d_change_5m",
)
QUALITY_FEATURES = ("greek_usable_ratio",)
CHANGE_FEATURES = (
    "cusum_positive",
    "cusum_negative",
    "cusum_imbalance",
    "bocpd_last_probability",
    "bocpd_peak_1m_probability",
)
WALL_HAZARD_FEATURES = (
    "call_wall_distance_scale",
    "put_wall_distance_scale",
    "zero_gamma_distance_scale",
    "expected_move_scale",
)
MOE_GATE_FEATURES = (
    "return_15m_scale",
    "call_wall_distance_scale",
    "put_wall_distance_scale",
)
TARGETS = (
    "direction_up_5m",
    "breakout_followthrough_15m",
    "reversal_15m",
    "pullback_resume_15m",
)
TARGET_ELIGIBILITY = {
    "direction_up_5m": None,
    "breakout_followthrough_15m": "breakout_eligible",
    "reversal_15m": "impulse_eligible",
    "pullback_resume_15m": "impulse_eligible",
}


def parse_at(value: object) -> datetime | None:
    if not isinstance(value, str):
        return None
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        return None
    return parsed.astimezone(UTC)


def finite(value: object) -> float | None:
    if isinstance(value, bool) or not isinstance(value, int | float):
        return None
    result = float(value)
    return result if math.isfinite(result) else None


def read_jsonl(path: Path) -> Iterable[dict[str, Any]]:
    with path.open(encoding="utf-8") as handle:
        for line in handle:
            if not line.strip():
                continue
            row = json.loads(line)
            if isinstance(row, dict):
                yield row


def load_surfaces() -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for path in sorted(SURFACE_ROOT.glob("date=*/hour=*/snapshots.jsonl")):
        day = date.fromisoformat(path.parts[-3].split("=", 1)[1])
        if not START <= day <= END or day.weekday() >= 5:
            continue
        expiry = day.strftime("%Y%m%d")
        for document in read_jsonl(path):
            as_of = parse_at(document.get("as_of"))
            created_at = parse_at(document.get("created_at")) or as_of
            if as_of is None or created_at is None or as_of > created_at:
                continue
            local = created_at.astimezone(ET)
            if not time(9, 35) <= local.timetz().replace(tzinfo=None) <= time(15, 30):
                continue
            front = next(
                (
                    item
                    for item in document.get("expiries") or ()
                    if isinstance(item, Mapping) and str(item.get("expiry")) == expiry
                ),
                None,
            )
            if not isinstance(front, Mapping):
                continue
            rows.append(
                {
                    "session_date": day,
                    "decision_at": created_at,
                    "surface_as_of": as_of,
                    "surface": dict(front),
                }
            )
    rows.sort(key=lambda item: item["decision_at"])
    return rows


def load_greek_snapshots() -> dict[date, tuple[list[float], list[dict[str, Any]]]]:
    by_day: dict[date, list[dict[str, Any]]] = defaultdict(list)
    for path in sorted(GREEK_ROOT.glob("date=*/snapshots.jsonl")):
        day = date.fromisoformat(path.parent.name.split("=", 1)[1])
        if not START <= day <= END or day.weekday() >= 5:
            continue
        expiry = day.strftime("%Y%m%d")
        for document in read_jsonl(path):
            as_of = parse_at(document.get("as_of"))
            proxy = document.get("signed_gex_proxy")
            coverage = document.get("coverage")
            if (
                as_of is None
                or str(document.get("expiry")) != expiry
                or not isinstance(proxy, Mapping)
                or not isinstance(coverage, Mapping)
            ):
                continue
            by_day[day].append(
                {
                    "as_of": as_of,
                    "net_gamma_ratio_proxy": finite(proxy.get("net_gamma_ratio")),
                    "greek_usable_ratio": finite(coverage.get("usable_ratio")),
                    "proxy_quality": proxy.get("quality"),
                }
            )
    result = {}
    for day, rows in by_day.items():
        rows.sort(key=lambda item: item["as_of"])
        result[day] = ([item["as_of"].timestamp() for item in rows], rows)
    return result


def load_market_minutes() -> dict[tuple[date, str], tuple[list[float], list[float]]]:
    glob = str(QUOTE_ROOT / "date=*" / "provider=schwab" / "hour=*" / "quotes.parquet")
    query = """
    WITH filtered AS (
      SELECT
        CAST(received_at AT TIME ZONE 'America/New_York' AS DATE) AS session_date,
        date_trunc('minute', received_at) AS minute_utc,
        instrument_id,
        received_at,
        effective_price
      FROM read_parquet(?, hive_partitioning=true, union_by_name=true)
      WHERE date BETWEEN ? AND ?
        AND provider = 'schwab'
        AND quality = 'live'
        AND effective_price > 0
        AND instrument_id IN ('index:SPX', 'index:VIX', 'index:VIX1D')
        AND CAST(received_at AT TIME ZONE 'America/New_York' AS TIME)
              BETWEEN TIME '09:15:00' AND TIME '16:00:00'
    )
    SELECT
      session_date,
      instrument_id,
      epoch(max(received_at)) AS available_epoch,
      arg_max(effective_price, received_at) AS price
    FROM filtered
    GROUP BY 1, 2, minute_utc
    ORDER BY 1, 2, available_epoch
    """
    connection = duckdb.connect()
    try:
        raw = connection.execute(query, [glob, START, END]).fetchall()
    finally:
        connection.close()
    grouped: dict[tuple[date, str], tuple[list[float], list[float]]] = {}
    staging: dict[tuple[date, str], list[tuple[float, float]]] = defaultdict(list)
    for day, instrument, epoch, price in raw:
        staging[(day, instrument)].append((float(epoch), float(price)))
    for key, values in staging.items():
        values.sort()
        grouped[key] = ([row[0] for row in values], [row[1] for row in values])
    return grouped


def load_spx_five_seconds() -> dict[date, tuple[list[float], list[float]]]:
    """Load a causal 5-second SPX path without carrying quotes across sessions."""
    glob = str(QUOTE_ROOT / "date=*" / "provider=schwab" / "hour=*" / "quotes.parquet")
    query = """
    WITH filtered AS (
      SELECT
        CAST(received_at AT TIME ZONE 'America/New_York' AS DATE) AS session_date,
        floor(epoch(received_at) / 5.0) AS bucket,
        received_at,
        effective_price
      FROM read_parquet(?, hive_partitioning=true, union_by_name=true)
      WHERE date BETWEEN ? AND ?
        AND provider = 'schwab'
        AND quality = 'live'
        AND effective_price > 0
        AND instrument_id = 'index:SPX'
        AND CAST(received_at AT TIME ZONE 'America/New_York' AS TIME)
              BETWEEN TIME '09:15:00' AND TIME '16:00:00'
    )
    SELECT
      session_date,
      epoch(max(received_at)) AS available_epoch,
      arg_max(effective_price, received_at) AS price
    FROM filtered
    GROUP BY 1, bucket
    ORDER BY 1, available_epoch
    """
    connection = duckdb.connect()
    try:
        raw = connection.execute(query, [glob, START, END]).fetchall()
    finally:
        connection.close()
    staging: dict[date, list[tuple[float, float]]] = defaultdict(list)
    for day, epoch, price in raw:
        staging[day].append((float(epoch), float(price)))
    return {
        day: (
            [row[0] for row in sorted(values)],
            [row[1] for row in sorted(values)],
        )
        for day, values in staging.items()
    }


def asof_value(
    series: tuple[Sequence[float], Sequence[float]] | None,
    at: datetime,
    *,
    max_age_seconds: float = 90.0,
) -> float | None:
    if series is None:
        return None
    epochs, values = series
    target = at.timestamp()
    index = bisect.bisect_right(epochs, target) - 1
    if index < 0 or target - epochs[index] > max_age_seconds:
        return None
    return float(values[index])


def path_values(
    series: tuple[Sequence[float], Sequence[float]] | None,
    start: datetime,
    end: datetime,
) -> list[float]:
    if series is None:
        return []
    epochs, values = series
    left = bisect.bisect_right(epochs, start.timestamp())
    right = bisect.bisect_right(epochs, end.timestamp())
    return [float(value) for value in values[left:right]]


def causal_grid(
    series: tuple[Sequence[float], Sequence[float]] | None,
    at: datetime,
    *,
    lookback_seconds: int = 900,
    step_seconds: int = 5,
    max_age_seconds: float = 20.0,
) -> np.ndarray | None:
    if series is None:
        return None
    epochs, values = series
    grid = []
    for offset in range(lookback_seconds, -1, -step_seconds):
        target = at.timestamp() - offset
        index = bisect.bisect_right(epochs, target) - 1
        if index < 0 or target - epochs[index] > max_age_seconds:
            return None
        grid.append(float(values[index]))
    return np.asarray(grid, dtype=float)


def robust_standardize(values: np.ndarray) -> np.ndarray:
    centered = values - float(np.median(values))
    scale = 1.4826 * float(np.median(np.abs(centered)))
    if not math.isfinite(scale) or scale < 1e-6:
        scale = max(float(np.std(values)), 1e-6)
    return centered / scale


def bocpd_probabilities(
    standardized_increments: np.ndarray,
    *,
    hazard: float = 1 / 60,
    max_run_length: int = 60,
) -> np.ndarray:
    """Small known-variance BOCPD filter; all observations are historical."""
    run_probability = np.asarray([1.0])
    means = np.asarray([0.0])
    precisions = np.asarray([1.0])
    change_probabilities = []
    root_two_pi = math.sqrt(2 * math.pi)
    for observation in standardized_increments:
        predictive_variance = 1.0 + 1.0 / precisions
        predictive = np.exp(
            -0.5 * (observation - means) ** 2 / predictive_variance
        ) / (root_two_pi * np.sqrt(predictive_variance))
        prior_predictive = math.exp(-0.25 * observation**2) / (root_two_pi * math.sqrt(2.0))
        changepoint = hazard * prior_predictive
        growth = run_probability * predictive * (1.0 - hazard)
        updated_probability = np.concatenate(([changepoint], growth))[: max_run_length + 1]
        normalizer = float(np.sum(updated_probability))
        if not math.isfinite(normalizer) or normalizer <= 0:
            updated_probability = np.zeros_like(updated_probability)
            updated_probability[0] = 1.0
        else:
            updated_probability /= normalizer
        prior_precision = 1.0
        updated_means = np.concatenate(
            (
                np.asarray([(prior_precision * 0.0 + observation) / (prior_precision + 1.0)]),
                (precisions * means + observation) / (precisions + 1.0),
            )
        )[: max_run_length + 1]
        updated_precisions = np.concatenate(
            (np.asarray([prior_precision + 1.0]), precisions + 1.0)
        )[: max_run_length + 1]
        run_probability = updated_probability
        means = updated_means
        precisions = updated_precisions
        change_probabilities.append(float(updated_probability[0]))
    return np.asarray(change_probabilities, dtype=float)


def path_shape_and_change_features(prices: np.ndarray) -> dict[str, Any]:
    increments = np.diff(prices)
    standardized = robust_standardize(increments)
    positive = 0.0
    negative = 0.0
    allowance = 0.25
    for value in standardized:
        positive = max(0.0, positive + float(value) - allowance)
        negative = max(0.0, negative - float(value) - allowance)
    denominator = math.sqrt(max(len(standardized), 1))
    change_probability = bocpd_probabilities(standardized)
    realized = math.sqrt(float(np.sum(increments**2)))
    motif_indices = np.linspace(0, len(prices) - 1, 37).round().astype(int)
    motif = (prices[motif_indices] - prices[0]) / max(realized, 1e-6)
    return {
        "cusum_positive": positive / denominator,
        "cusum_negative": negative / denominator,
        "cusum_imbalance": (positive - negative) / denominator,
        "bocpd_last_probability": float(change_probability[-1]),
        "bocpd_peak_1m_probability": float(np.max(change_probability[-12:])),
        "motif_window": motif,
    }


def nearest_greek(
    store: Mapping[date, tuple[list[float], list[dict[str, Any]]]],
    day: date,
    at: datetime,
) -> dict[str, Any]:
    series = store.get(day)
    if series is None:
        return {}
    epochs, rows = series
    index = bisect.bisect_right(epochs, at.timestamp()) - 1
    if index < 0 or at.timestamp() - epochs[index] > 90.0:
        return {}
    return rows[index]


def realized_scale(values: Sequence[float]) -> float | None:
    if len(values) < 10:
        return None
    realized = math.sqrt(math.fsum((right - left) ** 2 for left, right in zip(values, values[1:])))
    return max(2.5, 1.25 * realized)


def feature_rows() -> tuple[list[dict[str, Any]], dict[str, Any]]:
    surfaces = load_surfaces()
    greeks = load_greek_snapshots()
    market = load_market_minutes()
    five_seconds = load_spx_five_seconds()
    result: list[dict[str, Any]] = []
    skipped = defaultdict(int)
    for item in surfaces:
        day = item["session_date"]
        at = item["decision_at"]
        front = item["surface"]
        spx_series = market.get((day, "index:SPX"))
        spot = asof_value(spx_series, at)
        p1 = asof_value(spx_series, at - timedelta(minutes=1))
        p5 = asof_value(spx_series, at - timedelta(minutes=5))
        p15 = asof_value(spx_series, at - timedelta(minutes=15))
        f5 = asof_value(spx_series, at + timedelta(minutes=5))
        f15 = asof_value(spx_series, at + timedelta(minutes=15))
        history = path_values(spx_series, at - timedelta(minutes=15), at)
        future5 = path_values(spx_series, at, at + timedelta(minutes=5))
        future15 = path_values(spx_series, at, at + timedelta(minutes=15))
        scale = realized_scale(history)
        if None in (spot, p1, p5, p15, f5, f15, scale) or not future5 or not future15:
            skipped["path_incomplete"] += 1
            continue
        assert spot is not None and p1 is not None and p5 is not None and p15 is not None
        assert f5 is not None and f15 is not None and scale is not None
        high15, low15 = max(future15), min(future15)
        prior_direction = 1.0 if spot > p15 else -1.0 if spot < p15 else 0.0
        local_high, local_low = max(history), min(history)
        local_range = max(local_high - local_low, 1e-9)
        call_wall = finite(front.get("call_wall"))
        put_wall = finite(front.get("put_wall"))
        zero_gamma = finite(front.get("zero_gamma"))
        upper_levels = [value for value in (call_wall, zero_gamma) if value is not None and value > spot]
        lower_levels = [value for value in (put_wall, zero_gamma) if value is not None and value < spot]
        upper = min(upper_levels) if upper_levels else None
        lower = max(lower_levels) if lower_levels else None
        up_break = upper is not None and high15 >= upper and f15 >= upper + 0.10 * scale
        down_break = lower is not None and low15 <= lower and f15 <= lower - 0.10 * scale
        barrier_outcome = (
            1
            if up_break and not down_break
            else -1
            if down_break and not up_break
            else int(math.copysign(1, f15 - spot))
            if up_break and down_break and f15 != spot
            else 0
        )
        adverse5 = (
            spot - min(future5)
            if prior_direction > 0
            else max(future5) - spot
            if prior_direction < 0
            else 0.0
        )
        greek = nearest_greek(greeks, day, at)
        vix = asof_value(market.get((day, "index:VIX")), at, max_age_seconds=600.0)
        vix1d = asof_value(market.get((day, "index:VIX1D")), at, max_age_seconds=600.0)
        vix1d_5 = asof_value(
            market.get((day, "index:VIX1D")),
            at - timedelta(minutes=5),
            max_age_seconds=600.0,
        )
        five_second_path = causal_grid(five_seconds.get(day), at)
        change_features = (
            path_shape_and_change_features(five_second_path)
            if five_second_path is not None
            else {
                **{name: None for name in CHANGE_FEATURES},
                "motif_window": None,
            }
        )
        if five_second_path is None:
            skipped["five_second_path_incomplete"] += 1
        row = {
            "session_date": day,
            "decision_at": at,
            "surface_as_of": item["surface_as_of"],
            "return_1m_scale": (spot - p1) / scale,
            "return_5m_scale": (spot - p5) / scale,
            "return_15m_scale": (spot - p15) / scale,
            "range_15m_scale": local_range / scale,
            "range_position_15m": (spot - local_low) / local_range,
            "zero_gamma_distance_scale": (zero_gamma - spot) / scale if zero_gamma is not None else None,
            "call_wall_distance_scale": (call_wall - spot) / scale if call_wall is not None else None,
            "put_wall_distance_scale": (spot - put_wall) / scale if put_wall is not None else None,
            "expected_move_scale": (finite(front.get("expected_move_points")) or math.nan) / scale,
            "atm_iv": finite(front.get("atm_iv")),
            "atm_iv_jump_5m": finite(front.get("atm_iv_jump_5m")),
            "put_skew_25d": finite(front.get("put_skew_25d")),
            "put_skew_change_5m": finite(front.get("put_skew_25d_change_5m")),
            "call_skew_25d": finite(front.get("call_skew_25d")),
            "smile_curvature": finite(front.get("smile_curvature")),
            "net_gamma_ratio_proxy": greek.get("net_gamma_ratio_proxy"),
            "greek_usable_ratio": greek.get("greek_usable_ratio"),
            "vix1d_vix_ratio": vix1d / vix if vix1d and vix else None,
            "vix1d_change_5m": vix1d - vix1d_5 if vix1d is not None and vix1d_5 is not None else None,
            "direction_up_5m": int(f5 > spot),
            "breakout_eligible": int(upper is not None or lower is not None),
            "impulse_eligible": int(abs(spot - p15) >= 0.50 * scale),
            "breakout_followthrough_15m": int(up_break or down_break),
            "barrier_outcome_15m": barrier_outcome,
            "up_breakout_followthrough_15m": int(up_break),
            "down_breakout_followthrough_15m": int(down_break),
            "reversal_15m": int(
                prior_direction != 0
                and prior_direction * (f15 - spot) <= -0.50 * scale
                and abs(spot - p15) >= 0.50 * scale
            ),
            "pullback_resume_15m": int(
                prior_direction != 0
                and abs(spot - p15) >= 0.50 * scale
                and 0.25 * scale <= adverse5 <= 0.80 * scale
                and prior_direction * (f15 - spot) >= 0.25 * scale
            ),
            **change_features,
        }
        result.append(row)
    sessions = sorted({row["session_date"] for row in result})
    quality = {
        "surface_rows": len(surfaces),
        "model_rows": len(result),
        "sessions": len(sessions),
        "first_session": sessions[0].isoformat() if sessions else None,
        "last_session": sessions[-1].isoformat() if sessions else None,
        "skipped": dict(skipped),
    }
    return result, quality


def available_features(rows: Sequence[Mapping[str, Any]], names: Sequence[str]) -> tuple[str, ...]:
    return tuple(
        name
        for name in names
        if sum(finite(row.get(name)) is not None for row in rows) / max(len(rows), 1) >= 0.50
    )


def matrix(rows: Sequence[Mapping[str, Any]], names: Sequence[str]) -> np.ndarray:
    return np.asarray(
        [[finite(row.get(name)) if finite(row.get(name)) is not None else np.nan for name in names] for row in rows],
        dtype=float,
    )


def safe_auc(y: np.ndarray, probability: np.ndarray) -> float | None:
    return float(roc_auc_score(y, probability)) if len(set(y.tolist())) > 1 else None


def walk_forward(
    rows: Sequence[Mapping[str, Any]],
    *,
    target: str,
    feature_sets: Mapping[str, Sequence[str]],
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    sessions = sorted({row["session_date"] for row in rows})
    predictions: list[dict[str, Any]] = []
    for test_day in sessions[MIN_TRAIN_SESSIONS:]:
        train = [row for row in rows if row["session_date"] < test_day]
        test = [row for row in rows if row["session_date"] == test_day]
        y_train = np.asarray([int(row[target]) for row in train], dtype=int)
        if not test or len(set(y_train.tolist())) < 2:
            continue
        baseline = float(np.mean(y_train))
        y_test = np.asarray([int(row[target]) for row in test], dtype=int)
        for model_name, names in feature_sets.items():
            if model_name == "intercept":
                probabilities = np.full(len(test), baseline, dtype=float)
            else:
                pipeline = Pipeline(
                    [
                        ("impute", SimpleImputer(strategy="median")),
                        ("scale", StandardScaler()),
                        ("model", LogisticRegression(C=0.10, max_iter=2_000)),
                    ]
                )
                pipeline.fit(matrix(train, names), y_train)
                probabilities = pipeline.predict_proba(matrix(test, names))[:, 1]
            for row, actual, probability in zip(test, y_test, probabilities, strict=True):
                predictions.append(
                    {
                        "session_date": test_day,
                        "target": target,
                        "model": model_name,
                        "actual": int(actual),
                        "probability": float(np.clip(probability, 1e-6, 1 - 1e-6)),
                    }
                )
    metrics = []
    for model_name in feature_sets:
        selected = [row for row in predictions if row["model"] == model_name]
        if not selected:
            continue
        y = np.asarray([row["actual"] for row in selected], dtype=int)
        probability = np.asarray([row["probability"] for row in selected], dtype=float)
        metrics.append(
            {
                "target": target,
                "model": model_name,
                "n": len(selected),
                "sessions": len({row["session_date"] for row in selected}),
                "base_rate": float(np.mean(y)),
                "brier": float(brier_score_loss(y, probability)),
                "log_loss": float(log_loss(y, probability, labels=[0, 1])),
                "auc": safe_auc(y, probability),
            }
        )
    return metrics, predictions


def clustered_delta(
    predictions: Sequence[Mapping[str, Any]],
    *,
    left: str = "path+structure",
    right: str = "path",
    replications: int = 2_000,
) -> dict[str, Any]:
    # Preserve within-session row order for paired model errors.
    grouped: dict[date, dict[str, list[Mapping[str, Any]]]] = defaultdict(lambda: defaultdict(list))
    for row in predictions:
        grouped[row["session_date"]][str(row["model"])].append(row)
    session_deltas = []
    for day, models in grouped.items():
        left_rows = models.get(left, [])
        right_rows = models.get(right, [])
        if len(left_rows) != len(right_rows) or not left_rows:
            continue
        left_error = np.mean([(row["probability"] - row["actual"]) ** 2 for row in left_rows])
        right_error = np.mean([(row["probability"] - row["actual"]) ** 2 for row in right_rows])
        session_deltas.append((day, float(left_error - right_error)))
    if not session_deltas:
        return {"delta_brier": None, "ci95": [None, None], "sessions": 0}
    values = np.asarray([row[1] for row in session_deltas], dtype=float)
    rng = np.random.default_rng(20260819)
    draws = np.asarray(
        [np.mean(rng.choice(values, size=len(values), replace=True)) for _ in range(replications)]
    )
    return {
        "delta_brier": float(np.mean(values)),
        "ci95": [float(np.quantile(draws, 0.025)), float(np.quantile(draws, 0.975))],
        "sessions": len(values),
    }


def multiclass_brier(actual: np.ndarray, probabilities: np.ndarray) -> float:
    classes = (-1, 0, 1)
    encoded = np.asarray([[int(value == label) for label in classes] for value in actual])
    return float(np.mean(np.sum((probabilities - encoded) ** 2, axis=1)))


def competing_risk_walk_forward(
    rows: Sequence[Mapping[str, Any]],
    feature_sets: Mapping[str, Sequence[str]],
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    classes = np.asarray([-1, 0, 1], dtype=int)
    sessions = sorted({row["session_date"] for row in rows})
    predictions: list[dict[str, Any]] = []
    for test_day in sessions[MIN_TRAIN_SESSIONS:]:
        train = [row for row in rows if row["session_date"] < test_day]
        test = [row for row in rows if row["session_date"] == test_day]
        y_train = np.asarray([int(row["barrier_outcome_15m"]) for row in train], dtype=int)
        if not test or len(set(y_train.tolist())) < 2:
            continue
        y_test = np.asarray([int(row["barrier_outcome_15m"]) for row in test], dtype=int)
        counts = np.asarray([np.sum(y_train == label) + 1 for label in classes], dtype=float)
        priors = counts / np.sum(counts)
        for model_name, names in feature_sets.items():
            if model_name == "intercept":
                probabilities = np.tile(priors, (len(test), 1))
            else:
                pipeline = Pipeline(
                    [
                        ("impute", SimpleImputer(strategy="median")),
                        ("scale", StandardScaler()),
                        ("model", LogisticRegression(C=0.10, max_iter=2_000)),
                    ]
                )
                pipeline.fit(matrix(train, names), y_train)
                fitted = pipeline.predict_proba(matrix(test, names))
                probabilities = np.full((len(test), len(classes)), 1e-6)
                fitted_classes = pipeline.named_steps["model"].classes_
                for source_index, label in enumerate(fitted_classes):
                    target_index = int(np.flatnonzero(classes == label)[0])
                    probabilities[:, target_index] = fitted[:, source_index]
                probabilities /= np.sum(probabilities, axis=1, keepdims=True)
            for row, actual, probability in zip(test, y_test, probabilities, strict=True):
                predictions.append(
                    {
                        "session_date": test_day,
                        "model": model_name,
                        "actual": int(actual),
                        "probabilities": probability.tolist(),
                    }
                )
    metrics = []
    for model_name in feature_sets:
        selected = [row for row in predictions if row["model"] == model_name]
        if not selected:
            continue
        actual = np.asarray([row["actual"] for row in selected], dtype=int)
        probabilities = np.asarray([row["probabilities"] for row in selected], dtype=float)
        metrics.append(
            {
                "model": model_name,
                "n": len(selected),
                "sessions": len({row["session_date"] for row in selected}),
                "class_rates": {
                    str(label): float(np.mean(actual == label)) for label in classes
                },
                "multiclass_brier": multiclass_brier(actual, probabilities),
                "log_loss": float(log_loss(actual, probabilities, labels=classes)),
                "accuracy": float(
                    np.mean(classes[np.argmax(probabilities, axis=1)] == actual)
                ),
            }
        )
    return metrics, predictions


def clustered_multiclass_delta(
    predictions: Sequence[Mapping[str, Any]],
    *,
    left: str,
    right: str,
    replications: int = 2_000,
) -> dict[str, Any]:
    grouped: dict[date, dict[str, list[Mapping[str, Any]]]] = defaultdict(lambda: defaultdict(list))
    for row in predictions:
        grouped[row["session_date"]][str(row["model"])].append(row)
    deltas = []
    for day, models in grouped.items():
        left_rows = models.get(left, [])
        right_rows = models.get(right, [])
        if len(left_rows) != len(right_rows) or not left_rows:
            continue
        actual = np.asarray([row["actual"] for row in left_rows], dtype=int)
        left_probability = np.asarray([row["probabilities"] for row in left_rows], dtype=float)
        right_probability = np.asarray([row["probabilities"] for row in right_rows], dtype=float)
        deltas.append(
            multiclass_brier(actual, left_probability)
            - multiclass_brier(actual, right_probability)
        )
    if not deltas:
        return {"delta_multiclass_brier": None, "ci95": [None, None], "sessions": 0}
    values = np.asarray(deltas, dtype=float)
    rng = np.random.default_rng(20260819)
    draws = np.asarray(
        [np.mean(rng.choice(values, size=len(values), replace=True)) for _ in range(replications)]
    )
    return {
        "delta_multiclass_brier": float(np.mean(values)),
        "ci95": [float(np.quantile(draws, 0.025)), float(np.quantile(draws, 0.975))],
        "sessions": len(values),
    }


def fitted_state_space(
    train: Sequence[Mapping[str, Any]],
    test: Sequence[Mapping[str, Any]],
    names: Sequence[str],
) -> tuple[np.ndarray, np.ndarray, int, list[dict[str, float]]]:
    imputer = SimpleImputer(strategy="median")
    scaler = StandardScaler()
    train_scaled = scaler.fit_transform(imputer.fit_transform(matrix(train, names)))
    test_scaled = scaler.transform(imputer.transform(matrix(test, names)))
    candidates = []
    for components in range(2, 6):
        model = GaussianMixture(
            n_components=components,
            covariance_type="diag",
            reg_covar=1e-4,
            n_init=2,
            max_iter=500,
            random_state=20260819,
        ).fit(train_scaled)
        candidates.append((float(model.bic(train_scaled)), components, model))
    candidates.sort(key=lambda item: item[0])
    _, selected_components, selected = candidates[0]
    diagnostics = [
        {"components": components, "bic": bic}
        for bic, components, _ in sorted(candidates, key=lambda item: item[1])
    ]
    return (
        selected.predict_proba(train_scaled),
        selected.predict_proba(test_scaled),
        selected_components,
        diagnostics,
    )


def latent_state_walk_forward(
    rows: Sequence[Mapping[str, Any]],
    *,
    target: str,
    path_names: Sequence[str],
    state_names: Sequence[str],
) -> tuple[list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]]]:
    sessions = sorted({row["session_date"] for row in rows})
    predictions: list[dict[str, Any]] = []
    selections = []
    for test_day in sessions[MIN_TRAIN_SESSIONS:]:
        train = [row for row in rows if row["session_date"] < test_day]
        test = [row for row in rows if row["session_date"] == test_day]
        y_train = np.asarray([int(row[target]) for row in train], dtype=int)
        if not test or len(set(y_train.tolist())) < 2:
            continue
        train_state, test_state, components, diagnostics = fitted_state_space(
            train,
            test,
            state_names,
        )
        selections.append(
            {
                "session_date": test_day,
                "components": components,
                "bic": diagnostics,
            }
        )
        path_train = matrix(train, path_names)
        path_test = matrix(test, path_names)
        model_inputs = {
            "path": (path_train, path_test),
            "path+latent_state": (
                np.column_stack((path_train, train_state)),
                np.column_stack((path_test, test_state)),
            ),
        }
        for model_name, (train_x, test_x) in model_inputs.items():
            pipeline = Pipeline(
                [
                    ("impute", SimpleImputer(strategy="median")),
                    ("scale", StandardScaler()),
                    ("model", LogisticRegression(C=0.10, max_iter=2_000)),
                ]
            )
            pipeline.fit(train_x, y_train)
            probabilities = pipeline.predict_proba(test_x)[:, 1]
            for row, probability in zip(test, probabilities, strict=True):
                predictions.append(
                    {
                        "session_date": test_day,
                        "target": target,
                        "model": model_name,
                        "actual": int(row[target]),
                        "probability": float(np.clip(probability, 1e-6, 1 - 1e-6)),
                    }
                )
    metrics = []
    for model_name in ("path", "path+latent_state"):
        selected = [row for row in predictions if row["model"] == model_name]
        if not selected:
            continue
        y = np.asarray([row["actual"] for row in selected], dtype=int)
        probability = np.asarray([row["probability"] for row in selected], dtype=float)
        metrics.append(
            {
                "target": target,
                "model": model_name,
                "n": len(selected),
                "sessions": len({row["session_date"] for row in selected}),
                "base_rate": float(np.mean(y)),
                "brier": float(brier_score_loss(y, probability)),
                "log_loss": float(log_loss(y, probability, labels=[0, 1])),
                "auc": safe_auc(y, probability),
            }
        )
    return metrics, predictions, selections


def binary_prediction_metrics(
    predictions: Sequence[Mapping[str, Any]],
    *,
    target: str,
    model_names: Sequence[str],
) -> list[dict[str, Any]]:
    metrics = []
    for model_name in model_names:
        selected = [row for row in predictions if row["model"] == model_name]
        if not selected:
            continue
        actual = np.asarray([row["actual"] for row in selected], dtype=int)
        probability = np.asarray([row["probability"] for row in selected], dtype=float)
        metrics.append(
            {
                "target": target,
                "model": model_name,
                "n": len(selected),
                "sessions": len({row["session_date"] for row in selected}),
                "base_rate": float(np.mean(actual)),
                "brier": float(brier_score_loss(actual, probability)),
                "log_loss": float(log_loss(actual, probability, labels=[0, 1])),
                "auc": safe_auc(actual, probability),
            }
        )
    return metrics


def motif_walk_forward(
    rows: Sequence[Mapping[str, Any]],
    *,
    target: str,
    path_names: Sequence[str],
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    usable = [row for row in rows if isinstance(row.get("motif_window"), np.ndarray)]
    sessions = sorted({row["session_date"] for row in usable})
    predictions: list[dict[str, Any]] = []
    for test_day in sessions[MIN_TRAIN_SESSIONS:]:
        train = [row for row in usable if row["session_date"] < test_day]
        test = [row for row in usable if row["session_date"] == test_day]
        y_train = np.asarray([int(row[target]) for row in train], dtype=int)
        if not test or len(set(y_train.tolist())) < 2:
            continue
        train_shapes = np.stack([row["motif_window"] for row in train])
        test_shapes = np.stack([row["motif_window"] for row in test])
        dictionary = KMeans(
            n_clusters=min(8, max(2, len(train) // 50)),
            n_init=10,
            max_iter=500,
            random_state=20260819,
        ).fit(train_shapes)
        motif_train = dictionary.transform(train_shapes)
        motif_test = dictionary.transform(test_shapes)
        path_train = matrix(train, path_names)
        path_test = matrix(test, path_names)
        inputs = {
            "path": (path_train, path_test),
            "path+motif": (
                np.column_stack((path_train, motif_train)),
                np.column_stack((path_test, motif_test)),
            ),
        }
        for model_name, (train_x, test_x) in inputs.items():
            pipeline = Pipeline(
                [
                    ("impute", SimpleImputer(strategy="median")),
                    ("scale", StandardScaler()),
                    ("model", LogisticRegression(C=0.10, max_iter=2_000)),
                ]
            )
            pipeline.fit(train_x, y_train)
            probabilities = pipeline.predict_proba(test_x)[:, 1]
            for row, probability in zip(test, probabilities, strict=True):
                predictions.append(
                    {
                        "session_date": test_day,
                        "target": target,
                        "model": model_name,
                        "actual": int(row[target]),
                        "probability": float(np.clip(probability, 1e-6, 1 - 1e-6)),
                    }
                )
    return (
        binary_prediction_metrics(
            predictions,
            target=target,
            model_names=("path", "path+motif"),
        ),
        predictions,
    )


def sparse_moe_walk_forward(
    rows: Sequence[Mapping[str, Any]],
    *,
    target: str,
    gate_names: Sequence[str],
    expert_names: Sequence[str],
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    sessions = sorted({row["session_date"] for row in rows})
    predictions: list[dict[str, Any]] = []
    for test_day in sessions[MIN_TRAIN_SESSIONS:]:
        train = [row for row in rows if row["session_date"] < test_day]
        test = [row for row in rows if row["session_date"] == test_day]
        y_train = np.asarray([int(row[target]) for row in train], dtype=int)
        if not test or len(set(y_train.tolist())) < 2:
            continue

        gate_imputer = SimpleImputer(strategy="median")
        gate_scaler = StandardScaler()
        gate_train = gate_scaler.fit_transform(gate_imputer.fit_transform(matrix(train, gate_names)))
        gate_test = gate_scaler.transform(gate_imputer.transform(matrix(test, gate_names)))
        gate = GaussianMixture(
            n_components=2,
            covariance_type="diag",
            reg_covar=1e-4,
            n_init=5,
            max_iter=500,
            random_state=20260819,
        ).fit(gate_train)
        train_responsibility = gate.predict_proba(gate_train)
        test_responsibility = gate.predict_proba(gate_test)

        expert_imputer = SimpleImputer(strategy="median")
        expert_scaler = StandardScaler()
        expert_train = expert_scaler.fit_transform(
            expert_imputer.fit_transform(matrix(train, expert_names))
        )
        expert_test = expert_scaler.transform(
            expert_imputer.transform(matrix(test, expert_names))
        )
        global_model = LogisticRegression(C=0.10, max_iter=2_000).fit(
            expert_train,
            y_train,
        )
        global_probability = global_model.predict_proba(expert_test)[:, 1]
        expert_probabilities = []
        for component in range(2):
            weights = train_responsibility[:, component]
            class_weight = [float(np.sum(weights[y_train == label])) for label in (0, 1)]
            if min(class_weight) < 5.0:
                expert_probabilities.append(global_probability)
                continue
            expert = LogisticRegression(C=0.10, max_iter=2_000).fit(
                expert_train,
                y_train,
                sample_weight=weights,
            )
            expert_probabilities.append(expert.predict_proba(expert_test)[:, 1])
        moe_probability = np.sum(
            test_responsibility * np.column_stack(expert_probabilities),
            axis=1,
        )
        for model_name, probabilities in (
            ("sparse_global", global_probability),
            ("sparse_moe_2expert", moe_probability),
        ):
            for row, probability in zip(test, probabilities, strict=True):
                predictions.append(
                    {
                        "session_date": test_day,
                        "target": target,
                        "model": model_name,
                        "actual": int(row[target]),
                        "probability": float(np.clip(probability, 1e-6, 1 - 1e-6)),
                    }
                )
    return (
        binary_prediction_metrics(
            predictions,
            target=target,
            model_names=("sparse_global", "sparse_moe_2expert"),
        ),
        predictions,
    )


def descriptive_state_space(
    rows: Sequence[Mapping[str, Any]],
    names: Sequence[str],
) -> dict[str, Any]:
    imputer = SimpleImputer(strategy="median")
    scaler = StandardScaler()
    scaled = scaler.fit_transform(imputer.fit_transform(matrix(rows, names)))
    candidates = []
    for components in range(2, 7):
        model = GaussianMixture(
            n_components=components,
            covariance_type="diag",
            reg_covar=1e-4,
            n_init=5,
            max_iter=500,
            random_state=20260819,
        ).fit(scaled)
        candidates.append((float(model.bic(scaled)), components, model))
    candidates.sort(key=lambda item: item[0])
    _, components, selected = candidates[0]
    labels = selected.predict(scaled)
    stability = []
    for seed in (11, 29, 47, 83):
        alternative = GaussianMixture(
            n_components=components,
            covariance_type="diag",
            reg_covar=1e-4,
            n_init=1,
            max_iter=500,
            random_state=seed,
        ).fit(scaled)
        stability.append(float(adjusted_rand_score(labels, alternative.predict(scaled))))
    summary_features = (
        "return_15m_scale",
        "range_15m_scale",
        "zero_gamma_distance_scale",
        "call_wall_distance_scale",
        "put_wall_distance_scale",
        "expected_move_scale",
        "atm_iv_jump_5m",
        "net_gamma_ratio_proxy",
        "vix1d_vix_ratio",
    )
    summaries = []
    for state in range(components):
        selected_rows = [row for row, label in zip(rows, labels, strict=True) if label == state]
        summaries.append(
            {
                "state": state,
                "rows": len(selected_rows),
                "occupancy": len(selected_rows) / len(rows),
                "sessions": len({row["session_date"] for row in selected_rows}),
                "feature_means": {
                    feature: (
                        float(np.mean(values))
                        if (
                            values := [
                                value
                                for row in selected_rows
                                if (value := finite(row.get(feature))) is not None
                            ]
                        )
                        else None
                    )
                    for feature in summary_features
                },
                "outcome_rates_descriptive_only": {
                    target: float(np.mean([int(row[target]) for row in selected_rows]))
                    for target in TARGETS
                },
            }
        )
    transitions = 0
    self_transitions = 0
    previous: tuple[date, datetime, int] | None = None
    for row, label in sorted(
        zip(rows, labels, strict=True),
        key=lambda item: item[0]["decision_at"],
    ):
        if previous is not None:
            previous_day, previous_at, previous_label = previous
            if (
                previous_day == row["session_date"]
                and (row["decision_at"] - previous_at).total_seconds() <= 600
            ):
                transitions += 1
                self_transitions += int(previous_label == int(label))
        previous = (row["session_date"], row["decision_at"], int(label))
    return {
        "selected_components": components,
        "bic": [
            {"components": component_count, "bic": bic}
            for bic, component_count, _ in sorted(candidates, key=lambda item: item[1])
        ],
        "seed_stability_ari": stability,
        "mean_seed_stability_ari": float(np.mean(stability)),
        "self_transition_rate": self_transitions / transitions if transitions else None,
        "states": summaries,
        "note": "Full-window states are descriptive only; OOS edge uses fold-local states.",
    }


def json_safe(value: object) -> object:
    if isinstance(value, Mapping):
        return {str(key): json_safe(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [json_safe(item) for item in value]
    if isinstance(value, (date, datetime)):
        return value.isoformat()
    if isinstance(value, np.integer):
        return int(value)
    if isinstance(value, np.floating):
        return float(value) if math.isfinite(float(value)) else None
    if isinstance(value, float) and not math.isfinite(value):
        return None
    return value


def run_analysis() -> dict[str, Any]:
    rows, quality = feature_rows()
    path = available_features(rows, PATH_FEATURES)
    structure = available_features(rows, STRUCTURE_FEATURES)
    change = available_features(rows, CHANGE_FEATURES)
    wall_hazard = available_features(rows, WALL_HAZARD_FEATURES)
    feature_sets = {
        "intercept": (),
        "path": path,
        "structure": structure,
        "path+structure": (*path, *structure),
    }
    metrics: list[dict[str, Any]] = []
    increments = []
    factor_ablation = []
    latent_metrics = []
    latent_increments = []
    latent_state_selections = []
    change_metrics = []
    change_increments = []
    motif_metrics = []
    motif_increments = []
    moe_metrics = []
    moe_increments = []
    prevalences = []
    feature_coverage = [
        {
            "feature": name,
            "available_rate": sum(finite(row.get(name)) is not None for row in rows) / max(len(rows), 1),
        }
        for name in (*PATH_FEATURES, *STRUCTURE_FEATURES, *CHANGE_FEATURES, *QUALITY_FEATURES)
    ]
    for target in TARGETS:
        eligibility = TARGET_ELIGIBILITY[target]
        target_rows = (
            rows
            if eligibility is None
            else [row for row in rows if int(row[eligibility]) == 1]
        )
        prevalences.append(
            {
                "target": target,
                "eligibility": eligibility or "all_rows",
                "n": len(target_rows),
                "sessions": len({row["session_date"] for row in target_rows}),
                "positive_rate": sum(int(row[target]) for row in target_rows)
                / max(len(target_rows), 1),
            }
        )
        target_metrics, predictions = walk_forward(
            target_rows,
            target=target,
            feature_sets=feature_sets,
        )
        metrics.extend(target_metrics)
        increments.append({"target": target, **clustered_delta(predictions)})
        factor_sets = {
            "path": path,
            **{
                f"path+{feature}": (*path, feature)
                for feature in structure
            },
        }
        factor_metrics, factor_predictions = walk_forward(
            target_rows,
            target=target,
            feature_sets=factor_sets,
        )
        for factor_metric in factor_metrics:
            model_name = str(factor_metric["model"])
            if model_name == "path":
                continue
            delta = clustered_delta(
                factor_predictions,
                left=model_name,
                right="path",
                replications=1_000,
            )
            factor_ablation.append(
                {
                    "target": target,
                    "feature": model_name.removeprefix("path+"),
                    "brier": factor_metric["brier"],
                    "auc": factor_metric["auc"],
                    **delta,
                }
            )
        state_metrics, state_predictions, state_selections = latent_state_walk_forward(
            target_rows,
            target=target,
            path_names=path,
            state_names=(*path, *structure),
        )
        latent_metrics.extend(state_metrics)
        latent_increments.append(
            {
                "target": target,
                **clustered_delta(
                    state_predictions,
                    left="path+latent_state",
                    right="path",
                ),
            }
        )
        latent_state_selections.extend(
            {"target": target, **selection} for selection in state_selections
        )
        target_motif_metrics, target_motif_predictions = motif_walk_forward(
            target_rows,
            target=target,
            path_names=path,
        )
        motif_metrics.extend(target_motif_metrics)
        motif_increments.append(
            {
                "target": target,
                **clustered_delta(
                    target_motif_predictions,
                    left="path+motif",
                    right="path",
                ),
            }
        )
        if target == "reversal_15m":
            target_change_metrics, target_change_predictions = walk_forward(
                target_rows,
                target=target,
                feature_sets={
                    "path": path,
                    "path+change": (*path, *change),
                    "path+change+structure": (*path, *change, *structure),
                },
            )
            change_metrics.extend(target_change_metrics)
            for model_name in ("path+change", "path+change+structure"):
                change_increments.append(
                    {
                        "target": target,
                        "model": model_name,
                        **clustered_delta(
                            target_change_predictions,
                            left=model_name,
                            right="path",
                        ),
                    }
                )
        if target != "direction_up_5m":
            target_moe_metrics, target_moe_predictions = sparse_moe_walk_forward(
                target_rows,
                target=target,
                gate_names=MOE_GATE_FEATURES,
                expert_names=(*path, *wall_hazard),
            )
            moe_metrics.extend(target_moe_metrics)
            moe_increments.append(
                {
                    "target": target,
                    **clustered_delta(
                        target_moe_predictions,
                        left="sparse_moe_2expert",
                        right="sparse_global",
                    ),
                }
            )

    barrier_rows = [row for row in rows if int(row["breakout_eligible"]) == 1]
    competing_risk_features = {
        "intercept": (),
        "path": path,
        "walls": wall_hazard,
        "path+walls": (*path, *wall_hazard),
    }
    competing_risk_metrics, competing_risk_predictions = competing_risk_walk_forward(
        barrier_rows,
        competing_risk_features,
    )
    competing_risk_increment = clustered_multiclass_delta(
        competing_risk_predictions,
        left="walls",
        right="path",
    )
    return json_safe(
        {
            "generated_at": datetime.now(UTC),
            "scope": "RTH point-in-time structure event study; no strategy decisions",
            "window": {"start": START, "end": END},
            "quality": quality,
            "feature_sets": feature_sets,
            "competing_risk_feature_sets": competing_risk_features,
            "moe_gate_features": MOE_GATE_FEATURES,
            "feature_coverage": feature_coverage,
            "target_prevalence": prevalences,
            "metrics": metrics,
            "structure_increment": increments,
            "factor_ablation": factor_ablation,
            "latent_state_metrics": latent_metrics,
            "latent_state_increment": latent_increments,
            "latent_state_selections": latent_state_selections,
            "competing_risk_metrics": competing_risk_metrics,
            "competing_risk_increment": competing_risk_increment,
            "change_point_metrics": change_metrics,
            "change_point_increment": change_increments,
            "motif_metrics": motif_metrics,
            "motif_increment": motif_increments,
            "sparse_moe_metrics": moe_metrics,
            "sparse_moe_increment": moe_increments,
            "descriptive_state_space": descriptive_state_space(
                rows,
                (*path, *structure),
            ),
            "limitations": [
                "Call-positive/put-negative GEX is an OI proxy; dealer position sign is unknown.",
                "Surface snapshots can be wide-quote degraded and OI freshness varies by lane/provider.",
                "Only session-held-out inference is reported; rows within a session are not independent.",
                "This first pass is RTH-only. GTH requires a separate ES/basis clock and IBKR-only structure contract.",
                "No option PnL claim is made; exact-leg ask-to-bid economics remain a separate gate.",
                "Motifs are fold-local K-means shape dictionaries, not an exhaustive matrix-profile search.",
                "BOCPD uses a fixed known-variance Gaussian filter and CUSUM uses frozen constants; neither is a trade trigger.",
            ],
        }
    )


def metric_rows(analysis: Mapping[str, Any], target: str) -> list[dict[str, Any]]:
    return [row for row in analysis["metrics"] if row["target"] == target]


def tldr(analysis: Mapping[str, Any]) -> str:
    lines = []
    for row in analysis["structure_increment"]:
        delta = row["delta_brier"]
        low, high = row["ci95"]
        verdict = "支持增量" if high is not None and high < 0 else "未确认"
        lines.append(
            f"- {row['target']}: combined−path Brier {delta:+.4f}, "
            f"session bootstrap 95% CI [{low:+.4f}, {high:+.4f}]，{verdict}。"
        )
    for row in analysis.get("latent_state_increment") or ():
        delta = row["delta_brier"]
        low, high = row["ci95"]
        verdict = "支持状态增量" if high is not None and high < 0 else "状态增量未确认"
        lines.append(
            f"- latent {row['target']}: state+path−path Brier {delta:+.4f}, "
            f"95% CI [{low:+.4f}, {high:+.4f}]，{verdict}。"
        )
    hazard = analysis.get("competing_risk_increment") or {}
    if hazard.get("delta_multiclass_brier") is not None:
        low, high = hazard["ci95"]
        lines.append(
            f"- wall competing-risk: walls−path multiclass Brier "
            f"{hazard['delta_multiclass_brier']:+.4f}, 95% CI [{low:+.4f}, {high:+.4f}]。"
        )
    for key, label in (
        ("change_point_increment", "change-point reversal"),
        ("motif_increment", "5s motif"),
        ("sparse_moe_increment", "sparse MoE"),
    ):
        for row in analysis.get(key) or ():
            delta = row.get("delta_brier")
            low, high = row.get("ci95", (None, None))
            if delta is None:
                continue
            lines.append(
                f"- {label} {row['target']}: ΔBrier {delta:+.4f}, "
                f"95% CI [{low:+.4f}, {high:+.4f}]。"
            )
    return "\n".join(lines)


def build_notebook(analysis: Mapping[str, Any]) -> nbformat.NotebookNode:
    source_literal = repr(str(SOURCE_PATH))
    setup = f"""
import importlib.util
import json
spec = importlib.util.spec_from_file_location("structure_event_study", {source_literal})
module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(module)
analysis = module.run_analysis()
print(json.dumps(analysis["quality"], indent=2, ensure_ascii=False))
""".strip()
    results = """
for target in module.TARGETS:
    print("\\n", target)
    for row in module.metric_rows(analysis, target):
        print(row)
print("\\nStructure increment")
for row in analysis["structure_increment"]:
    print(row)
print("\\nBest one-factor increments by target")
for target in module.TARGETS:
    rows = sorted(
        (row for row in analysis["factor_ablation"] if row["target"] == target),
        key=lambda row: row["delta_brier"],
    )
    print(target, rows[:5])
print("\\nLatent-state OOS increment")
for row in analysis["latent_state_increment"]:
    print(row)
print("\\nDescriptive state space")
print(json.dumps(analysis["descriptive_state_space"], indent=2, ensure_ascii=False))
print("\\nWall competing-risk hazard")
for row in analysis["competing_risk_metrics"]:
    print(row)
print("increment", analysis["competing_risk_increment"])
print("\\nCUSUM/BOCPD reversal")
for row in analysis["change_point_metrics"]:
    print(row)
print("increment", analysis["change_point_increment"])
print("\\n5-second motif dictionary")
for row in analysis["motif_increment"]:
    print(row)
print("\\nSparse two-expert model")
for row in analysis["sparse_moe_increment"]:
    print(row)
""".strip()
    checks = """
assert analysis["quality"]["sessions"] >= 15
assert all(row["sessions"] >= 5 for row in analysis["structure_increment"])
assert all(row["sessions"] >= 5 for row in analysis["latent_state_increment"])
assert analysis["competing_risk_increment"]["sessions"] >= 5
assert all(row["sessions"] >= 5 for row in analysis["change_point_increment"])
assert all(row["sessions"] >= 5 for row in analysis["motif_increment"])
assert all(row["sessions"] >= 5 for row in analysis["sparse_moe_increment"])
assert 2 <= analysis["descriptive_state_space"]["selected_components"] <= 6
assert all(
    row["available_rate"] >= 0.50
    for row in analysis["feature_coverage"]
    if row["feature"] in analysis["feature_sets"]["path+structure"]
)
print("causal/session-held-out acceptance checks passed")
""".strip()
    notebook = nbformat.v4.new_notebook(
        cells=[
            nbformat.v4.new_markdown_cell(
                "# SPX RTH 原始路径 × 期权结构事件研究\n\n"
                "## tl;dr\n\n"
                + tldr(analysis)
            ),
            nbformat.v4.new_markdown_cell(
                "## Context & Methods\n\n"
                "- 不读取 production strategy/candidate 作为样本或标签。\n"
                "- 结构快照必须先于 decision time；原始 SPX/VIX/VIX1D 仅作 backward as-of join。\n"
                "- 标签覆盖 5m direction、15m 墙位突破延续、反转、回撤恢复。\n"
                "- 突破额外拆成上破站稳 / 下破站稳 / 未突破 competing risk。\n"
                "- 5 秒路径构造因果 CUSUM/BOCPD 与 fold-local motif dictionary。\n"
                "- sparse MoE gate 固定只看 15m return 与 Call/Put wall 距离。\n"
                "- 以前 10 个 session 起步，之后 expanding session-held-out walk-forward。\n"
                "- 主比较为 path-only 与 path+structure 的 Brier 差，按 session bootstrap。\n\n"
                "### Key Assumptions\n\n"
                "GEX 采用 Call+ / Put− OI proxy，不代表真实 dealer 仓位；结构用于条件化，"
                "不能直接宣称对冲流方向。"
            ),
            nbformat.v4.new_markdown_cell("## Data\n\n读取 5 分钟 IV surface、点时 Greeks/OI proxy 和 Schwab live SPX/VIX/VIX1D。"),
            nbformat.v4.new_code_cell(setup),
            nbformat.v4.new_markdown_cell("## Results"),
            nbformat.v4.new_code_cell(results),
            nbformat.v4.new_code_cell(
                "print(json.dumps(analysis['feature_coverage'], indent=2, ensure_ascii=False))"
            ),
            nbformat.v4.new_markdown_cell(
                "## Takeaways\n\n"
                + tldr(analysis)
                + "\n\n任何区间跨 0 的结果都只算未确认；下一步只能增加独立 session，不能靠继续调阈值制造显著性。"
            ),
            nbformat.v4.new_code_cell(checks),
        ],
        metadata={"kernelspec": {"display_name": "Python 3", "language": "python", "name": "python3"}},
    )
    for index, cell in enumerate(notebook.cells):
        cell["id"] = f"structure-event-{index:02d}"
    nbformat.validate(notebook)
    return notebook


def main() -> int:
    analysis = run_analysis()
    ARTIFACT_PATH.parent.mkdir(parents=True, exist_ok=True)
    ARTIFACT_PATH.write_text(
        json.dumps(analysis, indent=2, ensure_ascii=False) + "\n",
        encoding="utf-8",
    )
    notebook = build_notebook(analysis)
    client = NotebookClient(notebook, timeout=900, kernel_name="python3")
    client.execute(cwd=str(REPO_ROOT))
    nbformat.validate(notebook)
    nbformat.write(notebook, NOTEBOOK_PATH)
    print(NOTEBOOK_PATH)
    print(ARTIFACT_PATH)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
