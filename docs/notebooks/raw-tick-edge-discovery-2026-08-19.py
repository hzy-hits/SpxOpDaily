# %% [markdown]
# # SPX 0DTE 原始行情 Edge Discovery
#
# 本研究从可用时点上的原始 quote updates 出发，不读取生产策略候选、决策、版本、阈值、
# setup 或管理规则。持有期、特征块、模型和置信门槛只在非封存 session 中选择；最后六个
# session 保持封存，仅用于一次最终检验。
#
# 生产影子运行通过 `SPX_SPARK_RESEARCH_OUTPUT_ROOT` 把合同与结果写到运行时数据目录；
# 默认仍写入 `docs/research`，用于可复现报告。无论输出位置如何，所有 action authority 为 none。

# %%
from __future__ import annotations

from collections import defaultdict
from dataclasses import asdict, dataclass
from datetime import date, datetime, time, timedelta
import hashlib
import json
import math
import os
from pathlib import Path
from typing import Any, Iterable, Mapping, Sequence
from zoneinfo import ZoneInfo

import duckdb
import numpy as np
from sklearn.ensemble import HistGradientBoostingRegressor
from sklearn.impute import SimpleImputer
from sklearn.linear_model import Ridge
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import StandardScaler


REPO_ROOT = next(
    (path for path in (Path.cwd(), *Path.cwd().parents) if (path / "src/spx_spark").is_dir()),
    None,
)
if REPO_ROOT is None:
    raise RuntimeError("Run from the spx-spark repository")

QUOTE_ROOT = Path(
    os.environ.get("SPX_SPARK_QUOTE_ROOT", "/srv/data/spx-spark/data/lake/quotes/schema=v1")
)
RESEARCH_OUTPUT_ROOT = Path(
    os.environ.get(
        "SPX_SPARK_RESEARCH_OUTPUT_ROOT",
        str(REPO_ROOT / "docs/research"),
    )
).expanduser()
RESEARCH_OUTPUT_ROOT.mkdir(parents=True, exist_ok=True)
NY = ZoneInfo("America/New_York")
UTC = ZoneInfo("UTC")
BUCKET_SECONDS = 5
DECISION_SECONDS = 15
HOLDING_SECONDS = (15, 30, 60, 120, 240, 480, 900, 1800, 3600)
PAST_SECONDS = (5, 15, 30, 60, 120, 300, 600)
CONFIDENCE_QUANTILES = (0.50, 0.65, 0.80, 0.90, 0.95, 0.975, 0.99)
OPTION_MAX_RELATIVE_SPREADS = (0.01, 0.02, 0.05, 0.10, 0.20)
VALIDATION_SESSIONS = 8
SEALED_TEST_SESSIONS = 6
MIN_TRAIN_SESSIONS = 10
RNG_SEED = 20260819
RESEARCH_END_DATE = date.fromisoformat(
    os.environ.get("SPX_SPARK_RESEARCH_END_DATE", "2026-08-18")
)


def finite(value: object) -> float | None:
    if isinstance(value, bool) or not isinstance(value, int | float):
        return None
    result = float(value)
    return result if math.isfinite(result) else None


def json_safe(value: object) -> object:
    if isinstance(value, Mapping):
        return {str(key): json_safe(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [json_safe(item) for item in value]
    if isinstance(value, np.ndarray):
        return [json_safe(item) for item in value.tolist()]
    if isinstance(value, np.integer):
        return int(value)
    if isinstance(value, np.floating):
        result = float(value)
        return result if math.isfinite(result) else None
    if isinstance(value, datetime | date):
        return value.isoformat()
    if isinstance(value, float) and not math.isfinite(value):
        return None
    return value


@dataclass(frozen=True)
class ModeContract:
    mode: str
    target: str
    instruments: tuple[str, ...]
    start_time: time
    end_time: time
    minimum_coverage: float
    target_label: str


RTH = ModeContract(
    mode="rth",
    target="index:SPX",
    instruments=(
        "index:SPX",
        "future:ES",
        "future:NQ",
        "future:RTY",
        "future:YM",
        "equity:SPY",
        "index:VIX",
    ),
    start_time=time(9, 30),
    end_time=time(16, 0),
    minimum_coverage=0.95,
    target_label="SPX",
)

GTH = ModeContract(
    mode="gth",
    target="future:ES",
    instruments=("future:ES", "future:NQ", "future:RTY", "future:YM"),
    start_time=time(20, 15),
    end_time=time(9, 25),
    minimum_coverage=0.95,
    target_label="ES proxy",
)


@dataclass
class InstrumentGrid:
    price: np.ndarray
    bid: np.ndarray
    ask: np.ndarray
    bid_size: np.ndarray
    ask_size: np.ndarray
    depth_imbalance: np.ndarray
    microprice_deviation_bps: np.ndarray
    ofi: np.ndarray
    volume_delta: np.ndarray
    quote_count: np.ndarray
    age_seconds: np.ndarray


@dataclass
class SessionGrid:
    session_date: date
    mode: str
    epoch_seconds: np.ndarray
    instruments: dict[str, InstrumentGrid]
    target_coverage: float
    raw_rows: int


@dataclass
class SampleSet:
    X: np.ndarray
    feature_names: tuple[str, ...]
    session_dates: np.ndarray
    epoch_seconds: np.ndarray
    target_price: np.ndarray
    outcomes: dict[int, np.ndarray]
    delayed_outcomes: dict[str, np.ndarray]


@dataclass(frozen=True)
class ModelSpec:
    family: str
    feature_block: str
    parameter: float

    @property
    def name(self) -> str:
        return f"{self.family}:{self.feature_block}:{self.parameter:g}"


def available_dates() -> list[date]:
    values: list[date] = []
    for path in QUOTE_ROOT.glob("date=2026-*/provider=schwab"):
        try:
            values.append(date.fromisoformat(path.parent.name.removeprefix("date=")))
        except ValueError:
            continue
    return sorted(set(values))


def daily_bucket_sql(instruments: Sequence[str]) -> str:
    instrument_sql = ", ".join(f"'{value}'" for value in instruments)
    return f"""
    WITH filtered AS (
      SELECT
        received_at,
        source_at,
        instrument_id,
        effective_price,
        bid,
        ask,
        bid_size,
        ask_size,
        volume,
        lag(bid) OVER instrument_window AS previous_bid,
        lag(ask) OVER instrument_window AS previous_ask,
        lag(bid_size) OVER instrument_window AS previous_bid_size,
        lag(ask_size) OVER instrument_window AS previous_ask_size,
        lag(volume) OVER instrument_window AS previous_volume
      FROM read_parquet(?, hive_partitioning=true, union_by_name=true)
      WHERE provider = 'schwab'
        AND instrument_id IN ({instrument_sql})
        AND quality = 'live'
        AND lower(coalesce(market_data_type, 'live')) IN ('live', '1')
        AND effective_price > 0
        AND source_at IS NOT NULL
        AND source_at >= received_at - INTERVAL 30 SECOND
        AND source_at <= received_at + INTERVAL 5 SECOND
        AND (quote_time IS NULL OR quote_time <= received_at + INTERVAL 5 SECOND)
      WINDOW instrument_window AS (
        PARTITION BY instrument_id ORDER BY received_at, source_at
      )
    ), normalized AS (
      SELECT
        to_timestamp((floor(epoch(received_at) / 5) + 1) * 5) AS bucket_end,
        instrument_id,
        received_at,
        effective_price,
        bid,
        ask,
        bid_size,
        ask_size,
        CASE
          WHEN bid_size IS NOT NULL AND ask_size IS NOT NULL
            AND bid_size + ask_size > 0
          THEN (bid_size - ask_size) / (bid_size + ask_size)
        END AS depth_imbalance,
        CASE
          WHEN bid > 0 AND ask >= bid AND bid_size IS NOT NULL AND ask_size IS NOT NULL
            AND bid_size + ask_size > 0
          THEN (((ask * bid_size + bid * ask_size) / (bid_size + ask_size))
                / ((bid + ask) / 2) - 1) * 10000
        END AS microprice_deviation_bps,
        coalesce(
          CASE WHEN bid >= previous_bid THEN bid_size ELSE 0 END, 0
        ) - coalesce(
          CASE WHEN bid <= previous_bid THEN previous_bid_size ELSE 0 END, 0
        ) - coalesce(
          CASE WHEN ask <= previous_ask THEN ask_size ELSE 0 END, 0
        ) + coalesce(
          CASE WHEN ask >= previous_ask THEN previous_ask_size ELSE 0 END, 0
        ) AS ofi,
        CASE
          WHEN volume >= previous_volume THEN volume - previous_volume
          ELSE 0
        END AS volume_delta
      FROM filtered
    )
    SELECT
      bucket_end,
      instrument_id,
      arg_max(effective_price, received_at) AS price,
      arg_max(bid, received_at) AS bid,
      arg_max(ask, received_at) AS ask,
      arg_max(bid_size, received_at) AS bid_size,
      arg_max(ask_size, received_at) AS ask_size,
      arg_max(depth_imbalance, received_at) AS depth_imbalance,
      arg_max(microprice_deviation_bps, received_at) AS microprice_deviation_bps,
      sum(ofi) AS ofi,
      sum(volume_delta) AS volume_delta,
      count(*) AS quote_count
    FROM normalized
    GROUP BY 1, 2
    ORDER BY 1, 2
    """


BUCKET_COLUMNS = (
    "bucket_end",
    "instrument_id",
    "price",
    "bid",
    "ask",
    "bid_size",
    "ask_size",
    "depth_imbalance",
    "microprice_deviation_bps",
    "ofi",
    "volume_delta",
    "quote_count",
)


def load_daily_buckets(session_date: date) -> tuple[list[dict[str, Any]], str]:
    path = QUOTE_ROOT / f"date={session_date.isoformat()}" / "provider=schwab" / "hour=*" / "quotes.parquet"
    query = daily_bucket_sql(tuple(dict.fromkeys((*RTH.instruments, *GTH.instruments))))
    connection = duckdb.connect()
    try:
        rows = connection.execute(query, [str(path)]).fetchall()
    finally:
        connection.close()
    return [dict(zip(BUCKET_COLUMNS, row, strict=True)) for row in rows], query


def session_bounds(session_date: date, contract: ModeContract) -> tuple[datetime, datetime]:
    if contract.mode == "rth":
        start = datetime.combine(session_date, contract.start_time, tzinfo=NY)
        end = datetime.combine(session_date, contract.end_time, tzinfo=NY)
    else:
        start = datetime.combine(session_date - timedelta(days=1), contract.start_time, tzinfo=NY)
        end = datetime.combine(session_date, contract.end_time, tzinfo=NY)
    return start.astimezone(UTC), end.astimezone(UTC)


def as_float_array(values: Sequence[object]) -> np.ndarray:
    return np.asarray([float(value) if finite(value) is not None else np.nan for value in values])


def build_instrument_grid(
    rows: Sequence[Mapping[str, Any]],
    timeline: np.ndarray,
    *,
    max_age_seconds: int,
) -> InstrumentGrid:
    size = len(timeline)
    empty = np.full(size, np.nan)
    zeros = np.zeros(size)
    if not rows:
        return InstrumentGrid(*(empty.copy() for _ in range(7)), zeros.copy(), zeros.copy(), zeros.copy(), empty.copy())

    ordered = sorted(rows, key=lambda row: row["bucket_end"])
    observed_epoch = np.asarray([int(row["bucket_end"].timestamp()) for row in ordered], dtype=np.int64)
    positions = np.searchsorted(observed_epoch, timeline, side="right") - 1
    valid = positions >= 0
    age = np.full(size, np.nan)
    age[valid] = timeline[valid] - observed_epoch[positions[valid]]
    fresh = valid & (age <= max_age_seconds)

    def state(column: str) -> np.ndarray:
        source = as_float_array([row[column] for row in ordered])
        output = np.full(size, np.nan)
        output[fresh] = source[positions[fresh]]
        return output

    exact_index = {int(value): index for index, value in enumerate(timeline)}

    def flow(column: str) -> np.ndarray:
        output = np.zeros(size)
        for row in ordered:
            index = exact_index.get(int(row["bucket_end"].timestamp()))
            value = finite(row[column])
            if index is not None and value is not None:
                output[index] += value
        return output

    return InstrumentGrid(
        price=state("price"),
        bid=state("bid"),
        ask=state("ask"),
        bid_size=state("bid_size"),
        ask_size=state("ask_size"),
        depth_imbalance=state("depth_imbalance"),
        microprice_deviation_bps=state("microprice_deviation_bps"),
        ofi=flow("ofi"),
        volume_delta=flow("volume_delta"),
        quote_count=flow("quote_count"),
        age_seconds=age,
    )


def build_session_grid(
    session_date: date,
    contract: ModeContract,
    daily_rows: Sequence[Mapping[str, Any]],
) -> SessionGrid:
    start, end = session_bounds(session_date, contract)
    start_epoch = int(start.timestamp())
    end_epoch = int(end.timestamp())
    timeline = np.arange(start_epoch + BUCKET_SECONDS, end_epoch + 1, BUCKET_SECONDS, dtype=np.int64)
    relevant = [
        row
        for row in daily_rows
        if start_epoch < int(row["bucket_end"].timestamp()) <= end_epoch
        and row["instrument_id"] in contract.instruments
    ]
    by_instrument: dict[str, list[Mapping[str, Any]]] = defaultdict(list)
    for row in relevant:
        by_instrument[str(row["instrument_id"])].append(row)
    grids: dict[str, InstrumentGrid] = {}
    for instrument in contract.instruments:
        max_age = 30 if instrument == "index:VIX" else 15
        grids[instrument] = build_instrument_grid(
            by_instrument[instrument], timeline, max_age_seconds=max_age
        )
    coverage = float(np.mean(np.isfinite(grids[contract.target].price)))
    return SessionGrid(
        session_date=session_date,
        mode=contract.mode,
        epoch_seconds=timeline,
        instruments=grids,
        target_coverage=coverage,
        raw_rows=sum(int(row["quote_count"]) for row in relevant),
    )


print("Loading causal five-second quote-update buckets by session …")
all_dates = [
    value
    for value in available_dates()
    if date(2026, 7, 13) <= value <= RESEARCH_END_DATE
]
daily_cache: dict[date, list[dict[str, Any]]] = {}
source_sql = daily_bucket_sql(tuple(dict.fromkeys((*RTH.instruments, *GTH.instruments))))
for ordinal, session_date in enumerate(all_dates, start=1):
    rows, _ = load_daily_buckets(session_date)
    daily_cache[session_date] = rows
    if ordinal % 5 == 0 or ordinal == len(all_dates):
        print(f"  loaded {ordinal}/{len(all_dates)} dates")

session_grids: dict[str, list[SessionGrid]] = {}
for contract in (RTH, GTH):
    candidates = [build_session_grid(value, contract, daily_cache[value]) for value in all_dates]
    session_grids[contract.mode] = [
        grid for grid in candidates if grid.target_coverage >= contract.minimum_coverage
    ]
    print(
        contract.mode,
        {
            "complete_sessions": len(session_grids[contract.mode]),
            "coverage_min": min(grid.target_coverage for grid in session_grids[contract.mode]),
            "coverage_median": float(np.median([grid.target_coverage for grid in session_grids[contract.mode]])),
        },
    )

# %% [markdown]
# ## Causal feature construction
#
# 每个五秒桶只在桶结束时可用；决策每十五秒观察一次。预测窗口是从 15 秒到 60 分钟的
# 几何网格，没有固定的 20 分钟标签。特征只描述价格路径、L1 order-flow、跨资产领先/滞后
# 和时段/波动状态。

# %%
def shifted(values: np.ndarray, steps: int) -> np.ndarray:
    output = np.full_like(values, np.nan, dtype=float)
    if steps == 0:
        output[:] = values
    elif steps < len(values):
        output[steps:] = values[:-steps]
    return output


def log_return(values: np.ndarray, steps: int) -> np.ndarray:
    previous = shifted(values, steps)
    with np.errstate(divide="ignore", invalid="ignore"):
        result = np.log(values / previous) * 10000
    result[~np.isfinite(result)] = np.nan
    return result


def rolling_sum(values: np.ndarray, steps: int, *, minimum_fraction: float = 0.8) -> np.ndarray:
    finite_mask = np.isfinite(values)
    cleaned = np.where(finite_mask, values, 0.0)
    prefix = np.concatenate(([0.0], np.cumsum(cleaned)))
    counts = np.concatenate(([0], np.cumsum(finite_mask.astype(int))))
    output = np.full(len(values), np.nan)
    if steps <= 0 or steps > len(values):
        return output
    sums = prefix[steps:] - prefix[:-steps]
    valid_counts = counts[steps:] - counts[:-steps]
    valid = valid_counts >= math.ceil(steps * minimum_fraction)
    output[steps - 1 :] = np.where(valid, sums, np.nan)
    return output


def rolling_mean(values: np.ndarray, steps: int) -> np.ndarray:
    sums = rolling_sum(values, steps)
    counts = rolling_sum(np.isfinite(values).astype(float), steps, minimum_fraction=0.0)
    with np.errstate(divide="ignore", invalid="ignore"):
        result = sums / counts
    result[~np.isfinite(result)] = np.nan
    return result


def realized_volatility(price: np.ndarray, seconds: int) -> np.ndarray:
    increments = log_return(price, 1)
    squared = increments * increments
    return np.sqrt(rolling_sum(squared, seconds // BUCKET_SECONDS))


def path_efficiency(price: np.ndarray, seconds: int) -> np.ndarray:
    steps = seconds // BUCKET_SECONDS
    net = np.abs(log_return(price, steps))
    path = rolling_sum(np.abs(log_return(price, 1)), steps)
    with np.errstate(divide="ignore", invalid="ignore"):
        result = net / path
    result[~np.isfinite(result)] = np.nan
    return result


def feature_column(
    columns: list[np.ndarray],
    names: list[str],
    name: str,
    values: np.ndarray,
) -> None:
    columns.append(np.asarray(values, dtype=float))
    names.append(name)


def session_feature_matrix(
    grid: SessionGrid,
    contract: ModeContract,
) -> tuple[np.ndarray, tuple[str, ...]]:
    target = grid.instruments[contract.target]
    columns: list[np.ndarray] = []
    names: list[str] = []

    for seconds in PAST_SECONDS:
        feature_column(
            columns,
            names,
            f"price/{contract.target}/return_{seconds}s_bps",
            log_return(target.price, seconds // BUCKET_SECONDS),
        )
    for seconds in (60, 300, 600):
        feature_column(
            columns,
            names,
            f"price/{contract.target}/rv_{seconds}s_bps",
            realized_volatility(target.price, seconds),
        )
        feature_column(
            columns,
            names,
            f"price/{contract.target}/efficiency_{seconds}s",
            path_efficiency(target.price, seconds),
        )

    micro_instruments = ("future:ES",) if contract.target == "future:ES" else (contract.target, "future:ES")
    for instrument in micro_instruments:
        values = grid.instruments[instrument]
        with np.errstate(divide="ignore", invalid="ignore"):
            spread_bps = (values.ask - values.bid) / values.price * 10000
        feature_column(columns, names, f"micro/{instrument}/depth_imbalance", values.depth_imbalance)
        feature_column(
            columns,
            names,
            f"micro/{instrument}/microprice_deviation_bps",
            values.microprice_deviation_bps,
        )
        feature_column(columns, names, f"micro/{instrument}/spread_bps", spread_bps)
        depth = values.bid_size + values.ask_size
        for seconds in (15, 60, 300):
            steps = seconds // BUCKET_SECONDS
            ofi_sum = rolling_sum(values.ofi, steps, minimum_fraction=0.0)
            quote_sum = rolling_sum(values.quote_count, steps, minimum_fraction=0.0)
            depth_mean = rolling_mean(depth, steps)
            with np.errstate(divide="ignore", invalid="ignore"):
                normalized_ofi = ofi_sum / (depth_mean * np.sqrt(np.maximum(quote_sum, 1)))
            feature_column(
                columns,
                names,
                f"micro/{instrument}/normalized_ofi_{seconds}s",
                normalized_ofi,
            )
            feature_column(
                columns,
                names,
                f"micro/{instrument}/quote_count_{seconds}s",
                np.log1p(quote_sum),
            )
            feature_column(
                columns,
                names,
                f"micro/{instrument}/volume_delta_{seconds}s",
                np.log1p(rolling_sum(values.volume_delta, steps, minimum_fraction=0.0)),
            )

    cross_returns: dict[tuple[str, int], np.ndarray] = {}
    for instrument in contract.instruments:
        if instrument == contract.target:
            continue
        price = grid.instruments[instrument].price
        for seconds in (15, 60, 300):
            values = log_return(price, seconds // BUCKET_SECONDS)
            cross_returns[(instrument, seconds)] = values
            feature_column(
                columns,
                names,
                f"cross/{instrument}/return_{seconds}s_bps",
                values,
            )

    if contract.mode == "rth":
        for seconds in (15, 60, 300):
            target_return = log_return(target.price, seconds // BUCKET_SECONDS)
            feature_column(
                columns,
                names,
                f"cross/es_minus_spx_{seconds}s_bps",
                cross_returns[("future:ES", seconds)] - target_return,
            )

    elapsed = grid.epoch_seconds - grid.epoch_seconds[0]
    duration = max(float(elapsed[-1]), 1.0)
    phase = 2 * np.pi * elapsed / duration
    feature_column(columns, names, "state/session_progress", elapsed / duration)
    feature_column(columns, names, "state/time_sin", np.sin(phase))
    feature_column(columns, names, "state/time_cos", np.cos(phase))
    rv_60 = realized_volatility(target.price, 60)
    rv_600 = realized_volatility(target.price, 600)
    with np.errstate(divide="ignore", invalid="ignore"):
        feature_column(columns, names, "state/rv_60_to_600", rv_60 / rv_600)
    return np.column_stack(columns), tuple(names)


def build_sample_set(grids: Sequence[SessionGrid], contract: ModeContract) -> SampleSet:
    matrices: list[np.ndarray] = []
    dates: list[np.ndarray] = []
    epochs: list[np.ndarray] = []
    prices: list[np.ndarray] = []
    outcomes: dict[int, list[np.ndarray]] = {seconds: [] for seconds in HOLDING_SECONDS}
    delayed: dict[str, list[np.ndarray]] = {
        f"{seconds}s_delay_{delay}s": []
        for seconds in HOLDING_SECONDS
        for delay in (5, 10, 15)
        if delay < seconds
    }
    feature_names: tuple[str, ...] | None = None
    decision_stride = DECISION_SECONDS // BUCKET_SECONDS
    minimum_history = max(PAST_SECONDS) // BUCKET_SECONDS

    for grid in grids:
        matrix, current_names = session_feature_matrix(grid, contract)
        if feature_names is None:
            feature_names = current_names
        elif feature_names != current_names:
            raise AssertionError("feature schema drift")
        indices = np.arange(minimum_history, len(grid.epoch_seconds), decision_stride)
        matrices.append(matrix[indices])
        dates.append(np.asarray([grid.session_date.isoformat()] * len(indices), dtype=object))
        epochs.append(grid.epoch_seconds[indices])
        target_price = grid.instruments[contract.target].price
        prices.append(target_price[indices])
        for seconds in HOLDING_SECONDS:
            future_index = indices + seconds // BUCKET_SECONDS
            values = np.full(len(indices), np.nan)
            valid = future_index < len(target_price)
            with np.errstate(divide="ignore", invalid="ignore"):
                values[valid] = np.log(target_price[future_index[valid]] / target_price[indices[valid]]) * 10000
            outcomes[seconds].append(values)
            for delay in (5, 10, 15):
                if delay >= seconds:
                    continue
                delayed_entry = indices + delay // BUCKET_SECONDS
                delayed_values = np.full(len(indices), np.nan)
                delayed_valid = valid & (delayed_entry < len(target_price))
                with np.errstate(divide="ignore", invalid="ignore"):
                    delayed_values[delayed_valid] = (
                        np.log(
                            target_price[future_index[delayed_valid]]
                            / target_price[delayed_entry[delayed_valid]]
                        )
                        * 10000
                    )
                delayed[f"{seconds}s_delay_{delay}s"].append(delayed_values)

    if feature_names is None:
        raise RuntimeError(f"No complete {contract.mode} sessions")
    return SampleSet(
        X=np.concatenate(matrices),
        feature_names=feature_names,
        session_dates=np.concatenate(dates),
        epoch_seconds=np.concatenate(epochs),
        target_price=np.concatenate(prices),
        outcomes={seconds: np.concatenate(values) for seconds, values in outcomes.items()},
        delayed_outcomes={key: np.concatenate(values) for key, values in delayed.items()},
    )


sample_sets = {
    contract.mode: build_sample_set(session_grids[contract.mode], contract)
    for contract in (RTH, GTH)
}
print(
    {
        mode: {"samples": len(samples.X), "features": len(samples.feature_names)}
        for mode, samples in sample_sets.items()
    }
)

# %% [markdown]
# ## Discovery and sealed validation
#
# 每种交易时段按完整 session 顺序切分：最早的 session 训练，中间八个只用于选择模型、
# 持有期和置信分位数，最后六个完全封存。最终每个 RTH/GTH 只检验一个冠军，跨两种时段
# 对最终 p-value 做 Holm 校正。

# %%
def feature_indices(feature_names: Sequence[str], block: str) -> np.ndarray:
    allowed = {
        "price": ("price/",),
        "micro": ("price/", "micro/"),
        "cross": ("price/", "micro/", "cross/"),
        "full": ("price/", "micro/", "cross/", "state/"),
    }[block]
    return np.asarray(
        [index for index, name in enumerate(feature_names) if name.startswith(allowed)],
        dtype=int,
    )


def model_specs() -> list[ModelSpec]:
    specs = [
        ModelSpec("ridge", block, alpha)
        for block in ("price", "micro", "cross", "full")
        for alpha in (0.1, 10.0)
    ]
    specs.append(ModelSpec("histgb", "full", 10.0))
    return specs


def make_model(spec: ModelSpec) -> Pipeline:
    if spec.family == "ridge":
        return Pipeline(
            [
                ("imputer", SimpleImputer(strategy="median")),
                ("scale", StandardScaler()),
                ("model", Ridge(alpha=spec.parameter)),
            ]
        )
    if spec.family == "histgb":
        return Pipeline(
            [
                ("imputer", SimpleImputer(strategy="median")),
                (
                    "model",
                    HistGradientBoostingRegressor(
                        learning_rate=0.05,
                        max_iter=60,
                        max_leaf_nodes=7,
                        min_samples_leaf=100,
                        l2_regularization=spec.parameter,
                        early_stopping=True,
                        validation_fraction=0.15,
                        random_state=RNG_SEED,
                    ),
                ),
            ]
        )
    raise ValueError(spec)


def chronological_split(samples: SampleSet) -> dict[str, list[str]]:
    sessions = sorted(set(str(value) for value in samples.session_dates))
    train_count = len(sessions) - VALIDATION_SESSIONS - SEALED_TEST_SESSIONS
    if train_count < MIN_TRAIN_SESSIONS:
        raise RuntimeError(f"Only {len(sessions)} complete sessions")
    return {
        "train": sessions[:train_count],
        "validation": sessions[train_count : train_count + VALIDATION_SESSIONS],
        "test": sessions[-SEALED_TEST_SESSIONS:],
    }


def date_mask(samples: SampleSet, sessions: Iterable[str]) -> np.ndarray:
    return np.isin(samples.session_dates, np.asarray(list(sessions), dtype=object))


def select_signals(
    predictions: np.ndarray,
    outcomes: np.ndarray,
    samples: SampleSet,
    eligible: np.ndarray,
    *,
    threshold: float,
    cooldown_seconds: int,
) -> np.ndarray:
    chosen: list[int] = []
    base = (
        eligible
        & np.isfinite(predictions)
        & np.isfinite(outcomes)
        & (np.abs(predictions) >= threshold)
    )
    for session_date in sorted(set(str(value) for value in samples.session_dates[base])):
        indices = np.flatnonzero(base & (samples.session_dates == session_date))
        next_allowed = -1
        for index in indices:
            timestamp = int(samples.epoch_seconds[index])
            if timestamp < next_allowed:
                continue
            chosen.append(int(index))
            next_allowed = timestamp + cooldown_seconds
    return np.asarray(chosen, dtype=int)


def session_outcomes(
    signal_indices: np.ndarray,
    predictions: np.ndarray,
    outcomes: np.ndarray,
    samples: SampleSet,
) -> tuple[np.ndarray, dict[str, float]]:
    if len(signal_indices) == 0:
        return np.asarray([], dtype=float), {}
    signed = np.sign(predictions[signal_indices]) * outcomes[signal_indices]
    grouped: dict[str, list[float]] = defaultdict(list)
    for index, value in zip(signal_indices, signed, strict=True):
        grouped[str(samples.session_dates[index])].append(float(value))
    means = {key: float(np.mean(values)) for key, values in grouped.items()}
    return signed, means


def evaluation_metrics(
    signal_indices: np.ndarray,
    predictions: np.ndarray,
    outcomes: np.ndarray,
    samples: SampleSet,
) -> dict[str, Any]:
    signed, grouped = session_outcomes(signal_indices, predictions, outcomes, samples)
    if len(signed) == 0:
        return {
            "signals": 0,
            "sessions": 0,
            "mean_signed_bps": None,
            "median_signed_bps": None,
            "hit_rate": None,
            "worst_decile_bps": None,
            "positive_session_rate": None,
            "session_lcb90_bps": None,
            "session_means_bps": {},
        }
    session_values = np.asarray(list(grouped.values()), dtype=float)
    standard_error = (
        float(np.std(session_values, ddof=1) / np.sqrt(len(session_values)))
        if len(session_values) > 1
        else float("inf")
    )
    return {
        "signals": len(signed),
        "sessions": len(grouped),
        "mean_signed_bps": float(np.mean(signed)),
        "median_signed_bps": float(np.median(signed)),
        "hit_rate": float(np.mean(signed > 0)),
        "worst_decile_bps": float(np.quantile(signed, 0.1)),
        "positive_session_rate": float(np.mean(session_values > 0)),
        "session_lcb90_bps": float(np.mean(session_values) - 1.645 * standard_error),
        "session_means_bps": grouped,
    }


def discovery_search(
    mode: str,
    samples: SampleSet,
    split: Mapping[str, Sequence[str]],
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    train_dates = date_mask(samples, split["train"])
    validation_dates = date_mask(samples, split["validation"])
    results: list[dict[str, Any]] = []
    for holding_ordinal, holding_seconds in enumerate(HOLDING_SECONDS, start=1):
        target = samples.outcomes[holding_seconds]
        train = train_dates & np.isfinite(target)
        validation = validation_dates & np.isfinite(target)
        for spec in model_specs():
            indices = feature_indices(samples.feature_names, spec.feature_block)
            indices = indices[
                np.any(np.isfinite(samples.X[train][:, indices]), axis=0)
            ]
            model = make_model(spec)
            model.fit(samples.X[train][:, indices], target[train])
            prediction = np.full(len(target), np.nan)
            prediction[train] = model.predict(samples.X[train][:, indices])
            prediction[validation] = model.predict(samples.X[validation][:, indices])
            for quantile in CONFIDENCE_QUANTILES:
                threshold = float(np.quantile(np.abs(prediction[train]), quantile))
                signal_indices = select_signals(
                    prediction,
                    target,
                    samples,
                    validation,
                    threshold=threshold,
                    cooldown_seconds=holding_seconds,
                )
                metrics = evaluation_metrics(signal_indices, prediction, target, samples)
                eligible_candidate = metrics["signals"] >= 25 and metrics["sessions"] >= 6
                score = metrics["session_lcb90_bps"] if eligible_candidate else -float("inf")
                results.append(
                    {
                        "mode": mode,
                        "holding_seconds": holding_seconds,
                        "model": spec.name,
                        "family": spec.family,
                        "feature_block": spec.feature_block,
                        "parameter": spec.parameter,
                        "confidence_quantile": quantile,
                        "threshold_bps": threshold,
                        "eligible": eligible_candidate,
                        "selection_score": score,
                        "validation": metrics,
                    }
                )
        print(f"  {mode}: searched {holding_ordinal}/{len(HOLDING_SECONDS)} holding windows")

    eligible = [row for row in results if row["eligible"]]
    if not eligible:
        raise RuntimeError(f"No eligible {mode} discovery configuration")
    champion = max(
        eligible,
        key=lambda row: (
            row["selection_score"],
            row["validation"]["mean_signed_bps"],
            -row["holding_seconds"],
        ),
    )
    return results, champion


def parse_model_spec(champion: Mapping[str, Any]) -> ModelSpec:
    return ModelSpec(
        family=str(champion["family"]),
        feature_block=str(champion["feature_block"]),
        parameter=float(champion["parameter"]),
    )


def exact_session_sign_flip_pvalue(session_means: Mapping[str, float]) -> float | None:
    values = np.asarray(list(session_means.values()), dtype=float)
    if len(values) == 0 or len(values) > 20:
        return None
    observed = float(np.mean(values))
    exceedances = 0
    total = 1 << len(values)
    for mask in range(total):
        signs = np.asarray([1.0 if mask & (1 << bit) else -1.0 for bit in range(len(values))])
        if float(np.mean(values * signs)) >= observed - 1e-12:
            exceedances += 1
    return exceedances / total


def session_bootstrap_interval(
    session_means: Mapping[str, float],
    *,
    repetitions: int = 5000,
) -> tuple[float | None, float | None]:
    values = np.asarray(list(session_means.values()), dtype=float)
    if len(values) == 0:
        return None, None
    rng = np.random.default_rng(RNG_SEED)
    draws = rng.choice(values, size=(repetitions, len(values)), replace=True).mean(axis=1)
    return float(np.quantile(draws, 0.025)), float(np.quantile(draws, 0.975))


def sealed_evaluation(
    mode: str,
    samples: SampleSet,
    split: Mapping[str, Sequence[str]],
    champion: Mapping[str, Any],
) -> tuple[dict[str, Any], np.ndarray, np.ndarray]:
    holding_seconds = int(champion["holding_seconds"])
    target = samples.outcomes[holding_seconds]
    fit_dates = date_mask(samples, (*split["train"], *split["validation"]))
    test_dates = date_mask(samples, split["test"])
    fit = fit_dates & np.isfinite(target)
    test = test_dates & np.isfinite(target)
    spec = parse_model_spec(champion)
    indices = feature_indices(samples.feature_names, spec.feature_block)
    indices = indices[np.any(np.isfinite(samples.X[fit][:, indices]), axis=0)]
    model = make_model(spec)
    model.fit(samples.X[fit][:, indices], target[fit])
    prediction = np.full(len(target), np.nan)
    prediction[fit] = model.predict(samples.X[fit][:, indices])
    prediction[test] = model.predict(samples.X[test][:, indices])
    threshold = float(
        np.quantile(np.abs(prediction[fit]), float(champion["confidence_quantile"]))
    )
    signal_indices = select_signals(
        prediction,
        target,
        samples,
        test,
        threshold=threshold,
        cooldown_seconds=holding_seconds,
    )
    metrics = evaluation_metrics(signal_indices, prediction, target, samples)
    ci_low, ci_high = session_bootstrap_interval(metrics["session_means_bps"])
    p_value = exact_session_sign_flip_pvalue(metrics["session_means_bps"])

    delays: dict[str, dict[str, Any]] = {}
    for delay in (5, 10, 15):
        key = f"{holding_seconds}s_delay_{delay}s"
        if key not in samples.delayed_outcomes:
            continue
        delayed_target = samples.delayed_outcomes[key]
        valid_signals = signal_indices[np.isfinite(delayed_target[signal_indices])]
        delays[f"{delay}s"] = evaluation_metrics(
            valid_signals, prediction, delayed_target, samples
        )

    neighbors: dict[str, dict[str, Any]] = {}
    holding_index = HOLDING_SECONDS.index(holding_seconds)
    for neighbor_index in (holding_index - 1, holding_index + 1):
        if not 0 <= neighbor_index < len(HOLDING_SECONDS):
            continue
        neighbor = HOLDING_SECONDS[neighbor_index]
        neighbor_target = samples.outcomes[neighbor]
        valid_signals = signal_indices[np.isfinite(neighbor_target[signal_indices])]
        neighbors[f"{neighbor}s"] = evaluation_metrics(
            valid_signals, prediction, neighbor_target, samples
        )

    signal_rows = []
    for index in signal_indices:
        signal_rows.append(
            {
                "session_date": str(samples.session_dates[index]),
                "decision_at": datetime.fromtimestamp(int(samples.epoch_seconds[index]), tz=UTC).isoformat(),
                "direction": "up" if prediction[index] > 0 else "down",
                "prediction_bps": float(prediction[index]),
                "realized_signed_bps": float(np.sign(prediction[index]) * target[index]),
            }
        )
    result = {
        "mode": mode,
        "holding_seconds": holding_seconds,
        "model": spec.name,
        "feature_block": spec.feature_block,
        "confidence_quantile": champion["confidence_quantile"],
        "frozen_threshold_bps": threshold,
        "metrics": metrics,
        "session_bootstrap_95_bps": [ci_low, ci_high],
        "exact_one_sided_session_sign_flip_p": p_value,
        "execution_delay_robustness": delays,
        "neighbor_horizon_robustness": neighbors,
        "signals": signal_rows,
    }
    return result, prediction, signal_indices


splits = {mode: chronological_split(samples) for mode, samples in sample_sets.items()}
search_results: dict[str, list[dict[str, Any]]] = {}
champions: dict[str, dict[str, Any]] = {}
sealed_results: dict[str, dict[str, Any]] = {}
sealed_predictions: dict[str, np.ndarray] = {}
sealed_signal_indices: dict[str, np.ndarray] = {}

for mode, samples in sample_sets.items():
    print(f"Searching {mode} without strategy-derived labels …")
    search_results[mode], champions[mode] = discovery_search(mode, samples, splits[mode])
    sealed_results[mode], sealed_predictions[mode], sealed_signal_indices[mode] = sealed_evaluation(
        mode, samples, splits[mode], champions[mode]
    )


def holm_adjust(values: Mapping[str, float | None]) -> dict[str, float | None]:
    valid = sorted(
        ((key, value) for key, value in values.items() if value is not None),
        key=lambda item: item[1],
    )
    adjusted: dict[str, float | None] = {key: None for key in values}
    running = 0.0
    count = len(valid)
    for rank, (key, value) in enumerate(valid):
        running = max(running, min(1.0, float(value) * (count - rank)))
        adjusted[key] = running
    return adjusted


adjusted_p = holm_adjust(
    {
        mode: finite(result["exact_one_sided_session_sign_flip_p"])
        for mode, result in sealed_results.items()
    }
)
for mode, result in sealed_results.items():
    result["holm_adjusted_p_across_modes"] = adjusted_p[mode]
    lower = finite(result["session_bootstrap_95_bps"][0])
    result["validated_directional_edge"] = bool(
        lower is not None
        and lower > 0
        and adjusted_p[mode] is not None
        and adjusted_p[mode] <= 0.10
    )
    print(mode, result["metrics"], {"holm_p": adjusted_p[mode], "validated": result["validated_directional_edge"]})

# %% [markdown]
# ## Exact-BBO option falsification
#
# 方向预测并不等于期权 edge。这里仍不调用现有策略：仅在 validation session 中比较
# 30/40/50/60 delta 的单腿和 5/10/15 点 debit vertical。合约在决策时用已知 delta 选择，
# 五秒后按逐腿 ask/bid 入场，在所选自由持有期末按逐腿 bid/ask 平仓；最后六个 session
# 只检验 validation 预选的一种结构。

# %%
OPTION_DELTA_TARGETS = (0.30, 0.40, 0.50, 0.60)
VERTICAL_WIDTHS = (5, 10, 15)
OPTION_ENTRY_DELAY_SECONDS = 5
FEE_DOLLARS_PER_LEG_SIDE = 1.50


def validation_champion_signals(
    samples: SampleSet,
    split: Mapping[str, Sequence[str]],
    champion: Mapping[str, Any],
) -> tuple[np.ndarray, np.ndarray]:
    holding_seconds = int(champion["holding_seconds"])
    target = samples.outcomes[holding_seconds]
    train_dates = date_mask(samples, split["train"])
    validation_dates = date_mask(samples, split["validation"])
    train = train_dates & np.isfinite(target)
    validation = validation_dates & np.isfinite(target)
    spec = parse_model_spec(champion)
    indices = feature_indices(samples.feature_names, spec.feature_block)
    indices = indices[np.any(np.isfinite(samples.X[train][:, indices]), axis=0)]
    model = make_model(spec)
    model.fit(samples.X[train][:, indices], target[train])
    prediction = np.full(len(target), np.nan)
    prediction[validation] = model.predict(samples.X[validation][:, indices])
    signal_indices = select_signals(
        prediction,
        target,
        samples,
        validation,
        threshold=float(champion["threshold_bps"]),
        cooldown_seconds=holding_seconds,
    )
    return prediction, signal_indices


def directional_signal_records(
    samples: SampleSet,
    prediction: np.ndarray,
    indices: np.ndarray,
    phase: str,
    *,
    direction_candidate: str | None = None,
    holding_seconds: int | None = None,
) -> list[dict[str, Any]]:
    return [
        {
            "signal_id": f"{phase}:{index}",
            "phase": phase,
            "session_date": str(samples.session_dates[index]),
            "decision_at": datetime.fromtimestamp(int(samples.epoch_seconds[index]), tz=UTC),
            "direction": "up" if prediction[index] > 0 else "down",
            "direction_candidate": direction_candidate,
            "holding_seconds": holding_seconds,
        }
        for index in indices
    ]


def option_quote_filter(provider: str, session_date: date, *, require_delta: bool) -> str:
    delta_filter = "AND delta IS NOT NULL AND abs(delta) BETWEEN 0.05 AND 0.85" if require_delta else ""
    return f"""
      SELECT
        instrument_id,
        strike,
        \"right\" AS option_right,
        received_at,
        source_at,
        quote_time,
        bid,
        ask,
        delta
      FROM read_parquet(?, hive_partitioning=true, union_by_name=true)
      WHERE provider = '{provider}'
        AND instrument_type = 'option'
        AND underlier = 'SPX'
        AND trading_class = 'SPXW'
        AND expiry = DATE '{session_date.isoformat()}'
        AND quality = 'live'
        AND lower(coalesce(market_data_type, 'live')) IN ('live', '1')
        AND bid >= 0 AND ask >= bid AND ask > 0
        AND source_at IS NOT NULL
        AND source_at >= received_at - INTERVAL 30 SECOND
        AND source_at <= received_at + INTERVAL 5 SECOND
        AND (quote_time IS NULL OR quote_time <= received_at + INTERVAL 5 SECOND)
        {delta_filter}
      QUALIFY row_number() OVER (
        PARTITION BY instrument_id, received_at
        ORDER BY source_at DESC, quote_time DESC NULLS LAST, bid DESC, ask DESC
      ) = 1
    """


def option_selection_states(
    provider: str,
    session_date: date,
    signals: Sequence[Mapping[str, Any]],
) -> list[dict[str, Any]]:
    path = QUOTE_ROOT / f"date={session_date.isoformat()}" / f"provider={provider}" / "hour=*" / "quotes.parquet"
    connection = duckdb.connect()
    try:
        connection.execute(
            "CREATE TEMP TABLE option_signals(signal_id VARCHAR, decision_at TIMESTAMPTZ, option_right VARCHAR)"
        )
        connection.executemany(
            "INSERT INTO option_signals VALUES (?, ?, ?)",
            [
                (
                    signal["signal_id"],
                    signal["decision_at"],
                    "C" if signal["direction"] == "up" else "P",
                )
                for signal in signals
            ],
        )
        quote_sql = option_quote_filter(provider, session_date, require_delta=True)
        rows = connection.execute(
            f"""
            WITH option_quotes AS ({quote_sql}),
            contracts AS (
              SELECT DISTINCT instrument_id, strike, option_right FROM option_quotes
            ),
            signal_contracts AS (
              SELECT signal.*, contract.instrument_id, contract.strike
              FROM option_signals AS signal
              JOIN contracts AS contract USING (option_right)
            )
            SELECT
              signal_contracts.signal_id,
              signal_contracts.decision_at,
              signal_contracts.option_right,
              signal_contracts.instrument_id,
              signal_contracts.strike,
              option_quotes.received_at,
              option_quotes.bid,
              option_quotes.ask,
              option_quotes.delta
            FROM signal_contracts
            ASOF LEFT JOIN option_quotes
              ON signal_contracts.instrument_id = option_quotes.instrument_id
             AND signal_contracts.decision_at >= option_quotes.received_at
            ORDER BY 1, 5
            """,
            [str(path)],
        ).fetchall()
    finally:
        connection.close()
    columns = (
        "signal_id",
        "decision_at",
        "option_right",
        "instrument_id",
        "strike",
        "received_at",
        "bid",
        "ask",
        "delta",
    )
    return [dict(zip(columns, row, strict=True)) for row in rows]


def candidate_contracts(
    provider: str,
    session_date: date,
    signals: Sequence[Mapping[str, Any]],
    *,
    maximum_age_seconds: int,
) -> list[dict[str, Any]]:
    states = option_selection_states(provider, session_date, signals)
    signal_lookup = {str(signal["signal_id"]): signal for signal in signals}
    grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for state in states:
        received_at = state["received_at"]
        delta = finite(state["delta"])
        if received_at is None or delta is None:
            continue
        age = (state["decision_at"] - received_at).total_seconds()
        if not 0 <= age <= maximum_age_seconds:
            continue
        grouped[str(state["signal_id"])].append(state)

    candidates: list[dict[str, Any]] = []
    for signal_id, signal_states in grouped.items():
        by_strike = {float(state["strike"]): state for state in signal_states}
        direction = str(signal_lookup[signal_id]["direction"])
        for delta_target in OPTION_DELTA_TARGETS:
            long_state = min(
                signal_states,
                key=lambda state: abs(abs(float(state["delta"])) - delta_target),
            )
            base = {
                **signal_lookup[signal_id],
                "delta_target": delta_target,
                "long_id": long_state["instrument_id"],
                "long_strike": float(long_state["strike"]),
                "selection_delta": float(long_state["delta"]),
                "selection_age_seconds": (
                    long_state["decision_at"] - long_state["received_at"]
                ).total_seconds(),
                "selection_relative_spread": (
                    (float(long_state["ask"]) - float(long_state["bid"]))
                    / float(long_state["ask"])
                ),
            }
            prefix = signal_lookup[signal_id].get("direction_candidate")
            candidate_prefix = f"{prefix}::" if prefix else ""
            candidates.append(
                {
                    **base,
                    "candidate": f"{candidate_prefix}delta_{delta_target:.2f}/outright",
                    "short_id": None,
                }
            )
            for width in VERTICAL_WIDTHS:
                short_strike = float(long_state["strike"]) + (width if direction == "up" else -width)
                short_state = by_strike.get(short_strike)
                if short_state is None:
                    continue
                candidates.append(
                    {
                        **base,
                        "candidate": f"{candidate_prefix}delta_{delta_target:.2f}/vertical_{width}",
                        "short_id": short_state["instrument_id"],
                        "short_strike": short_strike,
                        "selection_relative_spread": max(
                            float(base["selection_relative_spread"]),
                            (float(short_state["ask"]) - float(short_state["bid"]))
                            / float(short_state["ask"]),
                        ),
                    }
                )
    return candidates


def option_event_states(
    provider: str,
    session_date: date,
    candidates: Sequence[Mapping[str, Any]],
    *,
    holding_seconds: int,
) -> dict[tuple[str, str, str, str], dict[str, Any]]:
    events: list[tuple[int, str, str, str, str, datetime]] = []
    event_metadata: dict[int, tuple[str, str, str, str]] = {}
    event_id = 0
    for candidate in candidates:
        for leg_role in ("long", "short"):
            instrument_id = candidate[f"{leg_role}_id"]
            if instrument_id is None:
                continue
            for event_kind, offset in (
                ("entry", OPTION_ENTRY_DELAY_SECONDS),
                ("exit", int(candidate.get("holding_seconds") or holding_seconds)),
            ):
                key = (
                    str(candidate["signal_id"]),
                    str(candidate["candidate"]),
                    leg_role,
                    event_kind,
                )
                event_metadata[event_id] = key
                events.append(
                    (
                        event_id,
                        str(candidate["signal_id"]),
                        str(candidate["candidate"]),
                        leg_role,
                        str(instrument_id),
                        candidate["decision_at"] + timedelta(seconds=offset),
                    )
                )
                event_id += 1

    if not events:
        return {}
    path = QUOTE_ROOT / f"date={session_date.isoformat()}" / f"provider={provider}" / "hour=*" / "quotes.parquet"
    connection = duckdb.connect()
    try:
        columns = list(zip(*events, strict=True))
        connection.execute(
            """CREATE TEMP TABLE option_events AS SELECT
                 unnest(?::INTEGER[]) AS event_id,
                 unnest(?::VARCHAR[]) AS signal_id,
                 unnest(?::VARCHAR[]) AS candidate,
                 unnest(?::VARCHAR[]) AS leg_role,
                 unnest(?::VARCHAR[]) AS instrument_id,
                 unnest(?::TIMESTAMPTZ[]) AS event_at
               """,
            [list(column) for column in columns],
        )
        quote_sql = option_quote_filter(provider, session_date, require_delta=False)
        rows = connection.execute(
            f"""
            WITH option_quotes AS ({quote_sql})
            SELECT
              option_events.event_id,
              option_events.event_at,
              option_quotes.received_at,
              option_quotes.bid,
              option_quotes.ask
            FROM option_events
            ASOF LEFT JOIN option_quotes
              ON option_events.instrument_id = option_quotes.instrument_id
             AND option_events.event_at >= option_quotes.received_at
            ORDER BY 1
            """,
            [str(path)],
        ).fetchall()
    finally:
        connection.close()
    output: dict[tuple[str, str, str, str], dict[str, Any]] = {}
    for current_event_id, event_at, received_at, bid, ask in rows:
        output[event_metadata[int(current_event_id)]] = {
            "event_at": event_at,
            "received_at": received_at,
            "bid": finite(bid),
            "ask": finite(ask),
        }
    return output


def executable_option_trades(
    mode: str,
    signals: Sequence[Mapping[str, Any]],
    *,
    holding_seconds: int,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    provider = "schwab" if mode == "rth" else "ibkr"
    maximum_age = 5 if mode == "rth" else 15
    by_date: dict[date, list[Mapping[str, Any]]] = defaultdict(list)
    for signal in signals:
        by_date[date.fromisoformat(str(signal["session_date"]))].append(signal)

    trades: list[dict[str, Any]] = []
    candidate_rows = 0
    for ordinal, (session_date, daily_signals) in enumerate(sorted(by_date.items()), start=1):
        candidates = candidate_contracts(
            provider,
            session_date,
            daily_signals,
            maximum_age_seconds=maximum_age,
        )
        candidate_rows += len(candidates)
        event_states = option_event_states(
            provider,
            session_date,
            candidates,
            holding_seconds=holding_seconds,
        )
        for candidate in candidates:
            states: dict[tuple[str, str], dict[str, Any]] = {}
            valid = True
            for leg_role in ("long", "short"):
                if candidate[f"{leg_role}_id"] is None:
                    continue
                for event_kind in ("entry", "exit"):
                    key = (
                        str(candidate["signal_id"]),
                        str(candidate["candidate"]),
                        leg_role,
                        event_kind,
                    )
                    state = event_states.get(key)
                    if state is None or state["received_at"] is None:
                        valid = False
                        break
                    age = (state["event_at"] - state["received_at"]).total_seconds()
                    if (
                        not 0 <= age <= maximum_age
                        or state["bid"] is None
                        or state["ask"] is None
                        or state["ask"] < state["bid"]
                    ):
                        valid = False
                        break
                    states[(leg_role, event_kind)] = state
                if not valid:
                    break
            if not valid:
                continue
            long_entry = float(states[("long", "entry")]["ask"])
            long_exit = float(states[("long", "exit")]["bid"])
            if candidate["short_id"] is None:
                entry_debit = long_entry
                exit_credit = long_exit
                legs = 1
            else:
                short_entry = float(states[("short", "entry")]["bid"])
                short_exit = float(states[("short", "exit")]["ask"])
                entry_debit = long_entry - short_entry
                exit_credit = long_exit - short_exit
                legs = 2
            if entry_debit <= 0:
                continue
            fee_points = (legs * 2 * FEE_DOLLARS_PER_LEG_SIDE) / 100
            net_points = exit_credit - entry_debit - fee_points
            trades.append(
                {
                    **candidate,
                    "entry_debit_points": entry_debit,
                    "exit_credit_points": exit_credit,
                    "net_points": net_points,
                    "net_dollars": net_points * 100,
                    "net_return": net_points / entry_debit,
                    "fee_points": fee_points,
                }
            )
        print(f"  {mode} option BBO: {ordinal}/{len(by_date)} dates")
    diagnostics = {
        "provider": provider,
        "signals": len(signals),
        "candidate_contract_rows": candidate_rows,
        "executable_candidate_trades": len(trades),
        "maximum_bbo_age_seconds": maximum_age,
        "entry_delay_seconds": OPTION_ENTRY_DELAY_SECONDS,
    }
    return trades, diagnostics


def option_metrics(trades: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    if not trades:
        return {
            "trades": 0,
            "sessions": 0,
            "mean_net_points": None,
            "mean_net_dollars": None,
            "mean_net_return": None,
            "median_net_return": None,
            "hit_rate": None,
            "positive_session_rate": None,
            "session_lcb90_return": None,
            "session_mean_returns": {},
        }
    returns = np.asarray([float(trade["net_return"]) for trade in trades])
    points = np.asarray([float(trade["net_points"]) for trade in trades])
    grouped: dict[str, list[float]] = defaultdict(list)
    for trade in trades:
        grouped[str(trade["session_date"])].append(float(trade["net_return"]))
    session_means = {key: float(np.mean(values)) for key, values in grouped.items()}
    session_values = np.asarray(list(session_means.values()))
    standard_error = (
        float(np.std(session_values, ddof=1) / np.sqrt(len(session_values)))
        if len(session_values) > 1
        else float("inf")
    )
    return {
        "trades": len(trades),
        "sessions": len(grouped),
        "mean_net_points": float(np.mean(points)),
        "mean_net_dollars": float(np.mean(points) * 100),
        "mean_net_return": float(np.mean(returns)),
        "median_net_return": float(np.median(returns)),
        "hit_rate": float(np.mean(points > 0)),
        "positive_session_rate": float(np.mean(session_values > 0)),
        "session_lcb90_return": float(np.mean(session_values) - 1.645 * standard_error),
        "session_mean_returns": session_means,
    }


def select_option_candidate(
    trades: Sequence[Mapping[str, Any]],
    *,
    directional_signals: int,
) -> tuple[dict[str, Any] | None, list[dict[str, Any]]]:
    grouped: dict[str, list[Mapping[str, Any]]] = defaultdict(list)
    for trade in trades:
        grouped[str(trade["candidate"])].append(trade)
    results = []
    for candidate, candidate_trades in grouped.items():
        metrics = option_metrics(candidate_trades)
        coverage = len(candidate_trades) / max(directional_signals, 1)
        eligible = metrics["trades"] >= 25 and metrics["sessions"] >= 6 and coverage >= 0.50
        results.append(
            {
                "candidate": candidate,
                "coverage": coverage,
                "eligible": eligible,
                "selection_score": metrics["session_lcb90_return"] if eligible else None,
                "validation": metrics,
            }
        )
    eligible_rows = [row for row in results if row["eligible"]]
    champion = (
        max(eligible_rows, key=lambda row: float(row["selection_score"]))
        if eligible_rows
        else None
    )
    return champion, sorted(
        results,
        key=lambda row: float(row["selection_score"] or -float("inf")),
        reverse=True,
    )


option_results: dict[str, dict[str, Any]] = {}
for mode, samples in sample_sets.items():
    validation_prediction, validation_indices = validation_champion_signals(
        samples, splits[mode], champions[mode]
    )
    validation_signals = directional_signal_records(
        samples, validation_prediction, validation_indices, "validation"
    )
    test_signals = directional_signal_records(
        samples,
        sealed_predictions[mode],
        sealed_signal_indices[mode],
        "sealed_test",
    )
    holding_seconds = int(champions[mode]["holding_seconds"])
    combined_trades, diagnostics = executable_option_trades(
        mode,
        (*validation_signals, *test_signals),
        holding_seconds=holding_seconds,
    )
    validation_trades = [trade for trade in combined_trades if trade["phase"] == "validation"]
    test_trades = [trade for trade in combined_trades if trade["phase"] == "sealed_test"]
    option_champion, option_search = select_option_candidate(
        validation_trades,
        directional_signals=len(validation_signals),
    )
    if option_champion is None:
        option_results[mode] = {
            "status": "insufficient_executable_validation_coverage",
            "diagnostics": diagnostics,
            "validation_search": option_search,
            "sealed_test": None,
        }
        continue
    champion_name = str(option_champion["candidate"])
    champion_test_trades = [
        trade for trade in test_trades if trade["candidate"] == champion_name
    ]
    test_metrics = option_metrics(champion_test_trades)
    test_metrics["directional_signals"] = len(test_signals)
    test_metrics["executable_coverage"] = len(champion_test_trades) / max(len(test_signals), 1)
    ci_low, ci_high = session_bootstrap_interval(test_metrics["session_mean_returns"])
    p_value = exact_session_sign_flip_pvalue(test_metrics["session_mean_returns"])
    option_results[mode] = {
        "status": "tested",
        "diagnostics": diagnostics,
        "validation_champion": option_champion,
        "validation_search": option_search,
        "sealed_test": {
            "candidate": champion_name,
            "metrics": test_metrics,
            "session_bootstrap_95_return": [ci_low, ci_high],
            "exact_one_sided_session_sign_flip_p": p_value,
            "trades": champion_test_trades,
        },
    }

option_adjusted_p = holm_adjust(
    {
        mode: (
            finite(result["sealed_test"]["exact_one_sided_session_sign_flip_p"])
            if result["sealed_test"] is not None
            else None
        )
        for mode, result in option_results.items()
    }
)
for mode, result in option_results.items():
    if result["sealed_test"] is None:
        result["validated_executable_option_edge"] = False
        continue
    result["sealed_test"]["holm_adjusted_p_across_modes"] = option_adjusted_p[mode]
    lower = finite(result["sealed_test"]["session_bootstrap_95_return"][0])
    result["validated_executable_option_edge"] = bool(
        lower is not None
        and lower > 0
        and option_adjusted_p[mode] is not None
        and option_adjusted_p[mode] <= 0.10
    )
    print(
        mode,
        "option",
        result["sealed_test"]["candidate"],
        result["sealed_test"]["metrics"],
        {"holm_p": option_adjusted_p[mode], "validated": result["validated_executable_option_edge"]},
    )


def direction_candidate_key(row: Mapping[str, Any]) -> str:
    return (
        f"h{int(row['holding_seconds'])}_"
        f"{row['family']}_{row['feature_block']}_{float(row['parameter']):g}_"
        f"q{float(row['confidence_quantile']):g}"
    )


def best_direction_candidate_per_horizon(
    rows: Sequence[Mapping[str, Any]],
) -> list[dict[str, Any]]:
    output = []
    for holding_seconds in HOLDING_SECONDS:
        eligible = [
            row
            for row in rows
            if row["eligible"] and int(row["holding_seconds"]) == holding_seconds
        ]
        if eligible:
            output.append(max(eligible, key=lambda row: float(row["selection_score"])))
    positive = [row for row in output if float(row["selection_score"]) > 0]
    return sorted(
        positive,
        key=lambda row: float(row["selection_score"]),
        reverse=True,
    )[:5]


def select_joint_option_candidate(
    trades: Sequence[Mapping[str, Any]],
    signal_counts: Mapping[str, int],
) -> tuple[dict[str, Any] | None, list[dict[str, Any]]]:
    grouped: dict[str, list[Mapping[str, Any]]] = defaultdict(list)
    for trade in trades:
        grouped[str(trade["candidate"])].append(trade)
    rows = []
    for candidate, candidate_trades in grouped.items():
        direction_key = candidate.split("::", 1)[0]
        for maximum_spread in OPTION_MAX_RELATIVE_SPREADS:
            gated_trades = [
                trade
                for trade in candidate_trades
                if float(trade["selection_relative_spread"]) <= maximum_spread
            ]
            metrics = option_metrics(gated_trades)
            coverage = len(gated_trades) / max(signal_counts.get(direction_key, 0), 1)
            eligible = (
                metrics["trades"] >= 25
                and metrics["sessions"] >= 6
                and coverage >= 0.10
            )
            rows.append(
                {
                    "candidate": candidate,
                    "direction_candidate": direction_key,
                    "maximum_selection_relative_spread": maximum_spread,
                    "coverage": coverage,
                    "eligible": eligible,
                    "selection_score": metrics["session_lcb90_return"] if eligible else None,
                    "validation": metrics,
                }
            )
    eligible_rows = [row for row in rows if row["eligible"]]
    champion = (
        max(eligible_rows, key=lambda row: float(row["selection_score"]))
        if eligible_rows
        else None
    )
    return champion, sorted(
        rows,
        key=lambda row: (
            float(row["selection_score"])
            if row["selection_score"] is not None
            else -float("inf")
        ),
        reverse=True,
    )


joint_option_results: dict[str, dict[str, Any]] = {}
for mode, samples in sample_sets.items():
    direction_rows = best_direction_candidate_per_horizon(search_results[mode])
    direction_lookup = {direction_candidate_key(row): row for row in direction_rows}
    joint_validation_signals: list[dict[str, Any]] = []
    signal_counts: dict[str, int] = {}
    for row in direction_rows:
        key = direction_candidate_key(row)
        prediction, indices = validation_champion_signals(samples, splits[mode], row)
        records = directional_signal_records(
            samples,
            prediction,
            indices,
            f"joint_validation:{key}",
            direction_candidate=key,
            holding_seconds=int(row["holding_seconds"]),
        )
        signal_counts[key] = len(records)
        joint_validation_signals.extend(records)
    validation_trades, joint_diagnostics = executable_option_trades(
        mode,
        joint_validation_signals,
        holding_seconds=0,
    )
    joint_champion, joint_search = select_joint_option_candidate(
        validation_trades,
        signal_counts,
    )
    if joint_champion is None:
        joint_option_results[mode] = {
            "status": "insufficient_executable_validation_coverage",
            "direction_candidates": direction_rows,
            "diagnostics": joint_diagnostics,
            "validation_search": joint_search,
            "sealed_test": None,
            "validated_executable_option_edge": False,
        }
        continue

    selected_direction_key = str(joint_champion["direction_candidate"])
    selected_direction = direction_lookup[selected_direction_key]
    _, test_prediction, test_indices = sealed_evaluation(
        mode,
        samples,
        splits[mode],
        selected_direction,
    )
    test_signals = directional_signal_records(
        samples,
        test_prediction,
        test_indices,
        f"joint_sealed_test:{selected_direction_key}",
        direction_candidate=selected_direction_key,
        holding_seconds=int(selected_direction["holding_seconds"]),
    )
    test_trades, test_diagnostics = executable_option_trades(
        mode,
        test_signals,
        holding_seconds=int(selected_direction["holding_seconds"]),
    )
    selected_candidate = str(joint_champion["candidate"])
    selected_maximum_spread = float(joint_champion["maximum_selection_relative_spread"])
    selected_trades = [
        trade
        for trade in test_trades
        if trade["candidate"] == selected_candidate
        and float(trade["selection_relative_spread"]) <= selected_maximum_spread
    ]
    test_metrics = option_metrics(selected_trades)
    test_metrics["directional_signals"] = len(test_signals)
    test_metrics["executable_coverage"] = len(selected_trades) / max(len(test_signals), 1)
    ci_low, ci_high = session_bootstrap_interval(test_metrics["session_mean_returns"])
    p_value = exact_session_sign_flip_pvalue(test_metrics["session_mean_returns"])
    joint_option_results[mode] = {
        "status": "tested",
        "direction_candidates": direction_rows,
        "diagnostics": joint_diagnostics,
        "validation_champion": joint_champion,
        "validation_search": joint_search,
        "sealed_test": {
            "direction_candidate": selected_direction_key,
            "direction_spec": selected_direction,
            "candidate": selected_candidate,
            "diagnostics": test_diagnostics,
            "metrics": test_metrics,
            "session_bootstrap_95_return": [ci_low, ci_high],
            "exact_one_sided_session_sign_flip_p": p_value,
            "trades": selected_trades,
        },
    }

joint_adjusted_p = holm_adjust(
    {
        mode: (
            finite(result["sealed_test"]["exact_one_sided_session_sign_flip_p"])
            if result["sealed_test"] is not None
            else None
        )
        for mode, result in joint_option_results.items()
    }
)
for mode, result in joint_option_results.items():
    if result["sealed_test"] is None:
        continue
    result["sealed_test"]["holm_adjusted_p_across_modes"] = joint_adjusted_p[mode]
    lower = finite(result["sealed_test"]["session_bootstrap_95_return"][0])
    validation_score = finite(result["validation_champion"]["selection_score"])
    result["validated_executable_option_edge"] = bool(
        validation_score is not None
        and validation_score > 0
        and lower is not None
        and lower > 0
        and joint_adjusted_p[mode] is not None
        and joint_adjusted_p[mode] <= 0.10
    )
    print(
        mode,
        "joint option",
        result["validation_champion"]["candidate"],
        result["sealed_test"]["metrics"],
        {
            "validation_lcb": validation_score,
            "holm_p": joint_adjusted_p[mode],
            "validated": result["validated_executable_option_edge"],
        },
    )


# %% [markdown]
# ## Regime and event atlas: hindsight labels, causal event inputs
#
# 全天路径只用于事后标签，不进入当时的特征。振幅和路径形态分成两个轴；事件不使用固定
# 20 分钟结果，而使用由事件前波动缩放的对称 first-passage barrier，未触发则在收盘删失。
# 这部分只生成下一轮可冻结的研究假设：最后六个 session 已经被前面的方向研究看过，不能
# 再伪装成新的 sealed test。

# %%
IV_SURFACE_ROOT = Path(
    os.environ.get(
        "SPX_SPARK_IV_SURFACE_ROOT",
        "/srv/data/spx-spark/data/features/iv_surface",
    )
)
EVENT_DECISION_SECONDS = 60
EVENT_COOLDOWN_SECONDS = 15 * 60
DAY_LOW_RANGE_POINTS = 50.0
DAY_HIGH_RANGE_POINTS = 80.0
FORWARD_CONTRACT_VERSION = "raw_tick_event_forward.v2"
FORWARD_START_DATE = date(2026, 8, 19)
FORWARD_HYPOTHESES = {
    "right_pullback_resume": {
        "candidate": "right_pullback_resume::delta_0.60/vertical_15",
        "maximum_selection_relative_spread": 0.05,
    },
}
FORWARD_MIN_COMPLETE_SESSIONS = 20
FORWARD_MIN_EVENT_SESSIONS = 8
FORWARD_MIN_TRADES = 30
FORWARD_MIN_EXECUTABLE_COVERAGE = 0.20
EVENT_QUALIFIER_FIELDS = (
    "session_progress",
    "impulse_15m_scale",
    "resume_60s_scale",
    "efficiency_300s",
    "efficiency_600s",
    "rv_rate_60_to_600",
    "directional_breadth_60s",
    "directional_breadth_300s",
    "inverse_vix_alignment_60s",
    "directional_es_ofi_60s",
)
EVENT_LOCATION_FIELDS = (
    "surface_as_of",
    "surface_age_seconds",
    "gex_between_walls",
    "gex_directional_room_points",
    "gex_directional_room_scale",
    "gex_gamma_state",
)
EVENT_QUALIFIER_DEFINITIONS = {
    "unfiltered": "all causal pullback/resume events",
    "breadth_60_ge_0.6": "directional cross-asset breadth over 60s >= 0.6",
    "breadth_300_ge_0.6": "directional cross-asset breadth over 300s >= 0.6",
    "efficiency_300_ge_0.25": "causal SPX path efficiency over 300s >= 0.25",
    "efficiency_600_ge_0.25": "causal SPX path efficiency over 600s >= 0.25",
    "rv_rate_ratio_ge_1.15": (
        "causal per-unit-time SPX rv(60s) / rv(600s) >= 1.15"
    ),
    "rv_rate_ratio_le_0.85": (
        "causal per-unit-time SPX rv(60s) / rv(600s) <= 0.85"
    ),
    "es_ofi_aligned": "direction-adjusted ES normalized OFI over 60s > 0",
    "vix_inverse_aligned": "VIX 60s move is inverse-aligned with SPX direction",
    "first_session_third": "causal session progress <= one third",
    "last_session_third": "causal session progress >= two thirds",
    "impulse_scale_ge_1.5": "directional 15m impulse >= 1.5 local scales",
}


def _finite_prices_at_stride(
    grid: SessionGrid,
    contract: ModeContract,
    seconds: int,
) -> tuple[np.ndarray, np.ndarray]:
    stride = seconds // BUCKET_SECONDS
    indices = np.arange(stride - 1, len(grid.epoch_seconds), stride, dtype=int)
    prices = grid.instruments[contract.target].price[indices]
    valid = np.isfinite(prices)
    return indices[valid], prices[valid]


def day_path_record(grid: SessionGrid, contract: ModeContract) -> dict[str, Any] | None:
    indices, prices = _finite_prices_at_stride(grid, contract, 300)
    if len(prices) < 70:
        return None
    open_price, close_price = float(prices[0]), float(prices[-1])
    high, low = float(np.max(prices)), float(np.min(prices))
    day_range = high - low
    if day_range <= 0:
        return None
    net = close_price - open_price
    direction = 1.0 if net >= 0 else -1.0
    increments = np.diff(prices)
    displacement_efficiency = abs(net) / day_range
    path_efficiency_value = abs(net) / max(float(np.sum(np.abs(increments))), 1e-9)
    close_location = (close_price - low) / day_range
    close_directional_extreme = close_location if direction > 0 else 1.0 - close_location
    if direction > 0:
        running = np.maximum.accumulate(prices)
        adverse = float(np.max(running - prices))
    else:
        running = np.minimum.accumulate(prices)
        adverse = float(np.max(prices - running))
    adverse_fraction = adverse / day_range
    nonzero = increments[np.abs(increments) > 1e-9]
    turns = int(np.sum(nonzero[:-1] * nonzero[1:] < 0)) if len(nonzero) > 1 else 0

    amplitude = (
        "low"
        if day_range <= DAY_LOW_RANGE_POINTS
        else "high"
        if day_range >= DAY_HIGH_RANGE_POINTS
        else "mid"
    )
    # Interpretable path labels are frozen here and used only as ex-post outcomes.
    # Ambiguous days remain mixed rather than being forced into trend/range.
    if (
        displacement_efficiency >= 0.65
        and close_directional_extreme >= 0.75
        and adverse_fraction <= 0.60
    ):
        path_kind = "trend"
    elif displacement_efficiency <= 0.35 and path_efficiency_value <= 0.10:
        path_kind = "range"
    else:
        path_kind = "mixed"
    return {
        "session_date": grid.session_date.isoformat(),
        "mode": contract.mode,
        "amplitude_axis": amplitude,
        "path_axis": path_kind,
        "direction": "up" if direction > 0 else "down",
        "range_points": day_range,
        "net_points": net,
        "displacement_efficiency": displacement_efficiency,
        "path_efficiency": path_efficiency_value,
        "close_directional_extreme": close_directional_extreme,
        "max_adverse_fraction": adverse_fraction,
        "five_minute_turns": turns,
    }


def hindsight_trend_launch(
    grid: SessionGrid,
    day: Mapping[str, Any],
    contract: ModeContract,
) -> dict[str, Any] | None:
    if day.get("path_axis") != "trend":
        return None
    price = grid.instruments[contract.target].price
    direction = 1.0 if day.get("direction") == "up" else -1.0
    day_range = float(day["range_points"])
    target_distance = 0.50 * day_range
    invalidation_distance = 0.20 * day_range
    start = 30 * 60 // BUCKET_SECONDS
    end = len(price) - 1
    for index in range(start, end, EVENT_DECISION_SECONDS // BUCKET_SECONDS):
        if not math.isfinite(float(price[index])):
            continue
        future = direction * (price[index + 1 :] - price[index])
        finite_future = np.flatnonzero(np.isfinite(future))
        if len(finite_future) == 0:
            continue
        target_hits = finite_future[future[finite_future] >= target_distance]
        if len(target_hits) == 0:
            continue
        target_offset = int(target_hits[0])
        prefix = future[: target_offset + 1]
        if np.nanmin(prefix) < -invalidation_distance:
            continue
        target_index = index + 1 + target_offset
        return {
            "session_date": grid.session_date.isoformat(),
            "direction": day["direction"],
            "launch_at": datetime.fromtimestamp(
                int(grid.epoch_seconds[index]), tz=UTC
            ).isoformat(),
            "launch_index": index,
            "target_at": datetime.fromtimestamp(
                int(grid.epoch_seconds[target_index]), tz=UTC
            ).isoformat(),
            "target_distance_points": target_distance,
            "invalidation_distance_points": invalidation_distance,
        }
    return None


def load_rth_surface_frames(session_date: date) -> list[dict[str, Any]]:
    frames: list[dict[str, Any]] = []
    root = IV_SURFACE_ROOT / f"date={session_date.isoformat()}"
    for path in sorted(root.glob("hour=*/snapshots.jsonl")):
        try:
            lines = path.read_text(encoding="utf-8").splitlines()
        except OSError:
            continue
        for line in lines:
            try:
                payload = json.loads(line)
                as_of = datetime.fromisoformat(str(payload["as_of"])).astimezone(UTC)
            except (json.JSONDecodeError, KeyError, TypeError, ValueError):
                continue
            local = as_of.astimezone(NY)
            if local.date() != session_date or not time(9, 30) <= local.time() < time(16, 0):
                continue
            front_expiry = str(payload.get("front_expiry") or "")
            front = next(
                (
                    item
                    for item in payload.get("expiries") or ()
                    if isinstance(item, Mapping)
                    and str(item.get("expiry") or "") == front_expiry
                ),
                None,
            )
            if not isinstance(front, Mapping):
                continue
            frames.append(
                {
                    "as_of": as_of,
                    "underlier": finite(payload.get("underlier_price")),
                    "put_wall": finite(front.get("put_wall")),
                    "call_wall": finite(front.get("call_wall")),
                    "zero_gamma": finite(front.get("zero_gamma")),
                    "expected_move": finite(front.get("expected_move_points")),
                    "atm_iv": finite(front.get("atm_iv")),
                    "gamma_coverage": finite(front.get("gamma_coverage_ratio")),
                    "gamma_state": str(front.get("gamma_state") or "unknown"),
                }
            )
    frames.sort(key=lambda row: row["as_of"])
    deduped: dict[datetime, dict[str, Any]] = {row["as_of"]: row for row in frames}
    return [deduped[key] for key in sorted(deduped)]


def local_event_scale(
    price: np.ndarray,
    index: int,
    *,
    expected_move: float | None,
) -> float | None:
    lookback = 15 * 60 // BUCKET_SECONDS
    if index < lookback or not math.isfinite(float(price[index])):
        return None
    window = price[index - lookback : index + 1]
    valid = window[np.isfinite(window)]
    if len(valid) < 0.8 * len(window):
        return None
    increments = np.diff(valid)
    realized = float(np.sqrt(np.sum(increments * increments)))
    candidates = [2.5, 1.25 * realized]
    if expected_move is not None and expected_move > 0:
        candidates.append(0.10 * expected_move)
    return max(candidates)


def _causal_window_stats(
    values: np.ndarray,
    index: int,
    seconds: int,
) -> dict[str, float | None]:
    steps = seconds // BUCKET_SECONDS
    if index < steps:
        return {"return_bps": None, "rv_bps": None, "efficiency": None}
    window = values[index - steps : index + 1]
    adjacent = np.isfinite(window[:-1]) & np.isfinite(window[1:])
    if (
        not math.isfinite(float(window[0]))
        or not math.isfinite(float(window[-1]))
        or int(np.sum(adjacent)) < math.ceil(0.8 * steps)
    ):
        return {"return_bps": None, "rv_bps": None, "efficiency": None}
    with np.errstate(divide="ignore", invalid="ignore"):
        increments = np.log(window[1:][adjacent] / window[:-1][adjacent]) * 10000
        net = math.log(float(window[-1]) / float(window[0])) * 10000
    increments = increments[np.isfinite(increments)]
    if not math.isfinite(net) or len(increments) < math.ceil(0.8 * steps):
        return {"return_bps": None, "rv_bps": None, "efficiency": None}
    path = float(np.sum(np.abs(increments)))
    return {
        "return_bps": float(net),
        "rv_bps": float(np.sqrt(np.sum(increments * increments))),
        "efficiency": abs(float(net)) / path if path > 1e-12 else 0.0,
    }


def _directional_breadth(
    grid: SessionGrid,
    contract: ModeContract,
    index: int,
    direction: int,
    seconds: int,
) -> float | None:
    aligned: list[float] = []
    for instrument in ("future:ES", "future:NQ", "future:RTY", "future:YM", "equity:SPY"):
        if instrument not in contract.instruments:
            continue
        value = _causal_window_stats(
            grid.instruments[instrument].price,
            index,
            seconds,
        )["return_bps"]
        if value is not None:
            aligned.append(float(direction * np.sign(value)))
    return float(np.mean(aligned)) if len(aligned) >= 3 else None


def _directional_es_ofi(
    grid: SessionGrid,
    index: int,
    direction: int,
) -> float | None:
    if "future:ES" not in grid.instruments:
        return None
    values = grid.instruments["future:ES"]
    steps = 60 // BUCKET_SECONDS
    if index < steps:
        return None
    segment = slice(index - steps + 1, index + 1)
    depth = values.bid_size[segment] + values.ask_size[segment]
    finite_depth = depth[np.isfinite(depth) & (depth > 0)]
    quote_count = float(np.sum(values.quote_count[segment]))
    if len(finite_depth) < math.ceil(0.5 * steps) or quote_count <= 0:
        return None
    normalized = float(np.sum(values.ofi[segment])) / (
        float(np.mean(finite_depth)) * math.sqrt(max(quote_count, 1.0))
    )
    return float(direction * normalized) if math.isfinite(normalized) else None


def causal_event_features(
    grid: SessionGrid,
    contract: ModeContract,
    index: int,
    direction: int,
    scale: float,
) -> dict[str, float | None]:
    target = grid.instruments[contract.target].price
    stats_60 = _causal_window_stats(target, index, 60)
    stats_300 = _causal_window_stats(target, index, 300)
    stats_600 = _causal_window_stats(target, index, 600)
    rv_60 = stats_60["rv_bps"]
    rv_600 = stats_600["rv_bps"]
    vix_alignment = None
    if "index:VIX" in contract.instruments:
        vix_return = _causal_window_stats(
            grid.instruments["index:VIX"].price,
            index,
            60,
        )["return_bps"]
        if vix_return is not None:
            vix_alignment = float(-direction * np.sign(vix_return))
    window15 = 15 * 60 // BUCKET_SECONDS
    stride60 = 60 // BUCKET_SECONDS
    impulse = (
        direction * (float(target[index]) - float(target[index - window15])) / scale
        if index >= window15
        and math.isfinite(float(target[index]))
        and math.isfinite(float(target[index - window15]))
        else None
    )
    resume = (
        direction * (float(target[index]) - float(target[index - stride60])) / scale
        if index >= stride60
        and math.isfinite(float(target[index]))
        and math.isfinite(float(target[index - stride60]))
        else None
    )
    return {
        "session_progress": index / max(len(target) - 1, 1),
        "impulse_15m_scale": impulse,
        "resume_60s_scale": resume,
        "efficiency_300s": stats_300["efficiency"],
        "efficiency_600s": stats_600["efficiency"],
        "rv_rate_60_to_600": (
            float((rv_60 / rv_600) * math.sqrt(600 / 60))
            if rv_60 is not None and rv_600 is not None and rv_600 > 1e-12
            else None
        ),
        "directional_breadth_60s": _directional_breadth(
            grid,
            contract,
            index,
            direction,
            60,
        ),
        "directional_breadth_300s": _directional_breadth(
            grid,
            contract,
            index,
            direction,
            300,
        ),
        "inverse_vix_alignment_60s": vix_alignment,
        "directional_es_ofi_60s": _directional_es_ofi(grid, index, direction),
    }


def first_passage_event(
    grid: SessionGrid,
    *,
    contract: ModeContract,
    index: int,
    direction: int,
    scale: float,
    event_type: str,
    structure_kind: str | None = None,
    structure_level: float | None = None,
    surface: Mapping[str, Any] | None = None,
) -> dict[str, Any] | None:
    price = grid.instruments[contract.target].price
    if index + 1 >= len(price) or not math.isfinite(float(price[index])):
        return None
    entry = float(price[index])
    future = direction * (price[index + 1 :] - entry)
    finite_indices = np.flatnonzero(np.isfinite(future))
    if len(finite_indices) == 0:
        return None
    target_hits = finite_indices[future[finite_indices] >= scale]
    stop_hits = finite_indices[future[finite_indices] <= -scale]
    target_offset = int(target_hits[0]) if len(target_hits) else None
    stop_offset = int(stop_hits[0]) if len(stop_hits) else None
    if target_offset is None and stop_offset is None:
        outcome = "censored"
        offset = int(finite_indices[-1])
    elif stop_offset is None or (
        target_offset is not None and target_offset <= stop_offset
    ):
        outcome = "target_first"
        offset = int(target_offset)
    else:
        outcome = "stop_first"
        offset = int(stop_offset)
    exit_index = index + 1 + offset
    path = future[: offset + 1]
    signed_exit = float(direction * (price[exit_index] - entry))
    surface_row = dict(surface or {})
    return {
        "session_date": grid.session_date.isoformat(),
        "event_type": event_type,
        "direction": "up" if direction > 0 else "down",
        "decision_at": datetime.fromtimestamp(
            int(grid.epoch_seconds[index]), tz=UTC
        ).isoformat(),
        "outcome_at": datetime.fromtimestamp(
            int(grid.epoch_seconds[exit_index]), tz=UTC
        ).isoformat(),
        "outcome": outcome,
        "entry_spx": entry,
        "scale_points": scale,
        "signed_exit_points": signed_exit,
        "return_r": signed_exit / scale,
        "mfe_r": float(np.nanmax(path) / scale),
        "mae_r": float(np.nanmin(path) / scale),
        "time_to_event_seconds": int(
            grid.epoch_seconds[exit_index] - grid.epoch_seconds[index]
        ),
        "structure_kind": structure_kind,
        "structure_level": structure_level,
        "surface_as_of": (
            surface_row.get("as_of").isoformat()
            if isinstance(surface_row.get("as_of"), datetime)
            else None
        ),
        "surface_age_seconds": (
            int(grid.epoch_seconds[index] - surface_row["as_of"].timestamp())
            if isinstance(surface_row.get("as_of"), datetime)
            else None
        ),
        "expected_move_points": surface_row.get("expected_move"),
        "gamma_state": surface_row.get("gamma_state"),
        **causal_event_features(grid, contract, index, direction, scale),
    }


def _cooldown_append(
    events: list[dict[str, Any]],
    candidate: dict[str, Any] | None,
    next_allowed: dict[tuple[str, str], int],
    *,
    epoch: int,
) -> None:
    if candidate is None:
        return
    key = (str(candidate["event_type"]), str(candidate["direction"]))
    if epoch < next_allowed.get(key, -1):
        return
    events.append(candidate)
    next_allowed[key] = epoch + EVENT_COOLDOWN_SECONDS


def causal_price_events(
    grid: SessionGrid,
    contract: ModeContract,
) -> list[dict[str, Any]]:
    price = grid.instruments[contract.target].price
    stride = EVENT_DECISION_SECONDS // BUCKET_SECONDS
    window30 = 30 * 60 // BUCKET_SECONDS
    window15 = 15 * 60 // BUCKET_SECONDS
    events: list[dict[str, Any]] = []
    next_allowed: dict[tuple[str, str], int] = {}
    for index in range(window30, len(price), stride):
        if not math.isfinite(float(price[index])):
            continue
        scale = local_event_scale(price, index, expected_move=None)
        if scale is None:
            continue
        past = price[index - window30 : index]
        if np.sum(np.isfinite(past)) < 0.9 * len(past):
            continue
        prior_high, prior_low = float(np.nanmax(past)), float(np.nanmin(past))
        epoch = int(grid.epoch_seconds[index])
        one_minute = price[index] - price[index - stride]
        impulse15 = price[index] - price[index - window15]
        if price[index] > prior_high and price[index - stride] <= prior_high:
            _cooldown_append(
                events,
                first_passage_event(
                    grid,
                    contract=contract,
                    index=index,
                    direction=1,
                    scale=scale,
                    event_type="right_breakout",
                ),
                next_allowed,
                epoch=epoch,
            )
        elif price[index] < prior_low and price[index - stride] >= prior_low:
            _cooldown_append(
                events,
                first_passage_event(
                    grid,
                    contract=contract,
                    index=index,
                    direction=-1,
                    scale=scale,
                    event_type="right_breakout",
                ),
                next_allowed,
                epoch=epoch,
            )

        recent = price[index - window15 : index + 1]
        recent_high, recent_low = float(np.nanmax(recent)), float(np.nanmin(recent))
        if impulse15 >= scale:
            pullback = recent_high - float(price[index])
            if 0.25 * scale <= pullback <= 0.80 * scale and one_minute > 0:
                _cooldown_append(
                    events,
                    first_passage_event(
                        grid,
                        contract=contract,
                        index=index,
                        direction=1,
                        scale=scale,
                        event_type="right_pullback_resume",
                    ),
                    next_allowed,
                    epoch=epoch,
                )
        elif impulse15 <= -scale:
            pullback = float(price[index]) - recent_low
            if 0.25 * scale <= pullback <= 0.80 * scale and one_minute < 0:
                _cooldown_append(
                    events,
                    first_passage_event(
                        grid,
                        contract=contract,
                        index=index,
                        direction=-1,
                        scale=scale,
                        event_type="right_pullback_resume",
                    ),
                    next_allowed,
                    epoch=epoch,
                )
    return events


def causal_structure_events(
    grid: SessionGrid,
    frames: Sequence[Mapping[str, Any]],
) -> list[dict[str, Any]]:
    price = grid.instruments[RTH.target].price
    events: list[dict[str, Any]] = []
    next_allowed: dict[tuple[str, str], int] = {}
    for frame in frames:
        as_of = frame.get("as_of")
        if not isinstance(as_of, datetime):
            continue
        index = int(np.searchsorted(grid.epoch_seconds, int(as_of.timestamp()), side="left"))
        if index >= len(price) or index < 5 * 60 // BUCKET_SECONDS:
            continue
        expected_move = finite(frame.get("expected_move"))
        scale = local_event_scale(price, index, expected_move=expected_move)
        if scale is None:
            continue
        tolerance = max(2.5, 0.10 * expected_move) if expected_move else 2.5
        spot = float(price[index])
        prior = float(price[index - 5 * 60 // BUCKET_SECONDS])
        epoch = int(grid.epoch_seconds[index])
        for kind, direction, approaching in (
            ("call_wall", -1, spot > prior),
            ("put_wall", 1, spot < prior),
        ):
            level = finite(frame.get(kind))
            if level is None or abs(spot - level) > tolerance or not approaching:
                continue
            _cooldown_append(
                events,
                first_passage_event(
                    grid,
                    contract=RTH,
                    index=index,
                    direction=direction,
                    scale=scale,
                    event_type="left_gex_rejection",
                    structure_kind=kind,
                    structure_level=level,
                    surface=frame,
                ),
                next_allowed,
                epoch=epoch,
            )
        zero = finite(frame.get("zero_gamma"))
        if zero is not None and abs(spot - zero) <= tolerance:
            approach = spot - prior
            if abs(approach) > 1e-9:
                direction = -1 if approach > 0 else 1
                _cooldown_append(
                    events,
                    first_passage_event(
                        grid,
                        contract=RTH,
                        index=index,
                        direction=direction,
                        scale=scale,
                        event_type="left_zero_gamma_rejection",
                        structure_kind="zero_gamma",
                        structure_level=zero,
                        surface=frame,
                    ),
                    next_allowed,
                    epoch=epoch,
                )
    return events


def event_metrics(rows: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    resolved = [row for row in rows if row.get("outcome") != "censored"]
    by_session: dict[str, list[float]] = defaultdict(list)
    for row in resolved:
        by_session[str(row["session_date"])].append(float(row["return_r"]))
    session_means = {
        key: float(np.mean(values)) for key, values in sorted(by_session.items())
    }
    ci_low, ci_high = session_bootstrap_interval(session_means)
    return {
        "events": len(rows),
        "resolved": len(resolved),
        "censored": len(rows) - len(resolved),
        "sessions": len({str(row["session_date"]) for row in rows}),
        "target_first_rate": (
            float(np.mean([row.get("outcome") == "target_first" for row in resolved]))
            if resolved
            else None
        ),
        "mean_return_r": (
            float(np.mean([float(row["return_r"]) for row in resolved]))
            if resolved
            else None
        ),
        "median_time_to_event_seconds": (
            float(np.median([float(row["time_to_event_seconds"]) for row in resolved]))
            if resolved
            else None
        ),
        "positive_session_rate": (
            float(np.mean(np.asarray(list(session_means.values())) > 0))
            if session_means
            else None
        ),
        "session_bootstrap_95_mean_r": [ci_low, ci_high],
        "exact_one_sided_session_sign_flip_p": exact_session_sign_flip_pvalue(
            session_means
        ),
        "session_means_r": session_means,
    }


def cohort_name(session_date: str, mode: str) -> str:
    split = splits[mode]
    if session_date in split["train"]:
        return "development"
    if session_date in split["validation"]:
        return "retrospective_validation"
    return "previously_seen_tail"


def launch_feature_effects(
    grids: Sequence[SessionGrid],
    days: Mapping[str, Mapping[str, Any]],
    launches: Sequence[Mapping[str, Any]],
    contract: ModeContract,
) -> list[dict[str, Any]]:
    matrices: dict[str, tuple[np.ndarray, tuple[str, ...]]] = {
        grid.session_date.isoformat(): session_feature_matrix(grid, contract)
        for grid in grids
    }
    controls = [
        grid for grid in grids if days[grid.session_date.isoformat()]["path_axis"] != "trend"
    ]
    target = contract.target
    selected_names = (
        f"price/{target}/return_60s_bps",
        f"price/{target}/return_300s_bps",
        f"price/{target}/return_600s_bps",
        f"price/{target}/rv_60s_bps",
        f"price/{target}/rv_300s_bps",
        f"price/{target}/efficiency_300s",
        f"price/{target}/efficiency_600s",
        f"micro/{target}/normalized_ofi_60s",
        f"micro/{target}/normalized_ofi_300s",
        *(
            ("micro/future:ES/normalized_ofi_60s",)
            if target != "future:ES" and "future:ES" in contract.instruments
            else ()
        ),
        *tuple(
            name
            for instrument in contract.instruments
            if instrument != target
            for name in (
                f"cross/{instrument}/return_60s_bps",
                f"cross/{instrument}/return_300s_bps",
            )
        ),
        *(("cross/es_minus_spx_60s_bps",) if contract.mode == "rth" else ()),
        "state/rv_60_to_600",
    )
    launch_values: dict[str, list[float]] = defaultdict(list)
    control_values: dict[str, list[float]] = defaultdict(list)
    history_steps = 15 * 60 // BUCKET_SECONDS
    for launch in launches:
        session = str(launch["session_date"])
        matrix, names = matrices[session]
        name_index = {name: index for index, name in enumerate(names)}
        index = int(launch["launch_index"])
        if index < history_steps:
            continue
        direction = 1.0 if launch["direction"] == "up" else -1.0
        clock_offset = index
        candidate_controls = [
            other
            for other in controls
            if clock_offset < len(other.epoch_seconds)
        ]
        if not candidate_controls:
            continue
        for name in selected_names:
            column = name_index.get(name)
            if column is None:
                continue
            value = matrix[index, column] - matrix[index - history_steps, column]
            if not math.isfinite(float(value)):
                continue
            directional = (
                "return_" in name
                or "normalized_ofi" in name
                or "es_minus_spx" in name
            ) and "index:VIX" not in name
            launch_values[name].append(float(value) * (direction if directional else 1.0))
            nearest: list[float] = []
            for other in candidate_controls:
                other_matrix, other_names = matrices[other.session_date.isoformat()]
                try:
                    other_column = other_names.index(name)
                except ValueError:
                    continue
                other_value = (
                    other_matrix[clock_offset, other_column]
                    - other_matrix[clock_offset - history_steps, other_column]
                )
                if math.isfinite(float(other_value)):
                    nearest.append(
                        float(other_value) * (direction if directional else 1.0)
                    )
            if nearest:
                control_values[name].append(float(np.median(nearest)))
    effects: list[dict[str, Any]] = []
    for name in selected_names:
        launch_array = np.asarray(launch_values.get(name) or (), dtype=float)
        control_array = np.asarray(control_values.get(name) or (), dtype=float)
        if len(launch_array) < 2 or len(control_array) < 2:
            continue
        pooled = float(
            np.sqrt((np.var(launch_array, ddof=1) + np.var(control_array, ddof=1)) / 2)
        )
        effect = (
            (float(np.mean(launch_array)) - float(np.mean(control_array))) / pooled
            if pooled > 1e-12
            else 0.0
        )
        effects.append(
            {
                "feature": name,
                "launches": len(launch_array),
                "launch_mean_change_15m": float(np.mean(launch_array)),
                "matched_clock_control_mean_change_15m": float(np.mean(control_array)),
                "standardized_difference": effect,
            }
        )
    return sorted(effects, key=lambda row: abs(row["standardized_difference"]), reverse=True)


rth_day_rows = [
    row
    for grid in session_grids["rth"]
    if (row := day_path_record(grid, RTH)) is not None
]
rth_days = {str(row["session_date"]): row for row in rth_day_rows}
rth_launches = [
    launch
    for grid in session_grids["rth"]
    if (
        launch := hindsight_trend_launch(
            grid,
            rth_days[grid.session_date.isoformat()],
            RTH,
        )
    )
    is not None
]
rth_surface_frames = {
    grid.session_date.isoformat(): load_rth_surface_frames(grid.session_date)
    for grid in session_grids["rth"]
}
rth_events: list[dict[str, Any]] = []
for grid in session_grids["rth"]:
    session = grid.session_date.isoformat()
    day = rth_days[session]
    rows = [
        *causal_price_events(grid, RTH),
        *causal_structure_events(grid, rth_surface_frames[session]),
    ]
    for row in rows:
        row["amplitude_axis"] = day["amplitude_axis"]
        row["path_axis_hindsight"] = day["path_axis"]
        row["cohort"] = cohort_name(session, "rth")
    rth_events.extend(rows)

event_types = sorted({str(row["event_type"]) for row in rth_events})
event_cohorts = (
    "development",
    "retrospective_validation",
    "previously_seen_tail",
)
event_summary = {
    event_type: {
        cohort: event_metrics(
            [
                row
                for row in rth_events
                if row["event_type"] == event_type and row["cohort"] == cohort
            ]
        )
        for cohort in event_cohorts
    }
    for event_type in event_types
}

amplitude_counts = {
    key: sum(row["amplitude_axis"] == key for row in rth_day_rows)
    for key in ("low", "mid", "high")
}
path_counts = {
    key: sum(row["path_axis"] == key for row in rth_day_rows)
    for key in ("trend", "range", "mixed")
}
surface_total = sum(len(rows) for rows in rth_surface_frames.values())
surface_wall = sum(
    row.get("put_wall") is not None and row.get("call_wall") is not None
    for rows in rth_surface_frames.values()
    for row in rows
)
surface_zero = sum(
    row.get("zero_gamma") is not None
    for rows in rth_surface_frames.values()
    for row in rows
)

gth_day_rows = [
    row
    for grid in session_grids["gth"]
    if (row := day_path_record(grid, GTH)) is not None
]
gth_days = {str(row["session_date"]): row for row in gth_day_rows}
gth_launches = [
    launch
    for grid in session_grids["gth"]
    if (
        launch := hindsight_trend_launch(
            grid,
            gth_days[grid.session_date.isoformat()],
            GTH,
        )
    )
    is not None
]
gth_events: list[dict[str, Any]] = []
for grid in session_grids["gth"]:
    session = grid.session_date.isoformat()
    day = gth_days[session]
    for row in causal_price_events(grid, GTH):
        row["amplitude_axis"] = day["amplitude_axis"]
        row["path_axis_hindsight"] = day["path_axis"]
        row["cohort"] = cohort_name(session, "gth")
        gth_events.append(row)

gth_event_types = sorted({str(row["event_type"]) for row in gth_events})
gth_event_summary = {
    event_type: {
        cohort: event_metrics(
            [
                row
                for row in gth_events
                if row["event_type"] == event_type and row["cohort"] == cohort
            ]
        )
        for cohort in event_cohorts
    }
    for event_type in gth_event_types
}
gth_amplitude_counts = {
    key: sum(row["amplitude_axis"] == key for row in gth_day_rows)
    for key in ("low", "mid", "high")
}
gth_path_counts = {
    key: sum(row["path_axis"] == key for row in gth_day_rows)
    for key in ("trend", "range", "mixed")
}


def event_signal_records(
    rows: Sequence[Mapping[str, Any]],
    *,
    event_type: str,
    cohort: str,
) -> list[dict[str, Any]]:
    output: list[dict[str, Any]] = []
    for ordinal, row in enumerate(rows):
        if row.get("event_type") != event_type or row.get("cohort") != cohort:
            continue
        try:
            decision_at = datetime.fromisoformat(str(row["decision_at"])).astimezone(UTC)
        except (KeyError, TypeError, ValueError):
            continue
        holding_seconds = int(row.get("time_to_event_seconds") or 0)
        if holding_seconds < OPTION_ENTRY_DELAY_SECONDS:
            continue
        output.append(
            {
                "signal_id": f"event:{event_type}:{cohort}:{ordinal}",
                "phase": cohort,
                "session_date": str(row["session_date"]),
                "decision_at": decision_at,
                "direction": str(row["direction"]),
                "direction_candidate": event_type,
                "holding_seconds": holding_seconds,
                "event_outcome": row.get("outcome"),
                "event_return_r": row.get("return_r"),
                "structure_kind": row.get("structure_kind"),
                **{field: row.get(field) for field in EVENT_QUALIFIER_FIELDS},
                **{field: row.get(field) for field in EVENT_LOCATION_FIELDS},
            }
        )
    return output


def event_qualifier_passes(row: Mapping[str, Any], qualifier: str) -> bool:
    if qualifier == "unfiltered":
        return True
    field_thresholds = {
        "breadth_60_ge_0.6": ("directional_breadth_60s", ">=", 0.60),
        "breadth_300_ge_0.6": ("directional_breadth_300s", ">=", 0.60),
        "efficiency_300_ge_0.25": ("efficiency_300s", ">=", 0.25),
        "efficiency_600_ge_0.25": ("efficiency_600s", ">=", 0.25),
        "rv_rate_ratio_ge_1.15": ("rv_rate_60_to_600", ">=", 1.15),
        "rv_rate_ratio_le_0.85": ("rv_rate_60_to_600", "<=", 0.85),
        "es_ofi_aligned": ("directional_es_ofi_60s", ">", 0.0),
        "vix_inverse_aligned": ("inverse_vix_alignment_60s", ">", 0.0),
        "first_session_third": ("session_progress", "<=", 1 / 3),
        "last_session_third": ("session_progress", ">=", 2 / 3),
        "impulse_scale_ge_1.5": ("impulse_15m_scale", ">=", 1.50),
    }
    if qualifier not in field_thresholds:
        raise KeyError(qualifier)
    field, operator, threshold = field_thresholds[qualifier]
    value = finite(row.get(field))
    if value is None:
        return False
    if operator == ">=":
        return value >= threshold
    if operator == "<=":
        return value <= threshold
    return value > threshold


def causal_qualifier_research(
    development_signals: Sequence[Mapping[str, Any]],
    validation_signals: Sequence[Mapping[str, Any]],
    tail_signals: Sequence[Mapping[str, Any]],
    development_trades: Sequence[Mapping[str, Any]],
    validation_trades: Sequence[Mapping[str, Any]],
    tail_trades: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    contract = FORWARD_HYPOTHESES["right_pullback_resume"]
    candidate = str(contract["candidate"])
    maximum_spread = float(contract["maximum_selection_relative_spread"])

    def fixed_trades(
        rows: Sequence[Mapping[str, Any]],
        qualifier: str,
    ) -> list[Mapping[str, Any]]:
        selected: list[Mapping[str, Any]] = []
        for row in rows:
            spread = finite(row.get("selection_relative_spread"))
            if (
                row.get("candidate") == candidate
                and spread is not None
                and spread <= maximum_spread
                and event_qualifier_passes(row, qualifier)
            ):
                selected.append(row)
        return selected

    search: list[dict[str, Any]] = []
    for qualifier, definition in EVENT_QUALIFIER_DEFINITIONS.items():
        signals = [
            row
            for row in development_signals
            if event_qualifier_passes(row, qualifier)
        ]
        trades = fixed_trades(development_trades, qualifier)
        metrics = option_metrics(trades)
        coverage = len(trades) / max(len(signals), 1)
        eligible = (
            len(signals) >= 12
            and int(metrics["trades"]) >= 12
            and int(metrics["sessions"]) >= 6
            and coverage >= 0.50
        )
        search.append(
            {
                "qualifier": qualifier,
                "definition": definition,
                "signals": len(signals),
                "coverage": coverage,
                "eligible": eligible,
                "selection_score": (
                    metrics["session_lcb90_return"] if eligible else None
                ),
                "metrics": metrics,
            }
        )
    eligible_rows = [row for row in search if row["eligible"]]
    champion = (
        max(eligible_rows, key=lambda row: float(row["selection_score"]))
        if eligible_rows
        else None
    )
    ranked = sorted(
        search,
        key=lambda row: (
            float(row["selection_score"])
            if row["selection_score"] is not None
            else -float("inf")
        ),
        reverse=True,
    )
    if champion is None:
        return {
            "status": "no_eligible_development_qualifier",
            "candidate": candidate,
            "maximum_selection_relative_spread": maximum_spread,
            "predefined_single_variable_qualifiers": len(search),
            "development_search": ranked,
            "retrospective_strict_pass": False,
            "notification_authority": False,
        }

    qualifier = str(champion["qualifier"])

    def evaluate(
        signals: Sequence[Mapping[str, Any]],
        trades: Sequence[Mapping[str, Any]],
    ) -> dict[str, Any]:
        selected_signals = [
            row for row in signals if event_qualifier_passes(row, qualifier)
        ]
        selected_trades = fixed_trades(trades, qualifier)
        metrics = option_metrics(selected_trades)
        metrics["signals"] = len(selected_signals)
        metrics["executable_coverage"] = len(selected_trades) / max(
            len(selected_signals),
            1,
        )
        ci_low, ci_high = session_bootstrap_interval(
            metrics["session_mean_returns"]
        )
        return {
            "metrics": metrics,
            "session_bootstrap_95_return": [ci_low, ci_high],
            "exact_one_sided_session_sign_flip_p": exact_session_sign_flip_pvalue(
                metrics["session_mean_returns"]
            ),
        }

    validation = evaluate(validation_signals, validation_trades)
    tail = evaluate(tail_signals, tail_trades)
    validation_metrics = validation["metrics"]
    tail_metrics = tail["metrics"]
    validation_lower = finite(validation["session_bootstrap_95_return"][0])
    tail_lower = finite(tail["session_bootstrap_95_return"][0])
    retrospective_strict_pass = bool(
        float(champion["selection_score"]) > 0
        and int(validation_metrics["trades"]) >= 12
        and int(validation_metrics["sessions"]) >= 6
        and validation_lower is not None
        and validation_lower > 0
        and finite(validation["exact_one_sided_session_sign_flip_p"]) is not None
        and float(validation["exact_one_sided_session_sign_flip_p"]) <= 0.10
        and float(validation_metrics["positive_session_rate"] or 0) >= 0.60
        and int(tail_metrics["trades"]) >= 8
        and int(tail_metrics["sessions"]) >= 4
        and tail_lower is not None
        and tail_lower > 0
        and finite(tail["exact_one_sided_session_sign_flip_p"]) is not None
        and float(tail["exact_one_sided_session_sign_flip_p"]) <= 0.125
        and float(tail_metrics["positive_session_rate"] or 0) >= 0.60
    )
    return {
        "status": "retrospectively_tested",
        "candidate": candidate,
        "maximum_selection_relative_spread": maximum_spread,
        "predefined_single_variable_qualifiers": len(search),
        "development_champion": champion,
        "development_search": ranked,
        "retrospective_validation": validation,
        "previously_seen_tail": tail,
        "retrospective_strict_pass": retrospective_strict_pass,
        "notification_authority": False,
        "forward_contract_action": (
            "A retrospective pass would justify a separately frozen future contract, "
            "never a production signal. The existing v2 contract remains unchanged."
        ),
    }


def event_exact_bbo_validation(
    event_type: str,
    rows: Sequence[Mapping[str, Any]],
    *,
    mode: str,
) -> dict[str, Any]:
    development_signals = event_signal_records(
        rows,
        event_type=event_type,
        cohort="development",
    )
    validation_signals = event_signal_records(
        rows,
        event_type=event_type,
        cohort="retrospective_validation",
    )
    tail_signals = event_signal_records(
        rows,
        event_type=event_type,
        cohort="previously_seen_tail",
    )
    combined_trades, diagnostics = executable_option_trades(
        mode,
        (*development_signals, *validation_signals, *tail_signals),
        holding_seconds=0,
    )
    development_trades = [
        trade for trade in combined_trades if trade.get("phase") == "development"
    ]
    validation_trades = [
        trade
        for trade in combined_trades
        if trade.get("phase") == "retrospective_validation"
    ]
    tail_trades = [
        trade
        for trade in combined_trades
        if trade.get("phase") == "previously_seen_tail"
    ]
    champion, search = select_joint_option_candidate(
        development_trades,
        {event_type: len(development_signals)},
    )
    vertical_champion, vertical_search = select_joint_option_candidate(
        [
            trade
            for trade in development_trades
            if "/vertical_" in str(trade.get("candidate") or "")
        ],
        {event_type: len(development_signals)},
    )
    qualifier_research = (
        causal_qualifier_research(
            development_signals,
            validation_signals,
            tail_signals,
            development_trades,
            validation_trades,
            tail_trades,
        )
        if mode == "rth" and event_type == "right_pullback_resume"
        else None
    )
    if champion is None:
        return {
            "status": "no_eligible_exact_bbo_structure",
            "diagnostics": diagnostics,
            "development_signals": len(development_signals),
            "validation_signals": len(validation_signals),
            "tail_signals": len(tail_signals),
            "development_search": search[:24],
            "production_compatible_vertical": {
                "status": "no_eligible_vertical",
                "development_search": vertical_search[:24],
            },
            "causal_qualifier_research": qualifier_research,
            "previously_seen_tail": None,
            "promotion_eligible": False,
        }

    def evaluate_fixed(candidate: Mapping[str, Any] | None) -> dict[str, Any] | None:
        if candidate is None:
            return None
        selected_candidate = str(candidate["candidate"])
        maximum_spread = float(candidate["maximum_selection_relative_spread"])
        selected_validation = [
            trade
            for trade in validation_trades
            if trade["candidate"] == selected_candidate
            and float(trade["selection_relative_spread"]) <= maximum_spread
        ]
        selected_tail = [
            trade
            for trade in tail_trades
            if trade["candidate"] == selected_candidate
            and float(trade["selection_relative_spread"]) <= maximum_spread
        ]
        validation_metrics = option_metrics(selected_validation)
        validation_metrics["signals"] = len(validation_signals)
        validation_metrics["executable_coverage"] = len(selected_validation) / max(
            len(validation_signals), 1
        )
        validation_ci_low, validation_ci_high = session_bootstrap_interval(
            validation_metrics["session_mean_returns"]
        )
        validation_p = exact_session_sign_flip_pvalue(
            validation_metrics["session_mean_returns"]
        )
        tail_metrics = option_metrics(selected_tail)
        tail_metrics["signals"] = len(tail_signals)
        tail_metrics["executable_coverage"] = len(selected_tail) / max(
            len(tail_signals), 1
        )
        ci_low, ci_high = session_bootstrap_interval(
            tail_metrics["session_mean_returns"]
        )
        p_value = exact_session_sign_flip_pvalue(
            tail_metrics["session_mean_returns"]
        )
        return {
            "development_champion": candidate,
            "retrospective_validation": {
                "candidate": selected_candidate,
                "maximum_selection_relative_spread": maximum_spread,
                "metrics": validation_metrics,
                "session_bootstrap_95_return": [
                    validation_ci_low,
                    validation_ci_high,
                ],
                "exact_one_sided_session_sign_flip_p": validation_p,
            },
            "previously_seen_tail": {
                "candidate": selected_candidate,
                "maximum_selection_relative_spread": maximum_spread,
                "metrics": tail_metrics,
                "session_bootstrap_95_return": [ci_low, ci_high],
                "exact_one_sided_session_sign_flip_p": p_value,
                "trades": selected_tail,
            },
        }

    unrestricted = evaluate_fixed(champion)
    assert unrestricted is not None
    vertical = evaluate_fixed(vertical_champion)
    vertical_payload = (
        {
            "status": "retrospectively_tested",
            **vertical,
            "development_search": vertical_search[:24],
            "development_supported": (
                finite(vertical_champion.get("selection_score")) is not None
                and float(vertical_champion["selection_score"]) > 0
            ),
            "production_contract_compatible": True,
        }
        if vertical is not None and vertical_champion is not None
        else {
            "status": "no_eligible_vertical",
            "development_search": vertical_search[:24],
            "development_supported": False,
            "production_contract_compatible": True,
        }
    )
    return {
        "status": "retrospectively_tested",
        "diagnostics": diagnostics,
        "development_signals": len(development_signals),
        "validation_signals": len(validation_signals),
        "tail_signals": len(tail_signals),
        "development_champion": champion,
        "development_search": search[:24],
        "retrospective_validation": unrestricted["retrospective_validation"],
        "previously_seen_tail": unrestricted["previously_seen_tail"],
        "production_compatible_vertical": vertical_payload,
        "causal_qualifier_research": qualifier_research,
        "promotion_eligible": False,
        "promotion_blocker": (
            "Tail sessions were already consumed by prior research; collect new forward "
            "sessions after freezing this event/option contract."
        ),
    }


event_option_validation = {
    event_type: event_exact_bbo_validation(event_type, rth_events, mode="rth")
    for event_type in event_types
}
gth_event_option_validation = {
    event_type: event_exact_bbo_validation(event_type, gth_events, mode="gth")
    for event_type in gth_event_types
}


def _forward_signal_records(
    rows: Sequence[Mapping[str, Any]],
    *,
    event_type: str,
) -> list[dict[str, Any]]:
    output: list[dict[str, Any]] = []
    for ordinal, row in enumerate(rows):
        session_date = date.fromisoformat(str(row["session_date"]))
        if session_date < FORWARD_START_DATE or row.get("event_type") != event_type:
            continue
        decision_at = datetime.fromisoformat(str(row["decision_at"])).astimezone(UTC)
        holding_seconds = int(row.get("time_to_event_seconds") or 0)
        if holding_seconds < OPTION_ENTRY_DELAY_SECONDS:
            continue
        output.append(
            {
                "signal_id": f"forward:{event_type}:{session_date}:{ordinal}",
                "phase": "strict_forward",
                "session_date": session_date.isoformat(),
                "decision_at": decision_at,
                "direction": str(row["direction"]),
                "direction_candidate": event_type,
                "holding_seconds": holding_seconds,
                "event_outcome": row.get("outcome"),
                "event_return_r": row.get("return_r"),
            }
        )
    return output


forward_contract_payload = {
    "schema_version": FORWARD_CONTRACT_VERSION,
    "frozen_at": "2026-08-19T07:21:49+00:00",
    "forward_start_session": FORWARD_START_DATE.isoformat(),
    "scope": (
        "RTH SPX/SPXW production-compatible Debit Vertical research only; "
        "no strategy_decision or notification authority"
    ),
    "data_contract": {
        "input": "causally available five-second SPX quote-update grid",
        "decision_frequency_seconds": EVENT_DECISION_SECONDS,
        "cooldown_seconds_by_event_and_direction": EVENT_COOLDOWN_SECONDS,
        "knowledge_guard": (
            "quality=live; source/quote clocks not from future; exact BBO fresh at both "
            "selection and fill"
        ),
    },
    "event_contract": {
        "right_breakout": (
            "cross prior causal 30-minute high/low on a 60-second decision point"
        ),
        "right_pullback_resume": (
            "15-minute impulse >= local scale; 0.25-0.80 scale pullback; latest "
            "one-minute move resumes impulse direction"
        ),
        "local_scale": (
            "max(2.5 points, 1.25*causal 15-minute realized path volatility); "
            "no fixed holding horizon"
        ),
        "outcome": (
            "symmetric first-passage target/stop at one local scale; otherwise censor "
            "at RTH close"
        ),
    },
    "option_contract": {
        "hypotheses": FORWARD_HYPOTHESES,
        "selection": "nearest absolute delta using information available at decision_at",
        "entry_delay_seconds": OPTION_ENTRY_DELAY_SECONDS,
        "entry": "long ask minus short bid at decision+5s",
        "exit": "long bid minus short ask at first-passage outcome time",
        "fee_dollars_per_leg_side": FEE_DOLLARS_PER_LEG_SIDE,
        "mid_allowed": False,
    },
    "promotion_gate": {
        "minimum_complete_forward_sessions": FORWARD_MIN_COMPLETE_SESSIONS,
        "minimum_event_sessions_per_hypothesis": FORWARD_MIN_EVENT_SESSIONS,
        "minimum_exact_bbo_trades_per_hypothesis": FORWARD_MIN_TRADES,
        "minimum_executable_coverage": FORWARD_MIN_EXECUTABLE_COVERAGE,
        "mean_net_dollars": "> 0",
        "session_bootstrap_95_return_lower": "> 0",
        "holm_adjusted_one_sided_session_sign_flip_p": "<= 0.05",
        "positive_session_rate": ">= 0.60",
    },
    "explicit_rejections": {
        "left_gex_rejection": "negative exact-BBO validation/tail economics",
        "left_zero_gamma_rejection": "negative exact-BBO validation/tail economics",
        "right_breakout": (
            "production-compatible vertical has negative development session LCB"
        ),
        "gth_right_breakout": "negative validation and tail IBKR exact-BBO economics",
        "gth_right_pullback_resume": (
            "negative validation and tail IBKR exact-BBO economics"
        ),
    },
    "integration_constraint": (
        "A future pass may authorize only a Debit Vertical candidate generated from the "
        "same raw event contract; adding RIGHT_PULLBACK_RESUME as a human-visible setup "
        "still requires explicit candidate-space approval."
    ),
}
forward_contract_hash = "sha256:" + hashlib.sha256(
    json.dumps(
        json_safe(forward_contract_payload),
        allow_nan=False,
        separators=(",", ":"),
        sort_keys=True,
    ).encode()
).hexdigest()
frozen_forward_contract = {
    **forward_contract_payload,
    "contract_hash": forward_contract_hash,
}

forward_complete_sessions = sorted(
    grid.session_date.isoformat()
    for grid in session_grids["rth"]
    if grid.session_date >= FORWARD_START_DATE
)
forward_results: dict[str, dict[str, Any]] = {}
for event_type, contract in FORWARD_HYPOTHESES.items():
    signals = _forward_signal_records(rth_events, event_type=event_type)
    trades, diagnostics = executable_option_trades("rth", signals, holding_seconds=0)
    selected = [
        trade
        for trade in trades
        if trade["candidate"] == contract["candidate"]
        and float(trade["selection_relative_spread"])
        <= float(contract["maximum_selection_relative_spread"])
    ]
    metrics = option_metrics(selected)
    metrics["signals"] = len(signals)
    metrics["executable_coverage"] = len(selected) / max(len(signals), 1)
    ci_low, ci_high = session_bootstrap_interval(metrics["session_mean_returns"])
    p_value = exact_session_sign_flip_pvalue(metrics["session_mean_returns"])
    forward_results[event_type] = {
        "candidate": contract["candidate"],
        "maximum_selection_relative_spread": contract[
            "maximum_selection_relative_spread"
        ],
        "diagnostics": diagnostics,
        "metrics": metrics,
        "session_bootstrap_95_return": [ci_low, ci_high],
        "exact_one_sided_session_sign_flip_p": p_value,
        "promotion_eligible": False,
    }

forward_adjusted_p = holm_adjust(
    {
        event_type: finite(result["exact_one_sided_session_sign_flip_p"])
        for event_type, result in forward_results.items()
    }
)
for event_type, result in forward_results.items():
    metrics = result["metrics"]
    ci_low = finite(result["session_bootstrap_95_return"][0])
    adjusted = forward_adjusted_p[event_type]
    gates = {
        "complete_sessions": len(forward_complete_sessions)
        >= FORWARD_MIN_COMPLETE_SESSIONS,
        "event_sessions": int(metrics["sessions"]) >= FORWARD_MIN_EVENT_SESSIONS,
        "trades": int(metrics["trades"]) >= FORWARD_MIN_TRADES,
        "executable_coverage": float(metrics["executable_coverage"])
        >= FORWARD_MIN_EXECUTABLE_COVERAGE,
        "positive_mean_net_dollars": (
            finite(metrics["mean_net_dollars"]) is not None
            and float(metrics["mean_net_dollars"]) > 0
        ),
        "positive_bootstrap_lower": ci_low is not None and ci_low > 0,
        "holm_p": adjusted is not None and adjusted <= 0.05,
        "positive_session_rate": (
            finite(metrics["positive_session_rate"]) is not None
            and float(metrics["positive_session_rate"]) >= 0.60
        ),
    }
    result["holm_adjusted_p_across_forward_hypotheses"] = adjusted
    result["promotion_gates"] = gates
    result["promotion_eligible"] = all(gates.values())

strict_forward_evaluation = {
    "contract_hash": forward_contract_hash,
    "complete_forward_sessions": forward_complete_sessions,
    "complete_forward_session_count": len(forward_complete_sessions),
    "status": (
        "awaiting_minimum_forward_sessions"
        if len(forward_complete_sessions) < FORWARD_MIN_COMPLETE_SESSIONS
        else "promotion_gate_passed"
        if any(result["promotion_eligible"] for result in forward_results.values())
        else "evaluated_no_hypothesis_passed"
    ),
    "results": forward_results,
}

FORWARD_CONTRACT_PATH = RESEARCH_OUTPUT_ROOT / "raw-tick-event-forward-contract-v2.json"
FORWARD_CONTRACT_PATH.write_text(
    json.dumps(json_safe(frozen_forward_contract), indent=2, ensure_ascii=False) + "\n",
    encoding="utf-8",
)

regime_event_discovery = {
    "status": "retrospective_hypothesis_generation_only",
    "promotion_eligible": False,
    "promotion_blocker": (
        "The six tail sessions were already consumed by the earlier direction search; "
        "this atlas requires genuinely new forward sessions before any push authority."
    ),
    "day_label_contract": {
        "amplitude_axis_points": {
            "low": f"range <= {DAY_LOW_RANGE_POINTS:g}",
            "mid": f"{DAY_LOW_RANGE_POINTS:g} < range < {DAY_HIGH_RANGE_POINTS:g}",
            "high": f"range >= {DAY_HIGH_RANGE_POINTS:g}",
        },
        "path_axis": {
            "trend": (
                "displacement_efficiency>=0.65, close_directional_extreme>=0.75, "
                "max_adverse_fraction<=0.60"
            ),
            "range": "displacement_efficiency<=0.35 and path_efficiency<=0.10",
            "mixed": "not forced into either class",
        },
        "counts": {"amplitude": amplitude_counts, "path": path_counts},
        "days": rth_day_rows,
    },
    "trend_launches": {
        "definition": (
            "earliest causal timestamp after 10:00 ET whose future path reaches "
            "0.50 full-day range in the day direction before a 0.20 range invalidation"
        ),
        "count": len(rth_launches),
        "events": rth_launches,
        "prelaunch_feature_changes_vs_clock_controls": launch_feature_effects(
            session_grids["rth"], rth_days, rth_launches, RTH
        ),
    },
    "gex_data_quality": {
        "surface_frames": surface_total,
        "two_wall_frames": surface_wall,
        "two_wall_coverage": surface_wall / max(surface_total, 1),
        "zero_gamma_frames": surface_zero,
        "zero_gamma_coverage": surface_zero / max(surface_total, 1),
        "warning": (
            "GEX is an OI/volume structural proxy, not observed dealer inventory; "
            "missing zero-gamma is unavailable data, not a neutral signal."
        ),
    },
    "event_label_contract": {
        "decision_frequency_seconds": EVENT_DECISION_SECONDS,
        "cooldown_seconds": EVENT_COOLDOWN_SECONDS,
        "barrier": (
            "symmetric first passage, scale=max(2.5 points, 1.25*causal 15m realized "
            "path volatility, 0.10*causal expected move when available)"
        ),
        "fixed_20_minute_horizon": False,
        "censoring": "session close",
        "event_types": event_types,
    },
    "event_summary": event_summary,
    "exact_bbo_option_validation": event_option_validation,
    "frozen_forward_contract": frozen_forward_contract,
    "strict_forward_evaluation": strict_forward_evaluation,
    "gth_event_discovery": {
        "status": "retrospective_hypothesis_generation_only",
        "target": "live ES path with IBKR SPXW exact BBO; never pooled with RTH",
        "gex_scope": (
            "cash SPX GEX wall/zero-gamma rejection is not copied into GTH; "
            "only price-path breakout and pullback/resume events are evaluated"
        ),
        "day_label_contract": {
            "amplitude_axis_es_points": {
                "low": f"range <= {DAY_LOW_RANGE_POINTS:g}",
                "mid": (
                    f"{DAY_LOW_RANGE_POINTS:g} < range < {DAY_HIGH_RANGE_POINTS:g}"
                ),
                "high": f"range >= {DAY_HIGH_RANGE_POINTS:g}",
            },
            "path_axis": {
                "trend": (
                    "displacement_efficiency>=0.65, close_directional_extreme>=0.75, "
                    "max_adverse_fraction<=0.60"
                ),
                "range": (
                    "displacement_efficiency<=0.35 and path_efficiency<=0.10"
                ),
                "mixed": "not forced into either class",
            },
            "counts": {
                "amplitude": gth_amplitude_counts,
                "path": gth_path_counts,
            },
            "days": gth_day_rows,
        },
        "trend_launches": {
            "definition": (
                "earliest causal timestamp 30 minutes after the GTH session starts whose "
                "future ES path reaches 0.50 full-session range in the session direction "
                "before a 0.20 range invalidation"
            ),
            "count": len(gth_launches),
            "events": gth_launches,
            "prelaunch_feature_changes_vs_clock_controls": launch_feature_effects(
                session_grids["gth"], gth_days, gth_launches, GTH
            ),
        },
        "event_summary": gth_event_summary,
        "exact_bbo_option_validation": gth_event_option_validation,
        "event_examples": {
            event_type: [
                row for row in gth_events if row["event_type"] == event_type
            ][:12]
            for event_type in gth_event_types
        },
        "promotion_eligible": False,
        "promotion_blocker": (
            "These sessions were already used for hypothesis generation; a GTH contract "
            "can be frozen only after selecting or rejecting the development structure, "
            "then must use new IBKR sessions."
        ),
    },
    "event_examples": {
        event_type: [
            row for row in rth_events if row["event_type"] == event_type
        ][:12]
        for event_type in event_types
    },
    "validation_requirement": (
        "Freeze candidate/filter/option mapping now; collect new sessions; validate by "
        "session-clustered walk-forward and conservative exact-BBO option PnL before "
        "strategy_decision can grant manual authority."
    ),
}
print(
    "regime/event atlas",
    {
        "day_counts": regime_event_discovery["day_label_contract"]["counts"],
        "trend_launches": len(rth_launches),
        "events": len(rth_events),
        "event_types": event_types,
        "gex_wall_coverage": regime_event_discovery["gex_data_quality"][
            "two_wall_coverage"
        ],
        "zero_gamma_coverage": regime_event_discovery["gex_data_quality"][
            "zero_gamma_coverage"
        ],
    },
)


# %% [markdown]
# ## Causal denoising and state-change benchmark
#
# 该 benchmark 不再增加任意因子组合，而是把同一个 pullback/resume 事件分别交给 raw、
# Hampel、pre-average 和 local-linear Kalman 四种观测层；再单独加入一个 breadth 状态门和
# 一个 CUSUM transition detector。所有参数在运行前固定，开发段只允许在六条 pipeline
# 中选一次；跨 pipeline 的 exact sign-flip max statistic 用于惩罚 data snooping。

# %%
DENOISING_PIPELINES = {
    "raw_pullback": {
        "observation": "raw",
        "detector": "pullback_resume",
        "breadth_gate": False,
    },
    "hampel25_pullback": {
        "observation": "causal_hampel_25s",
        "detector": "pullback_resume",
        "breadth_gate": False,
    },
    "preaverage15_pullback": {
        "observation": "causal_preaverage_15s",
        "detector": "pullback_resume",
        "breadth_gate": False,
    },
    "kalman_pullback": {
        "observation": "adaptive_local_linear_kalman",
        "detector": "pullback_resume",
        "breadth_gate": False,
    },
    "kalman_breadth_pullback": {
        "observation": "adaptive_local_linear_kalman",
        "detector": "pullback_resume",
        "breadth_gate": True,
    },
    "kalman_cusum_breadth_transition": {
        "observation": "adaptive_local_linear_kalman",
        "detector": "two_sided_cusum",
        "breadth_gate": True,
    },
}
DENOISING_MIN_RESOLVED_EVENTS = 12
DENOISING_MIN_SESSIONS = 6
DENOISING_BREADTH_THRESHOLD = 0.60
DENOISING_OPTION_CONTRACT = {
    "rth": {
        "suffix": "delta_0.60/vertical_15",
        "maximum_selection_relative_spread": 0.05,
    },
    "gth": {
        "suffix": "delta_0.60/vertical_15",
        "maximum_selection_relative_spread": 0.02,
    },
}


def causal_hampel(values: np.ndarray, *, window: int = 5) -> np.ndarray:
    output = values.astype(float, copy=True)
    for index in range(len(values)):
        start = max(0, index - window + 1)
        sample = values[start : index + 1]
        sample = sample[np.isfinite(sample)]
        current = finite(values[index])
        if current is None or len(sample) < 3:
            continue
        median = float(np.median(sample))
        mad = float(np.median(np.abs(sample - median)))
        robust_sigma = 1.4826 * mad
        if robust_sigma > 1e-12 and abs(current - median) > 3.0 * robust_sigma:
            output[index] = median
    return output


def causal_preaverage(values: np.ndarray, *, window: int = 3) -> np.ndarray:
    output = np.full(len(values), np.nan)
    weights = np.arange(1, window + 1, dtype=float)
    for index in range(window - 1, len(values)):
        sample = values[index - window + 1 : index + 1]
        valid = np.isfinite(sample)
        if int(np.sum(valid)) >= window - 1:
            output[index] = float(np.average(sample[valid], weights=weights[valid]))
    return output


def adaptive_local_linear_kalman(
    values: np.ndarray,
) -> tuple[np.ndarray, np.ndarray]:
    level = np.full(len(values), np.nan)
    standardized_innovation = np.full(len(values), np.nan)
    state = np.asarray([0.0, 0.0])
    covariance = np.eye(2)
    transition = np.asarray([[1.0, 1.0], [0.0, 1.0]])
    observation = np.asarray([1.0, 0.0])
    initialized = False
    measurement_variance = 0.25
    previous_observation: float | None = None
    for index, raw_value in enumerate(values):
        value = finite(raw_value)
        if value is None:
            if initialized:
                state = transition @ state
                covariance = transition @ covariance @ transition.T
                level[index] = state[0]
            continue
        if not initialized:
            state[0] = value
            level[index] = value
            previous_observation = value
            initialized = True
            continue
        assert previous_observation is not None
        increment = value - previous_observation
        measurement_variance = max(
            0.95 * measurement_variance + 0.05 * increment * increment,
            1e-4,
        )
        previous_observation = value
        process_covariance = measurement_variance * np.asarray(
            [[0.02, 0.0], [0.0, 0.002]]
        )
        predicted_state = transition @ state
        predicted_covariance = (
            transition @ covariance @ transition.T + process_covariance
        )
        innovation = value - float(observation @ predicted_state)
        innovation_variance = float(
            observation @ predicted_covariance @ observation
            + measurement_variance
        )
        kalman_gain = (predicted_covariance @ observation) / innovation_variance
        state = predicted_state + kalman_gain * innovation
        covariance = (
            np.eye(2) - np.outer(kalman_gain, observation)
        ) @ predicted_covariance
        level[index] = state[0]
        standardized_innovation[index] = innovation / math.sqrt(
            innovation_variance
        )
    return level, standardized_innovation


def _pipeline_observation(
    raw_price: np.ndarray,
    observation: str,
) -> tuple[np.ndarray, np.ndarray | None]:
    if observation == "raw":
        return raw_price, None
    if observation == "causal_hampel_25s":
        return causal_hampel(raw_price), None
    if observation == "causal_preaverage_15s":
        return causal_preaverage(raw_price), None
    if observation == "adaptive_local_linear_kalman":
        return adaptive_local_linear_kalman(raw_price)
    raise KeyError(observation)


def _breadth_passes(
    grid: SessionGrid,
    contract: ModeContract,
    index: int,
    direction: int,
) -> bool:
    breadth = _directional_breadth(grid, contract, index, direction, 300)
    return breadth is not None and breadth >= DENOISING_BREADTH_THRESHOLD


def causal_gex_location_features(
    row: Mapping[str, Any],
) -> dict[str, Any]:
    empty = {field: None for field in EVENT_LOCATION_FIELDS}
    try:
        decision_at = datetime.fromisoformat(str(row["decision_at"])).astimezone(UTC)
    except (KeyError, TypeError, ValueError):
        return empty
    frames = rth_surface_frames.get(str(row.get("session_date"))) or ()
    causal_frames = [
        frame
        for frame in frames
        if isinstance(frame.get("as_of"), datetime)
        and frame["as_of"] <= decision_at
    ]
    if not causal_frames:
        return empty
    frame = causal_frames[-1]
    age_seconds = int((decision_at - frame["as_of"]).total_seconds())
    if not 0 <= age_seconds <= 10 * 60:
        return empty
    spot = finite(row.get("entry_spx"))
    scale = finite(row.get("scale_points"))
    put_wall = finite(frame.get("put_wall"))
    call_wall = finite(frame.get("call_wall"))
    if spot is None or scale is None or scale <= 0:
        return empty
    direction = 1 if row.get("direction") == "up" else -1
    directional_room = (
        call_wall - spot
        if direction > 0 and call_wall is not None
        else spot - put_wall
        if direction < 0 and put_wall is not None
        else None
    )
    between_walls = (
        put_wall <= spot <= call_wall
        if put_wall is not None and call_wall is not None
        else None
    )
    return {
        "surface_as_of": frame["as_of"].isoformat(),
        "surface_age_seconds": age_seconds,
        "gex_between_walls": between_walls,
        "gex_directional_room_points": directional_room,
        "gex_directional_room_scale": (
            directional_room / scale if directional_room is not None else None
        ),
        "gex_gamma_state": str(frame.get("gamma_state") or "unknown"),
    }


def denoised_pullback_events(
    grid: SessionGrid,
    contract: ModeContract,
    *,
    pipeline: str,
    observed_price: np.ndarray,
    breadth_gate: bool,
) -> list[dict[str, Any]]:
    raw_price = grid.instruments[contract.target].price
    stride = EVENT_DECISION_SECONDS // BUCKET_SECONDS
    window15 = 15 * 60 // BUCKET_SECONDS
    events: list[dict[str, Any]] = []
    next_allowed: dict[tuple[str, str], int] = {}
    for index in range(window15, len(raw_price), stride):
        if not (
            math.isfinite(float(raw_price[index]))
            and math.isfinite(float(observed_price[index]))
            and math.isfinite(float(observed_price[index - window15]))
        ):
            continue
        scale = local_event_scale(raw_price, index, expected_move=None)
        if scale is None:
            continue
        recent = observed_price[index - window15 : index + 1]
        if int(np.sum(np.isfinite(recent))) < math.ceil(0.9 * len(recent)):
            continue
        one_minute = observed_price[index] - observed_price[index - stride]
        impulse15 = observed_price[index] - observed_price[index - window15]
        recent_high, recent_low = float(np.nanmax(recent)), float(np.nanmin(recent))
        direction = 0
        if impulse15 >= scale:
            pullback = recent_high - float(observed_price[index])
            if 0.25 * scale <= pullback <= 0.80 * scale and one_minute > 0:
                direction = 1
        elif impulse15 <= -scale:
            pullback = float(observed_price[index]) - recent_low
            if 0.25 * scale <= pullback <= 0.80 * scale and one_minute < 0:
                direction = -1
        if direction == 0 or (
            breadth_gate
            and not _breadth_passes(grid, contract, index, direction)
        ):
            continue
        epoch = int(grid.epoch_seconds[index])
        _cooldown_append(
            events,
            first_passage_event(
                grid,
                contract=contract,
                index=index,
                direction=direction,
                scale=scale,
                event_type=pipeline,
            ),
            next_allowed,
            epoch=epoch,
        )
    return events


def kalman_cusum_events(
    grid: SessionGrid,
    contract: ModeContract,
    *,
    pipeline: str,
    level: np.ndarray,
) -> list[dict[str, Any]]:
    raw_price = grid.instruments[contract.target].price
    stride = EVENT_DECISION_SECONDS // BUCKET_SECONDS
    start = 15 * 60 // BUCKET_SECONDS
    positive = 0.0
    negative = 0.0
    events: list[dict[str, Any]] = []
    next_allowed: dict[tuple[str, str], int] = {}
    for index in range(start, len(level), stride):
        scale = local_event_scale(raw_price, index, expected_move=None)
        if (
            scale is None
            or not math.isfinite(float(level[index]))
            or not math.isfinite(float(level[index - stride]))
        ):
            positive = 0.0
            negative = 0.0
            continue
        one_minute_move = float(level[index] - level[index - stride])
        standardized_move = one_minute_move / max(
            scale * math.sqrt(EVENT_DECISION_SECONDS / (15 * 60)),
            1e-9,
        )
        positive = max(0.0, positive + standardized_move - 0.25)
        negative = min(0.0, negative + standardized_move + 0.25)
        direction = 1 if positive >= 4.0 else -1 if negative <= -4.0 else 0
        if direction == 0:
            continue
        positive = 0.0
        negative = 0.0
        if not _breadth_passes(grid, contract, index, direction):
            continue
        epoch = int(grid.epoch_seconds[index])
        _cooldown_append(
            events,
            first_passage_event(
                grid,
                contract=contract,
                index=index,
                direction=direction,
                scale=scale,
                event_type=pipeline,
            ),
            next_allowed,
            epoch=epoch,
        )
    return events


def denoising_pipeline_events(
    grid: SessionGrid,
    contract: ModeContract,
    pipeline: str,
) -> list[dict[str, Any]]:
    specification = DENOISING_PIPELINES[pipeline]
    raw_price = grid.instruments[contract.target].price
    observed, _ = _pipeline_observation(
        raw_price,
        str(specification["observation"]),
    )
    if specification["detector"] == "two_sided_cusum":
        return kalman_cusum_events(
            grid,
            contract,
            pipeline=pipeline,
            level=observed,
        )
    return denoised_pullback_events(
        grid,
        contract,
        pipeline=pipeline,
        observed_price=observed,
        breadth_gate=bool(specification["breadth_gate"]),
    )


def familywise_sign_flip_max_pvalue(
    development_rows: Mapping[str, Sequence[Mapping[str, Any]]],
) -> float | None:
    sessions = sorted(
        {
            str(row["session_date"])
            for rows in development_rows.values()
            for row in rows
            if row.get("outcome") != "censored"
        }
    )
    if not sessions or len(sessions) > 20:
        return None
    matrix = np.zeros((len(sessions), len(development_rows)))
    for column, rows in enumerate(development_rows.values()):
        grouped: dict[str, list[float]] = defaultdict(list)
        for row in rows:
            if row.get("outcome") != "censored":
                grouped[str(row["session_date"])].append(float(row["return_r"]))
        for row_index, session in enumerate(sessions):
            matrix[row_index, column] = float(np.mean(grouped.get(session, [0.0])))
    observed = float(np.max(np.mean(matrix, axis=0)))
    exceedances = 0
    trials = 1 << len(sessions)
    for mask in range(trials):
        signs = np.asarray(
            [1.0 if mask & (1 << index) else -1.0 for index in range(len(sessions))]
        )
        statistic = float(np.max(np.mean(matrix * signs[:, None], axis=0)))
        exceedances += statistic >= observed - 1e-12
    return exceedances / trials


def evaluate_fixed_pipeline_option(
    mode: str,
    pipeline: str,
    signals: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    contract = DENOISING_OPTION_CONTRACT[mode]
    candidate = f"{pipeline}::{contract['suffix']}"
    trades, diagnostics = executable_option_trades(
        mode,
        signals,
        holding_seconds=0,
    )
    fixed = [
        trade
        for trade in trades
        if trade["candidate"] == candidate
        and float(trade["selection_relative_spread"])
        <= float(contract["maximum_selection_relative_spread"])
    ]
    by_cohort: dict[str, Any] = {}
    for cohort in event_cohorts:
        cohort_signals = [row for row in signals if row["phase"] == cohort]
        cohort_trades = [row for row in fixed if row["phase"] == cohort]
        metrics = option_metrics(cohort_trades)
        metrics["signals"] = len(cohort_signals)
        metrics["executable_coverage"] = len(cohort_trades) / max(
            len(cohort_signals),
            1,
        )
        ci_low, ci_high = session_bootstrap_interval(
            metrics["session_mean_returns"]
        )
        by_cohort[cohort] = {
            "metrics": metrics,
            "session_bootstrap_95_return": [ci_low, ci_high],
            "exact_one_sided_session_sign_flip_p": exact_session_sign_flip_pvalue(
                metrics["session_mean_returns"]
            ),
            "trades": cohort_trades,
        }
    return {
        "candidate": candidate,
        "maximum_selection_relative_spread": contract[
            "maximum_selection_relative_spread"
        ],
        "diagnostics": diagnostics,
        "cohorts": by_cohort,
    }


def denoising_benchmark_for_mode(contract: ModeContract) -> dict[str, Any]:
    pipeline_rows: dict[str, list[dict[str, Any]]] = {}
    for pipeline in DENOISING_PIPELINES:
        rows: list[dict[str, Any]] = []
        for grid in session_grids[contract.mode]:
            for row in denoising_pipeline_events(grid, contract, pipeline):
                row["cohort"] = cohort_name(str(row["session_date"]), contract.mode)
                if contract.mode == "rth":
                    row.update(causal_gex_location_features(row))
                rows.append(row)
        pipeline_rows[pipeline] = rows

    development_rows = {
        pipeline: [row for row in rows if row["cohort"] == "development"]
        for pipeline, rows in pipeline_rows.items()
    }
    development_search: list[dict[str, Any]] = []
    for pipeline, rows in development_rows.items():
        metrics = event_metrics(rows)
        eligible = (
            int(metrics["resolved"]) >= DENOISING_MIN_RESOLVED_EVENTS
            and int(metrics["sessions"]) >= DENOISING_MIN_SESSIONS
        )
        development_search.append(
            {
                "pipeline": pipeline,
                "specification": DENOISING_PIPELINES[pipeline],
                "eligible": eligible,
                "selection_score": (
                    metrics["session_bootstrap_95_mean_r"][0]
                    if eligible
                    else None
                ),
                "metrics": metrics,
            }
        )
    eligible_rows = [row for row in development_search if row["eligible"]]
    champion = (
        max(eligible_rows, key=lambda row: float(row["selection_score"]))
        if eligible_rows
        else None
    )
    ranked = sorted(
        development_search,
        key=lambda row: (
            float(row["selection_score"])
            if row["selection_score"] is not None
            else -float("inf")
        ),
        reverse=True,
    )
    familywise_p = familywise_sign_flip_max_pvalue(development_rows)
    if champion is None:
        return {
            "status": "no_eligible_development_pipeline",
            "development_search": ranked,
            "familywise_sign_flip_max_p": familywise_p,
            "notification_authority": False,
        }

    champion_name = str(champion["pipeline"])
    raw_name = "raw_pullback"
    underlying_cohorts = {
        cohort: event_metrics(
            [
                row
                for row in pipeline_rows[champion_name]
                if row["cohort"] == cohort
            ]
        )
        for cohort in event_cohorts
    }
    option_comparisons: dict[str, Any] = {}
    comparison_pipelines = [raw_name, champion_name]
    if contract.mode == "rth":
        comparison_pipelines.append("kalman_cusum_breadth_transition")
    for pipeline in dict.fromkeys(comparison_pipelines):
        signals = [
            signal
            for cohort in event_cohorts
            for signal in event_signal_records(
                pipeline_rows[pipeline],
                event_type=pipeline,
                cohort=cohort,
            )
        ]
        option_comparisons[pipeline] = evaluate_fixed_pipeline_option(
            contract.mode,
            pipeline,
            signals,
        )

    champion_option = option_comparisons[champion_name]["cohorts"]
    validation = champion_option["retrospective_validation"]
    tail = champion_option["previously_seen_tail"]
    validation_metrics = validation["metrics"]
    tail_metrics = tail["metrics"]
    validation_lower = finite(validation["session_bootstrap_95_return"][0])
    tail_lower = finite(tail["session_bootstrap_95_return"][0])
    strict_pass = bool(
        finite(champion["selection_score"]) is not None
        and float(champion["selection_score"]) > 0
        and familywise_p is not None
        and familywise_p <= 0.10
        and int(validation_metrics["trades"]) >= 12
        and int(validation_metrics["sessions"]) >= 6
        and validation_lower is not None
        and validation_lower > 0
        and finite(validation["exact_one_sided_session_sign_flip_p"]) is not None
        and float(validation["exact_one_sided_session_sign_flip_p"]) <= 0.10
        and int(tail_metrics["trades"]) >= 8
        and int(tail_metrics["sessions"]) >= 4
        and tail_lower is not None
        and tail_lower > 0
        and finite(tail["exact_one_sided_session_sign_flip_p"]) is not None
        and float(tail["exact_one_sided_session_sign_flip_p"]) <= 0.125
    )
    return {
        "status": "retrospectively_tested",
        "development_champion": champion,
        "development_search": ranked,
        "familywise_sign_flip_max_p": familywise_p,
        "underlying_champion_cohorts": underlying_cohorts,
        "fixed_option_comparison": option_comparisons,
        "retrospective_strict_pass": strict_pass,
        "notification_authority": False,
        "interpretation": (
            "Only the development cohort selected the pipeline. Validation and the "
            "previously seen tail can falsify it but cannot create forward evidence."
        ),
    }


denoising_state_benchmark = {
    contract.mode: denoising_benchmark_for_mode(contract)
    for contract in (RTH, GTH)
}
print(
    "denoising/state benchmark",
    {
        mode: {
            "champion": (
                result.get("development_champion") or {}
            ).get("pipeline"),
            "familywise_p": result.get("familywise_sign_flip_max_p"),
            "strict_pass": result.get("retrospective_strict_pass"),
        }
        for mode, result in denoising_state_benchmark.items()
    },
)


# %% [markdown]
# ## Approved forward lanes and layered falsification
#
# RTH 保留 v2 不变，另冻结 raw/pre-average 配对研究合同。GTH 使用 observed exact-BBO
# availability 加固定 Ridge conditional-EV hurdle；失败的方向层不能被执行层“救活”。GEX
# 只测试是否提供方向前方空间，CUSUM/BOCPD 只作为状态变化层。

# %%
DENOISING_FORWARD_VERSION = "raw_tick_denoising_forward.v1"
DENOISING_FORWARD_FROZEN_AT = "2026-08-19T09:41:19+00:00"
DENOISING_FORWARD_START_DATE = date(2026, 8, 20)
DENOISING_FORWARD_LANES = {
    "raw_control": {
        "pipeline": "raw_pullback",
        "candidate": "raw_pullback::delta_0.60/vertical_15",
        "maximum_selection_relative_spread": 0.05,
    },
    "preaverage_candidate": {
        "pipeline": "preaverage15_pullback",
        "candidate": "preaverage15_pullback::delta_0.60/vertical_15",
        "maximum_selection_relative_spread": 0.05,
    },
}
DENOISING_FORWARD_MIN_COMPLETE_SESSIONS = 20
DENOISING_FORWARD_MIN_EVENT_SESSIONS = 8
DENOISING_FORWARD_MIN_TRADES = 30
DENOISING_FORWARD_MIN_EXECUTABLE_COVERAGE = 0.50


def paired_session_improvement(
    treatment: Mapping[str, float],
    control: Mapping[str, float],
) -> dict[str, Any]:
    sessions = sorted(set(treatment) & set(control))
    deltas = {
        session: float(treatment[session]) - float(control[session])
        for session in sessions
    }
    ci_low, ci_high = session_bootstrap_interval(deltas)
    return {
        "paired_sessions": len(sessions),
        "mean_return_improvement": (
            float(np.mean(list(deltas.values()))) if deltas else None
        ),
        "session_bootstrap_95_improvement": [ci_low, ci_high],
        "exact_one_sided_session_sign_flip_p": exact_session_sign_flip_pvalue(
            deltas
        ),
        "session_deltas": deltas,
    }


def forward_pipeline_signals(
    contract: ModeContract,
    pipeline: str,
    *,
    start_date: date,
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for grid in session_grids[contract.mode]:
        if grid.session_date < start_date:
            continue
        for row in denoising_pipeline_events(grid, contract, pipeline):
            row["cohort"] = "strict_forward"
            if contract.mode == "rth":
                row.update(causal_gex_location_features(row))
            rows.append(row)
    return event_signal_records(
        rows,
        event_type=pipeline,
        cohort="strict_forward",
    )


denoising_forward_contract_payload = {
    "schema_version": DENOISING_FORWARD_VERSION,
    "frozen_at": DENOISING_FORWARD_FROZEN_AT,
    "forward_start_session": DENOISING_FORWARD_START_DATE.isoformat(),
    "scope": (
        "RTH raw-vs-15-second-pre-average paired research; no strategy_decision "
        "or notification authority"
    ),
    "existing_v2_contract_hash_unchanged": forward_contract_hash,
    "lanes": DENOISING_FORWARD_LANES,
    "shared_event_contract": {
        "decision_frequency_seconds": EVENT_DECISION_SECONDS,
        "cooldown_seconds": EVENT_COOLDOWN_SECONDS,
        "trigger": (
            "15-minute impulse >= causal local scale; 0.25-0.80 scale pullback; "
            "latest one-minute observation resumes impulse direction"
        ),
        "raw_control_observation": "causal five-second raw SPX price",
        "candidate_observation": (
            "causal trailing three-bucket weighted pre-average with weights 1,2,3"
        ),
        "outcome": "symmetric one-local-scale first passage; censor at RTH close",
    },
    "execution_contract": {
        "provider": "Schwab",
        "entry_delay_seconds": OPTION_ENTRY_DELAY_SECONDS,
        "maximum_bbo_age_seconds": 5,
        "entry": "long ask minus short bid",
        "exit": "long bid minus short ask",
        "fee_dollars_per_leg_side": FEE_DOLLARS_PER_LEG_SIDE,
        "mid_allowed": False,
    },
    "promotion_gate": {
        "minimum_complete_forward_sessions": DENOISING_FORWARD_MIN_COMPLETE_SESSIONS,
        "minimum_event_sessions_per_lane": DENOISING_FORWARD_MIN_EVENT_SESSIONS,
        "minimum_exact_bbo_trades_per_lane": DENOISING_FORWARD_MIN_TRADES,
        "minimum_executable_coverage_per_lane": (
            DENOISING_FORWARD_MIN_EXECUTABLE_COVERAGE
        ),
        "preaverage_mean_net_dollars": "> 0",
        "preaverage_session_bootstrap_95_return_lower": "> 0",
        "preaverage_one_sided_session_sign_flip_p": "<= 0.05",
        "paired_sessions": ">= 8",
        "paired_preaverage_minus_raw_bootstrap_95_lower": "> 0",
        "paired_one_sided_session_sign_flip_p": "<= 0.05",
    },
    "notification_authority": False,
    "strategy_decision_authority": False,
}
denoising_forward_contract_hash = "sha256:" + hashlib.sha256(
    json.dumps(
        json_safe(denoising_forward_contract_payload),
        allow_nan=False,
        separators=(",", ":"),
        sort_keys=True,
    ).encode()
).hexdigest()
frozen_denoising_forward_contract = {
    **denoising_forward_contract_payload,
    "contract_hash": denoising_forward_contract_hash,
}

denoising_forward_complete_sessions = sorted(
    grid.session_date.isoformat()
    for grid in session_grids["rth"]
    if grid.session_date >= DENOISING_FORWARD_START_DATE
)
denoising_forward_results: dict[str, dict[str, Any]] = {}
for lane, lane_contract in DENOISING_FORWARD_LANES.items():
    signals = forward_pipeline_signals(
        RTH,
        str(lane_contract["pipeline"]),
        start_date=DENOISING_FORWARD_START_DATE,
    )
    trades, diagnostics = executable_option_trades(
        "rth",
        signals,
        holding_seconds=0,
    )
    selected = [
        trade
        for trade in trades
        if trade["candidate"] == lane_contract["candidate"]
        and float(trade["selection_relative_spread"])
        <= float(lane_contract["maximum_selection_relative_spread"])
    ]
    metrics = option_metrics(selected)
    metrics["signals"] = len(signals)
    metrics["executable_coverage"] = len(selected) / max(len(signals), 1)
    ci_low, ci_high = session_bootstrap_interval(metrics["session_mean_returns"])
    denoising_forward_results[lane] = {
        "pipeline": lane_contract["pipeline"],
        "candidate": lane_contract["candidate"],
        "maximum_selection_relative_spread": lane_contract[
            "maximum_selection_relative_spread"
        ],
        "diagnostics": diagnostics,
        "metrics": metrics,
        "session_bootstrap_95_return": [ci_low, ci_high],
        "exact_one_sided_session_sign_flip_p": exact_session_sign_flip_pvalue(
            metrics["session_mean_returns"]
        ),
    }

denoising_forward_paired = paired_session_improvement(
    denoising_forward_results["preaverage_candidate"]["metrics"][
        "session_mean_returns"
    ],
    denoising_forward_results["raw_control"]["metrics"]["session_mean_returns"],
)
candidate_forward = denoising_forward_results["preaverage_candidate"]
candidate_forward_metrics = candidate_forward["metrics"]
candidate_forward_lower = finite(candidate_forward["session_bootstrap_95_return"][0])
paired_forward_lower = finite(
    denoising_forward_paired["session_bootstrap_95_improvement"][0]
)
denoising_forward_gates = {
    "complete_sessions": len(denoising_forward_complete_sessions)
    >= DENOISING_FORWARD_MIN_COMPLETE_SESSIONS,
    "both_lane_event_sessions": all(
        int(result["metrics"]["sessions"])
        >= DENOISING_FORWARD_MIN_EVENT_SESSIONS
        for result in denoising_forward_results.values()
    ),
    "both_lane_trades": all(
        int(result["metrics"]["trades"]) >= DENOISING_FORWARD_MIN_TRADES
        for result in denoising_forward_results.values()
    ),
    "both_lane_coverage": all(
        float(result["metrics"]["executable_coverage"])
        >= DENOISING_FORWARD_MIN_EXECUTABLE_COVERAGE
        for result in denoising_forward_results.values()
    ),
    "candidate_positive_mean": (
        finite(candidate_forward_metrics["mean_net_dollars"]) is not None
        and float(candidate_forward_metrics["mean_net_dollars"]) > 0
    ),
    "candidate_positive_lower": (
        candidate_forward_lower is not None and candidate_forward_lower > 0
    ),
    "candidate_p": (
        finite(candidate_forward["exact_one_sided_session_sign_flip_p"])
        is not None
        and float(candidate_forward["exact_one_sided_session_sign_flip_p"])
        <= 0.05
    ),
    "paired_sessions": int(denoising_forward_paired["paired_sessions"]) >= 8,
    "paired_positive_lower": (
        paired_forward_lower is not None and paired_forward_lower > 0
    ),
    "paired_p": (
        finite(denoising_forward_paired["exact_one_sided_session_sign_flip_p"])
        is not None
        and float(
            denoising_forward_paired["exact_one_sided_session_sign_flip_p"]
        )
        <= 0.05
    ),
}
strict_denoising_forward_evaluation = {
    "contract_hash": denoising_forward_contract_hash,
    "complete_forward_sessions": denoising_forward_complete_sessions,
    "complete_forward_session_count": len(denoising_forward_complete_sessions),
    "status": (
        "awaiting_minimum_forward_sessions"
        if len(denoising_forward_complete_sessions)
        < DENOISING_FORWARD_MIN_COMPLETE_SESSIONS
        else "promotion_gate_passed"
        if all(denoising_forward_gates.values())
        else "evaluated_gate_failed"
    ),
    "results": denoising_forward_results,
    "paired_preaverage_minus_raw": denoising_forward_paired,
    "promotion_gates": denoising_forward_gates,
    "promotion_eligible": all(denoising_forward_gates.values()),
    "notification_authority": False,
}
DENOISING_FORWARD_CONTRACT_PATH = (
    RESEARCH_OUTPUT_ROOT / "raw-tick-denoising-forward-contract-v1.json"
)
DENOISING_FORWARD_CONTRACT_PATH.write_text(
    json.dumps(
        json_safe(frozen_denoising_forward_contract),
        indent=2,
        ensure_ascii=False,
    )
    + "\n",
    encoding="utf-8",
)


GTH_HURDLE_FEATURES = (
    "selection_relative_spread",
    "entry_debit_points",
    "absolute_selection_delta",
    "session_progress",
    "rv_rate_60_to_600",
    "directional_breadth_300s",
    "directional_es_ofi_60s",
)
GTH_HURDLE_RIDGE_ALPHA = 10.0


def gth_hurdle_matrix(rows: Sequence[Mapping[str, Any]]) -> np.ndarray:
    matrix = np.full((len(rows), len(GTH_HURDLE_FEATURES)), np.nan)
    for row_index, row in enumerate(rows):
        for column, feature in enumerate(GTH_HURDLE_FEATURES):
            source = "selection_delta" if feature == "absolute_selection_delta" else feature
            value = finite(row.get(source))
            if value is not None:
                matrix[row_index, column] = abs(value) if feature == "absolute_selection_delta" else value
    return matrix


def make_gth_hurdle_model() -> Pipeline:
    return Pipeline(
        [
            ("imputer", SimpleImputer(strategy="median")),
            ("scaler", StandardScaler()),
            ("model", Ridge(alpha=GTH_HURDLE_RIDGE_ALPHA)),
        ]
    )


def option_result_with_predictions(
    rows: Sequence[Mapping[str, Any]],
    predictions: np.ndarray,
    *,
    signals: int,
) -> dict[str, Any]:
    scored = [
        {**row, "hurdle_predicted_net_return": float(prediction)}
        for row, prediction in zip(rows, predictions, strict=True)
        if math.isfinite(float(prediction))
    ]
    selected = [row for row in scored if row["hurdle_predicted_net_return"] > 0]
    all_metrics = option_metrics(rows)
    all_metrics["signals"] = signals
    all_metrics["executable_coverage"] = len(rows) / max(signals, 1)
    selected_metrics = option_metrics(selected)
    selected_metrics["signals"] = signals
    selected_metrics["executable_coverage"] = len(selected) / max(signals, 1)
    selected_metrics["conditional_selection_rate"] = len(selected) / max(len(rows), 1)
    ci_low, ci_high = session_bootstrap_interval(
        selected_metrics["session_mean_returns"]
    )
    return {
        "observed_exact_bbo": all_metrics,
        "selected_metrics": selected_metrics,
        "session_bootstrap_95_return": [ci_low, ci_high],
        "exact_one_sided_session_sign_flip_p": exact_session_sign_flip_pvalue(
            selected_metrics["session_mean_returns"]
        ),
        "selected_trades": selected,
    }


def gth_execution_hurdle_research() -> dict[str, Any]:
    benchmark = denoising_state_benchmark["gth"]
    champion = str(benchmark["development_champion"]["pipeline"])
    fixed_option = benchmark["fixed_option_comparison"][champion]
    cohort_payload = fixed_option["cohorts"]
    development = list(cohort_payload["development"]["trades"])
    validation = list(cohort_payload["retrospective_validation"]["trades"])
    tail = list(cohort_payload["previously_seen_tail"]["trades"])
    if len(development) < 12:
        return {
            "status": "insufficient_development_exact_bbo_trades",
            "pipeline": champion,
            "development_trades": len(development),
            "notification_authority": False,
        }

    development_matrix = gth_hurdle_matrix(development)
    development_target = np.asarray(
        [float(row["net_return"]) for row in development]
    )
    sessions = np.asarray([str(row["session_date"]) for row in development])
    oof_prediction = np.full(len(development), np.nan)
    for session in sorted(set(sessions)):
        held_out = sessions == session
        training = ~held_out
        if int(np.sum(training)) < 10:
            continue
        model = make_gth_hurdle_model()
        model.fit(development_matrix[training], development_target[training])
        oof_prediction[held_out] = model.predict(development_matrix[held_out])

    full_model = make_gth_hurdle_model()
    full_model.fit(development_matrix, development_target)
    imputer = full_model.named_steps["imputer"]
    output_features = list(imputer.get_feature_names_out(GTH_HURDLE_FEATURES))
    coefficients = np.asarray(full_model.named_steps["model"].coef_, dtype=float)
    standardized_loadings = sorted(
        [
            {"feature": feature, "standardized_loading": float(coefficient)}
            for feature, coefficient in zip(output_features, coefficients, strict=True)
        ],
        key=lambda row: abs(row["standardized_loading"]),
        reverse=True,
    )

    development_result = option_result_with_predictions(
        development,
        oof_prediction,
        signals=int(cohort_payload["development"]["metrics"]["signals"]),
    )
    validation_prediction = full_model.predict(gth_hurdle_matrix(validation))
    validation_result = option_result_with_predictions(
        validation,
        validation_prediction,
        signals=int(
            cohort_payload["retrospective_validation"]["metrics"]["signals"]
        ),
    )
    tail_prediction = full_model.predict(gth_hurdle_matrix(tail))
    tail_result = option_result_with_predictions(
        tail,
        tail_prediction,
        signals=int(cohort_payload["previously_seen_tail"]["metrics"]["signals"]),
    )

    valid_oof = np.isfinite(oof_prediction)
    oof_mae = (
        float(np.mean(np.abs(oof_prediction[valid_oof] - development_target[valid_oof])))
        if np.any(valid_oof)
        else None
    )
    direction_layer_pass = bool(
        finite(benchmark["development_champion"]["selection_score"]) is not None
        and float(benchmark["development_champion"]["selection_score"]) > 0
        and finite(benchmark["familywise_sign_flip_max_p"]) is not None
        and float(benchmark["familywise_sign_flip_max_p"]) <= 0.10
    )
    development_metrics = development_result["selected_metrics"]
    validation_metrics = validation_result["selected_metrics"]
    tail_metrics = tail_result["selected_metrics"]
    validation_lower = finite(validation_result["session_bootstrap_95_return"][0])
    tail_lower = finite(tail_result["session_bootstrap_95_return"][0])
    gates = {
        "direction_layer": direction_layer_pass,
        "development_oof_support": (
            int(development_metrics["trades"]) >= 8
            and int(development_metrics["sessions"]) >= 4
        ),
        "development_oof_positive": (
            finite(development_metrics["mean_net_dollars"]) is not None
            and float(development_metrics["mean_net_dollars"]) > 0
        ),
        "validation_support": (
            int(validation_metrics["trades"]) >= 8
            and int(validation_metrics["sessions"]) >= 4
        ),
        "validation_positive_lower": (
            validation_lower is not None and validation_lower > 0
        ),
        "validation_p": (
            finite(validation_result["exact_one_sided_session_sign_flip_p"])
            is not None
            and float(validation_result["exact_one_sided_session_sign_flip_p"])
            <= 0.125
        ),
        "tail_support": (
            int(tail_metrics["trades"]) >= 4
            and int(tail_metrics["sessions"]) >= 3
        ),
        "tail_positive_lower": tail_lower is not None and tail_lower > 0,
    }
    return {
        "status": "retrospectively_tested",
        "direction_pipeline": champion,
        "stage_one": (
            "observed fresh two-sided IBKR exact BBO and fixed relative-spread gate"
        ),
        "stage_two": (
            "fixed-alpha Ridge conditional net-return prediction; trade only if > 0"
        ),
        "features": GTH_HURDLE_FEATURES,
        "ridge_alpha": GTH_HURDLE_RIDGE_ALPHA,
        "development_oof_mae": oof_mae,
        "standardized_loadings": standardized_loadings,
        "development_oof": development_result,
        "retrospective_validation": validation_result,
        "previously_seen_tail": tail_result,
        "promotion_gates": gates,
        "retrospective_strict_pass": all(gates.values()),
        "notification_authority": False,
        "interpretation": (
            "Execution selection cannot authorize a trade when the upstream GTH "
            "direction layer has no development evidence."
        ),
    }


gth_hurdle_research = gth_execution_hurdle_research()


def gex_location_gate_passes(row: Mapping[str, Any]) -> bool:
    age = finite(row.get("surface_age_seconds"))
    room = finite(row.get("gex_directional_room_scale"))
    return bool(
        age is not None
        and 0 <= age <= 10 * 60
        and row.get("gex_between_walls") is True
        and room is not None
        and room >= 1.0
    )


def gex_location_gate_research() -> dict[str, Any]:
    benchmark = denoising_state_benchmark["rth"]
    champion = str(benchmark["development_champion"]["pipeline"])
    fixed = benchmark["fixed_option_comparison"][champion]["cohorts"]
    cohorts: dict[str, Any] = {}
    for cohort in event_cohorts:
        all_trades = list(fixed[cohort]["trades"])
        located = [
            row
            for row in all_trades
            if finite(row.get("surface_age_seconds")) is not None
        ]
        gated = [row for row in all_trades if gex_location_gate_passes(row)]
        baseline_metrics = option_metrics(all_trades)
        gated_metrics = option_metrics(gated)
        ci_low, ci_high = session_bootstrap_interval(
            gated_metrics["session_mean_returns"]
        )
        cohorts[cohort] = {
            "signals": int(fixed[cohort]["metrics"]["signals"]),
            "exact_bbo_trades": len(all_trades),
            "surface_located_trades": len(located),
            "surface_coverage_of_exact_bbo": len(located) / max(len(all_trades), 1),
            "gated_trades": len(gated),
            "gate_rate_of_exact_bbo": len(gated) / max(len(all_trades), 1),
            "baseline_metrics": baseline_metrics,
            "gated_metrics": gated_metrics,
            "mean_net_return_increment": (
                float(gated_metrics["mean_net_return"])
                - float(baseline_metrics["mean_net_return"])
                if gated_metrics["mean_net_return"] is not None
                and baseline_metrics["mean_net_return"] is not None
                else None
            ),
            "session_bootstrap_95_return": [ci_low, ci_high],
            "exact_one_sided_session_sign_flip_p": exact_session_sign_flip_pvalue(
                gated_metrics["session_mean_returns"]
            ),
        }
    development = cohorts["development"]
    validation = cohorts["retrospective_validation"]
    tail = cohorts["previously_seen_tail"]
    validation_lower = finite(validation["session_bootstrap_95_return"][0])
    tail_lower = finite(tail["session_bootstrap_95_return"][0])
    gates = {
        "development_support": (
            int(development["gated_metrics"]["trades"]) >= 12
            and int(development["gated_metrics"]["sessions"]) >= 6
        ),
        "development_positive_increment": (
            finite(development["mean_net_return_increment"]) is not None
            and float(development["mean_net_return_increment"]) > 0
        ),
        "validation_support": (
            int(validation["gated_metrics"]["trades"]) >= 12
            and int(validation["gated_metrics"]["sessions"]) >= 6
        ),
        "validation_positive_lower": (
            validation_lower is not None and validation_lower > 0
        ),
        "tail_support": (
            int(tail["gated_metrics"]["trades"]) >= 8
            and int(tail["gated_metrics"]["sessions"]) >= 4
        ),
        "tail_positive_lower": tail_lower is not None and tail_lower > 0,
    }
    return {
        "status": "retrospectively_tested",
        "pipeline": champion,
        "gate": (
            "causal surface age <=10m; spot between put/call walls; directional "
            "room to the next wall >= one local event scale"
        ),
        "gex_semantics": (
            "OI/volume structural location proxy; never observed dealer inventory"
        ),
        "cohorts": cohorts,
        "promotion_gates": gates,
        "retrospective_strict_pass": all(gates.values()),
        "notification_authority": False,
    }


gex_location_research = gex_location_gate_research()


BOCPD_HAZARD = 1 / 60
BOCPD_MAX_RUN_LENGTH = 120
BOCPD_PRIOR_PRECISION = 1.0
BOCPD_DIAGNOSTIC_THRESHOLD = 0.20


def causal_bocpd_change_probability(
    grid: SessionGrid,
    contract: ModeContract,
) -> np.ndarray:
    raw_price = grid.instruments[contract.target].price
    level, _ = adaptive_local_linear_kalman(raw_price)
    output = np.full(len(level), np.nan)
    stride = EVENT_DECISION_SECONDS // BUCKET_SECONDS
    start = 15 * 60 // BUCKET_SECONDS
    run_probability = np.asarray([1.0])
    means = np.asarray([0.0])
    precisions = np.asarray([BOCPD_PRIOR_PRECISION])
    for index in range(start, len(level), stride):
        scale = local_event_scale(raw_price, index, expected_move=None)
        if (
            scale is None
            or not math.isfinite(float(level[index]))
            or not math.isfinite(float(level[index - stride]))
        ):
            continue
        observation = float(level[index] - level[index - stride]) / max(
            scale * math.sqrt(EVENT_DECISION_SECONDS / (15 * 60)),
            1e-9,
        )
        predictive_variance = 1.0 + 1.0 / precisions
        predictive = np.exp(
            -0.5 * (observation - means) ** 2 / predictive_variance
        ) / np.sqrt(2 * math.pi * predictive_variance)
        prior_variance = 1.0 + 1.0 / BOCPD_PRIOR_PRECISION
        prior_predictive = math.exp(
            -0.5 * observation * observation / prior_variance
        ) / math.sqrt(2 * math.pi * prior_variance)
        changepoint_mass = (
            BOCPD_HAZARD * prior_predictive * float(np.sum(run_probability))
        )
        growth = (1.0 - BOCPD_HAZARD) * run_probability * predictive
        new_probability = np.concatenate(([changepoint_mass], growth))[
            : BOCPD_MAX_RUN_LENGTH + 1
        ]
        normalizer = float(np.sum(new_probability))
        if not math.isfinite(normalizer) or normalizer <= 1e-300:
            run_probability = np.asarray([1.0])
            means = np.asarray([0.0])
            precisions = np.asarray([BOCPD_PRIOR_PRECISION])
            continue
        run_probability = new_probability / normalizer
        output[index] = float(run_probability[0])
        new_precision_zero = BOCPD_PRIOR_PRECISION + 1.0
        new_mean_zero = observation / new_precision_zero
        growth_precisions = precisions + 1.0
        growth_means = (precisions * means + observation) / growth_precisions
        precisions = np.concatenate(([new_precision_zero], growth_precisions))[
            : len(run_probability)
        ]
        means = np.concatenate(([new_mean_zero], growth_means))[
            : len(run_probability)
        ]
    return output


def bocpd_prelaunch_diagnostic(
    grids: Sequence[SessionGrid],
    days: Mapping[str, Mapping[str, Any]],
    launches: Sequence[Mapping[str, Any]],
    contract: ModeContract,
) -> dict[str, Any]:
    probabilities = {
        grid.session_date.isoformat(): causal_bocpd_change_probability(grid, contract)
        for grid in grids
    }
    controls = [
        grid
        for grid in grids
        if days[grid.session_date.isoformat()]["path_axis"] != "trend"
    ]
    lookback = 15 * 60 // BUCKET_SECONDS
    launch_values: list[float] = []
    matched_controls: list[float] = []
    for launch in launches:
        session = str(launch["session_date"])
        index = int(launch["launch_index"])
        launch_window = probabilities[session][max(0, index - lookback) : index + 1]
        launch_window = launch_window[np.isfinite(launch_window)]
        if len(launch_window) == 0:
            continue
        control_values: list[float] = []
        for control in controls:
            control_probability = probabilities[control.session_date.isoformat()]
            if index >= len(control_probability):
                continue
            window = control_probability[max(0, index - lookback) : index + 1]
            window = window[np.isfinite(window)]
            if len(window):
                control_values.append(float(np.max(window)))
        if not control_values:
            continue
        launch_values.append(float(np.max(launch_window)))
        matched_controls.append(float(np.median(control_values)))
    launch_array = np.asarray(launch_values, dtype=float)
    control_array = np.asarray(matched_controls, dtype=float)
    pooled = (
        float(np.sqrt((np.var(launch_array, ddof=1) + np.var(control_array, ddof=1)) / 2))
        if len(launch_array) > 1 and len(control_array) > 1
        else 0.0
    )
    all_values = np.concatenate(
        [values[np.isfinite(values)] for values in probabilities.values()]
    )
    return {
        "status": "hindsight_launch_diagnostic_only",
        "mode": contract.mode,
        "trend_launches": len(launch_array),
        "mean_prelaunch_max_probability": (
            float(np.mean(launch_array)) if len(launch_array) else None
        ),
        "mean_matched_clock_control_max_probability": (
            float(np.mean(control_array)) if len(control_array) else None
        ),
        "standardized_difference": (
            float((np.mean(launch_array) - np.mean(control_array)) / pooled)
            if pooled > 1e-12
            else None
        ),
        "prelaunch_hit_rate_at_fixed_0.20": (
            float(np.mean(launch_array >= BOCPD_DIAGNOSTIC_THRESHOLD))
            if len(launch_array)
            else None
        ),
        "all_observation_probability_quantiles": (
            {
                "p50": float(np.quantile(all_values, 0.50)),
                "p90": float(np.quantile(all_values, 0.90)),
                "p99": float(np.quantile(all_values, 0.99)),
            }
            if len(all_values)
            else {}
        ),
        "hazard": BOCPD_HAZARD,
        "maximum_run_length_minutes": BOCPD_MAX_RUN_LENGTH,
        "trade_trigger_authority": False,
    }


bocpd_state_diagnostics = {
    "rth": bocpd_prelaunch_diagnostic(
        session_grids["rth"],
        rth_days,
        rth_launches,
        RTH,
    ),
    "gth": bocpd_prelaunch_diagnostic(
        session_grids["gth"],
        gth_days,
        gth_launches,
        GTH,
    ),
}

cusum_option_research = denoising_state_benchmark["rth"][
    "fixed_option_comparison"
]["kalman_cusum_breadth_transition"]
layered_followup_research = {
    "rth_denoising_forward_contract": frozen_denoising_forward_contract,
    "rth_denoising_forward_evaluation": strict_denoising_forward_evaluation,
    "gth_execution_hurdle": gth_hurdle_research,
    "rth_gex_location_gate": gex_location_research,
    "rth_cusum_state_transition": {
        "status": "retrospectively_tested",
        "pipeline": "kalman_cusum_breadth_transition",
        "fixed_option_result": cusum_option_research,
        "notification_authority": False,
    },
    "bocpd_state_diagnostics": bocpd_state_diagnostics,
    "production_action": "none",
}
print(
    "layered follow-up",
    {
        "denoising_forward_sessions": len(denoising_forward_complete_sessions),
        "gth_hurdle_pass": gth_hurdle_research.get("retrospective_strict_pass"),
        "gex_gate_pass": gex_location_research["retrospective_strict_pass"],
        "bocpd": {
            mode: result.get("standardized_difference")
            for mode, result in bocpd_state_diagnostics.items()
        },
    },
)


def ridge_factor_decomposition(
    samples: SampleSet,
    split: Mapping[str, Sequence[str]],
    champion: Mapping[str, Any],
) -> dict[str, Any]:
    spec = parse_model_spec(champion)
    if spec.family != "ridge":
        return {"available": False, "reason": "champion is nonlinear"}
    target = samples.outcomes[int(champion["holding_seconds"])]
    fit_dates = date_mask(samples, (*split["train"], *split["validation"]))
    fit = fit_dates & np.isfinite(target)
    indices = feature_indices(samples.feature_names, spec.feature_block)
    indices = indices[np.any(np.isfinite(samples.X[fit][:, indices]), axis=0)]
    model = make_model(spec)
    model.fit(samples.X[fit][:, indices], target[fit])
    coefficients = np.asarray(model.named_steps["model"].coef_, dtype=float)
    names = [samples.feature_names[index] for index in indices]
    loadings = sorted(
        (
            {"feature": name, "standardized_loading": float(coefficient)}
            for name, coefficient in zip(names, coefficients, strict=True)
        ),
        key=lambda row: abs(row["standardized_loading"]),
        reverse=True,
    )
    family_mass: dict[str, float] = defaultdict(float)
    for row in loadings:
        family_mass[row["feature"].split("/", 1)[0]] += abs(row["standardized_loading"])
    total = sum(family_mass.values())
    return {
        "available": True,
        "top_standardized_loadings": loadings[:15],
        "absolute_loading_share_by_family": {
            key: value / total for key, value in family_mass.items()
        },
        "interpretation_limit": (
            "Correlated feature loadings describe the fitted linear predictor; they are not causal effects."
        ),
    }


factor_decomposition = {
    mode: ridge_factor_decomposition(samples, splits[mode], champions[mode])
    for mode, samples in sample_sets.items()
}


def best_by_feature_block(rows: Sequence[Mapping[str, Any]]) -> dict[str, dict[str, Any]]:
    output: dict[str, dict[str, Any]] = {}
    for block in ("price", "micro", "cross", "full"):
        eligible = [row for row in rows if row["eligible"] and row["feature_block"] == block]
        if eligible:
            output[block] = max(eligible, key=lambda row: float(row["selection_score"]))
    return output


data_quality = {}
for contract in (RTH, GTH):
    grids = session_grids[contract.mode]
    data_quality[contract.mode] = {
        "target": contract.target_label,
        "complete_sessions": len(grids),
        "session_dates": [grid.session_date for grid in grids],
        "target_fresh_coverage_min": min(grid.target_coverage for grid in grids),
        "target_fresh_coverage_median": float(np.median([grid.target_coverage for grid in grids])),
        "causal_quote_updates": int(sum(grid.raw_rows for grid in grids)),
        "decision_samples": len(sample_sets[contract.mode].X),
        "features": len(sample_sets[contract.mode].feature_names),
        "split": splits[contract.mode],
    }

snapshot = {
    "title": "SPX 0DTE 原始行情 Edge Discovery",
    "generated_at": datetime.now(tz=UTC),
    "research_question": (
        "Can raw, causally available quote updates reveal a stable directional edge without "
        "using any existing strategy decision, setup, threshold, candidate, or management rule?"
    ),
    "scope": {
        "rth_target": "SPX",
        "gth_target": "ES proxy because SPX cash is not live in GTH",
        "input_grain": "near-tick quote updates causally aggregated into five-second buckets",
        "decision_frequency_seconds": DECISION_SECONDS,
        "holding_seconds_searched": HOLDING_SECONDS,
        "fixed_20_minute_label_used": False,
        "current_strategy_fields_used": [],
        "option_payoff_validation_complete": True,
    },
    "source": {
        "logical_path": "quote_lake/schema=v1/date=*/provider=schwab/hour=*/quotes.parquet",
        "provider": "Schwab",
        "knowledge_guard": [
            "quality=live",
            "market_data_type in {live,1}",
            "source_at between received_at-30s and received_at+5s",
            "quote_time no later than received_at+5s",
            "bucket available only at bucket_end",
        ],
        "sql": source_sql,
    },
    "data_quality": data_quality,
    "feature_families": {
        "price": "multi-scale returns, realized volatility, path efficiency",
        "micro": "L1 depth imbalance, microprice deviation, spread, OFI, quote and volume intensity",
        "cross": "ES/NQ/RTY/YM/SPY/VIX returns and RTH ES-SPX lead gaps",
        "state": "session progress, cyclical time and short/long realized-volatility ratio",
    },
    "model_spec": {
        "models": [asdict(spec) for spec in model_specs()],
        "search_candidates_per_mode": (
            len(HOLDING_SECONDS) * len(model_specs()) * len(CONFIDENCE_QUANTILES)
        ),
        "selection": "maximize validation session-clustered 90% lower confidence bound",
        "minimum_validation_support": "25 non-overlapping signals across at least 6 sessions",
        "final_hypotheses": "one preselected champion per mode; Holm adjustment across RTH and GTH",
        "sealed_test_sessions": SEALED_TEST_SESSIONS,
    },
    "discovery": {
        mode: {
            "champion": champions[mode],
            "best_by_feature_block": best_by_feature_block(search_results[mode]),
            "top_validation_candidates": sorted(
                [row for row in search_results[mode] if row["eligible"]],
                key=lambda row: float(row["selection_score"]),
                reverse=True,
            )[:12],
        }
        for mode in sample_sets
    },
    "factor_decomposition": factor_decomposition,
    "sealed_test": sealed_results,
    "exact_bbo_option_validation": option_results,
    "joint_direction_option_search": joint_option_results,
    "regime_event_discovery": regime_event_discovery,
    "denoising_state_benchmark": denoising_state_benchmark,
    "layered_followup_research": layered_followup_research,
    "limitations": [
        "The lake contains sampled near-tick quote updates, not every native exchange packet.",
        "Only 25 complete RTH and 26 complete GTH sessions are available; the sealed test has six sessions per mode.",
        "GTH predicts ES as an explicit proxy because SPX cash is not live overnight.",
        "Exact-BBO option P&L is evaluated only where historically subscribed contracts have fresh executable sides.",
        "The option quote universe was historically subscribed and may be selection-biased; exact-leg held-out validation must disclose coverage.",
    ],
}

SNAPSHOT_PATH = RESEARCH_OUTPUT_ROOT / "raw-tick-edge-discovery-2026-08-19.snapshot.json"
SNAPSHOT_PATH.parent.mkdir(parents=True, exist_ok=True)
SNAPSHOT_PATH.write_text(
    json.dumps(json_safe(snapshot), indent=2, ensure_ascii=False) + "\n",
    encoding="utf-8",
)
print(f"Wrote {SNAPSHOT_PATH}")
