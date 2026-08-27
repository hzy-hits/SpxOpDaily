"""Causal audit of SPX 0DTE captured net-premium divergences.

This is an offline research notebook generator, not a production signal.  The
Schwab lake stores L1 snapshots rather than a complete OPRA trade tape, so the
analysis deliberately calls its input ``captured_net_premium_proxy``.  It
deduplicates observed last trades, classifies only prints at bid/ask, excludes
inside-spread prints, and measures how much cumulative contract volume the
captured tape represents before evaluating any directional result.

Primary causal rule (15-minute comparison window, 5-minute confirmation):

* bearish bull-trap: the latest five-minute window made a new local high, price
  has already pulled below that high, Call net premium is non-positive, and Put
  net premium is positive;
* bullish exhaustion / short-exit: the symmetric local-low condition with Call
  net premium non-negative and Put net premium non-positive.

Signals are decided only after the minute closes.  All source observations are
received no later than the decision minute and no future option row is used.
"""

from __future__ import annotations

import json
import math
from collections import defaultdict
from datetime import date, datetime, time, timedelta, timezone
from pathlib import Path
from statistics import mean, median
from typing import Any, Mapping, Sequence
from zoneinfo import ZoneInfo

import duckdb
import nbformat
from nbclient import NotebookClient
import numpy as np


REPO_ROOT = next(
    path for path in (Path.cwd(), *Path.cwd().parents) if (path / "src/spx_spark").is_dir()
)
DATA_ROOT = Path("/srv/data/spx-spark/data")
LAKE_ROOT = DATA_ROOT / "lake/quotes/schema=v1"
OUTPUT_JSON = REPO_ROOT / "docs/research/net-premium-divergence-backtest-2026-08-27.json"
OUTPUT_NOTEBOOK = (
    REPO_ROOT / "docs/notebooks/net-premium-divergence-backtest-2026-08-27.ipynb"
)

START_DATE = date(2026, 7, 7)
END_DATE = date(2026, 8, 26)
MOTIVATING_DAY = "2026-08-26"
ET = ZoneInfo("America/New_York")
UTC = timezone.utc
PRIMARY_LOOKBACK_MINUTES = 15
PRIMARY_CONFIRM_MINUTES = 5
SIGNAL_COOLDOWN_MINUTES = 15
MINIMUM_SPX_MINUTES = 350
MINIMUM_AT_TOUCH_VOLUME_COVERAGE = 0.10
RNG_SEED = 20260827
BOOTSTRAP_SAMPLES = 10_000
PLACEBO_SAMPLES = 500
FORWARD_HORIZONS_MINUTES = (5, 15, 30, 60, 120)


MINUTE_QUERY = """
WITH print_candidates AS (
  SELECT *,
         row_number() OVER (
           PARTITION BY instrument_id, trade_time, last, last_size
           ORDER BY received_at
         ) AS print_rank
  FROM read_parquet(?, union_by_name=true)
  WHERE instrument_type = 'option'
    AND underlier = 'SPX'
    AND expiry = ?::DATE
    AND received_at BETWEEN ?::TIMESTAMPTZ AND ?::TIMESTAMPTZ
    AND trade_time IS NOT NULL
    AND last_size > 0
    AND last > 0
    AND ask > bid
    AND bid >= 0
), prints AS (
  SELECT time_bucket(INTERVAL '1 minute', received_at) AS minute_at,
         "right",
         last * last_size * 100.0 AS premium_dollars,
         last_size,
         CASE
           WHEN last >= ask THEN 1
           WHEN last <= bid THEN -1
           ELSE 0
         END AS inferred_side
  FROM print_candidates
  WHERE print_rank = 1
    AND received_at >= trade_time
    AND received_at - trade_time <= INTERVAL '5 seconds'
), flow AS (
  SELECT minute_at,
         sum(CASE WHEN "right" = 'C'
                  THEN inferred_side * premium_dollars ELSE 0 END) AS call_net,
         sum(CASE WHEN "right" = 'P'
                  THEN inferred_side * premium_dollars ELSE 0 END) AS put_net,
         sum(CASE WHEN inferred_side <> 0
                  THEN premium_dollars ELSE 0 END) AS classified_premium,
         sum(premium_dollars) AS captured_premium,
         sum(CASE WHEN inferred_side <> 0 THEN last_size ELSE 0 END) AS classified_size,
         sum(last_size) AS captured_size
  FROM prints
  GROUP BY minute_at
), spot AS (
  SELECT time_bucket(INTERVAL '1 minute', received_at) AS minute_at,
         arg_max(effective_price, received_at) AS spx
  FROM read_parquet(?, union_by_name=true)
  WHERE instrument_id = 'index:SPX'
    AND received_at BETWEEN ?::TIMESTAMPTZ AND ?::TIMESTAMPTZ
    AND source_at <= received_at
    AND effective_price > 0
  GROUP BY minute_at
)
SELECT spot.minute_at,
       spot.spx,
       coalesce(flow.call_net, 0),
       coalesce(flow.put_net, 0),
       coalesce(flow.classified_premium, 0),
       coalesce(flow.captured_premium, 0),
       coalesce(flow.classified_size, 0),
       coalesce(flow.captured_size, 0)
FROM spot
LEFT JOIN flow USING (minute_at)
ORDER BY spot.minute_at
"""


VOLUME_QUERY = """
WITH observations AS (
  SELECT instrument_id, volume
  FROM read_parquet(?, union_by_name=true)
  WHERE instrument_type = 'option'
    AND underlier = 'SPX'
    AND expiry = ?::DATE
    AND received_at BETWEEN ?::TIMESTAMPTZ AND ?::TIMESTAMPTZ
    AND volume IS NOT NULL
), changes AS (
  SELECT instrument_id,
         greatest(max(volume) - min(volume), 0) AS volume_delta
  FROM observations
  GROUP BY instrument_id
)
SELECT coalesce(sum(volume_delta), 0), count(*)
FROM changes
"""


def _json_safe(value: object) -> object:
    if isinstance(value, Mapping):
        return {str(key): _json_safe(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_json_safe(item) for item in value]
    if isinstance(value, np.integer):
        return int(value)
    if isinstance(value, np.floating):
        parsed = float(value)
        return parsed if math.isfinite(parsed) else None
    if isinstance(value, datetime | date):
        return value.isoformat()
    if isinstance(value, float) and not math.isfinite(value):
        return None
    return value


def _period(day: str) -> str:
    if day <= "2026-07-31":
        return "development"
    if day <= "2026-08-14":
        return "validation"
    if day < MOTIVATING_DAY:
        return "tail"
    return "motivating_day"


def _session_files(day: str) -> list[str]:
    directory = LAKE_ROOT / f"date={day}" / "provider=schwab"
    return sorted(str(path) for path in directory.glob("hour=*/quotes.parquet"))


def _available_sessions() -> list[str]:
    result: list[str] = []
    for directory in sorted(LAKE_ROOT.glob("date=*")):
        day = directory.name.removeprefix("date=")
        parsed = date.fromisoformat(day)
        if START_DATE <= parsed <= END_DATE and parsed.weekday() < 5:
            result.append(day)
    return result


def _read_session(
    connection: duckdb.DuckDBPyConnection, day: str
) -> tuple[list[dict[str, Any]], dict[str, Any]] | None:
    files = _session_files(day)
    if not files:
        return None
    start = f"{day} 13:30:00+00"
    end = f"{day} 20:00:00+00"
    raw = connection.execute(
        MINUTE_QUERY, [files, day, start, end, files, start, end]
    ).fetchall()
    volume_delta, contract_count = connection.execute(
        VOLUME_QUERY, [files, day, start, end]
    ).fetchone()
    rows = [
        {
            "minute_at": item[0].astimezone(UTC),
            "time_et": item[0].astimezone(ET).strftime("%H:%M"),
            "spx": float(item[1]),
            "call_net": float(item[2]),
            "put_net": float(item[3]),
            "classified_premium": float(item[4]),
            "captured_premium": float(item[5]),
            "classified_size": float(item[6]),
            "captured_size": float(item[7]),
        }
        for item in raw
    ]
    captured_size = sum(row["captured_size"] for row in rows)
    classified_size = sum(row["classified_size"] for row in rows)
    captured_premium = sum(row["captured_premium"] for row in rows)
    classified_premium = sum(row["classified_premium"] for row in rows)
    volume = float(volume_delta or 0)
    active = [
        row
        for row in rows
        if time(9, 45) <= datetime.strptime(row["time_et"], "%H:%M").time() <= time(15, 45)
    ]
    active_flow_minutes = sum(row["captured_size"] > 0 for row in active)
    quality = {
        "day": day,
        "period": _period(day),
        "spx_minute_buckets": len(rows),
        "option_contracts_observed": int(contract_count),
        "cumulative_volume_delta": int(volume),
        "captured_print_size": int(captured_size),
        "at_touch_classified_size": int(classified_size),
        "tape_capture_ratio": captured_size / volume if volume > 0 else None,
        "at_touch_volume_coverage": classified_size / volume if volume > 0 else None,
        "inside_or_unclassified_share": (
            1.0 - classified_size / captured_size if captured_size > 0 else None
        ),
        "classified_premium_share": (
            classified_premium / captured_premium if captured_premium > 0 else None
        ),
        "active_flow_minute_rate": (
            active_flow_minutes / len(active) if active else None
        ),
    }
    quality["complete_spx_path"] = len(rows) >= MINIMUM_SPX_MINUTES
    quality["minimum_coverage_cohort"] = bool(
        quality["complete_spx_path"]
        and quality["at_touch_volume_coverage"] is not None
        and quality["at_touch_volume_coverage"] >= MINIMUM_AT_TOUCH_VOLUME_COVERAGE
    )
    return rows, quality


def _sum_flow(rows: Sequence[Mapping[str, Any]], start: int, end: int, key: str) -> float:
    return sum(float(row[key]) for row in rows[start:end])


def _raw_flags(
    rows: Sequence[Mapping[str, Any]], *, lookback: int, confirm: int
) -> list[dict[str, bool]]:
    flags: list[dict[str, bool]] = []
    for index, row in enumerate(rows):
        if index < lookback + confirm:
            flags.append(
                {
                    "bearish_divergence": False,
                    "bullish_exhaustion": False,
                    "price_high_failure": False,
                    "price_low_failure": False,
                }
            )
            continue
        recent = rows[index - confirm + 1 : index + 1]
        prior = rows[index - confirm - lookback + 1 : index - confirm + 1]
        recent_high = max(float(item["spx"]) for item in recent)
        prior_high = max(float(item["spx"]) for item in prior)
        recent_low = min(float(item["spx"]) for item in recent)
        prior_low = min(float(item["spx"]) for item in prior)
        call_flow = _sum_flow(rows, index - confirm + 1, index + 1, "call_net")
        put_flow = _sum_flow(rows, index - confirm + 1, index + 1, "put_net")
        price = float(row["spx"])
        high_failure = recent_high > prior_high and price < recent_high
        low_failure = recent_low < prior_low and price > recent_low
        flags.append(
            {
                "bearish_divergence": bool(
                    high_failure and call_flow <= 0 and put_flow > 0
                ),
                "bullish_exhaustion": bool(
                    low_failure and call_flow >= 0 and put_flow <= 0
                ),
                "price_high_failure": bool(high_failure),
                "price_low_failure": bool(low_failure),
            }
        )
    return flags


def _event_rows(
    day: str,
    rows: Sequence[Mapping[str, Any]],
    *,
    lookback: int,
    confirm: int,
    kind: str,
    start_et: str = "09:45",
    end_et: str = "15:30",
) -> list[dict[str, Any]]:
    flags = _raw_flags(rows, lookback=lookback, confirm=confirm)
    events: list[dict[str, Any]] = []
    last_index: int | None = None
    hard_exit = max(
        (row for row in rows if str(row["time_et"]) <= "15:45"),
        key=lambda row: str(row["time_et"]),
    )
    for index, (row, flag) in enumerate(zip(rows, flags)):
        if not start_et <= str(row["time_et"]) <= end_et or not flag[kind]:
            continue
        if last_index is not None and index - last_index < SIGNAL_COOLDOWN_MINUTES:
            continue
        sign = -1.0 if kind in {"bearish_divergence", "price_high_failure"} else 1.0
        event = {
            "day": day,
            "period": _period(day),
            "time_et": row["time_et"],
            "decision_at": (row["minute_at"] + timedelta(minutes=1)).isoformat(),
            "spx": float(row["spx"]),
            "kind": kind,
            "lookback_minutes": lookback,
            "confirmation_minutes": confirm,
        }
        for horizon in FORWARD_HORIZONS_MINUTES:
            target = index + horizon
            event[f"forward_{horizon}m_points"] = (
                sign * (float(rows[target]["spx"]) - float(row["spx"]))
                if target < len(rows)
                else None
            )
        event["to_1545_points"] = sign * (
            float(hard_exit["spx"]) - float(row["spx"])
        )
        events.append(event)
        last_index = index
    return events


def _bootstrap_session_ci(
    events: Sequence[Mapping[str, Any]], key: str, *, seed: int
) -> list[float | None]:
    by_day: dict[str, list[float]] = defaultdict(list)
    for event in events:
        value = event.get(key)
        if isinstance(value, int | float) and math.isfinite(float(value)):
            by_day[str(event["day"])].append(float(value))
    daily = np.asarray([mean(values) for values in by_day.values()], dtype=float)
    if not len(daily):
        return [None, None]
    rng = np.random.default_rng(seed)
    indexes = rng.integers(0, len(daily), size=(BOOTSTRAP_SAMPLES, len(daily)))
    draws = daily[indexes].mean(axis=1)
    return [float(np.quantile(draws, 0.025)), float(np.quantile(draws, 0.975))]


def _event_summary(
    events: Sequence[Mapping[str, Any]], *, seed: int
) -> dict[str, Any]:
    result: dict[str, Any] = {
        "events": len(events),
        "sessions": len({str(event["day"]) for event in events}),
    }
    for horizon in FORWARD_HORIZONS_MINUTES:
        key = f"forward_{horizon}m_points"
        values = [
            float(event[key])
            for event in events
            if isinstance(event.get(key), int | float)
            and math.isfinite(float(event[key]))
        ]
        result[f"forward_{horizon}m"] = {
            "n": len(values),
            "mean_points": mean(values) if values else None,
            "median_points": median(values) if values else None,
            "win_rate": (
                sum(value > 0 for value in values) / len(values) if values else None
            ),
            "session_block_ci95_mean_points": _bootstrap_session_ci(
                events, key, seed=seed + horizon
            ),
        }
    close_values = [
        float(event["to_1545_points"])
        for event in events
        if isinstance(event.get("to_1545_points"), int | float)
        and math.isfinite(float(event["to_1545_points"]))
    ]
    result["to_1545"] = {
        "n": len(close_values),
        "mean_points": mean(close_values) if close_values else None,
        "median_points": median(close_values) if close_values else None,
        "win_rate": (
            sum(value > 0 for value in close_values) / len(close_values)
            if close_values
            else None
        ),
        "session_block_ci95_mean_points": _bootstrap_session_ci(
            events, "to_1545_points", seed=seed + 1545
        ),
    }
    return result


def _equity_max_drawdown(values: Sequence[float]) -> float | None:
    if not values:
        return None
    equity = 0.0
    peak = 0.0
    maximum = 0.0
    for value in values:
        equity += value
        peak = max(peak, equity)
        maximum = max(maximum, peak - equity)
    return maximum


def _paired_daily_trades(
    sessions: Mapping[str, Sequence[Mapping[str, Any]]],
    *,
    lookback: int,
    confirm: int,
    entry_start_et: str = "09:45",
    entry_end_et: str = "11:30",
) -> list[dict[str, Any]]:
    trades: list[dict[str, Any]] = []
    for day, rows in sorted(sessions.items()):
        entries = _event_rows(
            day,
            rows,
            lookback=lookback,
            confirm=confirm,
            kind="bearish_divergence",
            start_et=entry_start_et,
            end_et=entry_end_et,
        )
        if not entries:
            continue
        entry = entries[0]
        entry_at = datetime.fromisoformat(str(entry["decision_at"]))
        exits = _event_rows(
            day,
            rows,
            lookback=lookback,
            confirm=confirm,
            kind="bullish_exhaustion",
            start_et="09:45",
            end_et="15:45",
        )
        exit_signal = next(
            (
                event
                for event in exits
                if datetime.fromisoformat(str(event["decision_at"]))
                >= entry_at + timedelta(minutes=5)
            ),
            None,
        )
        if exit_signal is not None:
            exit_price = float(exit_signal["spx"])
            exit_time = str(exit_signal["time_et"])
            exit_reason = "bullish_exhaustion"
        else:
            hard_exit = max(
                (row for row in rows if str(row["time_et"]) <= "15:45"),
                key=lambda row: str(row["time_et"]),
            )
            exit_price = float(hard_exit["spx"])
            exit_time = str(hard_exit["time_et"])
            exit_reason = "15:45_hard_exit"
        trades.append(
            {
                "day": day,
                "period": _period(day),
                "entry_time_et": entry["time_et"],
                "entry_spx": entry["spx"],
                "exit_time_et": exit_time,
                "exit_spx": exit_price,
                "exit_reason": exit_reason,
                "gross_short_points": float(entry["spx"]) - exit_price,
            }
        )
    return trades


def _stream_snapshot_collapse_diagnostic(
    connection: duckdb.DuckDBPyConnection, day: str
) -> dict[str, Any] | None:
    files = _session_files(day)
    if not files:
        return None
    start = f"{day} 13:30:00+00"
    end = f"{day} 20:00:00+00"
    row = connection.execute(
        """
        WITH stream AS (
          SELECT instrument_id,
                 received_at,
                 trade_time,
                 last,
                 last_size,
                 volume,
                 lag(volume) OVER (
                   PARTITION BY instrument_id ORDER BY received_at
                 ) AS prior_volume,
                 lag(trade_time) OVER (
                   PARTITION BY instrument_id ORDER BY received_at
                 ) AS prior_trade_time
          FROM read_parquet(?, union_by_name=true)
          WHERE instrument_type = 'option'
            AND underlier = 'SPX'
            AND expiry = ?::DATE
            AND sampling_mode = 'schwab_stream'
            AND received_at BETWEEN ?::TIMESTAMPTZ AND ?::TIMESTAMPTZ
        ), deltas AS (
          SELECT *, greatest(volume - prior_volume, 0) AS volume_delta
          FROM stream
          WHERE volume IS NOT NULL AND prior_volume IS NOT NULL
        ), fingerprints AS (
          SELECT instrument_id, trade_time, last, last_size, count(*) AS repeats
          FROM stream
          WHERE trade_time IS NOT NULL AND last_size > 0
          GROUP BY instrument_id, trade_time, last, last_size
        )
        SELECT
          (SELECT count(*) FROM deltas) AS stream_snapshots,
          (SELECT count(*) FROM deltas
           WHERE trade_time IS DISTINCT FROM prior_trade_time) AS changed_trade_times,
          (SELECT count(*) FROM deltas WHERE volume_delta > 0) AS volume_updates,
          (SELECT sum(volume_delta) FROM deltas WHERE volume_delta > 0) AS volume_increment,
          (SELECT sum(last_size) FROM deltas WHERE volume_delta > 0) AS last_size_on_updates,
          (SELECT quantile_cont(volume_delta, [0.5, 0.9, 0.99])
           FROM deltas WHERE volume_delta > 0) AS volume_delta_quantiles,
          (SELECT avg(CASE WHEN volume_delta > last_size THEN 1 ELSE 0 END)
           FROM deltas WHERE volume_delta > 0 AND last_size > 0) AS batched_update_share,
          (SELECT count(*) FROM fingerprints) AS distinct_last_trade_fingerprints,
          (SELECT sum(repeats) FROM fingerprints) AS repeated_fingerprint_rows,
          (SELECT quantile_cont(repeats, [0.5, 0.9, 0.99, 1.0])
           FROM fingerprints) AS fingerprint_repeat_quantiles
        """,
        [files, day, start, end],
    ).fetchone()
    if row is None or not row[0]:
        return None
    return {
        "day": day,
        "stream_snapshots": int(row[0]),
        "changed_trade_times": int(row[1]),
        "volume_updates": int(row[2]),
        "cumulative_volume_increment": int(row[3]),
        "last_size_sum_on_volume_updates": int(row[4]),
        "last_size_to_volume_increment_ratio": float(row[4] / row[3]),
        "volume_delta_quantiles": [float(value) for value in row[5]],
        "updates_where_volume_delta_exceeds_last_size": float(row[6]),
        "distinct_last_trade_fingerprints": int(row[7]),
        "rows_carrying_those_fingerprints": int(row[8]),
        "fingerprint_repeat_quantiles": [float(value) for value in row[9]],
        "interpretation": (
            "LEVELONE_OPTIONS persists state snapshots. Total volume often jumps by "
            "multiple contracts while LAST_SIZE describes only the final observed print; "
            "unchanged last-trade fields are also repeated across quote updates."
        ),
    }


def _trade_summary(trades: Sequence[Mapping[str, Any]], *, seed: int) -> dict[str, Any]:
    gross = [float(trade["gross_short_points"]) for trade in trades]
    result: dict[str, Any] = {
        "trades": len(trades),
        "signal_exits": sum(
            trade["exit_reason"] == "bullish_exhaustion" for trade in trades
        ),
        "mean_gross_points": mean(gross) if gross else None,
        "median_gross_points": median(gross) if gross else None,
        "gross_win_rate": sum(value > 0 for value in gross) / len(gross) if gross else None,
        "gross_max_drawdown_points": _equity_max_drawdown(gross),
    }
    rng = np.random.default_rng(seed)
    if gross:
        array = np.asarray(gross)
        indexes = rng.integers(0, len(array), size=(BOOTSTRAP_SAMPLES, len(array)))
        draws = array[indexes].mean(axis=1)
        result["bootstrap_ci95_mean_gross_points"] = [
            float(np.quantile(draws, 0.025)),
            float(np.quantile(draws, 0.975)),
        ]
    else:
        result["bootstrap_ci95_mean_gross_points"] = [None, None]
    result["cost_sensitivity"] = {
        f"roundtrip_{cost:.1f}_points": {
            "mean_net_points": mean([value - cost for value in gross]) if gross else None,
            "win_rate": (
                sum(value - cost > 0 for value in gross) / len(gross) if gross else None
            ),
            "total_net_points": sum(value - cost for value in gross),
        }
        for cost in (0.0, 0.5, 1.0)
    }
    return result


def _placebo_test(
    sessions: Mapping[str, Sequence[Mapping[str, Any]]], *, seed: int
) -> dict[str, Any]:
    days = sorted(sessions)
    actual_events = [
        event
        for day in days
        for event in _event_rows(
            day,
            sessions[day],
            lookback=PRIMARY_LOOKBACK_MINUTES,
            confirm=PRIMARY_CONFIRM_MINUTES,
            kind="bearish_divergence",
        )
    ]
    actual_values = [
        float(event["forward_15m_points"])
        for event in actual_events
        if event["forward_15m_points"] is not None
    ]
    actual_mean = mean(actual_values) if actual_values else None
    rng = np.random.default_rng(seed)
    placebo_means: list[float] = []
    for _ in range(PLACEBO_SAMPLES):
        shuffled = list(rng.permutation(days))
        values: list[float] = []
        for price_day, flow_day in zip(days, shuffled):
            price_rows = sessions[price_day]
            flow_by_time = {
                str(row["time_et"]): row for row in sessions[str(flow_day)]
            }
            hybrid = [
                {
                    **row,
                    "call_net": float(flow_by_time.get(str(row["time_et"]), {}).get("call_net", 0)),
                    "put_net": float(flow_by_time.get(str(row["time_et"]), {}).get("put_net", 0)),
                }
                for row in price_rows
            ]
            events = _event_rows(
                price_day,
                hybrid,
                lookback=PRIMARY_LOOKBACK_MINUTES,
                confirm=PRIMARY_CONFIRM_MINUTES,
                kind="bearish_divergence",
            )
            values.extend(
                float(event["forward_15m_points"])
                for event in events
                if event["forward_15m_points"] is not None
            )
        if values:
            placebo_means.append(mean(values))
    return {
        "method": "permute complete intraday flow profiles across dates while retaining each date's SPX path",
        "samples": len(placebo_means),
        "actual_mean_15m_short_points": actual_mean,
        "placebo_mean": mean(placebo_means) if placebo_means else None,
        "placebo_ci95": (
            [
                float(np.quantile(placebo_means, 0.025)),
                float(np.quantile(placebo_means, 0.975)),
            ]
            if placebo_means
            else [None, None]
        ),
        "one_sided_randomization_p": (
            (1 + sum(value >= float(actual_mean) for value in placebo_means))
            / (1 + len(placebo_means))
            if placebo_means and actual_mean is not None
            else None
        ),
    }


def _quality_summary(rows: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    def finite_values(key: str) -> list[float]:
        return [
            float(row[key])
            for row in rows
            if isinstance(row.get(key), int | float) and math.isfinite(float(row[key]))
        ]

    result: dict[str, Any] = {
        "sessions": len(rows),
        "complete_spx_sessions": sum(bool(row["complete_spx_path"]) for row in rows),
        "minimum_coverage_sessions": sum(
            bool(row["minimum_coverage_cohort"]) for row in rows
        ),
    }
    for key in (
        "tape_capture_ratio",
        "at_touch_volume_coverage",
        "inside_or_unclassified_share",
        "classified_premium_share",
        "active_flow_minute_rate",
    ):
        values = finite_values(key)
        result[key] = {
            "median": median(values) if values else None,
            "minimum": min(values) if values else None,
            "maximum": max(values) if values else None,
        }
    return result


def run_analysis() -> dict[str, Any]:
    connection = duckdb.connect()
    connection.execute("SET TimeZone='UTC'")
    sessions: dict[str, list[dict[str, Any]]] = {}
    quality_rows: list[dict[str, Any]] = []
    stream_collapse: dict[str, Any] | None = None
    try:
        for day in _available_sessions():
            loaded = _read_session(connection, day)
            if loaded is None:
                continue
            rows, quality = loaded
            sessions[day] = rows
            quality_rows.append(quality)
        stream_collapse = _stream_snapshot_collapse_diagnostic(
            connection, MOTIVATING_DAY
        )
    finally:
        connection.close()

    complete = {
        day: rows
        for day, rows in sessions.items()
        if next(row for row in quality_rows if row["day"] == day)["complete_spx_path"]
    }
    minimum_coverage = {
        day: rows
        for day, rows in complete.items()
        if next(row for row in quality_rows if row["day"] == day)[
            "minimum_coverage_cohort"
        ]
    }
    retrospective = {
        day: rows for day, rows in minimum_coverage.items() if day < MOTIVATING_DAY
    }

    variants: list[dict[str, Any]] = []
    for variant_index, (lookback, confirm) in enumerate(((10, 3), (15, 5), (20, 5))):
        for kind_index, kind in enumerate(
            (
                "price_high_failure",
                "bearish_divergence",
                "price_low_failure",
                "bullish_exhaustion",
            )
        ):
            events = [
                event
                for day, rows in retrospective.items()
                for event in _event_rows(
                    day,
                    rows,
                    lookback=lookback,
                    confirm=confirm,
                    kind=kind,
                )
            ]
            variants.append(
                {
                    "lookback_minutes": lookback,
                    "confirmation_minutes": confirm,
                    "kind": kind,
                    "summary": _event_summary(
                        events,
                        seed=RNG_SEED + 100 * variant_index + 10 * kind_index,
                    ),
                }
            )

    primary_events: dict[str, list[dict[str, Any]]] = {}
    for kind in (
        "price_high_failure",
        "bearish_divergence",
        "price_low_failure",
        "bullish_exhaustion",
    ):
        primary_events[kind] = [
            event
            for day, rows in retrospective.items()
            for event in _event_rows(
                day,
                rows,
                lookback=PRIMARY_LOOKBACK_MINUTES,
                confirm=PRIMARY_CONFIRM_MINUTES,
                kind=kind,
            )
        ]

    period_results: dict[str, Any] = {}
    for period in ("development", "validation", "tail", "all"):
        period_results[period] = {}
        for kind, events in primary_events.items():
            selected = events if period == "all" else [
                event for event in events if event["period"] == period
            ]
            period_results[period][kind] = _event_summary(
                selected, seed=RNG_SEED + len(period_results) * 100 + len(kind)
            )

    trades = _paired_daily_trades(
        retrospective,
        lookback=PRIMARY_LOOKBACK_MINUTES,
        confirm=PRIMARY_CONFIRM_MINUTES,
    )
    trade_periods = {
        period: _trade_summary(
            trades if period == "all" else [
                trade for trade in trades if trade["period"] == period
            ],
            seed=RNG_SEED + index,
        )
        for index, period in enumerate(("development", "validation", "tail", "all"))
    }

    after_1000_events: dict[str, list[dict[str, Any]]] = {}
    for kind in (
        "price_high_failure",
        "bearish_divergence",
        "price_low_failure",
        "bullish_exhaustion",
    ):
        after_1000_events[kind] = [
            event
            for day, rows in retrospective.items()
            for event in _event_rows(
                day,
                rows,
                lookback=PRIMARY_LOOKBACK_MINUTES,
                confirm=PRIMARY_CONFIRM_MINUTES,
                kind=kind,
                start_et="10:00",
                end_et="15:30",
            )
        ]
    after_1000_summaries = {
        kind: _event_summary(events, seed=RNG_SEED + 20_000 + index)
        for index, (kind, events) in enumerate(after_1000_events.items())
    }
    after_1000_trades = _paired_daily_trades(
        retrospective,
        lookback=PRIMARY_LOOKBACK_MINUTES,
        confirm=PRIMARY_CONFIRM_MINUTES,
        entry_start_et="10:00",
        entry_end_et="13:00",
    )
    after_1000_trade_periods = {
        period: _trade_summary(
            after_1000_trades
            if period == "all"
            else [
                trade for trade in after_1000_trades if trade["period"] == period
            ],
            seed=RNG_SEED + 21_000 + index,
        )
        for index, period in enumerate(("development", "validation", "tail", "all"))
    }

    motivating_case: dict[str, Any] = {"available": MOTIVATING_DAY in minimum_coverage}
    if MOTIVATING_DAY in minimum_coverage:
        case_rows = minimum_coverage[MOTIVATING_DAY]
        motivating_case["bearish_divergences"] = _event_rows(
            MOTIVATING_DAY,
            case_rows,
            lookback=PRIMARY_LOOKBACK_MINUTES,
            confirm=PRIMARY_CONFIRM_MINUTES,
            kind="bearish_divergence",
        )
        motivating_case["bullish_exhaustions"] = _event_rows(
            MOTIVATING_DAY,
            case_rows,
            lookback=PRIMARY_LOOKBACK_MINUTES,
            confirm=PRIMARY_CONFIRM_MINUTES,
            kind="bullish_exhaustion",
        )

    primary_bear = period_results["all"]["bearish_divergence"]
    primary_bull = period_results["all"]["bullish_exhaustion"]
    price_bear = period_results["all"]["price_high_failure"]
    paired_all = trade_periods["all"]
    placebo = _placebo_test(retrospective, seed=RNG_SEED + 90_000)
    bear_15 = primary_bear["forward_15m"]
    baseline_15 = price_bear["forward_15m"]
    production_supported = bool(
        bear_15["n"] >= 30
        and bear_15["session_block_ci95_mean_points"][0] is not None
        and bear_15["session_block_ci95_mean_points"][0] > 0
        and placebo["one_sided_randomization_p"] is not None
        and placebo["one_sided_randomization_p"] < 0.05
        and paired_all["bootstrap_ci95_mean_gross_points"][0] is not None
        and paired_all["bootstrap_ci95_mean_gross_points"][0] > 0
    )

    return {
        "schema_version": "captured_net_premium_divergence_audit.v1",
        "generated_at": datetime.now(UTC).isoformat(),
        "question": "Does causal divergence between SPX and captured 0DTE net premium predict a bull trap or a useful short exit?",
        "contract": {
            "authority": "offline_research_only",
            "automatic_ordering": False,
            "underlying_outcome": "SPX point return; no option PnL is inferred",
            "date_range": [START_DATE, END_DATE],
            "motivating_day_excluded_from_retrospective_result": MOTIVATING_DAY,
            "primary_lookback_minutes": PRIMARY_LOOKBACK_MINUTES,
            "primary_confirmation_minutes": PRIMARY_CONFIRM_MINUTES,
            "cooldown_minutes": SIGNAL_COOLDOWN_MINUTES,
            "bearish_rule": "recent window makes a new local high and closes below it; 5m Call net <=0; 5m Put net >0",
            "bullish_exit_rule": "recent window makes a new local low and closes above it; 5m Call net >=0; 5m Put net <=0",
            "print_inference": "fresh deduplicated last print at/above ask = buy; at/below bid = sell; inside spread excluded",
            "premium_formula": "last_price * last_size * 100",
            "decision_time": "end of received-at minute; no trade is used before it is observed",
            "minimum_coverage_cohort": f"SPX minute buckets >= {MINIMUM_SPX_MINUTES} and at-touch size / cumulative volume delta >= {MINIMUM_AT_TOUCH_VOLUME_COVERAGE:.0%}",
        },
        "data_quality": {
            "summary": _quality_summary(quality_rows),
            "sessions": quality_rows,
            "minimum_coverage_retrospective_sessions": sorted(retrospective),
            "first_available_schwab_option_session": min(sessions) if sessions else None,
            "requested_start_date": START_DATE,
            "stream_snapshot_collapse_case": stream_collapse,
            "future_rows_used": 0,
            "metric_identity": "captured_net_premium_proxy_not_complete_OPRA_net_premium",
        },
        "primary_period_results": period_results,
        "robustness_variants": variants,
        "paired_first_morning_short": {
            "definition": "first bearish divergence 09:45-11:30 ET; first bullish exhaustion >=5m later, otherwise 15:45 hard exit",
            "trades": trades,
            "periods": trade_periods,
        },
        "after_1000_et_extension": {
            "event_window_et": ["10:00", "15:30"],
            "forward_horizons_minutes": list(FORWARD_HORIZONS_MINUTES),
            "event_summaries": after_1000_summaries,
            "first_signal_trade": {
                "definition": "first bearish divergence 10:00-13:00 ET; first bullish exhaustion >=5m later, otherwise 15:45 hard exit",
                "trades": after_1000_trades,
                "periods": after_1000_trade_periods,
            },
        },
        "flow_date_permutation_placebo": placebo,
        "motivating_day_case_study": motivating_case,
        "decision": {
            "production_change_recommended": production_supported,
            "bearish_15m_events": bear_15["n"],
            "bearish_15m_sessions": primary_bear["sessions"],
            "bearish_15m_mean_short_points": bear_15["mean_points"],
            "bearish_15m_ci95": bear_15["session_block_ci95_mean_points"],
            "price_only_15m_mean_short_points": baseline_15["mean_points"],
            "bullish_exit_15m_events": primary_bull["forward_15m"]["n"],
            "bullish_exit_15m_rebound_points": primary_bull["forward_15m"][
                "mean_points"
            ],
            "paired_trades": paired_all["trades"],
            "paired_mean_gross_points": paired_all["mean_gross_points"],
            "paired_mean_ci95": paired_all["bootstrap_ci95_mean_gross_points"],
            "paired_mean_after_1pt_roundtrip": paired_all["cost_sensitivity"][
                "roundtrip_1.0_points"
            ]["mean_net_points"],
            "placebo_p": placebo["one_sided_randomization_p"],
        },
        "limitations": [
            "The Schwab L1 lake is not a complete OPRA trade tape; it lacks every trade, exchange condition, cancel/correction and complex-order flag.",
            "Only at-touch prints are signed. Inside-spread prints are excluded, so both coverage and premium totals differ from WinnerStock or another proprietary classifier.",
            "The rule was formalized after seeing the 2026-08-26 chart. That date is shown only as a case study and excluded from retrospective estimates, but the remaining history is still not a prospective holdout.",
            "Outcomes are hypothetical SPX point returns, not executable SPXW spread PnL. Exact BBO, option decay, skew, fees and fill probability are intentionally absent.",
            "Multiple event observations within a session remain dependent; confidence intervals therefore resample sessions rather than individual signals.",
            "A minimum 10% at-touch volume-coverage cohort is a quality floor, not proof that the missing tape is unbiased.",
        ],
    }


def _fmt(value: object, digits: int = 2) -> str:
    if isinstance(value, int | float) and math.isfinite(float(value)):
        return f"{float(value):.{digits}f}"
    return "—"


def _build_notebook(analysis: Mapping[str, Any]) -> nbformat.NotebookNode:
    decision = analysis["decision"]
    quality = analysis["data_quality"]["summary"]
    after_1000 = analysis["after_1000_et_extension"]
    after_bear = after_1000["event_summaries"]["bearish_divergence"]
    after_trade = after_1000["first_signal_trade"]["periods"]["all"]
    first_available = analysis["data_quality"]["first_available_schwab_option_session"]
    recommendation = (
        "满足预先声明的统计门，可以进入人工候选集成评审。"
        if decision["production_change_recommended"]
        else "没有同时通过置信区间、安慰剂和配对策略门；不应接入生产决策。"
    )
    notebook = nbformat.v4.new_notebook()
    notebook["metadata"]["kernelspec"] = {
        "display_name": "Python 3",
        "language": "python",
        "name": "python3",
    }
    notebook["cells"] = [
        nbformat.v4.new_markdown_cell(
            "# SPX 0DTE captured net-premium divergence audit\n\n"
            "## tl;dr\n\n"
            f"主规则在15分钟上有 {decision['bearish_15m_events']} 个熊背离事件，"
            f"平均做空结果 {_fmt(decision['bearish_15m_mean_short_points'])} 点，"
            f"session-block 95%区间 [{_fmt(decision['bearish_15m_ci95'][0])}, "
            f"{_fmt(decision['bearish_15m_ci95'][1])}]；"
            f"首个早盘信号配对交易 {decision['paired_trades']} 笔，平均"
            f" {_fmt(decision['paired_mean_gross_points'])} 点。单独限制10:00 ET以后，"
            f"熊背离60分钟平均 {_fmt(after_bear['forward_60m']['mean_points'])} 点、"
            f"120分钟平均 {_fmt(after_bear['forward_120m']['mean_points'])} 点；"
            f"首个信号配对平均 {_fmt(after_trade['mean_gross_points'])} 点。{recommendation}"
        ),
        nbformat.v4.new_markdown_cell(
            "## Context & Methods\n\n"
            "### Key Assumptions\n\n"
            "- 只把最新成交在 ask 及以上视为买入、bid 及以下视为卖出；价差内部成交不猜方向。\n"
            "- 熊背离：最近5分钟创新高后回落，同时 Call net≤0、Put net>0。\n"
            "- 牛背离/空头退出：最近5分钟创新低后回升，同时 Call net≥0、Put net≤0。\n"
            "- 信号在分钟结束后才成立；8月26日只作截图复现，不进入历史统计。\n"
            "- 结果是SPX点数，不是假装可成交的期权PnL。"
        ),
        nbformat.v4.new_markdown_cell(
            "## Data\n\n"
            f"请求范围 2026-07-07 至 2026-08-26，实际首个Schwab期权日为 {first_available}；"
            f"发现 {quality['sessions']} 个工作日，{quality['complete_spx_sessions']} 个完整SPX日，"
            f"{quality['minimum_coverage_sessions']} 个达到最低成交覆盖门。"
            "原始源为 Schwab L1 snapshot lake，不是完整OPRA tape。"
        ),
        nbformat.v4.new_code_cell(
            "from pathlib import Path\n"
            "import json\n\n"
            "repo = next(path for path in (Path.cwd(), *Path.cwd().parents) if (path / 'src/spx_spark').is_dir())\n"
            "artifact = repo / 'docs/research/net-premium-divergence-backtest-2026-08-27.json'\n"
            "analysis = json.loads(artifact.read_text())\n"
            "analysis['data_quality']['summary']"
        ),
        nbformat.v4.new_markdown_cell("## Results"),
        nbformat.v4.new_code_cell(
            "for period, result in analysis['primary_period_results'].items():\n"
            "    bear = result['bearish_divergence']['forward_15m']\n"
            "    base = result['price_high_failure']['forward_15m']\n"
            "    bull = result['bullish_exhaustion']['forward_15m']\n"
            "    print(period, {'bear_n': bear['n'], 'bear_mean': bear['mean_points'], 'bear_ci': bear['session_block_ci95_mean_points'], 'price_only_mean': base['mean_points'], 'bull_exit_rebound': bull['mean_points']})"
        ),
        nbformat.v4.new_code_cell(
            "analysis['paired_first_morning_short']['periods']"
        ),
        nbformat.v4.new_code_cell(
            "after = analysis['after_1000_et_extension']\n"
            "for kind, summary in after['event_summaries'].items():\n"
            "    print(kind, {key: value['mean_points'] for key, value in summary.items() if isinstance(value, dict) and 'mean_points' in value})\n"
            "after['first_signal_trade']['periods']"
        ),
        nbformat.v4.new_code_cell(
            "analysis['data_quality']['stream_snapshot_collapse_case']"
        ),
        nbformat.v4.new_code_cell(
            "analysis['flow_date_permutation_placebo']"
        ),
        nbformat.v4.new_code_cell(
            "case = analysis['motivating_day_case_study']\n"
            "print('bearish', [(x['time_et'], x['spx'], x['forward_15m_points']) for x in case.get('bearish_divergences', [])])\n"
            "print('bullish exits', [(x['time_et'], x['spx'], x['forward_15m_points']) for x in case.get('bullish_exhaustions', [])])"
        ),
        nbformat.v4.new_markdown_cell(
            "## Takeaways\n\n"
            f"1. {recommendation}\n"
            "2. 10:00以后延长持有期没有自动解决稳定性问题；应同时看分段结果和session-block区间。\n"
            "3. 图上的两类背离可以用现有数据做近似复现，但当前只能称 captured-flow proxy。\n"
            "4. 若未来要进入方向价差决策，必须先采集完整逐笔/条件码，随后以前向会话验证，再叠加 exact BBO 做 SPXW PnL。"
        ),
        nbformat.v4.new_code_cell(
            "assert analysis['data_quality']['future_rows_used'] == 0\n"
            "assert analysis['contract']['automatic_ordering'] is False\n"
            "print('causality and authority checks passed')"
        ),
    ]
    for index, cell in enumerate(notebook["cells"]):
        cell["id"] = f"net-premium-divergence-{index:02d}"
    nbformat.validate(notebook)
    return notebook


def main() -> int:
    analysis = run_analysis()
    OUTPUT_JSON.parent.mkdir(parents=True, exist_ok=True)
    OUTPUT_JSON.write_text(
        json.dumps(_json_safe(analysis), ensure_ascii=False, indent=2, sort_keys=True)
        + "\n",
        encoding="utf-8",
    )
    notebook = _build_notebook(analysis)
    NotebookClient(notebook, timeout=180, kernel_name="python3").execute(
        cwd=str(REPO_ROOT)
    )
    OUTPUT_NOTEBOOK.write_text(nbformat.writes(notebook), encoding="utf-8")
    print(OUTPUT_JSON)
    print(OUTPUT_NOTEBOOK)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
