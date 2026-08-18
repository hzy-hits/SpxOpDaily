"""Offline replay of persisted RTH ES-volume/momentum facts against SPXW NBBO."""

from __future__ import annotations

import argparse
from collections import Counter, defaultdict
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
import json
import math
from pathlib import Path
import sqlite3
from typing import Any

import numpy as np

from spx_spark.analytics.options.strategy_payoff import (
    DEFAULT_MANAGEMENT_POLICY,
    PolicyMark,
    conservative_vertical_bbo,
    policy_mark_horizon_end,
    simulate_management_policy,
)
from spx_spark.application.order_map.strategy_regime import DEFAULT_STRATEGY_POLICY
from spx_spark.data_platform.research.odte_level_quotes import QuoteStore
from spx_spark.data_platform.research.odte_level_signals import OptionTick

SCHEMA_VERSION = "es_volume_momentum_backtest.v1"
STICK_SECONDS = 900.0
PROVIDERS = ("schwab", "ibkr")
VALID_EXITS = frozenset({"premium_stop", "trail", "hard_close"})
FUNNEL_NAMES = (
    "facts_rth_minutes",
    "elevated_minutes",
    "directional_minutes",
    "aligned_1m_5m_minutes",
    "strong_momentum_minutes",
    "atr_available_minutes",
    "not_too_late_minutes",
)


@dataclass(frozen=True, slots=True)
class FactObservation:
    session_date: str
    decision_at: datetime
    label: str | None
    direction: str | None
    pace_ratio: float | None
    return_1m_points: float | None
    return_5m_points: float | None
    atr_5m: float | None
    spx: float | None

    @property
    def minute(self) -> datetime:
        return self.decision_at.replace(second=0, microsecond=0)


@dataclass(frozen=True, slots=True)
class Geometry:
    cohort: str
    row_id: str
    session_date: date
    decision_at: datetime
    direction: str
    right: str
    long_strike: float
    short_strike: float
    fixed_provider: str | None
    metadata: Mapping[str, Any]


def _number(value: object) -> float | None:
    number = float(value) if isinstance(value, int | float) and not isinstance(value, bool) else None
    return number if number is not None and math.isfinite(number) else None


def _time(value: object) -> datetime | None:
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError:
        return None
    return parsed.astimezone(timezone.utc) if parsed.tzinfo else None


def _connect_ro(path: Path) -> sqlite3.Connection:
    connection = sqlite3.connect(f"file:{path.resolve()}?mode=ro", uri=True)
    connection.execute("PRAGMA query_only=ON")
    return connection


def read_fact_observations(
    database_path: Path, *, start_date: date, end_date: date
) -> list[FactObservation]:
    connection = _connect_ro(database_path)
    try:
        rows = connection.execute(
            """
            SELECT session_date, decision_at,
              json_extract(attributes_json,'$.market_facts.es_volume.label'),
              json_extract(attributes_json,'$.market_facts.es_volume.direction'),
              json_extract(attributes_json,'$.market_facts.es_volume.pace_ratio'),
              json_extract(attributes_json,'$.market_facts.path.return_1m_points'),
              json_extract(attributes_json,'$.market_facts.path.return_5m_points'),
              json_extract(attributes_json,'$.market_facts.path.atr_5m'),
              json_extract(attributes_json,'$.market_facts.spot.spx')
            FROM decisions
            WHERE strategy_name='strategy_signal_engine_v2'
              AND session_date BETWEEN ? AND ?
              AND json_extract(attributes_json,'$.market_facts.session.mode')='rth'
            ORDER BY session_date, decision_at, decision_id
            """,
            (start_date.isoformat(), end_date.isoformat()),
        ).fetchall()
    finally:
        connection.close()
    result = []
    for raw in rows:
        at = _time(raw[1])
        if at is not None:
            result.append(
                FactObservation(
                    str(raw[0]),
                    at,
                    str(raw[2]) if raw[2] is not None else None,
                    str(raw[3]).lower() if raw[3] is not None else None,
                    *(_number(value) for value in raw[4:]),
                )
            )
    return result


def _gate_stage(row: FactObservation) -> int:
    if row.label != "elevated":
        return 1
    if row.direction not in {"up", "down"}:
        return 2
    one, five = row.return_1m_points, row.return_5m_points
    if one is None or five is None:
        return 3
    aligned = (row.direction == "up" and one > 0 and five > 0) or (
        row.direction == "down" and one < 0 and five < 0
    )
    if not aligned:
        return 3
    if abs(one) < DEFAULT_STRATEGY_POLICY.es_momentum_min_return_1m or abs(
        five
    ) < DEFAULT_STRATEGY_POLICY.es_momentum_min_return_5m:
        return 4
    if row.atr_5m is None or row.atr_5m <= 0:
        return 5
    if abs(five) / row.atr_5m > DEFAULT_STRATEGY_POLICY.es_momentum_max_return_5m_atr:
        return 6
    return 7


def build_funnel(
    observations: Sequence[FactObservation],
) -> tuple[dict[str, int], list[FactObservation], dict[str, Any]]:
    """Count unique UTC minutes and use the first fully passing print per minute."""

    grouped: defaultdict[tuple[str, datetime], list[FactObservation]] = defaultdict(list)
    for row in observations:
        grouped[(row.session_date, row.minute)].append(row)
    counts = {
        name: sum(any(_gate_stage(row) >= stage for row in rows) for rows in grouped.values())
        for stage, name in enumerate(FUNNEL_NAMES, 1)
    }
    openings = [
        next(row for row in rows if _gate_stage(row) == 7)
        for _, rows in sorted(grouped.items())
        if any(_gate_stage(row) == 7 for row in rows)
    ]
    varied = sum(
        len(
            {
                (
                    row.label,
                    row.direction,
                    row.pace_ratio,
                    row.return_1m_points,
                    row.return_5m_points,
                    row.atr_5m,
                    row.spx,
                )
                for row in rows
            }
        )
        > 1
        for rows in grouped.values()
    )
    quality = {
        "raw_rth_fact_rows": len(observations),
        "unique_rth_fact_minutes": len(grouped),
        "minutes_with_intraminute_fact_variation": varied,
        "fact_session_dates": sorted({row.session_date for row in observations}),
        "first_qualifying_observation_per_open_minute": True,
    }
    return counts, openings, quality


def apply_direction_stick(
    openings: Sequence[FactObservation], *, stick_seconds: float = STICK_SECONDS
) -> list[FactObservation]:
    accepted, last = [], {}
    for row in sorted(openings, key=lambda value: value.decision_at):
        if row.direction not in {"up", "down"}:
            continue
        key = (row.session_date, row.direction)
        if key not in last or (row.decision_at - last[key]).total_seconds() >= stick_seconds:
            accepted.append(row)
            last[key] = row.decision_at
    return accepted


def _selected_geometry(decision_id: str, raw_at: str, raw_json: str) -> Geometry | None:
    attributes, at = json.loads(raw_json), _time(raw_at)
    candidate = attributes.get("candidate", {})
    long_leg, short_leg = candidate.get("long", {}), candidate.get("short", {})
    long_strike, short_strike = _number(long_leg.get("strike")), _number(short_leg.get("strike"))
    try:
        session = date.fromisoformat(str(attributes.get("session_date")))
    except ValueError:
        return None
    right = str(candidate.get("right") or "").upper()
    provider = str(candidate.get("quote", {}).get("provider") or "").lower()
    if at is None or None in {long_strike, short_strike} or right not in {"C", "P"} or not provider:
        return None
    return Geometry(
        "selected_card",
        decision_id,
        session,
        at,
        str(candidate.get("direction") or ""),
        right,
        float(long_strike),
        float(short_strike),
        provider,
        {
            "decision_id": decision_id,
            "opportunity_id": candidate.get("opportunity_id"),
            "persisted_entry_ask": _number(candidate.get("quote", {}).get("ask")),
            "setup_kind": "ES_VOLUME_MOMENTUM",
        },
    )


def read_selected_geometries(
    database_path: Path, *, start_date: date, end_date: date
) -> list[Geometry]:
    connection = _connect_ro(database_path)
    try:
        rows = connection.execute(
            """
            SELECT decision_id, decision_at, attributes_json FROM decisions
            WHERE strategy_name='strategy_signal_engine_v2'
              AND session_date BETWEEN ? AND ? AND status='selected'
              AND json_extract(attributes_json,'$.candidate.setup_kind')='ES_VOLUME_MOMENTUM'
            ORDER BY decision_at, decision_id
            """,
            (start_date.isoformat(), end_date.isoformat()),
        ).fetchall()
    finally:
        connection.close()
    return [geometry for row in rows if (geometry := _selected_geometry(*row)) is not None]


def factor_geometries(openings: Sequence[FactObservation]) -> list[Geometry]:
    result = []
    for row in openings:
        if row.spx is None or row.direction not in {"up", "down"}:
            continue
        atm = math.floor(row.spx / 5.0) * 5.0
        right, step = ("C", 10.0) if row.direction == "up" else ("P", -10.0)
        result.append(
            Geometry(
                "factor_signal",
                f"factor:{row.session_date}:{row.minute.isoformat()}:{row.direction}",
                date.fromisoformat(row.session_date),
                row.decision_at,
                row.direction.upper(),
                right,
                atm,
                atm + step,
                None,
                {
                    "floor_utc_minute": row.minute.isoformat(),
                    "spx": row.spx,
                    "pace_ratio": row.pace_ratio,
                    "return_1m_points": row.return_1m_points,
                    "return_5m_points": row.return_5m_points,
                    "atr_5m": row.atr_5m,
                    "return_5m_atr": abs(row.return_5m_points) / row.atr_5m,
                },
            )
        )
    return result


def _quote(long: OptionTick, short: OptionTick, provider: str, now: datetime) -> dict[str, Any]:
    def leg(tick: OptionTick) -> dict[str, Any]:
        return {
            "provider": provider,
            "bid": tick.bid,
            "ask": tick.ask,
            "source_at": (tick.source_at or tick.at).isoformat(),
        }

    return conservative_vertical_bbo(
        leg(long),
        leg(short),
        now=now,
        max_quote_age_seconds=DEFAULT_STRATEGY_POLICY.quote_max_age_seconds,
        max_source_skew_seconds=DEFAULT_STRATEGY_POLICY.quote_max_skew_seconds,
    )


def _marks(
    store: QuoteStore, geometry: Geometry, provider: str, end: datetime
) -> tuple[list[PolicyMark], bool]:
    series = [
        store.option_series(
            provider=provider,
            expiry=geometry.session_date,
            strike=strike,
            right=geometry.right,
            start=geometry.decision_at,
            end=end,
        )
        for strike in (geometry.long_strike, geometry.short_strike)
    ]
    if any(not ticks for ticks in series):
        return [], False
    latest: list[OptionTick | None] = [None, None]
    cursors, marks = [0, 0], []
    for at in sorted({tick.at for ticks in series for tick in ticks}):
        for index, ticks in enumerate(series):
            while cursors[index] < len(ticks) and ticks[cursors[index]].at <= at:
                latest[index] = ticks[cursors[index]]
                cursors[index] += 1
        if latest[0] is not None and latest[1] is not None:
            bbo = _quote(latest[0], latest[1], provider, at)
            if bbo.get("status") == "ready":
                marks.append(PolicyMark(at, float(bbo["bid"])))
    close = _quote(series[0][-1], series[1][-1], provider, end)
    close_ready = close.get("status") == "ready"
    if close_ready:
        marks.append(PolicyMark(end, float(close["bid"])))
    return marks, close_ready


def _drop(base: Mapping[str, Any], reason: str, **extra: Any) -> dict[str, Any]:
    return {**base, **extra, "label_status": "dropped", "drop_reason": reason}


def _label_one(store: QuoteStore, geometry: Geometry, end: datetime) -> dict[str, Any]:
    base = {
        "schema_version": f"{SCHEMA_VERSION}.row",
        "cohort": geometry.cohort,
        "row_id": geometry.row_id,
        "session_date": geometry.session_date.isoformat(),
        "decision_at": geometry.decision_at.isoformat(),
        "direction": geometry.direction,
        "right": geometry.right,
        "long_strike": geometry.long_strike,
        "short_strike": geometry.short_strike,
        "width_points": abs(geometry.short_strike - geometry.long_strike),
        **geometry.metadata,
    }
    snapshot = store.option_snapshot(
        expiry=geometry.session_date,
        as_of=geometry.decision_at,
        max_age_seconds=DEFAULT_STRATEGY_POLICY.quote_max_age_seconds,
        strikes=(geometry.long_strike, geometry.short_strike),
    )
    providers = (geometry.fixed_provider,) if geometry.fixed_provider else PROVIDERS
    selected = None
    reasons: Counter[str] = Counter()
    for provider in providers:
        long = snapshot.get((provider, float(geometry.long_strike), geometry.right))
        short = snapshot.get((provider, float(geometry.short_strike), geometry.right))
        if long is None or short is None:
            reasons["entry_leg_live_nbbo_unavailable"] += 1
            continue
        bbo = _quote(long, short, provider, geometry.decision_at)
        if bbo.get("status") == "ready":
            selected = provider, bbo
            break
        reasons.update(map(str, bbo.get("reasons") or ()))
    if selected is None:
        return _drop(base, "entry_bbo_unavailable", entry_reasons=dict(reasons))
    provider, entry = selected
    priced = {
        **base,
        "provider": provider,
        "entry_combo_ask": float(entry["ask"]),
        "entry_combo_bid": float(entry["bid"]),
        "entry_source_times": entry.get("source_times"),
    }
    marks, close_ready = _marks(store, geometry, provider, end)
    if not marks:
        return _drop(priced, "exit_marks_unavailable")
    label = simulate_management_policy(
        marks,
        entry_ask=float(entry["ask"]),
        leg_count=2,
        entry_at=geometry.decision_at,
        policy=DEFAULT_MANAGEMENT_POLICY,
        session_date=geometry.session_date,
    )
    audit = {"mark_count": len(marks), "hard_close_bbo_ready": close_ready}
    if label.exit_reason not in VALID_EXITS:
        return _drop(
            priced,
            "exit_bbo_unavailable",
            **audit,
            provisional_exit_reason=label.exit_reason,
            provisional_exit_at=label.exit_at.isoformat() if label.exit_at else None,
        )
    gross = float(label.exit_bid) - float(entry["ask"])
    return {
        **priced,
        **audit,
        "label_status": "labeled",
        "drop_reason": None,
        "exit_reason": label.exit_reason,
        "exit_at": label.exit_at.isoformat() if label.exit_at else None,
        "exit_combo_bid": label.exit_bid,
        "pnl_gross_points": round(gross, 6),
        "pnl_net_fees_points": label.policy_pnl_points,
        "fees_points": label.fees_points,
        "hit": gross > 0,
        "stop_hit": label.exit_reason == "premium_stop",
        "tp_armed": label.tp_armed,
        "mfe_points": label.mfe_points,
        "mae_points": label.mae_points,
        "policy_version": label.policy_version,
    }


def label_geometries(store: QuoteStore, geometries: Sequence[Geometry]) -> list[dict[str, Any]]:
    result = []
    grouped: defaultdict[date, list[Geometry]] = defaultdict(list)
    for geometry in geometries:
        grouped[geometry.session_date].append(geometry)
    for session, rows in sorted(grouped.items()):
        end = policy_mark_horizon_end(
            rows[0].decision_at, DEFAULT_MANAGEMENT_POLICY, session_date=session
        )
        store.load_option_window(
            expiry=session,
            strike_min=min(min(row.long_strike, row.short_strike) for row in rows),
            strike_max=max(max(row.long_strike, row.short_strike) for row in rows),
            start=min(row.decision_at for row in rows)
            - timedelta(seconds=DEFAULT_STRATEGY_POLICY.quote_max_age_seconds),
            end=end,
        )
        result.extend(_label_one(store, row, end) for row in rows)
    return result


def _stats(rows: Sequence[Mapping[str, Any]], field: str) -> dict[str, Any]:
    values = sorted(value for row in rows if (value := _number(row.get(field))) is not None)
    if not values:
        return {key: None if key != "n" else 0 for key in ("n", "mean", "median", "hit_rate", "p10", "p90")}
    return {
        "n": len(values),
        "mean": float(np.mean(values)),
        "median": float(np.median(values)),
        "hit_rate": sum(value > 0 for value in values) / len(values),
        "p10": float(np.quantile(values, 0.10)),
        "p90": float(np.quantile(values, 0.90)),
    }


def _factor_summary(rows: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    labeled = [row for row in rows if row.get("label_status") == "labeled"]
    dates = sorted({str(row["session_date"]) for row in labeled})
    by_day = {}
    for session in sorted({str(row["session_date"]) for row in rows}):
        day_rows = [row for row in rows if row["session_date"] == session]
        day_labeled = [row for row in day_rows if row.get("label_status") == "labeled"]
        by_day[session] = {
            "signals_after_stick": len(day_rows),
            "labeled": len(day_labeled),
            "dropped": len(day_rows) - len(day_labeled),
            "drop_reasons": dict(Counter(row.get("drop_reason") for row in day_rows if row.get("drop_reason"))),
            "pnl_stats_gross_points": _stats(day_labeled, "pnl_gross_points"),
            "pnl_stats_net_fees_points": _stats(day_labeled, "pnl_net_fees_points"),
        }
    authorized = len(labeled) >= 15 and len(dates) >= 5
    return {
        "signals_after_stick": len(rows),
        "labeled_signals": len(labeled),
        "dropped_signals": len(rows) - len(labeled),
        "drop_reasons": dict(Counter(row.get("drop_reason") for row in rows if row.get("drop_reason"))),
        "labeled_session_count": len(dates),
        "labeled_session_dates": dates,
        "pnl_stats_gross_points": _stats(labeled, "pnl_gross_points"),
        "pnl_stats_net_fees_points": _stats(labeled, "pnl_net_fees_points"),
        "by_session_date": by_day,
        "authorization": {
            "min_labeled_signals": 15,
            "min_labeled_sessions": 5,
            "authorized": authorized,
            "conclusion": "样本达到最低历史门槛，但仍需独立前向验证" if authorized else "现有历史不够，不能授权人读卡",
        },
    }


def build_report(
    *,
    observations: Sequence[FactObservation],
    funnel: Mapping[str, int],
    quality: Mapping[str, Any],
    factor_rows: Sequence[Mapping[str, Any]],
    selected_rows: Sequence[Mapping[str, Any]],
    baseline: Mapping[str, Any],
    start_date: date,
    end_date: date,
    rows_path: Path,
) -> dict[str, Any]:
    factor = _factor_summary(factor_rows)
    selected_labeled = [row for row in selected_rows if row.get("label_status") == "labeled"]
    base = baseline["unconditional_pnl"]["by_session_mode"]["rth"]
    conditional_mean, base_mean = factor["pnl_stats_gross_points"]["mean"], _number(base["mean"])
    return {
        "schema_version": SCHEMA_VERSION,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "scope": {
            "start_session_date": start_date.isoformat(),
            "end_session_date": end_date.isoformat(),
            "strategy_name": "strategy_signal_engine_v2",
            "setup_kind": "ES_VOLUME_MOMENTUM",
            "session_mode": "rth",
            "excluded_incomplete_session_date": "2026-08-18",
            "rows_file": str(rows_path),
        },
        "gate_contract": {
            "es_volume_label": "elevated",
            "directions": ["up", "down"],
            "min_abs_return_1m_points": DEFAULT_STRATEGY_POLICY.es_momentum_min_return_1m,
            "min_abs_return_5m_points": DEFAULT_STRATEGY_POLICY.es_momentum_min_return_5m,
            "max_abs_return_5m_atr": DEFAULT_STRATEGY_POLICY.es_momentum_max_return_5m_atr,
            "rth_winner_stick_seconds": STICK_SECONDS,
        },
        "funnel": {
            **funnel,
            "signals_after_direction_stick": len(factor_rows),
            "successfully_labeled_signals": factor["labeled_signals"],
        },
        "data_quality": dict(quality),
        "factor_backtest": factor,
        "selected_cards": {
            "expected_count": 2,
            "observed_count": len(selected_rows),
            "labeled_count": len(selected_labeled),
            "rows": list(selected_rows),
            "pnl_stats_gross_points": _stats(selected_labeled, "pnl_gross_points"),
            "pnl_stats_net_fees_points": _stats(selected_labeled, "pnl_net_fees_points"),
        },
        "comparison": {
            "conditional_rth_es_volume_momentum": factor["pnl_stats_gross_points"],
            "unconditional_rth_atm_10wide_hold_to_1545": base,
            "mean_delta_points": conditional_mean - base_mean if conditional_mean is not None and base_mean is not None else None,
            "significantly_better": False,
            "inference": "not_testable_insufficient_signals_and_sessions",
            "comparability_caveat": "conditional uses management_policy.v2; baseline uses hold-to-15:45 without stop/trail",
        },
        "honesty": {
            "entry": "same-provider causal live NBBO conservative ask; no mid",
            "exit": "management_policy.v2: 50% stop, trail, 15:45 ET hard close, no 20m stop",
            "pnl_units": "SPX points; gross before fees and net after 0.0528 points per vertical",
            "missing_entry_or_exit_is_dropped": True,
            "hmm_walls_and_exact_candidate_gates_ignored": True,
            "july_market_facts_used": False,
            "live_paths_written": False,
            "production_checkout_changed": False,
            "services_or_deployment_changed": False,
        },
        "source_counts": {
            "rth_fact_observations": len(observations),
            "baseline_labeled_rows": baseline.get("coverage", {}).get("labeled_rows"),
        },
    }


def run_backtest(
    *,
    database_path: Path,
    data_root: Path,
    baseline_report_path: Path,
    output_dir: Path,
    start_date: date,
    end_date: date,
) -> dict[str, Any]:
    if end_date < start_date:
        raise ValueError("end_date must not precede start_date")
    output, data = output_dir.resolve(), data_root.resolve()
    if output == data or data in output.parents:
        raise ValueError("research output_dir must not be inside data_root")
    observations = read_fact_observations(database_path, start_date=start_date, end_date=end_date)
    funnel, openings, quality = build_funnel(observations)
    selected = read_selected_geometries(database_path, start_date=start_date, end_date=end_date)
    store = QuoteStore(data_root)
    try:
        rows = label_geometries(store, [*selected, *factor_geometries(apply_direction_stick(openings))])
    finally:
        store.close()
    selected_rows = [row for row in rows if row["cohort"] == "selected_card"]
    factor_rows = [row for row in rows if row["cohort"] == "factor_signal"]
    output_dir.mkdir(parents=True, exist_ok=True)
    rows_path = output_dir / "es_volume_momentum.rows.jsonl"
    rows_path.write_text("".join(json.dumps(row, sort_keys=True) + "\n" for row in rows), encoding="utf-8")
    report = build_report(
        observations=observations,
        funnel=funnel,
        quality=quality,
        factor_rows=factor_rows,
        selected_rows=selected_rows,
        baseline=json.loads(baseline_report_path.read_text(encoding="utf-8")),
        start_date=start_date,
        end_date=end_date,
        rows_path=rows_path,
    )
    (output_dir / "es_volume_momentum.report.json").write_text(
        json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )
    return report


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--database", type=Path, required=True)
    parser.add_argument("--data-root", type=Path, required=True)
    parser.add_argument("--baseline-report", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--start-date", type=date.fromisoformat, default=date(2026, 8, 7))
    parser.add_argument("--end-date", type=date.fromisoformat, default=date(2026, 8, 17))
    args = parser.parse_args(argv)
    report = run_backtest(
        database_path=args.database,
        data_root=args.data_root,
        baseline_report_path=args.baseline_report,
        output_dir=args.output_dir,
        start_date=args.start_date,
        end_date=args.end_date,
    )
    print(json.dumps({"funnel": report["funnel"], "factor": report["factor_backtest"]}))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
