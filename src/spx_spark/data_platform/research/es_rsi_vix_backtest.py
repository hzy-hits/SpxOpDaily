"""Evaluate ES RSI trend signals and the incremental value of VIX confirmation."""

from __future__ import annotations

import csv
import json
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable

import numpy as np

from .es_rsi_vix_data import SessionCoverage, SessionSeries, load_outbox_sessions
from .es_rsi_vix_indicators import indicator_directions, signal_events


HORIZONS = (5, 15, 30, 60)
FILTERS = ("none", "vix", "vix_vix1d")
ROUND_TRIP_FRICTION_POINTS = 0.5


@dataclass(frozen=True)
class SignalOutcome:
    session_date: str
    variant: str
    volatility_filter: str
    signal_at: str
    direction: int
    horizon_minutes: int
    entry: float
    exit: float
    gross_points: float
    net_points: float
    mfe_points: float
    mae_points: float


@dataclass(frozen=True)
class Metric:
    variant: str
    volatility_filter: str
    horizon_minutes: int
    signals: int
    hit_rate: float | None
    mean_net_points: float | None
    median_net_points: float | None
    total_net_points: float | None
    mean_mfe_points: float | None
    mean_mae_points: float | None
    positive_sessions: int
    evaluated_sessions: int


def _change(values: np.ndarray, index: int, minutes: int) -> float | None:
    if index < minutes or not np.isfinite(values[[index - minutes, index]]).all():
        return None
    return float(values[index] - values[index - minutes])


def _volatility_confirms(
    session: SessionSeries,
    index: int,
    direction: int,
    mode: str,
) -> bool:
    if mode == "none":
        return True
    vix_change = _change(session.vix, index, 5)
    if vix_change is None or direction * -vix_change <= 0.01:
        return False
    if mode == "vix":
        return True
    vix1d_change = _change(session.vix1d, index, 5)
    return vix1d_change is not None and direction * -vix1d_change > 0.01


def _outcome(
    session: SessionSeries,
    *,
    index: int,
    direction: int,
    variant: str,
    volatility_filter: str,
    horizon: int,
) -> SignalOutcome | None:
    entry_index = index + 1
    exit_index = entry_index + horizon
    if exit_index >= len(session.es):
        return None
    path = session.es[entry_index : exit_index + 1]
    if not np.isfinite(path).all():
        return None
    entry = float(path[0])
    directed = direction * (path - entry)
    gross = float(directed[-1])
    return SignalOutcome(
        session_date=session.session_date,
        variant=variant,
        volatility_filter=volatility_filter,
        signal_at=session.times[index].isoformat(),
        direction=direction,
        horizon_minutes=horizon,
        entry=entry,
        exit=float(path[-1]),
        gross_points=gross,
        net_points=gross - ROUND_TRIP_FRICTION_POINTS,
        mfe_points=float(directed.max()),
        mae_points=float(directed.min()),
    )


def run_backtest(sessions: Iterable[SessionSeries]) -> list[SignalOutcome]:
    outcomes: list[SignalOutcome] = []
    for session in sessions:
        for variant, direction in indicator_directions(session.es).items():
            events = signal_events(direction)
            for volatility_filter in FILTERS:
                for index, side in events:
                    if not _volatility_confirms(session, index, side, volatility_filter):
                        continue
                    for horizon in HORIZONS:
                        result = _outcome(
                            session,
                            index=index,
                            direction=side,
                            variant=variant,
                            volatility_filter=volatility_filter,
                            horizon=horizon,
                        )
                        if result is not None:
                            outcomes.append(result)
    return outcomes


def summarize(outcomes: Iterable[SignalOutcome]) -> list[Metric]:
    grouped: dict[tuple[str, str, int], list[SignalOutcome]] = {}
    for row in outcomes:
        grouped.setdefault(
            (row.variant, row.volatility_filter, row.horizon_minutes), []
        ).append(row)
    metrics: list[Metric] = []
    for (variant, volatility_filter, horizon), rows in sorted(grouped.items()):
        net = np.array([row.net_points for row in rows])
        by_session: dict[str, float] = {}
        for row in rows:
            by_session[row.session_date] = by_session.get(row.session_date, 0.0) + row.net_points
        metrics.append(
            Metric(
                variant=variant,
                volatility_filter=volatility_filter,
                horizon_minutes=horizon,
                signals=len(rows),
                hit_rate=float((net > 0).mean()),
                mean_net_points=float(net.mean()),
                median_net_points=float(np.median(net)),
                total_net_points=float(net.sum()),
                mean_mfe_points=float(np.mean([row.mfe_points for row in rows])),
                mean_mae_points=float(np.mean([row.mae_points for row in rows])),
                positive_sessions=sum(value > 0 for value in by_session.values()),
                evaluated_sessions=len(by_session),
            )
        )
    return metrics


def _fmt(value: float | None, *, percent: bool = False) -> str:
    if value is None:
        return "-"
    return f"{value:.1%}" if percent else f"{value:+.2f}"


def _render_report(
    coverage: list[SessionCoverage],
    metrics: list[Metric],
    outcomes: list[SignalOutcome],
) -> str:
    complete = [row for row in coverage if row.observed_es_minutes >= 350]
    lines = [
        "# ES RSI + VIX 日内趋势试验回测",
        "",
        "## TL;DR",
        "",
        (
            f"样本仅 {len(coverage)} 个交易日、{len(complete)} 个接近完整 RTH session，"
            "不足以选参数或接入正式告警。下表只能判断候选是否值得继续 shadow。"
        ),
        "",
        "## 数据质量",
        "",
        "| 日期 | ES观测/391 | VIX观测 | VIX1D观测 | ES可用 | 最长缺口 |",
        "|---|---:|---:|---:|---:|---:|",
    ]
    for row in coverage:
        lines.append(
            f"| {row.session_date} | {row.observed_es_minutes} | {row.observed_vix_minutes} | "
            f"{row.observed_vix1d_minutes} | {row.usable_es_minutes} | "
            f"{row.longest_es_gap_minutes}m |"
        )

    selected = sorted(
        (row for row in metrics if row.horizon_minutes == 15),
        key=lambda row: (row.mean_net_points if row.mean_net_points is not None else -999),
        reverse=True,
    )
    lines.extend(
        [
            "",
            "## 15分钟结果",
            "",
            "每个信号在下一分钟入场，扣除 0.5 ES 点往返摩擦；信号需连续两分钟成立，"
            "并带 10 分钟冷却。",
            "",
            "| 指标 | VIX过滤 | 信号数 | 胜率 | 平均净点 | 中位净点 | 正收益session |",
            "|---|---|---:|---:|---:|---:|---:|",
        ]
    )
    for row in selected:
        lines.append(
            f"| {row.variant} | {row.volatility_filter} | {row.signals} | "
            f"{_fmt(row.hit_rate, percent=True)} | {_fmt(row.mean_net_points)} | "
            f"{_fmt(row.median_net_points)} | {row.positive_sessions}/{row.evaluated_sessions} |"
        )

    baseline = {
        (row.variant, row.volatility_filter): row
        for row in metrics
        if row.horizon_minutes == 15
    }
    lines.extend(
        [
            "",
            "## VIX增量",
            "",
            "| 指标 | 无过滤平均 | VIX平均 | VIX+VIX1D平均 | VIX+VIX1D信号数 |",
            "|---|---:|---:|---:|---:|",
        ]
    )
    for variant in sorted({row.variant for row in metrics}):
        raw = baseline.get((variant, "none"))
        vix = baseline.get((variant, "vix"))
        both = baseline.get((variant, "vix_vix1d"))
        lines.append(
            f"| {variant} | {_fmt(raw.mean_net_points if raw else None)} | "
            f"{_fmt(vix.mean_net_points if vix else None)} | "
            f"{_fmt(both.mean_net_points if both else None)} | {both.signals if both else 0} |"
        )

    lines.extend(
        [
            "",
            "## 结论与门槛",
            "",
            "- 本次不做最优参数选择；候选阈值均为预先固定，避免在三天样本上调参。",
            "- VIX只在RTH有可靠实时更新，不能用于GTH趋势确认；GTH需单独验证ES价格、成交量和跨市场代理。",
            "- 任何单日或少量信号的高胜率都不具备上线意义。",
            "- 继续收集至少20个完整RTH session；正式晋升建议要求至少100个独立信号、"
            "多数session净收益为正，且按日walk-forward相对5分钟动量基线有稳定增益。",
            "- 本回测衡量的是ES方向点数，不等同于SPXW期权收益；后续必须叠加决策时NBBO、IV、"
            "theta和真实可成交限价。",
            "",
            f"原始可评估信号结果共 {len(outcomes)} 行（含多个前瞻周期）。",
        ]
    )
    return "\n".join(lines) + "\n"


def write_artifacts(
    output_dir: str | Path,
    *,
    source_outbox: str | Path,
    coverage: list[SessionCoverage],
    metrics: list[Metric],
    outcomes: list[SignalOutcome],
) -> Path:
    target = Path(output_dir).expanduser().resolve()
    target.mkdir(parents=True, exist_ok=True)
    artifact = {
        "schema_version": 1,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "source_outbox": str(Path(source_outbox).expanduser().resolve()),
        "promotion_decision": "do_not_enable_insufficient_and_negative_evidence",
        "friction_points": ROUND_TRIP_FRICTION_POINTS,
        "method": {
            "entry_delay_minutes": 1,
            "horizons_minutes": list(HORIZONS),
            "signal_persistence_minutes": 2,
            "signal_cooldown_minutes": 10,
            "maximum_forward_fill_minutes": 2,
            "volatility_confirmation_lookback_minutes": 5,
        },
        "coverage": [asdict(row) for row in coverage],
        "metrics": [asdict(row) for row in metrics],
        "limitations": [
            "fewer_than_20_complete_rth_sessions",
            "outbox_snapshots_are_not_exchange_minute_bars",
            "es_points_do_not_model_spxw_iv_or_theta",
            "vix_is_rth_only",
        ],
    }
    (target / "artifact.json").write_text(
        json.dumps(artifact, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    (target / "report.md").write_text(
        _render_report(coverage, metrics, outcomes),
        encoding="utf-8",
    )
    with (target / "signals.csv").open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(asdict(outcomes[0])) if outcomes else [])
        if outcomes:
            writer.writeheader()
            writer.writerows(asdict(row) for row in outcomes)
    return target


def run(outbox: str | Path, output_dir: str | Path) -> Path:
    sessions, coverage = load_outbox_sessions(outbox)
    outcomes = run_backtest(sessions)
    metrics = summarize(outcomes)
    return write_artifacts(
        output_dir,
        source_outbox=outbox,
        coverage=coverage,
        metrics=metrics,
        outcomes=outcomes,
    )
