#!/usr/bin/env python3
"""Build the canonical portable report for the RTH 5m state rollout."""

from __future__ import annotations

import argparse
import json
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


REPORT_TITLE = "RTH 5分钟状态策略：数据修复与因果重放"
REPLAY_SOURCE_ID = "causal_replay"
AUDIT_SOURCE_ID = "data_quality_audit"
IMPLEMENTATION_SOURCE_ID = "implementation"
CLASSIFIED_STATES = {
    "TREND_UP",
    "TREND_DOWN",
    "LOW_VOL_RANGE",
    "HIGH_VOL_CHOP",
    "LOW_VOL_PIN",
}


def _read_json(path: Path) -> dict[str, Any]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError("replay payload must be a JSON object")
    return payload


def _number(value: object) -> float | None:
    if isinstance(value, bool) or not isinstance(value, int | float):
        return None
    parsed = float(value)
    return parsed if parsed == parsed and abs(parsed) != float("inf") else None


def _build_datasets(replay: dict[str, Any]) -> dict[str, list[dict[str, Any]]]:
    daily = [
        dict(row)
        for row in replay.get("daily_coverage", [])
        if isinstance(row, dict)
    ]
    observations = [
        dict(row)
        for row in replay.get("observations", [])
        if isinstance(row, dict)
    ]
    total_slots = len(observations)
    ready_slots = sum(row.get("input_status") == "ready" for row in observations)
    classified_slots = sum(row.get("state") in CLASSIFIED_STATES for row in observations)
    trading_days = sum(
        (_number(row.get("es_rth_ok_bars")) or 0) > 0 for row in daily
    )
    baseline_samples = [
        _number(
            row.get("lineage", {})
            .get("same_time_range", {})
            .get("sample_count")
        )
        for row in observations
        if isinstance(row.get("lineage"), dict)
        and isinstance(row.get("lineage", {}).get("same_time_range"), dict)
    ]
    max_baseline_sessions = int(max((value or 0) for value in baseline_samples))
    headline = [
        {
            "raw_trading_days": trading_days,
            "replay_slots": total_slots,
            "input_ready_ratio": ready_slots / total_slots if total_slots else 0.0,
            "classified_ratio": (
                classified_slots / total_slots if total_slots else 0.0
            ),
            "max_prior_baseline_sessions": max_baseline_sessions,
        }
    ]

    state_counts = Counter(str(row.get("state") or "UNKNOWN") for row in observations)
    state_distribution = [
        {
            "state": state,
            "slot_count": count,
            "slot_share": count / total_slots if total_slots else 0.0,
        }
        for state, count in sorted(
            state_counts.items(),
            key=lambda item: (-item[1], item[0]),
        )
    ]
    missing_counts = Counter(
        str(field)
        for row in observations
        for field in (
            row.get("input_missing", [])
            if isinstance(row.get("input_missing"), list)
            else []
        )
    )
    input_missing = [
        {
            "input": field,
            "missing_slots": count,
            "missing_share": count / total_slots if total_slots else 0.0,
        }
        for field, count in sorted(
            missing_counts.items(),
            key=lambda item: (-item[1], item[0]),
        )
    ]
    coverage_by_day = [
        {
            "trading_date": row.get("trading_date"),
            "weekday": row.get("weekday"),
            "es_ok_bars": row.get("es_rth_ok_bars"),
            "es_expected_bars": row.get("es_rth_expected_bars"),
            "es_bar_coverage_ratio": row.get("es_rth_ok_bar_coverage_ratio"),
            "sector_count": row.get("sector_instruments_present_count"),
            "replay_slots": row.get("replay_slot_count"),
            "complete_input_slots": row.get("complete_input_slot_count"),
            "complete_input_ratio": row.get("complete_input_slot_ratio"),
        }
        for row in daily
    ]
    coverage_long = [
        {
            "trading_date": row["trading_date"],
            "metric": metric,
            "ratio": _number(row.get(field)),
        }
        for row in coverage_by_day
        for metric, field in (
            ("ES 5m完整bar", "es_bar_coverage_ratio"),
            ("八输入完整", "complete_input_ratio"),
        )
        if _number(row.get(field)) is not None
    ]
    forward_summary = [
        {
            "state": row.get("state"),
            "horizon_minutes": row.get("horizon_minutes"),
            "sample_count": row.get("sample_count"),
            "median_endpoint_points": row.get("median_endpoint_points"),
            "mean_endpoint_points": row.get("mean_endpoint_points"),
            "directional_hit_ratio": row.get("directional_hit_ratio"),
            "evaluation_only": row.get("evaluation_only"),
        }
        for row in replay.get("forward_es_path_summary", [])
        if isinstance(row, dict)
    ]
    lifecycle = [
        {
            "date": "2026-07-20",
            "health_samples": 6221,
            "structure_pending": 4140,
            "invalidated_samples": 6191,
            "formal_signals": 0,
            "interpretation": "旧 lifecycle bug 污染",
        },
        {
            "date": "2026-07-21",
            "health_samples": 6215,
            "structure_pending": 2296,
            "invalidated_samples": 5955,
            "formal_signals": 0,
            "interpretation": "旧 lifecycle bug 污染",
        },
        {
            "date": "2026-07-22",
            "health_samples": 6209,
            "structure_pending": 1399,
            "invalidated_samples": 5884,
            "formal_signals": 0,
            "interpretation": "旧 lifecycle bug 污染",
        },
        {
            "date": "2026-07-23",
            "health_samples": 6210,
            "structure_pending": 0,
            "invalidated_samples": 9,
            "formal_signals": 7,
            "interpretation": "b7e1901 后恢复；7/7 被下游 gate 阻止",
        },
    ]
    changes = [
        {
            "layer": "ES 5m bars",
            "change": "5秒 source-time OHLC；首尾覆盖、连续网格和gap门控",
            "effect": "不再依赖长期为空的 SPX bar 文件",
        },
        {
            "layer": "八变量",
            "change": "RTH VWAP/OR/structure/ER/cross/range/breadth 因果派生",
            "effect": "缺失保持 UNCERTAIN，不补成中性",
        },
        {
            "layer": "Provider切换",
            "change": "5m volume 选择完整窗口 provider，重复 stale quote 去重",
            "effect": "Schwab→IBKR 时不再丢失已有5m量价窗",
        },
        {
            "layer": "Option coverage",
            "change": "bid>=0、ask>=bid、mid有限；拒绝-1占位和crossed market",
            "effect": "61档完整价对展示不再虚高",
        },
        {
            "layer": "Spring Gamma v3",
            "change": "ready 8/8 状态参与Shadow确认；option overlay独立",
            "effect": "期权缺失不抹掉市场状态；warming不误杀旧诊断",
        },
        {
            "layer": "15m报告",
            "change": "状态→观察位置→外部触发→只读方向映射",
            "effect": "不根据未计算的skew/edge虚构spread建议",
        },
    ]
    return {
        "headline": headline,
        "state_distribution": state_distribution,
        "input_missing": input_missing,
        "coverage_by_day": coverage_by_day,
        "coverage_long": coverage_long,
        "forward_summary": forward_summary,
        "lifecycle": lifecycle,
        "changes": changes,
    }


def _artifact(replay: dict[str, Any]) -> dict[str, Any]:
    datasets = _build_datasets(replay)
    headline = datasets["headline"][0]
    generated_at = str(
        replay.get("generated_at")
        or datetime.now(tz=timezone.utc).isoformat()
    )
    start = replay.get("window", {}).get("start_date")
    end = replay.get("window", {}).get("end_date")
    ready = float(headline["input_ready_ratio"])
    classified = float(headline["classified_ratio"])
    baseline_sessions = int(headline["max_prior_baseline_sessions"])
    executive_audit = (
        "## 结论\n\n"
        "**数据问题来自两个独立层面：旧状态机和缺失的特征生产合同，"
        "不能统一归因成“行情没采到”。** "
        "7/20–7/22 的 ES/SPY 原始 RTH 覆盖完整，但旧 lifecycle 把活动 path 大量置为 "
        "`INVALIDATED`；修复后的 7/23 记录到 7 个 formal signals。"
    )
    executive_replay = (
        "**因果重放结果。** 新因果重放在 "
        f"{start}–{end} 共评估 {int(headline['replay_slots'])} 个 5 分钟边界，"
        f"八输入完整率 {ready:.2%}，明确分类率 {classified:.2%}。"
    )
    executive_decision = (
        "**没有据此修改生产阈值。** 这些边界高度重叠、历史 session 少于 20 个，"
        "forward ES 只用于检查状态语义，不是净成本后策略回测。当前版本先修"
        "数据合同并保持 Shadow。"
    )
    sources = [
        {
            "id": REPLAY_SOURCE_ID,
            "label": f"RTH 5m causal replay, {start}–{end}",
            "path": "reports/market_state_5m/replay=2026-07-24/artifact.json",
            "query": {
                "engine": "DuckDB + production-equivalent Python",
                "id": "rth-market-state-5m-replay-20260724",
                "sql": (
                    "SELECT provider, instrument_id, source_at, received_at, "
                    "effective_price, volume FROM read_parquet("
                    "'lake/quotes/schema=v1/date=*/provider=*/hour=*/quotes.parquet', "
                    "hive_partitioning=true, union_by_name=true) "
                    "WHERE quality='live' AND instrument_id IN "
                    "('future:ES','equity:XLB','equity:XLC','equity:XLE',"
                    "'equity:XLF','equity:XLI','equity:XLK','equity:XLP',"
                    "'equity:XLRE','equity:XLU','equity:XLV','equity:XLY')"
                ),
                "description": (
                    "Loads live ES and eleven sector ETFs, replays only rows known by "
                    "each five-minute boundary, and attaches future ES paths afterwards."
                ),
                "executed_at": generated_at,
                "language": "sql",
                "filters": [
                    f"{start} through {end}",
                    "RTH state evaluated in America/New_York",
                    "received_at <= replay_as_of and source_at <= replay_as_of",
                    "no bar, NBBO, breadth, or missing-value interpolation",
                    "same-time baselines use only baseline_date < trading_date",
                ],
                "metric_definitions": [
                    "Input-ready ratio is 5m boundaries with all eight inputs available.",
                    "Classified ratio excludes UNCERTAIN.",
                    "Forward paths are overlapping evaluation labels, not independent trades.",
                ],
                "tables_used": ["lake/quotes/schema=v1"],
            },
        },
        {
            "id": AUDIT_SOURCE_ID,
            "label": "RTH lifecycle and data-quality audit, 2026-07-24",
            "path": "docs/rth-state-data-quality-audit-2026-07-24.md",
            "description": (
                "Point-in-time audit of quote coverage, lifecycle transitions, report "
                "cadence, option-chain gaps and confirmed-gate attribution."
            ),
            "query": {
                "engine": "DuckDB",
                "id": "rth-lifecycle-audit-extract-20260724",
                "sql": (
                    "SELECT * FROM (VALUES "
                    "('2026-07-20', 6221, 4140, 6191, 0, "
                    "'旧 lifecycle bug 污染'), "
                    "('2026-07-21', 6215, 2296, 5955, 0, "
                    "'旧 lifecycle bug 污染'), "
                    "('2026-07-22', 6209, 1399, 5884, 0, "
                    "'旧 lifecycle bug 污染'), "
                    "('2026-07-23', 6210, 0, 9, 7, "
                    "'b7e1901 后恢复；7/7 被下游 gate 阻止')"
                    ") AS audited(date, health_samples, structure_pending, "
                    "invalidated_samples, formal_signals, interpretation)"
                ),
                "description": (
                    "Reproduces the four reviewed lifecycle totals transcribed from "
                    "the saved point-in-time audit."
                ),
                "executed_at": generated_at,
                "language": "sql",
                "filters": [
                    "09:30 through 16:00 America/New_York",
                    "2026-07-20 through 2026-07-23",
                    "formal_signals counts unique transitions; health rows are samples",
                ],
                "metric_definitions": [
                    "health_samples counts repeated level-health observations.",
                    "formal_signals counts unique formal transitions.",
                ],
            },
        },
        {
            "id": IMPLEMENTATION_SOURCE_ID,
            "label": "RTH market-state v1 implementation and tests",
            "path": "docs/rth-five-minute-market-state-v1.md",
            "description": (
                "Pure scorer, causal input extractor, ES bar state, Spring Gamma "
                "Shadow integration and strict NBBO coverage changes."
            ),
        },
    ]
    cards = [
        {
            "id": "days_card",
            "description": "Sessions with at least one complete RTH ES 5m bar.",
            "dataset": "headline",
            "sourceId": REPLAY_SOURCE_ID,
            "metrics": [
                {
                    "label": "原始RTH交易日",
                    "field": "raw_trading_days",
                    "format": "number",
                }
            ],
        },
        {
            "id": "ready_card",
            "description": "Share of replay boundaries with all eight causal inputs.",
            "dataset": "headline",
            "sourceId": REPLAY_SOURCE_ID,
            "metrics": [
                {
                    "label": "八输入完整率",
                    "field": "input_ready_ratio",
                    "format": "percent",
                }
            ],
        },
        {
            "id": "classified_card",
            "description": "Share of boundaries assigned a state other than UNCERTAIN.",
            "dataset": "headline",
            "sourceId": REPLAY_SOURCE_ID,
            "metrics": [
                {
                    "label": "明确状态率",
                    "field": "classified_ratio",
                    "format": "percent",
                }
            ],
        },
        {
            "id": "baseline_card",
            "description": "Largest strictly-prior same-time range sample available.",
            "dataset": "headline",
            "sourceId": REPLAY_SOURCE_ID,
            "metrics": [
                {
                    "label": "最大历史基线",
                    "field": "max_prior_baseline_sessions",
                    "format": "number",
                    "unit": " sessions",
                }
            ],
        },
    ]
    charts = [
        {
            "id": "coverage_chart",
            "title": "逐日因果特征完整率",
            "subtitle": "ES bar完整不等于八输入完整；sector breadth和历史基线仍会独立fail closed",
            "intent": "trend",
            "question": "On which sessions were ES bars and all eight state inputs available?",
            "rationale": "Two causal coverage series expose where the feature funnel drops.",
            "type": "line",
            "dataset": "coverage_long",
            "sourceId": REPLAY_SOURCE_ID,
            "encodings": {
                "x": {
                    "field": "trading_date",
                    "type": "temporal",
                    "label": "交易日",
                },
                "y": {
                    "field": "ratio",
                    "type": "quantitative",
                    "format": "percent",
                    "label": "完整率",
                },
                "color": {
                    "field": "metric",
                    "type": "nominal",
                    "label": "证据层",
                },
                "tooltip": [
                    {
                        "field": "ratio",
                        "type": "quantitative",
                        "format": "percent",
                        "label": "完整率",
                    }
                ],
            },
            "valueFormat": "percent",
            "layout": "full",
            "labels": {"values": "none"},
            "maxRows": 50,
            "settings": {"showValues": False, "sort": "none"},
            "surface": {
                "surface": "export",
                "interactiveLegend": False,
                "showControls": False,
                "viewMode": "visualization",
            },
        },
        {
            "id": "state_chart",
            "title": "五分钟状态分布",
            "subtitle": "UNCERTAIN被保留；状态边界是重叠观察，不是独立交易样本",
            "intent": "comparison",
            "question": "How often did each rule state appear in the causal replay?",
            "rationale": "Counts show rule selectivity without presenting PnL as validated alpha.",
            "type": "bar",
            "dataset": "state_distribution",
            "sourceId": REPLAY_SOURCE_ID,
            "encodings": {
                "x": {"field": "state", "type": "nominal", "label": "状态"},
                "y": {
                    "field": "slot_count",
                    "type": "quantitative",
                    "format": "number",
                    "label": "5m边界数",
                },
                "tooltip": [
                    {
                        "field": "slot_share",
                        "type": "quantitative",
                        "format": "percent",
                        "label": "占比",
                    }
                ],
            },
            "valueFormat": "number",
            "layout": "full",
            "labels": {"values": "all"},
            "maxRows": 8,
            "settings": {"showValues": True, "sort": "descending"},
            "surface": {
                "surface": "export",
                "interactiveLegend": False,
                "showControls": False,
                "viewMode": "visualization",
            },
        },
    ]
    tables = [
        {
            "id": "coverage_table",
            "title": "逐日数据漏斗",
            "subtitle": "完整率以严格连续、首尾覆盖和因果时间为准。",
            "dataset": "coverage_by_day",
            "sourceId": REPLAY_SOURCE_ID,
            "defaultSort": {"field": "trading_date", "direction": "asc"},
            "density": "compact",
            "layout": "full",
            "columns": [
                {"field": "trading_date", "label": "日期", "type": "text"},
                {"field": "weekday", "label": "星期", "type": "text"},
                {"field": "es_ok_bars", "label": "ES完整bar", "format": "number"},
                {
                    "field": "es_bar_coverage_ratio",
                    "label": "ES bar完整率",
                    "format": "percent",
                },
                {"field": "sector_count", "label": "Sector数", "format": "number"},
                {
                    "field": "complete_input_slots",
                    "label": "8/8边界",
                    "format": "number",
                },
                {
                    "field": "complete_input_ratio",
                    "label": "8/8完整率",
                    "format": "percent",
                },
            ],
        },
        {
            "id": "missing_table",
            "title": "八输入缺失归因",
            "subtitle": "缺失保持缺失，不按0分或中性填充。",
            "dataset": "input_missing",
            "sourceId": REPLAY_SOURCE_ID,
            "defaultSort": {"field": "missing_slots", "direction": "desc"},
            "density": "compact",
            "layout": "full",
            "columns": [
                {"field": "input", "label": "输入", "type": "text"},
                {
                    "field": "missing_slots",
                    "label": "缺失边界",
                    "format": "number",
                },
                {
                    "field": "missing_share",
                    "label": "缺失率",
                    "format": "percent",
                },
            ],
        },
        {
            "id": "lifecycle_table",
            "title": "为什么周一至周三没有 formal signal",
            "subtitle": "原始行情可用，但旧 lifecycle 派生标签被 structure pending 污染。",
            "dataset": "lifecycle",
            "sourceId": AUDIT_SOURCE_ID,
            "defaultSort": {"field": "date", "direction": "asc"},
            "density": "compact",
            "layout": "full",
            "columns": [
                {"field": "date", "label": "日期", "type": "text"},
                {
                    "field": "health_samples",
                    "label": "health样本",
                    "format": "number",
                },
                {
                    "field": "structure_pending",
                    "label": "pending",
                    "format": "number",
                },
                {
                    "field": "invalidated_samples",
                    "label": "invalidated",
                    "format": "number",
                },
                {
                    "field": "formal_signals",
                    "label": "formal",
                    "format": "number",
                },
                {"field": "interpretation", "label": "解释", "type": "text"},
            ],
        },
        {
            "id": "forward_table",
            "title": "状态后的 ES 路径（仅语义检查）",
            "subtitle": "每5分钟滚动、样本高度重叠、未计期权成本；不能当作策略胜率。",
            "dataset": "forward_summary",
            "sourceId": REPLAY_SOURCE_ID,
            "defaultSort": {"field": "sample_count", "direction": "desc"},
            "density": "compact",
            "layout": "full",
            "columns": [
                {"field": "state", "label": "状态", "type": "text"},
                {
                    "field": "horizon_minutes",
                    "label": "Horizon(min)",
                    "format": "number",
                },
                {"field": "sample_count", "label": "n", "format": "number"},
                {
                    "field": "median_endpoint_points",
                    "label": "中位ES终值",
                    "format": "number",
                },
                {
                    "field": "mean_endpoint_points",
                    "label": "平均ES终值",
                    "format": "number",
                },
                {
                    "field": "directional_hit_ratio",
                    "label": "方向命中",
                    "format": "percent",
                },
            ],
        },
    ]
    blocks = [
        {
            "id": "title",
            "type": "markdown",
            "layout": "full",
            "body": f"# {REPORT_TITLE}",
        },
        {
            "id": "executive",
            "type": "markdown",
            "layout": "full",
            "sourceId": AUDIT_SOURCE_ID,
            "body": executive_audit,
        },
        {
            "id": "executive_replay",
            "type": "markdown",
            "layout": "full",
            "sourceId": REPLAY_SOURCE_ID,
            "body": executive_replay,
        },
        {
            "id": "executive_decision",
            "type": "markdown",
            "layout": "full",
            "sourceId": REPLAY_SOURCE_ID,
            "body": executive_decision,
        },
        {
            "id": "headline",
            "type": "metric-strip",
            "layout": "full",
            "cardIds": [card["id"] for card in cards],
        },
        {
            "id": "root_cause",
            "type": "markdown",
            "layout": "full",
            "sourceId": AUDIT_SOURCE_ID,
            "body": (
                "## 根因不是单一采集故障\n\n"
                "Schwab 在 7/20–7/23 的 ES/SPY/RSP RTH 分钟覆盖均为 390/390。"
                "真正让周一至周三 formal 归零的是旧 lifecycle：候选结构待确认时，"
                "活动 level path 被立即置为 `INVALIDATED`。`b7e1901` 修复后，7/23 "
                "恢复 7 个 formal signals；它们随后全部被剩余空间、方向一致性、"
                "reward/risk 或 option freshness gate 阻止。"
            ),
        },
        {
            "id": "lifecycle_block",
            "type": "table",
            "layout": "full",
            "tableId": "lifecycle_table",
        },
        {
            "id": "coverage_intro",
            "type": "markdown",
            "layout": "full",
            "sourceId": REPLAY_SOURCE_ID,
            "body": (
                "## 新状态的数据质量\n\n"
                "重放使用 `received_at` 作为知识时钟，并再次要求 `source_at` 不晚于"
                "评估时点；VWAP边界、历史baseline、连续bar和sector横截面时间偏差"
                "均严格fail closed。下面把“有ES bar”和“八项全部可用”分开显示。"
            ),
        },
        {
            "id": "coverage_chart_block",
            "type": "chart",
            "layout": "full",
            "chartId": "coverage_chart",
        },
        {
            "id": "coverage_table_block",
            "type": "table",
            "layout": "full",
            "tableId": "coverage_table",
        },
        {
            "id": "missing_table_block",
            "type": "table",
            "layout": "full",
            "tableId": "missing_table",
        },
        {
            "id": "states_intro",
            "type": "markdown",
            "layout": "full",
            "sourceId": REPLAY_SOURCE_ID,
            "body": (
                "## 状态结果不是下单信号\n\n"
                "D只描述方向，Q描述路径效率，V描述同刻波动强度。`UNCERTAIN`被保留；"
                "LOW_VOL_PIN在缺少整数strike和ATM跨式衰减确认时不会发出。"
            ),
        },
        {
            "id": "state_chart_block",
            "type": "chart",
            "layout": "full",
            "chartId": "state_chart",
        },
        {
            "id": "forward_table_block",
            "type": "table",
            "layout": "full",
            "tableId": "forward_table",
        },
        {
            "id": "implementation_intro",
            "type": "markdown",
            "layout": "full",
            "sourceId": IMPLEMENTATION_SOURCE_ID,
            "body": (
                "## 实现决策\n\n"
                "八变量状态与 option overlay 已拆开；只有 ready 8/8 状态参与 "
                "Spring Gamma v3 Shadow 确认，warming状态仅诊断。报告不根据"
                "未计算的skew、edge或双腿价格推荐spread。\n\n"
                "- ES 5m bars：按 source time 聚合，并门控首尾覆盖、连续网格和 gap。\n"
                "- 八变量：RTH VWAP、OR、结构、ER、穿越、同刻 range 和 breadth "
                "全部因果派生；缺失保持 `UNCERTAIN`。\n"
                "- Provider 切换：选取具有完整 5 分钟窗口的来源，并去重 stale quote。\n"
                "- Option coverage：拒绝负 bid、crossed market 和无效 mid。\n"
                "- Spring Gamma：ready 8/8 状态与 option overlay 分离。"
            ),
        },
        {
            "id": "parameter_decision",
            "type": "markdown",
            "layout": "full",
            "sourceId": REPLAY_SOURCE_ID,
            "body": (
                "## 参数决定\n\n"
                "**本轮保持用户给出的 D/Q/V 阈值，不调生产 gate。** 原因是历史边界"
                "重叠、同刻基线未满20个session，而且没有实际期权双腿成交、费用"
                "和滑点。下一步应累计至少20个合约一致的完整RTH "
                "sessions，再用预登记的walk-forward切分评估；训练日期和汇报日期"
                "不能是同一批。"
            ),
        },
        {
            "id": "parameter_audit_limit",
            "type": "markdown",
            "layout": "full",
            "sourceId": AUDIT_SOURCE_ID,
            "body": (
                "**历史执行证据还不可用于定参。** 旧派生 signal 受 lifecycle bug "
                "污染，且 7/20–7/23 最后30分钟的同日0DTE NBBO不可恢复；因此不能"
                "从这些日期推导可执行 spread 的净成本表现。"
            ),
        },
        {
            "id": "limitations",
            "type": "markdown",
            "layout": "full",
            "sourceId": REPLAY_SOURCE_ID,
            "body": (
                "## 限制与今天的验收\n\n"
                f"- 同刻历史基线当前最多 {baseline_sessions} 个session，低于目标20。"
            ),
        },
        {
            "id": "limitations_audit",
            "type": "markdown",
            "layout": "full",
            "sourceId": AUDIT_SOURCE_ID,
            "body": (
                "- 7/20–7/23 的15:30–16:00同日0DTE历史NBBO不可恢复，保持missing。"
            ),
        },
        {
            "id": "live_acceptance",
            "type": "markdown",
            "layout": "full",
            "body": (
                "- 今日RTH需现场验证09:30后的bar连续性、约10:00的8/8状态、"
                "15:30后的同日0DTE保留和15分钟报告渲染。\n"
                "- 自动下单仍关闭；本报告不提供个性化交易建议。"
            ),
        },
    ]
    return {
        "surface": "report",
        "manifest": {
            "surface": "report",
            "version": 1,
            "title": REPORT_TITLE,
            "description": (
                "Causal data-quality replay and implementation readout for the "
                "RTH eight-variable market-state rollout."
            ),
            "generatedAt": generated_at,
            "blocks": blocks,
            "cards": cards,
            "charts": charts,
            "tables": tables,
            "sources": sources,
        },
        "snapshot": {
            "version": 1,
            "generatedAt": generated_at,
            "status": "ready",
            "datasets": datasets,
        },
        "sources": sources,
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--replay", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    artifact = _artifact(_read_json(args.replay))
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(
        json.dumps(artifact, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    print(f"wrote {args.output}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
