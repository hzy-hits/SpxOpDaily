"""Chinese markdown report rendering for the 0DTE level backtest."""

from __future__ import annotations

import statistics
from datetime import datetime
from typing import Sequence

from .odte_level_signals import (
    PROFILE_BASELINE,
    PROFILE_CLOCK,
    PROFILE_GTH_360,
    PROFILE_SAT85,
    PROFILE_TRAIL33,
    PROFILE_TRAILING_TP,
    PROFILE_WIDE_INVALIDATION,
    SET_ORDER,
    SET_PREFILL,
    SET_TRADE_READY,
    SPREAD_VARIANTS,
    VARIANT_NAKED,
    VARIANT_SPREAD5,
    VARIANT_SPREAD10,
    VARIANT_SPREAD_WALL,
    VARIANTS,
    Trade,
)


def _fmt_money(value: float | None) -> str:
    return "-" if value is None else f"{value:+.2f}"


def _fmt_pct(value: float | None) -> str:
    return "-" if value is None else f"{value:.0%}"


def _fmt_pf(value: float | None) -> str:
    return "-" if value is None else f"{value:.2f}"


def _profit_factor(rows: Sequence[Trade]) -> float | None:
    wins = sum(row.pnl_usd for row in rows if row.pnl_usd > 0)
    losses = abs(sum(row.pnl_usd for row in rows if row.pnl_usd < 0))
    return (wins / losses) if losses > 0 else None


def _median_hold_minutes(rows: Sequence[Trade]) -> float | None:
    if not rows:
        return None
    holds = [
        (
            datetime.fromisoformat(row.exit_time) - datetime.fromisoformat(row.entry_time)
        ).total_seconds()
        / 60
        for row in rows
    ]
    return statistics.median(holds)


def _readiness_section(artifact: dict) -> list[str]:
    readiness = artifact.get("strategy_readiness") or {}
    thresholds = readiness.get("thresholds") or {}
    sessions = readiness.get("sessions") or {}
    cohorts = readiness.get("cohorts") or {}
    window = artifact.get("window") or {}

    def cohort_row(key: str) -> dict:
        value = cohorts.get(key) or {}
        return value if isinstance(value, dict) else {}

    session_count = int(sessions.get("contract_consistent_complete") or 0)
    session_target = int(thresholds.get("complete_sessions") or 20)
    rows = (
        (
            "contract-consistent complete sessions",
            session_count,
            session_target,
            "ready" if session_count >= session_target else "collecting",
        ),
        (
            "GTH exact entries",
            int(cohort_row("gth_exact_entry").get("count") or 0),
            int(thresholds.get("gth_exact_entries") or 20),
            str(cohort_row("gth_exact_entry").get("status") or "collecting"),
        ),
        (
            "Put exact entries",
            int(cohort_row("put_exact_entry").get("count") or 0),
            int(thresholds.get("put_exact_entries") or 20),
            str(cohort_row("put_exact_entry").get("status") or "collecting"),
        ),
        (
            "exact spread complete exits",
            int(cohort_row("exact_spread_complete_exit").get("count") or 0),
            int(thresholds.get("exact_spread_exits") or 20),
            str(cohort_row("exact_spread_complete_exit").get("status") or "collecting"),
        ),
    )
    automatic_promotion = str(bool(readiness.get("automatic_promotion"))).lower()
    lines = [
        "",
        "## 裁决冻结/样本就绪度",
        "",
        f"readiness 状态为 `{readiness.get('status') or 'collecting'}`；"
        f"`automatic_promotion={automatic_promotion}`。本节只决定何时允许人工复审，"
        "不会根据回测结果自动放宽参数、提升策略或开启下单。",
        "",
        "| 冻结门槛 | 当前 | 目标 | 状态 |",
        "|---|---:|---:|---|",
    ]
    lines.extend(f"| {label} | {count} | {target} | {status} |" for label, count, target, status in rows)
    lines.extend(
        [
            "",
            "回测 `complete_sessions` 只采用 `readiness.sessions.details` 中 "
            "`complete=true` 的健康完整日期；裁决门槛进一步要求这些日期位于 v3 forward "
            "policy window 且无 contract violation。observed partitions 仍单独保留，不能冒充"
            "完整 session。",
            "",
            f"当前 observed partitions={window.get('observed_partition_count', 0)}，"
            f"health-complete backtest sessions={len(window.get('complete_sessions') or [])}，"
            f"contract-consistent sessions={session_count}。",
        ]
    )
    blockers = readiness.get("blockers") or []
    if blockers:
        lines.extend(["", "当前冻结 blocker：`" + "`, `".join(map(str, blockers)) + "`。"])
    return lines


def _render_report(artifact: dict, trades: Sequence[Trade]) -> str:
    profile_sets = artifact["profiles"]
    profile_names = [profile["name"] for profile in artifact["profile_configs"]]
    sets = profile_sets[PROFILE_BASELINE]  # detailed tables are baseline-only
    window = artifact["window"]
    intent_coverage = artifact.get("trade_intent_coverage") or {}
    trade_ready_decisions = artifact.get("trade_ready_decisions") or []
    production_total = artifact.get("production_strategy_total") or {}
    lines = [
        "# 0DTE 点位告警策略回测报告",
        "",
        "## 方法简述",
        "",
        "- 三个 control/proxy 集:S1 confirmed 级别确认、S2 定价结果 prefill(touched 样本)、"
        "S3 GTH 回踩确认;另列持久化的 production trade_ready 决策集。",
        "- 只有 trade_ready × naked 计入 production strategy 口径。S2 只是"
        "follow-through-only observational proxy,不得并入生产策略总盈亏。",
        "- 四种执行:裸买 0DTE 期权(naked)、5 点/10 点垂直借记价差(spread5/spread10)、"
        "墙锚定价差(spread_wall:S1 按结构推导;S3 仅复现事件中保存的生产两腿)。",
        "- 四个通用退出 profile:baseline(失效 level±3、1.3× 固定止盈、15 分钟时间止损)、"
        "wide_invalidation(失效缓冲 max(3, 0.15×EM))、"
        "trailing_tp(浮盈 +15% 激活、回撤峰值浮盈 1/3 按 bid 出场)、"
        "gth_360(仅 GTH 信号时间止损 15→360 分钟)。",
        "- 三个 GTH 价差出场 profile(仅 S1 GTH + S3、仅价差执行,时钟锚定到期日 09:45 ET):"
        "sat85(价差价值 ≥85% 宽度按 bid 止盈)、trail33(≥50% 宽度激活、峰值浮盈回撤 "
        "1/3 止盈)、clock(只挂失效线 + 时钟)。",
        "- 成交价:多头腿按 ask 进场、止损/到期按 bid 出场;价差腿按 long ask − short bid "
        "进、long bid − short ask 出;固定止盈按 mid 触发并按 mid 出场。双腿必须满足"
        "入场时效/时间偏差和逐笔 mark 新鲜度限制,不使用未来 short quote 或无限前填。",
        "- 未建模佣金、显式滑点、队列顺序、部分成交和市场冲击；top-of-book 结果偏乐观。",
        "- 退出规则按顺序:失效(标的反向破 level+缓冲,S3 为 ES 跌破 trough)、目标墙/"
        "公式目标、止盈(fixed/trailing/sat85/trail33)、时间止损、数据末端兜底。"
        "标的报价超过 30 秒不能触发墙/失效;计划退出附近没有新鲜可执行 mark 时跳过,"
        "不把早期数据末端当成交。",
        "- S1 在信号后 15 秒跟进进场;S2 按首次触碰时间+合约+play 语义去重,在首次触碰后持有"
        "15 秒,按"
        "方向×(spot−trigger) ≥ max(2,0.05×EM) 过门后重新读取 ask,历史 prefill 不计入 PnL;"
        "S3 有生产 spread 时直接使用保存的 long/short strike,不重新按 delta 选腿。",
        "- trade_ready 仅回放记录的 provider/contract/evaluated_at/entry_limit/expires_at;"
        "只有 entry window 内 ask≤limit 才成交。成交前先触及记录的 target/invalidation"
        "则跳过,标的路径缺失/陈旧也 fail closed;不使用 outcome horizons 判门。历史 runtime"
        "通知延迟/TTL 漂移无法还原,窗口严格采用 stored expires_at,且未模拟人工反应延迟。",
        "- Control/proxy 的 provider 仅按入场窗口内最早可执行报价选择(long ask / short "
        "bid),不使用未来路径覆盖度;trade_ready 固定使用决策中保存的 provider。所有 GTH "
        "持仓最迟在到期日 16:00 ET 截止。",
        *_readiness_section(artifact),
        "",
        "## 样本量与数据窗口",
        "",
        (
            f"完整数据窗口为 {window['first_session'] or '-'} 至 "
            f"{window['last_session'] or '-'} 共 {window['trading_days']} 个交易日"
            f"(cutoff={window['cutoff_at']}),样本量有限,胜率/盈亏因子仅用于判断方向"
            "是否值得继续 shadow。"
        ),
        "",
        "| 信号集 | 信号数 |",
        "|---|---:|",
    ]
    for set_name in SET_ORDER:
        lines.append(f"| {set_name} | {sets[set_name]['signals']} |")

    if intent_coverage:
        counts = intent_coverage.get("records_by_status") or {}
        distinct = intent_coverage.get("distinct_event_ids_by_status") or {}
        lines.extend(
            [
                "",
                "## Production trade intent 覆盖度",
                "",
                "以下为原始 evaluation record 覆盖度,不是 pass rate;observing 是非决策遥测,"
                "既不算 blocked,也不生成交易 PnL。重复 blocked evaluation 不被伪装成独立信号。",
                "",
                "| 状态 | evaluation records | distinct event_id |",
                "|---|---:|---:|",
            ]
        )
        for status in ("observing", "blocked", SET_TRADE_READY):
            lines.append(f"| {status} | {counts.get(status, 0)} | {distinct.get(status, 0)} |")
        lines.extend(
            [
                "",
                f"可回放的 unique trade_ready intent: "
                f"{intent_coverage.get('replay_eligible_trade_ready_signals', 0)}。"
                "方向(up/down)只作结果切片,未作为禁用 Put 的规则。",
            ]
        )
        if trade_ready_decisions:
            lines.extend(
                [
                    "",
                    "| intent | direction | replay result | skip reason | PnL$ |",
                    "|---|---|---|---|---:|",
                ]
            )
            for decision in trade_ready_decisions:
                lines.append(
                    f"| {decision['intent_id']} | {decision['direction']} | "
                    f"{decision['execution_result']} | {decision['skip_reason'] or '-'} | "
                    f"{_fmt_money(decision['pnl_usd'])} |"
                )

    if production_total:
        result = production_total["result"]
        lines.extend(
            [
                "",
                "## Production strategy total（严格口径）",
                "",
                "此处只汇总 trade_ready × baseline × naked;confirmed、prefill、gth_dip "
                "均为 control/proxy,不加进这个总计。",
                "",
                "| 可执行 n | 跳过 | 胜率 | 平均盈亏$ | 总盈亏$ |",
                "|---:|---:|---:|---:|---:|",
                f"| {result['n']} | {sum(result['skipped'].values())} | "
                f"{_fmt_pct(result['winrate'])} | {_fmt_money(result['avg_pnl_usd'])} | "
                f"{_fmt_money(result['total_pnl_usd'])} |",
            ]
        )

    # --- variant comparison section (profile x set, naked) -------------------
    lines.extend(
        [
            "",
            "## 退出规则变体对比",
            "",
            "naked 口径,按 profile × 信号集汇总:",
            "",
            "| profile | 信号集 | n | 胜率 | 平均盈亏$ | 盈亏因子 | 总盈亏$ |",
            "|---|---|---:|---:|---:|---:|---:|",
        ]
    )
    for profile_name in profile_names:
        for set_name in SET_ORDER:
            bucket = profile_sets[profile_name][set_name]["variants"][VARIANT_NAKED]
            lines.append(
                f"| {profile_name} | {set_name} | {bucket['n']} | "
                f"{_fmt_pct(bucket['winrate'])} | {_fmt_money(bucket['avg_pnl_usd'])} | "
                f"{_fmt_pf(bucket['profit_factor'])} | {_fmt_money(bucket['total_pnl_usd'])} |"
            )
    reason_names = sorted({row.exit_reason for row in trades})
    lines.extend(
        [
            "",
            "退出原因对比(全部执行合并计数):",
            "",
            "| profile | 信号集 | " + " | ".join(reason_names) + " |",
            "|---|---|" + "---:|" * len(reason_names),
        ]
    )
    for profile_name in profile_names:
        for set_name in SET_ORDER:
            pooled: dict[str, int] = {}
            for variant in VARIANTS:
                for reason, count in profile_sets[profile_name][set_name]["variants"][variant][
                    "exit_reasons"
                ].items():
                    pooled[reason] = pooled.get(reason, 0) + count
            cells = " | ".join(str(pooled.get(reason, 0)) for reason in reason_names)
            lines.append(f"| {profile_name} | {set_name} | {cells} |")

    # --- GTH spread-exit rule comparison (sat85 / trail33 / clock) ------------
    lines.extend(
        [
            "",
            "## GTH 价差出场规则对比",
            "",
            "以下均为 pre-v3 control/proxy 回放，不是新的 production exact-spread cohort；"
            "当前 forward GTH exact entry 与完整 exit 都是 0/20，因此这些盈亏不得用于"
            "晋级、禁用或调参。",
            "",
            "评估集合:S3 gth_dip + S1 GTH confirmed;时钟锚定到期日 09:45 ET(DST-aware)。"
            "中位持有时间为 exit−entry 分钟。历史 S3 若未保存生产两腿则不重建,"
            "因此 spread_wall 行可能只来自 S1 的结构推导价差。",
            "",
            "| 规则 | 执行 | n | 胜率 | 平均盈亏$ | 盈亏因子 | 中位持有分钟 |",
            "|---|---|---:|---:|---:|---:|---:|",
        ]
    )
    for rule in (PROFILE_SAT85, PROFILE_TRAIL33, PROFILE_CLOCK):
        for variant in SPREAD_VARIANTS:
            rows = [row for row in trades if row.profile == rule and row.variant == variant]
            if rows:
                win = sum(row.pnl_usd > 0 for row in rows) / len(rows)
                avg = statistics.fmean(row.pnl_usd for row in rows)
                win_s, avg_s = _fmt_pct(win), _fmt_money(avg)
            else:
                win_s = avg_s = "-"
            hold = _median_hold_minutes(rows)
            lines.append(
                f"| {rule} | {variant} | {len(rows)} | {win_s} | {avg_s} | "
                f"{_fmt_pf(_profit_factor(rows))} | "
                f"{f'{hold:.0f}' if hold is not None else '-'} |"
            )

    # --- headline table for every profile -----------------------------------
    lines.extend(
        [
            "",
            "## 总体结果(每 profile × 信号 × 执行)",
            "",
            "| profile | 信号集 | 执行 | n | 跳过 | 胜率 | 平均盈亏$ | 中位盈亏$ | 盈亏因子 | 总盈亏$ |",
            "|---|---|---|---:|---:|---:|---:|---:|---:|---:|",
        ]
    )
    for profile_name in profile_names:
        for set_name in SET_ORDER:
            for variant in VARIANTS:
                bucket = profile_sets[profile_name][set_name]["variants"][variant]
                skipped = sum(bucket["skipped"].values())
                lines.append(
                    f"| {profile_name} | {set_name} | {variant} | {bucket['n']} | {skipped} | "
                    f"{_fmt_pct(bucket['winrate'])} | {_fmt_money(bucket['avg_pnl_usd'])} | "
                    f"{_fmt_money(bucket['median_pnl_usd'])} | "
                    f"{_fmt_pf(bucket['profit_factor'])} | "
                    f"{_fmt_money(bucket['total_pnl_usd'])} |"
                )

    # --- baseline-only detail -------------------------------------------------
    lines.extend(
        [
            "",
            "## 退出原因分布(baseline)",
            "",
            "| 信号集 | 执行 | 原因 | 次数 |",
            "|---|---|---|---:|",
        ]
    )
    for set_name in SET_ORDER:
        for variant in VARIANTS:
            reasons = sets[set_name]["variants"][variant]["exit_reasons"]
            for reason, count in reasons.items():
                lines.append(f"| {set_name} | {variant} | {reason} | {count} |")

    for slice_title, slice_key in (
        ("按 thesis/play(baseline)", "by_thesis"),
        ("按 direction(baseline,仅切片不判门)", "by_direction"),
        ("按 level_kind(baseline)", "by_level_kind"),
        ("按 trend_regime(baseline)", "by_trend_regime"),
        ("按小时桶(America/New_York,baseline)", "by_hour_bucket"),
        ("按到期交易日(baseline)", "by_session_date"),
        ("按到期交易日星期(baseline)", "by_weekday"),
    ):
        lines.extend(
            [
                "",
                f"## 切片:{slice_title}",
                "",
                "| 信号集 | 执行 | 切片 | n | 胜率 | 平均盈亏$ | 总盈亏$ |",
                "|---|---|---|---:|---:|---:|---:|",
            ]
        )
        for set_name in SET_ORDER:
            for variant in VARIANTS:
                slices = sets[set_name]["variants"][variant]["slices"][slice_key]
                for name, stats in slices.items():
                    lines.append(
                        f"| {set_name} | {variant} | {name} | {stats['n']} | "
                        f"{_fmt_pct(stats['winrate'])} | {_fmt_money(stats['avg_pnl_usd'])} | "
                        f"{_fmt_money(stats['total_pnl_usd'])} |"
                    )

    gate_rows = []
    for variant in VARIANTS:
        gate = sets[SET_PREFILL]["variants"][variant].get("ft_gate") or {}
        for label, key in (("通过生产门并重取 ask", "gated"),):
            stats = gate.get(key)
            if stats:
                gate_rows.append(
                    f"| {variant} | {label} | {stats['n']} | {_fmt_pct(stats['winrate'])} | "
                    f"{_fmt_money(stats['avg_pnl_usd'])} | {_fmt_money(stats['total_pnl_usd'])} |"
                )
    if gate_rows:
        lines.extend(
            [
                "",
                "## S2 生产 follow-through 门(max(2,0.05×EM),baseline)",
                "",
                "| 执行 | 分组 | n | 胜率 | 平均盈亏$ | 总盈亏$ |",
                "|---|---|---:|---:|---:|---:|",
                *gate_rows,
                "",
                "未通过门或门口径数据不足的信号直接记为 skip,不生成交易 PnL。",
            ]
        )

    lines.extend(["", "## 关键发现", ""])
    lines.extend(_findings(profile_sets, trades, profile_names))
    lines.append("")
    return "\n".join(lines) + "\n"


def _findings(
    profile_sets: dict, trades: Sequence[Trade], profile_names: Sequence[str]
) -> list[str]:
    """Compute the key-findings bullets from the actual aggregates.

    Generic bullets use the baseline profile; the last three bullets answer the
    profile-comparison questions (wide invalidation / trailing tp / gth 360).
    """
    sets = profile_sets[PROFILE_BASELINE]
    base_trades = [row for row in trades if row.profile == PROFILE_BASELINE]
    bullets: list[str] = []
    buckets = [
        (set_name, variant, sets[set_name]["variants"][variant])
        for set_name in SET_ORDER
        for variant in VARIANTS
    ]
    eligible = [(s, v, b) for s, v, b in buckets if b["n"] >= 5]
    if eligible:
        best = max(eligible, key=lambda item: item[2]["expectancy_usd"])
        bullets.append(
            f"baseline 期望值最高的组合是 {best[0]} × {best[1]}:n={best[2]['n']},"
            f"胜率 {_fmt_pct(best[2]['winrate'])},平均盈亏 {_fmt_money(best[2]['avg_pnl_usd'])}$,"
            f"盈亏因子 {_fmt_pf(best[2]['profit_factor'])}。"
        )
        worst = min(eligible, key=lambda item: item[2]["expectancy_usd"])
        bullets.append(
            f"baseline 期望值最低的组合是 {worst[0]} × {worst[1]}:n={worst[2]['n']},"
            f"胜率 {_fmt_pct(worst[2]['winrate'])},平均盈亏 {_fmt_money(worst[2]['avg_pnl_usd'])}$,"
            f"盈亏因子 {_fmt_pf(worst[2]['profit_factor'])}。"
        )
    for set_name in SET_ORDER:
        variants = sets[set_name]["variants"]
        naked = variants[VARIANT_NAKED]
        spread_rows = [
            row
            for row in base_trades
            if row.set_name == set_name and row.variant in (VARIANT_SPREAD5, VARIANT_SPREAD10)
        ]
        if naked["n"] >= 5 and spread_rows:
            spread_win = sum(row.pnl_usd > 0 for row in spread_rows) / len(spread_rows)
            spread_avg = statistics.fmean(row.pnl_usd for row in spread_rows)
            bullets.append(
                f"baseline {set_name}:裸买胜率 {_fmt_pct(naked['winrate'])} vs 价差合并胜率 "
                f"{_fmt_pct(spread_win)};平均盈亏 {_fmt_money(naked['avg_pnl_usd'])}$ vs "
                f"{_fmt_money(spread_avg)}$ —— 价差用尾部风险换胜率的取舍"
                f"{'成立' if spread_win > (naked['winrate'] or 0) else '在本样本不成立'}。"
            )
    gate = sets[SET_PREFILL]["variants"][VARIANT_NAKED].get("ft_gate") or {}
    gated, ungated = gate.get("gated"), gate.get("ungated")
    if gated and ungated:
        helps = (gated["avg_pnl_usd"] or 0) > (ungated["avg_pnl_usd"] or 0)
        bullets.append(
            f"S2 生产 follow-through 门(max(2,0.05×EM),baseline):通过组 n={gated['n']},胜率 "
            f"{_fmt_pct(gated['winrate'])},平均 {_fmt_money(gated['avg_pnl_usd'])}$;"
            f"未通过组 n={ungated['n']},胜率 {_fmt_pct(ungated['winrate'])},平均 "
            f"{_fmt_money(ungated['avg_pnl_usd'])}$。门槛在本样本{'有' if helps else '没有'}增益。"
        )
    elif gated:
        bullets.append(
            f"S2 生产 follow-through 门(max(2,0.05×EM),门后重取 ask):可执行 n={gated['n']},"
            f"胜率 {_fmt_pct(gated['winrate'])},平均 {_fmt_money(gated['avg_pnl_usd'])}$。"
            "未过门信号不虚构入场或 PnL。"
        )
    slice_source = [
        (set_name, variant, name, stats)
        for set_name in SET_ORDER
        for variant in VARIANTS
        for name, stats in sets[set_name]["variants"][variant]["slices"]["by_thesis"].items()
        if stats["n"] >= 5
    ]
    if slice_source:
        best = max(slice_source, key=lambda item: item[3]["avg_pnl_usd"])
        bullets.append(
            f"baseline 按 thesis/play 切,平均盈亏最好的是 {best[0]} × {best[1]} × {best[2]}:"
            f"n={best[3]['n']},平均 {_fmt_money(best[3]['avg_pnl_usd'])}$,"
            f"胜率 {_fmt_pct(best[3]['winrate'])}。"
        )

    def _naked_rows(profile_name: str) -> list[Trade]:
        return [
            row for row in trades if row.profile == profile_name and row.variant == VARIANT_NAKED
        ]

    # (a) does the wider, EM-scaled invalidation buffer reduce stop-outs and
    # raise expectancy?
    if PROFILE_WIDE_INVALIDATION in profile_names:
        base, wide = _naked_rows(PROFILE_BASELINE), _naked_rows(PROFILE_WIDE_INVALIDATION)
        if base and wide:
            base_inv = sum(row.exit_reason == "invalidation" for row in base)
            wide_inv = sum(row.exit_reason == "invalidation" for row in wide)
            base_avg = statistics.fmean(row.pnl_usd for row in base)
            wide_avg = statistics.fmean(row.pnl_usd for row in wide)
            bullets.append(
                f"(a) 放宽失效缓冲(max(3, 0.15×EM),naked 全部信号合并):invalidation 退出 "
                f"{base_inv}/{len(base)} → {wide_inv}/{len(wide)},平均盈亏 "
                f"{_fmt_money(base_avg)}$ → {_fmt_money(wide_avg)}$ —— "
                f"{'扫损减少且期望改善' if wide_inv < base_inv and wide_avg > base_avg else '未同时减少扫损和改善期望'}。"
            )
    # (b) does trailing beat the fixed +30% target?
    if PROFILE_TRAILING_TP in profile_names:
        base, trail = _naked_rows(PROFILE_BASELINE), _naked_rows(PROFILE_TRAILING_TP)
        if base and trail:
            base_pt = sum(row.exit_reason == "profit_target" for row in base)
            trail_tp = sum(row.exit_reason == "trailing_tp" for row in trail)
            base_avg = statistics.fmean(row.pnl_usd for row in base)
            trail_avg = statistics.fmean(row.pnl_usd for row in trail)
            base_win = sum(row.pnl_usd > 0 for row in base) / len(base)
            trail_win = sum(row.pnl_usd > 0 for row in trail) / len(trail)
            bullets.append(
                f"(b) 移动止盈 vs 固定 +30%(naked):固定止盈触发 {base_pt} 次,trailing 触发 "
                f"{trail_tp} 次;胜率 {_fmt_pct(base_win)} → {_fmt_pct(trail_win)},平均盈亏 "
                f"{_fmt_money(base_avg)}$ → {_fmt_money(trail_avg)}$ —— 本样本移动止盈"
                f"{'更优' if trail_avg > base_avg else '不优于固定止盈'}。"
            )
    # (c) does the 360-minute GTH convention stop the bleed?
    if PROFILE_GTH_360 in profile_names:

        def _gth_naked(profile_name: str) -> list[Trade]:
            return [
                row
                for row in trades
                if row.profile == profile_name
                and row.variant == VARIANT_NAKED
                and row.underlier_source.startswith("future:ES")
            ]

        base_gth, g360 = _gth_naked(PROFILE_BASELINE), _gth_naked(PROFILE_GTH_360)
        if base_gth and g360:
            base_avg = statistics.fmean(row.pnl_usd for row in base_gth)
            g360_avg = statistics.fmean(row.pnl_usd for row in g360)
            base_win = sum(row.pnl_usd > 0 for row in base_gth) / len(base_gth)
            g360_win = sum(row.pnl_usd > 0 for row in g360) / len(g360)
            g360_ts = sum(row.exit_reason == "time_stop" for row in g360)
            verdict = "胜率仍为零" if g360_win == 0 else "胜率不再为零(不再全亏)"
            verdict += ",但平均期望改善" if g360_avg > base_avg else ",平均期望反而恶化"
            bullets.append(
                f"(c) GTH 信号 360 分钟口径(naked,underlier=future:ES):baseline n={len(base_gth)},"
                f"胜率 {_fmt_pct(base_win)},平均 {_fmt_money(base_avg)}$ → gth_360 n={len(g360)},"
                f"胜率 {_fmt_pct(g360_win)},平均 {_fmt_money(g360_avg)}$(其中 {g360_ts} 笔按 "
                f"360 分钟 time_stop 出场)—— 本样本 GTH 口径{verdict}。"
            )
    # (d) sat85 vs trail33 vs clock on the production wall spread (GTH set)
    spread_rule_rows = {
        rule: [row for row in trades if row.profile == rule and row.variant == VARIANT_SPREAD_WALL]
        for rule in (PROFILE_SAT85, PROFILE_TRAIL33, PROFILE_CLOCK)
    }
    if any(spread_rule_rows.values()):
        parts = []
        for rule, rows in spread_rule_rows.items():
            if not rows:
                parts.append(f"{rule} 无成交")
                continue
            win = sum(row.pnl_usd > 0 for row in rows) / len(rows)
            avg = statistics.fmean(row.pnl_usd for row in rows)
            hold = _median_hold_minutes(rows)
            parts.append(
                f"{rule} n={len(rows)},胜率 {_fmt_pct(win)},平均 {_fmt_money(avg)}$,"
                f"盈亏因子 {_fmt_pf(_profit_factor(rows))},中位持有 {hold:.0f} 分钟"
            )
        scored = [
            (rule, statistics.fmean(row.pnl_usd for row in rows))
            for rule, rows in spread_rule_rows.items()
            if rows
        ]
        best_mean = max((mean for _, mean in scored), default=None)
        leaders = (
            [rule for rule, mean in scored if abs(mean - best_mean) < 1e-9]
            if best_mean is not None
            else []
        )
        verdict = (
            f"本样本期望值最高的是 {leaders[0]}。"
            if len(leaders) == 1
            else f"{', '.join(leaders)} 结果相同,本样本无法排序。"
        )
        bullets.append(f"(d) GTH 价差出场规则(spread_wall):{'；'.join(parts)}。{verdict}")
    s3_wall = profile_sets[PROFILE_SAT85]["gth_dip"]["variants"][VARIANT_SPREAD_WALL]
    if s3_wall["n"] == 0:
        bullets.append(
            "历史 S3 事件未保存生产 exact spread 两腿,严格 spread_wall 可回放数为 0;"
            "本轮不能验证生产 GTH sat85 出场。"
        )
    if base_trades:
        tail = min(base_trades, key=lambda row: row.pnl_usd)
        bullets.append(
            f"baseline 全场最差一笔:{tail.set_name} {tail.variant} {tail.contract_id} "
            f"{tail.exit_reason} 退出,亏损 {_fmt_money(tail.pnl_usd)}$ —— 裸买/价差的尾部"
            "风险差异需更大样本确认。"
        )
    return [f"- {bullet}" for bullet in bullets]
