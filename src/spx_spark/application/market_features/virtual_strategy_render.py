"""Deterministic five-section operator rendering for virtual exits."""

from __future__ import annotations

from typing import Mapping

from spx_spark.application.market_features.virtual_strategy_support import (
    _fmt,
    _number,
    _pct,
)
from spx_spark.notifier.operator_cards import render_operator_card


def render_virtual_strategy_exit(closed: Mapping[str, object]) -> str:
    """Render a shadow exit without implying a broker position or fill."""

    contracts = str(closed.get("contract_id") or "-")
    if closed.get("position_type") == "call_debit_spread":
        contracts = f"{closed.get('long_contract_id')} / 卖 {closed.get('short_contract_id')}"
    exit_bid = _number(closed.get("exit_bid"))
    exit_price = (
        f"虚拟入场  {_fmt(closed.get('entry_mid'))} · 可执行退出 bid {_fmt(exit_bid)}"
        if exit_bid is not None
        else f"虚拟入场  {_fmt(closed.get('entry_mid'))} · 无可执行退出报价；不计算退出收益"
    )
    snapshot = closed.get("exit_snapshot")
    snapshot = snapshot if isinstance(snapshot, Mapping) else {}
    target_lines = [
        f"MFE  {_pct(closed.get('mfe_fraction'))} · MAE {_pct(closed.get('mae_fraction'))}"
    ]
    if _number(closed.get("target_spx")) is not None:
        target_lines.append(f"冻结目标 SPX  {_fmt(closed.get('target_spx'))}")
    if _number(closed.get("invalidation_spx")) is not None:
        target_lines.append(f"冻结失效 SPX  {_fmt(closed.get('invalidation_spx'))}")
    opportunity = (
        closed.get("operator_opportunity_id")
        or closed.get("source_signal_id")
        or closed.get("episode_id")
        or "不可用"
    )
    return render_operator_card(
        desk_view="\n".join(
            (
                f"🔴 EXIT · {closed.get('exit_action') or 'exit'}",
                f"策略  {closed.get('source_kind') or '不可用'}",
                f"机会  {opportunity} · generation {closed.get('reentry_generation', 0)}",
                f"合约  {contracts}",
            )
        ),
        execution="\n".join(
            (f"动作  {closed.get('exit_action') or 'exit'}", exit_price)
        ),
        risk="\n".join(
            (
                f"退出原因  {closed.get('exit_reason') or '不可用'}",
                "仅为影子报价路径；不读取 IBKR 仓位，不代表挂单、成交或真实账户风险。",
                "自动下单  关闭",
            )
        ),
        targets="\n".join(target_lines),
        data_quality="\n".join(
            (
                f"exit price basis  {closed.get('exit_price_basis') or '不可用'} · "
                f"pnl status {closed.get('pnl_status') or '不可用'}",
                f"provider  {snapshot.get('provider') or '不可用'} · source time "
                f"{snapshot.get('quote_time') or snapshot.get('source_at') or '不可用'}",
                "缺少可执行 bid 时禁止计算或展示退出收益。",
            )
        ),
    )
