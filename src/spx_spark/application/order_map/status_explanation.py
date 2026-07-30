"""Strictly bounded LLM rewriting for the non-authoritative status reason."""

from __future__ import annotations

import json
import re
from typing import Any

from spx_spark.analytics.options.pricing import finite_float


STATUS_EXPLANATION_SYSTEM_PROMPT = "\n".join(
    (
        "你只负责把一条 SPX 状态卡的确定性阻断原因改写成一句简短中文。",
        "只输出一行，必须以“原因  ”开头，不得输出标题、列表、英文枚举或内部字段名。",
        "不得改变卡片已经给出的方向、触发、证伪、墙位、合约状态或执行权限。",
        "不得新增数字，不得写买入、卖出、开仓、挂单、限价、止盈、止损或任何操作建议。",
        "如果输入已经是清楚中文，原样返回。",
    )
)


def build_status_explanation_prompt(
    payload: dict[str, Any],
    deterministic_reason: str,
) -> str:
    """Expose only reason evidence; deterministic card fields stay out of reach."""

    decision = payload.get("level_decision")
    decision = decision if isinstance(decision, dict) else {}
    underlier = payload.get("underlier")
    underlier = underlier if isinstance(underlier, dict) else {}
    facts = {
        "reason": deterministic_reason,
        "underlier_source": underlier.get("source"),
        "decision_phase": decision.get("phase"),
        "structure_change_pending": decision.get("structure_change_pending"),
        "warnings": [str(item) for item in payload.get("warnings") or ()][:3],
    }
    return "事实:" + json.dumps(facts, ensure_ascii=False, separators=(",", ":"))


def status_explanation_output_valid(text: str) -> bool:
    value = str(text or "").strip()
    lexical_check = value.replace("SPX", "").replace("OI", "")
    if (
        not value.startswith("原因  ")
        or "\n" in value
        or len(value) > 100
        or re.search(r"[A-Za-z_]|\d|=", lexical_check)
    ):
        return False
    forbidden = ("买入", "卖出", "开仓", "挂单", "限价", "止盈", "止损", "下单")
    return not any(token in value for token in forbidden)


def humanize_operator_trigger(text: str) -> str:
    value = str(text)
    for internal, human in (
        ("REJECTED→RETEST→CONFIRMED", "拒绝→回测→确认"),
        ("ACCEPTED→RETEST→CONFIRMED", "接受→回测→确认"),
        ("REJECTED→CONFIRMED", "拒绝→确认"),
        ("状态机 CONFIRMED", "状态机确认"),
        ("完成 CONFIRMED", "完成确认"),
        ("CONFIRMED", "确认"),
    ):
        value = value.replace(internal, human)
    return value


def operator_reason_line(payload: dict[str, Any]) -> str:
    """Render the one deterministic reason line shown on compact status cards."""

    underlier = payload.get("underlier")
    underlier = underlier if isinstance(underlier, dict) else {}
    decision = payload.get("level_decision")
    decision = decision if isinstance(decision, dict) else {}
    intent = payload.get("trade_intent")
    intent = intent if isinstance(intent, dict) else {}
    gth = str(decision.get("session_mode") or "") == "globex"
    decision_spot = finite_float(decision.get("spot"))
    decision_es = finite_float(decision.get("es"))
    reasons: list[str] = []
    if (
        underlier.get("price") is None
        and decision_spot is None
        and not (gth and decision_es is not None)
    ):
        reasons.append("可靠 SPX 坐标暂不可用")
    if decision.get("structure_change_pending") is True:
        reasons.append("新结构仍在确认")
    if str(decision.get("phase") or "far").lower() == "far":
        reasons.append("当前未触发关键位，趋势信号继续独立评估")
    if intent.get("status") == "blocked":
        blocked = [
            _humanize_internal_reason(str(item))
            for item in intent.get("block_reasons") or ()
            if str(item)
        ]
        reasons.append(blocked[0] if blocked else "实时合约报价尚未就绪")
    if not reasons:
        reasons.append("等待下一次方向触发和实时合约报价")
    return "原因  " + "；".join(dict.fromkeys(reasons[:3]))


def _humanize_internal_reason(value: str) -> str:
    reason = str(value or "").strip()
    exact = {
        "source_signal_unavailable": "尚未出现方向触发",
        "exact_option_quote_unavailable": "实时合约报价尚未就绪",
        "exact_option_contract_unavailable": "尚未找到满足条件的实时合约",
        "pricing_gate_failed": "实时合约报价尚未就绪",
        "session_not_gth": "当前不在夜盘交易时段",
        "session_not_rth": "当前不在日盘交易时段",
        "event_expired": "方向触发已经过期",
        "insufficient_reward_to_risk": "当前收益风险比不足",
        "entry_deadline_expired": "当前入场有效期已过",
    }
    if reason in exact:
        return exact[reason]
    if "quote" in reason or "pricing" in reason:
        return "实时合约报价尚未就绪"
    if "source" in reason or "signal" in reason:
        return "尚未出现方向触发"
    if "expired" in reason or "stale" in reason:
        return "方向或报价已经过期"
    return "执行条件尚未完整"
