"""Strictly bounded LLM rewriting for the non-authoritative status reason."""

from __future__ import annotations

import json
import re
from typing import Any


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
