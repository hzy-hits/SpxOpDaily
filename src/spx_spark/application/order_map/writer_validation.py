"""Safety validation for LLM-rewritten order-map messages."""

from __future__ import annotations

import re


_NUMBER_PATTERN = re.compile(r"(?<![A-Za-z_])[-+]?\d+(?:\.\d+)?")
_TEMPLATE_CANDIDATE_PATTERN = re.compile(
    r"(?:\[(?:地图候选|条件计划)\]|计划\d+\s*·).*?SPXW\s+(\d{4}[CP])"
)
_GLOBEX_FORBIDDEN_PHRASES = (
    "无引力",
    "气垫",
    "gamma 燃料",
    "卖方收工",
    "真金白银",
    "JSON 中部被截断",
    "补齐 JSON",
    "完整 JSON",
    "I need the full JSON",
    "I'll pull",
)


def globex_writer_output_valid(text: str, template: str) -> bool:
    """Reject invented prices and causal decoration in an off-hours brief."""

    if any(phrase in text for phrase in _GLOBEX_FORBIDDEN_PHRASES):
        return False
    template_header = template.splitlines()[0].strip() if template.strip() else ""
    if template_header.startswith("【SPX 15m ·") and not text.startswith(template_header):
        return False
    allowed = [float(value) for value in _NUMBER_PATTERN.findall(template)]
    for raw in _NUMBER_PATTERN.findall(text):
        value = float(raw)
        if value in {0.0, 1.0}:
            continue
        tolerance = 0.11 if abs(value) < 100_000 else 0.0
        if not any(abs(value - candidate) <= tolerance for candidate in allowed):
            return False
    return True


def actionable_writer_output_valid(text: str, template: str) -> bool:
    """Require numeric fidelity and conditional-execution semantics."""

    if not globex_writer_output_valid(text, template):
        return False
    contracts = tuple(dict.fromkeys(_TEMPLATE_CANDIDATE_PATTERN.findall(template)))
    if contracts and any(contract not in text for contract in contracts):
        return False
    live_plan = "入场≤" in template or "实时执行: NBBO" in template
    if contracts:
        if live_plan:
            if not any(marker in text for marker in ("入场≤", "买入上限")):
                return False
            if "当前不可预挂" in text:
                return False
        elif "当前不可预挂" not in text:
            return False
    if "【条件计划】" in template:
        return text.startswith("【SPX 15m ·") and "\n\n" in text and "【条件计划】" in text
    return True
