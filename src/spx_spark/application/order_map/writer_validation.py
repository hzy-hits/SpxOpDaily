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
_REQUIRED_STATUS_LINE_PREFIXES = ("Put候选[",)
_PUT_TOKEN_PATTERN = re.compile(r"(?<![A-Za-z])put(?![A-Za-z])", re.IGNORECASE)
_PUT_ACTION_LANGUAGE_PATTERN = re.compile(
    r"(?:策略|候选|执行|订单|下单|挂单|买入|卖出|买|卖|交易|操作|资格|"
    r"可用|提供|准备|计划|开仓|成交|strategy|candidate|executable|execution|"
    r"order|trade|buy|sell|ready|eligible|available)",
    re.IGNORECASE,
)
_PUT_REPORT_CONTRADICTION_PATTERNS = (
    re.compile(
        r"(?:没有|并无|无|不存在)\s*(?:任何|可用的?|有效的?)?\s*"
        r"Put\s*(?:交易)?\s*(?:策略|候选)",
        re.IGNORECASE,
    ),
    re.compile(
        r"Put\s*(?:交易)?\s*(?:策略|候选)\s*(?:不存在|缺失|不可用|没有)",
        re.IGNORECASE,
    ),
    re.compile(r"(?<!不)可执行\s*(?:的)?\s*Put\s*(?:订单|计划|策略)?", re.IGNORECASE),
    re.compile(
        r"Put\s*(?:是|为)?\s*(?<!不)可执行\s*(?:的)?\s*(?:订单|计划|策略)?",
        re.IGNORECASE,
    ),
    re.compile(r"Put\s*(?:可以|可)\s*执行", re.IGNORECASE),
    re.compile(
        r"Put\s*(?:现)?已\s*(?:挂出|挂单|下单|成交)(?:\s*订单)?",
        re.IGNORECASE,
    ),
    re.compile(
        r"Put\s*(?:订单|计划|策略)\s*(?:现)?已\s*(?:挂出|挂单|下单|成交)",
        re.IGNORECASE,
    ),
    re.compile(
        r"(?:已|已经|准备|将要|可以|可)\s*(?:挂单|下单|买入|卖出|买)\s*Put",
        re.IGNORECASE,
    ),
    re.compile(
        r"(?:准备|将要)\s*(?:挂单|下单)\s*(?:买入|卖出|买|卖)?\s*Put",
        re.IGNORECASE,
    ),
    re.compile(r"\bPut\s+(?:is\s+)?(?:executable|trade[- ]?ready|live order)\b", re.IGNORECASE),
)


def globex_writer_output_valid(text: str, template: str) -> bool:
    """Reject invented or rebound numeric facts in an off-hours brief."""

    if any(phrase in text for phrase in _GLOBEX_FORBIDDEN_PHRASES):
        return False
    template_header = template.splitlines()[0].strip() if template.strip() else ""
    is_status_template = template_header.startswith("【SPX 15m ·")
    if is_status_template and not text.startswith(template_header):
        return False
    # Numeric membership alone is insufficient: an LLM can move a valid wall
    # value onto the wrong label or omit a populated L1 line. Every
    # deterministic template line containing a number must therefore survive
    # verbatim apart from whitespace. Narrative-only lines may still be edited.
    if is_status_template:
        normalized_output_lines = {
            re.sub(r"\s+", "", line) for line in text.splitlines() if line.strip()
        }
        for line in template.splitlines():
            normalized_line = re.sub(r"\s+", "", line)
            required = _NUMBER_PATTERN.search(line) or line.startswith(
                _REQUIRED_STATUS_LINE_PREFIXES
            )
            if required and normalized_line not in normalized_output_lines:
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
    if "Put候选[" in template and not _put_action_language_is_template_bound(
        text,
        template,
    ):
        return False
    if "Put候选[" in template and any(
        pattern.search(text) for pattern in _PUT_REPORT_CONTRADICTION_PATTERNS
    ):
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


def _put_action_language_is_template_bound(text: str, template: str) -> bool:
    """Allow Put action semantics only on deterministic template lines."""

    allowed_lines = {
        re.sub(r"\s+", "", line)
        for line in template.splitlines()
        if _PUT_TOKEN_PATTERN.search(line)
    }
    for line in text.splitlines():
        if not _PUT_TOKEN_PATTERN.search(line) or not _PUT_ACTION_LANGUAGE_PATTERN.search(line):
            continue
        if re.sub(r"\s+", "", line) not in allowed_lines:
            return False
    return True
