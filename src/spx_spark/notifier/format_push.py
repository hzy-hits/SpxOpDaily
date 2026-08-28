"""Format a single writer markdown into Feishu cards and Bark payloads.

Lane doctrine:
- Feishu: trading reading surface (interactive markdown card), including
  position events. System/ops alerts stay out.
- Bark main: everything — trading/position gets short lockscreen + optional
  markdown detail; ops stay short plain text in a separate group.
- Bark friend: market-signal trading only, short plain text.
"""

from __future__ import annotations

import re
from typing import Any

from spx_spark.notifier.policy import (
    alerts_are_market_signals,
    is_position_holding_alert,
    is_system_event_alert,
)

# Header color for Feishu interactive cards.
FEISHU_HEADER_BY_KIND = {
    "order_map": "blue",
    "status": "blue",
    "morning_map": "green",
    "post_close_review": "purple",
    "intraday_alert": "orange",
    "direct_event": "orange",
    "ops": "red",
    "system": "red",
}

BARK_OPS_GROUP_DEFAULT = "spx-ops"
BARK_TRADE_GROUP_DEFAULT = "spx-spark"

_MD_HEADING_RE = re.compile(r"^#{1,3}\s+")
_MD_BOLD_RE = re.compile(r"\*\*(.+?)\*\*")
_MD_CODE_RE = re.compile(r"`([^`]+)`")
_MD_BULLET_RE = re.compile(r"^[-*]\s+")
_MD_LINK_RE = re.compile(r"\[([^\]]+)\]\((https?://[^)]+)\)")
_URL_RE = re.compile(r"https?://[^\s)]+")
_SPX_STATUS_HEADER_RE = re.compile(r"^【(SPX (?:15m|状态) · .+)】$")
_STATUS_PLAN_RE = re.compile(r"^(计划\d+\s*·\s*\S+)\s{2}(.*)$")
_TABLE_SEPARATOR_CELL_RE = re.compile(r"^:?-{3,}:?$")
_WALL_LAYOUT_HEADERS = ["SPX 墙位", "结构", "合约", "当前 mid", "触位情景", "触发后参考"]
_INTERMEDIATE_WALL_LAYOUT_HEADERS = [
    "SPX 墙位",
    "结构",
    "合约",
    "当前 mid",
    "BS 触位区间",
    "触发后参考",
]
_LEGACY_WALL_LAYOUT_HEADERS = [
    "SPX 墙位",
    "结构",
    "合约",
    "当前 mid",
    "BS 触位价",
    "触发后参考",
]
_EXPOSURE_LAYOUT_HEADERS = [
    "SPX Strike",
    "位置",
    "CΔ / PΔ",
    "CΓ / PΓ",
    "OI GEX净/绝 · DEX净/绝",
    "量 GEX净/绝 · DEX净/绝",
]
_COLLAPSED_STATUS_SECTIONS = frozenset(
    {"Greeks 与波动", "ES 与跨资产确认", "风险中性分布", "观察情景与 BS 审计"}
)


def _status_card_template(text: str) -> str:
    if "🟢 MANUAL READY" in text:
        return "green"
    if "🔴 只观察" in text or "🔴 状态快照" in text:
        return "grey"
    if "CONFIRMED" in text:
        return "green"
    if any(
        phase in text
        for phase in (
            "APPROACHING",
            "TESTING",
            "BREAK_PENDING",
            "REJECT_PENDING",
            "RETEST",
        )
    ):
        return "orange"
    if "INVALIDATED" in text or "EXPIRED" in text:
        return "grey"
    return "blue"


def _format_status_line(line: str) -> str:
    plan = _STATUS_PLAN_RE.match(line)
    if plan:
        return f"- **{plan.group(1)}**　{plan.group(2)}"
    for label in (
        "时钟",
        "价格",
        "结构",
        "OI",
        "状态",
        "突破过滤",
        "ES确认",
        "波动",
        "执行",
        "方向",
        "等待",
        "证伪",
        "合约",
        "类型",
        "买入",
        "卖出",
        "NBBO",
        "限价",
        "触发",
        "止损",
        "目标",
        "退出",
        "有效",
        "风险",
        "解释",
        "权限",
        "变化",
        "数据",
    ):
        prefix = f"{label}  "
        if line.startswith(prefix):
            content = line.removeprefix(prefix)
            if label == "执行":
                return f"> **{label}**　{content}"
            return f"**{label}**　{content}"
    return line


def _status_card_parts(markdown: str) -> tuple[str, list[dict[str, Any]], str] | None:
    """Convert the compact SPX status text into a scannable Feishu card body."""
    lines = markdown.strip().splitlines()
    if not lines or (header := _SPX_STATUS_HEADER_RE.match(lines[0])) is None:
        return None

    blocks: list[list[str]] = [[]]
    for raw in lines[1:]:
        line = raw.strip()
        if not line:
            if blocks[-1]:
                blocks.append([])
            continue
        if line.startswith("【条件计划】"):
            line = "**条件计划**　标的触发后执行"
        blocks[-1].append(_format_status_line(line))
    blocks = [block for block in blocks if block]

    elements: list[dict[str, Any]] = []
    for index, block in enumerate(blocks):
        if index:
            elements.append({"tag": "hr"})
        elements.append(
            {
                "tag": "markdown",
                "content": "\n".join(block),
                "text_align": "left",
            }
        )
    return header.group(1), elements, _status_card_template(markdown)


def _sectioned_card_parts(
    markdown: str,
    *,
    fallback_title: str,
    template: str,
) -> tuple[str, list[dict[str, Any]], str] | None:
    """Split writer Markdown into consistent, scannable Feishu sections."""

    lines = markdown.strip().splitlines()
    if not lines or not any(line.startswith("## ") for line in lines):
        return None
    header_title = fallback_title
    first = lines[0].strip()
    if first.startswith("【") and first.endswith("】"):
        header_title = first.removeprefix("【").removesuffix("】")
        lines = lines[1:]
    elif first.startswith("# "):
        header_title = first.removeprefix("# ").strip()
        lines = lines[1:]

    blocks: list[list[str]] = []
    current: list[str] = []
    for raw in lines:
        line = raw.rstrip()
        if line.startswith("## ") and current:
            blocks.append(current)
            current = []
        if line or current:
            current.append(line)
    if current:
        blocks.append(current)

    elements: list[dict[str, Any]] = []
    table_index = 0
    for index, block in enumerate(blocks):
        content = "\n".join(block).strip()
        if not content:
            continue
        section_title = (
            content.splitlines()[0].removeprefix("## ").strip() if content.startswith("## ") else ""
        )
        collapsed = section_title in _COLLAPSED_STATUS_SECTIONS
        render_content = "\n".join(content.splitlines()[1:]).strip() if collapsed else content
        if index:
            elements.append({"tag": "hr"})
        block_elements, table_index = _markdown_and_table_elements(
            render_content,
            table_index=table_index,
        )
        if collapsed and block_elements:
            elements.append(
                {
                    "tag": "collapsible_panel",
                    "expanded": False,
                    "header": {
                        "title": {"tag": "plain_text", "content": section_title},
                        "background_color": "grey",
                        "vertical_align": "center",
                        "padding": "8px",
                    },
                    "border": {"color": "grey", "corner_radius": "6px"},
                    "padding": "8px",
                    "elements": block_elements,
                }
            )
        else:
            elements.extend(block_elements)
    return header_title, elements, template


def _table_cells(line: str) -> list[str] | None:
    stripped = line.strip()
    if not stripped.startswith("|") or not stripped.endswith("|"):
        return None
    return [cell.strip() for cell in stripped[1:-1].split("|")]


def _is_table_separator(cells: list[str] | None) -> bool:
    return bool(cells) and all(_TABLE_SEPARATOR_CELL_RE.fullmatch(cell) for cell in cells)


def _native_table_element(
    headers: list[str],
    rows: list[list[str]],
    *,
    table_index: int,
) -> dict[str, Any]:
    column_names = [f"c{index}" for index in range(len(headers))]
    return {
        "tag": "table",
        "element_id": f"table_{table_index + 1}",
        "page_size": min(max(len(rows), 1), 10),
        "row_height": "auto",
        "freeze_first_column": True,
        "header_style": {
            "text_align": "left",
            "text_size": "normal",
            "background_style": "grey",
            "text_color": "default",
            "bold": True,
            "lines": 1,
        },
        "columns": [
            {
                "name": name,
                "display_name": header,
                "data_type": "text",
                "width": "auto",
                "horizontal_align": (
                    "left" if header == "结构" or header.startswith("合约") else "right"
                ),
            }
            for index, (name, header) in enumerate(zip(column_names, headers, strict=True))
        ],
        "rows": [
            {
                name: row[index] if index < len(row) else ""
                for index, name in enumerate(column_names)
            }
            for row in rows
        ],
    }


def _wall_layout_element(rows: list[list[str]], *, table_index: int) -> dict[str, Any]:
    """Merge six analytical fields into a mobile-safe native three-column table."""

    compact_rows: list[list[str]] = []
    for row in rows:
        cells = [*row, *([""] * max(0, 6 - len(row)))]
        compact_rows.append(
            [
                f"{cells[0]}\n{cells[1]}",
                f"{cells[2]}\n现 {cells[3]}",
                f"触位情景 {cells[4]}\n触发后 {cells[5]}",
            ]
        )
    return _native_table_element(
        ["结构", "合约 / 现价", "触位情景 / 触发后"],
        compact_rows,
        table_index=table_index,
    )


def _exposure_layout_element(rows: list[list[str]], *, table_index: int) -> dict[str, Any]:
    """Fold the six-field exposure map into a mobile-safe three-column table."""

    compact_rows: list[list[str]] = []
    for row in rows:
        cells = [*row, *([""] * max(0, 6 - len(row)))]
        compact_rows.append(
            [
                f"{cells[0]}\n{cells[1]}",
                f"Δ {cells[2]}\nΓ {cells[3]}",
                f"OI {cells[4]}\n量 {cells[5]}",
            ]
        )
    return _native_table_element(
        ["Strike / 位置", "Delta / Gamma", "GEX / DEX proxy"],
        compact_rows,
        table_index=table_index,
    )


def _markdown_and_table_elements(
    content: str,
    *,
    table_index: int,
) -> tuple[list[dict[str, Any]], int]:
    """Convert GFM-style tables into native Feishu JSON 2.0 tables."""

    lines = content.splitlines()
    elements: list[dict[str, Any]] = []
    markdown_lines: list[str] = []

    def flush_markdown() -> None:
        markdown = "\n".join(markdown_lines).strip()
        if markdown:
            elements.append({"tag": "markdown", "content": markdown, "text_align": "left"})
        markdown_lines.clear()

    index = 0
    while index < len(lines):
        headers = _table_cells(lines[index])
        separator = _table_cells(lines[index + 1]) if index + 1 < len(lines) else None
        if headers and len(headers) >= 2 and _is_table_separator(separator):
            flush_markdown()
            index += 2
            rows: list[list[str]] = []
            while index < len(lines):
                row = _table_cells(lines[index])
                if row is None:
                    break
                rows.append(row)
                index += 1
            if rows:
                if headers in (
                    _WALL_LAYOUT_HEADERS,
                    _INTERMEDIATE_WALL_LAYOUT_HEADERS,
                    _LEGACY_WALL_LAYOUT_HEADERS,
                ):
                    elements.append(_wall_layout_element(rows, table_index=table_index))
                    table_index += 1
                elif headers == _EXPOSURE_LAYOUT_HEADERS:
                    elements.append(_exposure_layout_element(rows, table_index=table_index))
                    table_index += 1
                else:
                    elements.append(_native_table_element(headers, rows, table_index=table_index))
                    table_index += 1
            continue
        markdown_lines.append(lines[index])
        index += 1
    flush_markdown()
    return elements, table_index


def strip_markdown_light(text: str) -> str:
    """Enough to make a lockscreen line readable without raw ** markers."""
    lines: list[str] = []
    for raw in text.splitlines():
        line = _MD_HEADING_RE.sub("", raw)
        line = _MD_BULLET_RE.sub("• ", line)
        line = _MD_BOLD_RE.sub(r"\1", line)
        line = _MD_CODE_RE.sub(r"\1", line)
        lines.append(line.rstrip())
    return "\n".join(lines).strip()


def bark_lockscreen_summary(
    text: str,
    *,
    title: str = "",
    kind: str = "",
    include_links: bool = False,
    max_lines: int = 4,
    max_chars: int = 280,
) -> str:
    """Render a decision-first Bark preview instead of slicing writer prose."""

    sections, lines = _bark_content(text, title=title)
    if not lines:
        return strip_markdown_light(text)[:max_chars]
    upper = text.upper()
    if "EXIT REVIEW" in upper or "CANCEL ENTRY" in upper or "READY CANCELLED" in upper:
        selected = _bark_terminal_lines(sections, lines)
    elif (
        "MANUAL READY" in upper
        or kind == "trade_intent"
        or ("只许限价" in text and ("净借记" in text or "净贷记" in text))
    ):
        selected = _bark_ready_lines(sections, lines)
    elif "持仓" in title or "持仓事件" in text:
        selected = _bark_prefixed_lines(lines, ("状态", "动作", "风险", "数据"))
    elif "系统" in title or "系统状态" in text:
        selected = _bark_prefixed_lines(lines, ("状态", "影响", "动作", "数据"))
    elif any(token in text for token in ("NO TRADE", "只观察", "不做")):
        selected = _bark_observation_lines(sections, lines, title=title)
    else:
        selected = _bark_default_lines(sections, lines)

    selected = _unique_lines(selected)[:max_lines]
    if not selected:
        selected = lines[:max_lines]
    summary = "\n".join(selected).strip()
    if len(summary) > max_chars:
        summary = summary[: max_chars - 1].rstrip() + "…"
    if not include_links:
        return summary
    links: list[tuple[str, str]] = []
    for label, url in _MD_LINK_RE.findall(text):
        if url not in {item[1] for item in links}:
            links.append((strip_markdown_light(label), url))
    if not links:
        for line in text.splitlines():
            urls = _URL_RE.findall(line)
            for url in urls:
                if url not in {item[1] for item in links}:
                    label = line.split(url, 1)[0].strip(" ：:") or "查看图表"
                    links.append((strip_markdown_light(label), url))
    if not links:
        return summary
    link_line = "图表  " + " · ".join(f"[{label}]({url})" for label, url in links[:2])
    return f"{summary}\n\n{link_line}"


def bark_display_title(title: str) -> str:
    """Remove producer jargon from the Bark-only title."""

    replacements = (
        ("SPX GTH OPERATOR CANDIDATE · MANUAL ONLY", "SPX GTH 人工候选"),
        ("SPX GTH EXIT REVIEW · MANUAL ONLY", "SPX GTH 退出检查"),
        ("SPX GTH READY CANCELLED · MANUAL ONLY", "SPX GTH 候选已失效"),
        (" · MANUAL ONLY", ""),
    )
    compact = title
    for old, new in replacements:
        compact = compact.replace(old, new)
    return compact


def _bark_content(text: str, *, title: str) -> tuple[dict[str, list[str]], list[str]]:
    sections: dict[str, list[str]] = {"": []}
    current = ""
    flat: list[str] = []
    for raw in text.splitlines():
        stripped = raw.strip()
        if stripped.startswith("## "):
            current = stripped.removeprefix("## ").strip()
            sections.setdefault(current, [])
            continue
        if not stripped or (stripped.startswith("【") and stripped.endswith("】")):
            continue
        if stripped.startswith("# ") or _MD_LINK_RE.fullmatch(stripped):
            continue
        line = _clean_bark_line(strip_markdown_light(stripped))
        if not line or line == title or _bark_noise(line):
            continue
        sections.setdefault(current, []).append(line)
        flat.append(line)
    return sections, _unique_lines(flat)


def _clean_bark_line(line: str) -> str:
    for phrase in (
        "（仅事件背景，不是入场授权）",
        "（不等于入场授权）",
        "（仅结构背景）",
    ):
        line = line.replace(phrase, "")
    replacements = (
        ("0DTE vs next ATM IV gap ", "0DTE ATM IV − 次日 ATM IV："),
        (" 1h surface shift ", " 平均 IV 曲面 1小时变化："),
        (" 1h ATM IV change ", " ATM IV 1小时变化："),
        (" ATM IV jump ", " ATM IV 5分钟变化："),
        (" put skew steepening ", " Put 25Δ 偏斜 5分钟变化："),
        (" put skew ratio change ", " Put 偏斜比 5分钟变化："),
        (" surface shift ", " 平均 IV 曲面 5分钟变化："),
        ("level_source_invalidated", "关键位失效"),
        ("level_source_expired", "信号已过期"),
        ("level_source_not_confirmed", "关键位确认已撤销"),
        ("level_source_formal_signal_absent", "确认信号不再存在"),
        ("level_source_quality_invalid", "数据质量不再合格"),
        ("trend_transition_session_mismatch", "趋势状态已切换"),
    )
    for old, new in replacements:
        line = line.replace(old, new)
    return line.replace(" vol pts", " 波动率点").strip()


def _bark_noise(line: str) -> bool:
    return bool(
        line.startswith(("决策 id=", "数据  as_of=", "策略决策 本周期不可用"))
        or line.startswith("等待  对应关键位确认")
        or line.startswith("合约  当前没有可执行合约")
        or line.startswith("权限  自动下单关闭")
        or line.startswith("交易  本条不是 Call/Put 信号")
    )


def _section(sections: dict[str, list[str]], *names: str) -> list[str]:
    return [line for name in names for line in sections.get(name, ())]


def _first(lines: list[str], *prefixes: str) -> str:
    for prefix in prefixes:
        for line in lines:
            if line.startswith(prefix):
                return line
        for line in lines:
            if prefix in line:
                return line
    return ""


def _join_lines(*lines: str, limit: int = 132) -> str:
    joined = " · ".join(_unique_lines([line for line in lines if line]))
    return joined if len(joined) <= limit else joined[: limit - 1].rstrip() + "…"


def _unique_lines(lines: list[str]) -> list[str]:
    unique: list[str] = []
    seen: set[str] = set()
    for line in lines:
        key = re.sub(r"\s+", " ", line).strip().casefold()
        if line and key not in seen:
            seen.add(key)
            unique.append(line)
    return unique


def _bark_ready_lines(
    sections: dict[str, list[str]], lines: list[str]
) -> list[str]:
    desk = _section(sections, "Desk View", "结论")
    execution = _section(sections, "Execution", "执行")
    risk_lines = _section(sections, "Risk", "风险")
    targets = _section(sections, "Targets", "目标")
    lead = _first(desk, "MANUAL READY", "只许限价") or (desk[0] if desk else lines[0])
    buy = _first(execution, "买入")
    sell = _first(execution, "卖出")
    contract = _join_lines(buy, sell) if buy else _first(execution, "四腿", "合约")
    if not contract:
        contract = next(
            (
                line
                for line in execution
                if not line.startswith(
                    ("MANUAL READY", "类型", "Provider", "NBBO", "限价", "净借记", "净贷记", "有效", "提交", "权限")
                )
            ),
            "",
        )
    if "铁鹰" in lead:
        contract = ""
    entry = _first(execution, "限价", "净借记", "净贷记")
    valid = _first(execution, "有效")
    max_loss = _first(risk_lines, "最大亏损", "风险")
    stop = _first(risk_lines, "止损", "失效")
    target = _first(targets, "止盈", "目标", "SPX") or (targets[0] if targets else "")
    risk = _join_lines(max_loss, stop)
    if contract:
        risk = _join_lines(risk, target)
        return [lead, contract, _join_lines(entry, valid), risk]
    return [lead, _join_lines(entry, valid), risk, target]


def _bark_terminal_lines(
    sections: dict[str, list[str]], lines: list[str]
) -> list[str]:
    desk = _section(sections, "Desk View", "结论")
    execution = _section(sections, "Execution", "执行")
    risk = _section(sections, "Risk", "风险")
    lead = _first(desk + lines, "EXIT REVIEW", "READY CANCELLED", "CANCEL ENTRY")
    unfilled = _first(execution, "未成交")
    filled = _first(execution, "已成交")
    reason = _first(risk, "原因")
    contract = _first(risk, "合约")
    return [lead or lines[0], unfilled, filled, _join_lines(reason, contract)]


def _bark_observation_lines(
    sections: dict[str, list[str]], lines: list[str], *, title: str
) -> list[str]:
    desk = _section(sections, "Desk View", "结论")
    event = _first(lines, "事件", "变化")
    lead = _first(desk + lines, "NO TRADE", "不做", "只观察")
    if "波动率" in title and event:
        return [event, "结论  仅观察；不判断涨跌"]
    cause = _first(desk + lines, "主因", "原因", "方向", "解释")
    next_step = _first(lines, "下一步", "触发", "等待")
    quality = _first(lines, "数据", "执行")
    return [lead or event or lines[0], event or cause, next_step, quality]


def _bark_prefixed_lines(lines: list[str], prefixes: tuple[str, ...]) -> list[str]:
    selected = [_first(lines, prefix) for prefix in prefixes]
    return selected or lines[:4]


def _bark_default_lines(
    sections: dict[str, list[str]], lines: list[str]
) -> list[str]:
    desk = _section(sections, "Desk View", "结论")
    execution = _section(sections, "Execution", "执行")
    risk = _section(sections, "Risk", "风险")
    targets = _section(sections, "Targets", "目标", "下一步")
    lead = desk[0] if desk else lines[0]
    action = _first(execution, "限价", "执行", "动作", "买入", "卖出")
    risk_line = _first(risk, "止损", "失效", "风险") or (risk[0] if risk else "")
    target = _first(targets, "目标", "下一步") or (targets[0] if targets else "")
    return [lead, action, risk_line, target]


def push_lane_for_alerts(alerts: list[dict[str, object]]) -> str:
    """Classify a batch: trade / ops / mixed.

    Feishu only receives pure trade batches. Bark main receives all.
    """
    if not alerts:
        return "ops"
    if alerts_are_market_signals(alerts):
        return "trade"
    if all(is_system_event_alert(alert) for alert in alerts):
        return "ops"
    # Position events go to Feishu + Bark main (trade lane). Friend Bark stays
    # off because positions are not MARKET_SIGNAL kinds.
    if all(is_position_holding_alert(alert) for alert in alerts):
        return "trade"
    if any(is_system_event_alert(alert) for alert in alerts):
        return "mixed"
    # Reviewed market narratives that may include non-MARKET_SIGNAL kinds
    # (e.g. wall proximity already filtered) still count as trade if no ops.
    kinds = {str(alert.get("kind") or "") for alert in alerts}
    ops_prefixes = (
        "ibkr_session_",
        "market_data_",
        "required_data_",
        "optional_data_",
        "option_quote_freshness",
    )
    if any(kind.startswith(ops_prefixes) or kind in {"iv_surface_stale"} for kind in kinds):
        return "ops"
    return "trade"


def feishu_header_template(kind: str, *, lane: str = "trade", text: str = "") -> str:
    if lane == "ops":
        return FEISHU_HEADER_BY_KIND["ops"]
    if kind in FEISHU_HEADER_BY_KIND:
        template = FEISHU_HEADER_BY_KIND[kind]
    else:
        template = "blue"
    # Escalate color when the writer already said the script changed.
    if "剧本有变" in text or "需要看盘" in text:
        return "orange"
    return template


def build_feishu_card(
    markdown: str,
    *,
    title: str,
    kind: str = "status",
    lane: str = "trade",
) -> dict[str, Any]:
    """Feishu interactive card (schema 2.0) with a single markdown body."""
    # Feishu markdown is close to commonmark; keep writer output mostly intact.
    content = markdown.strip() or "（空推送）"
    # Soft length guard: webhook cards get awkward past ~30KB; truncate body.
    if len(content) > 28000:
        content = content[:27900].rstrip() + "\n\n…（已截断）"
    template = feishu_header_template(kind, lane=lane, text=content)
    if kind == "status":
        state_template = _status_card_template(content)
        if state_template != "blue":
            template = state_template
    status_parts = _status_card_parts(content) if kind == "status" else None
    header_title = title.strip() or "SPX Spark"
    body_elements: list[dict[str, Any]] = [
        {
            "tag": "markdown",
            "content": content,
            "text_align": "left",
        }
    ]
    sectioned = (
        _sectioned_card_parts(
            content,
            fallback_title=header_title,
            template=template,
        )
        if any(line.startswith("## ") for line in content.splitlines())
        else None
    )
    if sectioned is not None:
        header_title, body_elements, template = sectioned
    elif status_parts is not None:
        header_title, body_elements, template = status_parts
    elif sectioned := _sectioned_card_parts(
        content,
        fallback_title=header_title,
        template=template,
    ):
        header_title, body_elements, template = sectioned
    if len(header_title) > 50:
        header_title = header_title[:49] + "…"
    return {
        "schema": "2.0",
        "config": {"update_multi": True},
        "header": {
            "title": {"tag": "plain_text", "content": header_title},
            "template": template,
        },
        "body": {
            "direction": "vertical",
            "padding": "16px 16px 16px 16px",
            "elements": body_elements,
        },
    }


def bark_groups_for_lane(
    lane: str,
    *,
    trade_group: str,
    ops_group: str,
) -> str:
    if lane == "ops":
        return ops_group or BARK_OPS_GROUP_DEFAULT
    return trade_group or BARK_TRADE_GROUP_DEFAULT
