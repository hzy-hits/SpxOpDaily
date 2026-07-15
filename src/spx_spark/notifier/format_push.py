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
_SPX_STATUS_HEADER_RE = re.compile(r"^【(SPX 15m · .+)】$")
_STATUS_PLAN_RE = re.compile(r"^(计划\d+\s*·\s*\S+)\s{2}(.*)$")
_TABLE_SEPARATOR_CELL_RE = re.compile(r"^:?-{3,}:?$")


def _status_card_template(text: str) -> str:
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
        if index:
            elements.append({"tag": "hr"})
        block_elements, table_index = _markdown_and_table_elements(
            content,
            table_index=table_index,
        )
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
                "horizontal_align": "left" if index in {0, 3, 5} else "right",
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
                elements.append(
                    _native_table_element(headers, rows, table_index=table_index)
                )
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


def bark_lockscreen_summary(text: str, *, max_lines: int = 4, max_chars: int = 280) -> str:
    """First few non-empty lines for the iOS notification preview."""
    plain = strip_markdown_light(text)
    lines = [line for line in plain.splitlines() if line.strip()]
    if not lines:
        return plain[:max_chars]
    summary = "\n".join(lines[:max_lines]).strip()
    if len(summary) > max_chars:
        summary = summary[: max_chars - 1].rstrip() + "…"
    return summary


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
