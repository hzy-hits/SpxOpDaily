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
    header_title = title.strip() or "SPX Spark"
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
            "padding": "12px 12px 12px 12px",
            "elements": [
                {
                    "tag": "markdown",
                    "content": content,
                    "text_align": "left",
                }
            ],
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
