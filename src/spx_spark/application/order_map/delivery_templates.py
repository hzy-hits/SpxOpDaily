"""Template adapters for the order-map delivery renderers."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from spx_spark.application.order_map.delivery import (
    _gamma_proxy_line,
    _ict_liquidity_line,
    _pin_watch_title,
    _render_pin_stable_watch,
    _render_strategy_candidate,
    _strategy_card_title,
)
from spx_spark.application.order_map.guidance import price_action_playbook_text
from spx_spark.notifier.templates import RenderedMessage, Template, register


def _markdown_sections(text: str) -> tuple[Mapping[str, str], ...]:
    sections: list[Mapping[str, str]] = []
    title = ""
    lines: list[str] = []
    for line in text.splitlines():
        if line.startswith("## "):
            if title or any(item.strip() for item in lines):
                sections.append({"title": title, "content": "\n".join(lines).strip()})
            title = line[3:].strip()
            lines = []
        else:
            lines.append(line)
    if title or any(item.strip() for item in lines):
        sections.append({"title": title, "content": "\n".join(lines).strip()})
    return tuple(section for section in sections if section["content"])


def _render_trade_ready_template(context: Mapping[str, Any]) -> RenderedMessage:
    decision = context["decision"]
    candidate = context["candidate"]
    text = _render_strategy_candidate(decision, candidate) + str(context.get("extra") or "")
    text = (
        f"{text}\n\n## 决策参考\n"
        f"盘型：{price_action_playbook_text(decision)}\n"
        f"{_gamma_proxy_line(decision)}\n"
        f"{_ict_liquidity_line(decision, candidate)}"
    )
    if context.get("quote_status") == "refresh_required":
        text = "本次报价已过期；刷新组合报价并重新确认后才可提交限价单。\n\n" + text
    return RenderedMessage(
        title=_strategy_card_title(candidate),
        text=text,
        feishu_text=text,
        sections=_markdown_sections(text),
    )


def _render_pin_watch_template(context: Mapping[str, Any]) -> RenderedMessage:
    decision = context["decision"]
    text = _render_pin_stable_watch(
        decision,
        center=context["center"],
        minutes=context["minutes"],
        phase=context["phase"],
    )
    return RenderedMessage(
        title=_pin_watch_title(context["regime"], context["center"]),
        text=text,
        feishu_text=text,
        sections=_markdown_sections(text),
    )


register(Template("trade_ready", 1, "ready", ("bark", "feishu"), _render_trade_ready_template))
register(Template("pin_stable_watch", 1, "watch", ("bark", "feishu"), _render_pin_watch_template))
