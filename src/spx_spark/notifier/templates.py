"""Versioned message templates.

Templates own presentation only: a renderer receives immutable facts and returns
the channel text. The registry keeps one version per id so a wording change can
be reviewed and rolled back independently of the decision contract.
"""

from __future__ import annotations

from collections.abc import Callable, Mapping
from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True, slots=True)
class RenderedMessage:
    title: str
    text: str
    feishu_text: str | None = None
    sections: tuple[Mapping[str, str], ...] = ()


@dataclass(frozen=True, slots=True)
class Template:
    template_id: str
    version: int
    message_class: str
    channels: tuple[str, ...]
    render: Callable[[Mapping[str, Any]], RenderedMessage]

    def __post_init__(self) -> None:
        if not self.template_id.strip():
            raise ValueError("template_id is required")
        if self.version < 1:
            raise ValueError("template version must be >= 1")
        if not self.channels:
            raise ValueError("template channels are required")


_REGISTRY: dict[tuple[str, int], Template] = {}


def register(template: Template) -> Template:
    key = (template.template_id, template.version)
    if key in _REGISTRY:
        raise ValueError(f"duplicate template: {key}")
    _REGISTRY[key] = template
    return template


def template(template_id: str, version: int | None = None) -> Template:
    if version is not None:
        try:
            return _REGISTRY[(template_id, version)]
        except KeyError as exc:
            raise KeyError(f"unknown template: {template_id}.v{version}") from exc
    candidates = [
        item for (name, _version), item in _REGISTRY.items() if name == template_id
    ]
    if not candidates:
        raise KeyError(f"unknown template: {template_id}")
    return max(candidates, key=lambda item: item.version)


def render_message(
    template_id: str,
    *,
    channel: str,
    context: Mapping[str, Any],
    version: int | None = None,
) -> RenderedMessage:
    selected = template(template_id, version)
    if channel not in selected.channels:
        raise ValueError(f"template {template_id} does not render channel {channel}")
    return selected.render(context)


def registered_templates() -> tuple[Template, ...]:
    return tuple(_REGISTRY.values())
