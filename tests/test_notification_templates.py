from __future__ import annotations

import pytest

from spx_spark.notifier.templates import (
    RenderedMessage,
    Template,
    register,
    registered_templates,
    render_message,
)


def test_template_registry_versions_channels_and_builtins() -> None:
    register(Template("test_card", 1, "ready", ("bark",), lambda _ctx: RenderedMessage("t", "v1")))
    register(Template("test_card", 2, "ready", ("bark",), lambda _ctx: RenderedMessage("t", "v2")))

    assert render_message("test_card", channel="bark", context={}).text == "v2"
    assert render_message("test_card", channel="bark", context={}, version=1).text == "v1"
    with pytest.raises(ValueError):
        render_message("test_card", channel="feishu", context={})

    import spx_spark.application.order_map.delivery_templates  # noqa: F401

    builtins = {(item.template_id, item.version) for item in registered_templates()}
    assert {("trade_ready", 1), ("pin_stable_watch", 1)} <= builtins
