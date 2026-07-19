"""Shared helpers for IBKR stream collector tests."""

from __future__ import annotations

import pytest


def patch_stream(monkeypatch: pytest.MonkeyPatch, name: str, value) -> None:
    """Patch a stream dependency everywhere StreamCollector may look it up."""
    import spx_spark.ibkr.stream.deps as deps
    import spx_spark.ibkr.stream_collector as facade

    monkeypatch.setattr(deps, name, value, raising=False)
    monkeypatch.setattr(facade, name, value, raising=False)
    for mod_name in (
        "session_ops",
        "slow_poll_ops",
        "option_subscription_ops",
        "pin_ops",
        "spy_rotation_ops",
        "flush_ops",
        "collector",
        "supervisor",
        "cli",
        "session",
        "models",
    ):
        mod = __import__(f"spx_spark.ibkr.stream.{mod_name}", fromlist=["*"])
        if hasattr(mod, name):
            monkeypatch.setattr(mod, name, value)
