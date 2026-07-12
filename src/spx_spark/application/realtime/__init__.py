"""Realtime application package."""

from __future__ import annotations

from spx_spark.application.realtime.contracts import EngineTick
from spx_spark.application.realtime.engine import RealtimeEngine
from spx_spark.application.realtime.health import evaluate_engine_health

__all__ = ["EngineTick", "RealtimeEngine", "evaluate_engine_health"]
