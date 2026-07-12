"""Realtime pipeline step ordering (documentation + re-exports).

``RealtimeEngine.tick`` is the sole orchestrator; this module names the
allowed steps so callers/tests can reason about order without I/O.
"""

from __future__ import annotations

PIPELINE_STEPS = (
    "read_snapshot",
    "validate_quality",
    "analytics_kernel",
    "alert_evaluator",
    "publish_projection",
    "append_outbox",
    "record_metrics",
)

FORBIDDEN_SIDE_CHANNELS = (
    "send_wechat",
    "call_llm",
    "refresh_oauth",
    "operate_ib_gateway",
)
