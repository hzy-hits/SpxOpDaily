"""通知管道:选取→审阅→gate→双通道投递。"""

from spx_spark.notifier.missed_queue import append_missed, flush_missed
from spx_spark.notifier.model import (
    CommandRunner,
    NotificationResult,
    SinkResult,
    default_runner,
)
from spx_spark.notifier.pipeline import notify_payload
from spx_spark.notifier.policy import (
    alert_key,
    codex_message_requests_delivery,
    codex_message_respects_human_scope,
    direct_push_alerts,
    is_human_visible_alert,
    severity_value,
)
from spx_spark.notifier.prompts import build_codex_prompt, format_alert_message
from spx_spark.notifier.sinks import (
    openclaw_delivery_error,
    run_codex_exec,
    run_openclaw_agent,
    send_bark_message,
    send_openclaw_message,
)
from spx_spark.notifier.state import (
    mark_alerts_sent,
    select_alerts_for_notification,
)

__all__ = [
    "append_missed",
    "CommandRunner",
    "flush_missed",
    "NotificationResult",
    "SinkResult",
    "alert_key",
    "build_codex_prompt",
    "codex_message_requests_delivery",
    "codex_message_respects_human_scope",
    "default_runner",
    "direct_push_alerts",
    "format_alert_message",
    "is_human_visible_alert",
    "mark_alerts_sent",
    "notify_payload",
    "openclaw_delivery_error",
    "run_codex_exec",
    "run_openclaw_agent",
    "select_alerts_for_notification",
    "send_bark_message",
    "send_openclaw_message",
    "severity_value",
]
