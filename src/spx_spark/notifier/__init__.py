"""通知管道:选取→审阅→gate→双通道投递。"""

from spx_spark.notifier.missed_queue import append_missed, flush_missed
from spx_spark.notifier.deepseek import run_deepseek_reviewer
from spx_spark.notifier.dispatcher import (
    DispatchResult,
    EnqueueResult,
    consume_pending_notifications,
    dispatch_notification,
    enqueue_notification,
)
from spx_spark.notifier.model import (
    CommandRunner,
    NotificationResult,
    SinkResult,
    default_runner,
)
from spx_spark.notifier.pipeline import notify_payload
from spx_spark.notifier.receipts import NotificationEnvelope, notification_event_id
from spx_spark.notifier.policy import (
    alert_key,
    alerts_are_latency_critical,
    alerts_are_market_signals,
    codex_message_requests_delivery,
    codex_message_respects_human_scope,
    context_only_alerts,
    direct_push_alerts,
    is_human_visible_alert,
    is_market_signal_alert,
    severity_value,
    split_time_sensitive_review_candidates,
    strong_time_sensitive_score,
)
from spx_spark.notifier.prompts import build_codex_prompt, format_alert_message
from spx_spark.notifier.sinks import (
    openclaw_delivery_error,
    run_codex_exec,
    run_grok_agent,
    run_openclaw_agent,
    send_bark_friend_message,
    send_bark_message,
    send_feishu_card,
    send_openclaw_message,
    deliver_trade_push,
    any_delivery_ok,
    im_delivery_ok,
)
from spx_spark.notifier.state import (
    mark_alerts_sent,
    select_alerts_for_notification,
)

__all__ = [
    "append_missed",
    "CommandRunner",
    "DispatchResult",
    "EnqueueResult",
    "flush_missed",
    "NotificationResult",
    "NotificationEnvelope",
    "SinkResult",
    "alert_key",
    "alerts_are_latency_critical",
    "alerts_are_market_signals",
    "build_codex_prompt",
    "codex_message_requests_delivery",
    "codex_message_respects_human_scope",
    "context_only_alerts",
    "consume_pending_notifications",
    "default_runner",
    "dispatch_notification",
    "direct_push_alerts",
    "enqueue_notification",
    "format_alert_message",
    "is_human_visible_alert",
    "is_market_signal_alert",
    "mark_alerts_sent",
    "notify_payload",
    "notification_event_id",
    "openclaw_delivery_error",
    "run_codex_exec",
    "run_deepseek_reviewer",
    "run_grok_agent",
    "run_openclaw_agent",
    "select_alerts_for_notification",
    "send_bark_friend_message",
    "send_bark_message",
    "send_feishu_card",
    "send_openclaw_message",
    "deliver_trade_push",
    "any_delivery_ok",
    "im_delivery_ok",
    "severity_value",
    "split_time_sensitive_review_candidates",
    "strong_time_sensitive_score",
]
