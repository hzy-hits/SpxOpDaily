from __future__ import annotations

from datetime import datetime

from spx_spark.config import NotificationSettings
from spx_spark.notifier.model import CommandRunner, NotificationResult, SinkResult, default_runner
from spx_spark.notifier.policy import (
    codex_message_requests_delivery,
    codex_message_respects_human_scope,
    direct_push_alerts,
)
from spx_spark.notifier.prompts import build_codex_prompt, format_alert_message
from spx_spark.notifier.sinks import (
    bark_title_for_alerts,
    run_codex_exec,
    run_openclaw_agent,
    send_bark_message,
    send_openclaw_message,
)
from spx_spark.notifier.state import mark_alerts_sent, select_alerts_for_notification


def notify_payload(
    payload: dict[str, object],
    *,
    settings: NotificationSettings | None = None,
    runner: CommandRunner = default_runner,
    now: datetime | None = None,
) -> NotificationResult:
    settings = settings or NotificationSettings.from_env()
    if not settings.enabled:
        return NotificationResult(
            enabled=False,
            selected_count=0,
            sent_count=0,
            skipped_reason="disabled",
            sinks=(),
        )

    selected, sent_at_by_key = select_alerts_for_notification(payload, settings, now=now)
    if not selected:
        return NotificationResult(
            enabled=True,
            selected_count=0,
            sent_count=0,
            skipped_reason="no_alerts_after_severity_or_cooldown",
            sinks=(),
        )

    sinks: list[SinkResult] = []
    message = format_alert_message(payload, selected)
    bypass_alerts = direct_push_alerts(selected)
    review_candidates = [alert for alert in selected if alert not in bypass_alerts]
    alerts_marked_sent: list[dict[str, object]] = []
    if settings.openclaw_enabled:
        direct_result = send_openclaw_message(settings, message, runner=runner)
        sinks.append(direct_result)
        delivered_ok = direct_result.ok
        if settings.bark_enabled:
            bark_result = send_bark_message(settings, bark_title_for_alerts(selected), message)
            sinks.append(bark_result)
            delivered_ok = delivered_ok or bark_result.ok
        if delivered_ok:
            alerts_marked_sent = list(selected)
    elif bypass_alerts:
        bypass_message = format_alert_message(payload, bypass_alerts)
        direct_result = send_openclaw_message(settings, bypass_message, runner=runner)
        sinks.append(direct_result)
        delivered_ok = direct_result.ok
        if settings.bark_enabled:
            bark_result = send_bark_message(
                settings,
                bark_title_for_alerts(bypass_alerts),
                bypass_message,
            )
            sinks.append(bark_result)
            delivered_ok = delivered_ok or bark_result.ok
        if delivered_ok:
            alerts_marked_sent = list(bypass_alerts)
    if settings.openclaw_agent_enabled and review_candidates:
        agent_result, agent_message = run_openclaw_agent(
            settings,
            build_codex_prompt(payload, review_candidates),
            runner=runner,
        )
        sinks.append(agent_result)
        if agent_result.ok:
            should_deliver = (
                codex_message_requests_delivery(agent_message)
                if settings.codex_require_delivery_cue
                else True
            )
            scope_ok = codex_message_respects_human_scope(agent_message)
            if should_deliver and scope_ok:
                if settings.openclaw_agent_deliver:
                    agent_delivery = send_openclaw_message(settings, agent_message, runner=runner)
                    sinks.append(agent_delivery)
                    delivered_ok = agent_delivery.ok
                    if settings.bark_enabled:
                        bark_result = send_bark_message(
                            settings,
                            bark_title_for_alerts(review_candidates),
                            agent_message,
                        )
                        sinks.append(bark_result)
                        delivered_ok = delivered_ok or bark_result.ok
                    if delivered_ok:
                        alerts_marked_sent.extend(review_candidates)
                else:
                    # Analysis-only mode: still start the cooldown so the same
                    # bucket is not re-reviewed every cycle.
                    alerts_marked_sent.extend(review_candidates)
            elif should_deliver and not scope_ok:
                sinks.append(
                    SinkResult(
                        sink="openclaw_agent_scope_gate",
                        attempted=True,
                        ok=True,
                        error="openclaw agent output mentioned non-focus context",
                    )
                )
                # The review verdict stands for this bucket; don't re-run the
                # agent every cycle for the same alerts.
                alerts_marked_sent.extend(review_candidates)
            elif not should_deliver:
                sinks.append(
                    SinkResult(
                        sink="openclaw_agent_delivery_gate",
                        attempted=True,
                        ok=True,
                        error="openclaw agent output did not request delivery",
                    )
                )
                alerts_marked_sent.extend(review_candidates)
    elif settings.codex_enabled and review_candidates:
        codex_result, codex_message = run_codex_exec(
            settings,
            build_codex_prompt(payload, review_candidates),
            runner=runner,
        )
        sinks.append(codex_result)
        if codex_result.ok and settings.codex_deliver:
            should_deliver = (
                codex_message_requests_delivery(codex_message)
                if settings.codex_require_delivery_cue
                else True
            )
            scope_ok = codex_message_respects_human_scope(codex_message)
            if should_deliver:
                if scope_ok:
                    codex_delivery = send_openclaw_message(settings, codex_message, runner=runner)
                    sinks.append(codex_delivery)
                    delivered_ok = codex_delivery.ok
                    if settings.bark_enabled:
                        bark_result = send_bark_message(
                            settings,
                            bark_title_for_alerts(review_candidates),
                            codex_message,
                        )
                        sinks.append(bark_result)
                        delivered_ok = delivered_ok or bark_result.ok
                    if delivered_ok:
                        alerts_marked_sent.extend(review_candidates)
                else:
                    sinks.append(
                        SinkResult(
                            sink="codex_scope_gate",
                            attempted=True,
                            ok=True,
                            error="codex output mentioned non-focus context",
                        )
                    )
                    alerts_marked_sent.extend(review_candidates)
            else:
                sinks.append(
                    SinkResult(
                        sink="codex_delivery_gate",
                        attempted=True,
                        ok=True,
                        error="codex output did not request delivery",
                    )
                )
                alerts_marked_sent.extend(review_candidates)

    sent_count = sum(1 for sink in sinks if sink.sink in ("openclaw_message", "bark") and sink.ok)
    if alerts_marked_sent:
        mark_alerts_sent(alerts_marked_sent, sent_at_by_key, settings, now=now)
    skipped_reason = None if sinks else "no_enabled_sinks"
    return NotificationResult(
        enabled=True,
        selected_count=len(selected),
        sent_count=sent_count,
        skipped_reason=skipped_reason,
        sinks=tuple(sinks),
    )
