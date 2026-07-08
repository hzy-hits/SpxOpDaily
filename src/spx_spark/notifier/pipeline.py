from __future__ import annotations

from datetime import datetime, timezone

from spx_spark.config import NotificationSettings
from spx_spark.notifier.deepseek import deepseek_usage_limited, run_deepseek_reviewer
from spx_spark.notifier.llm_writer import load_previous_push, record_push
from spx_spark.notifier.missed_queue import append_missed, flush_missed
from spx_spark.notifier.model import CommandRunner, NotificationResult, SinkResult, default_runner
from spx_spark.notifier.policy import (
    alerts_are_market_signals,
    codex_message_requests_delivery,
    codex_message_respects_human_scope,
    direct_push_alerts,
    split_time_sensitive_review_candidates,
)
from spx_spark.notifier.prompts import build_codex_prompt, format_alert_message
from spx_spark.notifier.sinks import (
    bark_title_for_alerts,
    run_codex_exec,
    run_openclaw_agent,
    send_bark_friend_message,
    send_bark_message,
    send_openclaw_message,
)
from spx_spark.notifier.state import mark_alerts_sent, select_alerts_for_notification


def _failopen_critical_alerts(
    payload: dict[str, object],
    review_candidates: list[dict[str, object]],
    *,
    settings: NotificationSettings,
    runner: CommandRunner,
    now_utc: datetime,
    alerts_marked_sent: list[dict[str, object]],
    sinks: list[SinkResult],
) -> None:
    critical_alerts = [
        alert
        for alert in review_candidates
        if str(alert.get("severity", "")).lower() == "critical"
    ]
    if not critical_alerts:
        return
    failopen_message = format_alert_message(payload, critical_alerts)
    direct_result = send_openclaw_message(settings, failopen_message, runner=runner)
    sinks.append(direct_result)
    delivered_ok = direct_result.ok
    if not direct_result.ok:
        append_missed(
            settings.missed_queue_path,
            failopen_message,
            kind="failopen",
            at=now_utc,
        )
    if settings.bark_enabled:
        bark_result = send_bark_message(
            settings,
            bark_title_for_alerts(critical_alerts),
            failopen_message,
        )
        sinks.append(bark_result)
        delivered_ok = delivered_ok or bark_result.ok
    if delivered_ok:
        alerts_marked_sent.extend(critical_alerts)


def _mark_noncritical_reviewed_after_model_limit(
    review_candidates: list[dict[str, object]],
    *,
    alerts_marked_sent: list[dict[str, object]],
    sinks: list[SinkResult],
    sink_name: str,
) -> None:
    noncritical = [
        alert
        for alert in review_candidates
        if str(alert.get("severity", "")).lower() != "critical"
    ]
    if not noncritical:
        return
    alerts_marked_sent.extend(noncritical)
    sinks.append(
        SinkResult(
            sink=f"{sink_name}_rate_limit_cooldown",
            attempted=True,
            ok=True,
            error="model rate/usage limit; non-critical alerts marked reviewed",
        )
    )


def _deliver_review_message(
    *,
    message: str,
    review_candidates: list[dict[str, object]],
    settings: NotificationSettings,
    runner: CommandRunner,
    now_utc: datetime,
    alerts_marked_sent: list[dict[str, object]],
    sinks: list[SinkResult],
    reviewer_name: str,
    delivery_kind: str,
    deliver: bool,
) -> bool:
    should_deliver = (
        codex_message_requests_delivery(message) if settings.codex_require_delivery_cue else True
    )
    scope_ok = codex_message_respects_human_scope(message)
    if should_deliver and scope_ok:
        if deliver:
            delivery = send_openclaw_message(settings, message, runner=runner)
            sinks.append(delivery)
            if not delivery.ok:
                append_missed(
                    settings.missed_queue_path,
                    message,
                    kind=delivery_kind,
                    at=now_utc,
                )
            delivered_ok = delivery.ok
            if settings.bark_enabled:
                bark_result = send_bark_message(
                    settings,
                    bark_title_for_alerts(review_candidates),
                    message,
                )
                sinks.append(bark_result)
                delivered_ok = delivered_ok or bark_result.ok
            if settings.bark_friend_enabled and alerts_are_market_signals(review_candidates):
                sinks.append(
                    send_bark_friend_message(
                        settings,
                        bark_title_for_alerts(review_candidates),
                        message,
                    )
                )
            if delivered_ok:
                alerts_marked_sent.extend(review_candidates)
                record_push("intraday_alert", message, at=now_utc.isoformat())
        else:
            alerts_marked_sent.extend(review_candidates)
        return True

    gate = "scope_gate" if should_deliver else "delivery_gate"
    sinks.append(
        SinkResult(
            sink=f"{reviewer_name}_{gate}",
            attempted=True,
            ok=True,
            error=(
                f"{reviewer_name} output mentioned non-focus context"
                if should_deliver
                else f"{reviewer_name} output did not request delivery"
            ),
        )
    )
    alerts_marked_sent.extend(review_candidates)
    return True


def _handle_reviewer_failure(
    *,
    result: SinkResult,
    payload: dict[str, object],
    review_candidates: list[dict[str, object]],
    settings: NotificationSettings,
    runner: CommandRunner,
    now_utc: datetime,
    alerts_marked_sent: list[dict[str, object]],
    sinks: list[SinkResult],
) -> None:
    if deepseek_usage_limited(result.error):
        _mark_noncritical_reviewed_after_model_limit(
            review_candidates,
            alerts_marked_sent=alerts_marked_sent,
            sinks=sinks,
            sink_name=result.sink,
        )
    _failopen_critical_alerts(
        payload,
        review_candidates,
        settings=settings,
        runner=runner,
        now_utc=now_utc,
        alerts_marked_sent=alerts_marked_sent,
        sinks=sinks,
    )


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
    now_utc = now or datetime.now(tz=timezone.utc)
    if (
        settings.openclaw_enabled
        or settings.deepseek_enabled
        or settings.openclaw_agent_enabled
        or settings.codex_enabled
    ):
        digest_result = flush_missed(settings, runner=runner)
        if digest_result is not None:
            sinks.append(digest_result)

    message = format_alert_message(payload, selected)
    bypass_alerts = direct_push_alerts(selected)
    review_candidates = [alert for alert in selected if alert not in bypass_alerts]
    alerts_marked_sent: list[dict[str, object]] = []
    if settings.openclaw_enabled:
        direct_result = send_openclaw_message(settings, message, runner=runner)
        sinks.append(direct_result)
        if not direct_result.ok:
            append_missed(
                settings.missed_queue_path,
                message,
                kind="direct",
                at=now_utc,
            )
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
        if not direct_result.ok:
            append_missed(
                settings.missed_queue_path,
                bypass_message,
                kind="direct",
                at=now_utc,
            )
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

    if review_candidates:
        strong_review_candidates, weak_review_candidates = split_time_sensitive_review_candidates(
            payload,
            review_candidates,
            min_score=settings.review_min_time_sensitive_score,
        )
        if weak_review_candidates:
            alerts_marked_sent.extend(weak_review_candidates)
            sinks.append(
                SinkResult(
                    sink="review_prefilter",
                    attempted=True,
                    ok=True,
                    error="weak/non-time-sensitive alerts marked reviewed without LLM",
                )
            )
        review_candidates = strong_review_candidates

    if settings.deepseek_enabled and review_candidates:
        deepseek_result, deepseek_message = run_deepseek_reviewer(
            settings,
            build_codex_prompt(payload, review_candidates, previous_push=load_previous_push()),
        )
        sinks.append(deepseek_result)
        if deepseek_result.ok:
            _deliver_review_message(
                message=deepseek_message,
                review_candidates=review_candidates,
                settings=settings,
                runner=runner,
                now_utc=now_utc,
                alerts_marked_sent=alerts_marked_sent,
                sinks=sinks,
                reviewer_name="deepseek",
                delivery_kind="deepseek",
                deliver=settings.deepseek_deliver,
            )
            review_candidates = []
        elif not deepseek_result.ok:
            usage_limited = deepseek_usage_limited(deepseek_result.error)
            _handle_reviewer_failure(
                result=deepseek_result,
                payload=payload,
                review_candidates=review_candidates,
                settings=settings,
                runner=runner,
                now_utc=now_utc,
                alerts_marked_sent=alerts_marked_sent,
                sinks=sinks,
            )
            if usage_limited:
                review_candidates = []
    if settings.openclaw_agent_enabled and review_candidates:
        agent_result, agent_message = run_openclaw_agent(
            settings,
            build_codex_prompt(payload, review_candidates, previous_push=load_previous_push()),
            runner=runner,
        )
        sinks.append(agent_result)
        if agent_result.ok:
            _deliver_review_message(
                message=agent_message,
                review_candidates=review_candidates,
                settings=settings,
                runner=runner,
                now_utc=now_utc,
                alerts_marked_sent=alerts_marked_sent,
                sinks=sinks,
                reviewer_name="openclaw_agent",
                delivery_kind="agent",
                deliver=settings.openclaw_agent_deliver,
            )
        else:
            _handle_reviewer_failure(
                result=agent_result,
                payload=payload,
                review_candidates=review_candidates,
                settings=settings,
                runner=runner,
                now_utc=now_utc,
                alerts_marked_sent=alerts_marked_sent,
                sinks=sinks,
            )
    elif settings.codex_enabled and review_candidates:
        codex_result, codex_message = run_codex_exec(
            settings,
            build_codex_prompt(payload, review_candidates, previous_push=load_previous_push()),
            runner=runner,
        )
        sinks.append(codex_result)
        if codex_result.ok and settings.codex_deliver:
            _deliver_review_message(
                message=codex_message,
                review_candidates=review_candidates,
                settings=settings,
                runner=runner,
                now_utc=now_utc,
                alerts_marked_sent=alerts_marked_sent,
                sinks=sinks,
                reviewer_name="codex",
                delivery_kind="codex",
                deliver=settings.codex_deliver,
            )
            review_candidates = []
        elif codex_result.ok:
            _deliver_review_message(
                message=codex_message,
                review_candidates=review_candidates,
                settings=settings,
                runner=runner,
                now_utc=now_utc,
                alerts_marked_sent=alerts_marked_sent,
                sinks=sinks,
                reviewer_name="codex",
                delivery_kind="codex",
                deliver=settings.codex_deliver,
            )
            review_candidates = []
        elif not codex_result.ok:
            _handle_reviewer_failure(
                result=codex_result,
                payload=payload,
                review_candidates=review_candidates,
                settings=settings,
                runner=runner,
                now_utc=now_utc,
                alerts_marked_sent=alerts_marked_sent,
                sinks=sinks,
            )

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
