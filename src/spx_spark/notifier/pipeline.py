from __future__ import annotations

from dataclasses import replace
from datetime import datetime, timezone

from spx_spark.config import NotificationSettings
from spx_spark.notifier.deepseek import deepseek_usage_limited, run_deepseek_reviewer
from spx_spark.notifier.llm_writer import call_llm_writer, load_previous_push, record_push
from spx_spark.notifier.missed_queue import append_missed, flush_missed
from spx_spark.notifier.model import CommandRunner, NotificationResult, SinkResult, default_runner
from spx_spark.notifier.policy import (
    alert_key,
    alerts_are_market_signals,
    codex_message_delivery_verdict,
    codex_message_respects_human_scope,
    direct_push_alerts,
    split_time_sensitive_review_candidates,
)
from spx_spark.notifier.prompts import (
    build_codex_prompt,
    build_direct_push_prompt,
    format_alert_message,
)
from spx_spark.notifier.format_push import push_lane_for_alerts
from spx_spark.notifier.review_audit import append_review_audit
from spx_spark.notifier.sinks import (
    any_delivery_ok,
    bark_title_for_alerts,
    deliver_trade_push,
    im_delivery_ok,
    run_codex_exec,
    run_openclaw_agent,
)
from spx_spark.notifier.state import (
    load_sent_state,
    mark_alerts_sent,
    recent_intraday_shock_blocks_price_move,
    select_alerts_for_notification,
)


REVIEW_FAILURE_HIGH_FAILOPEN_KINDS = frozenset(
    {
        "price_move_from_close",
        "intraday_price_shock",
        "intraday_price_reclaim",
        "flip_reclaim_call",
        "call_wall_breakout_call",
    }
)


def _telemetry_alert_key(alert: dict[str, object]) -> str:
    return str(alert.get("decision_id") or alert_key(alert))


def _scope_sink(
    sink: SinkResult,
    alerts: list[dict[str, object]],
    *,
    verdict: str | None = None,
) -> SinkResult:
    return replace(
        sink,
        alert_keys=tuple(dict.fromkeys(_telemetry_alert_key(alert) for alert in alerts)),
        verdict=verdict or sink.verdict,
    )


def _scope_sinks(
    sinks: list[SinkResult],
    alerts: list[dict[str, object]],
    *,
    verdict: str | None = None,
) -> list[SinkResult]:
    return [_scope_sink(sink, alerts, verdict=verdict) for sink in sinks]


def _filter_recent_shock_correlations(
    alerts: list[dict[str, object]],
    *,
    settings: NotificationSettings,
    now_utc: datetime,
    alerts_marked_sent: list[dict[str, object]],
    sinks: list[SinkResult],
) -> tuple[list[dict[str, object]], list[dict[str, object]]]:
    """Recheck correlation immediately before delivery, after any LLM wait."""

    sent_at_by_key = load_sent_state(settings.state_path)
    for marked in alerts_marked_sent:
        if str(marked.get("kind") or "") == "intraday_price_shock":
            sent_at_by_key[alert_key(marked)] = now_utc.timestamp()
    kept: list[dict[str, object]] = []
    suppressed: list[dict[str, object]] = []
    for alert in alerts:
        if recent_intraday_shock_blocks_price_move(
            alert,
            sent_at_by_key,
            now_ts=now_utc.timestamp(),
        ):
            suppressed.append(alert)
        else:
            kept.append(alert)
    if suppressed:
        alerts_marked_sent.extend(suppressed)
        sinks.append(
            _scope_sink(
                SinkResult(
                sink="intraday_shock_correlation_gate",
                attempted=True,
                ok=True,
                error="fixed-cycle price move suppressed after same-direction realtime shock",
                ),
                suppressed,
                verdict="suppressed",
            )
        )
    return kept, suppressed


def _record_delivered_event_ids(
    alerts: list[dict[str, object]],
    acknowledged_event_ids: set[str],
) -> None:
    for alert in alerts:
        if alert.get("event_id"):
            acknowledged_event_ids.add(str(alert["event_id"]))
        # Shock and reclaim intentionally share one event id. Keep a
        # phase-specific acknowledgement so the monitor can recover if the
        # process exits between committing notifier state and monitor state.
        if str(alert.get("kind") or "") in {
            "intraday_price_shock",
            "intraday_price_reclaim",
            "flip_reclaim_call",
            "call_wall_breakout_call",
        } and alert.get("dedup_group"):
            acknowledged_event_ids.add(str(alert["dedup_group"]))


def _failopen_time_sensitive_alerts(
    payload: dict[str, object],
    review_candidates: list[dict[str, object]],
    *,
    settings: NotificationSettings,
    runner: CommandRunner,
    now_utc: datetime,
    alerts_marked_sent: list[dict[str, object]],
    acknowledged_event_ids: set[str],
    sinks: list[SinkResult],
) -> tuple[list[dict[str, object]], list[SinkResult], list[dict[str, object]]]:
    failopen_alerts = [
        alert
        for alert in review_candidates
        if str(alert.get("severity", "")).lower() == "critical"
        or (
            str(alert.get("severity", "")).lower() == "high"
            and str(alert.get("kind") or "") in REVIEW_FAILURE_HIGH_FAILOPEN_KINDS
        )
    ]
    failopen_alerts, suppressed = _filter_recent_shock_correlations(
        failopen_alerts,
        settings=settings,
        now_utc=now_utc,
        alerts_marked_sent=alerts_marked_sent,
        sinks=sinks,
    )
    if not failopen_alerts:
        return suppressed, [], suppressed
    handled_alerts = [*suppressed, *failopen_alerts]
    failopen_message = format_alert_message(payload, failopen_alerts)
    lane = push_lane_for_alerts(failopen_alerts)
    delivery_sinks = _scope_sinks(
        deliver_trade_push(
            settings,
            title=bark_title_for_alerts(failopen_alerts),
            text=failopen_message,
            kind="direct_event",
            lane=lane,
            friend=False,
            runner=runner,
        ),
        failopen_alerts,
    )
    sinks.extend(delivery_sinks)
    if not im_delivery_ok(delivery_sinks):
        append_missed(
            settings.missed_queue_path,
            failopen_message,
            kind="failopen",
            at=now_utc,
        )
    if any_delivery_ok(delivery_sinks):
        alerts_marked_sent.extend(failopen_alerts)
        _record_delivered_event_ids(failopen_alerts, acknowledged_event_ids)
        return handled_alerts, delivery_sinks, suppressed
    return suppressed, delivery_sinks, suppressed


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
        if str(alert.get("severity", "")).lower() not in {"critical", "high"}
    ]
    if not noncritical:
        return
    alerts_marked_sent.extend(noncritical)
    sinks.append(
        _scope_sink(
            SinkResult(
            sink=f"{sink_name}_rate_limit_cooldown",
            attempted=True,
            ok=True,
            error="model rate/usage limit; low-priority alerts marked reviewed",
            ),
            noncritical,
            verdict="reviewed",
        )
    )


def _deliver_review_message(
    *,
    payload: dict[str, object],
    message: str,
    review_candidates: list[dict[str, object]],
    settings: NotificationSettings,
    runner: CommandRunner,
    now_utc: datetime,
    alerts_marked_sent: list[dict[str, object]],
    acknowledged_event_ids: set[str],
    sinks: list[SinkResult],
    reviewer_sink: SinkResult,
    reviewer_name: str,
    delivery_kind: str,
    deliver: bool,
) -> list[dict[str, object]]:
    parser_verdict = codex_message_delivery_verdict(message)
    should_deliver = parser_verdict == "deliver" if settings.codex_require_delivery_cue else True
    scope_ok = codex_message_respects_human_scope(message)
    delivery_sinks: list[SinkResult] = []

    if settings.codex_require_delivery_cue and parser_verdict == "invalid":
        parser_sink = SinkResult(
            sink=f"{reviewer_name}_parser_gate",
            attempted=True,
            ok=False,
            error=f"{reviewer_name} output has no explicit first-line delivery cue",
        )
        parser_sink = _scope_sink(parser_sink, review_candidates, verdict="blocked")
        sinks.append(parser_sink)
        delivered_failopen, delivery_sinks, suppressed = _failopen_time_sensitive_alerts(
            payload,
            review_candidates,
            settings=settings,
            runner=runner,
            now_utc=now_utc,
            alerts_marked_sent=alerts_marked_sent,
            acknowledged_event_ids=acknowledged_event_ids,
            sinks=sinks,
        )
        remaining = [alert for alert in review_candidates if alert not in delivered_failopen]
        append_review_audit(
            settings,
            at=now_utc,
            reviewer=reviewer_name,
            candidates=review_candidates,
            raw_reply=message,
            parser_verdict=parser_verdict,
            scope_ok=scope_ok,
            outcome=(
                "invalid_parser_pending"
                if remaining
                else "invalid_parser_correlated_suppressed"
                if suppressed and not any_delivery_ok(delivery_sinks)
                else "invalid_parser_failopen_delivered"
            ),
            reviewer_sink=reviewer_sink,
            delivery_sinks=delivery_sinks,
            error=parser_sink.error,
            details={"pending_count": len(remaining), "suppressed_count": len(suppressed)},
        )
        return remaining

    if should_deliver and scope_ok:
        suppressed: list[dict[str, object]] = []
        if deliver:
            delivery_candidates, suppressed = _filter_recent_shock_correlations(
                review_candidates,
                settings=settings,
                now_utc=now_utc,
                alerts_marked_sent=alerts_marked_sent,
                sinks=sinks,
            )
            if not delivery_candidates:
                append_review_audit(
                    settings,
                    at=now_utc,
                    reviewer=reviewer_name,
                    candidates=review_candidates,
                    raw_reply=message,
                    parser_verdict=parser_verdict,
                    scope_ok=scope_ok,
                    outcome="correlated_shock_suppressed",
                    reviewer_sink=reviewer_sink,
                    details={"suppressed_count": len(suppressed)},
                )
                return []
            if suppressed:
                message = format_alert_message(payload, delivery_candidates)
            lane = push_lane_for_alerts(delivery_candidates)
            friend = lane == "trade" and alerts_are_market_signals(delivery_candidates)
            delivery_sinks = _scope_sinks(
                deliver_trade_push(
                    settings,
                    title=bark_title_for_alerts(delivery_candidates),
                    text=message,
                    kind="intraday_alert",
                    lane=lane,
                    friend=friend,
                    runner=runner,
                ),
                delivery_candidates,
            )
            sinks.extend(delivery_sinks)
            if not im_delivery_ok(delivery_sinks):
                append_missed(
                    settings.missed_queue_path,
                    message,
                    kind=delivery_kind,
                    at=now_utc,
                )
            if any_delivery_ok(delivery_sinks):
                alerts_marked_sent.extend(delivery_candidates)
                _record_delivered_event_ids(delivery_candidates, acknowledged_event_ids)
                record_push("intraday_alert", message, at=now_utc.isoformat())
                outcome = "delivered"
                remaining: list[dict[str, object]] = []
            else:
                outcome = "delivery_failed_pending"
                remaining = list(delivery_candidates)
        else:
            alerts_marked_sent.extend(review_candidates)
            outcome = "delivery_disabled_reviewed"
            remaining = []
        append_review_audit(
            settings,
            at=now_utc,
            reviewer=reviewer_name,
            candidates=review_candidates,
            raw_reply=message,
            parser_verdict=parser_verdict,
            scope_ok=scope_ok,
            outcome=outcome,
            reviewer_sink=reviewer_sink,
            delivery_sinks=delivery_sinks,
            details={"suppressed_count": len(suppressed)} if deliver and suppressed else None,
        )
        return remaining

    if should_deliver:
        scope_sink = SinkResult(
            sink=f"{reviewer_name}_scope_gate",
            attempted=True,
            ok=False,
            error=f"{reviewer_name} output mentioned non-focus context",
        )
        scope_sink = _scope_sink(scope_sink, review_candidates, verdict="blocked")
        sinks.append(scope_sink)
        delivered_failopen, delivery_sinks, suppressed = _failopen_time_sensitive_alerts(
            payload,
            review_candidates,
            settings=settings,
            runner=runner,
            now_utc=now_utc,
            alerts_marked_sent=alerts_marked_sent,
            acknowledged_event_ids=acknowledged_event_ids,
            sinks=sinks,
        )
        remaining = [alert for alert in review_candidates if alert not in delivered_failopen]
        append_review_audit(
            settings,
            at=now_utc,
            reviewer=reviewer_name,
            candidates=review_candidates,
            raw_reply=message,
            parser_verdict=parser_verdict,
            scope_ok=scope_ok,
            outcome=(
                "scope_blocked_pending"
                if remaining
                else "scope_blocked_correlated_suppressed"
                if suppressed and not any_delivery_ok(delivery_sinks)
                else "scope_blocked_failopen_delivered"
            ),
            reviewer_sink=reviewer_sink,
            delivery_sinks=delivery_sinks,
            error=scope_sink.error,
            details={"pending_count": len(remaining), "suppressed_count": len(suppressed)},
        )
        return remaining

    veto_sink = SinkResult(
        sink=f"{reviewer_name}_delivery_gate",
        attempted=True,
        ok=True,
        error=f"{reviewer_name} output explicitly vetoed delivery",
    )
    veto_sink = _scope_sink(veto_sink, review_candidates, verdict="vetoed")
    sinks.append(veto_sink)
    alerts_marked_sent.extend(review_candidates)
    append_review_audit(
        settings,
        at=now_utc,
        reviewer=reviewer_name,
        candidates=review_candidates,
        raw_reply=message,
        parser_verdict=parser_verdict,
        scope_ok=scope_ok,
        outcome="vetoed",
        reviewer_sink=reviewer_sink,
        error=veto_sink.error,
    )
    return []


def _handle_reviewer_failure(
    *,
    result: SinkResult,
    payload: dict[str, object],
    review_candidates: list[dict[str, object]],
    settings: NotificationSettings,
    runner: CommandRunner,
    now_utc: datetime,
    alerts_marked_sent: list[dict[str, object]],
    acknowledged_event_ids: set[str],
    sinks: list[SinkResult],
) -> list[dict[str, object]]:
    if deepseek_usage_limited(result.error):
        _mark_noncritical_reviewed_after_model_limit(
            review_candidates,
            alerts_marked_sent=alerts_marked_sent,
            sinks=sinks,
            sink_name=result.sink,
        )
    delivered_failopen, failopen_sinks, suppressed = _failopen_time_sensitive_alerts(
        payload,
        review_candidates,
        settings=settings,
        runner=runner,
        now_utc=now_utc,
        alerts_marked_sent=alerts_marked_sent,
        acknowledged_event_ids=acknowledged_event_ids,
        sinks=sinks,
    )
    low_priority_consumed = [
        alert
        for alert in review_candidates
        if deepseek_usage_limited(result.error)
        and str(alert.get("severity", "")).lower() not in {"critical", "high"}
    ]
    remaining = [
        alert
        for alert in review_candidates
        if alert not in delivered_failopen and alert not in low_priority_consumed
    ]
    append_review_audit(
        settings,
        at=now_utc,
        reviewer=result.sink,
        candidates=review_candidates,
        raw_reply="",
        parser_verdict="not_run",
        scope_ok=None,
        outcome=(
            "review_failed_pending"
            if remaining
            else "review_failed_correlated_suppressed"
            if suppressed and not any_delivery_ok(failopen_sinks)
            else "review_failed_failopen_delivered"
        ),
        reviewer_sink=result,
        delivery_sinks=failopen_sinks,
        error=result.error,
        details={
            "failopen_delivered_count": len(delivered_failopen) - len(suppressed),
            "suppressed_count": len(suppressed),
            "pending_count": len(remaining),
        },
    )
    return remaining


def notify_payload(
    payload: dict[str, object],
    *,
    settings: NotificationSettings | None = None,
    runner: CommandRunner = default_runner,
    now: datetime | None = None,
    record_telemetry: bool = True,
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
        or settings.feishu_enabled
        or settings.bark_enabled
        or settings.deepseek_enabled
        or settings.openclaw_agent_enabled
        or settings.codex_enabled
    ):
        digest_result = flush_missed(settings, runner=runner)
        if digest_result is not None:
            sinks.append(replace(digest_result, alert_keys=("__missed_digest__",)))

    message = format_alert_message(payload, selected)
    bypass_alerts = direct_push_alerts(selected, payload)
    review_candidates = [alert for alert in selected if alert not in bypass_alerts]
    review_attempted = False
    alerts_marked_sent: list[dict[str, object]] = []
    acknowledged_event_ids: set[str] = set()
    if settings.openclaw_enabled and not (
        settings.feishu_enabled or settings.bark_enabled or settings.deepseek_enabled
    ):
        # Legacy: OpenClaw-only mode still dumps the raw template without review.
        delivery_sinks = _scope_sinks(
            deliver_trade_push(
                settings,
                title=bark_title_for_alerts(selected),
                text=message,
                kind="direct_event",
                lane=push_lane_for_alerts(selected),
                friend=False,
                runner=runner,
            ),
            selected,
        )
        sinks.extend(delivery_sinks)
        if not im_delivery_ok(delivery_sinks):
            append_missed(
                settings.missed_queue_path,
                message,
                kind="direct",
                at=now_utc,
            )
        if any_delivery_ok(delivery_sinks):
            alerts_marked_sent = list(selected)
            _record_delivered_event_ids(selected, acknowledged_event_ids)
        append_review_audit(
            settings,
            at=now_utc,
            reviewer="direct_policy",
            candidates=selected,
            raw_reply=message,
            parser_verdict="not_run",
            scope_ok=None,
            outcome="delivered" if any_delivery_ok(delivery_sinks) else "delivery_failed_pending",
            delivery_sinks=delivery_sinks,
        )
        review_candidates = []
    elif bypass_alerts:
        bypass_message = format_alert_message(payload, bypass_alerts)
        if settings.direct_push_llm_enabled:
            # Writer, not reviewer: the push decision is already made. Any
            # failure falls back to the raw template so events are never lost.
            written, _writer_error = call_llm_writer(
                build_direct_push_prompt(payload, bypass_alerts)
            )
            if written:
                bypass_message = written
        lane = push_lane_for_alerts(bypass_alerts)
        friend = lane == "trade" and alerts_are_market_signals(bypass_alerts)
        delivery_sinks = _scope_sinks(
            deliver_trade_push(
                settings,
                title=bark_title_for_alerts(bypass_alerts),
                text=bypass_message,
                kind="direct_event",
                lane=lane,
                friend=friend,
                runner=runner,
            ),
            bypass_alerts,
        )
        sinks.extend(delivery_sinks)
        if not im_delivery_ok(delivery_sinks):
            append_missed(
                settings.missed_queue_path,
                bypass_message,
                kind="direct",
                at=now_utc,
            )
        if any_delivery_ok(delivery_sinks):
            alerts_marked_sent = list(bypass_alerts)
            _record_delivered_event_ids(bypass_alerts, acknowledged_event_ids)
            record_push("direct_event", bypass_message, at=now_utc.isoformat())
        append_review_audit(
            settings,
            at=now_utc,
            reviewer="direct_policy",
            candidates=bypass_alerts,
            raw_reply=bypass_message,
            parser_verdict="not_run",
            scope_ok=None,
            outcome="delivered" if any_delivery_ok(delivery_sinks) else "delivery_failed_pending",
            delivery_sinks=delivery_sinks,
        )

    if review_candidates:
        strong_review_candidates, weak_review_candidates = split_time_sensitive_review_candidates(
            payload,
            review_candidates,
            min_score=settings.review_min_time_sensitive_score,
        )
        if weak_review_candidates:
            alerts_marked_sent.extend(weak_review_candidates)
            prefilter_sink = SinkResult(
                sink="review_prefilter",
                attempted=True,
                ok=True,
                error="weak/non-time-sensitive alerts marked reviewed without LLM",
            )
            prefilter_sink = _scope_sink(
                prefilter_sink,
                weak_review_candidates,
                verdict="reviewed",
            )
            sinks.append(prefilter_sink)
            append_review_audit(
                settings,
                at=now_utc,
                reviewer="review_prefilter",
                candidates=weak_review_candidates,
                raw_reply="",
                parser_verdict="not_run",
                scope_ok=None,
                outcome="prefiltered_reviewed",
                reviewer_sink=prefilter_sink,
            )
        review_candidates = strong_review_candidates

    if settings.deepseek_enabled and review_candidates:
        review_attempted = True
        deepseek_result, deepseek_message = run_deepseek_reviewer(
            settings,
            build_codex_prompt(payload, review_candidates, previous_push=load_previous_push()),
        )
        deepseek_result = _scope_sink(
            deepseek_result,
            review_candidates,
            verdict="reviewed" if deepseek_result.ok else "failed",
        )
        sinks.append(deepseek_result)
        if deepseek_result.ok:
            review_candidates = _deliver_review_message(
                payload=payload,
                message=deepseek_message,
                review_candidates=review_candidates,
                settings=settings,
                runner=runner,
                now_utc=now_utc,
                alerts_marked_sent=alerts_marked_sent,
                acknowledged_event_ids=acknowledged_event_ids,
                sinks=sinks,
                reviewer_sink=deepseek_result,
                reviewer_name="deepseek",
                delivery_kind="deepseek",
                deliver=settings.deepseek_deliver,
            )
        elif not deepseek_result.ok:
            review_candidates = _handle_reviewer_failure(
                result=deepseek_result,
                payload=payload,
                review_candidates=review_candidates,
                settings=settings,
                runner=runner,
                now_utc=now_utc,
                alerts_marked_sent=alerts_marked_sent,
                acknowledged_event_ids=acknowledged_event_ids,
                sinks=sinks,
            )
    if settings.openclaw_agent_enabled and review_candidates:
        review_attempted = True
        agent_result, agent_message = run_openclaw_agent(
            settings,
            build_codex_prompt(payload, review_candidates, previous_push=load_previous_push()),
            runner=runner,
        )
        agent_result = _scope_sink(
            agent_result,
            review_candidates,
            verdict="reviewed" if agent_result.ok else "failed",
        )
        sinks.append(agent_result)
        if agent_result.ok:
            review_candidates = _deliver_review_message(
                payload=payload,
                message=agent_message,
                review_candidates=review_candidates,
                settings=settings,
                runner=runner,
                now_utc=now_utc,
                alerts_marked_sent=alerts_marked_sent,
                acknowledged_event_ids=acknowledged_event_ids,
                sinks=sinks,
                reviewer_sink=agent_result,
                reviewer_name="openclaw_agent",
                delivery_kind="agent",
                deliver=settings.openclaw_agent_deliver,
            )
        else:
            review_candidates = _handle_reviewer_failure(
                result=agent_result,
                payload=payload,
                review_candidates=review_candidates,
                settings=settings,
                runner=runner,
                now_utc=now_utc,
                alerts_marked_sent=alerts_marked_sent,
                acknowledged_event_ids=acknowledged_event_ids,
                sinks=sinks,
            )
    if settings.codex_enabled and review_candidates:
        review_attempted = True
        codex_result, codex_message = run_codex_exec(
            settings,
            build_codex_prompt(payload, review_candidates, previous_push=load_previous_push()),
            runner=runner,
        )
        codex_result = _scope_sink(
            codex_result,
            review_candidates,
            verdict="reviewed" if codex_result.ok else "failed",
        )
        sinks.append(codex_result)
        if codex_result.ok and settings.codex_deliver:
            review_candidates = _deliver_review_message(
                payload=payload,
                message=codex_message,
                review_candidates=review_candidates,
                settings=settings,
                runner=runner,
                now_utc=now_utc,
                alerts_marked_sent=alerts_marked_sent,
                acknowledged_event_ids=acknowledged_event_ids,
                sinks=sinks,
                reviewer_sink=codex_result,
                reviewer_name="codex",
                delivery_kind="codex",
                deliver=settings.codex_deliver,
            )
        elif codex_result.ok:
            review_candidates = _deliver_review_message(
                payload=payload,
                message=codex_message,
                review_candidates=review_candidates,
                settings=settings,
                runner=runner,
                now_utc=now_utc,
                alerts_marked_sent=alerts_marked_sent,
                acknowledged_event_ids=acknowledged_event_ids,
                sinks=sinks,
                reviewer_sink=codex_result,
                reviewer_name="codex",
                delivery_kind="codex",
                deliver=settings.codex_deliver,
            )
        elif not codex_result.ok:
            review_candidates = _handle_reviewer_failure(
                result=codex_result,
                payload=payload,
                review_candidates=review_candidates,
                settings=settings,
                runner=runner,
                now_utc=now_utc,
                alerts_marked_sent=alerts_marked_sent,
                acknowledged_event_ids=acknowledged_event_ids,
                sinks=sinks,
            )

    if review_candidates and not review_attempted:
        append_review_audit(
            settings,
            at=now_utc,
            reviewer="review_pipeline",
            candidates=review_candidates,
            raw_reply="",
            parser_verdict="not_run",
            scope_ok=None,
            outcome="pending_no_reviewer_enabled",
            error="no reviewer enabled for time-sensitive candidates",
        )

    sent_count = sum(
        1 for sink in sinks if sink.sink in ("bark", "feishu") and sink.ok
    )
    if alerts_marked_sent or acknowledged_event_ids:
        mark_alerts_sent(
            alerts_marked_sent,
            sent_at_by_key,
            settings,
            now=now,
            acknowledged_event_ids=tuple(sorted(acknowledged_event_ids)),
        )
    skipped_reason = None if sinks else "no_enabled_sinks"
    result = NotificationResult(
        enabled=True,
        selected_count=len(selected),
        sent_count=sent_count,
        skipped_reason=skipped_reason,
        sinks=tuple(sinks),
        acknowledged_event_ids=tuple(sorted(acknowledged_event_ids)),
        selected_alert_keys=tuple(_telemetry_alert_key(alert) for alert in selected),
    )
    try:
        # Import lazily so the realtime notifier never initializes storage or
        # analytical dependencies when the data platform is disabled.
        from spx_spark.data_platform.integration import record_notification_result

        if record_telemetry:
            record_notification_result(
                payload=payload,
                selected_alerts=selected,
                notification=result.to_dict(),
                attempted_at=now_utc,
            )
    except Exception:
        # Notification success is authoritative; research telemetry is
        # explicitly fail-open and must not change delivery behavior.
        pass
    return result
