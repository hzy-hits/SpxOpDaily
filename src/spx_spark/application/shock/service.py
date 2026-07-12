"""Shock monitor CLI orchestration: evaluate → notify → research/telemetry."""

from __future__ import annotations

import argparse
import json
from dataclasses import asdict, replace
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from spx_spark.alert_model import Alert
from spx_spark.application.shock.delivery import (
    _notification_payload,
    event_greek_shadow_due,
    mark_alert_attempts,
    mark_event_greek_shadow_sampled,
    reconcile_acknowledged_alerts,
)
from spx_spark.application.shock.evaluator import rth_session_date, synchronized_live_sample
from spx_spark.application.shock.machine import (
    _strategy_alert,
    advance_monitor_state,
)
from spx_spark.application.shock.models import (
    RECLAIM_KIND,
    SHOCK_KIND,
    IntradayShockSettings,
    load_monitor_state,
)
from spx_spark.config import (
    NotificationSettings,
    StorageSettings,
    resolve_shock_notify_enabled,
)
from spx_spark.data_platform.integration import (
    IntradayResearchResult,
    persist_intraday_evaluation,
    prepare_intraday_evaluation,
    record_notification_result,
    record_outcome_rows,
)
from spx_spark.data_platform.settings import DataPlatformSettings
from spx_spark.greek_shadow import sample_zero_dte_greeks_shadow
from spx_spark.intraday_event_outcomes import (
    IntradayEventOutcomeSettings,
    IntradayEventOutcomeTracker,
    SynchronizedSPXSample,
)
from spx_spark.intraday_strategy import (
    STRATEGY_KINDS,
    IntradayStrategySettings,
    advance_intraday_strategy,
    structure_from_options_map,
    unavailable_structure,
)
from spx_spark.notifier import notify_payload
from spx_spark.notifier.policy import alert_key
from spx_spark.notifier.state import load_acknowledged_event_ids
from spx_spark.options_map import build_options_map
from spx_spark.settings import DEFAULT_ALERT_SETTINGS
from spx_spark.state_io import atomic_write_json_secure, exclusive_state_lock
from spx_spark.storage import LatestStateStore
from spx_spark.strategy.steven import (
    annotate_alerts_with_steven_context,
    load_steven_state_for_alerts,
)

def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run the lightweight SPX/ES shock monitor.")
    parser.add_argument("--json", action="store_true", help="Print JSON output.")
    parser.add_argument("--no-notify", action="store_true", help="Never send notifications.")
    return parser.parse_args(argv)


def run(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    settings = IntradayShockSettings.from_env()
    strategy_settings = IntradayStrategySettings.from_env()
    storage_settings = StorageSettings.from_env()
    data_platform_settings: DataPlatformSettings | None = None
    data_platform_config_error: str | None = None
    try:
        data_platform_settings = DataPlatformSettings.from_env()
    except Exception as exc:  # Optional research configuration is always fail-open.
        data_platform_config_error = f"{type(exc).__name__}:{exc}"
    latest = LatestStateStore(storage_settings).load()
    session_date = rth_session_date(latest.as_of)
    payload: dict[str, Any] = {
        "created_at": datetime.now(tz=timezone.utc).isoformat(),
        "as_of": latest.as_of.isoformat(),
        "alert_count": 0,
        "alerts": [],
    }
    if session_date is None:
        payload["skipped_reason"] = "outside_spx_rth"
    else:
        sample, sample_error = synchronized_live_sample(latest, settings)
        if sample is None:
            payload["skipped_reason"] = sample_error
        else:
            options_map = None
            option_structure_error: str | None = None
            try:
                options_map = build_options_map(latest)
                structure = structure_from_options_map(
                    options_map,
                    session_date=session_date,
                    observed_at=sample.at,
                    state=latest,
                )
            except Exception as exc:  # Price alerts must survive option-map failures.
                option_structure_error = f"{type(exc).__name__}:{exc}"
                structure = unavailable_structure(
                    observed_at=sample.at,
                    reason="option_structure_build_error",
                )
            state_path = Path(settings.state_path)
            notify_settings = replace(
                NotificationSettings.from_env(),
                direct_push_llm_enabled=False,
            )
            # Shock stays on the latency path by default; gated by the same
            # notification.enabled / --no-notify controls plus
            # shock_direct_delivery_enabled (independent of alert_engine
            # direct_delivery / outbox ownership).
            notify_enabled = resolve_shock_notify_enabled(
                no_notify=args.no_notify,
                settings=notify_settings,
            )
            with exclusive_state_lock(state_path):
                monitor_state = load_monitor_state(settings.state_path, session_date=session_date)
                monitor_state, price_alerts = advance_monitor_state(monitor_state, sample, settings)
                monitor_state, path_decision, strategy_signals = advance_intraday_strategy(
                    monitor_state,
                    sample,
                    structure,
                    strategy_settings,
                )
                alerts = [
                    *price_alerts,
                    *(
                        _strategy_alert(row, provider=sample.provider)
                        for row in strategy_signals
                    ),
                ]
                raw_price_alerts = tuple(price_alerts)
                if alerts and notify_enabled:
                    monitor_state, alerts = reconcile_acknowledged_alerts(
                        monitor_state,
                        alerts,
                        acknowledged_event_ids=set(
                            load_acknowledged_event_ids(notify_settings.state_path)
                        ),
                        at=sample.at,
                    )
                if alerts and notify_enabled:
                    monitor_state = mark_alert_attempts(
                        monitor_state,
                        alerts,
                        at=sample.at,
                        delivered=False,
                    )
                atomic_write_json_secure(state_path, monitor_state)

            if alerts and DEFAULT_ALERT_SETTINGS.steven_alert_context_enabled:
                try:
                    steven_state = load_steven_state_for_alerts(
                        StorageSettings.from_env().data_root
                    )
                    alerts = annotate_alerts_with_steven_context(
                        alerts,
                        steven_state,
                        as_of=sample.at,
                    )
                except Exception:  # noqa: BLE001 — never block shock alerts
                    pass

            prepared_research = None
            research_result = None
            research_error = data_platform_config_error
            if data_platform_settings is not None and data_platform_settings.enabled:
                try:
                    # Pure ID/record preparation only. No research I/O occurs
                    # before the latency-critical notification attempt.
                    prepared_research = prepare_intraday_evaluation(
                        session_date=session_date,
                        source_at=sample.at,
                        available_at=latest.as_of,
                        spx=sample.spx,
                        es=sample.es,
                        spx_source_at=sample.spx_source_at or sample.at,
                        es_source_at=sample.es_source_at or sample.at,
                        structure=asdict(structure),
                        path_decision=path_decision.to_dict(),
                        alerts=tuple(alert.to_dict() for alert in alerts),
                        strategy_config=asdict(strategy_settings),
                        settings=data_platform_settings,
                    )
                    research_result = prepared_research.result
                except Exception as exc:  # Research preparation must never suppress an alert.
                    research_error = f"{type(exc).__name__}:{exc}"
            elif data_platform_settings is not None:
                research_result = IntradayResearchResult(status="disabled")

            payload = _notification_payload(latest, monitor_state, alerts)
            payload["intraday_path"] = path_decision.to_dict()
            research_link_by_alert: dict[tuple[str, str], object] = {}
            if research_result is not None:
                alert_rows = payload.get("alerts")
                if isinstance(alert_rows, list):
                    for alert, row, link in zip(
                        alerts,
                        alert_rows,
                        research_result.alert_links,
                        strict=False,
                    ):
                        if isinstance(row, dict):
                            row["source_at"] = link.source_at.isoformat()
                            row["event_key"] = link.event_key
                            row["decision_id"] = link.decision_id
                        research_link_by_alert[(alert.kind, str(alert.event_id or ""))] = link
                payload["data_platform"] = {
                    "status": research_result.status,
                    "evaluation_event_key": research_result.evaluation_event_key,
                    "evaluation_decision_id": research_result.evaluation_decision_id,
                    "alert_link_count": len(research_result.alert_links),
                    "errors": list(research_result.errors),
                }
            elif research_error is not None:
                payload["data_platform"] = {"status": "error", "error": research_error}
            if option_structure_error is not None:
                payload["option_structure_error"] = option_structure_error

            # Delivery stays on the latency-critical path. Outcome and Greeks
            # telemetry run only after the deterministic alert attempt. Outbox
            # owns periodic alert_engine candidates; shock remains direct-push
            # unless shock_direct_delivery_enabled is flipped off.
            notification_result = None
            if alerts and notify_enabled:
                result = notify_payload(
                    payload,
                    settings=notify_settings,
                    now=sample.at,
                    record_telemetry=False,
                )
                notification_result = result
                payload["notification"] = result.to_dict()
                acknowledged = set(result.acknowledged_event_ids)
                delivered_alerts = [
                    alert
                    for alert in alerts
                    if alert.dedup_group is not None
                    and str(alert.dedup_group) in acknowledged
                ]
                if delivered_alerts:
                    with exclusive_state_lock(state_path):
                        latest_monitor_state = load_monitor_state(
                            settings.state_path,
                            session_date=session_date,
                        )
                        latest_monitor_state = mark_alert_attempts(
                            latest_monitor_state,
                            delivered_alerts,
                            at=sample.at,
                            delivered=True,
                        )
                        atomic_write_json_secure(state_path, latest_monitor_state)

            # Research persistence is deliberately after notification and its
            # durable delivery acknowledgement. It may spool, but cannot add
            # latency to the user-visible alert.
            if prepared_research is not None and data_platform_settings is not None:
                try:
                    research_result = persist_intraday_evaluation(
                        prepared_research,
                        settings=data_platform_settings,
                    )
                    payload["data_platform"] = {
                        "status": research_result.status,
                        "evaluation_event_key": research_result.evaluation_event_key,
                        "evaluation_decision_id": research_result.evaluation_decision_id,
                        "alert_link_count": len(research_result.alert_links),
                        "errors": list(research_result.errors),
                    }
                    if notification_result is not None:
                        selected_keys = set(notification_result.selected_alert_keys)
                        alert_rows = payload.get("alerts")
                        selected_rows = tuple(
                            row
                            for row in alert_rows
                            if isinstance(row, dict)
                            and str(row.get("decision_id") or alert_key(row)) in selected_keys
                        ) if isinstance(alert_rows, list) else ()
                        record_notification_result(
                            payload=payload,
                            selected_alerts=selected_rows,
                            notification=notification_result.to_dict(),
                            attempted_at=sample.at,
                            settings=data_platform_settings,
                        )
                except Exception as exc:  # Research storage must never suppress an alert.
                    payload["data_platform"] = {
                        "status": "error",
                        "error": f"{type(exc).__name__}:{exc}",
                    }

            outcome_summary: dict[str, object] = {"status": "ok", "records_emitted": 0}
            try:
                tracker = IntradayEventOutcomeTracker(IntradayEventOutcomeSettings.from_env())
                outcome_sample = SynchronizedSPXSample(
                    spx=sample.spx,
                    spx_source_at=sample.spx_source_at or sample.at,
                    es_source_at=sample.es_source_at or sample.at,
                )
                outcome_alerts = (
                    *raw_price_alerts,
                    *(alert for alert in alerts if alert.kind in STRATEGY_KINDS),
                )
                active_event = monitor_state.get("active_event")
                market_direction = (
                    str(active_event.get("direction"))
                    if isinstance(active_event, dict)
                    else "down"
                )
                for alert in outcome_alerts:
                    if not alert.event_id:
                        continue
                    if alert.kind == SHOCK_KIND:
                        phase = "shock"
                        direction = market_direction
                    elif alert.kind == RECLAIM_KIND:
                        phase = "reclaim"
                        direction = market_direction
                    else:
                        phase = "strategy"
                        direction = "up"
                    research_link = research_link_by_alert.get(
                        (alert.kind, str(alert.event_id))
                    )
                    if direction in {"up", "down"}:
                        tracker.observe_event(
                            event_id=str(alert.event_id),
                            phase=phase,
                            direction=direction,
                            sample=outcome_sample,
                            event_key=getattr(research_link, "event_key", None),
                            decision_id=getattr(research_link, "decision_id", None),
                        )
                emitted = tracker.observe_sample(outcome_sample)
                outcome_summary["records_emitted"] = len(emitted)
                if data_platform_settings is not None:
                    outcome_summary["ledger_records"] = len(
                        record_outcome_rows(
                            emitted,
                            settings=data_platform_settings,
                        )
                    )
            except Exception as exc:  # Outcome telemetry must never suppress an alert.
                outcome_summary = {
                    "status": "error",
                    "error": f"{type(exc).__name__}:{exc}",
                }
            payload["outcome_tracking"] = outcome_summary

            greek_event_results: list[dict[str, object]] = []
            sampled_alerts: list[Alert] = []
            for alert in raw_price_alerts:
                if not event_greek_shadow_due(monitor_state, alert):
                    continue
                trigger_kind = "shock" if alert.kind == SHOCK_KIND else "reclaim"
                research_link = research_link_by_alert.get(
                    (alert.kind, str(alert.event_id or ""))
                )
                result = sample_zero_dte_greeks_shadow(
                    latest,
                    data_root=storage_settings.data_root,
                    trigger_kind=trigger_kind,
                    event_id=str(alert.event_id),
                    event_at=sample.at,
                    trigger_metadata={
                        "direction": (
                            str(monitor_state.get("active_event", {}).get("direction"))
                            if isinstance(monitor_state.get("active_event"), dict)
                            else None
                        ),
                        "spx": sample.spx,
                        "es": sample.es,
                        "event_key": getattr(research_link, "event_key", None),
                    },
                    options_map=options_map,
                )
                greek_event_results.append(result.to_dict())
                if result.status != "error":
                    sampled_alerts.append(alert)
            if greek_event_results:
                payload["greek_shadow_events"] = greek_event_results
            if sampled_alerts:
                with exclusive_state_lock(state_path):
                    latest_monitor_state = load_monitor_state(
                        settings.state_path,
                        session_date=session_date,
                    )
                    latest_monitor_state = mark_event_greek_shadow_sampled(
                        latest_monitor_state,
                        sampled_alerts,
                        at=sample.at,
                    )
                    atomic_write_json_secure(state_path, latest_monitor_state)

    if args.json:
        print(json.dumps(payload, ensure_ascii=False, sort_keys=True))
    elif payload.get("skipped_reason"):
        print(f"Intraday shock monitor skipped: {payload['skipped_reason']}")
    else:
        print(f"Intraday shock alerts: {payload['alert_count']}")
    return 0


def main() -> None:
    raise SystemExit(run())
