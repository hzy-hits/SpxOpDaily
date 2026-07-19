"""Shock monitor CLI orchestration: evaluate → notify → research/telemetry."""

from __future__ import annotations

import argparse
import json
import math
import os
from dataclasses import asdict, replace
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Mapping

from spx_spark.alert_model import Alert
from spx_spark.application.order_map.level_decision_shadow import load_level_decision_shadow
from spx_spark.application.shock.delivery import (
    _notification_payload,
    event_greek_shadow_due,
    mark_alert_attempts,
    mark_event_greek_shadow_sampled,
    reconcile_acknowledged_alerts,
)
from spx_spark.application.shock.evaluator import (
    gth_session_date,
    live_es_sample,
    rth_session_date,
    synchronized_live_sample,
)
from spx_spark.application.shock.gth_dip import advance_gth_dip, mark_gth_delivery
from spx_spark.application.shock.level_projection import project_level_decision_machine
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
from spx_spark.ibkr.atm_reference import BASIS_MAX_ABS_POINTS
from spx_spark.macro_event_clock import macro_event_state
from spx_spark.market_calendar import DEFAULT_MARKET_CALENDAR
from spx_spark.intraday_event_outcomes import (
    IntradayEventOutcomeSettings,
    IntradayEventOutcomeTracker,
    SynchronizedSPXSample,
)
from spx_spark.intraday_strategy import (
    STRATEGY_KINDS,
    structure_from_options_map,
    unavailable_structure,
)
from spx_spark.notifier import notify_payload
from spx_spark.notifier.policy import alert_key
from spx_spark.notifier.state import load_acknowledged_event_ids
from spx_spark.options_map import build_options_map
from spx_spark.settings import DEFAULT_ALERT_SETTINGS, load_app_settings
from spx_spark.state_io import (
    atomic_write_json_secure,
    exclusive_state_lock,
    read_json_object,
)
from spx_spark.storage import LatestStateStore
from spx_spark.strategy_contract import policy_version
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
    level_policy = load_app_settings().level_decision
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
        gth_date = gth_session_date(latest.as_of)
        if gth_date is None or not settings.gth_dip_reclaim_enabled:
            payload["skipped_reason"] = "outside_spx_rth_or_gth"
        else:
            payload = _run_gth_dip_reclaim(
                latest,
                settings=settings,
                storage_settings=storage_settings,
                session_date=gth_date,
                no_notify=args.no_notify,
            )
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
                monitor_state, path_decision, strategy_signals = project_level_decision_machine(
                    monitor_state,
                    load_level_decision_shadow(storage_settings),
                    structure,
                    now=sample.at,
                    level_buffer_points=level_policy.break_buffer_points,
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
                        strategy_config=asdict(level_policy),
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


def _run_gth_dip_reclaim(
    latest,
    *,
    settings: IntradayShockSettings,
    storage_settings: StorageSettings,
    session_date: str,
    no_notify: bool,
) -> dict[str, Any]:
    sample, sample_error = live_es_sample(latest, settings)
    payload: dict[str, Any] = {
        "created_at": datetime.now(tz=timezone.utc).isoformat(),
        "as_of": latest.as_of.isoformat(),
        "session_mode": "spx_gth_es_led",
        "alert_count": 0,
        "alerts": [],
    }
    if sample is None:
        payload["skipped_reason"] = sample_error
        return payload
    sample_at, es, provider = sample
    expected_move: float | None = None
    try:
        options_map = build_options_map(latest)
        if options_map.expiries:
            raw_expected_move = options_map.expiries[0].expected_move_points
            if isinstance(raw_expected_move, int | float) and raw_expected_move > 0:
                expected_move = float(raw_expected_move)
    except Exception:  # ES-led detection must survive a missing GTH chain.
        options_map = None
    macro = macro_event_state(sample_at)
    virtual_state = read_json_object(
        Path(storage_settings.data_root) / "latest" / "virtual_strategy_state.json"
    )
    virtual_active = (
        virtual_state.get("active") if isinstance(virtual_state.get("active"), Mapping) else None
    )
    virtual_strategy_blocks_gth = _virtual_strategy_blocks_gth(virtual_active)
    level_shadow = read_json_object(
        Path(storage_settings.data_root) / "latest" / "level_decision_shadow_state.json"
    )
    structure_levels, es_spx_basis = _gth_spread_inputs(
        level_shadow,
        session_date=session_date,
        at=latest.as_of,
        max_age_seconds=settings.gth_structure_max_age_seconds,
    )
    trend_quality = _gth_trend_entry_quality(
        read_json_object(
            Path(storage_settings.data_root) / "latest" / "globex_trend_state.json"
        ),
        session_date=session_date,
        at=sample_at,
        max_age_seconds=settings.gth_structure_max_age_seconds,
    )
    state_path = Path(settings.state_path)
    with exclusive_state_lock(state_path):
        monitor_state = load_monitor_state(settings.state_path, session_date=session_date)
        gth_state, alert, signal = advance_gth_dip(
            monitor_state.get("gth_dip")
            if isinstance(monitor_state.get("gth_dip"), dict)
            else None,
            session_date=session_date,
            at=sample_at,
            es=es,
            provider=provider,
            expected_move_points=expected_move,
            short_horizon_seconds=settings.gth_short_horizon_seconds,
            long_horizon_seconds=settings.gth_long_horizon_seconds,
            short_min_drawdown_points=settings.gth_short_min_drawdown_points,
            long_min_drawdown_points=settings.gth_long_min_drawdown_points,
            short_min_descent_seconds=settings.gth_short_min_descent_seconds,
            long_min_descent_seconds=settings.gth_long_min_descent_seconds,
            expected_move_fraction=settings.gth_expected_move_fraction,
            reclaim_fraction=settings.gth_reclaim_fraction,
            min_reclaim_points=settings.gth_min_reclaim_points,
            confirm_samples=settings.gth_confirm_samples,
            confirm_hold_seconds=settings.gth_confirm_hold_seconds,
            session_warmup_seconds=settings.gth_session_warmup_seconds,
            max_signals_per_session=settings.gth_max_signals_per_session,
            cooldown_seconds=settings.gth_cooldown_seconds,
            delivery_retry_seconds=settings.retry_seconds,
            signal_expiry_seconds=settings.event_expiry_seconds,
            structure_levels=structure_levels,
            es_spx_basis=es_spx_basis,
            spread_min_width_points=settings.gth_spread_min_width_points,
            spread_max_width_points=settings.gth_spread_max_width_points,
            spread_default_width_points=settings.gth_spread_default_width_points,
            exit_clock_et=settings.gth_exit_clock_et,
            entry_quality=trend_quality,
            entry_allowed=(
                macro.get("entry_allowed") is True and not virtual_strategy_blocks_gth
            ),
        )
        monitor_state["gth_dip"] = gth_state
        monitor_state["updated_at"] = sample_at.isoformat()
        atomic_write_json_secure(state_path, monitor_state)
        if signal is not None and not signal.get("delivery_retry"):
            atomic_write_json_secure(
                Path(storage_settings.data_root) / "latest" / "gth_dip_reclaim_signal.json",
                {**signal, "macro_event": macro},
            )
            _append_gth_signal_audit(
                storage_settings,
                sample_at,
                {**signal, "macro_event": macro},
            )

    _append_gth_detector_health(
        storage_settings,
        sample_at,
        {
            "schema_version": 1,
            "policy_version": policy_version("gth_detector_runtime.v3", settings),
            "record_key": sample_at.isoformat(),
            "at": sample_at.isoformat(),
            "session_date": session_date,
            "provider": provider,
            "es": es,
            "detector_status": gth_state.get("status"),
            "entry_allowed": macro.get("entry_allowed") is True
            and not virtual_strategy_blocks_gth,
            "macro_mode": macro.get("mode"),
            "coordinate_kind": "raw_es",
            "instrument_id": "future:ES",
        },
    )

    notify_settings = replace(
        NotificationSettings.from_env(),
        direct_push_llm_enabled=False,
    )
    notify_enabled = resolve_shock_notify_enabled(
        no_notify=no_notify,
        settings=notify_settings,
    )
    alerts = [alert] if alert is not None else []
    acknowledged_ids = set(load_acknowledged_event_ids(notify_settings.state_path))
    if alert is not None and alert.dedup_group in acknowledged_ids:
        alerts = []
        with exclusive_state_lock(state_path):
            monitor_state = load_monitor_state(settings.state_path, session_date=session_date)
            gth = mark_gth_delivery(
                monitor_state.get("gth_dip") if isinstance(monitor_state.get("gth_dip"), dict) else {},
                event_id=str(alert.event_id),
                at=sample_at,
            )
            monitor_state["gth_dip"] = gth
            atomic_write_json_secure(state_path, monitor_state)
    payload = _notification_payload(latest, {"gth_dip": gth_state}, alerts)
    payload.update(
        {
            "session_mode": "spx_gth_es_led",
            "macro_event": macro,
            "expected_move_points": expected_move,
            "gth_dip": gth_state,
            "entry_suppressed_by_active_virtual_strategy": virtual_strategy_blocks_gth,
        }
    )
    if signal is not None and not signal.get("delivery_retry"):
        try:
            greek_result = sample_zero_dte_greeks_shadow(
                latest,
                data_root=storage_settings.data_root,
                trigger_kind="gth_dip_reclaim_call",
                event_id=str(signal["event_id"]),
                event_at=sample_at,
                trigger_metadata={
                    "direction": "up",
                    "es": es,
                    "drawdown_points": signal.get("drawdown_points"),
                    "recovery_fraction": signal.get("recovery_fraction"),
                    "spx_required": False,
                },
                options_map=options_map,
            )
            payload["greek_shadow_event"] = greek_result.to_dict()
        except Exception as exc:
            payload["greek_shadow_event"] = {
                "status": "error",
                "error": f"{type(exc).__name__}:{exc}",
            }
    if alerts and notify_enabled:
        result = notify_payload(
            payload,
            settings=notify_settings,
            now=sample_at,
            record_telemetry=False,
        )
        payload["notification"] = result.to_dict()
        if alert is not None and alert.dedup_group in set(result.acknowledged_event_ids):
            with exclusive_state_lock(state_path):
                monitor_state = load_monitor_state(settings.state_path, session_date=session_date)
                monitor_state["gth_dip"] = mark_gth_delivery(
                    monitor_state.get("gth_dip") if isinstance(monitor_state.get("gth_dip"), dict) else {},
                    event_id=str(alert.event_id),
                    at=sample_at,
                )
                atomic_write_json_secure(state_path, monitor_state)
    return payload


def _gth_spread_inputs(
    level_shadow: Mapping[str, object],
    *,
    session_date: str,
    at: datetime,
    max_age_seconds: float,
) -> tuple[dict[str, float] | None, float | None]:
    """Return only same-session, fresh, quality-qualified spread coordinates."""

    now = _state_time(at)
    updated_at = _state_time(level_shadow.get("updated_at"))
    if (
        now is None
        or updated_at is None
        or not _fresh_at(updated_at, now=now, max_age_seconds=max_age_seconds)
        or DEFAULT_MARKET_CALENDAR.research_expiry(now).isoformat() != session_date
        or DEFAULT_MARKET_CALENDAR.research_expiry(updated_at).isoformat() != session_date
    ):
        return None, None
    observation = level_shadow.get("latest_observation")
    if not isinstance(observation, Mapping) or observation.get("quality_ok") is not True:
        return None, None
    basis = observation.get("trigger_basis_points")
    if (
        isinstance(basis, bool)
        or not isinstance(basis, int | float)
        or not math.isfinite(float(basis))
        or abs(float(basis)) > BASIS_MAX_ABS_POINTS
    ):
        return None, None
    structure = level_shadow.get("structure")
    if (
        not isinstance(structure, Mapping)
        or structure.get("session_date") != session_date
        or str(structure.get("expiry") or "") != session_date
    ):
        return None, None
    confirmed_at = _state_time(structure.get("last_confirmed_at"))
    if confirmed_at is None or not _fresh_at(
        confirmed_at,
        now=now,
        max_age_seconds=max_age_seconds,
    ):
        return None, None
    levels = structure.get("levels")
    structure_levels = (
        {
            str(key): float(value)
            for key, value in levels.items()
            if (
                not isinstance(value, bool)
                and isinstance(value, int | float)
                and math.isfinite(float(value))
                and float(value) > 0
            )
        }
        if isinstance(levels, Mapping)
        else None
    )
    if not structure_levels:
        return None, None
    return structure_levels, float(basis)


def _gth_trend_entry_quality(
    trend_state: Mapping[str, object],
    *,
    session_date: str,
    at: datetime,
    max_age_seconds: float,
) -> dict[str, object]:
    """Freeze a non-enforcing GTH trend-alignment hypothesis at confirmation time."""

    now = _state_time(at)
    updated_at = _state_time(trend_state.get("updated_at"))
    session_id = str(trend_state.get("session_id") or "")
    regime = str(trend_state.get("regime") or "")
    reasons: list[str] = []
    if now is None or updated_at is None:
        reasons.append("trend_context_unavailable")
    elif not _fresh_at(updated_at, now=now, max_age_seconds=max_age_seconds):
        reasons.append("trend_context_stale")
    if session_id != f"{session_date}:globex":
        reasons.append("trend_session_mismatch")
    if regime != "bullish":
        reasons.append("trend_not_bullish")
    metrics = trend_state.get("metrics")
    metrics = metrics if isinstance(metrics, Mapping) else {}
    features = {
        "session_id": session_id or None,
        "trend_updated_at": updated_at.isoformat() if updated_at is not None else None,
        "regime": regime or None,
        "return_15m_points": metrics.get("return_15m_points"),
        "return_60m_points": metrics.get("return_60m_points"),
        "return_180m_points": metrics.get("return_180m_points"),
    }
    return {
        "mode": "shadow",
        "policy_version": "gth_trend_alignment_shadow_v1",
        "evaluated_at": now.isoformat() if now is not None else None,
        "verdict": "blocked" if reasons else "pass",
        "block_reasons": reasons,
        "features": features,
    }


def _virtual_strategy_blocks_gth(active: Mapping[str, object] | None) -> bool:
    """Only an already tracked two-leg GTH spread blocks another spread signal."""

    return bool(active and active.get("position_type") == "call_debit_spread")


def _state_time(value: object) -> datetime | None:
    if isinstance(value, datetime):
        parsed = value
    elif isinstance(value, str):
        try:
            parsed = datetime.fromisoformat(value)
        except ValueError:
            return None
    else:
        return None
    if parsed.tzinfo is None:
        return None
    return parsed.astimezone(timezone.utc)


def _fresh_at(value: datetime, *, now: datetime, max_age_seconds: float) -> bool:
    age = (now - value).total_seconds()
    return -5.0 <= age <= max_age_seconds


def _append_gth_signal_audit(
    storage: StorageSettings,
    at: datetime,
    payload: dict[str, object],
) -> None:
    path = (
        Path(storage.data_root)
        / "features"
        / "gth_dip_reclaim"
        / f"date={at.date().isoformat()}"
        / "events.jsonl"
    )
    path.parent.mkdir(parents=True, exist_ok=True)
    descriptor = os.open(path, os.O_APPEND | os.O_CREAT | os.O_WRONLY, 0o600)
    try:
        os.write(
            descriptor,
            (json.dumps(payload, ensure_ascii=False, sort_keys=True) + "\n").encode(),
        )
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def _append_gth_detector_health(
    storage: StorageSettings,
    at: datetime,
    payload: Mapping[str, object],
) -> None:
    session_date = str(payload.get("session_date") or "unknown")
    path = (
        Path(storage.data_root)
        / "features"
        / "gth_detector_health"
        / f"date={session_date}"
        / "samples.jsonl"
    )
    path.parent.mkdir(parents=True, exist_ok=True)
    descriptor = os.open(path, os.O_APPEND | os.O_CREAT | os.O_WRONLY, 0o600)
    try:
        os.write(
            descriptor,
            (json.dumps(dict(payload), ensure_ascii=False, sort_keys=True) + "\n").encode(),
        )
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def main() -> None:
    from spx_spark.application.runtime.intraday_shock_hot_worker import (
        run_locked_intraday_shock_once,
    )

    raise SystemExit(run_locked_intraday_shock_once(run))
