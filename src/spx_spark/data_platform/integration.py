"""Adapters from live SPX workflows into stable data-platform contracts."""

from __future__ import annotations

import hashlib
import json
import os
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import date, datetime, timezone
from typing import Any

from spx_spark.config import NY_TZ
from spx_spark.data_platform.contracts import (
    DecisionRecord,
    DeliveryRecord,
    EventRecord,
    FeatureSnapshotRecord,
    OutcomeRecord,
    StrategyVersionRecord,
)
from spx_spark.data_platform.ids import (
    deterministic_id,
    make_decision_id,
    make_event_key,
    make_feature_snapshot_id,
    make_outcome_id,
)
from spx_spark.data_platform.settings import DataPlatformSettings
from spx_spark.data_platform.telemetry import OperationalTelemetry, telemetry_from_settings
from spx_spark.notifier.policy import alert_key


STRATEGY_NAME = "spx_intraday_0dte"
UNKNOWN_ACTIVATION = datetime(1970, 1, 1, tzinfo=timezone.utc)
DELIVERY_CHANNELS = frozenset({"bark", "feishu", "openclaw"})


@dataclass(frozen=True)
class AlertResearchLink:
    event_key: str
    decision_id: str
    source_at: datetime


@dataclass(frozen=True)
class IntradayResearchResult:
    status: str
    evaluation_event_key: str | None = None
    evaluation_decision_id: str | None = None
    alert_links: tuple[AlertResearchLink, ...] = ()
    errors: tuple[str, ...] = ()


@dataclass(frozen=True)
class _PreparedDecisionBundle:
    event: EventRecord
    decision: DecisionRecord
    strategy_version: StrategyVersionRecord | None = None
    feature_snapshot: FeatureSnapshotRecord | None = None


@dataclass(frozen=True)
class PreparedIntradayEvaluation:
    result: IntradayResearchResult
    bundles: tuple[_PreparedDecisionBundle, ...] = ()


def prepare_intraday_evaluation(
    *,
    session_date: str,
    source_at: datetime,
    available_at: datetime,
    spx: float,
    es: float,
    spx_source_at: datetime,
    es_source_at: datetime,
    structure: Mapping[str, object],
    path_decision: Mapping[str, object],
    alerts: Sequence[Mapping[str, object]],
    strategy_config: Mapping[str, object],
    settings: DataPlatformSettings,
) -> PreparedIntradayEvaluation:
    """Build stable research links without touching disk or SQLite."""

    status = str(path_decision.get("status") or "neutral")
    if not alerts:
        return PreparedIntradayEvaluation(
            IntradayResearchResult(status="skipped_no_candidate")
        )

    writer_settings = settings
    source_at = _aware(source_at)
    available_at = max(_aware(available_at), source_at)
    session = date.fromisoformat(session_date)
    config_hash = _fingerprint(strategy_config)
    strategy_version = f"{writer_settings.writer_version}:{config_hash[:16]}"
    version_record = StrategyVersionRecord(
        strategy_name=STRATEGY_NAME,
        strategy_version=strategy_version,
        activated_at=UNKNOWN_ACTIVATION,
        git_commit=os.getenv("SPX_SPARK_GIT_COMMIT") or None,
        config_sha256=config_hash,
        metadata={"writer_version": writer_settings.writer_version},
    )

    evaluation_key = make_event_key(
        "intraday_strategy_evaluation",
        source_at,
        session_date,
        spx_source_at.isoformat(),
        es_source_at.isoformat(),
    )
    snapshot_id = make_feature_snapshot_id(evaluation_key, available_at, 1)
    evaluation_decision_id = make_decision_id(
        evaluation_key,
        STRATEGY_NAME,
        strategy_version,
        available_at,
    )
    event = EventRecord(
        event_key=evaluation_key,
        event_type="intraday_strategy_evaluation",
        session_date=session,
        source_at=source_at,
        received_at=available_at,
        available_at=available_at,
        phase=status,
        direction=None,
        data_quality="live",
        attributes={
            "spx": float(spx),
            "es": float(es),
            "spx_source_at": spx_source_at.isoformat(),
            "es_source_at": es_source_at.isoformat(),
        },
    )
    feature = FeatureSnapshotRecord(
        snapshot_id=snapshot_id,
        event_key=evaluation_key,
        captured_at=source_at,
        available_at=available_at,
        gamma_regime=str(path_decision.get("gamma_state") or "unknown"),
        payload={
            "structure": _sanitize_mapping(structure),
            "path_decision": _sanitize_mapping(path_decision),
            "spx": float(spx),
            "es": float(es),
        },
    )
    play = str(path_decision.get("play") or "")
    blocks = tuple(str(row) for row in path_decision.get("blocks", ()) if row)
    reasons = tuple(str(row) for row in path_decision.get("reasons", ()) if row)
    evaluation_decision = DecisionRecord(
        decision_id=evaluation_decision_id,
        event_key=evaluation_key,
        feature_snapshot_id=snapshot_id,
        strategy_name=STRATEGY_NAME,
        strategy_version=strategy_version,
        decision_at=available_at,
        available_at=available_at,
        status="context",
        action="observe",
        side="none",
        reason=(blocks or reasons or (None,))[0],
        gamma_regime=str(path_decision.get("gamma_state") or "unknown"),
        attributes={
            "play": play or None,
            "record_kind": "evaluation_context",
            "blocks": list(blocks),
            "reasons": list(reasons),
            "conditional_call_bias": bool(path_decision.get("conditional_call_bias")),
        },
    )
    bundles = [
        _PreparedDecisionBundle(
            strategy_version=version_record,
            event=event,
            feature_snapshot=feature,
            decision=evaluation_decision,
        )
    ]

    links: list[AlertResearchLink] = []
    for index, alert in enumerate(alerts):
        kind = str(alert.get("kind") or "unknown_alert")
        alert_source_at = min(_parse_at(alert.get("source_at")) or source_at, available_at)
        source_identity = str(
            alert.get("event_id")
            or alert.get("dedup_group")
            or deterministic_id("alert_source", index, alert.get("title"), alert.get("detail"))
        )
        alert_event_key = make_event_key(kind, alert_source_at, source_identity)
        alert_decision_id = make_decision_id(
            alert_event_key,
            STRATEGY_NAME,
            strategy_version,
            available_at,
        )
        alert_event = EventRecord(
            event_key=alert_event_key,
            event_type=kind,
            session_date=session,
            source_at=alert_source_at,
            received_at=available_at,
            available_at=available_at,
            phase=_phase_for_kind(kind),
            direction=_direction_from_alert(alert),
            data_quality=str(alert.get("quality") or "unknown"),
            attributes={
                "spx": float(spx),
                "es": float(es),
                "severity": str(alert.get("severity") or ""),
                "value": _number(alert.get("value")),
                "threshold": _number(alert.get("threshold")),
                "source_identity_key": deterministic_id("source_event", source_identity),
                "source_event_key": (
                    str(alert.get("source_event_key"))
                    if alert.get("source_event_key") is not None
                    else None
                ),
            },
        )
        alert_decision = DecisionRecord(
            decision_id=alert_decision_id,
            event_key=alert_event_key,
            feature_snapshot_id=snapshot_id,
            strategy_name=STRATEGY_NAME,
            strategy_version=strategy_version,
            decision_at=available_at,
            available_at=available_at,
            status="selected",
            action="notify",
            side=_side_for(kind),
            reason=str(alert.get("source_gate") or "") or None,
            gamma_regime=str(path_decision.get("gamma_state") or "unknown"),
            attributes={
                "alert_kind": kind,
                "record_kind": "alert_decision",
                "source_gate": str(alert.get("source_gate") or "") or None,
                "severity": str(alert.get("severity") or ""),
                "evaluation_decision_id": evaluation_decision_id,
            },
        )
        bundles.append(_PreparedDecisionBundle(event=alert_event, decision=alert_decision))
        links.append(
            AlertResearchLink(
                event_key=alert_event_key,
                decision_id=alert_decision_id,
                source_at=alert_source_at,
            )
        )

    return PreparedIntradayEvaluation(
        result=IntradayResearchResult(
            status="prepared",
            evaluation_event_key=evaluation_key,
            evaluation_decision_id=evaluation_decision_id,
            alert_links=tuple(links),
        ),
        bundles=tuple(bundles),
    )


def persist_intraday_evaluation(
    prepared: PreparedIntradayEvaluation,
    *,
    settings: DataPlatformSettings,
    telemetry: OperationalTelemetry | None = None,
) -> IntradayResearchResult:
    if not prepared.bundles:
        return prepared.result
    target = telemetry or telemetry_from_settings(settings)
    if target is None:
        return IntradayResearchResult(status="disabled")
    writes = [
        target.record_decision_bundle(
            strategy_version=bundle.strategy_version,
            event=bundle.event,
            feature_snapshot=bundle.feature_snapshot,
            decision=bundle.decision,
        )
        for bundle in prepared.bundles
    ]
    errors = tuple(row.error for row in writes if row.error)
    return IntradayResearchResult(
        status=_combined_status(tuple(row.status for row in writes)),
        evaluation_event_key=prepared.result.evaluation_event_key,
        evaluation_decision_id=prepared.result.evaluation_decision_id,
        alert_links=prepared.result.alert_links,
        errors=errors,
    )


def record_intraday_evaluation(
    *,
    session_date: str,
    source_at: datetime,
    available_at: datetime,
    spx: float,
    es: float,
    spx_source_at: datetime,
    es_source_at: datetime,
    structure: Mapping[str, object],
    path_decision: Mapping[str, object],
    alerts: Sequence[Mapping[str, object]],
    strategy_config: Mapping[str, object],
    settings: DataPlatformSettings | None = None,
    telemetry: OperationalTelemetry | None = None,
) -> IntradayResearchResult:
    writer_settings = settings or DataPlatformSettings.from_env()
    if not writer_settings.enabled and telemetry is None:
        return IntradayResearchResult(status="disabled")
    prepared = prepare_intraday_evaluation(
        session_date=session_date,
        source_at=source_at,
        available_at=available_at,
        spx=spx,
        es=es,
        spx_source_at=spx_source_at,
        es_source_at=es_source_at,
        structure=structure,
        path_decision=path_decision,
        alerts=alerts,
        strategy_config=strategy_config,
        settings=writer_settings,
    )
    return persist_intraday_evaluation(
        prepared,
        settings=writer_settings,
        telemetry=telemetry,
    )


def record_notification_result(
    *,
    payload: Mapping[str, object],
    selected_alerts: Sequence[Mapping[str, object]],
    notification: Mapping[str, object],
    attempted_at: datetime,
    settings: DataPlatformSettings | None = None,
    telemetry: OperationalTelemetry | None = None,
) -> tuple[str, ...]:
    writer_settings = settings or DataPlatformSettings.from_env()
    target = telemetry or telemetry_from_settings(writer_settings)
    if target is None:
        return ()
    attempted_at = _aware(attempted_at)
    sinks = tuple(row for row in notification.get("sinks", ()) if isinstance(row, Mapping))
    if not sinks:
        sinks = (
            {
                "sink": "notification_pipeline",
                "attempted": False,
                "ok": False,
                "error": notification.get("skipped_reason") or "no_enabled_sinks",
                "alert_keys": notification.get("selected_alert_keys") or (),
                "verdict": "skipped",
            },
        )
    written: list[str] = []
    for alert_index, alert in enumerate(selected_alerts):
        source_at = _parse_at(alert.get("source_at")) or _parse_at(payload.get("as_of"))
        source_at = min(source_at or attempted_at, attempted_at)
        kind = str(alert.get("kind") or "unknown_alert")
        source_identity = str(
            alert.get("event_id")
            or alert.get("dedup_group")
            or deterministic_id(
                "alert_source",
                alert_index,
                alert.get("title"),
                alert.get("detail"),
            )
        )
        phase = _phase_for_kind(kind)
        direction = _direction_from_alert(alert)
        data_quality = str(alert.get("quality") or "unknown")
        severity = str(alert.get("severity") or "")
        event_revision = deterministic_id(
            "event_revision",
            attempted_at,
            phase,
            direction,
            data_quality,
            severity,
        )
        event_key = str(
            alert.get("event_key")
            or make_event_key(
                kind,
                source_at,
                source_identity,
                event_revision,
            )
        )
        decision_id = str(
            alert.get("decision_id")
            or make_decision_id(
                event_key,
                f"alert::{kind}",
                writer_settings.writer_version,
                attempted_at,
            )
        )
        scoped_key = str(alert.get("decision_id") or alert_key(dict(alert)))
        context_consumed = any(
            _sink_applies_to_alert(
                sink,
                scoped_key=scoped_key,
                selected_count=len(selected_alerts),
            )
            and str(sink.get("sink") or "") == "context_policy"
            and str(sink.get("verdict") or "") == "consumed"
            for sink in sinks
        )
        has_prepared_link = bool(alert.get("decision_id") and alert.get("event_key"))
        if not has_prepared_link:
            event = EventRecord(
                event_key=event_key,
                event_type=kind,
                session_date=source_at.astimezone(NY_TZ).date(),
                source_at=source_at,
                received_at=attempted_at,
                available_at=attempted_at,
                phase=phase,
                direction=direction,
                data_quality=data_quality,
                attributes={"severity": severity},
            )
            decision = DecisionRecord(
                decision_id=decision_id,
                event_key=event_key,
                strategy_name=f"alert::{kind}",
                strategy_version=writer_settings.writer_version,
                decision_at=attempted_at,
                available_at=attempted_at,
                status="context" if context_consumed else "selected",
                action="observe" if context_consumed else "notify",
                side="none" if context_consumed else _side_for(kind),
                reason=(
                    "context_only_consumed"
                    if context_consumed
                    else str(alert.get("source_gate") or "") or None
                ),
                attributes={
                    "severity": severity,
                    "delivery_policy": "context_only" if context_consumed else "notify",
                },
            )
            target.record_decision_bundle(event=event, decision=decision)

        fingerprint = hashlib.sha256(
            f"{alert.get('title') or ''}\0{alert.get('detail') or ''}".encode("utf-8")
        ).hexdigest()[:32]
        for sink_index, sink in enumerate(sinks):
            if not _sink_applies_to_alert(
                sink,
                scoped_key=scoped_key,
                selected_count=len(selected_alerts),
            ):
                continue
            channel = str(sink.get("sink") or "unknown")
            ok = bool(sink.get("ok"))
            attempted = bool(sink.get("attempted"))
            verdict = str(sink.get("verdict") or "") or None
            status = _delivery_status(
                channel,
                attempted=attempted,
                ok=ok,
                verdict=verdict,
            )
            provider = _delivery_provider(channel)
            sent_at = attempted_at if status == "sent" else None
            veto_reason = (
                str(sink.get("error"))
                if sink.get("error") is not None
                and status in {"vetoed", "blocked", "suppressed"}
                else None
            )
            error_code = (
                str(sink.get("error")) if sink.get("error") is not None else None
            )
            attributes = {
                "attempted": attempted,
                "ok": ok,
                "verdict": verdict,
                "dry_run": bool(sink.get("dry_run")),
                "exit_code": (
                    sink.get("exit_code")
                    if isinstance(sink.get("exit_code"), int)
                    else None
                ),
            }
            delivery = DeliveryRecord(
                delivery_id=deterministic_id(
                    "delivery",
                    decision_id,
                    channel,
                    attempted_at,
                    sink_index,
                    provider,
                    status,
                    sent_at,
                    veto_reason,
                    error_code,
                    fingerprint,
                    attributes,
                ),
                decision_id=decision_id,
                channel=channel,
                provider=provider,
                status=status,
                attempted_at=attempted_at,
                sent_at=sent_at,
                veto_reason=veto_reason,
                error_code=error_code,
                message_fingerprint=fingerprint,
                attributes=attributes,
            )
            result = target.record_delivery(delivery)
            if result.status in {"recorded", "spooled"}:
                written.append(delivery.delivery_id)
    return tuple(written)


def record_outcome_rows(
    records: Sequence[Mapping[str, object]],
    *,
    settings: DataPlatformSettings | None = None,
    telemetry: OperationalTelemetry | None = None,
) -> tuple[str, ...]:
    writer_settings = settings or DataPlatformSettings.from_env()
    target = telemetry or telemetry_from_settings(writer_settings)
    if target is None:
        return ()
    written: list[str] = []
    for row in records:
        observed_at = _parse_at(row.get("observed_at"))
        target_at = _parse_at(row.get("target_at"))
        if observed_at is None or target_at is None:
            continue
        phase = str(row.get("phase") or "event")
        event_type = f"intraday_price_{phase}"
        event_key = str(
            row.get("event_key")
            or make_event_key(
                event_type,
                observed_at,
                str(row.get("observation_id") or row.get("record_key") or "unknown"),
            )
        )
        decision_id = (
            str(row.get("decision_id")) if row.get("decision_id") is not None else None
        )
        if decision_id is None:
            target.record_event(
                EventRecord(
                    event_key=event_key,
                    event_type=event_type,
                    session_date=observed_at.astimezone(NY_TZ).date(),
                    source_at=observed_at,
                    available_at=observed_at,
                    phase=phase,
                    direction=str(row.get("direction") or "") or None,
                    data_quality="live",
                    attributes={"start_spx": _number(row.get("start_spx"))},
                )
            )
        horizon = int(row.get("horizon_minutes") or 0)
        if horizon <= 0:
            continue
        outcome = OutcomeRecord(
            outcome_id=make_outcome_id(event_key, decision_id, horizon),
            event_key=event_key,
            decision_id=decision_id,
            horizon_minutes=horizon,
            status=str(row.get("status") or "unknown"),
            target_at=target_at,
            sampled_at=_parse_at(row.get("sample_at")),
            hypothesis_direction=(
                str(row.get("hypothesis_direction"))
                if row.get("hypothesis_direction") is not None
                else None
            ),
            spx_return_bps=_number(row.get("return_bps")),
            spx_mfe_bps=_number(row.get("mfe_bps")),
            spx_mae_bps=_number(row.get("mae_bps")),
            attributes={
                "reason": str(row.get("reason")) if row.get("reason") is not None else None,
                "start_spx": _number(row.get("start_spx")),
                "end_spx": _number(row.get("end_spx")),
                "path_high_return_bps": _number(row.get("path_high_return_bps")),
                "path_low_return_bps": _number(row.get("path_low_return_bps")),
                "sample_distance_seconds": _number(row.get("sample_distance_seconds")),
            },
        )
        result = target.record_outcome(outcome)
        if result.status in {"recorded", "spooled"}:
            written.append(outcome.outcome_id)
    return tuple(written)


def _fingerprint(value: Mapping[str, object]) -> str:
    return hashlib.sha256(
        json.dumps(
            _json_safe(value),
            allow_nan=False,
            separators=(",", ":"),
            sort_keys=True,
        ).encode("utf-8")
    ).hexdigest()


def _sanitize_mapping(value: Mapping[str, object]) -> Mapping[str, Any]:
    return {
        str(key): _json_safe(item)
        for key, item in value.items()
        if key not in {"event_id", "source_event_id"}
    }


def _json_safe(value: object) -> Any:
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, Mapping):
        return {str(key): _json_safe(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_json_safe(item) for item in value]
    if isinstance(value, (str, int, float, bool)) or value is None:
        return value
    return str(value)


def _combined_status(statuses: Sequence[str]) -> str:
    if "error" in statuses:
        return "error"
    if "spooled" in statuses:
        return "spooled"
    return "recorded"


def _side_for(value: str) -> str:
    normalized = value.lower()
    if "call" in normalized:
        return "call"
    if "put" in normalized:
        return "put"
    return "none"


def _phase_for_kind(kind: str) -> str | None:
    if kind.endswith("_shock"):
        return "shock"
    if kind.endswith("_reclaim"):
        return "reclaim"
    if "breakout" in kind:
        return "breakout"
    return None


def _direction_from_alert(alert: Mapping[str, object]) -> str | None:
    value = _number(alert.get("value"))
    kind = str(alert.get("kind") or "")
    if kind.endswith("_shock") and value is not None:
        return "up" if value > 0 else "down"
    if "call" in kind:
        return "up"
    if "put" in kind:
        return "down"
    return None


def _delivery_status(
    channel: str,
    *,
    attempted: bool,
    ok: bool,
    verdict: str | None,
) -> str:
    normalized_verdict = (verdict or "").lower()
    if normalized_verdict in {
        "vetoed",
        "blocked",
        "suppressed",
        "reviewed",
        "skipped",
        "consumed",
    }:
        return normalized_verdict
    if channel in DELIVERY_CHANNELS and attempted and ok:
        return "sent"
    if "gate" in channel or "prefilter" in channel:
        return "reviewed" if ok else "vetoed"
    if not attempted:
        return "skipped"
    return "ok" if ok else "failed"


def _sink_applies_to_alert(
    sink: Mapping[str, object],
    *,
    scoped_key: str,
    selected_count: int,
) -> bool:
    raw_scope = sink.get("alert_keys")
    scope = (
        {str(value) for value in raw_scope if isinstance(value, str) and value}
        if isinstance(raw_scope, (list, tuple))
        else set()
    )
    if scope:
        return scoped_key in scope
    return selected_count == 1


def _delivery_provider(channel: str) -> str:
    for provider in ("deepseek", "codex", "openclaw"):
        if channel.startswith(provider):
            return provider
    return channel


def _parse_at(value: object) -> datetime | None:
    if not isinstance(value, str) or not value:
        return None
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        return None
    return parsed.astimezone(timezone.utc)


def _aware(value: datetime) -> datetime:
    if value.tzinfo is None:
        raise ValueError("research timestamps must be timezone-aware")
    return value.astimezone(timezone.utc)


def _number(value: object) -> float | None:
    return float(value) if isinstance(value, (int, float)) else None
