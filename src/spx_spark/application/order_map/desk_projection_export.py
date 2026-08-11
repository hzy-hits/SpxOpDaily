"""Narrow Python-to-Rust desk-map projection adapter.

Python computes research and market structure.  This module exports those facts
as one atomic, versioned document; it does not decide whether or where to send a
human notification.
"""

from __future__ import annotations

import hashlib
import json
import math
import os
from dataclasses import asdict
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Mapping

from spx_spark.application.order_map.operator_status import (
    build_desk_map_projection,
    build_desk_message_sections,
)
from spx_spark.application.order_map.report_clock import (
    floor_report_slot_et,
    rth_report_slot,
)
from spx_spark.config import StorageSettings
from spx_spark.market_calendar import DEFAULT_MARKET_CALENDAR, ET
from spx_spark.state_io import atomic_write_json_secure, read_json_object


SCHEMA_VERSION = "desk_map_projection.v1"
DEFAULT_RTH_TTL = timedelta(minutes=20)
DEFAULT_GTH_TTL = timedelta(minutes=65)
MAX_RESEARCH_CONTEXT_AGE = timedelta(minutes=5)
STRATEGY_DISTRIBUTION_SCHEMA_VERSION = "strategy_distribution_forecast.v1"


def rust_report_owner_enabled() -> bool:
    """Return the explicit single-writer cutover switch, rejecting typos."""

    raw = os.getenv("SPX_RUST_REPORT_OWNER", "false").strip().lower()
    if raw in {"1", "true", "yes", "on"}:
        return True
    if raw in {"0", "false", "no", "off", ""}:
        return False
    raise ValueError("SPX_RUST_REPORT_OWNER must be a boolean")


def projection_path(storage: StorageSettings) -> Path:
    configured = os.getenv("SPX_RUST_DESK_PROJECTION_PATH", "").strip()
    if configured:
        return Path(configured)
    return Path(storage.data_root) / "latest" / "desk_map_projection.json"


def build_desk_map_wire(
    payload: Mapping[str, Any],
    changes: list[str],
    *,
    now: datetime,
    trading_date: str,
    storage: StorageSettings,
    published_at: datetime | None = None,
) -> dict[str, Any]:
    """Build the complete source projection consumed by the Rust report lane."""

    if now.tzinfo is None or now.utcoffset() is None:
        raise ValueError("desk-map projection requires timezone-aware now")
    now = now.astimezone(timezone.utc)
    published_at = published_at or now
    if published_at.tzinfo is None or published_at.utcoffset() is None:
        raise ValueError("desk-map projection requires timezone-aware published_at")
    published_at = published_at.astimezone(timezone.utc)
    if published_at < now:
        raise ValueError("desk-map published_at cannot precede evaluation time")
    rth_slot = rth_report_slot(now)
    rth_open = DEFAULT_MARKET_CALENDAR.is_rth_open(now)
    session = "rth" if rth_open else "gth"
    slot_key = (
        rth_slot.key if rth_slot is not None else _projection_slot_key(now, trading_date, session)
    )
    valid_until = published_at + (DEFAULT_RTH_TTL if rth_open else DEFAULT_GTH_TTL)
    projection = build_desk_map_projection(payload)
    sections = build_desk_message_sections(payload, now)
    stage = projection.stage.value.lower()
    quality = projection.data_quality.lower()
    quality_reasons = list(projection.quality_reasons)
    if stage == "ready" and (
        projection.direction not in {"up", "down"} or projection.thesis not in {"breakout", "fade"}
    ):
        # Preserve the legacy human card, but do not claim a typed READY state
        # at the Rust boundary unless the full direction/thesis is representable.
        stage = "paused"
        quality = "degraded"
        if projection.direction not in {"up", "down"}:
            quality_reasons.append("ready_direction_missing")
        if projection.thesis not in {"breakout", "fade"}:
            quality_reasons.append("ready_thesis_missing")
        quality_reasons = list(dict.fromkeys(quality_reasons))
    fingerprint = _structure_fingerprint(payload, projection, slot_key)
    observed_through = _observed_through(payload, now)
    research_context, _research_context_reason = _research_context(
        storage,
        published_at,
        trading_date=trading_date,
        session=session,
    )
    research_document_id = (
        str(research_context["document_id"]) if research_context is not None else None
    )
    structure = sections.structure
    if changes:
        structure = f"{structure}\nChanges: {'；'.join(changes)}"
    desk_view = sections.desk_view
    if context := _operator_context(payload).strip():
        desk_view = f"{desk_view}\n{context}"
    # Uncalibrated HMM / P-vs-Q stays on the audit wire only; never on Desk View.
    data_quality = sections.data_quality

    document: dict[str, Any] = {
        "schema_version": SCHEMA_VERSION,
        "projection_id": "pending",
        "source_snapshot_id": _source_snapshot_id(payload, fingerprint),
        "source_slot": slot_key,
        "trading_date_et": trading_date,
        "session": session,
        "observed_through": observed_through.isoformat().replace("+00:00", "Z"),
        "available_at": published_at.isoformat().replace("+00:00", "Z"),
        "valid_until": valid_until.isoformat().replace("+00:00", "Z"),
        "structure_fingerprint": fingerprint,
        "stage": stage,
        "phase": projection.phase.value,
        "direction": projection.direction,
        "thesis": projection.thesis,
        "level_kind": projection.level_kind or None,
        "level": projection.level,
        "quality": quality,
        "quality_reasons": quality_reasons,
        "research_context_document_id": research_document_id,
        "research_context": research_context,
        "action_authority": "none",
        "automatic_ordering": False,
        "message": {
            **asdict(sections),
            "desk_view": desk_view,
            "structure": structure,
            "data_quality": data_quality,
        },
    }
    identity_payload = {key: value for key, value in document.items() if key != "projection_id"}
    identity = _sha256(identity_payload)
    document["projection_id"] = f"desk-map:{identity[:24]}"
    return document


def persist_desk_map_projection(
    payload: Mapping[str, Any],
    changes: list[str],
    *,
    now: datetime,
    trading_date: str,
    storage: StorageSettings,
    published_at: datetime | None = None,
) -> dict[str, Any]:
    document = build_desk_map_wire(
        payload,
        changes,
        now=now,
        trading_date=trading_date,
        storage=storage,
        published_at=published_at,
    )
    atomic_write_json_secure(projection_path(storage), document)
    return document


def _operator_context(payload: Mapping[str, Any]) -> str:
    # strategy_decision Base Case already owns 结论/主因; do not append a second
    # conflicting level-machine reason into the same screen.
    if isinstance(payload.get("strategy_decision"), Mapping) and payload.get(
        "strategy_decision"
    ):
        return ""
    from spx_spark.application.order_map.status_explanation import operator_reason_line

    return operator_reason_line(dict(payload))


def _source_snapshot_id(payload: Mapping[str, Any], fallback: str) -> str:
    for key in ("option_structure_frame", "minute_market_frame"):
        value = payload.get(key)
        if isinstance(value, Mapping):
            candidate = str(value.get("frame_id") or value.get("snapshot_id") or "").strip()
            if candidate:
                return candidate
    decision = payload.get("level_decision")
    if isinstance(decision, Mapping):
        candidate = str(decision.get("event_id") or "").strip()
        if candidate:
            return candidate
    return f"snapshot:{fallback[:24]}"


def _observed_through(payload: Mapping[str, Any], now: datetime) -> datetime:
    candidates: list[object] = []
    for key in ("option_structure_frame", "minute_market_frame"):
        value = payload.get(key)
        if isinstance(value, Mapping):
            candidates.extend((value.get("observed_through"), value.get("available_at")))
    candidates.extend((payload.get("as_of"), payload.get("generated_at")))
    for candidate in candidates:
        if not candidate:
            continue
        try:
            parsed = datetime.fromisoformat(str(candidate).replace("Z", "+00:00"))
        except ValueError:
            continue
        if parsed.tzinfo is not None and parsed <= now:
            return parsed.astimezone(timezone.utc)
    return now


def _research_context(
    storage: StorageSettings,
    published_at: datetime,
    *,
    trading_date: str,
    session: str,
) -> tuple[dict[str, Any] | None, str | None]:
    document = read_json_object(
        Path(storage.data_root) / "latest" / "experimental_research_signals.json"
    )
    if document.get("schema_version") != "research_context.v2":
        return None, None
    if session not in {"rth", "gth"}:
        return None, "research_context_session_not_supported"

    frame = document.get("cross_index_frame")
    prior = document.get("prior_rth_context")
    regime = document.get("regime")
    forecasts = document.get("forecasts")
    close_location = document.get("close_location")
    if (
        not isinstance(frame, Mapping)
        or not isinstance(prior, Mapping)
        or (regime is not None and not isinstance(regime, Mapping))
        or not isinstance(forecasts, list)
        or len(forecasts) != 3
        or not isinstance(close_location, Mapping)
        or not str(document.get("document_id") or "").strip()
        or document.get("action_authority") != "none"
        or document.get("automatic_ordering") is not False
    ):
        return None, "research_context_contract_invalid"

    context_dates = {
        frame.get("trading_date_et"),
        prior.get("for_trading_date"),
        regime.get("trading_date_et") if isinstance(regime, Mapping) else trading_date,
    }
    if context_dates != {trading_date}:
        return None, "research_context_trading_date_mismatch"

    try:
        generated_at = datetime.fromisoformat(
            str(document.get("generated_at") or "").replace("Z", "+00:00")
        )
        target_values = [
            *(item.get("target_at") for item in forecasts if isinstance(item, Mapping)),
            close_location.get("target_at"),
        ]
        target_dates = {
            datetime.fromisoformat(str(value).replace("Z", "+00:00"))
            .astimezone(ET)
            .date()
            .isoformat()
            for value in target_values
        }
    except (TypeError, ValueError):
        return None, "research_context_contract_invalid"
    if len(target_values) != 4 or target_dates != {trading_date}:
        return None, "research_context_trading_date_mismatch"
    if generated_at.tzinfo is None or generated_at.astimezone(timezone.utc) > published_at:
        return None, "research_context_from_future"
    if published_at - generated_at.astimezone(timezone.utc) > MAX_RESEARCH_CONTEXT_AGE:
        return None, "research_context_stale"
    return dict(document), None


def _research_advisory_summary(
    context: Mapping[str, Any] | None,
    *,
    session: str,
) -> str | None:
    """Render one bounded HMM/range line without promoting research to action authority."""

    if context is None:
        return None
    facts: list[str] = []
    model_state: str | None = None
    regime = context.get("regime")
    if isinstance(regime, Mapping):
        posterior = regime.get("posterior")
        rows = [row for row in posterior or () if isinstance(row, Mapping)]
        ranked = sorted(
            (
                (float(probability), str(row.get("state_id") or ""))
                for row in rows
                if (probability := row.get("probability")) is not None
                and isinstance(probability, int | float)
                and str(row.get("state_id") or "").strip()
            ),
            reverse=True,
        )
        if ranked:
            probability, state_id = ranked[0]
            model_state = f"HMM {state_id} 模型权重 {probability:.0%}（不是上涨概率）"

    forecasts = context.get("forecasts")
    close_forecast = next(
        (
            row
            for row in forecasts or ()
            if isinstance(row, Mapping)
            and row.get("target") == "rth_close"
            and row.get("status") == "available"
        ),
        None,
    )
    if isinstance(close_forecast, Mapping):
        quantiles = close_forecast.get("quantiles")
        if isinstance(quantiles, Mapping):
            values = tuple(quantiles.get(key) for key in ("p10", "p50", "p90"))
            if all(isinstance(value, int | float) for value in values):
                p10, p50, p90 = (float(value) for value in values)
                facts.append(f"RTH收盘启发区间 {p10:.1f}/{p50:.1f}/{p90:.1f}")

    close_location = context.get("close_location")
    research_view = "弃权"
    primary_limitation = "缺少可用的收盘位置映射"
    close_bucket_available = False
    if isinstance(close_location, Mapping) and close_location.get("status") == "available":
        probabilities = close_location.get("probabilities")
        if isinstance(probabilities, Mapping):
            ranked_buckets = sorted(
                (
                    (float(probability), str(bucket))
                    for bucket, probability in probabilities.items()
                    if isinstance(probability, int | float)
                ),
                reverse=True,
            )
            if ranked_buckets:
                probability, bucket = ranked_buckets[0]
                close_bucket_available = True
                research_view = {
                    "lower_third": "下侧收盘倾向",
                    "middle_third": "区间/中位收盘",
                    "upper_third": "上侧收盘倾向",
                }.get(bucket, "不确定")
                facts.append(f"HMM映射后的主导收盘桶模型权重 {probability:.0%}")
        reason_codes = close_location.get("reason_codes")
        if isinstance(reason_codes, list | tuple) and reason_codes:
            primary_limitation = _research_limitation(str(reason_codes[0]))

    if model_state is not None and not close_bucket_available:
        facts.insert(0, model_state)

    reason_codes = {
        str(reason)
        for reason in context.get("regime_reason_codes") or ()
        if isinstance(reason, str)
    }
    basis = _research_basis(context, session=session, reason_codes=reason_codes)
    for reason in (
        "es_path_component_unavailable",
        "prior_rth_component_unavailable",
        "cash_index_component_unavailable",
    ):
        if reason in reason_codes:
            primary_limitation = _research_limitation(reason)
            break
    if not facts:
        return (
            "研究（HMM未校准，不改结论）："
            f"基线={research_view} · 可靠性=低（{basis}） · 主要限制={primary_limitation}"
        )
    return (
        "研究（HMM未校准，不改结论）："
        f"基线={research_view} · "
        + " · ".join(facts[:2])
        + f" · 可靠性=低（{basis}） · 主要限制={primary_limitation}"
    )


def _strategy_distribution_forecast(
    storage: StorageSettings,
    published_at: datetime,
    *,
    trading_date: str,
    session: str,
) -> tuple[dict[str, Any] | None, str | None]:
    """Load one fresh advisory artifact without granting it decision authority."""

    document = read_json_object(
        Path(storage.data_root) / "latest" / "strategy_distribution_forecast.json"
    )
    if not document:
        return None, None
    if document.get("schema_version") != STRATEGY_DISTRIBUTION_SCHEMA_VERSION:
        return None, "概率契约版本不匹配"
    if document.get("trading_date_et") != trading_date or document.get("session") != session:
        return None, "概率帧不属于当前交易时段"
    if (
        document.get("evidence_status") != "research_unvalidated"
        or document.get("action_authority") != "none"
        or document.get("automatic_ordering") is not False
    ):
        return None, "概率帧安全边界不合法"

    try:
        observed_through = _parse_aware_timestamp(document["observed_through"])
        available_at = _parse_aware_timestamp(document["available_at"])
        valid_until = _parse_aware_timestamp(document["valid_until"])
    except (KeyError, TypeError, ValueError):
        return None, "概率帧时间戳不合法"
    if observed_through > available_at or available_at > published_at:
        return None, "概率帧时间顺序不合法"
    if published_at > valid_until:
        return None, "概率帧已过期"

    q_event = document.get("q_event")
    p_event = document.get("p_event")
    decision = document.get("shadow_decision")
    candidates = document.get("strategy_candidates")
    if (
        not isinstance(q_event, Mapping)
        or not isinstance(p_event, Mapping)
        or not isinstance(decision, Mapping)
        or not isinstance(candidates, list)
        or not str(document.get("document_id") or "").strip()
        or q_event.get("measure") != "risk_neutral"
        or p_event.get("measure") != "physical"
        or q_event.get("event") != p_event.get("event")
        or decision.get("action") not in {"no_trade", "manual_candidate"}
    ):
        return None, "概率帧契约不完整"
    for estimate in (q_event, p_event):
        if not _valid_probability_estimate(estimate):
            return None, "概率值或证据字段不合法"
    return dict(document), None


def _strategy_distribution_summary(
    document: Mapping[str, Any] | None,
) -> str | None:
    """Render one bounded P-vs-Q line; never promote it into a direction gate."""

    if document is None:
        return None
    q_event = document.get("q_event")
    p_event = document.get("p_event")
    decision = document.get("shadow_decision")
    if not isinstance(q_event, Mapping) or not isinstance(p_event, Mapping):
        return None
    event = p_event.get("event")
    event = event if isinstance(event, Mapping) else {}
    kind = str(event.get("kind") or "")
    label = {
        "terminal_above": "上行终值跟随",
        "terminal_below": "下行终值跟随",
        "terminal_between": "区间终值",
        "upper_first_touch": "先触上沿",
        "lower_first_touch": "先触下沿",
    }.get(kind, "同一事件")
    horizon = _event_horizon_minutes(document, event)
    horizon_text = f"{horizon:g}分钟{label}" if horizon is not None else label

    p_probability = _probability_value(p_event)
    q_probability = _probability_value(q_event)
    action = (
        str(decision.get("action") or "no_trade").upper().replace("_", " ")
        if isinstance(decision, Mapping)
        else "NO TRADE"
    )
    if p_probability is None and q_probability is None:
        # An unavailable comparison is audit metadata, not a human decision.
        # Omitting it prevents every NO TRADE map from repeating another
        # content-free NO TRADE line.
        return None
    facts: list[str] = []
    if p_probability is not None:
        sample_count = _positive_int_or_none(p_event.get("sample_count"))
        session_count = _positive_int_or_none(p_event.get("session_count"))
        evidence = ""
        if sample_count is not None and session_count is not None:
            evidence = f"（前日止，n={sample_count}/{session_count}日"
            low = _probability_or_none(p_event.get("interval_low"))
            high = _probability_or_none(p_event.get("interval_high"))
            if low is not None and high is not None and low <= high:
                evidence += f"，区间{low:.0%}–{high:.0%}"
            reasons = p_event.get("reason_codes")
            if isinstance(reasons, list | tuple) and "physical_sample_below_minimum" in reasons:
                evidence += "，样本不足"
            if (
                isinstance(reasons, list | tuple)
                and "requested_cohort_below_minimum_using_global_baseline" in reasons
            ):
                evidence += "，使用全局确认事件基线"
            evidence += "）"
        facts.append(f"P {p_probability:.0%}{evidence}")
    else:
        facts.append("P 暂不可用")
    if q_probability is not None:
        facts.append(f"Q代理 {q_probability:.0%}")
    else:
        facts.append("Q代理暂不可用")
    if p_probability is not None and q_probability is not None:
        facts.append(f"P−Q {100.0 * (p_probability - q_probability):+.0f}pp")
    return (
        f"P/Q研究（未校准，不产生方向） {horizon_text}："
        + " · ".join(facts)
        + f"；未扣点差/滑点，真实成交与净收益标签尚不可用 → {action}"
    )


def _valid_probability_estimate(estimate: Mapping[str, Any]) -> bool:
    status = estimate.get("status")
    probability = estimate.get("probability")
    if status == "unavailable":
        return probability is None
    if status != "available" or _probability_or_none(probability) is None:
        return False
    for key in ("interval_low", "interval_high"):
        value = estimate.get(key)
        if value is not None and _probability_or_none(value) is None:
            return False
    for key in ("sample_count", "session_count"):
        value = estimate.get(key)
        if value is not None and _positive_int_or_none(value) is None:
            return False
    return True


def _event_horizon_minutes(
    document: Mapping[str, Any],
    event: Mapping[str, Any],
) -> float | None:
    try:
        available_at = _parse_aware_timestamp(document["available_at"])
        target = _parse_aware_timestamp(event["target_at"])
    except (KeyError, TypeError, ValueError):
        return None
    # The event horizon is defined when the forecast is built.  Source
    # observations can legitimately precede that instant by a few seconds and
    # must not turn a fixed five-minute contract into a noisy 5.x-minute label.
    seconds = (target - available_at).total_seconds()
    return seconds / 60.0 if seconds > 0.0 else None


def _parse_aware_timestamp(value: object) -> datetime:
    parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise ValueError("timestamp must be timezone-aware")
    return parsed.astimezone(timezone.utc)


def _probability_value(estimate: Mapping[str, Any]) -> float | None:
    if estimate.get("status") != "available":
        return None
    return _probability_or_none(estimate.get("probability"))


def _probability_or_none(value: object) -> float | None:
    if isinstance(value, bool) or not isinstance(value, int | float):
        return None
    parsed = float(value)
    return parsed if math.isfinite(parsed) and 0.0 <= parsed <= 1.0 else None


def _positive_int_or_none(value: object) -> int | None:
    return value if isinstance(value, int) and not isinstance(value, bool) and value > 0 else None


def _research_limitation(reason_code: str) -> str:
    return {
        "latent_state_location_mapping_unvalidated": "潜状态到收盘位置的映射未验证",
        "fixed_bootstrap_parameters_unvalidated": "固定bootstrap参数未验证",
        "fixed_bootstrap_hmm_shift_unvalidated": "HMM区间偏移未验证",
        "cash_index_component_unavailable": "现金跨指数确认不可用",
        "prior_rth_component_unavailable": "前日RTH上下文不可用",
        "es_path_component_unavailable": "夜盘ES路径不可用",
    }.get(reason_code, "研究输入仍需验证")


def _research_context_reason_text(reason_code: str) -> str:
    return {
        "research_context_session_not_supported": "当前时段不支持该研究帧",
        "research_context_contract_invalid": "研究帧契约不完整",
        "research_context_trading_date_mismatch": "研究帧不属于当前交易日",
        "research_context_from_future": "研究帧时间顺序异常",
        "research_context_stale": "研究帧已过期",
    }.get(reason_code, "研究帧校验失败")


def _research_basis(
    context: Mapping[str, Any],
    *,
    session: str,
    reason_codes: set[str],
) -> str:
    if session != "gth":
        return "RTH点时帧（按可用组件）"
    es_available = "es_path_component_unavailable" not in reason_codes
    prior = context.get("prior_rth_context")
    prior_status = str(prior.get("status") or "") if isinstance(prior, Mapping) else ""
    prior_available = (
        prior_status in {"ready", "partial"}
        and "prior_rth_component_unavailable" not in reason_codes
    )
    if es_available and prior_available:
        suffix = "完整" if prior_status == "ready" else "部分"
        return f"夜盘ES+{suffix}前日RTH"
    if es_available:
        return "夜盘ES为主（前日RTH不可用）"
    if prior_available:
        return "前日RTH为主（夜盘ES不可用）"
    return "GTH点时帧（按可用组件）"


def _structure_fingerprint(
    payload: Mapping[str, Any],
    projection: object,
    slot_key: str,
) -> str:
    option_frame = payload.get("option_structure_frame")
    option_frame = option_frame if isinstance(option_frame, Mapping) else {}
    option_structure = option_frame.get("structure")
    option_structure = option_structure if isinstance(option_structure, Mapping) else {}
    option_volatility = option_frame.get("volatility")
    option_volatility = option_volatility if isinstance(option_volatility, Mapping) else {}
    greek_reference = payload.get("spxw_0dte_greeks_reference")
    greek_reference = greek_reference if isinstance(greek_reference, Mapping) else {}
    greek_aggregate = greek_reference.get("aggregate")
    greek_aggregate = greek_aggregate if isinstance(greek_aggregate, Mapping) else {}
    source = {
        "slot": slot_key,
        "stage": getattr(getattr(projection, "stage"), "value"),
        "phase": getattr(getattr(projection, "phase"), "value"),
        "direction": getattr(projection, "direction"),
        "thesis": getattr(projection, "thesis"),
        "level_kind": getattr(projection, "level_kind"),
        "level": getattr(projection, "level"),
        "quality": getattr(projection, "data_quality"),
        "quality_reasons": getattr(projection, "quality_reasons"),
        "underlier": payload.get("underlier"),
        "es_last": payload.get("es_last"),
        "flip_zone": payload.get("flip_zone"),
        "gamma_context": {
            "gamma_state": option_structure.get("gamma_state") or payload.get("gamma_state"),
            "zero_gamma": option_structure.get("zero_gamma") or payload.get("zero_gamma"),
            "net_gamma_ratio": option_structure.get("net_gamma_ratio"),
            "gex_quality": option_structure.get("gex_quality"),
            "wall_rank_persistence": option_structure.get("wall_rank_persistence"),
            "atm_iv_change_5m": option_volatility.get("atm_iv_change_5m"),
            "atm_iv_change_15m": option_volatility.get("atm_iv_change_15m"),
            "atm_iv_change_60m": option_volatility.get("atm_iv_change_60m"),
            "gross_charm_5m_abs": greek_aggregate.get("gross_charm_5m_abs"),
            "gross_vanna_1vol_abs": greek_aggregate.get("gross_vanna_1vol_abs"),
            "dealer_position_sign": "unknown",
        },
    }
    return _sha256(source)


def _sha256(value: object) -> str:
    encoded = json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        default=str,
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def _projection_slot_key(now: datetime, trading_date: str, session: str) -> str:
    """Build a GTH/RTH source_slot that matches the Rust quarter-hour lane."""

    slot_at = floor_report_slot_et(now)
    return f"{trading_date}:{session}:{slot_at.strftime('%H:%M')}"


__all__ = [
    "SCHEMA_VERSION",
    "build_desk_map_wire",
    "persist_desk_map_projection",
    "projection_path",
    "rust_report_owner_enabled",
]
