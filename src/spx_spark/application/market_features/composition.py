"""Compose market and option frames into one auditable decision context."""

from __future__ import annotations

import hashlib
import json
from datetime import datetime
from typing import Any

from spx_spark.application.market_features.models import (
    DecisionAudit,
    DecisionContext,
    MinuteMarketFrame,
    OptionStructureFrame,
)
from spx_spark.settings.market_features import MarketFeatureSettings


def build_decision_context(
    market: MinuteMarketFrame,
    options: OptionStructureFrame,
    *,
    now: datetime,
    trend: dict[str, Any],
    level_decision: dict[str, Any],
    policy: MarketFeatureSettings | None = None,
) -> DecisionContext:
    policy = policy or MarketFeatureSettings()
    regime = str(trend.get("regime") or "neutral")
    es_spy = str(
        market.cross_asset.get("es_spy_direction_confirmation_15m") or "unavailable"
    )
    price_volume = str(market.volume.get("price_volume_alignment_5m") or "unavailable")
    liquidity = options.l1.metrics.get("liquidity_score")
    invalidations: list[str] = []
    if market.quality.value == "unavailable":
        invalidations.append("es_path_unavailable")
    if options.quality.value == "unavailable":
        invalidations.append("option_structure_unavailable")
    if es_spy == "divergent":
        invalidations.append("es_spy_direction_divergent")
    provider_divergence = market.cross_asset.get("es_provider_divergence")
    if isinstance(provider_divergence, dict) and provider_divergence.get("available") is False:
        invalidations.append("cross_provider_comparison_unavailable")
    if (
        isinstance(liquidity, int | float)
        and liquidity < policy.min_l1_liquidity_score
    ):
        invalidations.append("hot_option_liquidity_low")
    confirmations = {
        "globex_regime": regime,
        "es_spy_direction": es_spy,
        "price_volume_alignment": price_volume,
        "vix_vvix_direction": market.volatility.get(
            "vix_vvix_direction_confirmation_15m"
        ),
        "option_liquidity_score": liquidity,
        "level_phase": level_decision.get("phase"),
        "formal_level_signal": level_decision.get("formal_signal") is True,
    }
    context_key = {
        "market": market.frame_id,
        "options": options.frame_id,
        "regime": regime,
        "level_event": level_decision.get("event_id"),
        "level_phase": level_decision.get("phase"),
    }
    digest = hashlib.sha256(
        json.dumps(context_key, sort_keys=True).encode("utf-8")
    ).hexdigest()[:16]
    return DecisionContext(
        schema_version=1,
        context_id=f"decision:{digest}",
        as_of=now,
        session_id=market.session_id,
        market_frame_id=market.frame_id,
        option_frame_id=options.frame_id,
        trend=trend,
        level_decision=level_decision,
        confirmations=confirmations,
        invalidations=tuple(invalidations),
        data_quality={
            "market": market.quality.value,
            "options": options.quality.value,
            "option_l1": options.l1.quality.value,
        },
    )


def decision_signature(context: dict[str, Any]) -> tuple[object, ...]:
    trend = context.get("trend") if isinstance(context.get("trend"), dict) else {}
    level = (
        context.get("level_decision")
        if isinstance(context.get("level_decision"), dict)
        else {}
    )
    confirmations = (
        context.get("confirmations")
        if isinstance(context.get("confirmations"), dict)
        else {}
    )
    return (
        trend.get("regime"),
        level.get("event_id"),
        level.get("phase"),
        level.get("formal_signal"),
        confirmations.get("es_spy_direction"),
        confirmations.get("price_volume_alignment"),
    )


def build_decision_audit(
    context: DecisionContext,
    *,
    previous: dict[str, Any] | None,
) -> DecisionAudit | None:
    current = context.to_dict()
    if previous and decision_signature(previous) == decision_signature(current):
        return None
    level = context.level_decision
    trigger = "context_initialized" if not previous else "decision_context_changed"
    outcome_reference = str(level.get("event_id")) if level.get("event_id") else None
    digest = hashlib.sha256(
        f"{context.context_id}:{trigger}".encode("utf-8")
    ).hexdigest()[:16]
    return DecisionAudit(
        schema_version=1,
        audit_id=f"audit:{digest}",
        context_id=context.context_id,
        observed_at=context.as_of,
        trigger=trigger,
        decision_mid=None,
        order_limit=None,
        fill_price=None,
        slippage=None,
        outcome_status="linked" if outcome_reference else "context_only",
        outcome_reference=outcome_reference,
    )
