"""Fail-closed RTH projections for an unsafe ES bar publication."""

from __future__ import annotations

from collections.abc import Mapping


RTH_MARKET_STATE_FIELDS = (
    "price_vs_vwap",
    "vwap_slope",
    "opening_range_state",
    "market_structure",
    "efficiency_ratio",
    "vwap_cross_count",
    "same_time_range_ratio",
    "breadth_above_vwap",
)
RTH_ACTIONABLE_INTENT_STATUSES = frozenset({"trade_ready", "shadow_ready"})
RTH_ES_BAR_AUTHORITY_BLOCK_REASON = "rth_es_bar_consumer_not_ready"


def fence_rth_market_state_inputs(
    market_state_inputs: Mapping[str, object],
    *,
    es_bar_consumer: Mapping[str, object],
) -> dict[str, object]:
    """Remove every bar-derived input when the sampler publication is unsafe."""

    diagnostics = {
        **_dict(market_state_inputs.get("diagnostics")),
        "es_bar_consumer": dict(es_bar_consumer),
    }
    if es_bar_consumer.get("ready") is True:
        return {
            **dict(market_state_inputs),
            "diagnostics": diagnostics,
        }
    return {
        **dict(market_state_inputs),
        "status": "unavailable",
        "available_count": 0,
        "required_count": len(RTH_MARKET_STATE_FIELDS),
        "missing": list(RTH_MARKET_STATE_FIELDS),
        "values": {field: None for field in RTH_MARKET_STATE_FIELDS},
        "diagnostics": diagnostics,
    }


def fence_rth_market_state(
    rth_market_state: Mapping[str, object],
    *,
    es_bar_consumer: Mapping[str, object],
) -> dict[str, object]:
    if es_bar_consumer.get("ready") is True:
        return dict(rth_market_state)
    consumer_status = str(es_bar_consumer.get("status") or "unavailable")
    return {
        **dict(rth_market_state),
        "state": "UNCERTAIN",
        "market_state": "UNCERTAIN",
        "status": "unavailable",
        "classification_tier": "unavailable",
        "reasons": [
            f"es_bar_consumer_{consumer_status}",
            *[f"es_bar_consumer:{reason}" for reason in es_bar_consumer.get("reasons") or []],
        ],
    }


def fence_rth_trade_intent_authority(
    trade_intent: Mapping[str, object],
    *,
    es_bar_consumer: Mapping[str, object],
) -> dict[str, object]:
    """Remove RTH ready authority when the canonical ES publication is unsafe."""

    result = dict(trade_intent)
    if (
        result.get("status") not in RTH_ACTIONABLE_INTENT_STATUSES
        or es_bar_consumer.get("ready") is True
    ):
        return result

    consumer_status = str(es_bar_consumer.get("status") or "unavailable")
    existing_reasons = [
        item.strip()
        for item in result.get("block_reasons") or []
        if isinstance(item, str) and item.strip()
    ]
    consumer_reasons = [
        item.strip()
        for item in es_bar_consumer.get("reasons") or []
        if isinstance(item, str) and item.strip()
    ]
    reasons = [
        *existing_reasons,
        RTH_ES_BAR_AUTHORITY_BLOCK_REASON,
        f"es_bar_consumer_{consumer_status}",
        *(f"es_bar_consumer:{reason}" for reason in consumer_reasons),
    ]
    return {
        **result,
        "status": "blocked",
        "execution_eligible": False,
        "quote_observation_eligible": False,
        "rth_trade_ready_authority": False,
        "es_bar_consumer": dict(es_bar_consumer),
        "block_reasons": list(dict.fromkeys(reasons)),
    }


def _dict(value: object) -> dict[str, object]:
    return dict(value) if isinstance(value, dict) else {}
