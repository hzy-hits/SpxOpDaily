"""Causal observation features for the advisory online regime filter."""

from __future__ import annotations

import hashlib
import json
import math
from collections.abc import Mapping
from datetime import date


FEATURE_SCHEMA_VERSION = "market_regime_features.cross_index.v2"
CROSS_INDEX_FEATURE_SET_VERSION = "cash-index-rth-relative-breadth-dispersion:v1"
ES_FEATURE_WEIGHTS = {
    "return_15m_points": 0.30,
    "return_60m_points": 0.40,
    "vwap_distance_points": 0.20,
    "vwap_slope_15m_points": 0.10,
}
OBSERVATION_COMPONENT_WEIGHTS = {
    "es_path": 0.20,
    "cash_index": 0.70,
    "prior_rth": 0.10,
}


def build_feature_observation(
    market: Mapping[str, object],
    options: Mapping[str, object],
    prior_rth_context: Mapping[str, object],
    *,
    session_day: date | None,
) -> dict[str, object] | None:
    es = _mapping(market.get("es"))
    expected_move = _expected_move(options)
    raw = {name: _number(es.get(name)) for name in ES_FEATURE_WEIGHTS}
    available = {name: value for name, value in raw.items() if value is not None}
    es_score: float | None = None
    scale: float | None = None
    efficiency = _number(es.get("trend_efficiency_60m"))
    if available:
        fallback_scale = max(
            10.0,
            abs(available.get("return_60m_points", 0.0)),
            2.0 * abs(available.get("return_15m_points", 0.0)),
            2.0 * abs(available.get("vwap_distance_points", 0.0)),
        )
        scale = expected_move or fallback_scale
        total_weight = sum(ES_FEATURE_WEIGHTS[name] for name in available)
        es_score = (
            sum(
                ES_FEATURE_WEIGHTS[name] * max(-2.0, min(2.0, value / scale))
                for name, value in available.items()
            )
            / total_weight
        )
        if efficiency is not None:
            es_score *= 0.75 + 0.5 * max(0.0, min(1.0, efficiency))
        es_score = max(-2.0, min(2.0, es_score))

    cash_score, cash_payload = _cash_index_component(market)
    prior_score, prior_payload = _prior_rth_component(
        prior_rth_context,
        session_day=session_day,
    )
    component_scores = {
        "es_path": es_score,
        "cash_index": cash_score,
        "prior_rth": prior_score,
    }
    usable_scores = {name: score for name, score in component_scores.items() if score is not None}
    if not usable_scores:
        return None
    total_weight = sum(OBSERVATION_COMPONENT_WEIGHTS[name] for name in usable_scores)
    score = (
        sum(
            OBSERVATION_COMPONENT_WEIGHTS[name] * component_score
            for name, component_score in usable_scores.items()
        )
        / total_weight
    )
    payload: dict[str, object] = {
        "feature_schema_version": FEATURE_SCHEMA_VERSION,
        "direction_score": round(max(-2.0, min(2.0, score)), 10),
        "component_weights": OBSERVATION_COMPONENT_WEIGHTS,
        "components": {
            "es_path": {
                "status": "available" if es_score is not None else "unavailable",
                "score": round(es_score, 10) if es_score is not None else None,
                "scale_points": round(scale, 10) if scale is not None else None,
                "scale_source": (
                    "expected_move_points_0dte"
                    if expected_move is not None
                    else "bounded_local_fallback"
                ),
                "values": {name: raw[name] for name in ES_FEATURE_WEIGHTS},
                "trend_efficiency_60m": efficiency,
            },
            "cash_index": cash_payload,
            "prior_rth": prior_payload,
        },
        "degradation_reasons": sorted(
            f"{name}_component_unavailable"
            for name, component_score in component_scores.items()
            if component_score is None
        ),
    }
    frame_identity = market.get("frame_id") or market.get("as_of")
    payload["observation_id"] = _canonical_hash(
        {
            "feature_schema_version": FEATURE_SCHEMA_VERSION,
            "market_frame_identity": frame_identity,
        }
    )
    return payload


def _cash_index_component(
    market: Mapping[str, object],
) -> tuple[float | None, dict[str, object]]:
    cross_asset = _mapping(market.get("cross_asset"))
    selected = _mapping(cross_asset.get("cross_index"))
    if selected.get("source") == "globex_index":
        return _globex_index_component(selected)
    cash = _mapping(cross_asset.get("cash_index"))
    relative = _mapping(cash.get("relative_to_spx_15m_bps"))
    breadth = _mapping(cash.get("breadth_15m"))
    relative_values = [
        _number(relative.get(instrument)) for instrument in ("index:NDX", "index:DJI", "index:RUT")
    ]
    dispersion = _number(cash.get("dispersion_15m_bps"))
    up_count = _non_negative_int(breadth.get("up_count"))
    down_count = _non_negative_int(breadth.get("down_count"))
    flat_count = _non_negative_int(breadth.get("flat_count"))
    complete = (
        cash.get("status") == "ready"
        and cash.get("cash_session_open") is True
        and all(value is not None for value in relative_values)
        and dispersion is not None
        and up_count is not None
        and down_count is not None
        and flat_count is not None
        and up_count + down_count + flat_count == 4
    )
    payload: dict[str, object] = {
        "status": "available" if complete else "degraded",
        "source": "cash_index",
        "relative_to_spx_15m_bps": {
            instrument: relative.get(instrument)
            for instrument in ("index:SPX", "index:NDX", "index:DJI", "index:RUT")
        },
        "dispersion_15m_bps": dispersion,
        "breadth_15m": {
            "up_count": up_count,
            "down_count": down_count,
            "flat_count": flat_count,
        },
        "missing_instruments": list(cash.get("missing_instruments") or ()),
        "reason_codes": list(cash.get("reason_codes") or ()),
        "semantics": "observed_cash_index_price_regime_not_market_maker_behavior",
    }
    return _finish_cross_index_score(
        payload,
        complete=complete,
        relative_values=relative_values,
        dispersion=dispersion,
        up_count=up_count,
        down_count=down_count,
    )


def _globex_index_component(
    selected: Mapping[str, object],
) -> tuple[float | None, dict[str, object]]:
    relative = _mapping(selected.get("relative_to_anchor_15m_bps"))
    breadth = _mapping(selected.get("breadth_15m"))
    relative_values = [
        _number(relative.get(instrument))
        for instrument in ("future:NQ", "future:YM", "future:RTY")
    ]
    dispersion = _number(selected.get("dispersion_15m_bps"))
    up_count = _non_negative_int(breadth.get("up_count"))
    down_count = _non_negative_int(breadth.get("down_count"))
    flat_count = _non_negative_int(breadth.get("flat_count"))
    complete = (
        selected.get("status") == "ready"
        and selected.get("session_open") is True
        and all(value is not None for value in relative_values)
        and dispersion is not None
        and up_count is not None
        and down_count is not None
        and flat_count is not None
        and up_count + down_count + flat_count == 4
    )
    payload: dict[str, object] = {
        "status": "available" if complete else "degraded",
        "source": "globex_index",
        "anchor": "future:ES",
        "relative_to_es_15m_bps": {
            instrument: relative.get(instrument)
            for instrument in ("future:ES", "future:NQ", "future:YM", "future:RTY")
        },
        "dispersion_15m_bps": dispersion,
        "breadth_15m": {
            "up_count": up_count,
            "down_count": down_count,
            "flat_count": flat_count,
        },
        "missing_instruments": list(selected.get("missing_instruments") or ()),
        "reason_codes": list(selected.get("reason_codes") or ()),
        "calibration": selected.get("calibration"),
        "semantics": "observed_globex_futures_relative_to_es_not_cash_index",
    }
    return _finish_cross_index_score(
        payload,
        complete=complete,
        relative_values=relative_values,
        dispersion=dispersion,
        up_count=up_count,
        down_count=down_count,
    )


def _finish_cross_index_score(
    payload: dict[str, object],
    *,
    complete: bool,
    relative_values: list[float | None],
    dispersion: float | None,
    up_count: int | None,
    down_count: int | None,
) -> tuple[float | None, dict[str, object]]:
    if not complete:
        payload["score"] = None
        return None, payload
    resolved_relative = [float(value) for value in relative_values if value is not None]
    assert dispersion is not None and up_count is not None and down_count is not None
    breadth_score = (up_count - down_count) / 4.0
    relative_score = max(
        -1.0,
        min(1.0, math.fsum(resolved_relative) / len(resolved_relative) / 25.0),
    )
    dispersion_penalty = max(0.0, min(1.0, dispersion / 50.0))
    score = max(
        -2.0,
        min(
            2.0,
            breadth_score * (1.0 - 0.25 * dispersion_penalty) + 0.25 * relative_score,
        ),
    )
    payload.update(
        {
            "score": round(score, 10),
            "breadth_score": round(breadth_score, 10),
            "relative_strength_score": round(relative_score, 10),
            "dispersion_penalty": round(dispersion_penalty, 10),
        }
    )
    return score, payload


def _prior_rth_component(
    prior: Mapping[str, object],
    *,
    session_day: date | None,
) -> tuple[float | None, dict[str, object]]:
    cross = _mapping(prior.get("cross_index"))
    returns = _mapping(cross.get("return_bps"))
    values = [
        _number(returns.get(instrument))
        for instrument in ("index:SPX", "index:NDX", "index:DJI", "index:RUT")
    ]
    available_return_count = sum(value is not None for value in values)
    breadth = _mapping(cross.get("breadth"))
    dispersion = _number(cross.get("return_dispersion_bps"))
    date_matches = (
        session_day is not None and prior.get("for_trading_date") == session_day.isoformat()
    )
    complete = (
        prior.get("schema_version") == "prior_rth_context.v2"
        and prior.get("status") in {"ready", "partial"}
        and cross.get("status") in {"ready", "partial"}
        and date_matches
        and available_return_count == 4
    )
    reason_codes = list(cross.get("reason_codes") or prior.get("reasons") or ())
    if available_return_count < 4:
        reason_codes.append("prior_rth_returns_incomplete")
    payload: dict[str, object] = {
        "status": "available" if complete else "degraded",
        "schema_version": prior.get("schema_version"),
        "for_trading_date": prior.get("for_trading_date"),
        "return_bps": {
            instrument: returns.get(instrument)
            for instrument in ("index:SPX", "index:NDX", "index:DJI", "index:RUT")
        },
        "return_dispersion_bps": dispersion,
        "breadth": breadth,
        "reason_codes": sorted(set(reason_codes)),
        "semantics": "observed_prior_rth_cash_index_regime_not_market_maker_behavior",
    }
    if not complete:
        payload["score"] = None
        return None, payload
    resolved = [float(value) for value in values if value is not None]
    up_count = sum(value > 0.0 for value in resolved)
    down_count = sum(value < 0.0 for value in resolved)
    breadth_score = (up_count - down_count) / len(resolved)
    mean_return_score = math.fsum(math.tanh(value / 75.0) for value in resolved) / len(resolved)
    dispersion_penalty = max(0.0, min(1.0, dispersion / 100.0)) if dispersion is not None else 0.0
    score = max(
        -2.0,
        min(
            2.0,
            0.75 * mean_return_score + 0.25 * breadth_score * (1.0 - dispersion_penalty),
        ),
    )
    payload.update(
        {
            "score": round(score, 10),
            "mean_return_score": round(mean_return_score, 10),
            "breadth_score": round(breadth_score, 10),
            "dispersion_penalty": round(dispersion_penalty, 10),
        }
    )
    return score, payload


def _expected_move(options: Mapping[str, object]) -> float | None:
    value = _number(_mapping(options.get("volatility")).get("expected_move_points_0dte"))
    return value if value is not None and value > 0.0 else None


def _mapping(value: object) -> dict[str, object]:
    return dict(value) if isinstance(value, Mapping) else {}


def _number(value: object) -> float | None:
    if isinstance(value, bool) or not isinstance(value, int | float):
        return None
    parsed = float(value)
    return parsed if math.isfinite(parsed) else None


def _non_negative_int(value: object) -> int | None:
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        return None
    return value


def _canonical_hash(value: object) -> str:
    encoded = json.dumps(value, allow_nan=False, separators=(",", ":"), sort_keys=True).encode()
    return "sha256:" + hashlib.sha256(encoded).hexdigest()


__all__ = [
    "CROSS_INDEX_FEATURE_SET_VERSION",
    "ES_FEATURE_WEIGHTS",
    "FEATURE_SCHEMA_VERSION",
    "OBSERVATION_COMPONENT_WEIGHTS",
    "build_feature_observation",
]
