"""Point-in-time fact assembly for the unified strategy decision."""

from __future__ import annotations

from collections.abc import Mapping
from datetime import datetime, timezone
from typing import Any

from spx_spark.market_calendar import DEFAULT_MARKET_CALENDAR
from spx_spark.storage import LatestState


def build_market_fact_pack(
    payload: Mapping[str, Any],
    latest: LatestState,
    now: datetime,
) -> dict[str, Any]:
    decision_at = _utc(now)
    lineage_reasons: list[str] = []
    market = _point_in_time_frame(payload, "minute_market_frame", decision_at, lineage_reasons)
    option = _point_in_time_frame(payload, "option_structure_frame", decision_at, lineage_reasons)
    underlier = _mapping(payload.get("underlier"))
    es = _mapping(market.get("es"))
    diagnostics = _mapping(market.get("diagnostics"))
    rth_state = _mapping(diagnostics.get("rth_market_state"))
    rth_lineage = _mapping(rth_state.get("input_lineage"))
    rth_values = _mapping(rth_lineage.get("values"))
    rth_diagnostics = _mapping(rth_lineage.get("diagnostics"))
    opening_range = _mapping(rth_diagnostics.get("opening_range"))
    structure = _mapping(option.get("structure"))
    density = _mapping(option.get("density"))
    volatility = _mapping(option.get("volatility"))
    cross_asset = _mapping(market.get("cross_asset"))
    returns = _mapping(cross_asset.get("returns"))
    macro = _mapping(payload.get("macro_event"))
    trading_date = str(payload.get("trading_date") or "")

    spx = _number(underlier.get("price"))
    es_price = _first_number(es.get("price"), payload.get("es_last"))
    basis = es_price - spx if es_price is not None and spx is not None else None
    quality_reasons = list(lineage_reasons)
    if spx is None:
        quality_reasons.append("spx_price_unavailable")
    if payload.get("pricing_allowed") is not True:
        quality_reasons.append("pricing_not_authorized")
    if market.get("quality") != "ready":
        quality_reasons.append("market_frame_not_ready")
    if option.get("quality") != "ready":
        quality_reasons.append("option_frame_not_ready")
    l1 = _mapping(option.get("l1"))
    if l1.get("quality") != "ready":
        quality_reasons.append("option_l1_not_ready")
    quality_reasons = list(dict.fromkeys(quality_reasons))

    source_times = [
        value
        for value in (
            _point_in_time(latest.as_of, decision_at, "latest_state", lineage_reasons),
            _frame_time(market),
            _frame_time(option),
        )
        if value is not None
    ]
    available_at = max(source_times, default=decision_at)
    if lineage_reasons:
        quality_reasons = list(dict.fromkeys([*quality_reasons, *lineage_reasons]))

    return {
        "schema_version": "market_fact_pack.v1",
        "decision_at": decision_at.isoformat(),
        "available_at": available_at.isoformat(),
        "session_date": trading_date or None,
        "session_phase": payload.get("session_phase"),
        "minutes_to_close": _minutes_to_close(trading_date, decision_at),
        "spot": {
            "spx": spx,
            "es": es_price,
            "es_spx_basis": basis,
            "pricing_source": underlier.get("source"),
        },
        "path": {
            "market_state": rth_state.get("market_state") or rth_state.get("state"),
            "direction_score": _number(rth_state.get("D")),
            "efficiency_ratio_30m": _first_number(
                rth_values.get("efficiency_ratio"), es.get("trend_efficiency_60m")
            ),
            "vwap_crosses_30m": _number(rth_values.get("vwap_cross_count")),
            "price_vs_vwap": rth_values.get("price_vs_vwap"),
            "vwap_slope": _first_number(
                rth_values.get("vwap_slope"), es.get("vwap_slope_15m_points")
            ),
            "breadth_above_vwap": _number(rth_values.get("breadth_above_vwap")),
            "opening_range_state": rth_values.get("opening_range_state"),
            "opening_range_high": _number(opening_range.get("orh")),
            "opening_range_low": _number(opening_range.get("orl")),
            "distance_to_vwap_points": _number(es.get("vwap_distance_points")),
            "impulse_15m_points": _number(es.get("return_15m_points")),
            "return_60m_points": _number(es.get("return_60m_points")),
        },
        "value_center": {
            "es_vwap": _number(es.get("vwap")),
            "spx_equivalent": (
                _number(es.get("vwap")) - basis
                if _number(es.get("vwap")) is not None and basis is not None
                else None
            ),
        },
        "cross_section": {
            "returns": dict(returns),
            "es_spy_confirmation": cross_asset.get("es_spy_direction_confirmation_15m"),
            "cancellation_score": None,
        },
        "volatility": {
            "vix": _number(volatility.get("vix")),
            "vix1d": _number(volatility.get("vix1d")),
            "atm_iv_0dte": _number(volatility.get("atm_iv_0dte")),
            "atm_iv_change_5m": _number(volatility.get("atm_iv_change_5m")),
            "atm_iv_change_15m": _number(volatility.get("atm_iv_change_15m")),
            "expected_move_points": _first_number(
                volatility.get("expected_move_points_0dte"), payload.get("expected_move_points")
            ),
        },
        "structure": {
            "zero_gamma": _first_number(structure.get("zero_gamma"), payload.get("zero_gamma")),
            "flip_zone": structure.get("flip_zone") or payload.get("flip_zone"),
            "put_wall": _number(structure.get("put_wall")),
            "call_wall": _number(structure.get("call_wall")),
            "q_median": _number(density.get("median")),
            "q_clipped_mass_fraction": _number(density.get("clipped_mass_fraction")),
            "gex_quality": structure.get("gex_quality"),
        },
        "event": {
            "state": macro.get("mode") or "unavailable",
            "entry_allowed": macro.get("entry_allowed") is True,
            "active_event": macro.get("active_event"),
            "next_event": macro.get("next_event"),
        },
        "quality": {
            "status": "ready" if not quality_reasons else "degraded",
            "reasons": quality_reasons,
            "market": market.get("quality") or "unavailable",
            "options": option.get("quality") or "unavailable",
            "l1": l1.get("quality") or "unavailable",
        },
        "legacy_candidates": [
            {
                "play": row.get("play"),
                "level": row.get("level"),
                "contract_id": row.get("contract_id"),
            }
            for row in payload.get("candidates") or ()
            if isinstance(row, Mapping)
        ],
    }


def _point_in_time_frame(
    payload: Mapping[str, Any],
    key: str,
    now: datetime,
    reasons: list[str],
) -> Mapping[str, Any]:
    frame = _mapping(payload.get(key))
    if not frame:
        reasons.append(f"{key}_missing")
        return {}
    observed = _parse_time(frame.get("available_at") or frame.get("as_of"))
    if observed is None:
        reasons.append(f"{key}_lineage_missing")
        return {}
    if observed > now:
        reasons.append(f"{key}_from_future")
        return {}
    return frame


def _point_in_time(
    value: object, now: datetime, label: str, reasons: list[str]
) -> datetime | None:
    parsed = _parse_time(value)
    if parsed is None or parsed > now:
        reasons.append(f"{label}_from_future" if parsed else f"{label}_lineage_missing")
        return None
    return parsed


def _frame_time(frame: Mapping[str, Any]) -> datetime | None:
    return _parse_time(frame.get("available_at") or frame.get("as_of"))


def _minutes_to_close(trading_date: str, now: datetime) -> int | None:
    try:
        session = DEFAULT_MARKET_CALENDAR.session(datetime.fromisoformat(trading_date).date())
    except ValueError:
        return None
    if session is None:
        return None
    return max(int((session.close_at - now).total_seconds() // 60), 0)


def _mapping(value: object) -> Mapping[str, Any]:
    return value if isinstance(value, Mapping) else {}


def _number(value: object) -> float | None:
    return float(value) if isinstance(value, int | float) else None


def _first_number(*values: object) -> float | None:
    for value in values:
        number = _number(value)
        if number is not None:
            return number
    return None


def _parse_time(value: object) -> datetime | None:
    if isinstance(value, datetime):
        parsed = value
    elif isinstance(value, str):
        try:
            parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError:
            return None
    else:
        return None
    if parsed.tzinfo is None:
        return None
    return parsed.astimezone(timezone.utc)


def _utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        raise ValueError("strategy decision time must be timezone-aware")
    return value.astimezone(timezone.utc)
