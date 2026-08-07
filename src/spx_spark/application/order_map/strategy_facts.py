"""Point-in-time fact assembly for the unified strategy decision."""

from __future__ import annotations

from collections.abc import Mapping
from datetime import datetime, timezone
from typing import Any

from spx_spark.market_calendar import DEFAULT_MARKET_CALENDAR
from spx_spark.storage import LatestState


def build_market_fact_pack(
    payload: Mapping[str, Any], latest: LatestState, now: datetime
) -> dict[str, Any]:
    decision_at, lineage = _utc(now), []
    market = _frame(payload, "minute_market_frame", decision_at, lineage)
    option = _frame(payload, "option_structure_frame", decision_at, lineage)
    underlier, es = _map(payload.get("underlier")), _map(market.get("es"))
    rth = _map(_map(_map(market.get("diagnostics")).get("rth_market_state")))
    rth_lineage = _map(rth.get("input_lineage"))
    values, diagnostics = _map(rth_lineage.get("values")), _map(rth_lineage.get("diagnostics"))
    opening, averages = _map(diagnostics.get("opening_range")), _map(diagnostics.get("moving_averages"))
    structure, density = _map(option.get("structure")), _map(option.get("density"))
    volatility = _map(option.get("volatility"))
    market_volatility, volume = _map(market.get("volatility")), _map(market.get("volume"))
    macro = _map(payload.get("macro_event"))
    trigger, gth = _map(payload.get("level_decision")), _map(payload.get("gth_level_manual_candidate"))
    forecast = _map(payload.get("strategy_distribution_forecast"))
    q_event, p_event = _map(forecast.get("q_event")), _map(forecast.get("p_event"))
    trading_date = str(payload.get("trading_date") or "")
    spx, es_price = _number(underlier.get("price")), _first(es.get("price"), payload.get("es_last"))
    basis = es_price - spx if es_price is not None and spx is not None else None
    l1, quality = _map(option.get("l1")), list(lineage)
    if spx is None:
        quality.append("spx_price_unavailable")
    if payload.get("pricing_allowed") is not True:
        quality.append("pricing_not_authorized")
    for label, status in (
        ("market_frame", market.get("quality")),
        ("option_frame", option.get("quality")),
        ("option_l1", l1.get("quality")),
    ):
        if status != "ready":
            quality.append(f"{label}_not_ready")
    source_times = [item for item in (
        _past_time(latest.as_of, decision_at, "latest_state", lineage),
        _frame_time(market), _frame_time(option),
    ) if item]
    quality = list(dict.fromkeys([*quality, *lineage]))
    es_vwap = _number(es.get("vwap"))
    return {
        "schema_version": "market_fact_pack.v1",
        "decision_at": decision_at.isoformat(),
        "available_at": max(source_times, default=decision_at).isoformat(),
        "session_date": trading_date or None,
        "minutes_to_close": _minutes_to_close(trading_date, decision_at),
        "spot": {"spx": spx, "es": es_price, "es_spx_basis": basis,
                 "pricing_source": underlier.get("source")},
        "path": {
            "market_state": rth.get("market_state") or rth.get("state"),
            "direction_score": _number(rth.get("D")),
            "efficiency_ratio_30m": _first(values.get("efficiency_ratio"), es.get("trend_efficiency_30m")),
            "vwap_crosses_30m": _number(values.get("vwap_cross_count")),
            "price_vs_vwap": values.get("price_vs_vwap"),
            "vwap_slope": _first(values.get("vwap_slope"), es.get("vwap_slope_15m_points")),
            "breadth_above_vwap": _number(values.get("breadth_above_vwap")),
            "opening_range_state": values.get("opening_range_state"),
            "opening_range_high": _number(opening.get("orh")),
            "opening_range_low": _number(opening.get("orl")),
            "distance_to_vwap_points": _number(es.get("vwap_distance_points")),
            "impulse_15m_points": _number(es.get("return_15m_points")),
            "return_60m_points": _number(es.get("return_60m_points")),
            "atr_5m": _number(averages.get("atr_5m")),
            "pin_path_spx": [
                float(value) - basis for value in es.get("pin_path_1m") or ()
                if isinstance(value, int | float) and basis is not None
            ],
        },
        "value_center": {
            "es_vwap": es_vwap, "spx_equivalent": es_vwap - basis if es_vwap is not None and basis is not None else None,
            **{f"spx_{window}": float(value) - basis
               for window, value in _map(volume.get("value_centers_es")).items()
               if isinstance(value, int | float) and basis is not None},
        },
        "volatility": {
            "vix": _number(volatility.get("vix")), "vix1d": _number(volatility.get("vix1d")),
            "atm_iv_0dte": _number(volatility.get("atm_iv_0dte")),
            "atm_iv_change_5m": _number(volatility.get("atm_iv_change_5m")),
            "atm_iv_change_15m": _number(volatility.get("atm_iv_change_15m")),
            "expected_move_points": _first(volatility.get("expected_move_points_0dte"),
                                            payload.get("expected_move_points")),
            "vix_return_15m_pct": _number(market_volatility.get("vix_return_15m_pct")),
            "atm_straddle_decay_15m": _number(volatility.get("atm_straddle_decay_15m")),
        },
        "structure": {
            "zero_gamma": _first(structure.get("zero_gamma"), payload.get("zero_gamma")),
            "flip_zone": structure.get("flip_zone") or payload.get("flip_zone"),
            "put_wall": _number(structure.get("put_wall")),
            "call_wall": _number(structure.get("call_wall")),
            "q_median": _number(density.get("median")),
            "q_mode": _number(density.get("mode")),
            "q_local_mass_5pt": dict(_map(density.get("local_mass_5pt"))),
            "q_clipped_mass_fraction": _number(density.get("clipped_mass_fraction")),
            "gex_quality": structure.get("gex_quality"),
            "zero_gamma_migration_points": _number(structure.get("zero_gamma_migration_points")),
        },
        "event": {"state": macro.get("mode") or "unavailable",
                  "entry_allowed": macro.get("entry_allowed") is True,
                  "active_event": macro.get("active_event"), "next_event": macro.get("next_event")},
        "trigger": {"phase": trigger.get("phase"), "direction": trigger.get("direction"),
                    "thesis": trigger.get("thesis"), "level_kind": trigger.get("level_kind"),
                    "level": _number(trigger.get("level")),
                    "levels": dict(_map(trigger.get("levels"))), "event_id": trigger.get("event_id")},
        "gth_evidence": {
            "status": gth.get("status"), "evaluated_at": gth.get("evaluated_at"),
            "direction": gth.get("direction"), "path_kind": gth.get("path_kind"),
            "manual_action_eligible": gth.get("manual_action_eligible") is True,
            "execution_eligible": gth.get("execution_eligible") is True,
            "block_reasons": list(gth.get("block_reasons") or ()),
            "long_contract_id": gth.get("long_contract_id"), "short_contract_id": gth.get("short_contract_id"),
            "trigger_level": _number(gth.get("trigger_level")),
            "current_spx": _first(gth.get("current_parity_spx"), gth.get("current_spx")),
            "target_spx": _number(gth.get("target_spx")),
            "invalidation_spx": _number(gth.get("invalidation_spx")),
            "valid_until": gth.get("valid_until"), "exit_at": gth.get("exit_at"),
            "exact_spread_snapshot": gth.get("exact_spread_snapshot"),
        },
        "probability": {
            "q": q_event.get("probability"), "p_empirical": p_event.get("probability"),
            "p_interval_low": p_event.get("interval_low"),
            "n_raw": p_event.get("n_raw") or p_event.get("sample_count") or 0,
            "n_effective": p_event.get("n_effective") or 0.0,
            "historical_sessions": list(p_event.get("historical_sessions") or ()),
            "event": q_event.get("event"), "valid_until": forecast.get("valid_until"),
            "quality": forecast.get("quality") or "unavailable",
        },
        "quality": {"status": "ready" if not quality else "degraded", "reasons": quality,
                    "market": market.get("quality") or "unavailable",
                    "options": option.get("quality") or "unavailable",
                    "l1": l1.get("quality") or "unavailable"},
    }


def _frame(payload: Mapping[str, Any], key: str, now: datetime, reasons: list[str]) -> Mapping[str, Any]:
    frame = _map(payload.get(key))
    observed = _frame_time(frame)
    if not frame:
        reasons.append(f"{key}_missing")
    elif observed is None:
        reasons.append(f"{key}_lineage_missing")
    elif observed > now:
        reasons.append(f"{key}_from_future")
    else:
        return frame
    return {}


def _past_time(value: object, now: datetime, label: str, reasons: list[str]) -> datetime | None:
    parsed = _time(value)
    if parsed is None or parsed > now:
        reasons.append(f"{label}_{'from_future' if parsed else 'lineage_missing'}")
        return None
    return parsed


def _frame_time(frame: Mapping[str, Any]) -> datetime | None:
    return _time(frame.get("available_at") or frame.get("as_of"))


def _minutes_to_close(trading_date: str, now: datetime) -> int | None:
    try:
        session = DEFAULT_MARKET_CALENDAR.session(datetime.fromisoformat(trading_date).date())
    except ValueError:
        return None
    return max(int((session.close_at - now).total_seconds() // 60), 0) if session else None


def _map(value: object) -> Mapping[str, Any]:
    return value if isinstance(value, Mapping) else {}


def _number(value: object) -> float | None:
    return float(value) if isinstance(value, int | float) else None


def _first(*values: object) -> float | None:
    return next((number for value in values if (number := _number(value)) is not None), None)


def _time(value: object) -> datetime | None:
    if isinstance(value, datetime):
        parsed = value
    elif isinstance(value, str):
        try:
            parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError:
            return None
    else:
        return None
    return parsed.astimezone(timezone.utc) if parsed.tzinfo else None


def _utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        raise ValueError("strategy decision time must be timezone-aware")
    return value.astimezone(timezone.utc)
