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
    coordinate = _map(payload.get("trigger_coordinate"))
    underlier = _map(payload.get("spot")) or _map(payload.get("underlier"))
    es = _map(market.get("es"))
    rth = _map(_map(_map(market.get("diagnostics")).get("rth_market_state")))
    rth_lineage = _map(rth.get("input_lineage"))
    values, diagnostics = _map(rth_lineage.get("values")), _map(rth_lineage.get("diagnostics"))
    opening, averages = _map(diagnostics.get("opening_range")), _map(diagnostics.get("moving_averages"))
    structure, density = _map(option.get("structure")), _map(option.get("density"))
    volatility = _map(option.get("volatility"))
    market_volatility, volume = _map(market.get("volatility")), _map(market.get("volume"))
    macro = _map(payload.get("macro_event"))
    trigger = _map(payload.get("level_decision"))
    gth_level = _map(payload.get("gth_level_manual_candidate"))
    gth_dip_reclaim = _map(payload.get("gth_dip_reclaim_evidence"))
    episode = _map(payload.get("session_episode"))
    provider_control = _map(
        payload.get("strategy_entry_control") or payload.get("provider_entry_control")
    )
    forecast = _map(payload.get("strategy_distribution_forecast"))
    q_event, p_event = _map(forecast.get("q_event")), _map(forecast.get("p_event"))
    trading_date = str(payload.get("trading_date") or "")
    spx = _first(
        underlier.get("spx_observed_value"),
        underlier.get("price"),
        coordinate.get("spx_observed_value"),
    )
    es_price = _first(es.get("price"), payload.get("es_last"))
    basis = es_price - spx if es_price is not None and spx is not None else None
    coordinate_basis = _first(
        underlier.get("basis"),
        underlier.get("basis_points"),
        coordinate.get("basis_points"),
    )
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
    session_mode = (
        "rth"
        if DEFAULT_MARKET_CALENDAR.is_rth_open(decision_at)
        else "gth"
        if DEFAULT_MARKET_CALENDAR.is_spx_gth_open(decision_at)
        else "closed"
    )
    coordinate_ready = spx is not None
    market_ready = market.get("quality") == "ready"
    atr_ready = _number(averages.get("atr_5m")) is not None
    vwap_ready = any(
        value is not None
        for value in (
            values.get("price_vs_vwap"),
            es.get("vwap"),
            es.get("vwap_distance_points"),
        )
    )
    opening_range_ready = any(
        value is not None
        for value in (
            values.get("opening_range_state"),
            opening.get("orh"),
            opening.get("orl"),
        )
    )
    path_ready = market_ready and atr_ready and vwap_ready
    structure_ready = (
        option.get("quality") == "ready"
        and l1.get("quality") == "ready"
        and structure.get("gex_quality")
        in {None, "", "open_interest_gex", "ibkr_ok"}
    )
    value_center_ready = all(
        _number(_map(volume.get("value_centers_es")).get(window)) is not None
        for window in ("15m", "30m", "60m")
    )
    pin_inputs_ready = bool(
        value_center_ready
        and _number(density.get("mode")) is not None
        and _map(density.get("local_mass_5pt"))
        and _number(volatility.get("atm_straddle_decay_15m")) is not None
    )
    macro_allowed = macro.get("entry_allowed") is True
    provider_allowed = provider_control.get("allowed") is not False
    session_legal = session_mode != "closed"
    global_reasons = []
    if not session_legal:
        global_reasons.append("session_not_open_for_spxw_strategy")
    if not coordinate_ready:
        global_reasons.append("spx_price_unavailable")
    if not market_ready:
        global_reasons.append(
            next(
                (
                    reason
                    for reason in quality
                    if reason.startswith("minute_market_frame_")
                ),
                "market_frame_not_ready",
            )
        )
    if not macro_allowed:
        global_reasons.append("macro_entry_not_authorized")
    if not provider_allowed:
        global_reasons.append(
            str(provider_control.get("reason") or "provider_advice_not_authorized")
        )
    vertical_reasons = []
    if not coordinate_ready:
        vertical_reasons.append("spx_price_unavailable")
    if not path_ready:
        vertical_reasons.append("vertical_path_inputs_unavailable")
    butterfly_reasons = list(vertical_reasons)
    if not structure_ready:
        butterfly_reasons.append("butterfly_structure_capability_unavailable")
    if not pin_inputs_ready:
        butterfly_reasons.append("butterfly_value_center_or_density_unavailable")
    episode_phase = str(episode.get("phase") or "").strip().lower() or None
    break_direction = str(episode.get("break_direction") or "").strip().lower() or None
    episode_setup_direction = (
        "UP"
        if episode_phase in {"v_reversal_confirmed", "recovery"}
        and break_direction == "down"
        else "DOWN"
        if episode_phase in {"v_reversal_confirmed", "recovery"}
        and break_direction == "up"
        else None
    )
    return {
        "schema_version": "market_fact_pack.v1",
        "decision_at": decision_at.isoformat(),
        "available_at": max(source_times, default=decision_at).isoformat(),
        "session_date": trading_date or None,
        "minutes_to_close": _minutes_to_close(trading_date, decision_at),
        "session": {"mode": session_mode, "legal": session_legal},
        "spot": {
            "spx": spx,
            "spx_observed_value": spx,
            "observed_value": _first(
                underlier.get("observed_value"), coordinate.get("observed_value"), spx
            ),
            "es": es_price,
            "es_spx_basis": basis,
            "basis": coordinate_basis,
            "kind": underlier.get("kind") or coordinate.get("kind"),
            "source": underlier.get("source") or coordinate.get("source"),
            "pricing_source": underlier.get("source") or coordinate.get("source"),
        },
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
            "strike_differential_context": dict(
                _map(density.get("strike_differential_context"))
            ),
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
        "session_episode": {
            "phase": episode_phase,
            "break_direction": break_direction,
            "break_level": _number(episode.get("break_level")),
            "break_level_kind": episode.get("break_level_kind"),
            "reversal_direction": episode.get("reversal_direction"),
            "setup_direction": episode_setup_direction,
            "episode_id": episode.get("episode_id"),
            "phase_at": episode.get("phase_at"),
        },
        "gth_evidence": _gth_fact(gth_level),
        "gth_dip_reclaim_evidence": _gth_fact(gth_dip_reclaim),
        "probability": {
            "q": q_event.get("probability"), "p_empirical": p_event.get("probability"),
            "p_interval_low": p_event.get("interval_low"),
            "n_raw": p_event.get("n_raw") or p_event.get("sample_count") or 0,
            "n_effective": p_event.get("n_effective") or 0.0,
            "historical_sessions": list(p_event.get("historical_sessions") or ()),
            "event": q_event.get("event"), "valid_until": forecast.get("valid_until"),
            "quality": forecast.get("quality") or "unavailable",
        },
        "capabilities": {
            "global": {
                "ready": not global_reasons,
                "session_legal": session_legal,
                "coordinate_ready": coordinate_ready,
                "market_frame_ready": market_ready,
                "macro_entry_allowed": macro_allowed,
                "provider_advice_allowed": provider_allowed,
                "reasons": global_reasons,
            },
            "path": {
                "ready": path_ready,
                "vwap_ready": vwap_ready,
                "opening_range_ready": opening_range_ready,
                "atr_ready": atr_ready,
            },
            "vertical": {
                "ready": coordinate_ready and path_ready,
                "setup_inputs_ready": coordinate_ready and path_ready,
                "exact_quote_requirement": "candidate_specific_two_leg_bbo",
                "pricing_frame_authorized": payload.get("pricing_allowed") is True,
                "reasons": vertical_reasons,
            },
            "butterfly": {
                "ready": coordinate_ready
                and path_ready
                and structure_ready
                and pin_inputs_ready,
                "structure_ready": structure_ready,
                "value_center_density_ready": pin_inputs_ready,
                "exact_quote_requirement": "candidate_specific_three_leg_bbo",
                "reasons": list(dict.fromkeys(butterfly_reasons)),
            },
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


def _gth_fact(evidence: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "status": evidence.get("status"),
        "evaluated_at": evidence.get("evaluated_at"),
        "source_kind": evidence.get("source_kind"),
        "direction": evidence.get("direction"),
        "path_kind": evidence.get("path_kind"),
        "candidate_scope": evidence.get("candidate_scope"),
        "execution_mode": evidence.get("execution_mode"),
        "manual_action_eligible": evidence.get("manual_action_eligible") is True,
        "selector_evidence_eligible": evidence.get("selector_evidence_eligible") is True,
        "operator_notification_eligible": (
            evidence.get("operator_notification_eligible") is True
        ),
        "execution_eligible": evidence.get("execution_eligible") is True,
        "edge_authority": evidence.get("edge_authority"),
        "edge_authority_required": evidence.get("edge_authority_required"),
        "edge_authority_reason": evidence.get("edge_authority_reason"),
        "signal_absence_reason": evidence.get("signal_absence_reason"),
        "block_reasons": list(evidence.get("block_reasons") or ()),
        "long_contract_id": evidence.get("long_contract_id"),
        "short_contract_id": evidence.get("short_contract_id"),
        "trigger_level": _number(evidence.get("trigger_level")),
        "current_spx": _first(evidence.get("current_parity_spx"), evidence.get("current_spx")),
        "target_spx": _number(evidence.get("target_spx")),
        "invalidation_spx": _number(evidence.get("invalidation_spx")),
        "valid_until": evidence.get("valid_until"),
        "exit_at": evidence.get("exit_at"),
        "exact_spread_snapshot": evidence.get("exact_spread_snapshot"),
    }


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
