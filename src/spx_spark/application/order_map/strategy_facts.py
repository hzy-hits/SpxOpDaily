"""Point-in-time fact assembly for the unified strategy decision."""

from __future__ import annotations

from collections.abc import Mapping
from datetime import datetime, timedelta, timezone
from typing import Any

from spx_spark.application.order_map.strategy_regime import (
    DEFAULT_STRATEGY_POLICY,
    StrategyPolicy,
    pin_stable_center,
)
from spx_spark.market_calendar import DEFAULT_MARKET_CALENDAR
from spx_spark.storage import LatestState


_DENOISING_FORWARD_CONTRACT_HASH = (
    "sha256:fc276ff1d44bf4a150ff18889c445a6eaa68b12131b93b4c191765617fc1fb27"
)
_WALL_HAZARD_CONTRACT_HASH = (
    "sha256:ff0e0d1204b97af334ec3d65679bc0dcfdb9e4b3084912e650af6caef05494a2"
)


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
    shock_state = _map(payload.get("intraday_shock_state"))
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
    coordinate_basis = _first(
        underlier.get("basis"),
        underlier.get("basis_points"),
        coordinate.get("basis_points"),
    )
    spot_kind = underlier.get("kind") or coordinate.get("kind")
    spot_source = underlier.get("source") or coordinate.get("source")
    if spx is None and es_price is not None and coordinate_basis is not None:
        # Resolver already intended ES-equivalent when official/parity SPX is
        # briefly unactionable; reuse the same-cycle market-frame ES + basis
        # rather than fail-closed on a stale cash print.
        spx = es_price - coordinate_basis
        if not spot_kind or spot_kind == "unavailable":
            spot_kind = "es_equivalent"
            spot_source = f"future:ES+basis:{coordinate_basis:.4f}"
    basis = es_price - spx if es_price is not None and spx is not None else None
    l1, quality = _map(option.get("l1")), list(lineage)
    if spx is None:
        quality.append("spx_price_unavailable")
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
    atr = _first(
        averages.get("atr_5m"),
        _map(diagnostics.get("rolling_path_percentiles")).get("atr_5m"),
        _map(diagnostics.get("atr")).get("value"),
    )
    atr_ready = atr is not None
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
    trend_evaluable = all(
        value is not None
        for value in (
            _number(rth.get("D")),
            _first(values.get("efficiency_ratio"), es.get("trend_efficiency_30m")),
            _number(values.get("vwap_cross_count")),
            _number(values.get("breadth_above_vwap")),
            _first(values.get("vwap_slope"), es.get("vwap_slope_15m_points")),
        )
    )
    # OI-GEX is a scoring input, not a butterfly capability gate. v2 §11.4
    # drops the gamma-alignment term when GEX quality is unavailable; Pin
    # already fail-closes if walls/Q/VC cannot align. Missing OI must not
    # veto an otherwise ready PIN_STABLE butterfly.
    structure_ready = (
        option.get("quality") == "ready" and l1.get("quality") == "ready"
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
        and _number(market_volatility.get("vix_return_15m_pct")) is not None
        and _number(values.get("breadth_above_vwap")) is not None
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
    opening_high = _spx_level(opening.get("orh"), basis)
    opening_low = _spx_level(opening.get("orl"), basis)
    rth_bars = _spx_bars(diagnostics.get("rth_bar_path"), basis)
    rth_bar_vwaps = {
        str(key): adjusted
        for key, value in _map(diagnostics.get("rth_bar_vwaps")).items()
        if (adjusted := _spx_level(value, basis)) is not None
    }
    es_volume = dict(_map(payload.get("es_volume")))
    rth_setups = _rth_setup_facts(
        rth_bars,
        rth_bar_vwaps,
        opening_high=opening_high,
        opening_low=opening_low,
        market_structure=str(values.get("market_structure") or ""),
        episode_phase=episode_phase,
        episode_direction=episode_setup_direction,
        episode_level=_number(episode.get("break_level")),
        episode_id=episode.get("episode_id"),
        episode_phase_at=episode.get("phase_at"),
        spot=spx,
        call_wall=_number(structure.get("call_wall")),
        put_wall=_number(structure.get("put_wall")),
        policy=DEFAULT_STRATEGY_POLICY,
    )
    momentum_setup = _es_volume_momentum_setup(
        es_volume,
        return_1m=_number(es.get("return_1m_points")),
        return_5m=_number(es.get("return_5m_points")),
        atr=atr,
        spot=spx,
        now=decision_at,
        policy=DEFAULT_STRATEGY_POLICY,
    )
    if momentum_setup:
        rth_setups.append(momentum_setup)
    preaverage_setup = _preaverage_pullback_setup(
        payload,
        decision_at=decision_at,
        session_date=trading_date,
    )
    if preaverage_setup:
        rth_setups.append(preaverage_setup)
    wall_hazard_setup = _wall_hazard_setup(
        payload,
        decision_at=decision_at,
        session_date=trading_date,
        policy=DEFAULT_STRATEGY_POLICY,
    )
    if wall_hazard_setup:
        rth_setups.append(wall_hazard_setup)
    failed_break_evaluable = opening_range_ready and bool(rth_bars)
    shock = _shock_fact(
        shock_state,
        atr=atr,
        session_date=trading_date,
    )
    shock_blocks_butterfly = shock["state"] in {"ACTIVE", "POST_SHOCK_DISCOVERY"}
    if shock_blocks_butterfly:
        butterfly_reasons.append(f"shock_{str(shock['state']).lower()}")
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
            "kind": spot_kind,
            "source": spot_source,
            "pricing_source": spot_source,
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
            "opening_range_high": opening_high,
            "opening_range_low": opening_low,
            "market_structure": values.get("market_structure"),
            "vwap": es_vwap - basis if es_vwap is not None and basis is not None else None,
            "distance_to_vwap_points": _number(es.get("vwap_distance_points")),
            "impulse_15m_points": _number(es.get("return_15m_points")),
            "return_1m_points": _number(es.get("return_1m_points")),
            "return_5m_points": _number(es.get("return_5m_points")),
            "return_60m_points": _number(es.get("return_60m_points")),
            "atr_5m": atr,
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
            "put_skew_25d_0dte": _number(volatility.get("put_skew_25d_0dte")),
            "call_skew_25d_0dte": _number(volatility.get("call_skew_25d_0dte")),
            "atm_iv_minus_es_realized_vol": _number(
                market_volatility.get("atm_iv_minus_es_realized_vol")
            ),
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
            "q_p10": _number(density.get("p10")),
            "q_p25": _number(density.get("p25")),
            "q_median": _number(density.get("median")),
            "q_p75": _number(density.get("p75")),
            "q_p90": _number(density.get("p90")),
            "q_mode": _number(density.get("mode")),
            "q_strike_range": list(density.get("strike_range") or ()),
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
        "rth_setups": rth_setups,
        "es_volume": es_volume,
        "pin_latch": _pin_latch_fact(payload, session_date=trading_date or None),
        "shock": shock,
        "gth_evidence": _gth_fact(gth_level),
        "gth_dip_reclaim_evidence": _gth_fact(gth_dip_reclaim),
        "cross_index": _cross_index_fact(market),
        "hmm": _hmm_fact(payload, decision_at),
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
                "trend_evaluable": trend_evaluable,
                "failed_break_evaluable": failed_break_evaluable,
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
                and pin_inputs_ready
                and not shock_blocks_butterfly,
                "structure_ready": structure_ready,
                "value_center_density_ready": pin_inputs_ready,
                "shock_inactive": not shock_blocks_butterfly,
                "exact_quote_requirement": "candidate_specific_three_leg_bbo",
                "reasons": list(dict.fromkeys(butterfly_reasons)),
            },
            "shock": {
                "ready": shock["state"] in {
                    "ACTIVE",
                    "POST_SHOCK_DISCOVERY",
                    "RECLAIMED",
                    "NONE",
                },
                "state": shock["state"],
            },
        },
        "quality": {"status": "ready" if not quality else "degraded", "reasons": quality,
                    "market": market.get("quality") or "unavailable",
                    "options": option.get("quality") or "unavailable",
                    "l1": l1.get("quality") or "unavailable"},
    }


def _es_volume_momentum_setup(
    es_volume: Mapping[str, Any],
    *,
    return_1m: float | None,
    return_5m: float | None,
    atr: float | None,
    spot: float | None,
    now: datetime,
    policy: StrategyPolicy,
) -> dict[str, Any] | None:
    """RTH short-cycle setup: elevated ES pace plus aligned 1m/5m momentum."""

    if not es_volume:
        return None
    label = str(es_volume.get("label") or "")
    vol_dir = str(es_volume.get("direction") or "").lower()
    price_delta = _number(es_volume.get("price_delta"))
    trigger = None
    if spot is not None and price_delta is not None:
        trigger = spot - price_delta
    elif spot is not None:
        trigger = spot
    detected_at = now.isoformat()
    base = {
        "setup_kind": "ES_VOLUME_MOMENTUM",
        "setup_variant": "ES_PACE_1M5M",
        "source": "es_volume_momentum",
        "trigger_level": trigger,
        "pace_label": label,
        "pace_ratio": _number(es_volume.get("pace_ratio")),
        "volume_direction": vol_dir or None,
        "return_1m_points": return_1m,
        "return_5m_points": return_5m,
    }
    if label in {"", "no_baseline", "session_reset"}:
        return _with_setup_window(
            {**base, "state": "SETUP_DETECTED", "direction": None, "reason": "es_volume_unavailable"},
            detected_at=detected_at,
            blocked_by="es_volume_unavailable",
        )
    if label != "elevated":
        return _with_setup_window(
            {**base, "state": "SETUP_DETECTED", "direction": None, "reason": "es_volume_not_elevated"},
            detected_at=detected_at,
            blocked_by="es_volume_not_elevated",
        )
    if vol_dir not in {"up", "down"}:
        return _with_setup_window(
            {
                **base,
                "state": "SETUP_DETECTED",
                "direction": None,
                "reason": "es_volume_momentum_direction_flat",
            },
            detected_at=detected_at,
            blocked_by="es_volume_momentum_direction_flat",
        )
    direction = "UP" if vol_dir == "up" else "DOWN"
    if return_1m is None or return_5m is None:
        return _with_setup_window(
            {
                **base,
                "state": "SETUP_DETECTED",
                "direction": direction,
                "reason": "es_volume_momentum_unevaluable",
            },
            detected_at=detected_at,
            blocked_by="es_volume_momentum_unevaluable",
        )
    want = 1 if direction == "UP" else -1
    sign_1m = 0 if return_1m == 0 else (1 if return_1m > 0 else -1)
    sign_5m = 0 if return_5m == 0 else (1 if return_5m > 0 else -1)
    if sign_1m != want or sign_5m != want:
        return _with_setup_window(
            {
                **base,
                "state": "SETUP_DETECTED",
                "direction": direction,
                "reason": "es_volume_momentum_not_aligned",
            },
            detected_at=detected_at,
            blocked_by="es_volume_momentum_not_aligned",
        )
    if abs(return_1m) < policy.es_momentum_min_return_1m or abs(return_5m) < policy.es_momentum_min_return_5m:
        return _with_setup_window(
            {
                **base,
                "state": "SETUP_DETECTED",
                "direction": direction,
                "reason": "es_volume_momentum_too_weak",
            },
            detected_at=detected_at,
            blocked_by="es_volume_momentum_too_weak",
        )
    if atr is None or atr <= 0:
        return _with_setup_window(
            {
                **base,
                "state": "SETUP_DETECTED",
                "direction": direction,
                "reason": "es_volume_momentum_unevaluable",
            },
            detected_at=detected_at,
            blocked_by="es_volume_momentum_unevaluable",
        )
    exhausted = (
        atr is not None
        and atr > 0
        and abs(return_5m) / atr > policy.es_momentum_max_return_5m_atr
    )
    if exhausted:
        return _with_setup_window(
            {
                **base,
                "state": "ENTRY_TOO_LATE",
                "direction": direction,
                "reason": "es_volume_momentum_too_late",
            },
            detected_at=detected_at,
            window_opens_at=detected_at,
            blocked_by="es_volume_momentum_too_late",
        )
    return _with_setup_window(
        {
            **base,
            "state": "ENTRY_WINDOW_OPEN",
            "direction": direction,
            "reason": "es_volume_momentum_aligned",
        },
        detected_at=detected_at,
        window_opens_at=detected_at,
    )


def _rth_setup_facts(
    bars: list[dict[str, Any]],
    bar_vwaps: Mapping[str, float],
    *,
    opening_high: float | None,
    opening_low: float | None,
    market_structure: str,
    episode_phase: str | None,
    episode_direction: str | None,
    episode_level: float | None,
    episode_id: object,
    episode_phase_at: object = None,
    spot: float | None = None,
    call_wall: float | None = None,
    put_wall: float | None = None,
    policy: StrategyPolicy = DEFAULT_STRATEGY_POLICY,
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    if episode_phase in {"v_reversal_confirmed", "recovery"} and episode_direction:
        rows.append(
            _episode_reclaim_setup(
                direction=episode_direction,
                trigger_level=episode_level,
                episode_id=episode_id,
                episode_phase_at=episode_phase_at,
                spot=spot,
                call_wall=call_wall,
                put_wall=put_wall,
                policy=policy,
            )
        )
    for level, break_side in ((opening_high, "UP"), (opening_low, "DOWN")):
        if level is not None and (
            row := _or_failed_break(
                bars, level, break_side, market_structure, hold_bars=policy.rth_setup_hold_bars
            )
        ):
            rows.append(row)
    rows.extend(
        _vwap_pullbacks(
            bars,
            bar_vwaps,
            opening_high=opening_high,
            opening_low=opening_low,
            hold_bars=policy.rth_setup_hold_bars,
        )
    )
    return rows


def _episode_reclaim_setup(
    *,
    direction: str,
    trigger_level: float | None,
    episode_id: object,
    episode_phase_at: object,
    spot: float | None,
    call_wall: float | None,
    put_wall: float | None,
    policy: StrategyPolicy,
) -> dict[str, Any]:
    target = call_wall if direction == "UP" else put_wall
    progress = _trigger_target_progress(spot, trigger_level, target)
    late = (
        progress is not None
        and progress >= policy.failed_break_max_trigger_target_progress
    )
    return _with_setup_window(
        {
            "setup_kind": "FAILED_BREAK_RECLAIM",
            "setup_variant": "SESSION_EPISODE",
            "state": "ENTRY_TOO_LATE" if late else "ENTRY_WINDOW_OPEN",
            "direction": direction,
            "trigger_level": trigger_level,
            "source": "session_episode_failed_break_reclaim",
            "event_id": episode_id,
            "reason": (
                "session_episode_reclaim_progress_too_late"
                if late
                else "session_episode_reclaim_confirmed"
            ),
            "trigger_target_progress": progress,
        },
        detected_at=episode_phase_at,
        window_opens_at=episode_phase_at,
        blocked_by="session_episode_reclaim_progress_too_late" if late else None,
    )


def _trigger_target_progress(
    spot: float | None, trigger: float | None, target: float | None
) -> float | None:
    if spot is None or trigger is None or target is None or target == trigger:
        return None
    return round(max(0.0, (spot - trigger) / (target - trigger)), 4)


def _or_failed_break(
    bars: list[dict[str, Any]],
    level: float,
    break_side: str,
    market_structure: str,
    *,
    hold_bars: int,
) -> dict[str, Any]:
    closes = [_number(bar.get("close")) for bar in bars]
    outside = (
        (lambda value: value is not None and value > level)
        if break_side == "UP"
        else (lambda value: value is not None and value < level)
    )
    accepted = [
        index
        for index in range(1, len(closes))
        if outside(closes[index - 1]) and outside(closes[index])
    ]
    if not accepted:
        return {}
    accepted_at = accepted[-1]
    reclaimed = next(
        (
            index
            for index in range(accepted_at + 1, len(closes))
            if closes[index] is not None and not outside(closes[index])
        ),
        None,
    )
    if reclaimed is None:
        return {}
    direction = "DOWN" if break_side == "UP" else "UP"
    validation = reclaimed + 1
    expected_structure = {"LH_ONLY", "LH_LL"} if direction == "DOWN" else {"HL_ONLY", "HH_HL"}
    opposite_structure = {"HL_ONLY", "HH_HL"} if direction == "DOWN" else {"LH_ONLY", "LH_LL"}
    if validation >= len(bars):
        state, reason = "SETUP_DETECTED", "next_5m_confirmation_pending"
        window_expires_at = None
    elif outside(closes[validation]):
        state, reason = "INVALIDATED", "next_5m_reaccepted_breakout"
        window_expires_at = None
    elif market_structure in expected_structure:
        state, reason, window_expires_at = _confirmation_hold_state(
            bars, validation, hold_bars=hold_bars, open_reason="or_reclaim_and_structure_confirmed"
        )
    elif market_structure in opposite_structure:
        state, reason = "INVALIDATED", "market_structure_opposes_failed_break"
        window_expires_at = None
    else:
        state, reason = "SETUP_DETECTED", "market_structure_confirmation_pending"
        window_expires_at = None
    detected_at = bars[reclaimed].get("bar_start")
    validated_at = bars[validation].get("bar_start") if validation < len(bars) else None
    window_opens_at = validated_at if state in {"ENTRY_WINDOW_OPEN", "ENTRY_TOO_LATE"} else None
    return _with_setup_window(
        {
            "setup_kind": "FAILED_BREAK_RECLAIM",
            "setup_variant": "OR_FAILED_BREAK",
            "state": state,
            "direction": direction,
            "trigger_level": level,
            "source": "rth_opening_range_failed_break",
            "reason": reason,
            "accepted_at": bars[accepted_at].get("bar_start"),
            "reclaimed_at": detected_at,
            "validated_at": validated_at,
        },
        detected_at=detected_at,
        window_opens_at=window_opens_at,
        window_expires_at=window_expires_at,
        blocked_by=None if state == "ENTRY_WINDOW_OPEN" else reason,
    )


def _vwap_pullbacks(
    bars: list[dict[str, Any]],
    bar_vwaps: Mapping[str, float],
    *,
    opening_high: float | None,
    opening_low: float | None,
    hold_bars: int,
) -> list[dict[str, Any]]:
    found: dict[str, dict[str, Any]] = {}
    for index, bar in enumerate(bars):
        at = str(bar.get("bar_start") or "")
        zones: list[tuple[str, float]] = []
        if (vwap := _number(bar_vwaps.get(at))) is not None:
            zones.append(("VWAP", vwap))
        if opening_high is not None and _accepted_before(bars, opening_high, index, above=True):
            zones.append(("ACCEPTED_ORH", opening_high))
        if opening_low is not None and _accepted_before(bars, opening_low, index, above=False):
            zones.append(("ACCEPTED_ORL", opening_low))
        for direction, allowed in (("UP", {"VWAP", "ACCEPTED_ORH"}), ("DOWN", {"VWAP", "ACCEPTED_ORL"})):
            for zone_kind, zone in zones:
                if zone_kind not in allowed or not _pullback_rejection(bar, zone, direction):
                    continue
                validation = index + 1
                window_expires_at = None
                if validation >= len(bars):
                    state, reason = "SETUP_DETECTED", "next_5m_confirmation_pending"
                    validated_at = None
                elif _pullback_validation(bar, bars[validation], zone, direction):
                    state, reason, window_expires_at = _confirmation_hold_state(
                        bars,
                        validation,
                        hold_bars=hold_bars,
                        open_reason="pullback_rejection_and_hold_confirmed",
                    )
                    validated_at = bars[validation].get("bar_start")
                else:
                    state, reason = "INVALIDATED", "pullback_validation_failed"
                    validated_at = bars[validation].get("bar_start")
                if _map(found.get(direction)).get("state") == "ENTRY_WINDOW_OPEN":
                    break
                found[direction] = _with_setup_window(
                    {
                        "setup_kind": "TREND_PULLBACK",
                        "setup_variant": f"{zone_kind}_PULLBACK",
                        "state": state,
                        "direction": direction,
                        "trigger_level": zone,
                        "source": "rth_vwap_trend_pullback",
                        "reason": reason,
                        "rejection_at": at,
                        "validated_at": validated_at,
                    },
                    detected_at=at,
                    window_opens_at=(
                        validated_at
                        if state in {"ENTRY_WINDOW_OPEN", "ENTRY_TOO_LATE"}
                        else None
                    ),
                    window_expires_at=window_expires_at,
                    blocked_by=None if state == "ENTRY_WINDOW_OPEN" else reason,
                )
                break
    return list(found.values())


def _accepted_before(
    bars: list[dict[str, Any]], level: float, before: int, *, above: bool
) -> bool:
    closes = [_number(bar.get("close")) for bar in bars[:before]]
    compare = (lambda value: value is not None and value > level) if above else (
        lambda value: value is not None and value < level
    )
    return any(compare(closes[index - 1]) and compare(closes[index]) for index in range(1, len(closes)))


def _pullback_rejection(bar: Mapping[str, Any], zone: float, direction: str) -> bool:
    low, high, close = (_number(bar.get(key)) for key in ("low", "high", "close"))
    if None in (low, high, close):
        return False
    return bool(low <= zone <= close) if direction == "UP" else bool(close <= zone <= high)


def _pullback_validation(
    rejection: Mapping[str, Any], validation: Mapping[str, Any], zone: float, direction: str
) -> bool:
    reject_edge = _number(rejection.get("low" if direction == "UP" else "high"))
    next_edge = _number(validation.get("low" if direction == "UP" else "high"))
    close = _number(validation.get("close"))
    if None in (reject_edge, next_edge, close):
        return False
    if direction == "UP":
        return bool(next_edge >= reject_edge and close >= zone)
    return bool(next_edge <= reject_edge and close <= zone)


def _spx_bars(value: object, basis: float | None) -> list[dict[str, Any]]:
    if not isinstance(value, list) or basis is None:
        return []
    rows = []
    for raw in value:
        bar = _map(raw)
        converted = {key: _spx_level(bar.get(key), basis) for key in ("open", "high", "low", "close")}
        if None not in converted.values():
            rows.append({"bar_start": bar.get("bar_start"), **converted})
    return sorted(rows, key=lambda row: str(row.get("bar_start") or ""))


def _spx_level(value: object, basis: float | None) -> float | None:
    number = _number(value)
    return number - basis if number is not None and basis is not None else None


def _shock_fact(
    state: Mapping[str, Any], *, atr: float | None, session_date: str
) -> dict[str, Any]:
    state_session = str(state.get("session_date") or "")
    if state_session and session_date and state_session != session_date:
        return {
            "state": "NONE",
            "started_at": None,
            "magnitude_atr": None,
            "cross_asset_confirmed": None,
        }
    active = _map(state.get("active_event"))
    rearm = _map(state.get("rearm"))
    last = _map(state.get("last_event"))
    if active:
        event = active
        shock_state = "RECLAIMED" if active.get("reclaim_confirmed_at") else "ACTIVE"
    elif rearm:
        event = last or rearm
        shock_state = "POST_SHOCK_DISCOVERY"
    elif last.get("reclaim_confirmed_at"):
        event, shock_state = last, "RECLAIMED"
    else:
        event, shock_state = {}, "NONE"
    anchor, extreme = _number(event.get("anchor_spx")), _number(event.get("extreme_spx"))
    magnitude = (
        abs(anchor - extreme) / atr
        if anchor is not None and extreme is not None and atr is not None and atr > 0
        else None
    )
    spx_bps, es_bps = _number(event.get("shock_spx_bps")), _number(event.get("shock_es_bps"))
    cross_asset = (
        spx_bps * es_bps > 0 if spx_bps is not None and es_bps is not None else None
    )
    return {
        "state": shock_state,
        "started_at": event.get("anchor_at"),
        "magnitude_atr": round(magnitude, 4) if magnitude is not None else None,
        "cross_asset_confirmed": cross_asset,
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


def _confirmation_hold_state(
    bars: list[dict[str, Any]],
    validation: int,
    *,
    hold_bars: int,
    open_reason: str,
) -> tuple[str, str, object]:
    last_open = validation + max(int(hold_bars), 0)
    expire_index = last_open + 1
    expires_at = bars[expire_index].get("bar_start") if expire_index < len(bars) else None
    if len(bars) - 1 <= last_open:
        return "ENTRY_WINDOW_OPEN", open_reason, expires_at
    return "ENTRY_TOO_LATE", open_reason, expires_at


def _with_setup_window(
    row: dict[str, Any],
    *,
    detected_at: object = None,
    window_opens_at: object = None,
    window_expires_at: object = None,
    blocked_by: object = None,
) -> dict[str, Any]:
    row.update(
        {
            "detected_at": detected_at,
            "window_opens_at": window_opens_at,
            "window_expires_at": window_expires_at,
            "blocked_by": blocked_by,
        }
    )
    return row


def _pin_latch_fact(payload: Mapping[str, Any], *, session_date: str | None) -> dict[str, Any]:
    previous = _map(payload.get("previous_strategy_decision"))
    if not previous or not session_date:
        return {}
    if str(previous.get("session_date") or "") != session_date:
        return {}
    regime = _map(previous.get("regime"))
    center = pin_stable_center(regime)
    if regime.get("terminal_state") != "PIN_STABLE" or center is None:
        return {}
    latch = {
        "terminal_state": "PIN_STABLE",
        "center": center,
        "session_date": session_date,
    }
    previous_decision_at = previous.get("decision_at")
    if isinstance(previous_decision_at, str) and _time(previous_decision_at) is not None:
        latch["decision_at"] = previous_decision_at
    pin = _map(regime.get("pin"))
    q_mode = _number(pin.get("q_mode"))
    if q_mode is not None:
        latch["q_mode"] = q_mode
    confirmation_count = _number(pin.get("center_confirmation_count"))
    if confirmation_count is not None:
        latch["center_confirmation_count"] = int(confirmation_count)
    first_seen_at = pin.get("center_first_seen_at")
    if isinstance(first_seen_at, str) and _time(first_seen_at) is not None:
        latch["center_first_seen_at"] = first_seen_at
    return latch


def _cross_index_fact(market: Mapping[str, Any]) -> dict[str, Any]:
    cross = _map(_map(market.get("cross_asset")).get("cross_index"))
    return {
        "source": cross.get("source"),
        "status": cross.get("status"),
        "session_open": cross.get("session_open") is True,
        "anchor": cross.get("anchor"),
        "missing_instruments": list(cross.get("missing_instruments") or ()),
        "reason_codes": list(cross.get("reason_codes") or ()),
    }


def _hmm_fact(payload: Mapping[str, Any], decision_at: datetime) -> dict[str, Any]:
    document = _map(
        payload.get("experimental_research_signals") or payload.get("research_context")
    )
    regime = _map(document.get("regime"))
    posterior = _posterior_map(regime.get("posterior") or document.get("posterior"))
    observed = _time(
        regime.get("observed_through")
        or regime.get("available_at")
        or regime.get("as_of")
        or document.get("generated_at")
        or document.get("as_of")
    )
    unavailable = {
        "status": "unavailable",
        "posterior": {},
        "dominant_state": None,
        "max_state_probability": None,
        "reason": "hmm_unavailable",
    }
    if document.get("action_authority") not in {None, "none"}:
        return {**unavailable, "reason": "hmm_action_authority_rejected"}
    if posterior is None or observed is None:
        return unavailable
    if observed > decision_at:
        return {**unavailable, "reason": "hmm_from_future"}
    age = (decision_at - observed).total_seconds()
    if age > DEFAULT_STRATEGY_POLICY.hmm_max_age_seconds:
        return {**unavailable, "reason": "hmm_stale"}
    dominant = max(posterior, key=posterior.__getitem__)
    return {
        "status": "available",
        "posterior": posterior,
        "dominant_state": dominant,
        "max_state_probability": posterior[dominant],
        "observed_through": observed.isoformat(),
        "reason": None,
    }


def _preaverage_pullback_setup(
    payload: Mapping[str, Any],
    *,
    decision_at: datetime,
    session_date: str,
) -> dict[str, Any]:
    document = _map(
        payload.get("experimental_research_signals") or payload.get("research_context")
    )
    signal = _map(document.get("denoising_forward"))
    signal_at = _time(signal.get("signal_at"))
    valid_until = _time(signal.get("valid_until"))
    generated_at = _time(document.get("generated_at"))
    if (
        document.get("action_authority") not in {None, "none"}
        or signal.get("action_authority") != "none"
        or signal.get("automatic_ordering") is not False
        or signal.get("schema_version") != "raw_tick_denoising_forward.v1"
        or signal.get("contract_hash") != _DENOISING_FORWARD_CONTRACT_HASH
        or signal.get("authorization_policy") != "strategy_policy.bootstrap.v40"
        or signal.get("evidence_status") != "forward_unvalidated_user_override"
        or signal.get("status") != "triggered"
        or signal.get("setup_kind") != "PREAVERAGE15_PULLBACK"
        or signal.get("session_date") != session_date
        or signal_at is None
        or valid_until is None
        or generated_at is None
        or signal_at > generated_at
        or generated_at > decision_at
        or (decision_at - signal_at).total_seconds() < 5.0
        or decision_at >= valid_until
    ):
        return {}
    return {
        **dict(signal),
        "state": "ENTRY_WINDOW_OPEN",
        "source": "rth_preaverage15_pullback",
        "evidence_contract_hash": _DENOISING_FORWARD_CONTRACT_HASH,
    }


def _wall_hazard_setup(
    payload: Mapping[str, Any],
    *,
    decision_at: datetime,
    session_date: str,
    policy: StrategyPolicy,
) -> dict[str, Any]:
    document = _map(
        payload.get("experimental_research_signals") or payload.get("research_context")
    )
    forward = _map(document.get("denoising_forward"))
    hazard = _map(forward.get("wall_hazard"))
    generated_at = _time(document.get("generated_at"))
    available_at = _time(hazard.get("available_at"))
    probabilities = _map(hazard.get("probabilities"))
    down = _number(probabilities.get("down_break"))
    flat = _number(probabilities.get("no_break"))
    up = _number(probabilities.get("up_break"))
    scale = _number(hazard.get("path_scale_points"))
    spot = _number(hazard.get("spot"))
    upper = _number(hazard.get("upper_barrier"))
    lower = _number(hazard.get("lower_barrier"))
    if (
        document.get("action_authority") not in {None, "none"}
        or hazard.get("action_authority") != "none"
        or hazard.get("automatic_ordering") is not False
        or hazard.get("schema_version") != "wall_competing_risk_hazard.v1"
        or hazard.get("contract_hash") != _WALL_HAZARD_CONTRACT_HASH
        or hazard.get("evidence_status") != "forward_unvalidated_user_override"
        or hazard.get("status") != "available"
        or generated_at is None
        or available_at is None
        or available_at > generated_at
        or generated_at > decision_at
        or not 0.0 <= (decision_at - available_at).total_seconds() <= 15.0
        or None in (down, flat, up, scale, spot)
        or abs(float(down) + float(flat) + float(up) - 1.0) > 1e-6
    ):
        return {}
    choices = [
        (float(up), "UP", upper, float(down)),
        (float(down), "DOWN", lower, float(up)),
    ]
    probability, direction, barrier, opposite = max(choices, key=lambda row: row[0])
    if (
        barrier is None
        or probability < policy.wall_hazard_min_side_probability
        or probability <= opposite
    ):
        return {}
    sign = 1.0 if direction == "UP" else -1.0
    return {
        "setup_kind": "WALL_BREAKOUT_HAZARD",
        "setup_variant": "walls_only_multinomial::15m_break_hold",
        "state": "ENTRY_WINDOW_OPEN",
        "direction": direction,
        "session_date": session_date,
        "signal_at": available_at.isoformat(),
        "valid_until": (available_at + timedelta(seconds=60)).isoformat(),
        "trigger_level": spot,
        "target_spx": barrier + sign * 0.10 * float(scale),
        "invalidation_spx": spot - sign * 0.50 * float(scale),
        "local_scale_points": scale,
        "hazard_probability": probability,
        "hazard_probabilities": dict(probabilities),
        "hazard_features": dict(_map(hazard.get("features"))),
        "hazard_oos": dict(_map(hazard.get("oos"))),
        "source": "rth_wall_breakout_hazard",
        "geometry_source": "wall_break_hold_competing_risk",
        "evidence_contract_hash": _WALL_HAZARD_CONTRACT_HASH,
        "authorization_policy": policy.policy_version,
        "evidence_status": "forward_unvalidated_user_override",
    }


def _posterior_map(raw: object) -> dict[str, float] | None:
    values: dict[str, float] = {}
    if isinstance(raw, Mapping):
        for state in ("state_00", "state_01", "state_02"):
            number = _number(raw.get(state))
            if number is None:
                return None
            values[state] = number
        return values
    if not isinstance(raw, list | tuple):
        return None
    for row in raw:
        item = _map(row)
        state = str(item.get("state_id") or "")
        number = _number(item.get("probability"))
        if state and number is not None:
            values[state] = number
    if any(state not in values for state in ("state_00", "state_01", "state_02")):
        return None
    return {state: values[state] for state in ("state_00", "state_01", "state_02")}


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
