"""Order-map orchestration: payload build, status/refresh/send runners."""

from __future__ import annotations

import argparse
import json
import os
import sys
import time as time_module
from dataclasses import asdict, replace
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from spx_spark.analytics.options.pricing import finite_float
from spx_spark.application.globex_trend.state import load_trend_state, trend_state_path
from spx_spark.application.market_features.greek_decision import build_greek_decision
from spx_spark.application.market_features.state import load_json, projection_paths
from spx_spark.application.order_map.bias_machine import load_intraday_call_bias
from spx_spark.application.order_map.candidate_presentation import (
    apply_candidate_presentation as _apply_candidate_presentation,
)
from spx_spark.application.order_map.candidates import build_candidates
from spx_spark.application.order_map.decision_consistency import (
    apply_decision_projections,
)
from spx_spark.application.order_map.delivery import send_order_map
from spx_spark.application.order_map.es_volume_attach import attach_es_volume_signal
from spx_spark.application.order_map.hl_volume import (
    attach_hl_volume_signal,
    default_hl_volume_sample_path,
)
from spx_spark.application.order_map.level_decision_shadow import (
    load_level_decision_shadow,
)
from spx_spark.application.order_map.level_trigger_repricing import (
    default_level_trigger_repricing_path,
)
from spx_spark.application.order_map.models import SHANGHAI_TZ
from spx_spark.application.order_map.prompts import (
    GLOBEX_CONTEXT_SYSTEM_PROMPT,
    actionable_writer_output_valid,
    build_status_prompt,
    globex_writer_output_valid,
    render_feishu_delivery_text,
    render_status_template,
)
from spx_spark.application.order_map.pricing_audit import (
    append_pricing_audit,
    build_pricing_audit_record,
)
from spx_spark.application.order_map.render import (
    render_template,
)
from spx_spark.application.order_map.research import (
    _index_value,
    _research_candidates,
    _research_wall_ladder,
    _wall_ladder_payload,
)
from spx_spark.application.order_map.signal_machine import annotate_call_bias_with_signal_mode
from spx_spark.application.order_map.spot import hyperliquid_sp500_price, resolve_spx_spot
from spx_spark.application.order_map.state import (
    REFRESH_COOLDOWN_SECONDS_DEFAULT,
    already_sent,
    default_state_path,
    load_order_map_state,
    mark_sent,
    material_changes,
    payload_fingerprint,
    session_phase,
    within_refresh_window,
    within_send_window,
    within_status_window,
)
from spx_spark.application.order_map.volume_machine import (
    default_es_volume_sample_path,
)
from spx_spark.config import NotificationSettings, StorageSettings
from spx_spark.greek_reference import (
    build_zero_dte_greeks_reference,
    write_zero_dte_greeks_snapshot,
)
from spx_spark.intraday_strategy import signed_gex_sign_method
from spx_spark.market_calendar import DEFAULT_MARKET_CALENDAR
from spx_spark.macro_event_clock import macro_event_state
from spx_spark.notifier.dispatcher import dispatch_notification
from spx_spark.notifier.llm_writer import generate_push_text, load_previous_push, record_push
from spx_spark.notifier.model import CommandRunner, default_runner
from spx_spark.notifier.receipts import NotificationEnvelope, notification_event_id
from spx_spark.options_map import build_options_map
from spx_spark.ibkr.position_watcher import default_positions_path, load_snapshot
from spx_spark.storage import LatestState, LatestStateStore, configured_quote_use_decision
from spx_spark.settings import load_app_settings
from spx_spark.settings.order_map import DEFAULT_ORDER_MAP_POLICY, OrderMapPolicy


STATUS_KEY_WINDOW_PHASES = frozenset(
    {
        "europe_session",
        "us_data_hour",
        "us_open_hour",
        "us_midday_confirmation",
    }
)
GTH_STATUS_PHASES = frozenset({"asia_globex", "europe_session", "us_data_hour"})
GTH_STATUS_CADENCE_SECONDS = 15.0 * 60.0


def build_order_payload(
    state: LatestState,
    *,
    now: datetime | None = None,
    policy: OrderMapPolicy = DEFAULT_ORDER_MAP_POLICY,
) -> dict[str, Any]:
    now = now or state.as_of
    options_map = build_options_map(state)
    warnings = list(options_map.warnings)

    front = options_map.expiries[0] if options_map.expiries else None
    expiry = front.expiry if front is not None else None
    expected_move_points = front.expected_move_points if front is not None else None
    gamma_state = front.gamma_state if front is not None else "unknown"
    zero_gamma = front.zero_gamma if front is not None else None
    flip_zone = list(front.gamma_flip_zone) if front is not None and front.gamma_flip_zone else None

    if options_map.underlier.price is None:
        warnings.append("missing underlier reference")
    if not options_map.expiries:
        warnings.append("missing expiries")
    if front is not None and front.gex_quality == "no_open_interest_gex":
        warnings.append("no open interest; walls unavailable")

    resolution = resolve_spx_spot(state, options_map, warnings=warnings, now=now)
    pricing_spot = resolution.pricing_price if resolution.pricing_allowed else None
    conditional_call_bias = load_intraday_call_bias(now=now)
    candidates = build_candidates(
        state,
        options_map,
        warnings,
        now=now,
        resolution=resolution,
        conditional_call_bias=conditional_call_bias,
        policy=policy,
    )
    candidate_rows = [asdict(candidate) for candidate in candidates]
    macro_event = macro_event_state(now)
    greeks_audit_reference = build_zero_dte_greeks_reference(
        replace(state, as_of=now),
        options_map=options_map,
        focus_contract_ids=(candidate.contract_id for candidate in candidates),
        max_serialized_contracts=max(len(candidates), 1),
        serialized_scenario_names=(
            "spot_down_0_25pct",
            "spot_up_0_25pct",
            "clock_plus_5m",
            "clock_plus_15m",
            "clock_plus_30m",
            "iv_down_1vol",
            "iv_down_3vol",
        ),
    )
    greek_decision = build_greek_decision(
        greeks_audit_reference,
        candidate_rows,
        macro_event=macro_event,
        policy=load_app_settings().market_features,
    )
    greeks_reference = {
        **greeks_audit_reference,
        "serialized_contract_count": 0,
        "contracts": [],
    }
    beijing = now.astimezone(SHANGHAI_TZ)
    trigger_coordinate = _report_trigger_coordinate(state, resolution, now=now)

    # Keep prior-close change as context. Expected-move consumption is attached
    # later from the current GTH session so yesterday's move cannot leak into it.
    spx_quote = state.best_quote("index:SPX")
    prior_close = finite_float(spx_quote.close) if spx_quote is not None else None
    day_move_points = (
        round(pricing_spot - prior_close, 1) if pricing_spot is not None and prior_close else None
    )

    return {
        "kind": "order_map",
        "as_of": state.as_of.isoformat(),
        "beijing_time": beijing.strftime("%H:%M"),
        "trading_date": DEFAULT_MARKET_CALENDAR.research_expiry(now).isoformat(),
        "underlier": {
            "price": pricing_spot,
            "source": resolution.pricing_source if resolution.pricing_allowed else None,
        },
        "research_reference": {
            "price": resolution.research_price,
            "source": resolution.research_source,
        },
        "pricing_reference": {
            "price": pricing_spot,
            "source": resolution.pricing_source if resolution.pricing_allowed else None,
            "pricing_allowed": resolution.pricing_allowed,
            "gate_state": resolution.gate_state,
            "reason": resolution.reason,
            "divergence_bps": resolution.divergence_bps,
        },
        "trigger_coordinate": trigger_coordinate,
        "pricing_allowed": resolution.pricing_allowed,
        "research_only": resolution.research_only,
        "analysis_mode": "globex_context" if resolution.research_only else "executable",
        "expiry": expiry,
        "expected_move_points": expected_move_points,
        "candidates": candidate_rows,
        "conditional_call_bias": annotate_call_bias_with_signal_mode(
            conditional_call_bias
            or {
                "status": "neutral",
                "play": None,
                "signed_gex_sign_method": signed_gex_sign_method(
                    front.gex_weighting if front is not None else None
                ),
                "dealer_position_sign": "unknown",
            }
        ),
        "signed_gex_proxy": {
            "net_gex": front.net_gex if front is not None else None,
            "abs_gex": front.abs_gex if front is not None else None,
            "net_gamma_ratio": front.net_gamma_ratio if front is not None else None,
            "gamma_state": front.gamma_state if front is not None else "unknown",
            "weighting": front.gex_weighting if front is not None else None,
            "sign_method": signed_gex_sign_method(
                front.gex_weighting if front is not None else None
            ),
            "dealer_position_sign": "unknown",
            "direction": "unknown",
        },
        "spxw_0dte_greeks_reference": greeks_reference,
        "_spxw_0dte_greeks_audit": greeks_audit_reference,
        "greek_decision": greek_decision,
        "macro_event": macro_event,
        "research_candidates": (
            _research_candidates(
                state,
                options_map,
                research_price=resolution.research_price,
                as_of=now,
            )
            if resolution.research_only
            else []
        ),
        "gamma_state": gamma_state,
        "zero_gamma": zero_gamma,
        "flip_zone": flip_zone,
        "wall_ladder": (
            _wall_ladder_payload(
                state,
                options_map,
                pricing_spot,
                now=now,
                policy=policy,
            )
            if resolution.pricing_allowed
            else {"call_walls": [], "put_walls": []}
        ),
        "research_wall_ladder": (
            _research_wall_ladder(
                state,
                options_map,
                research_price=resolution.research_price,
                as_of=now,
            )
            if resolution.research_only
            else {"call_walls": [], "put_walls": []}
        ),
        "wall_method": front.wall_method if front is not None else None,
        "day_move": {
            "prior_close": prior_close,
            "points": day_move_points,
            "em_used_fraction": None,
            "em_move_points": None,
            "em_baseline": None,
            "em_baseline_source": "es_gth_open",
            "em_session_id": None,
        },
        "rn_density": (
            front.rn_density.to_dict()
            if resolution.pricing_allowed and front is not None and front.rn_density
            else None
        ),
        "max_pain": (
            front.max_pain.to_dict()
            if resolution.pricing_allowed and front is not None and front.max_pain
            else None
        ),
        "vol_context": {
            "vix": _index_value(state, "index:VIX"),
            "vix1d": _index_value(state, "index:VIX1D"),
            "vvix": _index_value(state, "index:VVIX"),
            "skew": _index_value(state, "index:SKEW"),
        },
        "hl_sp500_perp": hyperliquid_sp500_price(state, as_of=now),
        "es_last": _index_value(state, "future:ES"),
        "session_phase": session_phase(now),
        "warnings": list(dict.fromkeys(warnings)),
    }


def _report_trigger_coordinate(
    state: LatestState,
    resolution,
    *,
    now: datetime,
) -> dict[str, object]:
    if DEFAULT_MARKET_CALENDAR.is_rth_open(now):
        quote = state.best_quote("index:SPX")
        if quote is not None and configured_quote_use_decision(quote, as_of=now).pricing_allowed:
            return {
                "kind": "official_spx",
                "instrument_id": "index:SPX",
                "observed_value": quote.effective_price,
                "source": "index:SPX",
            }
        return {
            "kind": "unavailable",
            "instrument_id": None,
            "observed_value": None,
            "source": "official_spx_unavailable_use_realtime_es_equivalent",
        }
    if resolution.pricing_source == "chain_implied":
        return {
            "kind": "chain_implied_spx",
            "instrument_id": "synthetic:SPXW_PARITY",
            "observed_value": resolution.pricing_price,
            "source": "chain_implied",
        }
    return {
        "kind": "unavailable",
        "instrument_id": None,
        "observed_value": None,
        "source": "chain_implied_unavailable_use_realtime_es_equivalent",
    }


def persist_zero_dte_greeks_reference(
    payload: dict[str, Any],
    storage_settings: StorageSettings,
) -> None:
    reference = payload.get("_spxw_0dte_greeks_audit")
    if not isinstance(reference, dict):
        reference = payload.get("spxw_0dte_greeks_reference")
    data_root = getattr(storage_settings, "data_root", None)
    if not isinstance(reference, dict) or not isinstance(data_root, str) or not data_root:
        return
    try:
        write_zero_dte_greeks_snapshot(reference, data_root=data_root)
    except OSError as exc:
        print(f"0DTE Greeks snapshot write failed: {exc}", file=sys.stderr)


def persist_order_map_pricing_audit(
    payload: dict[str, Any],
    storage_settings: StorageSettings,
    *,
    now: datetime,
    report_kind: str,
    template: str,
    result: dict[str, Any],
) -> None:
    try:
        append_pricing_audit(
            storage_settings.data_root,
            build_pricing_audit_record(
                payload,
                generated_at=now,
                report_kind=report_kind,
                template=template,
                delivered_text=str(result.get("text") or ""),
                writer=str(result.get("writer") or "unknown"),
                delivered_ok=result.get("delivered_ok") is True,
            ),
        )
    except OSError as exc:
        print(f"Order-map pricing audit write failed: {exc}", file=sys.stderr)


def _payload_is_thin(payload: dict[str, Any]) -> bool:
    """True when the snapshot caught a mid-rotation flush (missing spot/OI/plays)."""
    research_reference = (
        payload.get("research_reference")
        if isinstance(payload.get("research_reference"), dict)
        else {}
    )
    if payload.get("research_only") is True and research_reference.get("price") is not None:
        return False
    underlier = payload.get("underlier") if isinstance(payload.get("underlier"), dict) else {}
    if underlier.get("price") is None:
        return True
    if not payload.get("candidates"):
        return True
    warnings = payload.get("warnings")
    if isinstance(warnings, list) and any("no open interest" in str(item) for item in warnings):
        return True
    fingerprint = payload_fingerprint(payload)
    if (
        fingerprint.get("put_wall") is None
        and fingerprint.get("call_wall") is None
        and fingerprint.get("flip_low") is None
    ):
        return True
    return False


def _payload_has_retryable_candidate_gap(payload: dict[str, Any]) -> bool:
    """True when an intended play is missing only because its quote is stale.

    Keep this separate from ``_payload_is_thin``: if the retry budget expires,
    the status push should still report the degraded candidate instead of
    silently skipping the whole snapshot. Non-fresh feed modes and structural
    play skips remain fail-closed without delaying the push.
    """
    warnings = payload.get("warnings")
    if not isinstance(warnings, list):
        return False
    return any(
        str(item).startswith("bad_quality_for_") and ":transport_stale_after_" in str(item)
        for item in warnings
    )


def _recent_market_frame_es(
    frame: dict[str, Any], *, now: datetime, max_age_seconds: float
) -> tuple[float | None, str | None]:
    if frame.get("quality") == "unavailable":
        return None, None
    try:
        as_of = datetime.fromisoformat(str(frame.get("as_of")))
    except ValueError:
        return None, None
    if abs((now - as_of).total_seconds()) > max_age_seconds:
        return None, None
    es = frame.get("es") if isinstance(frame.get("es"), dict) else {}
    return finite_float(es.get("price")), str(es.get("provider") or "") or None


def build_order_payload_with_retry(
    storage_settings: StorageSettings,
    *,
    now: datetime,
    attempts: int = 7,
    delay_seconds: float = 10.0,
) -> dict[str, Any]:
    """Rebuild thin/stale payloads across one option-rotation retry budget."""
    payload: dict[str, Any] = {}
    app = load_app_settings()
    policy = app.order_map
    state: LatestState | None = None
    started_at = time_module.monotonic()
    evaluation_now = now
    for attempt in range(attempts):
        if attempt:
            elapsed_seconds = max(time_module.monotonic() - started_at, 0.0)
            evaluation_now = now + timedelta(seconds=elapsed_seconds)
        state = LatestStateStore(storage_settings).load(now=evaluation_now)
        payload = build_order_payload(state, now=evaluation_now, policy=policy)
        if not (_payload_is_thin(payload) or _payload_has_retryable_candidate_gap(payload)):
            break
        if attempt < attempts - 1:
            time_module.sleep(delay_seconds)
    if state is not None:
        feature_paths = projection_paths(storage_settings.data_root)
        market_frame = load_json(feature_paths["market"])
        option_frame = load_json(feature_paths["option"])
        payload["globex_trend"] = load_trend_state(trend_state_path(storage_settings.data_root))
        payload["gth_dip_reclaim_signal"] = load_json(
            Path(storage_settings.data_root) / "latest" / "gth_dip_reclaim_signal.json"
        )
        payload["minute_market_frame"] = market_frame
        _apply_gth_em_usage(payload, market_frame)
        payload["option_structure_frame"] = option_frame
        apply_decision_projections(
            payload,
            level_decision=load_level_decision_shadow(storage_settings),
            market_frame=market_frame,
            option_frame=option_frame,
            decision_context=load_json(feature_paths["decision"]),
            max_level_drift_points=app.market_features.trade_structure_drift_points,
        )
        payload["level_trigger_repricing"] = load_json(
            default_level_trigger_repricing_path(storage_settings)
        )
        if payload.get("es_last") is None:
            es_price, es_provider = _recent_market_frame_es(
                market_frame,
                now=evaluation_now,
                max_age_seconds=max(
                    app.market_features.interval_seconds * 2,
                    app.market_features.max_quote_age_seconds,
                ),
            )
            if es_price is not None:
                payload["es_last"] = es_price
                payload["es_last_source"] = f"minute_frame:{es_provider or 'unknown'}"
        payload["context_cross_checks"] = {
            "es": payload.get("es_last"),
            "hyperliquid": payload.get("hl_sp500_perp"),
        }
        attach_es_volume_signal(
            payload,
            state,
            sample_path=default_es_volume_sample_path(storage_settings),
            now=evaluation_now,
            policy=policy,
        )
        attach_hl_volume_signal(
            payload,
            state,
            storage_settings=storage_settings,
            sample_path=default_hl_volume_sample_path(storage_settings),
            now=evaluation_now,
        )
        _apply_candidate_presentation(payload, now=evaluation_now)
    return payload


def _apply_gth_em_usage(
    payload: dict[str, Any],
    market_frame: dict[str, Any],
) -> None:
    """Anchor EM usage to the current session's 20:15 ET SPX GTH open."""

    day_move = payload.get("day_move")
    es = market_frame.get("es")
    if not isinstance(day_move, dict) or not isinstance(es, dict):
        return
    frame_session = str(market_frame.get("session_id") or "")
    payload_session = str(payload.get("trading_date") or "")
    if not frame_session or frame_session != payload_session:
        return
    gth_open = finite_float(es.get("gth_open_price"))
    current_es = finite_float(payload.get("es_last"))
    if current_es is None:
        current_es = finite_float(es.get("price"))
    expected_move = finite_float(payload.get("expected_move_points"))
    if gth_open is None or current_es is None or expected_move is None or expected_move <= 0:
        return
    em_move = current_es - gth_open
    day_move.update(
        {
            "em_used_fraction": round(abs(em_move) / expected_move, 2),
            "em_move_points": round(em_move, 1),
            "em_baseline": gth_open,
            "em_baseline_source": "es_gth_open",
            "em_session_id": frame_session,
        }
    )


def _status_fingerprint(payload: dict[str, Any]) -> dict[str, Any]:
    fingerprint = payload_fingerprint(payload)
    phase = payload.get("session_phase")
    fingerprint["status_phase"] = str(phase.get("name") or "") if isinstance(phase, dict) else ""
    fingerprint["decision_thesis"] = _decision_thesis(payload)
    plans = payload.get("plan_candidates")
    plan = plans[0] if isinstance(plans, list) and len(plans) == 1 else None
    if isinstance(plan, dict):
        fingerprint["trade_intent_id"] = str(plan.get("intent_id") or "")
        strike = finite_float(plan.get("strike"))
        right = str(plan.get("right") or "")
        fingerprint["trade_contract"] = f"{strike:g}{right}" if strike is not None else ""
    else:
        fingerprint["trade_intent_id"] = ""
        fingerprint["trade_contract"] = ""
    return fingerprint


def _decision_thesis(payload: dict[str, Any]) -> str:
    plans = payload.get("plan_candidates")
    if isinstance(plans, list) and len(plans) == 1 and isinstance(plans[0], dict):
        plan = plans[0]
        return f"plan:{plan.get('play') or '-'}@{finite_float(plan.get('level'))}"
    intent = payload.get("trade_intent")
    intent_status = str(intent.get("status") or "") if isinstance(intent, dict) else ""
    regime = payload.get("regime_decision")
    if intent_status in {"blocked", "trade_ready"} and isinstance(regime, dict):
        mode = str(regime.get("mode") or "unknown")
        direction = str(regime.get("direction") or "unknown")
        if mode != "unknown" or direction != "unknown":
            return f"regime:{mode}:{direction}"
    return ""


def _status_material_changes(
    previous: dict[str, Any] | None,
    current: dict[str, Any],
) -> list[str]:
    changes = material_changes(previous, current)
    if not isinstance(previous, dict):
        return changes
    prior_thesis = str(previous.get("decision_thesis") or "")
    current_thesis = str(current.get("decision_thesis") or "")
    if prior_thesis != current_thesis and (prior_thesis or current_thesis):
        if prior_thesis and current_thesis:
            changes.append(
                f"决策剧本 {_thesis_label(prior_thesis)}→{_thesis_label(current_thesis)}"
            )
        elif current_thesis:
            changes.append(f"决策剧本建立 {_thesis_label(current_thesis)}")
        else:
            changes.append(f"决策剧本失效 {_thesis_label(prior_thesis)}")
    prior_intent = str(previous.get("trade_intent_id") or "")
    current_intent = str(current.get("trade_intent_id") or "")
    if prior_thesis == current_thesis and prior_intent != current_intent:
        prior_contract = str(previous.get("trade_contract") or "-")
        current_contract = str(current.get("trade_contract") or "-")
        if prior_intent and current_intent:
            changes.append(f"执行意图更新 {prior_contract}→{current_contract}")
        elif current_intent:
            changes.append(f"执行意图建立 {current_contract}")
        elif prior_intent:
            changes.append(f"执行意图失效 {prior_contract}")
    return changes


def _thesis_label(value: str) -> str:
    if value.startswith("plan:"):
        play_and_level = value.removeprefix("plan:")
        play, _, level = play_and_level.partition("@")
        label = {
            "level_breakout_call": "向上突破",
            "level_breakout_put": "向下突破",
            "level_fade_call": "下破拒绝",
            "level_fade_put": "上破拒绝",
        }.get(play, play)
        return f"{label}@{level}" if level else label
    if value.startswith("regime:"):
        _, mode, direction = (value.split(":", 2) + ["unknown", "unknown"])[:3]
        mode_label = {
            "trending": "趋势",
            "mean_reverting": "均值回归",
            "transition": "过渡",
        }.get(mode, mode)
        direction_label = {"up": "偏多", "down": "偏空", "neutral": "中性"}.get(
            direction, direction
        )
        return f"{mode_label}{direction_label}"
    return value


def _has_open_position_risk(storage_settings: StorageSettings) -> bool:
    snapshot = load_snapshot(default_positions_path(storage_settings))
    return bool(snapshot and any(position.qty != 0 for position in snapshot.positions))


def _status_delivery_reason(
    previous: dict[str, Any],
    fingerprint: dict[str, Any],
    changes: list[str],
    *,
    now: datetime,
    trading_date: str,
    position_risk: bool,
) -> str | None:
    if previous.get("last_status_date") != trading_date:
        return "initial_status"
    phase = str(fingerprint.get("status_phase") or "")
    previous_fingerprint = previous.get("status_fingerprint") or previous.get("fingerprint")
    previous_phase = (
        str(previous_fingerprint.get("status_phase") or "")
        if isinstance(previous_fingerprint, dict)
        else ""
    )
    if phase in STATUS_KEY_WINDOW_PHASES and previous_phase != phase:
        return f"key_window:{phase}"
    if position_risk:
        return "open_position_risk"
    if phase in GTH_STATUS_PHASES:
        prior_intent = (
            str(previous_fingerprint.get("trade_intent_id") or "")
            if isinstance(previous_fingerprint, dict)
            else ""
        )
        current_intent = str(fingerprint.get("trade_intent_id") or "")
        if not prior_intent and not current_intent:
            structural_changes = [
                change for change in changes if not change.startswith("决策剧本")
            ]
            if structural_changes:
                return "material_changes"
            last_status_at = finite_float(previous.get("last_status_at"))
            if (
                last_status_at is None
                or int(now.timestamp() // GTH_STATUS_CADENCE_SECONDS)
                > int(last_status_at // GTH_STATUS_CADENCE_SECONDS)
            ):
                return f"gth_quarter_hour_heartbeat:{phase}"
            return None
    if changes:
        return "material_changes"
    return None


def run_status(
    args: argparse.Namespace,
    *,
    now: datetime,
    state_path: str,
    trading_date: str,
    runner: CommandRunner = default_runner,
) -> int:
    if not args.force and not within_status_window(now):
        print(json.dumps({"skipped": True, "reason": "outside_status_window"}))
        return 0

    previous = load_order_map_state(state_path)
    storage_settings = StorageSettings.from_env()
    payload = build_order_payload_with_retry(storage_settings, now=now)
    if _payload_is_thin(payload) and not args.force:
        # A normal slow-poll/rotation gap; the next run gets the full snapshot.
        print(json.dumps({"skipped": True, "reason": "thin_snapshot_sampling_gap"}))
        return 0
    fingerprint = _status_fingerprint(payload)
    changes = _status_material_changes(
        previous.get("status_fingerprint") or previous.get("fingerprint"),
        fingerprint,
    )
    template = render_status_template(payload, changes, now)

    if args.dry_run:
        print(template)
        print(json.dumps({"dry_run": True, "changes": changes}, ensure_ascii=False))
        return 0

    delivery_reason = (
        "forced"
        if args.force
        else _status_delivery_reason(
            previous,
            fingerprint,
            changes,
            now=now,
            trading_date=trading_date,
            position_risk=_has_open_position_risk(storage_settings),
        )
    )
    if delivery_reason is None:
        print(json.dumps({"skipped": True, "reason": "no_material_changes"}))
        return 0

    settings = NotificationSettings.from_env()
    research_only = payload.get("research_only") is True
    text, writer = generate_push_text(
        template,
        build_status_prompt(payload, template, load_previous_push()),
        settings,
        runner=runner,
        system=GLOBEX_CONTEXT_SYSTEM_PROMPT if research_only else None,
    )
    if writer != "template":
        valid = (
            globex_writer_output_valid(text, template)
            if research_only
            else actionable_writer_output_valid(text, template)
        )
        if not valid:
            text, writer = template, "template_validation_fallback"
    feishu_text = render_feishu_delivery_text(payload, changes, now, text)
    event_id = notification_event_id(
        "status",
        source="order_map_status",
        occurred_at=now,
        identity=json.dumps(fingerprint, sort_keys=True, separators=(",", ":")),
    )
    dispatch = dispatch_notification(
        settings,
        NotificationEnvelope(
            event_id=event_id,
            source="order_map_status",
            kind="status",
            lane="scheduled_report",
            occurred_at=now,
        ),
        title="SPX 15分钟市场状态",
        text=text,
        friend=True,
        feishu_text=feishu_text,
        runner=runner,
        attempted_at=now,
    )
    delivery_sinks = list(dispatch.sinks)
    delivered_ok = dispatch.delivered
    im_ok = any(s.sink == "feishu" and s.ok for s in delivery_sinks)
    bark_ok = any(s.sink == "bark" and s.ok for s in delivery_sinks)
    feishu_ok = any(s.sink == "feishu" and s.ok for s in delivery_sinks)

    if delivered_ok:
        persist_zero_dte_greeks_reference(payload, storage_settings)
        mark_sent(state_path, trading_date, fingerprint=fingerprint, now=now, kind="status")
        record_push("market_status", text, at=now.isoformat())
    result = {
        "text": text,
        "writer": writer,
        "im_ok": im_ok,
        "bark_ok": bark_ok,
        "feishu_ok": feishu_ok,
        "delivered_ok": delivered_ok,
        "changes": changes,
        "delivery_reason": delivery_reason,
    }
    persist_order_map_pricing_audit(
        payload,
        storage_settings,
        now=now,
        report_kind="status",
        template=template,
        result=result,
    )
    print(json.dumps(result, ensure_ascii=False))
    if not delivered_ok:
        return 1
    return 0


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Send SPX Spark order map push.")
    parser.add_argument("--dry-run", action="store_true", help="Print template only.")
    parser.add_argument(
        "--force", action="store_true", help="Skip time window and idempotency gate."
    )
    parser.add_argument(
        "--refresh",
        action="store_true",
        help="Re-push only when key levels moved materially since the last push.",
    )
    parser.add_argument(
        "--status",
        action="store_true",
        help="Push a market status report (fixed cadence, Beijing 14:15 -> US open).",
    )
    return parser.parse_args(argv)


def run_refresh(
    args: argparse.Namespace, *, now: datetime, state_path: str, trading_date: str
) -> int:
    if not args.force and not within_refresh_window(now):
        print(json.dumps({"skipped": True, "reason": "outside_refresh_window"}))
        return 0

    previous = load_order_map_state(state_path)
    if not args.force and previous.get("last_map_date") != trading_date:
        print(json.dumps({"skipped": True, "reason": "no_baseline_push_today"}))
        return 0
    # Cooldown is keyed on map pushes only (baseline + refreshes); the
    # interleaved status reports must not reset it.
    last_map_at = finite_float(previous.get("last_map_at"))
    cooldown = float(
        os.getenv("SPX_ORDER_MAP_REFRESH_COOLDOWN_SECONDS", "") or REFRESH_COOLDOWN_SECONDS_DEFAULT
    )
    if not args.force and last_map_at is not None and now.timestamp() - last_map_at < cooldown:
        print(json.dumps({"skipped": True, "reason": "refresh_cooldown"}))
        return 0

    storage_settings = StorageSettings.from_env()
    payload = build_order_payload_with_retry(storage_settings, now=now)
    if payload.get("research_only") is True and not args.dry_run:
        print(json.dumps({"skipped": True, "reason": "research_only_no_direct_map"}))
        return 0
    if _payload_is_thin(payload) and not args.force:
        print(json.dumps({"skipped": True, "reason": "thin_snapshot_sampling_gap"}))
        return 0
    fingerprint = _status_fingerprint(payload)
    changes = _status_material_changes(
        previous.get("map_fingerprint") or previous.get("fingerprint"),
        fingerprint,
    )

    if changes:
        header = f"【条件交易地图·更新】变化: {'; '.join(changes)}"
    else:
        header = "【条件交易地图·更新】关键位无实质变化，情景价随最新报价刷新"
    if args.dry_run:
        print(header)
        print(render_template(payload))
        print(json.dumps({"dry_run": True, "changes": changes}, ensure_ascii=False))
        return 0
    if not changes and not args.force:
        print(json.dumps({"skipped": True, "reason": "no_material_changes"}))
        return 0

    settings = NotificationSettings.from_env()
    result = send_order_map(
        payload, settings, now=now, extra_header=header, previous_push=load_previous_push()
    )
    persist_order_map_pricing_audit(
        payload,
        storage_settings,
        now=now,
        report_kind="refresh",
        template="\n".join((header, render_template(payload))),
        result=result,
    )
    if (
        result.get("delivered_ok")
        or result["im_ok"]
        or result["bark_ok"]
        or result.get("feishu_ok")
    ):
        persist_zero_dte_greeks_reference(payload, storage_settings)
        mark_sent(state_path, trading_date, fingerprint=fingerprint, now=now, kind="map")
        record_push("order_map_refresh", result["text"], at=now.isoformat())
    result["changes"] = changes
    print(json.dumps(result, ensure_ascii=False))
    if not (
        result.get("delivered_ok")
        or result["im_ok"]
        or result["bark_ok"]
        or result.get("feishu_ok")
    ):
        return 1
    return 0


def run(argv: list[str] | None = None, *, now: datetime | None = None) -> int:
    args = parse_args(argv)
    now = now or datetime.now(tz=timezone.utc)
    storage_settings = StorageSettings.from_env()
    state_path = default_state_path(storage_settings)
    trading_date = DEFAULT_MARKET_CALENDAR.research_expiry(now).isoformat()

    if args.status:
        return run_status(args, now=now, state_path=state_path, trading_date=trading_date)

    if args.refresh:
        return run_refresh(args, now=now, state_path=state_path, trading_date=trading_date)

    if not args.force and not args.dry_run:
        if not within_send_window(now):
            print(json.dumps({"skipped": True, "reason": "outside_send_window"}))
            return 0
        if already_sent(state_path, trading_date):
            print(json.dumps({"skipped": True, "reason": "already_sent"}))
            return 0

    payload = build_order_payload_with_retry(storage_settings, now=now)
    template = render_template(payload)

    if args.dry_run:
        print(template)
        print(json.dumps({"dry_run": True}))
        return 0
    if payload.get("research_only") is True:
        print(json.dumps({"skipped": True, "reason": "research_only_no_direct_map"}))
        return 0

    settings = NotificationSettings.from_env()
    result = send_order_map(payload, settings, now=now, previous_push=load_previous_push())
    persist_order_map_pricing_audit(
        payload,
        storage_settings,
        now=now,
        report_kind="baseline",
        template=template,
        result=result,
    )
    if (
        result.get("delivered_ok")
        or result["im_ok"]
        or result["bark_ok"]
        or result.get("feishu_ok")
    ):
        persist_zero_dte_greeks_reference(payload, storage_settings)
        mark_sent(
            state_path,
            trading_date,
            fingerprint=_status_fingerprint(payload),
            now=now,
            kind="map",
        )
        record_push("order_map", result["text"], at=now.isoformat())
    print(json.dumps(result, ensure_ascii=False))
    if not (
        result.get("delivered_ok")
        or result["im_ok"]
        or result["bark_ok"]
        or result.get("feishu_ok")
    ):
        return 1
    return 0


def main() -> None:
    raise SystemExit(run())
