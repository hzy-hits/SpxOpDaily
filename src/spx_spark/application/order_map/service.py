"""Order-map orchestration: payload build, status/refresh/send runners."""

from __future__ import annotations

import argparse
import json
import os
import time as time_module
from dataclasses import asdict, replace
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from spx_spark.analytics.options.pricing import finite_float
from spx_spark.application.globex_trend.state import load_trend_state, trend_state_path
from spx_spark.application.market_features.greek_decision import build_greek_decision
from spx_spark.application.market_features.state import load_json, projection_paths
from spx_spark.application.notifications.report_enqueue import (
    daily_report_semantic,
    enqueue_order_map_status,
    material_report_identity,
    order_map_status_semantic,
    stable_report_slot,
)
from spx_spark.application.order_map.audit_persistence import (
    persist_order_map_pricing_audit,
    persist_zero_dte_greeks_reference as _persist_zero_dte_greeks_reference,
)
from spx_spark.application.order_map.bias_machine import load_intraday_call_bias
from spx_spark.application.order_map.call_spread_shadow import (
    build_skew_spread_shadows as _build_spread_shadows,
)
from spx_spark.application.order_map.candidate_presentation import (
    apply_candidate_presentation as _apply_candidate_presentation,
)
from spx_spark.application.order_map.candidates import build_candidates
from spx_spark.application.order_map.convexity_idea_radar import (
    attach_convexity_idea_radar,
)
from spx_spark.application.order_map.decision_consistency import (
    apply_decision_projections,
)
from spx_spark.application.order_map.delivery import send_order_map
from spx_spark.application.order_map.desk_projection_export import (
    persist_desk_map_projection,
    rust_report_owner_enabled,
)
from spx_spark.application.order_map.es_volume_attach import attach_es_volume_signal
from spx_spark.application.order_map.frozen_structure import attach_frozen_option_structure
from spx_spark.application.order_map.hl_volume import (
    attach_hl_volume_signal,
    default_hl_volume_sample_path,
)
from spx_spark.application.order_map.level_decision_shadow import load_level_decision_shadow
from spx_spark.application.order_map.level_trigger_repricing import (
    default_level_trigger_repricing_path,
)
from spx_spark.application.order_map.models import SHANGHAI_TZ
from spx_spark.application.order_map.prompts import (
    render_feishu_delivery_text,
    render_operator_status_brief,
    render_status_template,
)
from spx_spark.application.order_map.status_delivery import (
    GTH_STATUS_PHASES,
    _status_fingerprint,
    _status_material_changes,
    status_delivery_reason as _status_delivery_reason,
)
from spx_spark.application.order_map.render import render_template
from spx_spark.application.order_map.report_clock import rth_report_slot
from spx_spark.application.order_map.research import (
    _index_value,
    _research_candidates,
    _research_wall_ladder,
    _strike_price_coverage,
    _wall_ladder_payload,
)
from spx_spark.application.order_map.signal_machine import annotate_call_bias_with_signal_mode
from spx_spark.application.order_map.spot import (
    hyperliquid_sp500_price,
    report_trigger_coordinate,
    resolve_spx_spot,
)
from spx_spark.application.order_map.spring_gamma_projection import (
    attach_spring_gamma_v3_shadow,
)
from spx_spark.application.order_map.strategy_select import build_strategy_decision
from spx_spark.application.order_map.state import (
    REFRESH_COOLDOWN_SECONDS_DEFAULT,
    already_sent,
    current_session_is_gth,
    default_state_path,
    load_order_map_state,
    mark_sent,
    payload_fingerprint,
    session_phase,
    within_refresh_window,
    within_send_window,
    within_status_window,
)
from spx_spark.application.order_map.volume_machine import default_es_volume_sample_path
from spx_spark.config import NotificationSettings, StorageSettings
from spx_spark.greek_reference import (
    build_zero_dte_greeks_reference,
    write_zero_dte_greeks_snapshot,
)
from spx_spark.intraday_strategy import signed_gex_sign_method
from spx_spark.market_calendar import DEFAULT_MARKET_CALENDAR
from spx_spark.macro_event_clock import macro_event_state
from spx_spark.notifier.llm_writer import load_previous_push, record_push
from spx_spark.notifier.model import CommandRunner, NotificationEnvelope, default_runner
from spx_spark.notifier.dispatcher import inspect_notification_event, notification_event_exists
from spx_spark.notifier.unified_delivery import notification_event_id
from spx_spark.options_map import build_options_map
from spx_spark.ibkr.position_watcher import default_positions_path, load_snapshot
from spx_spark.storage import LatestState, LatestStateStore
from spx_spark.settings import load_app_settings
from spx_spark.settings.order_map import DEFAULT_ORDER_MAP_POLICY, OrderMapPolicy


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
    expiry = (
        front.expiry
        if front is not None
        else DEFAULT_MARKET_CALENDAR.research_expiry(now).strftime("%Y%m%d")
    )
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
    trigger_coordinate = report_trigger_coordinate(state, resolution, now=now)
    skew_spread_shadows = _build_spread_shadows(
        state, expiry=expiry, spot=pricing_spot, now=now, policy=policy
    )
    coverage_reference = (
        resolution.research_price if resolution.research_price is not None else pricing_spot
    )
    strike_price_coverage = _strike_price_coverage(
        state,
        expiry=expiry,
        reference_price=coverage_reference,
        as_of=now,
    )

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
        "strike_price_coverage": strike_price_coverage,
        **skew_spread_shadows,
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


def persist_zero_dte_greeks_reference(
    payload: dict[str, Any],
    storage_settings: StorageSettings,
) -> None:
    _persist_zero_dte_greeks_reference(
        payload, storage_settings, writer=write_zero_dte_greeks_snapshot
    )


def _payload_is_thin(payload: dict[str, Any]) -> bool:
    """True when the snapshot caught a mid-rotation flush (missing spot/OI/plays)."""
    research_reference = payload.get("research_reference")
    if not isinstance(research_reference, dict):
        research_reference = {}
    if payload.get("research_only") is True and research_reference.get("price") is not None:
        return False
    underlier = payload.get("underlier") if isinstance(payload.get("underlier"), dict) else {}
    if underlier.get("price") is None:
        return True
    warnings = payload.get("warnings")
    no_open_interest = isinstance(warnings, list) and any(
        "no open interest" in str(item) for item in warnings
    )
    if no_open_interest and current_session_is_gth(payload, {}):
        return False
    if not payload.get("candidates"):
        return True
    if no_open_interest:
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
        payload["gth_path_ranks"] = load_json(
            Path(storage_settings.data_root) / "latest" / "gth_path_ranks.json"
        )
        payload["gth_level_manual_candidate"] = load_json(
            Path(storage_settings.data_root) / "latest" / "gth_level_manual_candidate.json"
        )
        payload["strategy_distribution_forecast"] = load_json(
            Path(storage_settings.data_root) / "latest" / "strategy_distribution_forecast.json"
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
        attach_frozen_option_structure(payload, option_frame)
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
    attach_spring_gamma_v3_shadow(
        payload,
        storage_settings.data_root,
        settings=getattr(app, "spring_gamma_v3", None),
        now=evaluation_now,
    )
    attach_convexity_idea_radar(payload, now=evaluation_now)
    if state is None:
        raise RuntimeError("order map did not load a latest state")
    payload["strategy_decision"] = build_strategy_decision(
        payload,
        state,
        evaluation_now,
        data_root=storage_settings.data_root,
        probability_settings=app.strategy_distribution,
    )
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


def _has_open_position_risk(storage_settings: StorageSettings) -> bool:
    snapshot = load_snapshot(default_positions_path(storage_settings))
    return bool(snapshot and any(position.qty != 0 for position in snapshot.positions))


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
    current_rth_slot = rth_report_slot(now)
    if _payload_is_thin(payload):
        warnings = payload.setdefault("warnings", [])
        if isinstance(warnings, list):
            warnings.append(
                "rth_heartbeat_degraded_snapshot"
                if current_rth_slot is not None
                else "gth_heartbeat_degraded_snapshot"
            )
    fingerprint = _status_fingerprint(payload)
    changes = _status_material_changes(
        previous.get("status_fingerprint") or previous.get("fingerprint"),
        fingerprint,
    )
    template = render_status_template(payload, changes, now)

    if args.dry_run:
        operator_brief = render_operator_status_brief(payload, changes, now)
        print(operator_brief)
        print(json.dumps({"dry_run": True, "changes": changes}, ensure_ascii=False))
        return 0

    rust_owner = rust_report_owner_enabled()
    rust_projection = persist_desk_map_projection(
        payload,
        [] if rust_owner else changes,
        now=now,
        trading_date=trading_date,
        storage=storage_settings,
        published_at=datetime.now(timezone.utc),
    )
    # The quarter-hour timer remains the standardized snapshot recorder.  Human
    # delivery is a separate material-change/desk-map decision below.
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
        snapshot_result = {
            "skipped": True,
            "reason": "snapshot_only_no_material_changes",
            "text": "",
            "writer": "deterministic_snapshot_only",
            "delivery_outcome": "suppressed_snapshot_only",
            "changes": changes,
            "report_slot_key": current_rth_slot.key if current_rth_slot is not None else None,
        }
        persist_order_map_pricing_audit(
            payload,
            storage_settings,
            now=now,
            report_kind="status_snapshot",
            template=template,
            result=snapshot_result,
        )
        print(json.dumps(snapshot_result, ensure_ascii=False))
        return 0

    semantic = order_map_status_semantic(
        trading_date=trading_date,
        now=now,
        delivery_reason=delivery_reason,
        current_rth_slot=current_rth_slot,
        fingerprint=fingerprint,
    )
    if rust_owner and semantic.lane == "scheduled_report":
        mirrored_result = {
            "skipped": True,
            "reason": "rust_report_owner",
            "accepted": False,
            "mirrored": True,
            "projection_id": rust_projection["projection_id"],
            "text": "",
            "writer": "rust_report_owner",
            "delivery_outcome": "rust_projection_persisted",
            "changes": changes,
            "report_slot_key": rust_projection["source_slot"],
        }
        persist_order_map_pricing_audit(
            payload,
            storage_settings,
            now=now,
            report_kind="status_snapshot",
            template=template,
            result=mirrored_result,
        )
        print(json.dumps(mirrored_result, ensure_ascii=False))
        return 0

    operator_brief = render_operator_status_brief(payload, changes, now)
    settings = NotificationSettings.from_env()
    text = operator_brief
    writer = "deterministic_desk_map"
    status_title = (
        "SPX GTH Desk Map（条件观察）"
        if payload.get("research_only") is True
        or str(fingerprint.get("status_phase") or "") in GTH_STATUS_PHASES
        else "SPX Desk Map"
    )
    feishu_text = render_feishu_delivery_text(payload, changes, now, text)
    if notification_event_exists(settings, semantic.event_id):
        inspection = inspect_notification_event(
            settings,
            NotificationEnvelope(
                event_id=semantic.event_id,
                source="order_map_status",
                kind="status",
                lane=semantic.lane,
                occurred_at=semantic.occurred_at,
                expires_at=semantic.expires_at,
            ),
            title=status_title,
            text=text,
            friend=True,
            feishu_text=feishu_text,
        )
        if not inspection.acceptable:
            rejected_result = {
                "skipped": False,
                "reason": f"outbox_reconciliation_failed:{inspection.reason}",
                "accepted": False,
                "duplicate": False,
                "notification_event_id": semantic.event_id,
                "text": text,
                "writer": writer,
                "delivery_outcome": "reconciliation_rejected",
                "changes": changes,
                "occurred_at": semantic.occurred_at.isoformat(),
                "report_slot_key": semantic.slot_key,
                "payload_matches": inspection.payload_matches,
                "targets_match": inspection.targets_match,
                "event_status": inspection.event_status,
            }
            persist_order_map_pricing_audit(
                payload,
                storage_settings,
                now=now,
                report_kind="status",
                template=template,
                result=rejected_result,
            )
            print(json.dumps(rejected_result, ensure_ascii=False))
            return 1
        mark_sent(state_path, trading_date, fingerprint=fingerprint, now=now, kind="status")
        duplicate_result = {
            "skipped": True,
            "reason": "outbox_already_accepted",
            "accepted": True,
            "duplicate": True,
            "notification_event_id": semantic.event_id,
            "text": operator_brief,
            "writer": "deterministic_desk_map",
            "delivery_outcome": "duplicate_already_accepted",
            "changes": changes,
            "occurred_at": semantic.occurred_at.isoformat(),
            "report_slot_key": semantic.slot_key,
        }
        persist_order_map_pricing_audit(
            payload,
            storage_settings,
            now=now,
            report_kind="status",
            template=template,
            result=duplicate_result,
        )
        print(json.dumps(duplicate_result, ensure_ascii=False))
        return 0
    result = enqueue_order_map_status(
        settings,
        text=text,
        feishu_text=feishu_text,
        title=status_title,
        trading_date=trading_date,
        now=now,
        delivery_reason=delivery_reason,
        current_rth_slot=current_rth_slot,
        fingerprint=fingerprint,
    )
    accepted = bool(result["accepted"])
    if accepted:
        persist_zero_dte_greeks_reference(payload, storage_settings)
        mark_sent(state_path, trading_date, fingerprint=fingerprint, now=now, kind="status")
        record_push("market_status", text, at=now.isoformat())
    result.update(
        text=text,
        writer=writer,
        changes=changes,
        delivery_reason=delivery_reason,
    )
    persist_order_map_pricing_audit(
        payload,
        storage_settings,
        now=now,
        report_kind="status",
        template=template,
        result=result,
    )
    print(json.dumps(result, ensure_ascii=False))
    if not accepted:
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
        help="Push an exchange-calendar GTH/RTH status heartbeat.",
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
    refresh_occurred_at = stable_report_slot(now, cadence_minutes=30)
    refresh_identity = material_report_identity(
        "order_map_refresh",
        trading_date=trading_date,
        occurred_at=refresh_occurred_at,
        fingerprint=fingerprint,
    )
    refresh_event_id = notification_event_id(
        "order_map",
        source="order_map",
        occurred_at=refresh_occurred_at,
        identity=refresh_identity,
    )
    if notification_event_exists(settings, refresh_event_id):
        mark_sent(state_path, trading_date, fingerprint=fingerprint, now=now, kind="map")
        print(
            json.dumps(
                {
                    "skipped": True,
                    "reason": "outbox_already_accepted",
                    "accepted": True,
                    "duplicate": True,
                    "notification_event_id": refresh_event_id,
                }
            )
        )
        return 0
    result = send_order_map(
        payload,
        settings,
        now=now,
        extra_header=header,
        previous_push=load_previous_push(),
        event_identity=refresh_identity,
        occurred_at=refresh_occurred_at,
    )
    persist_order_map_pricing_audit(
        payload,
        storage_settings,
        now=now,
        report_kind="refresh",
        template="\n".join((header, render_template(payload))),
        result=result,
    )
    accepted = bool(result.get("accepted"))
    if accepted:
        persist_zero_dte_greeks_reference(payload, storage_settings)
        mark_sent(state_path, trading_date, fingerprint=fingerprint, now=now, kind="map")
        record_push("order_map_refresh", result["text"], at=now.isoformat())
    result["changes"] = changes
    print(json.dumps(result, ensure_ascii=False))
    if not accepted:
        return 1
    return 0


def run(argv: list[str] | None = None, *, now: datetime | None = None) -> int:
    args = parse_args(argv)
    now = now or datetime.now(tz=timezone.utc)

    # The status lane must keep publishing the normalized desk-map projection,
    # but every legacy baseline/refresh path writes a scheduled report.  Fence
    # those paths before loading data or constructing an outbox event once Rust
    # owns the report lane.  --force must never bypass single-writer ownership.
    if not args.status and not args.dry_run and rust_report_owner_enabled():
        print(
            json.dumps(
                {
                    "skipped": True,
                    "reason": "rust_report_owner",
                    "accepted": False,
                    "writer": "rust_report_owner",
                    "delivery_outcome": "suppressed_legacy_scheduled_report",
                }
            )
        )
        return 0

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
    semantic = daily_report_semantic(
        payload,
        now=now,
        kind="order_map",
        source="order_map",
        identity_label="baseline_trading_date",
    )
    if notification_event_exists(settings, semantic.event_id):
        mark_sent(
            state_path,
            trading_date,
            fingerprint=_status_fingerprint(payload),
            now=now,
            kind="map",
        )
        print(
            json.dumps(
                {
                    "skipped": True,
                    "reason": "outbox_already_accepted",
                    "accepted": True,
                    "duplicate": True,
                    "notification_event_id": semantic.event_id,
                }
            )
        )
        return 0
    result = send_order_map(payload, settings, now=now, previous_push=load_previous_push())
    persist_order_map_pricing_audit(
        payload,
        storage_settings,
        now=now,
        report_kind="baseline",
        template=template,
        result=result,
    )
    accepted = bool(result.get("accepted"))
    if accepted:
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
    if not accepted:
        return 1
    return 0


def main() -> None:
    raise SystemExit(run())
