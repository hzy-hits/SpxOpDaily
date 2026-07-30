"""Runtime service for unified minute, option and decision-context frames."""

from __future__ import annotations

import argparse
import hashlib
import json
from collections.abc import Callable, Mapping
from dataclasses import replace
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from spx_spark.application.globex_trend.state import load_trend_state, trend_state_path
from spx_spark.application.market_features.composition import (
    build_decision_audit,
    build_decision_context,
)
from spx_spark.application.market_features.confirmed_gate_audit import (
    reconcile_confirmed_gate,
)
from spx_spark.application.market_features.es_bar_consumer import (
    load_consumable_es_bars,
)
from spx_spark.application.market_features.es_bar_consumer_fence import (
    fence_rth_market_state as _fence_rth_market_state,
    fence_rth_market_state_inputs as _fence_rth_market_state_inputs,
    fence_rth_trade_intent_authority as _fence_rth_trade_intent_authority,
)
from spx_spark.application.market_features.greek_decision import build_greek_decision
from spx_spark.application.market_features.gamma_prearm_plan import (
    process_gamma_prearm_plan,
)
from spx_spark.application.market_features.gth_level_manual_candidate import (
    process_gth_level_manual_candidate,
)
from spx_spark.application.market_features.market import (
    build_minute_market_frame,
    merge_minute_sample,
    normalized_market_sample,
    update_volume_baselines,
)
from spx_spark.application.market_features.models import DecisionContext
from spx_spark.application.market_features.market_state_5m import score_market_state_5m
from spx_spark.application.market_features.market_state_5m_inputs import (
    build_market_state_5m_inputs,
    project_spx_equivalent_moving_averages,
    update_same_time_range_baselines,
)
from spx_spark.application.market_features.market_sample_seed import (
    seed_samples_from_trend,
)
from spx_spark.application.market_features.options import (
    build_option_structure_frame,
    level_decision_live_structure,
    merge_option_history,
    option_frame_has_usable_live_structure,
)
from spx_spark.application.market_features.play_outcome_stats import (
    PlayOutcomeStats,
    PlayOutcomeStatsProvider,
)
from spx_spark.application.market_features.prior_rth_context import (
    gth_position_fraction,
    process_prior_rth_context,
)
from spx_spark.application.market_features.state import (
    append_audit,
    feature_state_path,
    load_json,
    projection_paths,
    save_json,
)
from spx_spark.application.market_features.session_episode import (
    advance_session_episode,
    record_session_episode_transition,
)
from spx_spark.application.market_features.spring_gamma_v3 import (
    MODEL_VERSION as SPRING_GAMMA_V3_MODEL_VERSION,
    SCHEMA_VERSION as SPRING_GAMMA_V3_SCHEMA_VERSION,
    build_spring_gamma_v3_shadow,
)
from spx_spark.application.market_features.spring_gamma_v3_io import (
    latest_spring_gamma_v3_shadow_path,
    persist_spring_gamma_v3_shadow,
    spring_gamma_v3_prediction_due,
    validate_spring_gamma_v3_shadow,
)
from spx_spark.application.market_features import spring_gamma_operator
from spx_spark.application.market_features.wall_probability import (
    build_wall_probability_tenor_shadow,
)
from spx_spark.application.market_features.trade_candidate import (
    advance_trade_candidate,
    gate_trade_intent,
    virtual_entry_intent,
)
from spx_spark.application.market_features.trade_intent import (
    evaluate_trade_intent,
    trade_intent_policy_version,
)
from spx_spark.application.market_features.trade_intent_producer_ledger import (
    record_trade_intent_producer_observation,
    trade_intent_deadline_diagnostics,
)
from spx_spark.application.market_features.trade_intent_runtime import process_trade_intent
from spx_spark.application.market_features.virtual_strategy import process_virtual_strategy
from spx_spark.application.order_map.level_decision_shadow import (
    load_level_decision_shadow,
    run_level_decision_shadow,
)
from spx_spark.application.order_map.decision_consistency import coherent_level_decision
from spx_spark.application.order_map.models import level_decision_play
from spx_spark.application.order_map.level_trigger_repricing import (
    default_level_trigger_repricing_path,
)
from spx_spark.config import StorageSettings
from spx_spark.features.exposure_map import build_exposure_map
from spx_spark.greek_reference import build_zero_dte_greeks_reference
from spx_spark.macro_event_clock import macro_event_state
from spx_spark.market_calendar import DEFAULT_MARKET_CALENDAR
from spx_spark.marketdata import as_utc
from spx_spark.options_map import (
    build_options_map,
    group_spxw_option_quotes,
)
from spx_spark.provider_failover import new_entry_control_decision
from spx_spark.provider_failover_controller import (
    ProviderFailoverSettings,
    load_failover_control,
)
from spx_spark.settings import load_app_settings
from spx_spark.settings.market_features import MarketFeatureSettings
from spx_spark.storage import LatestStateStore


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build unified market feature frames.")
    parser.add_argument("--json", action="store_true")
    return parser.parse_args(argv)


def run(
    argv: list[str] | None = None,
    *,
    now: datetime | None = None,
    action_clock: Callable[[], datetime] | None = None,
) -> int:
    args = parse_args(argv)
    evaluation_now = as_utc(now or datetime.now(tz=timezone.utc))
    resolved_action_clock = _resolve_action_clock(
        evaluation_now,
        evaluation_time_injected=now is not None,
        action_clock=action_clock,
    )
    app = load_app_settings()
    policy = app.market_features
    output: dict[str, Any] = {"ok": True, "at": evaluation_now.isoformat()}
    if not policy.enabled:
        output["skipped_reason"] = "disabled"
        if args.json:
            print(json.dumps(output, sort_keys=True))
        return 0

    storage = StorageSettings.from_env()
    failover_settings = ProviderFailoverSettings.from_env()
    provider_entry_control = _provider_entry_control(
        failover_settings,
        now=evaluation_now,
    )
    play_stats_provider = PlayOutcomeStatsProvider(
        Path(storage.data_root) / "features",
        settings=policy,
        cache_path=Path(storage.data_root) / "latest" / "play_outcome_stats_cache.json",
    )
    state_path = feature_state_path(storage.data_root)
    persisted = load_json(state_path)
    trend = load_trend_state(trend_state_path(storage.data_root))
    latest = LatestStateStore(storage).load(now=evaluation_now)
    options_map = build_options_map(latest, storage_settings=storage)
    exposure_map = build_exposure_map(latest)
    option_history = _dict_list(persisted.get("option_history"))
    option_frame, contracts = build_option_structure_frame(
        latest,
        options_map,
        now=evaluation_now,
        history=option_history,
        previous_contracts=_dict(persisted.get("option_contracts")),
        policy=policy,
        exposure_map=exposure_map,
        last_usable_frame=_dict(persisted.get("last_usable_option_frame")),
    )
    last_usable_option_frame = _dict(persisted.get("last_usable_option_frame"))
    if option_frame_has_usable_live_structure(option_frame):
        last_usable_option_frame = option_frame.to_dict()
    sample = normalized_market_sample(latest, now=evaluation_now, policy=policy)
    latest_root = Path(storage.data_root) / "latest"
    # The independent sampler publishes state before its v2 lease. The
    # consumer exposes bars only when the lease, writer fence, accepted marker,
    # and source timestamps all describe one fresh publication.
    es_bars, es_bar_consumer = load_consumable_es_bars(
        storage,
        now=evaluation_now,
    )
    existing_samples = _dict_list(persisted.get("market_samples"))
    if len(existing_samples) < 5:
        existing_samples = seed_samples_from_trend(trend, policy)
    samples = merge_minute_sample(
        existing_samples,
        sample,
        now=evaluation_now,
        policy=policy,
    )
    frame_samples = (
        samples if samples and samples[-1].get("at") == sample.get("at") else [*samples, sample]
    )
    range_baseline_path = latest_root / "market_state_5m_range_baselines.json"
    range_baselines = load_json(range_baseline_path)
    if es_bar_consumer["ready"] is True:
        range_baselines = update_same_time_range_baselines(
            range_baselines,
            bars=es_bars,
            now=evaluation_now,
        )
        save_json(range_baseline_path, range_baselines)
    market_state_inputs = build_market_state_5m_inputs(
        bars=es_bars,
        market_samples=frame_samples,
        range_baselines=range_baselines,
        now=evaluation_now,
    )
    market_state_inputs = _fence_rth_market_state_inputs(
        market_state_inputs,
        es_bar_consumer=es_bar_consumer,
    )
    market_state_values = _dict(market_state_inputs.get("values"))
    rth_market_state = score_market_state_5m(
        now=evaluation_now,
        price_vs_vwap=market_state_values.get("price_vs_vwap"),
        vwap_slope=_number(market_state_values.get("vwap_slope")),
        opening_range_state=market_state_values.get("opening_range_state"),
        market_structure=market_state_values.get("market_structure"),
        efficiency_ratio=_number(market_state_values.get("efficiency_ratio")),
        vwap_cross_count=(
            int(value)
            if (
                (value := market_state_values.get("vwap_cross_count")) is not None
                and isinstance(value, int)
                and not isinstance(value, bool)
            )
            else None
        ),
        same_time_range_ratio=_number(market_state_values.get("same_time_range_ratio")),
        breadth_above_vwap=_number(market_state_values.get("breadth_above_vwap")),
    )
    rth_market_state = _fence_rth_market_state(
        rth_market_state,
        es_bar_consumer=es_bar_consumer,
    )
    volume_baselines = _dict(persisted.get("volume_baselines"))
    expected_move = option_frame.volatility.get("expected_move_points_0dte")
    atm_iv = option_frame.volatility.get("atm_iv_0dte")
    market_frame = build_minute_market_frame(
        frame_samples,
        now=evaluation_now,
        expected_move_points=(
            float(expected_move) if isinstance(expected_move, int | float) else None
        ),
        atm_iv=float(atm_iv) if isinstance(atm_iv, int | float) else None,
        structural_levels=option_frame.structure,
        volume_baselines=volume_baselines,
        policy=policy,
    )
    input_diagnostics = _dict(market_state_inputs.get("diagnostics"))
    sample_instruments = _dict(sample.get("instruments"))
    selected_es = _dict(sample_instruments.get("future:ES"))
    selected_es_identity = selected_es.get("contract_identity")
    moving_averages = project_spx_equivalent_moving_averages(
        _dict(input_diagnostics.get("moving_averages")),
        es_spx_basis_points=_number(market_frame.cross_asset.get("es_spx_basis_points")),
        basis_contract_identity=(
            selected_es_identity
            if isinstance(selected_es_identity, str) and selected_es_identity
            else None
        ),
    )
    market_state_inputs = {
        **market_state_inputs,
        "diagnostics": {
            **input_diagnostics,
            "moving_averages": moving_averages,
        },
    }
    rth_market_state["input_lineage"] = market_state_inputs
    market_frame = replace(
        market_frame,
        diagnostics={
            **market_frame.diagnostics,
            "es_bar_consumer": es_bar_consumer,
            "rth_market_state": rth_market_state,
        },
    )
    prior_session = process_prior_rth_context(
        storage.data_root,
        frame_samples,
        latest,
        now=evaluation_now,
    )
    market_frame = replace(
        market_frame,
        diagnostics={
            **market_frame.diagnostics,
            "prior_rth_context": prior_session,
        },
    )
    current_gth_position = gth_position_fraction(market_frame.es)
    option_history = merge_option_history(option_history, option_frame, policy=policy)
    volume_baselines = update_volume_baselines(
        volume_baselines,
        market_frame,
        max_sessions=policy.volume_baseline_sessions,
    )
    level_decision_refresh_error: str | None = None
    try:
        raw_level_decision = run_level_decision_shadow(
            storage,
            None,
            now=evaluation_now,
            policy=app.level_decision,
            notifications_enabled=True,
            live_structure=level_decision_live_structure(option_frame),
        )
    except Exception as exc:  # The last durable decision remains usable on refresh failure.
        level_decision_refresh_error = f"{type(exc).__name__}:{exc}"
        raw_level_decision = load_level_decision_shadow(storage)
    level_decision = coherent_level_decision(
        raw_level_decision,
        expiry=option_frame.front_expiry,
        structure=option_frame.structure,
        max_level_drift_points=policy.trade_structure_drift_points,
    )
    previous_session_episode = _dict(persisted.get("session_episode"))
    session_episode = advance_session_episode(
        previous_session_episode,
        session_id=market_frame.session_id,
        now=evaluation_now,
        spot=_number(option_frame.structure.get("underlier")),
        market=market_frame,
        options=option_frame,
        policy=policy,
    )
    record_session_episode_transition(
        storage,
        previous_session_episode or None,
        session_episode,
        now=evaluation_now,
    )
    macro_event = macro_event_state(evaluation_now)
    context = build_decision_context(
        market_frame,
        option_frame,
        now=evaluation_now,
        trend=trend,
        level_decision=level_decision,
        macro_event=macro_event,
        session_episode=session_episode,
        prior_session=prior_session,
        policy=policy,
    )
    repricing = load_json(default_level_trigger_repricing_path(storage))
    play_stats = _lookup_play_stats(play_stats_provider, context, policy=policy)
    trade_intent = evaluate_trade_intent(
        context,
        market_frame,
        option_frame,
        latest,
        repricing,
        now=evaluation_now,
        feature_policy=policy,
        order_policy=app.order_map,
        play_stats=play_stats,
    )
    trade_intent = _fence_rth_trade_intent_authority(
        trade_intent,
        es_bar_consumer=es_bar_consumer,
    )
    trade_intent = _apply_provider_entry_control(
        trade_intent,
        provider_entry_control,
    )
    trade_candidate = advance_trade_candidate(
        storage,
        latest,
        trade_intent,
        now=evaluation_now,
    )
    trade_intent = gate_trade_intent(trade_intent, trade_candidate)
    confirmed_gate = reconcile_confirmed_gate(
        storage,
        raw_level_decision,
        trade_intent,
        now=evaluation_now,
    )
    if trade_intent.get("status") == "trade_ready":
        trade_intent = {
            **trade_intent,
            "spring_gamma": spring_gamma_operator.spring_gamma_operator_view(
                load_json(latest_spring_gamma_v3_shadow_path(storage.data_root)),
                now=evaluation_now,
                expected_expiry=str(option_frame.front_expiry or ""),
            ),
        }
    expected_trade_intent_policy_version = trade_intent_policy_version(policy, app.order_map)
    producer_ledger, intent_delivery = _record_and_process_trade_intent(
        storage,
        trade_intent,
        now=evaluation_now,
        feature_policy=policy,
        order_policy=app.order_map,
        expected_policy_version=expected_trade_intent_policy_version,
        action_clock=resolved_action_clock,
    )
    producer_deadline = _dict(producer_ledger.get("deadline"))
    delivery_action_now = as_utc(
        datetime.fromisoformat(str(producer_deadline["action_revalidation_at"]))
    )

    # Research overlays enrich the persisted/output context only. They run
    # after the trade-critical producer ledger and durable delivery attempt.
    contract_id = str(trade_intent.get("contract_id") or "")
    focused = build_zero_dte_greeks_reference(
        latest,
        options_map=options_map,
        focus_contract_ids=(contract_id,) if contract_id else (),
        max_serialized_contracts=1 if contract_id else 0,
        serialized_scenario_names=(
            "clock_plus_5m",
            "clock_plus_15m",
            "clock_plus_30m",
            "iv_down_1vol",
            "iv_down_3vol",
        ),
    )
    greek_decision = build_greek_decision(
        focused,
        [trade_intent] if contract_id else [],
        macro_event=macro_event,
        policy=policy,
    )
    spring_gamma_v3 = _process_spring_gamma_v3_shadow(
        storage=storage,
        latest_state=latest,
        options_map=options_map,
        market_frame=market_frame,
        option_frame=option_frame,
        greek_reference=focused,
        exposure_map=exposure_map,
        level_decision=level_decision,
        now=evaluation_now,
        settings=app.spring_gamma_v3,
    )
    spring_gamma_snapshot = load_json(
        latest_spring_gamma_v3_shadow_path(storage.data_root)
    )
    if contract_id:
        score = greek_decision.get("contract_scores", {}).get(contract_id)
        if isinstance(score, dict):
            trade_intent = {**trade_intent, "greek_confidence": score}
    context = replace(
        context,
        trade_intent=trade_intent,
        greek_decision=greek_decision,
        trade_candidate=trade_candidate,
        confirmed_gate=confirmed_gate,
    )
    # Delivery may cross a process/network boundary.  Never open or close a
    # lifecycle episode from the evaluation clock or the earlier quote snapshot.
    action_now = as_utc(resolved_action_clock())
    action_latest = LatestStateStore(storage).load(now=action_now)
    action_macro_event = macro_event_state(action_now)
    action_provider_entry_control = _provider_entry_control(
        failover_settings,
        now=action_now,
    )
    gamma_prearm_plan = process_gamma_prearm_plan(
        storage,
        repricing,
        raw_level_decision,
        now=action_now,
        spring_gamma=spring_gamma_snapshot,
        prior_session=prior_session,
        gth_position_fraction=current_gth_position,
    )
    gth_signal = load_json(Path(storage.data_root) / "latest" / "gth_dip_reclaim_signal.json")
    gth_level_manual_candidate = process_gth_level_manual_candidate(
        storage,
        action_latest,
        raw_level_decision,
        trend_state=trend,
        spring_gamma=spring_gamma_snapshot,
        macro_event=action_macro_event,
        now=action_now,
        policy=policy,
        new_entries_allowed=action_provider_entry_control["allowed"] is True,
        new_entries_block_reason=str(action_provider_entry_control.get("reason") or "unknown"),
        prior_session=prior_session,
        gth_position_fraction=current_gth_position,
        play_stats=play_stats,
    )
    virtual_strategy = process_virtual_strategy(
        storage,
        action_latest,
        trade_intent=virtual_entry_intent(trade_candidate),
        gth_signal=gth_signal,
        option_structure=option_frame.structure,
        macro_event=action_macro_event,
        greek_decision=greek_decision,
        now=action_now,
        policy=policy,
        expected_trade_intent_policy_version=expected_trade_intent_policy_version,
        new_entries_allowed=action_provider_entry_control["allowed"] is True,
        new_entries_block_reason=str(action_provider_entry_control.get("reason") or "unknown"),
    )
    context = replace(
        context,
        virtual_strategy=virtual_strategy,
        # Retire the standalone dip-reclaim execution lane.  The signal remains
        # available to research/virtual accounting, while all operator actions
        # flow through the unified level/trend manual-live contract below.
        gth_manual_candidate={},
        gth_level_manual_candidate=gth_level_manual_candidate,
    )
    previous_context = _dict(persisted.get("last_decision_context"))
    audit = build_decision_audit(context, previous=previous_context or None)
    projections = projection_paths(storage.data_root)
    save_json(projections["market"], market_frame.to_dict())
    save_json(projections["option"], option_frame.to_dict())
    save_json(projections["decision"], context.to_dict())
    save_json(projections["session_episode"], session_episode)
    if audit is not None:
        append_audit(
            storage.data_root,
            context.session_id,
            audit.to_dict(),
        )
    state_payload: dict[str, Any] = {
        "schema_version": 1,
        "market_samples": samples,
        "option_history": option_history,
        "option_contracts": contracts,
        "last_usable_option_frame": last_usable_option_frame,
        "volume_baselines": volume_baselines,
        "session_episode": session_episode,
        "last_decision_context": context.to_dict(),
    }
    previous_state = {key: value for key, value in persisted.items() if key != "updated_at"}
    if previous_state == state_payload and isinstance(persisted.get("updated_at"), str):
        # Content unchanged: keep the prior timestamp so save_json can skip
        # the rewrite instead of churning a multi-MB file every cycle.
        state_payload["updated_at"] = persisted["updated_at"]
    else:
        state_payload["updated_at"] = evaluation_now.isoformat()
    save_json(state_path, state_payload)
    output.update(
        {
            "market_frame_id": market_frame.frame_id,
            "market_quality": market_frame.quality.value,
            "option_frame_id": option_frame.frame_id,
            "option_quality": option_frame.quality.value,
            "l1_quality": option_frame.l1.quality.value,
            "decision_context_id": context.context_id,
            "audit_appended": audit is not None,
            "trade_intent_status": trade_intent.get("status"),
            "trade_intent_producer_ledger": producer_ledger,
            "trade_intent_deadline": producer_deadline,
            "trade_intent_evaluation_to_action_revalidation_ms": (
                producer_deadline.get("evaluation_to_action_revalidation_ms")
            ),
            "trade_intent_action_revalidation_exceeded_ttl": (
                producer_deadline.get("action_revalidation_exceeded_ttl")
            ),
            "trade_intent_delivery": intent_delivery,
            "evaluation_at": evaluation_now.isoformat(),
            "action_revalidated_at": delivery_action_now.isoformat(),
            "action_quote_state_created_at": action_latest.created_at.isoformat(),
            "action_macro_event": action_macro_event,
            "action_provider_entry_control": action_provider_entry_control,
            "trade_candidate": trade_candidate,
            "confirmed_gate": confirmed_gate,
            "level_decision_refresh_error": level_decision_refresh_error,
            "virtual_strategy": virtual_strategy,
            "gamma_prearm_plan": gamma_prearm_plan,
            "gth_level_manual_candidate": gth_level_manual_candidate,
            "spring_gamma_v3_shadow": spring_gamma_v3,
        }
    )
    if args.json:
        print(json.dumps(output, sort_keys=True))
    return 0


def _record_and_process_trade_intent(
    storage: StorageSettings,
    trade_intent: Mapping[str, object],
    *,
    now: datetime,
    feature_policy: MarketFeatureSettings,
    order_policy: Any,
    expected_policy_version: str,
    action_now: datetime | None = None,
    action_clock: Callable[[], datetime] | None = None,
) -> tuple[dict[str, object], dict[str, object]]:
    """Persist producer evidence, then clock and perform action revalidation."""

    try:
        producer_ledger = record_trade_intent_producer_observation(
            storage,
            trade_intent,
            now=now,
        )
    except Exception as exc:  # noqa: BLE001 - diagnostics must not suppress delivery
        producer_ledger = {
            "ok": False,
            "observed_at": as_utc(now).isoformat(),
            "heartbeat": "write_failed",
            "delivery_expectation": (
                "write_failed" if trade_intent.get("status") == "trade_ready" else "not_trade_ready"
            ),
            "error": f"{type(exc).__name__}:{exc}",
        }
    revalidation_now = as_utc(
        action_clock()
        if action_clock is not None
        else action_now
        if action_now is not None
        else now
    )
    producer_ledger["deadline"] = trade_intent_deadline_diagnostics(
        trade_intent,
        evaluation_now=now,
        action_now=revalidation_now,
    )
    delivery = process_trade_intent(
        storage,
        trade_intent,
        now=now,
        feature_policy=feature_policy,
        order_policy=order_policy,
        expected_policy_version=expected_policy_version,
        action_now=revalidation_now,
    )
    return producer_ledger, delivery


def _provider_entry_control(
    settings: ProviderFailoverSettings,
    *,
    now: datetime,
) -> dict[str, object]:
    if settings.enabled:
        return new_entry_control_decision(
            load_failover_control(settings.state_path),
            now=now,
            max_age_seconds=settings.control_state_max_age_seconds,
        )
    return {
        "allowed": True,
        "reason": "provider_failover_disabled",
        "mode": None,
        "updated_at": None,
        "age_seconds": None,
        "max_age_seconds": settings.control_state_max_age_seconds,
    }


def _apply_provider_entry_control(
    trade_intent: Mapping[str, object],
    control: Mapping[str, object],
) -> dict[str, object]:
    """Attach provider incident telemetry without vetoing a valid manual card.

    The exact session-authoritative SPXW quote is revalidated by the intent
    contract itself.  A separate failover lifecycle must not overrule that
    fresher, instrument-specific evidence for a manual-only notification.
    """

    result = {**trade_intent, "provider_failover_control": dict(control)}
    if control.get("allowed") is not True and result.get("status") == "trade_ready":
        result["provider_incident_warning"] = str(control.get("reason") or "provider_incident")
    return result


def _process_spring_gamma_v3_shadow(
    *,
    storage: StorageSettings,
    latest_state: object,
    options_map: object,
    market_frame: object,
    option_frame: object,
    greek_reference: dict[str, Any],
    exposure_map: object,
    level_decision: dict[str, object],
    now: datetime,
    settings: object,
) -> dict[str, object]:
    """Evaluate and persist the isolated research shadow without failing the hot loop."""

    enabled = (
        settings.get("enabled", True)
        if isinstance(settings, Mapping)
        else getattr(settings, "enabled", True)
    )
    if enabled is False:
        return {
            "evaluated": False,
            "status": "disabled",
            "prediction_id": None,
            "reason": "shadow_disabled",
        }

    interval = 900
    session_id = "unknown"
    expected_expiry = DEFAULT_MARKET_CALENDAR.research_expiry(now).strftime("%Y%m%d")
    try:
        configured_interval = getattr(settings, "prediction_interval_seconds", interval)
        if isinstance(configured_interval, bool):
            raise ValueError("prediction_interval_seconds must be a positive integer")
        parsed_interval = int(configured_interval)
        if parsed_interval <= 0:
            raise ValueError("prediction_interval_seconds must be a positive integer")
        interval = parsed_interval
        market_payload = market_frame.to_dict()
        if not isinstance(market_payload, dict):
            raise TypeError("market_frame.to_dict() must return a mapping")
        session_id = str(market_payload.get("session_id") or "unknown")
        latest_path = latest_spring_gamma_v3_shadow_path(storage.data_root)
        latest_shadow = _reusable_spring_gamma_v3_shadow(
            load_json(latest_path),
            now=now,
            session_id=session_id,
            expected_expiry=expected_expiry,
        )
        if not spring_gamma_v3_prediction_due(
            latest_shadow,
            now=now,
            session_id=session_id,
            prediction_interval_seconds=interval,
        ):
            return {
                "evaluated": False,
                "status": str(latest_shadow.get("status") or "unknown"),
                "prediction_id": latest_shadow.get("prediction_id"),
            }

        shadow = build_spring_gamma_v3_shadow(
            market_frame=market_frame,
            option_frame=option_frame,
            greek_reference=greek_reference,
            exposure_map=exposure_map,
            now=now,
            expected_expiry=expected_expiry,
            settings=settings,
            level_decision=level_decision,
        )
        direction = shadow.get("direction")
        direction_decision = (
            str(direction.get("decision") or "abstain")
            if isinstance(direction, dict)
            else "abstain"
        )
        wall_probability = build_wall_probability_tenor_shadow(
            options_map=options_map,
            grouped_quotes=group_spxw_option_quotes(
                latest_state,
                storage_settings=storage,
            ),
            option_frame=option_frame,
            direction=direction_decision,
            now=now,
            horizons=getattr(settings, "horizons_minutes", (15, 30, 60)),
        )
        shadow = validate_spring_gamma_v3_shadow(
            _attach_wall_probability_shadow(shadow, wall_probability)
        )
    except Exception as exc:  # A research calculation must never stop production frames.
        shadow = _failed_spring_gamma_v3_shadow(
            now=now,
            session_id=session_id,
            expected_expiry=expected_expiry,
            error=exc,
        )

    try:
        persisted = persist_spring_gamma_v3_shadow(
            shadow,
            data_root=storage.data_root,
            prediction_interval_seconds=interval,
        )
    except Exception as exc:  # Preserve the production hot loop on research I/O failure.
        return {
            "evaluated": True,
            "status": "failed",
            "prediction_id": shadow.get("prediction_id"),
            "error": f"{type(exc).__name__}:{exc}",
        }
    return {
        "evaluated": True,
        "status": shadow.get("status"),
        "prediction_id": shadow.get("prediction_id"),
        **persisted,
    }


def _reusable_spring_gamma_v3_shadow(
    payload: dict[str, Any],
    *,
    now: datetime,
    session_id: str,
    expected_expiry: str,
) -> dict[str, Any]:
    """Return only a current-session shadow that may suppress this bucket."""

    try:
        record = validate_spring_gamma_v3_shadow(payload)
        text = str(record["as_of"]).strip()
        as_of = datetime.fromisoformat(f"{text[:-1]}+00:00" if text.endswith(("Z", "z")) else text)
    except (TypeError, ValueError):
        return {}
    if (
        record.get("session_id") != session_id
        or record.get("expiry") != expected_expiry
        or as_utc(as_of) > as_utc(now)
    ):
        return {}
    return record


def _attach_wall_probability_shadow(
    shadow: dict[str, object],
    wall_probability: dict[str, object],
) -> dict[str, object]:
    """Combine the two isolated shadows and preserve a complete input identity."""

    combined = dict(shadow)
    combined["wall_probability"] = wall_probability
    if wall_probability.get("status") != "ready":
        if combined.get("status") == "ready":
            direction = combined.get("direction")
            if isinstance(direction, dict):
                combined["direction"] = {**direction, "decision": "abstain"}
            combined.update(
                {
                    "status": "abstain",
                    "regime": "abstain",
                    "opportunity": "abstain",
                    "abstain": True,
                }
            )
        wall_reasons = [
            str(reason) for reason in wall_probability.get("abstain_reasons", []) if str(reason)
        ]
        combined["abstain_reasons"] = list(
            dict.fromkeys(
                [
                    *[str(reason) for reason in combined.get("abstain_reasons", []) if str(reason)],
                    *[f"wall_probability:{reason}" for reason in wall_reasons],
                ]
            )
        )

    direction_fingerprint = str(combined.get("input_fingerprint") or "")
    combined["direction_input_fingerprint"] = direction_fingerprint
    encoded = json.dumps(
        {
            "direction_input_fingerprint": direction_fingerprint,
            "wall_probability": wall_probability,
        },
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    ).encode()
    fingerprint = hashlib.sha256(encoded).hexdigest()
    combined["input_fingerprint"] = fingerprint
    combined["prediction_id"] = (
        f"spring-gamma-v3:{combined.get('session_id') or 'unknown'}:"
        f"{combined.get('expiry') or 'unknown'}:{fingerprint[:16]}"
    )
    return combined


def _failed_spring_gamma_v3_shadow(
    *,
    now: datetime,
    session_id: str,
    expected_expiry: str,
    error: Exception,
) -> dict[str, object]:
    error_code = f"{type(error).__name__}:{error}"
    fingerprint = hashlib.sha256(
        f"{now.isoformat()}|{session_id}|{expected_expiry}|{error_code}".encode()
    ).hexdigest()
    return {
        "schema_version": SPRING_GAMMA_V3_SCHEMA_VERSION,
        "model_version": SPRING_GAMMA_V3_MODEL_VERSION,
        "prediction_id": f"spring-gamma-v3:{session_id}:{expected_expiry}:{fingerprint[:16]}",
        "input_fingerprint": fingerprint,
        "as_of": now.isoformat(),
        "session_id": session_id,
        "session": "unknown",
        "expiry": expected_expiry,
        "status": "failed",
        "mode": "shadow",
        "direction_authority": "none",
        "action_authority": "none",
        "actionable": False,
        "automatic_ordering": False,
        "calibration_status": "uncalibrated_shadow",
        "direction": {"decision": "abstain"},
        "regime": "abstain",
        "opportunity": "abstain",
        "abstain": True,
        "abstain_reasons": ["shadow_runtime_failure"],
        "error": error_code,
    }


def _lookup_play_stats(
    provider: PlayOutcomeStatsProvider,
    context: DecisionContext,
    *,
    policy: MarketFeatureSettings,
) -> PlayOutcomeStats | None:
    if not policy.play_stats_enabled:
        return None
    level = context.level_decision
    play = level_decision_play(
        str(level.get("thesis") or "none"),
        str(level.get("direction") or ""),
    )
    level_kind = str(level.get("level_kind") or "")
    if play is None or not level_kind:
        return None
    return provider.lookup(play, level_kind)


def _system_utcnow() -> datetime:
    return datetime.now(tz=timezone.utc)


def _resolve_action_clock(
    evaluation_now: datetime,
    *,
    evaluation_time_injected: bool,
    action_clock: Callable[[], datetime] | None,
) -> Callable[[], datetime]:
    if action_clock is not None:
        return action_clock
    if evaluation_time_injected:
        return lambda: evaluation_now
    return _system_utcnow


def _dict(value: object) -> dict[str, Any]:
    return dict(value) if isinstance(value, dict) else {}


def _dict_list(value: object) -> list[dict[str, Any]]:
    return (
        [dict(item) for item in value or [] if isinstance(item, dict)]
        if isinstance(value, list)
        else []
    )


def _number(value: object) -> float | None:
    return float(value) if isinstance(value, int | float) else None


def main() -> None:
    # Direct CLI/legacy scheduler invocations share the persistent worker's
    # sole-owner lock. The hot worker calls run() inside its already-held lock.
    from spx_spark.application.runtime.market_features_hot_worker import (
        run_locked_market_features_once,
    )

    raise SystemExit(run_locked_market_features_once(run))


if __name__ == "__main__":
    main()
