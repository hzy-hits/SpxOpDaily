"""Runtime service for unified minute, option and decision-context frames."""

from __future__ import annotations

import argparse
import json
from dataclasses import replace
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from spx_spark.application.globex_trend.state import load_trend_state, trend_state_path
from spx_spark.application.market_features.composition import (
    build_decision_audit,
    build_decision_context,
)
from spx_spark.application.market_features.greek_decision import build_greek_decision
from spx_spark.application.market_features.market import (
    build_minute_market_frame,
    merge_minute_sample,
    normalized_market_sample,
    session_segment,
    update_volume_baselines,
)
from spx_spark.application.market_features.options import (
    build_option_structure_frame,
    merge_option_history,
)
from spx_spark.application.market_features.state import (
    append_audit,
    feature_state_path,
    load_json,
    projection_paths,
    save_json,
)
from spx_spark.application.market_features.trade_intent import evaluate_trade_intent
from spx_spark.application.market_features.trade_intent_runtime import process_trade_intent
from spx_spark.application.market_features.virtual_strategy import process_virtual_strategy
from spx_spark.application.order_map.level_decision_shadow import (
    load_level_decision_shadow,
)
from spx_spark.application.order_map.level_trigger_repricing import (
    default_level_trigger_repricing_path,
)
from spx_spark.config import StorageSettings
from spx_spark.features.exposure_map import build_exposure_map
from spx_spark.greek_reference import build_zero_dte_greeks_reference
from spx_spark.macro_event_clock import macro_event_state
from spx_spark.marketdata import as_utc
from spx_spark.options_map import build_options_map
from spx_spark.settings import load_app_settings
from spx_spark.settings.market_features import MarketFeatureSettings
from spx_spark.storage import LatestStateStore


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build unified market feature frames.")
    parser.add_argument("--json", action="store_true")
    return parser.parse_args(argv)


def run(argv: list[str] | None = None, *, now: datetime | None = None) -> int:
    args = parse_args(argv)
    evaluation_now = as_utc(now or datetime.now(tz=timezone.utc))
    app = load_app_settings()
    policy = app.market_features
    output: dict[str, Any] = {"ok": True, "at": evaluation_now.isoformat()}
    if not policy.enabled:
        output["skipped_reason"] = "disabled"
        if args.json:
            print(json.dumps(output, sort_keys=True))
        return 0

    storage = StorageSettings.from_env()
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
    )
    sample = normalized_market_sample(latest, now=evaluation_now, policy=policy)
    existing_samples = _dict_list(persisted.get("market_samples"))
    if len(existing_samples) < 5:
        existing_samples = _seed_samples_from_trend(trend, policy)
    samples = merge_minute_sample(
        existing_samples,
        sample,
        now=evaluation_now,
        policy=policy,
    )
    frame_samples = (
        samples
        if samples and samples[-1].get("at") == sample.get("at")
        else [*samples, sample]
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
    option_history = merge_option_history(option_history, option_frame, policy=policy)
    volume_baselines = update_volume_baselines(
        volume_baselines,
        market_frame,
        max_sessions=policy.volume_baseline_sessions,
    )
    level_decision = dict(load_level_decision_shadow(storage))
    macro_event = macro_event_state(evaluation_now)
    context = build_decision_context(
        market_frame,
        option_frame,
        now=evaluation_now,
        trend=trend,
        level_decision=level_decision,
        macro_event=macro_event,
        policy=policy,
    )
    repricing = load_json(default_level_trigger_repricing_path(storage))
    trade_intent = evaluate_trade_intent(
        context,
        market_frame,
        option_frame,
        latest,
        repricing,
        now=evaluation_now,
        feature_policy=policy,
        order_policy=app.order_map,
    )
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
    if contract_id:
        score = greek_decision.get("contract_scores", {}).get(contract_id)
        if isinstance(score, dict):
            trade_intent = {**trade_intent, "greek_confidence": score}
    context = replace(
        context,
        trade_intent=trade_intent,
        greek_decision=greek_decision,
    )
    intent_delivery = process_trade_intent(
        storage,
        trade_intent,
        now=evaluation_now,
    )
    gth_signal = load_json(
        Path(storage.data_root) / "latest" / "gth_dip_reclaim_signal.json"
    )
    virtual_strategy = process_virtual_strategy(
        storage,
        latest,
        trade_intent=trade_intent,
        gth_signal=gth_signal,
        option_structure=option_frame.structure,
        macro_event=macro_event,
        greek_decision=greek_decision,
        now=evaluation_now,
        policy=policy,
    )
    context = replace(context, virtual_strategy=virtual_strategy)
    previous_context = _dict(persisted.get("last_decision_context"))
    audit = build_decision_audit(context, previous=previous_context or None)
    projections = projection_paths(storage.data_root)
    save_json(projections["market"], market_frame.to_dict())
    save_json(projections["option"], option_frame.to_dict())
    save_json(projections["decision"], context.to_dict())
    if audit is not None:
        append_audit(
            storage.data_root,
            context.session_id,
            audit.to_dict(),
        )
    save_json(
        state_path,
        {
            "schema_version": 1,
            "updated_at": evaluation_now.isoformat(),
            "market_samples": samples,
            "option_history": option_history,
            "option_contracts": contracts,
            "volume_baselines": volume_baselines,
            "last_decision_context": context.to_dict(),
        },
    )
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
            "trade_intent_delivery": intent_delivery,
            "virtual_strategy": virtual_strategy,
        }
    )
    if args.json:
        print(json.dumps(output, sort_keys=True))
    return 0


def _dict(value: object) -> dict[str, Any]:
    return dict(value) if isinstance(value, dict) else {}


def _dict_list(value: object) -> list[dict[str, Any]]:
    return [dict(item) for item in value or [] if isinstance(item, dict)] if isinstance(value, list) else []


def _seed_samples_from_trend(
    trend: dict[str, Any],
    policy: MarketFeatureSettings,
) -> list[dict[str, Any]]:
    session_id = str(trend.get("session_id") or "")
    rows: list[dict[str, Any]] = []
    for item in trend.get("samples") or []:
        if not isinstance(item, dict):
            continue
        at = item.get("at")
        price = item.get("price")
        if not isinstance(at, str) or not isinstance(price, int | float):
            continue
        try:
            observed_at = as_utc(datetime.fromisoformat(at))
        except ValueError:
            continue
        rows.append(
            {
                "at": observed_at.isoformat(),
                "session_id": session_id,
                "segment": session_segment(observed_at, policy=policy),
                "instruments": {
                    "future:ES": {
                        "price": float(price),
                        "provider": item.get("provider"),
                        "source_at": item.get("source_at") or at,
                        "volume": None,
                        "quality": "live",
                    }
                },
                "es_by_provider": {},
            }
        )
    return rows


def main() -> None:
    raise SystemExit(run())


if __name__ == "__main__":
    main()
