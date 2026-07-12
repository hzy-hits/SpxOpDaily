"""Order-map orchestration: payload build, status/refresh/send runners."""

from __future__ import annotations

import argparse
import json
import os
import sys
import time as time_module
from dataclasses import asdict, replace
from datetime import datetime, timedelta, timezone
from typing import Any

from spx_spark.analytics.options.pricing import finite_float
from spx_spark.application.order_map.bias_machine import load_intraday_call_bias
from spx_spark.application.order_map.candidates import build_candidates
from spx_spark.application.order_map.delivery import send_order_map
from spx_spark.application.order_map.es_volume_attach import attach_es_volume_signal
from spx_spark.application.order_map.hl_volume import (
    attach_hl_volume_signal,
    default_hl_volume_sample_path,
)
from spx_spark.application.order_map.models import SHANGHAI_TZ
from spx_spark.application.order_map.prompts import build_status_prompt, render_status_template
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
from spx_spark.notifier.llm_writer import generate_push_text, load_previous_push, record_push
from spx_spark.notifier.missed_queue import append_missed
from spx_spark.notifier.model import CommandRunner, default_runner
from spx_spark.notifier.sinks import any_delivery_ok, deliver_trade_push, im_delivery_ok
from spx_spark.options_map import build_options_map
from spx_spark.storage import LatestState, LatestStateStore


def build_order_payload(state: LatestState, *, now: datetime | None = None) -> dict[str, Any]:
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
    )
    greeks_audit_reference = build_zero_dte_greeks_reference(
        replace(state, as_of=now),
        options_map=options_map,
        focus_contract_ids=(candidate.contract_id for candidate in candidates),
        max_serialized_contracts=2,
    )
    greeks_reference = {
        **greeks_audit_reference,
        "serialized_contract_count": 0,
        "contracts": [],
    }
    beijing = now.astimezone(SHANGHAI_TZ)

    # Day move vs expected move: the writer's anti-FOMO anchor. "The drop has
    # already consumed 120% of today's EM" is the number that talks a reader
    # out of shorting the bottom of a slide or panic-selling into a wall band.
    spx_quote = state.best_quote("index:SPX")
    prior_close = finite_float(spx_quote.close) if spx_quote is not None else None
    day_move_points = (
        round(pricing_spot - prior_close, 1) if pricing_spot is not None and prior_close else None
    )
    em_used_fraction = None
    if day_move_points is not None and expected_move_points and expected_move_points > 0:
        em_used_fraction = round(abs(day_move_points) / expected_move_points, 2)

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
        "pricing_allowed": resolution.pricing_allowed,
        "research_only": resolution.research_only,
        "expiry": expiry,
        "expected_move_points": expected_move_points,
        "candidates": [asdict(candidate) for candidate in candidates],
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
            _wall_ladder_payload(state, options_map, pricing_spot, now=now)
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
            "em_used_fraction": em_used_fraction,
        },
        "rn_density": (
            front.rn_density.to_dict()
            if resolution.pricing_allowed and front is not None and front.rn_density
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


def build_order_payload_with_retry(
    storage_settings: StorageSettings,
    *,
    now: datetime,
    attempts: int = 7,
    delay_seconds: float = 10.0,
) -> dict[str, Any]:
    """Reload latest state for thin snapshots or a stale action candidate.

    Thin snapshots happen during slow-poll windows (the stream blocks ~30-50s
    without flushing) and option line rotation gaps. A single intended play can
    also fall outside the 15-second actionability window while the other plays
    remain present. The retry budget spans one full option rotation, and every
    attempt rebuilds the whole payload against an advancing evaluation time.
    """
    payload: dict[str, Any] = {}
    state: LatestState | None = None
    started_at = time_module.monotonic()
    evaluation_now = now
    for attempt in range(attempts):
        if attempt:
            elapsed_seconds = max(time_module.monotonic() - started_at, 0.0)
            evaluation_now = now + timedelta(seconds=elapsed_seconds)
        state = LatestStateStore(storage_settings).load(now=evaluation_now)
        payload = build_order_payload(state, now=evaluation_now)
        if not (_payload_is_thin(payload) or _payload_has_retryable_candidate_gap(payload)):
            break
        if attempt < attempts - 1:
            time_module.sleep(delay_seconds)
    if state is not None:
        attach_es_volume_signal(
            payload,
            state,
            sample_path=default_es_volume_sample_path(storage_settings),
            now=evaluation_now,
        )
        attach_hl_volume_signal(
            payload,
            state,
            storage_settings=storage_settings,
            sample_path=default_hl_volume_sample_path(storage_settings),
            now=evaluation_now,
        )
    return payload



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
        # Normal sampling gap (slow poll / line rotation), not an outage:
        # skip quietly, the next 15-minute run will have full data.
        print(json.dumps({"skipped": True, "reason": "thin_snapshot_sampling_gap"}))
        return 0
    fingerprint = payload_fingerprint(payload)
    changes = material_changes(previous.get("fingerprint"), fingerprint)
    # Combined push: status narrative + the order-map limit table used to be
    # two interleaved 30-minute pushes; the map template rides along so the
    # writer (and the raw fallback) always carries concrete limit prices.
    template = render_status_template(payload, changes, now)
    if payload.get("research_only") is not True:
        template = "\n".join((template, render_template(payload)))

    if args.dry_run:
        print(template)
        print(json.dumps({"dry_run": True, "changes": changes}, ensure_ascii=False))
        return 0

    settings = NotificationSettings.from_env()
    research_only = payload.get("research_only") is True
    if research_only:
        # Research status is deliberately deterministic: an unconstrained
        # writer response must never turn proxy context into trade language.
        text, writer = template, "template"
    else:
        text, writer = generate_push_text(
            template,
            build_status_prompt(payload, template, load_previous_push()),
            settings,
            runner=runner,
        )
    delivery_sinks = deliver_trade_push(
        settings,
        title="研究状态" if research_only else "市场状态",
        text=text,
        kind="status",
        lane="ops" if research_only else "trade",
        friend=not research_only,
        runner=runner,
    )
    delivered_ok = any_delivery_ok(delivery_sinks)
    if not research_only and not im_delivery_ok(delivery_sinks):
        append_missed(
            settings.missed_queue_path,
            text,
            kind="order_map_research" if research_only else "order_map_status",
            at=now,
        )
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
    }
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
    fingerprint = payload_fingerprint(payload)
    changes = material_changes(previous.get("fingerprint"), fingerprint)

    if changes:
        header = f"【挂单地图·更新】变化: {'; '.join(changes)}"
    else:
        header = "【挂单地图·更新】关键位无实质变化，限价随最新报价刷新"
    if args.dry_run:
        print(header)
        print(render_template(payload))
        print(json.dumps({"dry_run": True, "changes": changes}, ensure_ascii=False))
        return 0

    settings = NotificationSettings.from_env()
    result = send_order_map(
        payload, settings, now=now, extra_header=header, previous_push=load_previous_push()
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
            fingerprint=payload_fingerprint(payload),
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
