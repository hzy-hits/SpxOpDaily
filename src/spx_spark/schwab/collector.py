"""Schwab option-chain collector: fetch chains and persist normalized quotes."""

from __future__ import annotations

import json
import logging
import time
from contextlib import redirect_stdout
from io import StringIO
from datetime import datetime
from typing import Any

from spx_spark.config import SchwabSettings, SchwabStreamSettings, StorageSettings, env_csv
from spx_spark.market_calendar import DEFAULT_MARKET_CALENDAR, ET
from spx_spark.marketdata import as_utc
from spx_spark.provider_adapter import persist_provider_snapshot
from spx_spark.settings import load_app_settings
from spx_spark.settings.schwab import SchwabSettingsSlice
from spx_spark.schwab.chain_cycle import collect_chain_cycle
from spx_spark.schwab.collector_io import (
    collect_quote_batches as _collect_quote_batches,
    fetch_chain,
    gateway_request_window as _gateway_request_window,
)
from spx_spark.schwab.collector_state import (
    COLLECTOR_STATE_FILE_NAME,
    CollectorBudgetState,
    chain_is_due,
    collector_state_path,
    load_collector_budget_state,
    prune_request_timestamps,
    record_requests,
    save_collector_budget_state,
)
from spx_spark.schwab.hot_lane import hot_plan_is_fresh
from spx_spark.schwab.market_data_plan import (
    cadence_seconds,
    collection_profile,
    effective_profile,
    planner_tick_seconds as profile_planner_tick_seconds,
    planned_requests_per_minute,
)
from spx_spark.schwab.quota_machine import (
    QuotaPolicy,
    QuotaState,
    advance_quota_state,
)
from spx_spark.schwab.quote_lane import collect_quote_lane
from spx_spark.schwab.request_models import (
    CollectionProfile,
    QuotaMode,
    SchwabLane,
)
from spx_spark.schwab.symbols import (
    canonical_underlier_for_schwab,
    resolved_schwab_canonical_quote_symbols,
    resolved_schwab_quote_symbols,
    schwab_option_chain_underliers,
    schwab_quote_symbols,
)
from spx_spark.schwab.verifier import build_schwab_client


LOGGER = logging.getLogger(__name__)

__all__ = [
    "COLLECTOR_STATE_FILE_NAME",
    "CollectorBudgetState",
    "chain_is_due",
    "prune_request_timestamps",
]


def run(
    argv: list[str] | None = None,
    *,
    now: datetime | None = None,
    typed_settings: SchwabSettingsSlice | None = None,
) -> int:
    del argv
    evaluation_now = as_utc(now or datetime.now(tz=ET))
    settings = SchwabSettings.from_env()
    storage_settings = StorageSettings.from_env()
    stream_settings = SchwabStreamSettings.from_env(data_root=storage_settings.data_root)
    typed_settings = typed_settings or load_app_settings().schwab
    client = build_schwab_client(settings)
    if client is None:
        print(json.dumps({"ok": False, "skipped": True, "reason": "missing_schwab_auth"}))
        return 0

    configured_quote_symbols = env_csv(
        "SCHWAB_COLLECT_QUOTES",
        ",".join(schwab_quote_symbols()),
    )
    quote_symbols = resolved_schwab_quote_symbols(
        configured_quote_symbols,
        now=evaluation_now,
    )
    if stream_settings.mode == "live":
        streaming_symbols = set(
            resolved_schwab_canonical_quote_symbols(
                stream_settings.canonical_symbols,
                now=evaluation_now,
            )
        )
        quote_symbols = [symbol for symbol in quote_symbols if symbol not in streaming_symbols]
    chain_symbols = env_csv(
        "SCHWAB_COLLECT_CHAINS",
        ",".join(schwab_option_chain_underliers()),
    )
    chain_canonicals = [canonical_underlier_for_schwab(symbol) for symbol in chain_symbols]

    state_path = collector_state_path(storage_settings)
    budget_state = load_collector_budget_state(state_path)
    original_state = budget_state.to_dict()
    quota_policy = QuotaPolicy(
        nominal_requests_per_minute=typed_settings.capacity.nominal_requests_per_minute,
        planned_requests_per_minute=typed_settings.capacity.planned_requests_per_minute,
    )
    quota_state = QuotaState(
        mode=QuotaMode(budget_state.quota_mode),
        consecutive_successes=budget_state.quota_consecutive_successes,
        stable_windows=budget_state.quota_stable_windows,
    )
    gateway_window = _gateway_request_window(client)
    quota_state = advance_quota_state(
        quota_state,
        gateway_window,
        policy=quota_policy,
        retry_after_elapsed=(
            quota_state.mode is QuotaMode.THROTTLED
            and gateway_window.throttled == 0
            and gateway_window.failures == 0
        ),
    )
    current_expiry, next_expiry = DEFAULT_MARKET_CALENDAR.research_expiries(evaluation_now)
    current_expiry_text = current_expiry.strftime("%Y%m%d")
    burst = bool(budget_state.burst_until and evaluation_now < budget_state.burst_until)
    profile = effective_profile(
        collection_profile(evaluation_now, burst=burst),
        quota_state.mode,
    )
    front_plan_cadence = cadence_seconds(
        SchwabLane.FRONT_CHAIN,
        profile=profile,
        policy=typed_settings.cadence,
        underlier="SPX",
    )
    if not hot_plan_is_fresh(
        hot_expiry=budget_state.hot_expiry,
        expected_expiry=current_expiry_text,
        planned_at=budget_state.chain_last_fetched_at.get("SPX:front"),
        now=evaluation_now,
        max_age_seconds=max(
            typed_settings.hot_lane.max_plan_age_seconds,
            2 * front_plan_cadence,
        ),
    ):
        budget_state.hot_symbols = []
        budget_state.hot_expiry = None
        budget_state.hot_reference_spot = None
    request_ceiling = typed_settings.capacity.planned_requests_per_minute
    requests_before = record_requests(budget_state, count=0, now=evaluation_now)

    quote_counts: dict[str, int] = {}
    errors: list[str] = []
    request_count = 0
    chains_fetched: list[str] = []
    chains_skipped: list[str] = []
    chain_lanes_fetched: list[str] = []
    chain_lanes_skipped: list[str] = []
    coverage_summary: dict[str, dict[str, Any]] = {}
    chain_as_of: dict[str, str | None] = {
        canonical: (
            budget_state.chain_last_fetched_at[canonical].isoformat()
            if canonical in budget_state.chain_last_fetched_at
            else None
        )
        for canonical in chain_canonicals
    }

    quote_result = collect_quote_lane(
        client=client,
        quote_symbols=quote_symbols,
        budget_state=budget_state,
        profile=profile,
        quota_mode=quota_state.mode,
        now=evaluation_now,
        request_ceiling=request_ceiling,
        requests_used=requests_before + request_count,
        settings=settings,
        typed_settings=typed_settings,
        storage_settings=storage_settings,
        require_hot_plan=True,
        already_attempted=False,
        collect_batches=_collect_quote_batches,
        persist=persist_provider_snapshot,
    )
    request_count += quote_result.request_count
    quote_counts.update(quote_result.quote_counts)
    errors.extend(quote_result.errors)

    chain_cycle = collect_chain_cycle(
        client=client,
        chain_symbols=chain_symbols,
        quote_symbols=quote_symbols,
        current_expiry=current_expiry,
        next_expiry=next_expiry,
        now=evaluation_now,
        profile=profile,
        quota_mode=quota_state.mode,
        budget_state=budget_state,
        settings=settings,
        typed_settings=typed_settings,
        storage_settings=storage_settings,
        available_requests=max(request_ceiling - requests_before - request_count, 0),
        fetch=fetch_chain,
        persist=persist_provider_snapshot,
    )
    request_count += chain_cycle.request_count
    quote_counts.update(chain_cycle.quote_counts)
    errors.extend(chain_cycle.errors)
    chains_fetched.extend(chain_cycle.chains_fetched)
    chains_skipped.extend(chain_cycle.chains_skipped)
    chain_lanes_fetched.extend(chain_cycle.lanes_fetched)
    chain_lanes_skipped.extend(chain_cycle.lanes_skipped)
    chain_as_of.update(chain_cycle.chain_as_of)
    coverage_summary.update(chain_cycle.coverage)

    quote_result = collect_quote_lane(
        client=client,
        quote_symbols=quote_symbols,
        budget_state=budget_state,
        profile=profile,
        quota_mode=quota_state.mode,
        now=evaluation_now,
        request_ceiling=request_ceiling,
        requests_used=requests_before + request_count,
        settings=settings,
        typed_settings=typed_settings,
        storage_settings=storage_settings,
        require_hot_plan=False,
        already_attempted=quote_result.attempted,
        collect_batches=_collect_quote_batches,
        persist=persist_provider_snapshot,
    )
    request_count += quote_result.request_count
    quote_counts.update(quote_result.quote_counts)
    errors.extend(quote_result.errors)

    requests_last_minute = record_requests(
        budget_state,
        count=request_count,
        now=evaluation_now,
    )
    gateway_window = _gateway_request_window(client)
    quota_state = advance_quota_state(
        quota_state,
        gateway_window,
        policy=quota_policy,
        retry_after_elapsed=(
            quota_state.mode is QuotaMode.THROTTLED
            and gateway_window.throttled == 0
            and gateway_window.failures == 0
        ),
    )
    budget_state.quota_mode = quota_state.mode.value
    budget_state.quota_consecutive_successes = quota_state.consecutive_successes
    budget_state.quota_stable_windows = quota_state.stable_windows
    if budget_state.to_dict() != original_state:
        save_collector_budget_state(state_path, budget_state)

    if requests_last_minute > request_ceiling:
        LOGGER.warning(
            "Schwab collector request budget soft guardrail exceeded: "
            "%s requests in trailing 60s (warning threshold %s/min, gateway cap 120/min)",
            requests_last_minute,
            request_ceiling,
        )

    quota_deferred = any("planned_request_ceiling" in error for error in errors)
    ok = bool(quote_counts) or bool(chains_skipped) or quota_deferred
    summary = {
        "ok": ok,
        "symbols": list(quote_counts.keys()),
        "quote_counts": quote_counts,
        "errors": errors,
        "request_count": request_count,
        "requests_last_minute": requests_last_minute,
        "scheduled_successes_last_minute": requests_last_minute,
        "chains_fetched": chains_fetched,
        "chains_skipped": chains_skipped,
        "chain_as_of": chain_as_of,
        "chain_lanes_fetched": chain_lanes_fetched,
        "chain_lanes_skipped": chain_lanes_skipped,
        "coverage": coverage_summary,
        "profile": profile.value,
        "planned_requests_per_minute": planned_requests_per_minute(
            profile,
            typed_settings.cadence,
        ),
        "request_ceiling": request_ceiling,
        "quota_mode": quota_state.mode.value,
        "gateway_request_window": {
            "attempts": gateway_window.attempts,
            "retries": gateway_window.retries,
            "throttled": gateway_window.throttled,
            "failures": gateway_window.failures,
            "response_bytes": gateway_window.response_bytes,
        },
        "hot_symbol_count": len(budget_state.hot_symbols),
    }
    print(json.dumps(summary, sort_keys=True))
    return 0 if ok or not quote_symbols and not chain_symbols else 1


def main() -> None:
    raise SystemExit(run())


def run_loop(*, planner_tick_seconds: float | None = None) -> int:
    """Long-running single owner; cadence remains lane-driven inside each cycle."""

    typed_settings = load_app_settings().schwab
    if planner_tick_seconds is not None and planner_tick_seconds <= 0:
        raise ValueError("planner tick must be positive")
    last_emit = 0.0
    while True:
        started = time.monotonic()
        output = StringIO()
        with redirect_stdout(output):
            exit_code = run(typed_settings=typed_settings)
        text = output.getvalue().strip()
        payload: dict[str, Any] = {}
        if text:
            try:
                parsed = json.loads(text.splitlines()[-1])
                payload = parsed if isinstance(parsed, dict) else {}
            except json.JSONDecodeError:
                payload = {"ok": False, "error": "invalid_collector_output"}
        now_monotonic = time.monotonic()
        should_emit = bool(
            exit_code
            or payload.get("request_count")
            or payload.get("errors")
            or now_monotonic - last_emit >= 30.0
        )
        if should_emit:
            print(text or json.dumps(payload, sort_keys=True), flush=True)
            last_emit = now_monotonic
        elapsed = time.monotonic() - started
        try:
            profile = CollectionProfile(str(payload.get("profile")))
        except ValueError:
            profile = CollectionProfile.OFF_HOURS
        resolved_tick_seconds = planner_tick_seconds or profile_planner_tick_seconds(
            profile,
            typed_settings.cadence,
        )
        time.sleep(max(resolved_tick_seconds - elapsed, 0.05))


def loop_main() -> None:
    try:
        raise SystemExit(run_loop())
    except KeyboardInterrupt:
        raise SystemExit(0) from None


if __name__ == "__main__":
    main()
