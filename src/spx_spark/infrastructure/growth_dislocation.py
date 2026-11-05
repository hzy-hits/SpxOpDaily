"""Scheduled Growth Dislocation LEAPS scanner using the shared Schwab/outbox stack."""

from __future__ import annotations

import csv
import hashlib
import json
import math
from collections import Counter
from collections.abc import Callable, Mapping
from dataclasses import asdict, dataclass, replace
from datetime import date, datetime, timedelta, timezone
from functools import partial
from itertools import zip_longest
from pathlib import Path
from time import monotonic
from typing import Any
from urllib.error import HTTPError, URLError

from spx_spark.analytics.growth_dislocation import (
    POLICY_VERSION,
    apply_crowding,
    candidate_sort_key,
    core_display_row,
    new_core_member,
    price_features,
    price_location_52w,
    priority_sort_key,
    quote_preference_scores,
    score_candidate,
    select_target_leaps,
    spread_mid_ratio,
)
from spx_spark.config import IbkrSettings, NotificationSettings, SchwabSettings
from spx_spark.ibkr.adapter import (
    IvPercentileSnapshot,
    iv_percentile_snapshot_from_cached_payload,
    iv_percentile_snapshot_to_payload,
)
from spx_spark.ibkr.verifier import (
    IbkrHistoricalDataUnavailable,
    fetch_iv_percentile_snapshots,
)
from spx_spark.market_calendar import DEFAULT_MARKET_CALENDAR, ET
from spx_spark.notifier.dispatcher import EnqueueResult, enqueue_notification
from spx_spark.notifier.model import NotificationEnvelope
from spx_spark.schwab.growth_dislocation import (
    DailyClose,
    EquityQuote,
    LeapsChain,
    OptionContract,
    fetch_daily_closes,
    fetch_equity_quote_batch,
    fetch_leaps_chain,
)
from spx_spark.schwab.verifier import SchwabClient, build_schwab_client
from spx_spark.settings import load_app_settings
from spx_spark.settings.growth_dislocation import GrowthDislocationSettings
from spx_spark.state_io import (
    atomic_write_json_secure,
    exclusive_state_lock,
    read_json_object,
)

SCHEMA_VERSION = "growth_dislocation_leaps.v7"
STATE_SCHEMA_VERSION = 3
SCHEDULE_MINUTE = 30
SCHEDULE_GRACE_MINUTES = 5
DAILY_SUMMARY_HOUR_ET = 20
MAX_CLOSE_HISTORY = 270
ProviderError = (
    HTTPError,
    URLError,
    TimeoutError,
    OSError,
    ValueError,
    IbkrHistoricalDataUnavailable,
)
IvPercentileFetcher = Callable[[list[str]], Mapping[str, IvPercentileSnapshot]]

@dataclass(frozen=True, slots=True)
class UniverseMember:
    symbol: str
    provider_symbol: str
    company: str
    sector: str
    subindustry: str
    classification_level: str
    sector_benchmark: str
    memberships: tuple[str, ...]

    @property
    def crowding_group(self) -> str:
        if self.classification_level == "subindustry" and self.subindustry != "Unknown":
            return f"subindustry:{self.subindustry}"
        return f"sector:{self.sector_benchmark}"

@dataclass(frozen=True, slots=True)
class Universe:
    members: tuple[UniverseMember, ...]
    metadata: tuple[str, ...]

@dataclass(frozen=True, slots=True)
class ScanOutcome:
    document: dict[str, Any]
    notification: EnqueueResult | None

def scheduled_mode(now: datetime) -> str | None:
    ny = _aware(now).astimezone(ET)
    if (
        ny.hour == DAILY_SUMMARY_HOUR_ET
        and 0 <= ny.minute < SCHEDULE_GRACE_MINUTES
        and DEFAULT_MARKET_CALENDAR.is_trading_day(ny.date())
    ):
        return "daily"
    if (
        SCHEDULE_MINUTE <= ny.minute < SCHEDULE_MINUTE + SCHEDULE_GRACE_MINUTES
        and DEFAULT_MARKET_CALENDAR.is_rth_open(ny)
    ):
        return "rth"
    return None

def load_universe(path: Path) -> Universe:
    lines = path.read_text(encoding="utf-8").splitlines()
    metadata = tuple(line[1:].strip() for line in lines if line.startswith("#"))
    reader = csv.DictReader(line for line in lines if line and not line.startswith("#"))
    members: list[UniverseMember] = []
    seen: set[str] = set()
    for row in reader:
        symbol = str(row.get("symbol") or "").strip().upper()
        provider_symbol = str(row.get("provider_symbol") or "").strip().upper()
        if not symbol or not provider_symbol or symbol in seen:
            raise ValueError("Growth-dislocation universe has blank or duplicate symbols")
        seen.add(symbol)
        members.append(
            UniverseMember(
                symbol=symbol,
                provider_symbol=provider_symbol,
                company=str(row.get("company") or "").strip(),
                sector=str(row.get("sector") or "Unknown").strip(),
                subindustry=str(row.get("subindustry") or "Unknown").strip(),
                classification_level=str(row.get("classification_level") or "fallback").strip(),
                sector_benchmark=str(row.get("sector_benchmark") or "SPY").strip().upper(),
                memberships=tuple(
                    item for item in str(row.get("memberships") or "").split("|") if item
                ),
            )
        )
    if not members:
        raise ValueError("Growth-dislocation universe is empty")
    return Universe(tuple(members), metadata)

def scan_once(
    *,
    now: datetime,
    mode: str,
    client: SchwabClient,
    policy: GrowthDislocationSettings,
    universe: Universe,
    data_root: Path,
    iv_percentile_fetcher: IvPercentileFetcher | None = None,
    notification_settings: NotificationSettings | None = None,
    enqueue: Callable[..., EnqueueResult] = enqueue_notification,
) -> ScanOutcome:
    if mode not in {"rth", "daily"}:
        raise ValueError("Growth-dislocation scan mode must be rth or daily")
    at = _aware(now).astimezone(timezone.utc)
    ny = at.astimezone(ET)
    latest_path = data_root / "latest" / "growth_dislocation_leaps.json"
    state_path = data_root / "runtime" / "growth_dislocation_state.json"
    with exclusive_state_lock(state_path):
        state = _valid_state(read_json_object(state_path))
        document, fingerprint = _build_document(
            now=at,
            ny=ny,
            mode=mode,
            client=client,
            policy=policy,
            universe=universe,
            state=state,
            iv_percentile_fetcher=iv_percentile_fetcher,
        )
        at = datetime.fromisoformat(document["generated_at"])
        ny = at.astimezone(ET)
        current_symbols = {str(row["symbol"]) for row in document["all_candidates"]}
        previous_symbols = {str(symbol) for symbol in state.get("active_candidate_symbols", [])}
        complete = bool(document.get("scan_complete"))
        added_symbols, exited_symbols = _apply_core_pool(
            document=document,
            state=state,
            mode=mode,
            at=at,
            policy=policy,
        )
        document["added_symbols"] = added_symbols
        document["exited_symbols"] = exited_symbols
        strict_added = sorted(current_symbols - previous_symbols) if complete else []
        document["strict_added_symbols"] = strict_added
        fingerprint = _material_fingerprint(document)
        document["material_fingerprint"] = fingerprint
        atomic_write_json_secure(latest_path, document)
        notification: EnqueueResult | None = None
        should_notify = mode == "daily" or bool(added_symbols or exited_symbols or strict_added)
        if should_notify and notification_settings is not None:
            title, text = render_notification(document)
            _, feishu_text = render_notification(document, discoveries=True)
            # New intraday discovery events belong only to Feishu. Existing daily
            # routing and its legacy text remain unchanged.
            delivery_settings = notification_settings
            if mode != "daily":
                delivery_settings = replace(
                    notification_settings, bark_enabled=False, bark_friend_enabled=False,
                )
            slot_at = ny.replace(second=0, microsecond=0).astimezone(timezone.utc)
            envelope = NotificationEnvelope(
                event_id=_notification_id(mode, slot_at, fingerprint),
                source="growth_dislocation",
                kind="growth_dislocation_scan",
                lane="growth_dislocation",
                occurred_at=slot_at,
                expires_at=slot_at + timedelta(hours=4),
            )
            notification = enqueue(
                delivery_settings,
                envelope,
                title=title,
                text=text,
                friend=False,
                feishu_text=feishu_text,
                enqueued_at=at,
            )
        state["schema_version"] = STATE_SCHEMA_VERSION
        state["policy_version"] = POLICY_VERSION
        state["last_scan_at"] = at.isoformat()
        state["last_scan_mode"] = mode
        state["priority_symbols"] = [row["symbol"] for row in document["all_candidates"]]
        if complete:
            state["active_candidate_symbols"] = sorted(current_symbols)
        else:
            state["active_candidate_symbols"] = sorted(previous_symbols)
        state.pop("pending_added_symbols", None)
        atomic_write_json_secure(state_path, state)
    return ScanOutcome(document=document, notification=notification)

def _build_document(
    *,
    now: datetime,
    ny: datetime,
    mode: str,
    client: SchwabClient,
    policy: GrowthDislocationSettings,
    universe: Universe,
    state: dict[str, Any],
    iv_percentile_fetcher: IvPercentileFetcher | None,
) -> tuple[dict[str, Any], str]:
    scan_started_at, started_clock = now, monotonic()
    budget = policy.rth_request_budget if mode == "rth" else policy.daily_request_budget
    requests_used = 0
    errors: list[str] = []
    quotes: dict[str, EquityQuote] = {}
    requested_quote_symbols = 0
    provider_symbols = list(
        dict.fromkeys(
            [member.provider_symbol for member in universe.members]
            + [member.sector_benchmark for member in universe.members]
        )
    )
    for offset in range(0, len(provider_symbols), 500):
        if requests_used >= budget:
            break
        batch = provider_symbols[offset : offset + 500]
        requests_used += 1
        requested_quote_symbols += len(batch)
        try:
            quotes.update(fetch_equity_quote_batch(client, batch))
        except ProviderError as exc:
            errors.append(f"quotes:{type(exc).__name__}")

    now = scan_started_at + timedelta(seconds=monotonic() - started_clock)
    ny = now.astimezone(ET)
    rejection_counts: Counter[str] = Counter()
    membership_observations: dict[str, dict[str, Any]] = {}
    price_hard_survivors: list[tuple[UniverseMember, EquityQuote, float]] = []
    for member in universe.members:
        quote = quotes.get(member.provider_symbol)
        reason, location = _hard_filter(member, quote, policy=policy, now=now, mode=mode)
        membership_observations[member.symbol] = _membership_observation(
            member=member,
            quote=quote,
            price_location=location,
            screen_reason=reason,
        )
        if reason is not None:
            rejection_counts[reason] += 1
            continue
        assert quote is not None and location is not None
        price_hard_survivors.append((member, quote, location))

    quotes_ready = len(price_hard_survivors)
    price_hard_survivors = _detail_order(
        price_hard_survivors, state, policy=policy, request_budget=budget - requests_used,
    )
    selected_symbols = {member.symbol for member, _quote, _location in price_hard_survivors}
    for symbol, observation in membership_observations.items():
        observation["selected_this_run"] = symbol in selected_symbols
        if observation["screen_reason"] is None and symbol not in selected_symbols:
            observation["screen_reason"] = "not_evaluated_this_run"

    iv_cache = _mapping_state(state, "iv_percentiles")
    iv_snapshots = _fresh_iv_snapshots(iv_cache, trading_day=ny.date())
    price_survivor_symbols = [
        _ibkr_provider_symbol(member.symbol) for member, _quote, _location in price_hard_survivors
    ]
    refresh_symbols = (
        price_survivor_symbols
        if mode == "daily"
        else [symbol for symbol in price_survivor_symbols if symbol not in iv_snapshots]
    )
    refreshed_iv_symbols: set[str] = set()
    if refresh_symbols:
        if iv_percentile_fetcher is None:
            errors.append("ibkr_ivp:not_configured")
        else:
            try:
                fetched = iv_percentile_fetcher(refresh_symbols)
                for raw_symbol, snapshot in fetched.items():
                    symbol = _ibkr_provider_symbol(raw_symbol)
                    if (
                        snapshot.ivp_13w is None
                        or snapshot.ivp_26w is None
                        or snapshot.ivp_52w is None
                    ):
                        continue
                    iv_snapshots[symbol] = snapshot
                    iv_cache[symbol] = iv_percentile_snapshot_to_payload(snapshot)
                    refreshed_iv_symbols.add(symbol)
            except ProviderError as exc:
                errors.append(f"ibkr_ivp:{type(exc).__name__}")

    hard_survivors: list[tuple[UniverseMember, EquityQuote, float]] = []
    for item in price_hard_survivors:
        member = item[0]
        snapshot = iv_snapshots.get(_ibkr_provider_symbol(member.symbol))
        observation = membership_observations[member.symbol]
        observation.update(
            {
                "ivp_13w": snapshot.ivp_13w if snapshot else None,
                "ivp_26w": snapshot.ivp_26w if snapshot else None,
                "ivp_52w": snapshot.ivp_52w if snapshot else None,
                "iv_history_as_of": (
                    (snapshot.as_of_date or snapshot.observed_at.astimezone(ET).date()).isoformat()
                    if snapshot else None
                ),
                "iv_observed_at": snapshot.observed_at.isoformat() if snapshot else None,
                "iv_refreshed_this_run": _ibkr_provider_symbol(member.symbol) in refreshed_iv_symbols,
            }
        )
        if (
            snapshot is None
            or snapshot.ivp_13w is None
            or snapshot.ivp_26w is None
            or snapshot.ivp_52w is None
        ):
            observation["screen_reason"] = "ibkr_iv_percentile_missing"
            rejection_counts["ibkr_iv_percentile_missing"] += 1
            continue
        if any(not 0.0 <= value <= 1.0 for value in (snapshot.ivp_13w, snapshot.ivp_26w, snapshot.ivp_52w)):
            observation["screen_reason"] = "ibkr_iv_percentile_invalid"
            rejection_counts["ibkr_iv_percentile_invalid"] += 1
            continue
        observation["screen_reason"] = None
        hard_survivors.append(item)

    history_cache = _mapping_state(state, "price_histories")
    # Only this run's validated broker history may feed scores. A cached bar
    # can be an earlier intraday observation, even when its date looks complete.
    histories: dict[str, Any] = {}
    detail_cache = _mapping_state(state, "details")
    required_benchmarks = list(
        dict.fromkeys(
            member.sector_benchmark for member, _quote, _location in hard_survivors
        )
    )
    for symbol in required_benchmarks:
        if requests_used >= budget:
            break
        requests_used += 1
        history, reason = _refresh_close_history(
            client, symbol, now=now, mode=mode, quote=quotes.get(symbol),
            max_quote_age=policy.quote_max_age_seconds,
        )
        if reason:
            errors.append(f"history:{symbol}:{reason}")
        else:
            histories[symbol] = _close_history_payload(history)
            history_cache[symbol] = histories[symbol]

    prioritized = hard_survivors
    detailed_now: set[str] = set()
    detail_evaluated_now: set[str] = set()
    target_leaps_rejected: set[str] = set()
    for member, quote, _location in prioritized:
        if requests_used >= budget:
            break
        requests_used += 1
        history, reason = _refresh_close_history(
            client, member.provider_symbol, now=now, mode=mode, quote=quote,
            max_quote_age=policy.quote_max_age_seconds,
        )
        if reason:
            errors.append(f"history:{member.symbol}:{reason}")
            continue
        histories[member.symbol] = _close_history_payload(history)
        history_cache[member.symbol] = histories[member.symbol]
        if requests_used >= budget:
            break
        requests_used += 1
        try:
            chain = fetch_leaps_chain(
                client,
                member.provider_symbol,
                as_of=ny.date(),
                min_dte=policy.min_leaps_dte,
                max_dte=policy.max_leaps_dte,
                strike_count=policy.chain_strike_count,
            )
        except ProviderError as exc:
            errors.append(f"chain:{member.symbol}:{type(exc).__name__}")
            continue
        now = scan_started_at + timedelta(seconds=monotonic() - started_clock)
        ny = now.astimezone(ET)
        detail_evaluated_now.add(member.symbol)
        membership_observations[member.symbol].update(
            {
                "leaps_depth_refreshed": True,
                "max_option_dte": chain.max_dte,
            }
        )
        contract_rejections: dict[str, int] = {}
        target = select_target_leaps(
            chain.contracts,
            policy,
            spot=quote.last,
            rejection_counts=contract_rejections,
        )
        membership_observations[member.symbol]["contract_rejection_counts"] = contract_rejections
        if target is None:
            reason = "target_leaps_hard_filter" if chain.contracts else "target_leaps_missing"
            membership_observations[member.symbol]["screen_reason"] = reason
            # Missing/invalid fields, an empty response, or stale quotes cannot
            # prove that a previously eligible member is now untradeable.
            membership_observations[member.symbol]["strict_rejection_confirmed"] = (
                chain.complete and bool(contract_rejections)
                and set(contract_rejections) <= {
                    "dte_out_of_range", "delta_out_of_range", "spread_above_limit",
                    "open_interest_below_limit", "extrinsic_above_limit",
                }
                and not chain.delayed
                and all(
                    contract.quote_at is not None
                    and 0 <= (now - contract.quote_at).total_seconds()
                    and (
                        (now - contract.quote_at).total_seconds() <= policy.option_quote_max_age_seconds
                        if mode == "rth"
                        else contract.quote_at.astimezone(ET).date() == ny.date()
                    )
                    for contract in chain.contracts
                )
            )
            rejection_counts[reason] += 1
            detail_cache.pop(member.symbol, None)
            target_leaps_rejected.add(member.symbol)
            continue
        detail_cache[member.symbol] = _detail_payload(target, chain, fetched_at=now)
        detailed_now.add(member.symbol)

    # Quotes received during network I/O are not future data relative to the
    # completed scan. Preserve the injected reference time for historical tests.
    now = scan_started_at + timedelta(seconds=monotonic() - started_clock)
    ny = now.astimezone(ET)
    strict_candidates: list[dict[str, Any]] = []
    warming: list[dict[str, Any]] = []
    for member, quote, location in hard_survivors:
        if member.symbol in target_leaps_rejected:
            continue
        history = _close_history(histories.get(member.symbol))
        row, reasons = _candidate_row(
            member=member,
            quote=quote,
            price_location=location,
            history=history,
            detail=_mapping(detail_cache.get(member.symbol)),
            benchmark_histories=histories,
            policy=policy,
            now=now,
            ny=ny,
            mode=mode,
            detail_current=member.symbol in detailed_now,
            iv_snapshot=iv_snapshots.get(_ibkr_provider_symbol(member.symbol)),
        )
        if reasons:
            warming.append(row)
            membership_observations[member.symbol].update(
                {
                    "data_quality_reasons": row["data_quality_reasons"],
                    "screen_reason": row["data_quality_reasons"][0],
                }
            )
            for reason in reasons:
                rejection_counts[reason] += 1
            continue
        scored = score_candidate(row, policy)
        if scored is None:
            membership_observations[member.symbol]["screen_reason"] = (
                "iv_or_liquidity_hard_filter"
            )
            rejection_counts["iv_or_liquidity_hard_filter"] += 1
            continue
        row.update(scored)
        row["data_quality"] = "ready"
        membership_observations[member.symbol].update(
            {
                "strict_eligible": True,
                "today_state": row["state"],
                "screen_reason": None,
            }
        )
        strict_candidates.append(row)

    top, reserve = apply_crowding(strict_candidates, policy)
    notification_top, _notification_reserve = apply_crowding(
        strict_candidates,
        policy,
        sort_key=priority_sort_key,
    )
    warming.sort(
        key=lambda row: (
            len(row.get("data_quality_reasons") or []),
            float(row.get("price_location_52w") or 1.0),
            str(row["symbol"]),
        )
    )
    request_limited = requested_quote_symbols < len(provider_symbols) or (
        requests_used >= budget and len(detail_evaluated_now) < len(hard_survivors)
    ) or (quotes_ready > 0 and not selected_symbols)
    document: dict[str, Any] = {
        "schema_version": SCHEMA_VERSION,
        "policy_version": POLICY_VERSION,
        "generated_at": now.isoformat(),
        "mode": mode,
        "scan_scope": "budgeted_ranked_shortlist",
        "timezone": "America/New_York",
        "universe_metadata": list(universe.metadata),
        "scan_complete": not request_limited and not errors,
        "automatic_ordering": False,
        "research_only": True,
        "counts": {
            "universe": len(universe.members),
            "quotes_present": sum(member.provider_symbol in quotes for member in universe.members),
            "price_hard_survivors": len(price_hard_survivors),
            "quotes_ready": quotes_ready,
            "shortlisted": len(selected_symbols),
            "not_evaluated": quotes_ready - len(selected_symbols),
            "ivp_snapshots": sum(symbol in iv_snapshots for symbol in price_survivor_symbols),
            "ivp_refreshed": len(refreshed_iv_symbols),
            "ivp_missing": rejection_counts["ibkr_iv_percentile_missing"],
            "ivp_cached": sum(
                symbol in iv_snapshots and symbol not in refreshed_iv_symbols
                for symbol in price_survivor_symbols
            ),
            "hard_survivors": len(hard_survivors),
            "detailed_this_run": len(detail_evaluated_now),
            "strict_candidates": len(strict_candidates),
            "warming_rows": len(warming),
        },
        "request_budget": budget,
        "requests_used": requests_used,
        "top10": top,
        "notification_top10": notification_top,
        "reserve": reserve,
        "watchlist": warming[: max(policy.top_count * 3, policy.top_count)],
        "all_candidates": sorted(
            strict_candidates,
            key=candidate_sort_key,
        ),
        "membership_observations": [
            membership_observations[symbol] for symbol in sorted(membership_observations)
        ],
        "rejection_counts": dict(sorted(rejection_counts.items())),
        "data_quality": {
            "status": "partial" if request_limited or errors else "complete",
            "request_limited": request_limited,
            "ivp_incomplete": bool(rejection_counts["ibkr_iv_percentile_missing"]),
            "errors": sorted(set(errors)),
            "volatility_contract": "13/26/52-week IBKR IV percentiles and absolute LEAPS IV are continuous score inputs, not exclusion thresholds; missing/invalid fields do not receive an invented score",
            "underlying_option_volume": "unavailable_from_current_schwab_endpoints",
            "classification": "official sector fallback; subindustry unavailable",
        },
    }
    fingerprint = _material_fingerprint(document)
    document["material_fingerprint"] = fingerprint
    return document, fingerprint

def _hard_filter(
    member: UniverseMember,
    quote: EquityQuote | None,
    *,
    policy: GrowthDislocationSettings,
    now: datetime,
    mode: str,
) -> tuple[str | None, float | None]:
    del member
    if quote is None:
        return "quote_missing", None
    if None in (
        quote.last,
        quote.low_52w,
        quote.high_52w,
        quote.market_cap,
        quote.dividend_yield,
        quote.quote_at,
        quote.optionable,
    ):
        return "quote_fields_missing", None
    if mode == "rth":
        if not quote.realtime:
            return "quote_delayed", None
        assert quote.quote_at is not None
        if not 0 <= (
            now - quote.quote_at.astimezone(timezone.utc)
        ).total_seconds() <= policy.quote_max_age_seconds:
            return "quote_frozen", None
    elif (
        quote.quote_at is None or quote.quote_at > now
        or quote.quote_at.astimezone(ET).date() != now.astimezone(ET).date()
    ):
        return "quote_frozen", None
    assert quote.last is not None and quote.low_52w is not None and quote.high_52w is not None
    location = price_location_52w(quote.last, quote.low_52w, quote.high_52w)
    if location is None:
        return "price_range_invalid", None
    if not quote.optionable:
        return "not_optionable", location
    return None, location


def _membership_observation(
    *,
    member: UniverseMember,
    quote: EquityQuote | None,
    price_location: float | None,
    screen_reason: str | None,
) -> dict[str, Any]:
    return {
        "symbol": member.symbol,
        "quote_observed": quote is not None,
        "quote_at": quote.quote_at.isoformat() if quote and quote.quote_at else None,
        "price_location_52w": price_location,
        "market_cap": quote.market_cap if quote else None,
        "dividend_yield": quote.dividend_yield if quote else None,
        "optionable": quote.optionable if quote else None,
        "ivp_13w": None,
        "ivp_26w": None,
        "ivp_52w": None,
        "iv_history_as_of": None,
        "iv_observed_at": None,
        "iv_refreshed_this_run": False,
        "strict_eligible": False,
        "today_state": "STALE",
        "screen_reason": screen_reason,
        "data_quality_reasons": [],
        "leaps_depth_refreshed": False,
        "max_option_dte": None,
    }


def _apply_core_pool(
    *,
    document: dict[str, Any],
    state: dict[str, Any],
    mode: str,
    at: datetime,
    policy: GrowthDislocationSettings,
) -> tuple[list[str], list[str]]:
    """Use the same current-score Top N for entry and exit on completed batches.

    Incomplete scans freeze membership; an individual missing observation keeps
    its member STALE. Neither condition turns yesterday's snapshot into a pass.
    """

    core = _mapping_state(state, "core_pool")
    members = _mapping_records(core.get("members"))
    core.pop("entry_streaks", None)
    for member in members.values():
        member.pop("exit_streak", None)
        member.pop("exit_reason", None)
    archive = [dict(row) for row in core.get("archive", []) if isinstance(row, Mapping)]
    rejected = {
        str(symbol).upper() for symbol in core.get("research_rejected_symbols", []) if str(symbol)
    }
    candidates = {
        str(row["symbol"]): dict(row)
        for row in document.get("all_candidates", [])
        if isinstance(row, Mapping) and row.get("symbol")
    }
    ranked, _reserve = apply_crowding(
        [row for symbol, row in candidates.items() if symbol not in rejected],
        policy, sort_key=priority_sort_key,
    )
    ranked_symbols = {row["symbol"] for row in ranked}
    warming = {
        str(row["symbol"]): dict(row)
        for row in document.get("watchlist", [])
        if isinstance(row, Mapping) and row.get("symbol")
    }
    observations = {
        str(row["symbol"]): dict(row)
        for row in document.get("membership_observations", [])
        if isinstance(row, Mapping) and row.get("symbol")
    }
    quality_complete = _mapping(document.get("data_quality")).get("status") == "complete"
    quality_complete = quality_complete and bool(document.get("scan_complete"))
    initialized = bool(core.get("initialized"))
    added: list[dict[str, str]] = []
    exited: list[dict[str, str]] = []
    at_label = at.isoformat()

    if quality_complete:
        if not initialized:
            core["bootstrapped_at"] = at_label
        initialized = True
        for symbol in list(members):
            member = members[symbol]
            observation = observations.get(symbol, {})
            reason = _core_exit_signal(
                symbol=symbol,
                observation=observation,
                rejected=rejected,
            )
            if symbol in candidates and symbol not in ranked_symbols:
                reason = reason or "RANKED_OUT"
            if reason is not None:
                exited.append({"symbol": symbol, "reason": reason})
                archive.append(
                    {
                        "symbol": symbol,
                        "entered_at": member.get("entered_at"),
                        "exited_at": at_label,
                        "reason": reason,
                        "observation": observation,
                    }
                )
                members.pop(symbol)
                continue
            if symbol in candidates:
                member["lifecycle_status"] = "CORE_ACTIVE"
                member["pause_reason"] = None
                member["snapshot"] = candidates[symbol]
                member["last_eligible_at"] = at_label
            else:
                member["lifecycle_status"] = "CORE_PAUSED"
                member["pause_reason"] = str(
                    observation.get("screen_reason") or "observation_missing"
                )
            if observation.get("selected_this_run"):
                member["last_complete_observed_at"] = at_label

        eligible_nonmembers = ranked_symbols - set(members) - rejected
        for symbol in sorted(eligible_nonmembers):
            members[symbol] = new_core_member(
                candidates[symbol],
                at=at_label,
                reason="SCORE_TOP_POOL",
            )
            added.append({"symbol": symbol, "reason": "SCORE_TOP_POOL"})

        ranked, _reserve = apply_crowding(
            [row for symbol, row in candidates.items() if symbol in members],
            policy,
            sort_key=priority_sort_key,
        )
        core["top_opportunities"] = ranked
        if mode == "daily":
            core["last_complete_daily_at"] = at_label

    stored_top = [
        dict(row) for row in core.get("top_opportunities", []) if isinstance(row, Mapping)
    ]
    displayed_top = [
        core_display_row(
            member=members.get(str(row.get("symbol")), {}),
            stored=row,
            current=candidates.get(str(row.get("symbol"))),
            warming=warming.get(str(row.get("symbol"))),
            observation=observations.get(str(row.get("symbol"))),
        )
        for row in stored_top
    ]
    core_rows = [
        core_display_row(
            member=member,
            stored=_mapping(member.get("snapshot")),
            current=candidates.get(symbol),
            warming=warming.get(symbol),
            observation=observations.get(symbol),
        )
        for symbol, member in members.items()
    ]
    core_rows.sort(key=priority_sort_key)

    core.update(
        {
            "initialized": initialized,
            "members": members,
            "archive": archive,
            "last_scan_at": at_label,
        }
    )
    core_status = (
        "BOOTSTRAP_PENDING"
        if not initialized
        else "FROZEN"
        if not quality_complete
        else "ACTIVE"
    )
    if not initialized:
        change_message = "Waiting for the first complete scan"
    elif not quality_complete:
        change_message = "Membership frozen due to partial scan"
    else:
        change_message = "Score-ranked Top N enters; refreshed members ranked out or untradeable exit; unassessed/missing data stays STALE"
    document["core_pool_status"] = core_status
    document["core_membership_updated"] = quality_complete
    document["core_pool"] = core_rows
    document["core_top_opportunities"] = displayed_top
    document["core_entry_pending"] = []
    document["core_changes"] = {
        "added": added,
        "exited": exited,
        "frozen": not quality_complete,
        "message": change_message,
    }
    counts = dict(_mapping(document.get("counts")))
    counts.update(
        {
            "core_pool": len(members),
            "core_active": sum(
                row["snapshot_status"] == "current" for row in core_rows
            ),
            "core_paused": sum(
                row["snapshot_status"] != "current" for row in core_rows
            ),
            "core_entry_pending": 0,
        }
    )
    document["counts"] = counts
    return [row["symbol"] for row in added], [row["symbol"] for row in exited]


def _mapping_records(value: Any) -> dict[str, dict[str, Any]]:
    if not isinstance(value, Mapping):
        return {}
    return {
        str(symbol): dict(row)
        for symbol, row in value.items()
        if str(symbol) and isinstance(row, Mapping)
    }


def _core_exit_signal(
    *,
    symbol: str,
    observation: Mapping[str, Any],
    rejected: set[str],
) -> str | None:
    if symbol in rejected:
        return "RESEARCH_REJECTED"
    reason = str(observation.get("screen_reason") or "")
    # Consume the existing scanner's fresh-data verdict, not a second, looser
    # set of thresholds or old cached field values.
    if reason in {
        "not_optionable",
        "iv_or_liquidity_hard_filter",
    } or (reason == "target_leaps_hard_filter" and observation.get("strict_rejection_confirmed")):
        return reason
    return None


def _candidate_row(
    *,
    member: UniverseMember,
    quote: EquityQuote,
    price_location: float,
    history: list[DailyClose],
    detail: Mapping[str, Any],
    benchmark_histories: Mapping[str, Any],
    policy: GrowthDislocationSettings,
    now: datetime,
    ny: datetime,
    mode: str,
    detail_current: bool,
    iv_snapshot: IvPercentileSnapshot | None,
) -> tuple[dict[str, Any], list[str]]:
    reasons: list[str] = []
    features = price_features([entry.close for entry in history])
    if features is None:
        reasons.append("price_history_warming")
        features = {}
    sector = price_features(
        [entry.close for entry in _close_history(benchmark_histories.get(member.sector_benchmark))]
    )
    if sector is None:
        reasons.append("sector_history_warming")
    if iv_snapshot is None:
        reasons.append("ibkr_iv_percentile_missing")
    else:
        if iv_snapshot.ivp_13w is None:
            reasons.append("ibkr_ivp_13w_missing")
        if iv_snapshot.ivp_26w is None:
            reasons.append("ibkr_ivp_26w_missing")
        if iv_snapshot.ivp_52w is None:
            reasons.append("ibkr_ivp_52w_missing")
    if not detail:
        reasons.append("target_leaps_warming")
    elif not detail_current:
        reasons.append("target_leaps_not_refreshed_this_run")
    option_quote_at = _parse_datetime(detail.get("quote_at"))
    if detail:
        if bool(detail.get("delayed")):
            reasons.append("target_leaps_delayed")
        elif option_quote_at is None:
            reasons.append("target_leaps_timestamp_missing")
        elif option_quote_at > now or (
            (now - option_quote_at).total_seconds() > policy.option_quote_max_age_seconds
            if mode == "rth" else option_quote_at.astimezone(ET).date() != ny.date()
        ):
            reasons.append("target_leaps_frozen")
    bid = _optional_float(detail.get("bid"))
    ask = _optional_float(detail.get("ask"))
    spread = spread_mid_ratio(bid, ask)
    if detail and spread is None:
        reasons.append("target_leaps_bbo_invalid")
    current_iv = _optional_float(detail.get("volatility"))
    if detail and current_iv is None:
        reasons.append("target_leaps_iv_missing")
    row: dict[str, Any] = {
        "symbol": member.symbol,
        "company": member.company,
        "memberships": list(member.memberships),
        "state": "WATCH",
        "final_score": None,
        "price_location_52w": price_location,
        "ivp_13w": iv_snapshot.ivp_13w if iv_snapshot else None,
        "ivp_26w": iv_snapshot.ivp_26w if iv_snapshot else None,
        "ivp_52w": iv_snapshot.ivp_52w if iv_snapshot else None,
        "iv_rank_13w": iv_snapshot.iv_rank_13w if iv_snapshot else None,
        "iv_rank_26w": iv_snapshot.iv_rank_26w if iv_snapshot else None,
        "iv_rank_52w": iv_snapshot.iv_rank_52w if iv_snapshot else None,
        "iv_filter_source": "ibkr_tws_option_implied_volatility_history",
        "iv_data_notes": ([] if iv_snapshot else ["ibkr_tws_iv_history_missing"]),
        "iv_observed_at": iv_snapshot.observed_at.isoformat() if iv_snapshot else None,
        "iv_history_as_of": (
            iv_snapshot.as_of_date.isoformat()
            if iv_snapshot and iv_snapshot.as_of_date is not None
            else None
        ),
        "current_iv": current_iv,
        "realized_vol_20d": features.get("realized_vol_20d"),
        "iv_rv_ratio": (
            current_iv / float(features["realized_vol_20d"])
            if current_iv is not None and float(features.get("realized_vol_20d") or 0.0) > 0.0
            else None
        ),
        "rsi14": features.get("rsi14"),
        "rsi14_min_20d": features.get("rsi14_min_20d"),
        "return_5d": features.get("return_5d"),
        "return_10d": features.get("return_10d"),
        "sector_return_5d": sector.get("return_5d") if sector else None,
        "sector_return_10d": sector.get("return_10d") if sector else None,
        "rs_5d_sector": None,
        "rs_10d_sector": None,
        "ma5": features.get("ma5"),
        "ma10": features.get("ma10"),
        "low_20d": features.get("low_20d"),
        "distance_from_20d_low": features.get("distance_from_20d_low"),
        "last": quote.last,
        "market_cap": quote.market_cap,
        "dividend_yield": quote.dividend_yield,
        "leaps_symbol": detail.get("symbol"),
        "leaps_expiry": detail.get("expiry"),
        "leaps_dte": detail.get("dte"),
        "leaps_strike": detail.get("strike"),
        "leaps_delta": detail.get("delta"),
        "leaps_bid": bid,
        "leaps_ask": ask,
        "leaps_spread_mid": spread,
        "spread_mid": spread,
        "target_leaps_oi": detail.get("open_interest"),
        "max_option_dte": detail.get("max_dte"),
        "observed_leaps_chain_volume": detail.get("observed_volume"),
        "underlying_avg_option_volume": None,
        "subindustry": member.subindustry,
        "sector": member.sector,
        "classification_level": member.classification_level,
        "sector_benchmark": member.sector_benchmark,
        "crowding_group": member.crowding_group,
        "quote_status": "live" if mode == "rth" else "frozen",
        "option_quote_status": _option_status(
            detail=detail,
            delayed=bool(detail.get("delayed")),
            quote_at=option_quote_at,
            now=now,
            mode=mode,
            max_age_seconds=policy.option_quote_max_age_seconds,
        ),
        "quote_at": quote.quote_at.isoformat() if quote.quote_at else None,
        "option_quote_at": option_quote_at.isoformat() if option_quote_at else None,
        "detail_refreshed_this_run": detail_current,
        "data_quality": "warming" if reasons else "ready",
        "data_quality_reasons": sorted(set(reasons)),
        "as_of_et": ny.isoformat(),
        "price_history_as_of": history[-1].day.isoformat() if features and history else None,
    }
    return row, sorted(set(reasons))

def render_notification(
    document: Mapping[str, Any], *, discoveries: bool = False,
) -> tuple[str, str]:
    mode = str(document.get("mode") or "rth")
    title = "Growth Dislocation LEAPS · 日报" if mode == "daily" else "Growth Dislocation LEAPS · 状态更新"
    counts = _mapping(document.get("counts"))
    changes = _mapping(document.get("core_changes"))
    added, exited = _change_label(changes.get("added")), _change_label(changes.get("exited"))
    lines = [
        f"# {title}\n\n## Core Pool\n",
        f"- 状态 **{document.get('core_pool_status', 'BOOTSTRAP_PENDING')}**；"
        f"成员 **{counts.get('core_pool', 0)}**；Active **{counts.get('core_active', 0)}**；"
        f"Paused **{counts.get('core_paused', 0)}**；"
        f"Entry Pending **{counts.get('core_entry_pending', 0)}**",
        f"- Changes：新增 {added}；退出 {exited}",
        f"- {changes.get('message', 'Membership state unavailable')}",
        "",
        "| Symbol | Core Status | Today State | Reason | 52W位置 | IVP 52W | Market Cap |",
        "|---|---:|---:|---|---:|---:|---:|",
    ]
    core_pool = document.get("core_pool", [])
    core_rows = [
        "| {symbol} | {core_status} | {today_state} | {reason} | {location} | {ivp_52w} | {market_cap} |".format(
            symbol=row.get("symbol", "-"), core_status=str(row.get("core_status") or "CORE_PAUSED").removeprefix("CORE_"),
            today_state=row.get("today_state", row.get("state", "-")), reason=row.get("pause_reason") or row.get("exit_reason") or "—",
            location=_pct(row.get("price_location_52w")), ivp_52w=_pct(row.get("ivp_52w")),
            market_cap=_market_cap(row.get("market_cap")),
        )
        for row in (core_pool if isinstance(core_pool, list) else []) if isinstance(row, Mapping)
    ]
    lines.extend(core_rows or ["| — | — | — | Core Pool 尚无成员 | — | — | — |"])
    if discoveries:
        # Feishu's current-scan report is independent of historical membership.
        # The default renderer and notification routing are left unchanged.
        quality = _mapping(document.get("data_quality"))
        lines = [
            f"# {title}\n\n## 当日扫描\n",
            f"扫描时间：{document.get('generated_at', '—')}",
            f"Universe {counts.get('universe', 0)}；基础数据就绪 {counts.get('quotes_ready', 0)}；"
            f"本轮细查 {counts.get('shortlisted', 0)}；完成评分 {counts.get('strict_candidates', 0)}；"
            f"待补齐详细数据 {counts.get('warming_rows', 0)}",
            f"预算内分批扫描：另有 {counts.get('not_evaluated', 0)} 只本轮未细查，不是策略淘汰；"
            "Top 是本轮已评估标的排名，不宣称全市场已完成评分。",
            "本轮新增：" + (", ".join(document.get("strict_added_symbols", [])) or "无"),
            f"扫描执行：{quality.get('status', 'unknown')}（不代表所有源数据刚刷新）；"
            f"Schwab 请求 {document.get('requests_used')} / {document.get('request_budget')}",
            f"IVP 本轮刷新 {counts.get('ivp_refreshed', 0)}；沿用有效缓存 {counts.get('ivp_cached', 0)}；"
            f"缺失 {counts.get('ivp_missing', 0)}。缓存不冒充本轮刷新。",
            f"Core Pool 本轮变化：新增 {added}；退出 {exited}",
            "仅做候选发现与排序；TRIGGER 不等于买入。按综合评分竞争 Core Top 名额；旧成员无留存加分。",
            "评分：52W位置 35.00% + 52W IVP 30.00% + 13W/26W IVP 10.00% + 市值 10.00% "
            "+ 股息 5.00% + 修复/行业RS 5.00% + IV成本 5.00%。偏好超限扣分，不单项淘汰。",
            "\n## 淘汰与数据缺口\n",
        ]
        reason_labels = {
            "above_52w_location": "52W位置超限",
            "below_market_cap": "市值不足",
            "dividend_yield": "股息率超限",
            "ivp_13w_above_limit": "13W IVP超限",
            "ivp_26w_above_limit": "26W IVP超限",
            "ivp_52w_above_limit": "52W IVP超限",
            "iv_or_liquidity_hard_filter": "波动率或合约条件不符",
            "ibkr_iv_percentile_missing": "IVP数据缺失（非策略淘汰）",
            "ibkr_iv_percentile_invalid": "IVP数据无效（非策略淘汰）",
            "not_evaluated_this_run": "预算轮转，本轮未细查",
            "target_leaps_hard_filter": "目标LEAPS不合格",
            "target_leaps_missing": "目标LEAPS缺失",
            "quote_missing": "股票报价缺失",
            "quote_fields_missing": "股票报价字段缺失",
            "quote_frozen": "股票报价过期",
            "quote_delayed": "股票报价延迟",
            "not_optionable": "无期权",
            "price_history_warming": "股票日线未完成校验",
            "sector_history_warming": "行业基准日线未完成校验",
            "dte_out_of_range": "期限不符",
            "delta_missing": "Delta缺失",
            "delta_out_of_range": "Delta不符",
            "bid_ask_invalid": "买卖报价无效",
            "spread_above_limit": "价差过宽",
            "iv_missing": "IV缺失",
            "iv_above_limit": "IV超过上限",
            "open_interest_missing": "持仓量缺失",
            "open_interest_below_limit": "持仓量不足",
            "extrinsic_invalid": "时间价值无效",
            "extrinsic_above_limit": "时间价值过高",
        }
        for reason, count in sorted(_mapping(document.get("rejection_counts")).items()):
            if count:
                lines.append(f"- {reason_labels.get(reason, reason)}：{count}")
        for error in quality.get("errors", []):
            lines.append(f"- 数据错误：{error}")
        for row in document.get("membership_observations", []):
            reasons = row.get("contract_rejection_counts")
            if reasons:
                lines.append(f"- {row['symbol']} 合约首个失败条件：" + ", ".join(
                    f"{reason_labels.get(reason, reason)}={count:.2f}"
                    for reason, count in sorted(reasons.items())
                ))
    else:
        lines += [
            "\n## Scanner 状态\n",
            f"- 严格候选 **{counts.get('strict_candidates', 0)}**；"
            f"未完成数据行（正文隐藏） **{counts.get('warming_rows', 0)}**；"
            f"本轮详细刷新 **{counts.get('detailed_this_run', 0)} / {counts.get('hard_survivors', 0)}**",
            f"- Schwab 请求 **{document.get('requests_used')} / {document.get('request_budget')}**；"
            f"Data Quality **{_mapping(document.get('data_quality')).get('status', 'unknown')}**",
            "- 仅做候选发现与排序；TRIGGER 不等于买入，仍需基本面复核。",
        ]
    lines += [
        "\n## Top Opportunities（综合评分，52W优先）\n",
        "| Symbol | Today State | Score | Market Cap | Timing | 52W位置 | IVP 52W | IVP 13W/26W | RSI | 行业RS 5D | LEAPS | IV | IV/RV20 | OI | 时间价值/股价 | Spread |",
        "|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---|---:|---:|---:|---:|---:|",
    ]
    top = document.get("notification_top10" if discoveries else "core_top_opportunities", [])
    if isinstance(top, list) and top:
        for row in top:
            if not isinstance(row, Mapping):
                continue
            lines.append(
                "| {symbol} | {state} | {priority} | {market_cap} | {score} | {location} | {ivp_52w} | {ivp} | {rsi} | {sector_rs} | {contract} | {current_iv} | {iv_rv} | {oi} | {extrinsic} | {spread} |".format(
                    symbol=row.get("symbol", "-"),
                    state=row.get("today_state", row.get("state", "-")),
                    priority=_fmt(row.get("priority_score")),
                    market_cap=_market_cap(row.get("market_cap")),
                    score=_fmt(row.get("timing_score")),
                    location=_pct(row.get("price_location_52w")),
                    ivp_52w=_pct(row.get("ivp_52w")),
                    ivp=_iv_gate_label(row),
                    rsi=_fmt(row.get("rsi14")),
                    sector_rs=_pct(row.get("rs_5d_sector")),
                    contract=row.get("leaps_symbol") or "-",
                    current_iv=_pct(row.get("current_iv")),
                    iv_rv=_fmt(row.get("iv_rv_ratio")),
                    oi=_fmt(row.get("target_leaps_oi")),
                    extrinsic=_pct(row.get("extrinsic_value_ratio")),
                    spread=_pct(row.get("leaps_spread_mid")),
                )
            )
    elif discoveries:
        lines.append("| — | 本轮无完成评分且可交易的候选，详见上方淘汰与数据缺口 |" + " — |" * 14)
    else:
        lines.append(
            "| — | WATCH | — | — | — | — | — | 当前无 Active 严格候选；Core Pool 成员见上表 | — | — | — | — | — | — | — | — |"
        )
    if discoveries:
        if top:
            lines.append("\n数据日期（ET；RTH日线末点是盘中观察，非最终收盘）：")
        for row in top:
            lines.append(
                f"- {row['symbol']}：日线 {row.get('price_history_as_of') or '未知'}；"
                f"IV历史 {row.get('iv_history_as_of') or '未知'}；"
                f"IV获取时间 {row.get('iv_observed_at') or '未知'}"
            )
        lines.extend([
            "\n## Core Pool · 跟踪记录（非当日推荐）\n",
            f"Active {counts.get('core_active', 0)}；STALE {counts.get('core_paused', 0)}；"
            f"Changes：新增 {added}；退出 {exited}",
            str(changes.get("message", "")),
            "数据缺失不等于策略淘汰；STALE 成员不展示历史评分与历史合约，本轮未获取的字段留空。",
            "",
            "| Symbol | Today State | 本轮原因 | 52W位置 | IVP 52W | Market Cap | 报价源时间 | IV历史日期 | 最后合格快照（历史） |",
            "|---|---|---|---:|---:|---:|---|---|---|",
        ])
        for row in sorted(core_pool, key=lambda r: (
            r.get("snapshot_status") != "current", str(r.get("symbol")),
        )):
            current = _mapping(row.get("current_observation"))
            lines.append(
                f"| {row['symbol']} | {row.get('today_state', 'STALE')} | "
                f"{reason_labels.get(current.get('screen_reason'), current.get('screen_reason')) or '—'} | "
                f"{_pct(current.get('price_location_52w'))} | "
                f"{_pct(current.get('ivp_52w'))} | {_market_cap(current.get('market_cap'))} | "
                f"{current.get('quote_at') or '未获取'} | {current.get('iv_history_as_of') or '未获取'} | "
                f"{row.get('snapshot_as_of_et') or row.get('as_of_et') or '未知'} |"
            )
        if not core_pool:
            lines.append("| — | 暂无成员 |" + " — |" * 7)
    return title, "\n".join(lines)


def _change_label(value: Any) -> str:
    rows = value if isinstance(value, list) else []
    labels = [
        f"{row.get('symbol')} ({row.get('reason')})"
        for row in rows
        if isinstance(row, Mapping) and row.get("symbol")
    ]
    return ", ".join(labels) or "无"

def _detail_order(
    survivors: list[tuple[UniverseMember, EquityQuote, float]],
    state: dict[str, Any],
    *,
    policy: GrowthDislocationSettings,
    request_budget: int,
) -> list[tuple[UniverseMember, EquityQuote, float]]:
    # Rank cheap quote data before spending IBKR history / Schwab detail calls.
    # Interleave incumbent refresh, new leaders and exploration. Incumbents get
    # data coverage, never a score bonus or a guaranteed membership slot.
    ordered = sorted(
        survivors,
        key=lambda item: (
            -quote_preference_scores(item[2], item[1].market_cap, item[1].dividend_yield, policy)["prefilter_score"],
            item[0].symbol,
        ),
    )
    members = _mapping(_mapping(state.get("core_pool")).get("members"))
    incumbents = sorted(
        (item for item in ordered if item[0].symbol in members),
        key=lambda item: (str(_mapping(members[item[0].symbol]).get("last_complete_observed_at") or ""), item[0].symbol),
    )
    newcomers = [item for item in ordered if item[0].symbol not in members]
    leaders = newcomers[: max(policy.top_count * 2, request_budget // 2)]
    remaining = newcomers[len(leaders):]
    cursor = int(state.get("detail_cursor") or 0) % max(1, len(remaining))
    rotation = remaining[cursor:] + remaining[:cursor]
    selected: list[tuple[UniverseMember, EquityQuote, float]] = []
    benchmarks: set[str] = set()
    rotated = 0
    for group in zip_longest(incumbents, leaders[::2], leaders[1::2], rotation):
        for lane, item in enumerate(group):
            if item is None:
                continue
            cost = 2 + int(item[0].sector_benchmark not in benchmarks)
            if cost > request_budget:
                continue
            selected.append(item)
            request_budget -= cost
            benchmarks.add(item[0].sector_benchmark)
            if lane == 3:
                rotated += 1
        if request_budget < 2:
            break
    if remaining:
        state["detail_cursor"] = (cursor + rotated) % len(remaining)
    return selected

def _detail_payload(
    target: OptionContract,
    chain: LeapsChain,
    *,
    fetched_at: datetime,
) -> dict[str, Any]:
    return {
        **asdict(target),
        "expiry": target.expiry.isoformat(),
        "quote_at": target.quote_at.isoformat() if target.quote_at else None,
        "max_dte": chain.max_dte,
        "observed_volume": chain.observed_volume,
        "delayed": chain.delayed,
        "fetched_at": fetched_at.isoformat(),
    }

def _valid_state(raw: dict[str, object]) -> dict[str, Any]:
    if raw.get("schema_version") not in {None, 2, STATE_SCHEMA_VERSION}:
        return {}
    # V6 smoothed the score curve, V7 changed notification priority, V8 added
    # the Core Pool, V9 made RSI score-only, V10 tightened all three entry gates,
    # V11 made IV/RV score-only, V12 makes Core membership dynamic. Preserve
    # prior members/caches so the next complete scan evaluates them honestly.
    if raw.get("policy_version") not in {
        None,
        "growth_dislocation_leaps.v5",
        "growth_dislocation_leaps.v6",
        "growth_dislocation_leaps.v7",
        "growth_dislocation_leaps.v8",
        "growth_dislocation_leaps.v9",
        "growth_dislocation_leaps.v10",
        "growth_dislocation_leaps.v11",
        "growth_dislocation_leaps.v12",
        POLICY_VERSION,
    }:
        return {}
    return dict(raw)

def _mapping_state(state: dict[str, Any], key: str) -> dict[str, Any]:
    value = state.get(key)
    resolved = dict(value) if isinstance(value, Mapping) else {}
    state[key] = resolved
    return resolved

def _fresh_iv_snapshots(
    cache: Mapping[str, Any],
    *,
    trading_day: date,
) -> dict[str, IvPercentileSnapshot]:
    snapshots: dict[str, IvPercentileSnapshot] = {}
    for raw_symbol, raw_payload in cache.items():
        if not isinstance(raw_payload, Mapping):
            continue
        snapshot = iv_percentile_snapshot_from_cached_payload(
            raw_payload,
            fallback_symbol=str(raw_symbol),
        )
        if (
            snapshot is None
            or snapshot.ivp_13w is None
            or snapshot.ivp_26w is None
            or snapshot.ivp_52w is None
        ):
            continue
        basis_day = snapshot.as_of_date or snapshot.observed_at.astimezone(ET).date()
        age = DEFAULT_MARKET_CALENDAR.trading_days_elapsed(basis_day, trading_day)
        if age is not None and age <= 1:
            snapshots[_ibkr_provider_symbol(snapshot.provider_symbol)] = snapshot
    return snapshots

def _mapping(value: Any) -> Mapping[str, Any]:
    return value if isinstance(value, Mapping) else {}

def _ibkr_provider_symbol(symbol: str) -> str:
    return " ".join(symbol.strip().upper().replace(".", " ").replace("/", " ").split())

def _close_history(value: Any) -> list[DailyClose]:
    rows = value if isinstance(value, list) else []
    by_day: dict[date, DailyClose] = {}
    for row in rows:
        if not isinstance(row, Mapping):
            continue
        try:
            day = date.fromisoformat(str(row["date"]))
            close = float(row["close"])
        except (KeyError, TypeError, ValueError):
            continue
        if math.isfinite(close) and close > 0.0:
            by_day[day] = DailyClose(day, close)
    return [by_day[day] for day in sorted(by_day)][-MAX_CLOSE_HISTORY:]


def _refresh_close_history(
    client: SchwabClient,
    symbol: str,
    *,
    now: datetime,
    mode: str,
    quote: EquityQuote | None,
    max_quote_age: float,
) -> tuple[list[DailyClose], str | None]:
    """Repair from broker bars, then validate actual exchange sessions, not row count.

    Completed sessions always come from daily bars. Only RTH may append a fresh
    same-session quote; after-hours prices never overwrite the official close.
    This deliberately replaces the old sparse/provisional cache on each scan.
    """
    ny = now.astimezone(ET)
    session = DEFAULT_MARKET_CALENDAR.session(ny.date())
    completed_day = (
        ny.date() if session and ny >= session.close_at
        else DEFAULT_MARKET_CALENDAR.previous_trading_day(ny.date())
    )
    try:
        history = [
            row for row in fetch_daily_closes(client, symbol)
            if row.day <= completed_day and DEFAULT_MARKET_CALENDAR.is_trading_day(row.day)
        ][-MAX_CLOSE_HISTORY:]
    except ProviderError as exc:
        return [], type(exc).__name__
    if not history:
        return [], "insufficient_sessions"
    if history[-1].day != completed_day or any(
        current.day != DEFAULT_MARKET_CALENDAR.next_trading_day(prior.day)
        for prior, current in zip(history, history[1:])
    ):
        return [], "missing_sessions"
    if any(not math.isfinite(row.close) or row.close <= 0.0 for row in history):
        return [], "invalid_close"
    if mode == "rth":
        if (
            not session or not session.open_at <= ny < session.close_at
            or quote is None or not quote.realtime or quote.last is None
            or quote.quote_at is None or quote.quote_at.astimezone(ET).date() != ny.date()
            or not 0.0 <= (now - quote.quote_at).total_seconds() <= max_quote_age
        ):
            return [], "current_quote_unavailable"
        history = _upsert_close(history, ny.date(), quote.last)
    if len(history) < 35:
        return [], "insufficient_sessions"
    return history, None


def _close_history_payload(history: list[DailyClose]) -> list[dict[str, Any]]:
    return [
        {"date": row.day.isoformat(), "close": row.close} for row in history[-MAX_CLOSE_HISTORY:]
    ]

def _upsert_close(history: list[DailyClose], day: date, close: float) -> list[DailyClose]:
    by_day = {row.day: row for row in history}
    by_day[day] = DailyClose(day, close)
    return [by_day[key] for key in sorted(by_day)][-MAX_CLOSE_HISTORY:]

def _parse_datetime(value: Any) -> datetime | None:
    try:
        parsed = datetime.fromisoformat(str(value))
    except ValueError:
        return None
    return parsed.astimezone(timezone.utc) if parsed.tzinfo is not None else None

def _optional_float(value: Any) -> float | None:
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return None
    return parsed if parsed == parsed else None

def _option_status(
    *,
    detail: Mapping[str, Any],
    delayed: bool,
    quote_at: datetime | None,
    now: datetime,
    mode: str,
    max_age_seconds: float,
) -> str:
    if not detail or quote_at is None:
        return "missing"
    if delayed:
        return "delayed"
    if mode == "daily":
        return "frozen"
    return "live" if (now - quote_at).total_seconds() <= max_age_seconds else "frozen"

def _material_fingerprint(document: Mapping[str, Any]) -> str:
    keys = (
        "symbol",
        "state",
        "final_score",
        "price_dislocation_score",
        "ivp_52w_score",
        "priority_score",
        "price_location_52w",
        "ivp_13w",
        "ivp_26w",
        "ivp_52w",
        "iv_rank_13w",
        "iv_rank_26w",
        "iv_rank_52w",
        "current_iv",
        "realized_vol_20d",
        "iv_rv_ratio",
        "iv_filter_source",
        "rsi14",
        "leaps_symbol",
        "leaps_dte",
        "leaps_delta",
        "leaps_bid",
        "leaps_ask",
        "leaps_spread_mid",
        "target_leaps_oi",
        "extrinsic_value_ratio",
        "data_quality",
        "data_quality_reasons",
    )

    def material_row(row: Mapping[str, Any]) -> dict[str, Any]:
        result: dict[str, Any] = {}
        for key in keys:
            value = row.get(key)
            result[key] = round(value, 3) if isinstance(value, float) else value
        return result

    payload = {
        "top10": [material_row(row) for row in document.get("top10", [])],
        "notification_top10": [
            material_row(row) for row in document.get("notification_top10", [])
        ],
        "reserve": [material_row(row) for row in document.get("reserve", [])],
        "watchlist": [material_row(row) for row in document.get("watchlist", [])],
        "data_quality": document.get("data_quality"),
        "core_pool_status": document.get("core_pool_status"),
        "core_top_opportunities": [
            material_row(row) for row in document.get("core_top_opportunities", [])
        ],
        "core_changes": document.get("core_changes"),
    }
    encoded = json.dumps(payload, sort_keys=True, separators=(",", ":"), allow_nan=False).encode()
    return hashlib.sha256(encoded).hexdigest()

def _notification_id(mode: str, occurred_at: datetime, fingerprint: str) -> str:
    return f"growth-dislocation:{mode}:{occurred_at:%Y%m%dT%H%MZ}:{fingerprint[:16]}"

def _aware(value: datetime) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError("Growth-dislocation timestamps must be timezone-aware")
    return value

def _fmt(value: Any) -> str:
    return "—" if value is None else f"{float(value):.2f}"


def _market_cap(value: Any) -> str:
    if value is None:
        return "—"
    market_cap = float(value)
    if market_cap >= 1_000_000_000_000:
        return f"${market_cap / 1_000_000_000_000:.2f}T"
    return f"${market_cap / 1_000_000_000:.2f}B"

def _iv_gate_label(row: Mapping[str, Any]) -> str:
    return f"{_pct(row.get('ivp_13w'))} / {_pct(row.get('ivp_26w'))}"

def _pct(value: Any) -> str:
    return "—" if value is None else f"{float(value) * 100.0:.2f}%"

def run(*, now: datetime | None = None, force: bool = False) -> int:
    at = _aware(now or datetime.now(tz=timezone.utc))
    mode = scheduled_mode(at)
    if mode is None and not force:
        print(json.dumps({"event": "growth_dislocation_scan", "outcome": "outside_schedule"}))
        return 0
    mode = mode or ("rth" if DEFAULT_MARKET_CALENDAR.is_rth_open(at) else "daily")
    settings = load_app_settings()
    policy = settings.growth_dislocation
    if not policy.enabled:
        print(json.dumps({"event": "growth_dislocation_scan", "outcome": "disabled"}))
        return 0
    universe_path = Path(policy.universe_path)
    if not universe_path.is_absolute():
        universe_path = Path.cwd() / universe_path
    schwab_settings = SchwabSettings.from_env()
    client = build_schwab_client(schwab_settings)
    if client is None:
        print(json.dumps({"event": "growth_dislocation_scan", "outcome": "schwab_unavailable"}))
        return 1
    ibkr_settings = IbkrSettings.from_env()
    outcome = scan_once(
        now=at,
        mode=mode,
        client=client,
        policy=policy,
        universe=load_universe(universe_path),
        data_root=Path(settings.storage.data_root),
        iv_percentile_fetcher=partial(
            fetch_iv_percentile_snapshots,
            settings=ibkr_settings,
            timeout_seconds=policy.ibkr_history_timeout_seconds,
            concurrency=policy.ibkr_history_concurrency,
        ),
        notification_settings=NotificationSettings.from_env(),
    )
    print(
        json.dumps(
            {
                "event": "growth_dislocation_scan",
                "outcome": "completed",
                "mode": mode,
                "counts": outcome.document["counts"],
                "requests_used": outcome.document["requests_used"],
                "scan_complete": outcome.document["scan_complete"],
                "notification_outcome": (
                    outcome.notification.outcome
                    if outcome.notification is not None
                    else "unchanged"
                ),
            },
            sort_keys=True,
        )
    )
    return 0
