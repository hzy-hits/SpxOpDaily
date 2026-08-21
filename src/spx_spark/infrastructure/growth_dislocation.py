"""Scheduled Growth Dislocation LEAPS scanner using the shared Schwab/outbox stack."""

from __future__ import annotations

import csv
import hashlib
import json
from collections import Counter
from collections.abc import Callable, Mapping
from dataclasses import asdict, dataclass
from datetime import date, datetime, timedelta, timezone
from functools import partial
from pathlib import Path
from typing import Any
from urllib.error import HTTPError, URLError

from spx_spark.analytics.growth_dislocation import (
    POLICY_VERSION,
    apply_crowding,
    candidate_sort_key,
    price_features,
    price_location_52w,
    score_sort_key,
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

SCHEMA_VERSION = "growth_dislocation_leaps.v5"
STATE_SCHEMA_VERSION = 2
SCHEDULE_MINUTE = 30
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
        and ny.minute == 0
        and DEFAULT_MARKET_CALENDAR.is_trading_day(ny.date())
    ):
        return "daily"
    if ny.minute == SCHEDULE_MINUTE and DEFAULT_MARKET_CALENDAR.is_rth_open(ny):
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
        current_symbols = {str(row["symbol"]) for row in document["all_candidates"]}
        previous_symbols = {str(symbol) for symbol in state.get("active_candidate_symbols", [])}
        pending_symbols = {str(symbol) for symbol in state.get("pending_added_symbols", [])}
        complete = bool(document.get("scan_complete"))
        added_symbols = (
            sorted(((current_symbols - previous_symbols) | pending_symbols) & current_symbols)
            if complete
            else []
        )
        document["added_symbols"] = added_symbols
        atomic_write_json_secure(latest_path, document)
        notification: EnqueueResult | None = None
        should_notify = mode == "daily" or bool(added_symbols)
        if should_notify and notification_settings is not None:
            title, text = render_notification(document)
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
                notification_settings,
                envelope,
                title=title,
                text=text,
                friend=False,
                feishu_text=text,
                enqueued_at=at,
            )
            if notification.accepted:
                pending_symbols.difference_update(added_symbols)
            else:
                pending_symbols.update(added_symbols)
        elif added_symbols:
            pending_symbols.update(added_symbols)
        state["schema_version"] = STATE_SCHEMA_VERSION
        state["policy_version"] = POLICY_VERSION
        state["last_scan_at"] = at.isoformat()
        state["last_scan_mode"] = mode
        state["priority_symbols"] = [row["symbol"] for row in document["all_candidates"]]
        if complete:
            state["active_candidate_symbols"] = sorted(current_symbols)
            state["pending_added_symbols"] = sorted(pending_symbols & current_symbols)
        else:
            state["active_candidate_symbols"] = sorted(previous_symbols)
            state["pending_added_symbols"] = sorted(pending_symbols)
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
    budget = policy.rth_request_budget if mode == "rth" else policy.daily_request_budget
    requests_used = 0
    errors: list[str] = []
    quotes: dict[str, EquityQuote] = {}
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
        try:
            quotes.update(fetch_equity_quote_batch(client, batch))
        except ProviderError as exc:
            errors.append(f"quotes:{type(exc).__name__}")

    rejection_counts: Counter[str] = Counter()
    price_hard_survivors: list[tuple[UniverseMember, EquityQuote, float]] = []
    for member in universe.members:
        quote = quotes.get(member.provider_symbol)
        reason, location = _hard_filter(member, quote, policy=policy, now=now, mode=mode)
        if reason is not None:
            rejection_counts[reason] += 1
            continue
        assert quote is not None and location is not None
        price_hard_survivors.append((member, quote, location))

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
                    if snapshot.ivp_13w is None or snapshot.ivp_26w is None:
                        continue
                    iv_snapshots[symbol] = snapshot
                    iv_cache[symbol] = iv_percentile_snapshot_to_payload(snapshot)
                    refreshed_iv_symbols.add(symbol)
            except ProviderError as exc:
                errors.append(f"ibkr_ivp:{type(exc).__name__}")

    hard_survivors: list[tuple[UniverseMember, EquityQuote, float]] = []
    iv_warming_survivors: list[tuple[UniverseMember, EquityQuote, float]] = []
    for item in price_hard_survivors:
        member = item[0]
        snapshot = iv_snapshots.get(_ibkr_provider_symbol(member.symbol))
        if snapshot is None or snapshot.ivp_13w is None or snapshot.ivp_26w is None:
            iv_warming_survivors.append(item)
            continue
        if snapshot.ivp_13w > policy.max_ivp_13w:
            rejection_counts["ivp_13w_above_limit"] += 1
            continue
        if snapshot.ivp_26w > policy.max_ivp_26w:
            rejection_counts["ivp_26w_above_limit"] += 1
            continue
        hard_survivors.append(item)

    histories = _mapping_state(state, "price_histories")
    detail_cache = _mapping_state(state, "details")
    required_benchmarks = list(
        dict.fromkeys(
            member.sector_benchmark for member, _quote, _location in hard_survivors
        )
    )
    for symbol in required_benchmarks:
        history = _close_history(histories.get(symbol))
        if len(history) < 35 and requests_used < budget:
            requests_used += 1
            try:
                history = list(fetch_daily_closes(client, symbol))
            except ProviderError as exc:
                errors.append(f"history:{symbol}:{type(exc).__name__}")
        benchmark_quote = quotes.get(symbol)
        if benchmark_quote is not None and benchmark_quote.last is not None:
            history = _upsert_close(history, ny.date(), benchmark_quote.last)
        if history:
            histories[symbol] = _close_history_payload(history)

    prioritized = _detail_order(hard_survivors, state)
    detailed_now: set[str] = set()
    detail_evaluated_now: set[str] = set()
    target_leaps_rejected: set[str] = set()
    for member, quote, _location in prioritized:
        if requests_used >= budget:
            break
        history = _close_history(histories.get(member.symbol))
        if len(history) < 35:
            requests_used += 1
            try:
                history = list(fetch_daily_closes(client, member.provider_symbol))
            except ProviderError as exc:
                errors.append(f"history:{member.symbol}:{type(exc).__name__}")
                continue
        if quote.last is not None:
            history = _upsert_close(history, ny.date(), quote.last)
            histories[member.symbol] = _close_history_payload(history)
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
        detail_evaluated_now.add(member.symbol)
        target = select_target_leaps(chain.contracts, policy)
        if target is None:
            rejection_counts["target_leaps_missing"] += 1
            detail_cache.pop(member.symbol, None)
            target_leaps_rejected.add(member.symbol)
            continue
        detail_cache[member.symbol] = _detail_payload(target, chain, fetched_at=now)
        detailed_now.add(member.symbol)

    strict_candidates: list[dict[str, Any]] = []
    warming: list[dict[str, Any]] = []
    for member, quote, location in hard_survivors:
        if member.symbol in target_leaps_rejected:
            continue
        history = _close_history(histories.get(member.symbol))
        if quote.last is not None:
            history = _upsert_close(history, ny.date(), quote.last)
            histories[member.symbol] = _close_history_payload(history)
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
            for reason in reasons:
                rejection_counts[reason] += 1
            continue
        scored = score_candidate(row, policy)
        if scored is None:
            rejection_counts["iv_or_liquidity_hard_filter"] += 1
            continue
        row.update(scored)
        row["data_quality"] = "ready"
        strict_candidates.append(row)

    for member, quote, location in iv_warming_survivors:
        row, reasons = _candidate_row(
            member=member,
            quote=quote,
            price_location=location,
            history=_close_history(histories.get(member.symbol)),
            detail={},
            benchmark_histories=histories,
            policy=policy,
            now=now,
            ny=ny,
            mode=mode,
            detail_current=False,
            iv_snapshot=iv_snapshots.get(_ibkr_provider_symbol(member.symbol)),
        )
        warming.append(row)
        for reason in reasons:
            rejection_counts[reason] += 1

    top, reserve = apply_crowding(strict_candidates, policy)
    notification_top, _notification_reserve = apply_crowding(
        strict_candidates,
        policy,
        sort_key=score_sort_key,
    )
    warming.sort(
        key=lambda row: (
            len(row.get("data_quality_reasons") or []),
            float(row.get("price_location_52w") or 1.0),
            str(row["symbol"]),
        )
    )
    request_limited = bool(hard_survivors) and len(detail_evaluated_now) < len(hard_survivors)
    ivp_incomplete = bool(iv_warming_survivors)
    document: dict[str, Any] = {
        "schema_version": SCHEMA_VERSION,
        "policy_version": POLICY_VERSION,
        "generated_at": now.isoformat(),
        "mode": mode,
        "timezone": "America/New_York",
        "universe_metadata": list(universe.metadata),
        "scan_complete": not request_limited and not errors,
        "automatic_ordering": False,
        "research_only": True,
        "counts": {
            "universe": len(universe.members),
            "quotes_present": sum(member.provider_symbol in quotes for member in universe.members),
            "price_hard_survivors": len(price_hard_survivors),
            "ivp_snapshots": sum(symbol in iv_snapshots for symbol in price_survivor_symbols),
            "ivp_refreshed": len(refreshed_iv_symbols),
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
        "rejection_counts": dict(sorted(rejection_counts.items())),
        "data_quality": {
            "status": "partial" if request_limited or ivp_incomplete or errors else "complete",
            "request_limited": request_limited,
            "ivp_incomplete": ivp_incomplete,
            "errors": sorted(set(errors)),
            "volatility_contract": "13-week and 26-week percentile calculated from IBKR TWS daily OPTION_IMPLIED_VOLATILITY history <= configured limits; 52-week percentile is context-only",
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
    ):
        return "quote_fields_missing", None
    if mode == "rth":
        if not quote.realtime:
            return "quote_delayed", None
        assert quote.quote_at is not None
        if (
            now - quote.quote_at.astimezone(timezone.utc)
        ).total_seconds() > policy.quote_max_age_seconds:
            return "quote_frozen", None
    elif (
        quote.quote_at is None or quote.quote_at.astimezone(ET).date() != now.astimezone(ET).date()
    ):
        return "quote_frozen", None
    assert quote.last is not None and quote.low_52w is not None and quote.high_52w is not None
    location = price_location_52w(quote.last, quote.low_52w, quote.high_52w)
    if location is None:
        return "price_range_invalid", None
    if location > policy.max_price_location_52w:
        return "above_52w_location", location
    if quote.market_cap is None or quote.market_cap < policy.min_market_cap:
        return "below_market_cap", location
    if quote.dividend_yield is None or quote.dividend_yield >= policy.max_dividend_yield:
        return "dividend_yield", location
    if not quote.optionable:
        return "not_optionable", location
    return None, location

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
    if not detail:
        reasons.append("target_leaps_warming")
    elif not detail_current:
        reasons.append("target_leaps_not_refreshed_this_run")
    option_quote_at = _parse_datetime(detail.get("quote_at"))
    if detail and mode == "rth":
        if bool(detail.get("delayed")):
            reasons.append("target_leaps_delayed")
        elif option_quote_at is None:
            reasons.append("target_leaps_timestamp_missing")
        elif (now - option_quote_at).total_seconds() > policy.option_quote_max_age_seconds:
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
    }
    return row, sorted(set(reasons))

def render_notification(document: Mapping[str, Any]) -> tuple[str, str]:
    mode = str(document.get("mode") or "rth")
    title = (
        "Growth Dislocation LEAPS · 日报" if mode == "daily" else "Growth Dislocation LEAPS · 新增"
    )
    counts = _mapping(document.get("counts"))
    lines = [
        f"# {title}",
        "",
        "## Scanner 状态",
        "",
        (
            f"- 严格候选 **{counts.get('strict_candidates', 0)}**；"
            f"未完成数据行（正文隐藏） **{counts.get('warming_rows', 0)}**；"
            f"本轮详细刷新 **{counts.get('detailed_this_run', 0)} / {counts.get('hard_survivors', 0)}**"
        ),
        f"- Schwab 请求 **{document.get('requests_used')} / {document.get('request_budget')}**；"
        f"Data Quality **{_mapping(document.get('data_quality')).get('status', 'unknown')}**",
        f"- 本轮新增严格候选：{', '.join(str(item) for item in document.get('added_symbols', [])) or '无'}",
        "- 仅做候选发现与排序；TRIGGER 不等于买入，仍需基本面复核。",
        "",
        "## 严格候选 Top 10（Score 排序）",
        "",
        "| Symbol | State | Market Cap | Score | 52W位置 | IVP 13W/26W | RSI | 行业RS 5D | LEAPS | Spread |",
        "|---|---:|---:|---:|---:|---:|---:|---:|---|---:|",
    ]
    top = document.get("notification_top10", document.get("top10"))
    if isinstance(top, list) and top:
        for row in top:
            if not isinstance(row, Mapping):
                continue
            lines.append(
                "| {symbol} | {state} | {market_cap} | {score} | {location} | {ivp} | {rsi} | {sector_rs} | {contract} | {spread} |".format(
                    symbol=row.get("symbol", "-"),
                    state=row.get("state", "-"),
                    market_cap=_market_cap(row.get("market_cap")),
                    score=_fmt(row.get("final_score")),
                    location=_pct(row.get("price_location_52w")),
                    ivp=_iv_gate_label(row),
                    rsi=_fmt(row.get("rsi14")),
                    sector_rs=_pct(row.get("rs_5d_sector")),
                    contract=row.get("leaps_symbol") or "-",
                    spread=_pct(row.get("leaps_spread_mid")),
                )
            )
    else:
        lines.append("| — | WATCH | — | — | — | 暂无严格候选 | — | — | — | — |")
    return title, "\n".join(lines)

def _detail_order(
    survivors: list[tuple[UniverseMember, EquityQuote, float]],
    state: dict[str, Any],
) -> list[tuple[UniverseMember, EquityQuote, float]]:
    by_symbol = {item[0].symbol: item for item in survivors}
    priority = [
        str(symbol) for symbol in state.get("priority_symbols", []) if str(symbol) in by_symbol
    ]
    remaining = sorted(
        (item for item in survivors if item[0].symbol not in set(priority)),
        key=lambda item: (item[2], item[0].symbol),
    )
    cursor = int(state.get("detail_cursor") or 0)
    if remaining:
        cursor %= len(remaining)
        remaining = remaining[cursor:] + remaining[:cursor]
        state["detail_cursor"] = (cursor + 1) % len(remaining)
    return [by_symbol[symbol] for symbol in priority] + remaining

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
    if raw.get("schema_version") not in {None, STATE_SCHEMA_VERSION}:
        return {}
    if raw.get("policy_version") not in {None, POLICY_VERSION}:
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
        if snapshot is None or snapshot.ivp_13w is None or snapshot.ivp_26w is None:
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
        if close > 0.0:
            by_day[day] = DailyClose(day, close)
    return [by_day[day] for day in sorted(by_day)][-MAX_CLOSE_HISTORY:]

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
        "leaps_bid",
        "leaps_ask",
        "leaps_spread_mid",
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
