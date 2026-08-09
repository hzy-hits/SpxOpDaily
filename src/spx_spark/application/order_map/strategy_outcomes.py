"""Causal multi-horizon marks for selected and rejected strategy candidates."""

from __future__ import annotations

import json
from collections.abc import Mapping, Sequence
from datetime import date, datetime, time, timedelta, timezone
from functools import lru_cache
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

from spx_spark.application.order_map.strategy_regime import MARK_HORIZONS_MINUTES
from spx_spark.infrastructure.operational_db import (
    persist_strategy_outcome,
    read_due_strategy_observations,
)
from spx_spark.marketdata import Quote, as_utc, instrument_matches_id
from spx_spark.storage import LatestState, configured_quote_use_decision

NEW_YORK = ZoneInfo("America/New_York")

def observe_due_strategy_outcomes(
    latest: LatestState,
    *,
    now: datetime,
    data_root: str | Path,
    horizon_minutes: int | Sequence[int] | None = None,
    database_path: str | Path | None = None,
) -> dict[str, Any]:
    """Persist fresh conservative exit marks without claiming a fill.

    Default horizons follow ``MARK_HORIZONS_MINUTES`` (v3 multi-horizon marks).
    Pass a single int to retain the legacy one-horizon behaviour in tests.
    """

    sampled_at = as_utc(now)
    horizons: int | Sequence[int] = (
        MARK_HORIZONS_MINUTES if horizon_minutes is None else horizon_minutes
    )
    pending = read_due_strategy_observations(
        now=sampled_at,
        horizon_minutes=horizons,
        database_path=database_path,
    )
    statuses: dict[str, int] = {}
    outcome_ids = []
    for observation in pending:
        value = _observe(
            observation,
            latest=latest,
            sampled_at=sampled_at,
            data_root=Path(data_root),
        )
        outcome_id = persist_strategy_outcome(value, database_path=database_path)
        status = str(value["status"])
        statuses[status] = statuses.get(status, 0) + 1
        outcome_ids.append(outcome_id)
    return {
        "observed": len(outcome_ids),
        "statuses": statuses,
        "outcome_ids": outcome_ids,
        "horizons": list(horizons) if not isinstance(horizons, int) else [horizons],
    }


def _observe(
    observation: Mapping[str, Any],
    *,
    latest: LatestState,
    sampled_at: datetime,
    data_root: Path,
) -> dict[str, Any]:
    decision = _map(observation.get("decision"))
    legs = [dict(_map(item)) for item in observation.get("legs") or ()]
    decision_id = str(decision.get("decision_id") or "")
    target_at = _time(observation.get("target_at"))
    horizon_minutes = int(observation.get("horizon_minutes") or 0)
    if horizon_minutes <= 0:
        raise ValueError("strategy outcome horizon must be positive")
    entry_debit = _entry_debit(legs)
    exit_legs, exit_reasons = _exit_legs(legs, latest=latest, now=sampled_at)
    exit_credit = _exit_credit(exit_legs) if not exit_reasons else None
    entry_spx = _number(_map(_map(decision.get("market_facts")).get("spot")).get("spx"))
    current_spx, spx_reason = _current_spx(latest, sampled_at)
    reasons = [*exit_reasons]
    if entry_debit is None:
        reasons.append("entry_debit_unavailable")
    if current_spx is None:
        reasons.append(spx_reason or "exit_spx_unavailable")
    candidate = _map(decision.get("candidate")) or _map(
        _map(decision.get("why_not")).get("nearest_candidate")
    )
    regime = _map(decision.get("regime"))
    breach = _invalidation_breach(
        decision,
        candidate,
        data_root=data_root,
        decision_at=_time(decision.get("decision_at")),
        target_at=target_at,
    )
    censor_kind = _censor_kind(
        observation,
        decision=decision,
        target_at=target_at,
        breach=breach,
        exit_credit=exit_credit,
        entry_debit=entry_debit,
    )
    status = "observed" if censor_kind is None else "censored"
    if status == "censored" and censor_kind is not None:
        reasons.append(f"censor:{censor_kind}")
    option_return = (
        (exit_credit - entry_debit) / entry_debit * 10_000.0
        if status == "observed" and entry_debit and exit_credit is not None
        else None
    )
    gross_pnl = (
        round((exit_credit - entry_debit) * 100.0, 2)
        if status == "observed" and exit_credit is not None and entry_debit is not None
        else None
    )
    spx_return = (
        (current_spx / entry_spx - 1.0) * 10_000.0
        if status == "observed" and current_spx is not None and entry_spx not in (None, 0.0)
        else None
    )
    return {
        "decision_id": decision_id,
        "horizon_minutes": horizon_minutes,
        "status": status,
        "target_at": target_at.isoformat(),
        "sampled_at": sampled_at.isoformat(),
        "hypothesis_direction": str(candidate.get("direction") or "none").lower(),
        "spx_return_bps": round(spx_return, 6) if spx_return is not None else None,
        "option_return_bps": round(option_return, 6) if option_return is not None else None,
        "attributes": {
            "schema_version": "strategy_outcome_mark.v2",
            "label_basis": "decision_quote_shadow_not_fill",
            "entry_combo_ask": entry_debit,
            "exit_combo_bid": exit_credit,
            "combo_bid": exit_credit,
            "combo_ask": _exit_ask(exit_legs) if not exit_reasons else None,
            "gross_option_pnl": gross_pnl,
            "net_option_pnl": None,
            "commission": None,
            "slippage": None,
            "fill_status": "not_observed_no_order_capability",
            "reasons": sorted(set(reasons)),
            "exit_legs": exit_legs,
            "sample_lag_seconds": round((sampled_at - target_at).total_seconds(), 3),
            "spot_spx": current_spx,
            "regime_terminal_state": regime.get("terminal_state") or regime.get("path_state"),
            "censor_kind": censor_kind,
            "invalidation_breached": breach["invalidation_breached"],
            "breach_at": breach["breach_at"],
            "breach_scan_gap": breach["breach_scan_gap"],
            "label_kind": (
                "structural_exit"
                if breach["invalidation_breached"] is True
                else "horizon_mark"
            ),
        },
    }


def _invalidation_breach(
    decision: Mapping[str, Any],
    candidate: Mapping[str, Any],
    *,
    data_root: Path,
    decision_at: datetime,
    target_at: datetime,
) -> dict[str, Any]:
    session_date = str(decision.get("session_date") or "").strip()
    invalidation = candidate.get("invalidation_spx")
    if not session_date or invalidation in {None, ""}:
        return {
            "invalidation_breached": None,
            "breach_at": None,
            "breach_scan_gap": False,
        }
    path = (
        data_root
        / "features"
        / "spx_standardized_samples"
        / f"date={session_date}"
        / "events.jsonl"
    )
    try:
        stat = path.stat()
    except OSError:
        return {
            "invalidation_breached": None,
            "breach_at": None,
            "breach_scan_gap": True,
        }
    rows = _load_spx_minute_session(str(path), stat.st_mtime_ns, stat.st_size)
    start = decision_at.replace(second=0, microsecond=0)
    end = target_at.replace(second=0, microsecond=0)
    expected_minutes = int((end - start).total_seconds() // 60)
    by_minute = {
        minute: row
        for minute, row in rows
        if start <= minute <= end
    }
    gap_run = 0
    for offset in range(expected_minutes + 1):
        minute = start + timedelta(minutes=offset)
        row = by_minute.get(minute)
        low, high = _minute_bounds(row)
        if low is None or high is None:
            gap_run += 1
            if gap_run > 2:
                return {
                    "invalidation_breached": None,
                    "breach_at": None,
                    "breach_scan_gap": True,
                }
            continue
        gap_run = 0
        if _breach_hit(candidate, invalidation, low=low, high=high):
            return {
                "invalidation_breached": True,
                "breach_at": minute.isoformat(),
                "breach_scan_gap": False,
            }
    return {
        "invalidation_breached": False,
        "breach_at": None,
        "breach_scan_gap": False,
    }


@lru_cache(maxsize=64)
def _load_spx_minute_session(
    path_text: str, _mtime_ns: int, _size: int
) -> tuple[tuple[datetime, Mapping[str, Any]], ...]:
    rows: list[tuple[datetime, Mapping[str, Any]]] = []
    try:
        lines = Path(path_text).read_text(encoding="utf-8").splitlines()
    except OSError:
        return ()
    for line in lines:
        try:
            decoded = json.loads(line)
        except json.JSONDecodeError:
            continue
        row = _map(decoded)
        minute = _time(row.get("minute")) if row else None
        if minute is None:
            continue
        rows.append((minute, row))
    rows.sort(key=lambda item: item[0])
    return tuple(rows)


def _minute_bounds(row: Mapping[str, Any] | None) -> tuple[float | None, float | None]:
    if not row or row.get("status") != "selected":
        return None, None
    selected = _map(row.get("selected"))
    if not selected:
        return None, None
    low = _number(selected.get("low"))
    high = _number(selected.get("high"))
    price = _number(selected.get("price"))
    return low if low is not None else price, high if high is not None else price


def _breach_hit(
    candidate: Mapping[str, Any],
    invalidation: object,
    *,
    low: float,
    high: float,
) -> bool:
    direction = str(candidate.get("direction") or "").upper()
    if direction == "UP":
        level = _number(invalidation)
        return level is not None and low <= level
    if direction == "DOWN":
        level = _number(invalidation)
        return level is not None and high >= level
    levels = [
        level
        for item in (
            invalidation
            if isinstance(invalidation, Sequence) and not isinstance(invalidation, (str, bytes))
            else (invalidation,)
        )
        if (level := _number(item)) is not None
    ]
    if not levels:
        return False
    return low <= min(levels) or high >= max(levels)


def _censor_kind(
    observation: Mapping[str, Any],
    *,
    decision: Mapping[str, Any],
    target_at: datetime,
    breach: Mapping[str, Any],
    exit_credit: float | None,
    entry_debit: float | None,
) -> str | None:
    hint = str(observation.get("censor_hint") or "")
    if hint == "service_gap":
        return "service_gap"
    if hint == "session_end_before_horizon" or _session_end_before_horizon(
        decision, target_at
    ):
        return "session_end_before_horizon"
    if breach.get("invalidation_breached") is True:
        return "breach_quote_unavailable"
    if exit_credit is None or entry_debit is None:
        return "quote_gap"
    return None


def _session_end_before_horizon(
    decision: Mapping[str, Any], target_at: datetime
) -> bool:
    try:
        day = date.fromisoformat(str(decision.get("session_date") or ""))
    except ValueError:
        return False
    close_at = datetime.combine(day, time(16, 0), tzinfo=NEW_YORK).astimezone(timezone.utc)
    return target_at > close_at


def _entry_debit(legs: list[dict[str, Any]]) -> float | None:
    values = []
    for leg in legs:
        quantity, bid, ask = (_number(leg.get(key)) for key in ("quantity", "bid", "ask"))
        if quantity is None or bid is None or ask is None:
            return None
        values.append(quantity * (ask if quantity > 0 else bid))
    debit = sum(values)
    return round(debit, 4) if debit > 0.0 else None


def _exit_credit(legs: list[dict[str, Any]]) -> float | None:
    values = []
    for leg in legs:
        quantity, bid, ask = (_number(leg.get(key)) for key in ("quantity", "bid", "ask"))
        if quantity is None or bid is None or ask is None:
            return None
        values.append(quantity * (bid if quantity > 0 else ask))
    return round(max(sum(values), 0.0), 4)


def _exit_ask(legs: list[dict[str, Any]]) -> float | None:
    values = []
    for leg in legs:
        quantity, bid, ask = (_number(leg.get(key)) for key in ("quantity", "bid", "ask"))
        if quantity is None or bid is None or ask is None:
            return None
        values.append(quantity * (ask if quantity > 0 else bid))
    ask = sum(values)
    return round(ask, 4) if ask > 0.0 else None


def _exit_legs(
    entry_legs: list[dict[str, Any]], *, latest: LatestState, now: datetime
) -> tuple[list[dict[str, Any]], list[str]]:
    result, reasons = [], []
    for leg in entry_legs:
        contract_id = str(leg.get("instrument_id") or "")
        attributes = _json_map(leg.get("attributes_json"))
        provider = str(attributes.get("provider") or "")
        candidates = [
            quote
            for quote in latest.quotes
            if instrument_matches_id(quote.instrument, contract_id)
            and quote.provider.value == provider
            and quote.bid is not None
            and quote.ask is not None
            and configured_quote_use_decision(quote, as_of=now).pricing_allowed
        ]
        quote = max(candidates, key=_transport_at, default=None)
        if quote is None:
            reasons.append(f"exit_leg_unavailable:{contract_id}:{provider or 'unknown'}")
            continue
        result.append(
            {
                "instrument_id": contract_id,
                "provider": provider,
                "quantity": leg.get("quantity"),
                "bid": quote.bid,
                "ask": quote.ask,
                "source_at": _transport_at(quote).isoformat(),
                "provider_source_at": (
                    quote.quote_time or quote.trade_time
                ).isoformat()
                if quote.quote_time or quote.trade_time
                else None,
            }
        )
    if len(result) != len(entry_legs):
        reasons.append("exit_combo_incomplete")
    return result, reasons


def _current_spx(latest: LatestState, now: datetime) -> tuple[float | None, str | None]:
    quote = latest.best_quote("index:SPX")
    if quote is None:
        return None, "exit_spx_quote_missing"
    decision = configured_quote_use_decision(quote, as_of=now)
    value = _number(quote.effective_price)
    if value is None or not decision.pricing_allowed:
        return None, f"exit_spx_{decision.reason}"
    return value, None


def _transport_at(quote: Quote) -> datetime:
    return as_utc(quote.last_update_at or quote.received_at)


def _json_map(value: object) -> Mapping[str, Any]:
    if not isinstance(value, str):
        return {}
    try:
        decoded = json.loads(value)
    except json.JSONDecodeError:
        return {}
    return _map(decoded)


def _map(value: object) -> Mapping[str, Any]:
    return value if isinstance(value, Mapping) else {}


def _number(value: object) -> float | None:
    return float(value) if isinstance(value, int | float) and not isinstance(value, bool) else None


def _time(value: object) -> datetime:
    parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        raise ValueError("strategy outcome timestamp must be timezone-aware")
    return parsed.astimezone(timezone.utc)
