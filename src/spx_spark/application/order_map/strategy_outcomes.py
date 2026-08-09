"""Causal multi-horizon marks for selected and rejected strategy candidates."""

from __future__ import annotations

import json
from collections.abc import Mapping, Sequence
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from spx_spark.application.order_map.strategy_regime import MARK_HORIZONS_MINUTES
from spx_spark.infrastructure.operational_db import (
    persist_strategy_outcome,
    read_due_strategy_observations,
)
from spx_spark.marketdata import Quote, as_utc, instrument_matches_id
from spx_spark.storage import LatestState, configured_quote_use_decision


def observe_due_strategy_outcomes(
    latest: LatestState,
    *,
    now: datetime,
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
        value = _observe(observation, latest=latest, sampled_at=sampled_at)
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
    observation: Mapping[str, Any], *, latest: LatestState, sampled_at: datetime
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
    status = "observed" if exit_credit is not None and entry_debit is not None else "exit_quote_unavailable"
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
        if current_spx is not None and entry_spx not in (None, 0.0)
        else None
    )
    candidate = _map(decision.get("candidate")) or _map(
        _map(decision.get("why_not")).get("nearest_candidate")
    )
    regime = _map(decision.get("regime"))
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
        },
    }


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
