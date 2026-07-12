"""TradFi / chain / Hyperliquid research and pricing spot resolution."""

from __future__ import annotations

import os
from dataclasses import replace
from datetime import datetime, timezone

from spx_spark.analytics.options.pricing import finite_float
from spx_spark.application.order_map.models import HL_SP500_PROXY_ID, SpotResolution
from spx_spark.market_calendar import DEFAULT_MARKET_CALENDAR
from spx_spark.market_context import build_market_context
from spx_spark.options_map import OptionsMap, actionable_chain_implied_spot
from spx_spark.storage import LatestState, configured_quote_use_decision


def spx_cash_session_open(now_utc: datetime) -> bool:
    return DEFAULT_MARKET_CALENDAR.is_rth_open(now_utc)


def hyperliquid_sp500_price(
    state: LatestState,
    *,
    as_of: datetime | None = None,
) -> float | None:
    quote = state.best_quote(HL_SP500_PROXY_ID)
    if quote is None:
        return None
    decision = configured_quote_use_decision(quote, as_of=as_of or state.as_of)
    if not decision.research_usable:
        return None
    return finite_float(quote.mid or quote.mark or quote.effective_price)


def _actionable_chain_spot(
    state: LatestState,
    options_map: OptionsMap,
    *,
    as_of: datetime,
) -> float | None:
    if not options_map.expiries:
        return None
    front = options_map.expiries[0]
    return actionable_chain_implied_spot(
        state,
        expiry=front.expiry,
        as_of=as_of,
    )


def _actionable_tradfi_spot(
    state: LatestState,
    *,
    as_of: datetime,
) -> tuple[float | None, str | None]:
    # Only cash SPX is level-compatible with SPXW option repricing. ES/MES
    # remain independent liveness/basis anchors; SPY*10 is an ATM fallback.
    for instrument_id, multiplier in (("index:SPX", 1.0),):
        quote = state.best_quote(instrument_id)
        if quote is None:
            continue
        decision = configured_quote_use_decision(quote, as_of=as_of)
        price = finite_float(quote.effective_price)
        if decision.pricing_allowed and price is not None and price > 0:
            return price * multiplier, instrument_id
    return None, None


def _hl_basis_thresholds(pricing_source: str | None) -> tuple[float, float]:
    if pricing_source and pricing_source.startswith("future:"):
        warn_name = "HYPERLIQUID_PROXY_FUTURES_BASIS_WARN_BPS"
        block_name = "HYPERLIQUID_PROXY_FUTURES_BASIS_BLOCK_BPS"
        defaults = (80.0, 150.0)
    else:
        warn_name = "HYPERLIQUID_PROXY_BASIS_WARN_BPS"
        block_name = "HYPERLIQUID_PROXY_BASIS_BLOCK_BPS"
        defaults = (50.0, 100.0)
    return (
        float(os.getenv(warn_name, str(defaults[0]))),
        float(os.getenv(block_name, str(defaults[1]))),
    )


def resolve_spx_spot(
    state: LatestState,
    options_map: OptionsMap,
    *,
    warnings: list[str] | None = None,
    now: datetime | None = None,
) -> SpotResolution:
    """Separate research context from prices allowed to drive trade math."""
    now = now or datetime.now(tz=timezone.utc)
    hl_price = hyperliquid_sp500_price(state, as_of=now)
    market_context = build_market_context(replace(state, as_of=now))
    derived = market_context.get("derived")
    market_gate = (
        derived.get("hyperliquid_spx_proxy")
        if isinstance(derived, dict) and isinstance(derived.get("hyperliquid_spx_proxy"), dict)
        else {}
    )
    chain_price = _actionable_chain_spot(state, options_map, as_of=now)
    tradfi_price, tradfi_source = _actionable_tradfi_spot(state, as_of=now)
    candidate_price = chain_price if chain_price is not None else tradfi_price
    candidate_source = "chain_implied" if chain_price is not None else tradfi_source

    divergence_bps = None
    gate_state = "anchor_only"
    pricing_allowed = candidate_price is not None
    reason = "actionable chain or TradFi reference available"
    if hl_price is not None:
        if candidate_price is None:
            gate_state = "unanchored"
            pricing_allowed = False
            reason = "Hyperliquid is the only usable reference"
        else:
            divergence_bps = (hl_price / candidate_price - 1.0) * 10_000.0
            warn_bps, block_bps = _hl_basis_thresholds(candidate_source)
            if abs(divergence_bps) >= block_bps:
                gate_state = "basis_blocked"
                pricing_allowed = False
                reason = "Hyperliquid divergence exceeds the basis block threshold"
            elif abs(divergence_bps) >= warn_bps:
                gate_state = "basis_warn"
                pricing_allowed = False
                reason = "Hyperliquid divergence exceeds the basis warning threshold"
            else:
                gate_state = "basis_ok"
                pricing_allowed = True
                reason = "Hyperliquid is anchored to actionable chain or TradFi evidence"
        market_gate_state = str(market_gate.get("state") or "")
        if market_gate_state in {
            "unanchored_context_only",
            "basis_warn",
            "basis_blocked",
        }:
            gate_state = (
                "unanchored"
                if market_gate_state == "unanchored_context_only"
                else market_gate_state
            )
            pricing_allowed = False
            reason = str(market_gate.get("reason") or reason)
            market_basis = market_gate.get("basis_bps")
            if isinstance(market_basis, int | float):
                divergence_bps = float(market_basis)
    elif candidate_price is None:
        gate_state = "missing"
        pricing_allowed = False
        reason = "No usable SPX research or pricing reference"

    if not pricing_allowed and warnings is not None:
        warnings.append(f"pricing blocked: {gate_state} ({reason})")

    outside_cash = not spx_cash_session_open(now)
    if hl_price is not None and (outside_cash or candidate_price is None):
        research_price, research_source = hl_price, "hl_perp"
    elif candidate_price is not None:
        research_price, research_source = candidate_price, candidate_source
    elif hl_price is not None:
        research_price, research_source = hl_price, "hl_perp"
    else:
        research_price = None
        research_source = None
        fallback_source = options_map.underlier.source
        fallback_quote = state.best_quote(fallback_source) if fallback_source else None
        if fallback_quote is not None:
            fallback_decision = configured_quote_use_decision(fallback_quote, as_of=now)
            if fallback_decision.research_usable:
                research_price = finite_float(options_map.underlier.price)
                research_source = fallback_source

    return SpotResolution(
        research_price=research_price,
        research_source=research_source,
        pricing_price=candidate_price if pricing_allowed else None,
        pricing_source=candidate_source if pricing_allowed else None,
        pricing_allowed=pricing_allowed,
        gate_state=gate_state,
        reason=reason,
        divergence_bps=divergence_bps,
    )
