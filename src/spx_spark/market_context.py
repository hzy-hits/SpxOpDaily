from __future__ import annotations

import json
import os
from dataclasses import asdict, dataclass
from pathlib import Path
from statistics import median

from spx_spark.marketdata import MarketDataQuality, Quote
from spx_spark.runtime_config import runtime_value
from spx_spark.storage import LatestState, configured_quote_use_decision


DEFAULT_MARKET_CONTEXT_INSTRUMENTS = (
    "index:SPX",
    "index:VIX",
    "index:VIX1D",
    "index:VIX9D",
    "index:VIX3M",
    "index:VVIX",
    "index:SKEW",
    "index:NDX",
    "index:RUT",
    "index:DJI",
    "index:DJU",
    "equity:SPY",
    "equity:QQQ",
    "equity:IWM",
    "equity:DIA",
    "equity:HYG",
    "equity:LQD",
    "equity:TLT",
    "equity:IEF",
    "equity:SHY",
    "equity:UUP",
    "equity:GLD",
    "equity:USO",
    "equity:RSP",
    "equity:XLU",
    "future:ES",
    "future:MES",
    "crypto_perp:xyz:SP500",
)

BAD_CONTEXT_QUALITIES = {
    MarketDataQuality.MISSING.value,
    MarketDataQuality.ERROR.value,
    MarketDataQuality.STALE.value,
    MarketDataQuality.UNKNOWN.value,
}

SECTOR_BREADTH_QUALITIES = {
    MarketDataQuality.LIVE,
    MarketDataQuality.FROZEN,
    MarketDataQuality.STALE,
}

HYPERLIQUID_PROXY_IDS = (
    "crypto_perp:xyz:SP500",
    "crypto_perp:SPX",
)

TRADFI_ANCHOR_IDS = (
    "index:SPX",
    "future:ES",
    "future:MES",
)


@dataclass(frozen=True)
class MarketContextEntry:
    instrument_id: str
    provider: str | None
    quality: str
    price: float | None
    close: float | None
    move_bps: float | None
    bid: float | None
    ask: float | None
    spread_bps: float | None
    age_ms: float | None
    freshness: str
    research_usable: bool
    alert_allowed: bool
    pricing_allowed: bool
    use_reason: str

    def to_dict(self) -> dict[str, object]:
        return asdict(self)


def build_market_context(
    state: LatestState,
    *,
    instrument_ids: tuple[str, ...] = DEFAULT_MARKET_CONTEXT_INSTRUMENTS,
) -> dict[str, object]:
    entries = [context_entry(state, instrument_id) for instrument_id in instrument_ids]
    by_id = {entry.instrument_id: entry for entry in entries}
    live_count = sum(1 for entry in entries if entry.quality == MarketDataQuality.LIVE.value)
    usable_count = sum(1 for entry in entries if entry.price is not None and entry.research_usable)
    return {
        "as_of": state.as_of.isoformat(),
        "entries": [entry.to_dict() for entry in entries],
        "quality_summary": {
            "live_count": live_count,
            "usable_count": usable_count,
            "total_count": len(entries),
        },
        "derived": {
            "vix1d_vix9d": ratio(by_id, "index:VIX1D", "index:VIX9D"),
            "vix9d_vix": ratio(by_id, "index:VIX9D", "index:VIX"),
            "vix_vix3m": ratio(by_id, "index:VIX", "index:VIX3M"),
            "qqq_spy": ratio(by_id, "equity:QQQ", "equity:SPY"),
            "iwm_spy": ratio(by_id, "equity:IWM", "equity:SPY"),
            "dia_spy": ratio(by_id, "equity:DIA", "equity:SPY"),
            "rsp_spy": ratio(by_id, "equity:RSP", "equity:SPY"),
            "xlu_spy": ratio(by_id, "equity:XLU", "equity:SPY"),
            "hyg_lqd": ratio(by_id, "equity:HYG", "equity:LQD"),
            "tlt_ief": ratio(by_id, "equity:TLT", "equity:IEF"),
            "spx_sector_breadth": spx_sector_breadth(state),
            "hyperliquid_spx_proxy": hyperliquid_spx_proxy_gate(by_id),
            "polymarket_context": load_latest_polymarket_context(),
        },
    }


def context_entry(state: LatestState, instrument_id: str) -> MarketContextEntry:
    quote = state.best_quote(instrument_id)
    if quote is None:
        return MarketContextEntry(
            instrument_id=instrument_id,
            provider=None,
            quality=MarketDataQuality.MISSING.value,
            price=None,
            close=None,
            move_bps=None,
            bid=None,
            ask=None,
            spread_bps=None,
            age_ms=None,
            freshness="unknown",
            research_usable=False,
            alert_allowed=False,
            pricing_allowed=False,
            use_reason="quote_missing",
        )
    return entry_from_quote(quote, state=state)


def entry_from_quote(quote: Quote, *, state: LatestState) -> MarketContextEntry:
    price = quote.effective_price
    close = quote.close
    move_bps = None
    if price is not None and close is not None and close > 0:
        move_bps = (price / close - 1.0) * 10_000.0
    decision = configured_quote_use_decision(quote, as_of=state.as_of)
    return MarketContextEntry(
        instrument_id=quote.instrument.canonical_id,
        provider=quote.provider.value,
        quality=quote.quality.value,
        price=price,
        close=close,
        move_bps=move_bps,
        bid=quote.bid,
        ask=quote.ask,
        spread_bps=quote.spread_bps,
        age_ms=quote.quote_age_ms(state.as_of),
        freshness=decision.freshness.value,
        research_usable=decision.research_usable,
        alert_allowed=decision.alert_allowed,
        pricing_allowed=decision.pricing_allowed,
        use_reason=decision.reason,
    )


def ratio(
    entries: dict[str, MarketContextEntry],
    numerator_id: str,
    denominator_id: str,
) -> float | None:
    numerator = entries.get(numerator_id)
    denominator = entries.get(denominator_id)
    if numerator is None or denominator is None:
        return None
    if not numerator.research_usable or not denominator.research_usable:
        return None
    if numerator.price is None or denominator.price is None or denominator.price <= 0:
        return None
    return numerator.price / denominator.price


def spx_sector_breadth(state: LatestState) -> dict[str, object]:
    raw_instrument_ids = runtime_value("market_context.spx_sector_instrument_ids")
    if not isinstance(raw_instrument_ids, list):
        raise TypeError("market_context.spx_sector_instrument_ids must be a list")
    instrument_ids = tuple(str(value) for value in raw_instrument_ids)
    min_usable = int(runtime_value("market_context.sector_breadth_min_usable"))
    max_age_ms = float(runtime_value("market_context.sector_quote_max_age_seconds")) * 1_000.0
    unchanged_band = float(runtime_value("market_context.sector_unchanged_band_bps"))
    directional_score = float(runtime_value("market_context.sector_directional_bias_score"))
    confirmation_move = float(runtime_value("market_context.direction_confirmation_move_bps"))

    usable_moves: list[float] = []
    for instrument_id in instrument_ids:
        move = fresh_move_from_close_bps(
            state,
            instrument_id,
            max_age_ms=max_age_ms,
        )
        if move is not None:
            usable_moves.append(move)

    advancing = sum(move > unchanged_band for move in usable_moves)
    declining = sum(move < -unchanged_band for move in usable_moves)
    unchanged = len(usable_moves) - advancing - declining
    breadth_score = (advancing - declining) / len(usable_moves) if usable_moves else None
    sufficient = len(usable_moves) >= min_usable

    spy_move = fresh_move_from_close_bps(
        state,
        "equity:SPY",
        max_age_ms=max_age_ms,
    )
    rsp_move = fresh_move_from_close_bps(
        state,
        "equity:RSP",
        max_age_ms=max_age_ms,
    )
    confirmations_available = spy_move is not None and rsp_move is not None
    directional_bias = "neutral_unclear"
    if sufficient and breadth_score is not None and confirmations_available:
        assert spy_move is not None
        assert rsp_move is not None
        if (
            breadth_score >= directional_score
            and spy_move >= confirmation_move
            and rsp_move >= confirmation_move
        ):
            directional_bias = "bullish"
        elif (
            breadth_score <= -directional_score
            and spy_move <= -confirmation_move
            and rsp_move <= -confirmation_move
        ):
            directional_bias = "bearish"
        else:
            directional_bias = "mixed_tactical"

    if not sufficient:
        state_label = "insufficient_fresh_sectors"
    elif confirmations_available:
        state_label = "usable_confirmed"
    else:
        state_label = "usable_unconfirmed"

    return {
        "state": state_label,
        "confirmation_state": (
            "spy_rsp_confirmed" if confirmations_available else "spy_rsp_missing_or_stale"
        ),
        "directional_bias": directional_bias,
        "usable_sector_count": len(usable_moves),
        "configured_sector_count": len(instrument_ids),
        "advancing_sector_count": advancing,
        "declining_sector_count": declining,
        "unchanged_sector_count": unchanged,
        "breadth_score": breadth_score,
        "median_sector_move_bps": median(usable_moves) if usable_moves else None,
        "spy_move_bps": spy_move,
        "rsp_move_bps": rsp_move,
        "minimum_usable_sectors": min_usable,
        "directional_score_threshold": directional_score,
        "confirmation_move_bps": confirmation_move,
    }


def fresh_move_from_close_bps(
    state: LatestState,
    instrument_id: str,
    *,
    max_age_ms: float,
) -> float | None:
    quote = state.best_quote(instrument_id)
    if quote is None or quote.quality not in SECTOR_BREADTH_QUALITIES:
        return None
    age_ms = quote.quote_age_ms(state.as_of)
    if age_ms is None or age_ms > max_age_ms:
        return None
    price = quote.effective_price
    close = quote.close
    if price is None or close is None or close <= 0:
        return None
    return (price / close - 1.0) * 10_000.0


def env_float(name: str, default: float) -> float:
    raw = os.getenv(name)
    if not raw:
        return default
    return float(raw)


def latest_polymarket_context_path() -> Path:
    explicit = os.getenv("POLYMARKET_LATEST_CONTEXT_PATH")
    if explicit:
        return Path(explicit)
    data_root = (
        os.getenv("MARKET_DATA_DATA_ROOT")
        or os.getenv("MAINTENANCE_DATA_ROOT")
        or str(runtime_value("maintenance.data_root"))
    )
    return Path(data_root) / "latest" / "polymarket_context.json"


def missing_polymarket_context() -> dict[str, object]:
    return {
        "state": "missing",
        "research_only": True,
        "human_visible": False,
        "usage_gate": "context_only_no_kelly_no_direct_alert",
    }


def load_latest_polymarket_context() -> dict[str, object]:
    path = latest_polymarket_context_path()
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return missing_polymarket_context()
    return payload if isinstance(payload, dict) else missing_polymarket_context()


def usable_entry(entry: MarketContextEntry | None) -> bool:
    return bool(entry and entry.price is not None and entry.research_usable)


def actionable_entry(entry: MarketContextEntry | None) -> bool:
    return bool(entry and entry.price is not None and entry.alert_allowed)


def first_usable_entry(
    entries: dict[str, MarketContextEntry],
    instrument_ids: tuple[str, ...],
) -> MarketContextEntry | None:
    for instrument_id in instrument_ids:
        entry = entries.get(instrument_id)
        if usable_entry(entry):
            return entry
    return None


def first_actionable_entry(
    entries: dict[str, MarketContextEntry],
    instrument_ids: tuple[str, ...],
) -> MarketContextEntry | None:
    for instrument_id in instrument_ids:
        entry = entries.get(instrument_id)
        if actionable_entry(entry):
            return entry
    return None


def hyperliquid_spx_proxy_gate(entries: dict[str, MarketContextEntry]) -> dict[str, object]:
    default_warn_bps = env_float(
        "HYPERLIQUID_PROXY_BASIS_WARN_BPS",
        float(runtime_value("hyperliquid.proxy_basis_warn_bps")),
    )
    default_block_bps = env_float(
        "HYPERLIQUID_PROXY_BASIS_BLOCK_BPS",
        float(runtime_value("hyperliquid.proxy_basis_block_bps")),
    )
    proxy = first_usable_entry(entries, HYPERLIQUID_PROXY_IDS)
    if proxy is None:
        return {
            "state": "missing",
            "usable_for_alert": False,
            "context_only": True,
            "reason": "Hyperliquid SPX proxy missing or degraded.",
            "basis_bps": None,
            "anchor": None,
            "anchor_is_future": False,
            "warn_bps": default_warn_bps,
            "block_bps": default_block_bps,
        }

    anchor = first_actionable_entry(entries, TRADFI_ANCHOR_IDS)
    if anchor is None:
        return {
            "state": "unanchored_context_only",
            "usable_for_alert": False,
            "context_only": True,
            "reason": "No live ES/MES/SPX anchor; Hyperliquid proxy is context only.",
            "basis_bps": None,
            "anchor": None,
            "proxy": proxy.instrument_id,
            "anchor_is_future": False,
            "warn_bps": default_warn_bps,
            "block_bps": default_block_bps,
        }

    anchor_is_future = anchor.instrument_id.startswith("future:")
    if anchor_is_future:
        warn_bps = env_float(
            "HYPERLIQUID_PROXY_FUTURES_BASIS_WARN_BPS",
            float(runtime_value("hyperliquid.proxy_futures_basis_warn_bps")),
        )
        block_bps = env_float(
            "HYPERLIQUID_PROXY_FUTURES_BASIS_BLOCK_BPS",
            float(runtime_value("hyperliquid.proxy_futures_basis_block_bps")),
        )
    else:
        warn_bps = default_warn_bps
        block_bps = default_block_bps

    assert proxy.price is not None
    assert anchor.price is not None
    basis_bps = (proxy.price / anchor.price - 1.0) * 10_000.0
    abs_basis = abs(basis_bps)
    if abs_basis >= block_bps:
        state = "basis_blocked"
        usable_for_alert = False
    elif abs_basis >= warn_bps:
        state = "basis_warn"
        usable_for_alert = False
    else:
        state = "basis_ok"
        usable_for_alert = True
    return {
        "state": state,
        "usable_for_alert": usable_for_alert,
        "context_only": not usable_for_alert,
        "reason": (
            "Hyperliquid proxy is anchored to TradFi."
            if usable_for_alert
            else "Hyperliquid proxy basis is too wide for alert scoring."
        ),
        "basis_bps": basis_bps,
        "anchor": anchor.instrument_id,
        "proxy": proxy.instrument_id,
        "anchor_is_future": anchor_is_future,
        "warn_bps": warn_bps,
        "block_bps": block_bps,
    }
