from __future__ import annotations

import json
import os
from dataclasses import asdict, dataclass
from pathlib import Path

from spx_spark.marketdata import MarketDataQuality, Quote
from spx_spark.storage import LatestState


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
    "index:DJX",
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

HYPERLIQUID_PROXY_IDS = (
    "crypto_perp:xyz:SP500",
    "crypto_perp:SPX",
)

TRADFI_ANCHOR_IDS = (
    "future:ES",
    "future:MES",
    "index:SPX",
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
    usable_count = sum(
        1
        for entry in entries
        if entry.price is not None
        and entry.quality
        not in {
            MarketDataQuality.MISSING.value,
            MarketDataQuality.ERROR.value,
        }
    )
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
        )
    return entry_from_quote(quote, state=state)


def entry_from_quote(quote: Quote, *, state: LatestState) -> MarketContextEntry:
    price = quote.effective_price
    close = quote.close
    move_bps = None
    if price is not None and close is not None and close > 0:
        move_bps = (price / close - 1.0) * 10_000.0
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
    if numerator.price is None or denominator.price is None or denominator.price <= 0:
        return None
    return numerator.price / denominator.price


def env_float(name: str, default: float) -> float:
    raw = os.getenv(name)
    if not raw:
        return default
    return float(raw)


def latest_polymarket_context_path() -> Path:
    explicit = os.getenv("POLYMARKET_LATEST_CONTEXT_PATH")
    if explicit:
        return Path(explicit)
    data_root = os.getenv("MARKET_DATA_DATA_ROOT") or os.getenv("MAINTENANCE_DATA_ROOT") or "data"
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
    return bool(entry and entry.price is not None and entry.quality not in BAD_CONTEXT_QUALITIES)


def first_usable_entry(
    entries: dict[str, MarketContextEntry],
    instrument_ids: tuple[str, ...],
) -> MarketContextEntry | None:
    for instrument_id in instrument_ids:
        entry = entries.get(instrument_id)
        if usable_entry(entry):
            return entry
    return None


def hyperliquid_spx_proxy_gate(entries: dict[str, MarketContextEntry]) -> dict[str, object]:
    warn_bps = env_float("HYPERLIQUID_PROXY_BASIS_WARN_BPS", 50.0)
    block_bps = env_float("HYPERLIQUID_PROXY_BASIS_BLOCK_BPS", 100.0)
    proxy = first_usable_entry(entries, HYPERLIQUID_PROXY_IDS)
    if proxy is None:
        return {
            "state": "missing",
            "usable_for_alert": False,
            "context_only": True,
            "reason": "Hyperliquid SPX proxy missing or degraded.",
            "basis_bps": None,
            "anchor": None,
            "warn_bps": warn_bps,
            "block_bps": block_bps,
        }

    anchor = first_usable_entry(entries, TRADFI_ANCHOR_IDS)
    if anchor is None:
        return {
            "state": "unanchored_context_only",
            "usable_for_alert": False,
            "context_only": True,
            "reason": "No live ES/MES/SPX anchor; Hyperliquid proxy is context only.",
            "basis_bps": None,
            "anchor": None,
            "proxy": proxy.instrument_id,
            "warn_bps": warn_bps,
            "block_bps": block_bps,
        }

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
        "warn_bps": warn_bps,
        "block_bps": block_bps,
    }
