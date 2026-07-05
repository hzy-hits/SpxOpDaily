from __future__ import annotations

from dataclasses import asdict, dataclass

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
