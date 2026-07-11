"""One-off: golden options_map before exposure extraction (Phase 0)."""
from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path

from spx_spark.market_calendar import DEFAULT_MARKET_CALENDAR
from spx_spark.marketdata import (
    InstrumentId,
    MarketDataQuality,
    OptionGreeks,
    Provider,
    Quote,
)
from spx_spark.options_map import build_options_map
from spx_spark.storage import LatestState

AS_OF = datetime(2026, 7, 13, 14, 0, tzinfo=timezone.utc)
OUT = Path(__file__).resolve().parents[1] / "tests/golden/options_map_pre_extraction.json"


def make_option(
    *,
    expiry: str,
    strike: float,
    right: str,
    mark: float,
    iv: float,
    gamma: float,
    delta: float,
    open_interest: float | None,
    volume: float | None,
    now: datetime,
) -> Quote:
    return Quote(
        instrument=InstrumentId.option(
            "SPX",
            expiry=expiry,
            strike=strike,
            right=right,
            trading_class="SPXW",
        ),
        provider=Provider.IBKR,
        provider_symbol=f"SPXW:{expiry}:{strike}:{right}",
        received_at=now,
        quality=MarketDataQuality.LIVE,
        bid=mark - 0.1,
        ask=mark + 0.1,
        mark=mark,
        open_interest=open_interest,
        volume=volume,
        quote_time=now,
        greeks=OptionGreeks(
            implied_vol=iv,
            delta=delta,
            gamma=gamma,
            theta=-1.0,
            vega=0.3,
            model="test",
        ),
    )


def main() -> None:
    research = DEFAULT_MARKET_CALENDAR.research_expiry(AS_OF)
    expiry = research.strftime("%Y%m%d")
    print(f"research_expiry={research.isoformat()} expiry={expiry}")

    underlier = Quote(
        instrument=InstrumentId.index("SPX"),
        provider=Provider.IBKR,
        provider_symbol="index:SPX",
        received_at=AS_OF,
        quality=MarketDataQuality.LIVE,
        mark=7500.0,
        quote_time=AS_OF,
    )

    rows = [
        (7500.0, "C", 500.0, 1000.0, 0.002659482225, 0.503989356315, 10.0),
        (7500.0, "P", 2000.0, 800.0, 0.002659482225, -0.496010643685, 11.0),
        (7550.0, "C", 1500.0, 600.0, 0.002525063695, 0.373640314205, 7.5),
        (7550.0, "P", 100.0, 200.0, 0.002525063695, -0.626359685795, 8.0),
    ]
    quotes = [underlier]
    for strike, right, volume, oi, gamma, delta, mark in rows:
        quotes.append(
            make_option(
                expiry=expiry,
                strike=strike,
                right=right,
                mark=mark,
                iv=0.20,
                gamma=gamma,
                delta=delta,
                open_interest=oi,
                volume=volume,
                now=AS_OF,
            )
        )

    state = LatestState(
        created_at=AS_OF,
        as_of=AS_OF,
        quotes=tuple(quotes),
        best_quotes=tuple(quotes),
    )

    payload = build_options_map(state).to_dict()
    payload.pop("created_at", None)

    OUT.parent.mkdir(parents=True, exist_ok=True)
    OUT.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print(f"wrote {OUT}")


if __name__ == "__main__":
    main()
