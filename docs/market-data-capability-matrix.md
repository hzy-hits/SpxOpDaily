# Market-data capability matrix

This is the operational contract for what SPX Spark may use. HTTP success,
WebSocket subscription acceptance, and local receipt time are not evidence of
live market data; the provider source timestamp must advance.

| Lane | Session | Status | Permitted use | Prohibited use |
| --- | --- | --- | --- | --- |
| Schwab REST equity `regular` | US RTH | production | SPY/QQQ/IWM/RSP and cross-asset context | GTH SPX replacement |
| Schwab REST equity `extended` | eligible extended/overnight | production when its source time is newer | overnight ETF/equity direction and breadth | assuming every equity is 24/5; option pricing |
| Schwab stream ES/MES | CME Globex | production | path, returns, volume, VWAP, basis context | native SPX or SPXW GEX |
| Schwab stream NQ/RTY/YM | Globex | validation | acceptance telemetry only | agent/strategy decisions until promoted |
| Schwab stream one ES futures option | Globex | validation | entitlement, continuity, timestamp, spread and OI probe | ES surface/GEX; SPXW pricing |
| IBKR SPXW current expiry | Cboe GTH | production | exclusive GTH SPXW bid/ask, IV and option pricing | replacement with stale Schwab rows |
| Schwab SPXW current expiry | Cboe GTH | unavailable for live pricing | frozen audit/last structure only | GTH model price, limit, probability or entry |
| Schwab SPX cash index | outside RTH | frozen reference | last cash close/reference | executable spot or fresh GTH direction |
| Hyperliquid proxy | 24/7 | fallback context | thin directional cross-check | SPX price, option repricing or execution |

## Persisted session contract

Every Schwab REST equity quote records:

- `market_session`: the selected source block;
- `quote_time` / `trade_time`: timestamps of the selected block;
- `regular_source_at` and `extended_source_at`: both provider clocks;
- `session_observations`: both blocks with their bid, ask, last, mark and clocks.

Selection is source-time driven. A stale `extended` block cannot displace a
newer regular quote, and a fresh extended block can displace the frozen regular
block without being mislabeled as cash-session data.

## Acceptance-only lanes

NQ/RTY/YM and the ES futures-option probe are persisted for evidence but are not
referenced by the decision engine. `/healthz.stream.validation` reports, per
provider symbol, message rows, normalized rows, live rows and last source time.

Promotion requires a separate reviewed change after continuous session
coverage is measured. It must not happen automatically from one successful
subscription.
