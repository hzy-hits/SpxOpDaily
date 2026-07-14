# Schwab GTH market-data capability audit

## Scope

This document separates four capabilities that must not be treated as
interchangeable:

1. thinkorswim display/trading availability;
2. Trader API WebSocket streaming services;
3. Trader API REST regular quote fields;
4. Trader API REST `extended` quote fields.

An accepted subscription, HTTP 200, `realtime=true`, or a recent local receipt
time does not prove that the vendor source timestamp is current.

## 2026-07-14 early-GTH observation

Observation window: 21:18-21:22 ET on 2026-07-13, inside the SPX GTH session.
The production WebSocket remained connected with zero reconnects. A 20-second
paired observation produced:

| WebSocket service | Subscribed universe | Message delta | Row delta | Result |
| --- | --- | ---: | ---: | --- |
| `LEVELONE_FUTURES` | `/ESU26`, `/MESU26` | 20 | 40 | live |
| `LEVELONE_EQUITIES` | `$SPX`, `SPY`, `RSP` | 0 | 0 | no GTH updates |
| `LEVELONE_OPTIONS` | 160 current SPXW contracts | 0 | 0 | no GTH updates |

The newest WebSocket source times at the end of the observation were:

- futures: current to the observation second, normally 30-100ms latency;
- equities: `SPY` stopped at 20:00 ET; `RSP` stopped just before 20:00 ET;
- cash index: `$SPX` remained an old non-GTH value;
- SPXW options: the initial image stopped at 17:00 ET.

The option subscription itself was accepted and returned one 160-row initial
image. The absence of later option messages is therefore not a symbol-parser,
subscription-ACK, transport, or reconnect failure.

## REST cross-check

At the same time, `/marketdata/v1/quotes?fields=all` showed a different
capability matrix.

### Futures

The regular `quote` block had current source timestamps for:

- `/ESU26`, `/MESU26`;
- `/NQU26`, `/MNQU26`;
- `/RTYU26`, `/YMU26`;
- `/CLU26`, `/GCQ26`, `/ZNU26`.

Only ES/MES were directly verified on WebSocket. The other contracts prove
current Trader API REST entitlement, but remain `stream_unverified` until they
are subscribed on the production token-owner connection.

### Stocks and ETFs

The regular `quote` block stopped at 20:00 ET, but the nested `extended` block
continued updating for `SPY`, `QQQ`, `IWM`, `NVDA`, `TSLA`, and `GLD`. Across a
20-second paired read, every symbol advanced its extended quote timestamp and
bid/ask; most also advanced trade time. `RSP` did not advance in that window.

The current adapter reads only `payload.quote` and ignores
`payload.extended`. Consequently SPX Spark currently marks these usable REST
overnight quotes stale. This is an application normalization gap, separate from
the WebSocket service's lack of overnight equity messages.

### Cash indexes and SPXW

`$SPX`, `$VIX`, `$VIX1D`, `$NDX`, and `$RUT` did not have current GTH source
timestamps. Current-expiry SPXW REST quotes and the WebSocket initial image both
remained frozen at 17:00 ET. `realtime=true` on those payloads did not make the
prices current.

## Current production contract

| Instrument class | WebSocket GTH status | REST GTH status | Production use |
| --- | --- | --- | --- |
| ES/MES futures | verified live | verified live | primary Schwab GTH anchors |
| Other liquid futures | not directly stream-tested | verified live for sampled contracts | do not label stream-live yet |
| 24/5 stocks/ETFs | SPY verified not updating | live in `extended` for sampled eligible symbols | add explicit REST extended lane |
| Cash indexes | not live | not live | frozen reference only |
| SPXW index options | subscription accepted, not live | not live | IBKR GTH pricing; Schwab frozen structure only |
| ES futures option | `./ESU26C7600` verified live | verified live | validation only; never SPXW pricing |
| Forex | not tested | not tested | unsupported until a bounded probe passes |

## External product boundary

Schwab documents 24/5 trading for a selected stock/ETF universe specifically on
thinkorswim. That statement does not promise identical Trader API WebSocket
delivery. Schwab also documents nearly continuous futures sessions. Cboe's SPX
GTH and OPRA GTH availability establish that the exchange market exists, but do
not establish that every broker API redistributes it.

## Implemented follow-up

1. REST equity normalization selects the newer regular/extended source block
   and persists `market_session`, `regular_source_at`, `extended_source_at`, and
   both session observations.
2. NQ/RTY/YM run in an explicit validation universe with per-symbol message,
   normalization, live-row, and last-source telemetry.
3. `LEVELONE_FUTURES_OPTIONS` supports exactly one configured probe symbol.
   REST verified `./ESU26C7600` live at 2026-07-14 01:36 UTC. After the
   token-owner restart, WebSocket source time advanced continuously with live
   bid/ask rows. NQ/RTY/YM also passed the initial controlled live-stream check.
4. Pure GTH SPXW best-quote selection fails closed to IBKR. Schwab SPXW rows
   remain available as frozen audit/structure observations but cannot become
   the GTH pricing projection.
5. Five complete GTH sessions, including the 07:00-09:25 ET overlap, remain the
   production-quality acceptance window.
