# Schwab primary and IBKR fallback decision

## Decision

SPX Spark uses Schwab as the normal RTH SPX/SPXW provider and as a continuous
ES/MES provider. During SPX Global Trading Hours (GTH), IBKR is the production
SPXW pricing source unless fresh Schwab option coverage is proven by source
timestamps. IBKR is retained for three bounded responsibilities:

1. L1 market-data fallback when Schwab direct anchors fail health checks.
2. Read-only positions, orders, and fills required by a future broker adapter.
3. Programmatic execution only after a separately approved `live` rollout.

IBKR is not treated as a depth, tick, or full-chain advantage in the current
system because the deployed collector only consumes L1 data.

## Market-data state machine

The provider controller observes the configured direct anchors and persists one
of four modes:

- `schwab_primary`: Schwab is healthy; an idle IBKR standby disconnect is silent.
- `recovery_pending`: Schwab failed the configured consecutive-observation gate
  and IBKR fallback is being requested; entry eligibility is false until one
  direct provider is confirmed healthy.
- `ibkr_fallback`: IBKR has fresh direct anchors and has taken over.
- `both_unavailable`: neither direct provider is usable; new entries are blocked.

The persisted control document exposes `new_entries_allowed`. It is true only
for a fresh, active `schwab_primary` or `ibkr_fallback` state and otherwise
fails closed. There is no automated order writer today; a future broker adapter
must consume this gate before accepting any opening order.

Recovery requires consecutive healthy Schwab observations, so one good response
cannot flap the system back from fallback. Monitoring follows SPX RTH and the
regular ES Globex session while excluding the daily maintenance break and
weekend closure. Outside RTH, ES is the required direct anchor; an unavailable
cash SPX print alone cannot activate fallback.

Human notifications are edge-triggered:

- one notification when IBKR successfully takes over;
- one critical notification when both direct providers are unavailable;
- one notification after Schwab recovery is confirmed;
- no routine IBKR standby disconnect/reconnect notification;
- IBKR disconnect remains critical when an SPXW position exists or execution
  mode is explicitly `live`.

## Source selection

SPXW options are selected per canonical contract. Quality and freshness are
evaluated before the configured provider priority, so a fresh IBKR contract can
replace a stale Schwab contract without causing residual IBKR quotes to exclude
the rest of the Schwab chain.

The fast shock/reclaim path selects a same-provider SPX and ES pair using
`intraday_shock.anchor_provider_priority`. It never combines a Schwab SPX quote
with an IBKR ES quote into one synchronized observation or compares shock
endpoints from different providers. A sustained provider switch retires the old
event state after the configured reset interval so the detector does not freeze.

## Staged activation

`provider_failover.control_ibkr_stream_enabled` remains `false` until automatic
IBKR subscription-control acceptance is complete. This preserves the existing
five-second IBKR fast lane while the control state and transition notifications
run in production without dynamically stopping the required GTH fallback.

### Current entitlement: Trader API approved

The deployed Schwab developer app now has Trader API access. Streamer login
returns `streamerInfo`, the WebSocket subscription is accepted, and ES/MES
Level-One messages are live. An accepted 160-symbol SPXW subscription produces
the initial cached option snapshot, but no fresh option update during early
GTH. This must be reported as an option-coverage gap, not as an OAuth, gateway,
or whole-provider outage. RTH option streaming remains a separate capability
and is not disabled by the GTH result.

### SPXW GTH feed diagnosis

Cboe trades SPX/SPXW during Global Trading Hours from 8:15 p.m. through
9:25 a.m. ET. A successful Schwab REST response is therefore not evidence that
the returned option market is current. On 2026-07-13 the deployed Market Data
REST app returned `200`, `isDelayed=false`, and option `realtime=true`, while
every SPXW `quoteTimeInLong` still ended at 2026-07-10 20:59:59 UTC. The same
session returned a current `/ESU26` quote. This isolates the failure to Schwab's
REST SPXW GTH delivery rather than exchange closure, OAuth expiry, HTTP quota,
or the entire Schwab provider.

The 2026-07-14 GTH session ruled out an application trade-date rollover defect:

- at 20:55 ET on 2026-07-13, `hot_expiry` was `20260714` and all 160 hot symbols
  used OCC expiry `260714`;
- the REST request explicitly used `fromDate=2026-07-14` and
  `toDate=2026-07-14`, and Schwab returned map key `2026-07-14:1` plus
  `SPXW  260714...` contracts;
- `:1` is Schwab's calendar-day DTE while the ET wall clock is still July 13.
  SPX Spark keys the instrument by the explicit expiry date, not this suffix;
- two 80-contract chain reads 15 seconds apart had zero price changes, zero
  timestamp changes, and the same `underlyingPrice=7515.34`;
- the newest Schwab option source time remained 17:00 ET, while IBKR delivered
  current July 14 SPXW quotes during the same observation.

No documented request variant exposed a hidden GTH quote. Tests of
`fields=all/quote/extended/regular`, `indicative=true/false`, and chain
`entitlement=NP/PN/PP` all returned the same frozen pricing. The REST quote
`extended` section was empty for the SPXW contract. The Schwab Market Hours API
also described index options only with a `regularMarket` session from 09:30 to
16:15 ET, while its futures products explicitly included overnight sessions.
Taken together, the evidence indicates that the Trader API does not distribute
the complete OPRA GTH index-option session to this retail token. It does not
show an application symbol, expiry, parser, subscription, or transport failure.

Historical data contains Schwab SPXW updates beginning near 08:00 ET, so the
last portion of GTH must continue to be measured rather than assumed absent.
Until five sessions establish a repeatable start time, the supported production
contract is:

- Schwab ES/MES: live throughout Globex and used as an independent anchor;
- IBKR SPXW: live GTH bid/ask, mid, IV inputs, and trigger repricing;
- Schwab SPXW during GTH: frozen structure only, never an executable price;
- Schwab wide chain during RTH: primary breadth, OI, Greeks, and structure;
- provider mode: `ibkr_fallback` during GTH when IBKR option coverage passes,
  with `new_entries_allowed=true`; otherwise fail closed.

The collector reports request receipt time and vendor market time separately:

- `chain_as_of`: when SPX Spark fetched the chain;
- `chain_market_as_of`: newest vendor quote timestamp in that chain;
- `coverage.*.market_status`: `current`, `stale`, or `missing`;
- `fresh_usable_strikes` and `fresh_two_sided_ratio`: current pricing coverage;
- `positive_oi_strikes`: whether the response contains usable OI structure.

Schwab's unavailable `-999` model sentinels are normalized to null. A response
with all-zero OI and no valid model values no longer receives a fresh
`structure_time`. Provider transport may remain connected while the capability
is explicitly degraded because all priced quotes are stale.

There is no GTH/session request parameter on `/marketdata/v1/chains` or
`/marketdata/v1/quotes`; increasing REST cadence cannot repair a frozen upstream
snapshot. Trader API approval, reauthorization, non-empty `streamerInfo`, and
live streaming activation are complete. The remaining acceptance path is:

1. Keep the bounded 160-contract SPXW hot lane subscribed through complete GTH
   sessions without reconnecting when only membership order changes.
2. During five GTH sessions, measure the first current Schwab option source
   timestamp, nonzero quote updates, gaps/reconnects, and price parity with IBKR.
3. Treat Schwab as GTH-price capable only inside empirically current intervals;
   never infer freshness from HTTP 200, `realtime=true`, or subscription ACK.
4. Retain IBKR as the GTH SPXW source wherever Schwab remains frozen; this is a
   Schwab entitlement/product boundary, not an application defect.

The OAuth/gateway process now owns the Schwab WebSocket as well as the
refreshable token. Trader API access is approved and the deployed default is
`schwab.streaming.mode=live`. It subscribes the configured SPX, SPY, RSP, ES,
and MES Level-One universe plus the bounded current SPXW hot lane. Gateway
`/healthz` reports subscription acceptance, per-service message counts, and
last-message timestamps so transport, ES anchors, and GTH option coverage are
audited independently. A streaming failure cannot take down the local REST
gateway.

In `live` mode the REST collector excludes the WebSocket-owned symbols, so the
slower REST rows cannot overwrite the fast stream lane under the same canonical
provider key. Logical ES/MES roots are re-resolved on a configured cadence; a
quarterly contract change closes and reconnects the stream with the new symbols.

Failover recovery into `ibkr_fallback` requires consecutive healthy IBKR
observations (`provider_failover.ibkr_recovery_observations`, default `2`) so a
half-recovered farm cannot flap the system early. IBKR stream quote freeze on
connectivity loss is controlled by
`ibkr_stream.freeze_quotes_on_connectivity_loss` (default `true`).

Persistent IBKR client `172` also has an isolated position-shadow lane. When
connected, it performs the complete startup position fetch and writes
`ibkr_positions_shadow.json` on the configured cadence. Shadow failures are
reported without replacing the last complete snapshot or reconnecting the
market-data lane. Client `174` remains the production position source until an
RTH comparison proves contract, quantity, cost, and completeness parity.

The broker socket and the market-data subscriptions have separate gates.
Account visibility or `live` execution can keep client `172` connected in
account-only standby while IBKR L1 subscriptions are off. A competing-market-
data session starts the configured retry cooldown but leaves account standby
eligible, avoiding a blind position interval merely to probe L1 again.

The remaining rollout order is:

1. Complete five-session Schwab WebSocket source-timestamp, gap, reconnect, and
   field-coverage measurement during GTH and RTH.
2. Reconcile client `172` position shadowing against the temporary client `174`
   poller for one trading day.
3. Disable the legacy poller while keeping account visibility and alerts enabled.
4. Enable automatic IBKR stream control and keep its subscriptions to the
   configured minimal fallback universe.
5. Add read-only open-order/fill state; order writes remain prohibited until a
   separate live-execution approval.

The temporary compatibility variable `IBKR_POSITIONS_ENABLED` remains accepted
during migration. New deployments should set
`IBKR_BROKER_ACCOUNT_READ_ENABLED` and
`IBKR_LEGACY_POSITION_POLLER_ENABLED` explicitly.
