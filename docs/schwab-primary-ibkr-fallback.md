# Schwab primary and IBKR fallback decision

## Decision

SPX Spark uses Schwab as the normal RTH SPX/SPXW provider and as a continuous
ES/MES provider. During SPX Global Trading Hours (GTH), IBKR is the production
SPXW pricing source unless fresh Schwab option coverage is proven by source
timestamps. IBKR is retained for three bounded responsibilities:

1. L1 market-data fallback when Schwab direct anchors fail health checks.
2. The production SPXW pricing feed during GTH, when Schwab SPXW source
   timestamps are frozen.
3. Paper-order and execution-algorithm validation for a future broker adapter.

IBKR is not treated as a depth, tick, or full-chain advantage in the current
system because the deployed collector only consumes L1 data.

## Oracle broker-session decision

The normal Oracle deployment uses the dedicated IBKR Paper username and
`127.0.0.1:4002`. This changes the broker session, not the market-data matrix:

- Schwab remains the normal RTH SPX/SPXW provider.
- IBKR Paper remains the RTH fallback and the production SPXW GTH provider.
- The feed is usable only after live market-data type, advancing provider
  timestamps, two-sided prices, and configured SPXW coverage are verified.
- If the Paper feed is delayed, frozen, unsubscribed, or blocked by a competing
  market-data session, failover is unavailable and new entries fail closed.

The Live username is intentionally not kept logged in on Oracle. Live-account
positions, orders, fills, and PnL therefore remain outside this deployment's
authoritative state. IBKR Mobile or another explicitly approved live-account
surface remains the source of truth for real exposure.

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
- IBKR disconnect remains critical during GTH or active fallback because it
  removes the only accepted SPXW pricing lane.

Paper positions must not upgrade or suppress live-account risk alerts. They are
simulation state and are relevant only to paper execution tests.

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
Taken together, the evidence indicates that the Trader API path used by this
retail token does not currently distribute the complete OPRA GTH index-option
session. It does not show an application symbol, expiry, parser, subscription,
or transport failure.

This finding must not be generalized into an undocumented claim about Schwab's
internal feed topology. Cboe states that SPX/SPXW quotes and trades are carried
on OPRA GTH, and the same account can observe current SPXW GTH quotes in
thinkorswim. The deployed Trader API WebSocket nevertheless accepts the
160-contract `LEVELONE_OPTIONS` subscription, sends one 160-row initial image
whose newest source time is 17:00 ET, and then sends no further option message;
ES/MES messages continue normally on the same connection. The defensible
conclusion is therefore narrower: thinkorswim and Trader API expose different
GTH capabilities for this account/app today. We cannot infer whether the cause
is an entitlement route, product policy, or a Schwab-side implementation gap.

Primary exchange references:

- [Cboe C1 trading hours](https://www.cboe.com/about/hours/us-options) list SPX
  GTH from 20:15 to 09:25 ET;
- [Cboe's extended-hours FAQ](https://www.cboe.com/document/tech-spec/document/technical-specifications/equity-options-extended-trading-hours-faq/)
  states that proprietary index options quote and trade on the OPRA GTH system.

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

Persistent IBKR client `172` retains an isolated position-shadow implementation
for paper execution tests and a future approved Live deployment. In the normal
Paper market-data deployment, account reads, position shadowing, and the legacy
client `174` position poller are disabled. This prevents simulated Paper
positions from being mistaken for real exposure and removes a minute polling
task that cannot observe the Live account.

The position code is retained rather than deleted because a future execution
service needs the same contract, quantity, order, fill, and reconciliation
boundaries. Re-enabling it requires an explicit broker mode that labels every
snapshot as `paper` or `live`; only `live` may become authoritative for real
position alerts.

The remaining rollout order is:

1. Authenticate the dedicated Paper username on port `4002`.
2. Prove live SPXW GTH source timestamps, two-sided coverage, IV/Greeks fields,
   gaps, and reconnect behavior across a complete session.
3. Keep account reads and both position lanes disabled in the Paper
   market-data deployment.
4. Complete five-session Schwab WebSocket source-timestamp, gap, reconnect, and
   field-coverage measurement during GTH and RTH.
5. Add paper order/fill state only inside an explicitly labelled simulation
   mode; live order writes remain prohibited until a separate approval.

The temporary compatibility variable `IBKR_POSITIONS_ENABLED` remains accepted
during migration. New deployments should set
`IBKR_BROKER_ACCOUNT_READ_ENABLED` and
`IBKR_LEGACY_POSITION_POLLER_ENABLED` explicitly. The normal Paper deployment
sets all three position/account-read gates to `false`.
