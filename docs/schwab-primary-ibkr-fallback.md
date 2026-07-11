# Schwab primary and IBKR fallback decision

## Decision

SPX Spark uses Schwab as the normal SPX/SPXW/ES market-data provider. IBKR is
retained for three bounded responsibilities:

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
cannot flap the system back from fallback. Monitoring is RTH-only by default;
weekends and normal market closure cannot activate fallback or page the user.

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

`provider_failover.control_ibkr_stream_enabled` remains `false` until Schwab
WebSocket shadow acceptance is complete. This preserves the existing five-second
IBKR fast lane while the new control state and transition notifications run in
observation mode.

### Current entitlement: Market Data only, no Trader API

The deployed Schwab developer app currently has **Market Data product access
only**; it is **not** authorized for the Trader API (`/trader/v1/*`). The
WebSocket streamer login requires `/trader/v1/userPreference` to fetch
`streamerInfo`, so that call returns `401` even while Market Data REST quote
requests (`/marketdata/v1/quotes`, `/marketdata/v1/chains`) return `200`. This
is an account/app entitlement gap, not a token or code bug.

Consequently `schwab.streaming.mode` must stay `off` until Trader API access is
granted on the Schwab developer app and an RTH shadow-mode acceptance pass is
completed. Do not switch it to `shadow` or `live` before that, or the OAuth/
gateway process will loop on 401 streamer-login retries. REST quote and
option-chain collection are unaffected by this restriction.

The OAuth/gateway process now owns the optional Schwab WebSocket as well as the
refreshable token. Its default `shadow` mode subscribes the configured SPX,
SPY, RSP, ES, and MES Level-One universe, writes normal raw rows tagged
`sampling_mode=schwab_stream`, and keeps its latest state separate from the
production selector. A streaming failure therefore cannot take down the local
REST gateway or silently switch production quotes.

In `live` mode the REST collector excludes the WebSocket-owned symbols, so the
slower REST rows cannot overwrite the fast stream lane under the same canonical
provider key. Logical ES/MES roots are re-resolved on a configured cadence; a
quarterly contract change closes and reconnects the stream with the new symbols.

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

1. Compare Schwab WebSocket source timestamps, gaps, reconnect behavior, and
   field coverage during RTH.
2. Promote accepted Schwab streaming anchors from `shadow` to `live`.
3. Reconcile client `172` position shadowing against the temporary client `174`
   poller for one trading day.
4. Disable the legacy poller while keeping account visibility and alerts enabled.
5. Enable automatic IBKR stream control and keep its subscriptions to the
   configured minimal fallback universe.
6. Add read-only open-order/fill state; order writes remain prohibited until a
   separate live-execution approval.

The temporary compatibility variable `IBKR_POSITIONS_ENABLED` remains accepted
during migration. New deployments should set
`IBKR_BROKER_ACCOUNT_READ_ENABLED` and
`IBKR_LEGACY_POSITION_POLLER_ENABLED` explicitly.
