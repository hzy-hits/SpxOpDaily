# SPXW 0DTE Decision Surface

This dashboard is a read-only research projection for the front SPXW expiry and
the next actual SPXW trading expiry. It turns the existing option-chain snapshot
into a bounded spot-by-time scenario grid; it does not place or authorize
orders.

## What the colors mean

- `signed_gamma` uses the documented call-positive / put-negative proxy. Blue
  means a positive proxy value, coral/red means a negative proxy value, and the
  neutral midpoint is exactly zero. The scale is symmetric around zero.
- `gross_gamma` uses absolute contract Gamma and is always non-negative. It
  describes where Gamma mass is concentrated, without assigning a dealer side.
- `charm` and `vanna` use the same call-positive / put-negative position proxy
  as `signed_gamma`.
- A dashed zero ridge and peak/trough markers accompany the color field so sign
  and transitions are not encoded by color alone.

The position proxy can be weighted by open interest or reported cumulative
volume. Volume weighting is an activity proxy, not signed flow. Neither view
contains participant identity, buy/sell direction, open/close classification,
or a known market-maker book.

The scenario holds each contract's observed implied volatility and the selected
OI/volume weights fixed while spot and time-to-expiry move across the grid. The
time axis is therefore a decay projection, not a forecast of future spot,
volatility, positions, or flow. A calibrated evolving IV surface is a later
model layer and must not be inferred from this first version.

## Data and safety contract

The projection reads the canonical latest-state store and reuses the same
freshness policy and front/next-expiry selection as the options-map pipeline.
It publishes a versioned JSON snapshot atomically every five seconds. Each
snapshot carries `as_of`, `created_at`, `valid_until`, source/coverage quality,
and an explicit status.

If the underlier, option quotes, implied volatility, or remaining time is not
usable, the projection publishes an `unavailable` result with reasons and no
surface. The browser does not keep an old surface on screen after expiry. This
is especially important outside regular trading hours, when Schwab may not
provide a fresh SPXW chain.

Historical replay is a separate contract, endpoint, and browser mode. A replay
has `kind=spxw_surface_dashboard_replay`, `mode=replay`, `frozen=true`, and no
live `valid_until`. It is read once and never participates in the five-second
polling loop. The UI repeats `HISTORICAL REPLAY`, the ET as-of time, `Frozen
replay`, and `Not live` in text so color is not the only distinction.

The first replay is a point-in-time reconstruction at Friday 2026-07-17 14:35
ET (`2026-07-17T18:35:00Z`). It reads normalized Schwab rows received in the
preceding 15 seconds and requires every available `received_at`, `source_at`,
`quote_time`, `trade_time`, and `last_update_at` clock to be at or before the
requested cutoff. Equal-clock variants are resolved only by surface-input
completeness; the loader still selects one complete source row and never
stitches fields.
The checked slice resolved 35 complementary quote/structure variants, selected
zero future-clock rows, and left zero ambiguous instruments. At that cutoff,
SPX was 7462.30 with 1.79-second age; front-expiry usable coverage was 159/160
(99.375%) and next-expiry coverage was 80/80. The front surface therefore
retains an `unpaired_strike` warning. This is sufficient for a visual structure
replay, not exact-spread execution.

The replay also pins the Parquet and raw JSONL hashes, lake writer/schema,
effective freshness policy, and an artifact digest. Lake v1 does not preserve a
separate option `structure_time` and its `compacted_at` column is empty; the
payload marks both limitations explicitly. Consequently, loader-level
`field_stitching=false` does not prove that price, IV, and OI were observed on
one exchange clock. Before rendering, the browser recomputes the pinned policy
and artifact digests with Web Crypto and fails closed on any content change.

`automatic_ordering` is always `false`. Exact spread execution still requires
fresh, pinned IBKR leg quotes and the independent execution gates; this surface
cannot bypass them.

## Runtime layout

The projection worker writes only the dashboard payload to the dedicated
publish directory:

```text
/srv/data/spx-spark/data/published/spxw-surface/snapshot.json
/srv/data/spx-spark/data/published/spxw-surface/replays/2026-07-17T183500Z.json
```

The nginx sidecar mounts that directory read-only and exposes only the exact
live snapshot and checked replay endpoints. It shares the code-server network
namespace, so the intended browser entry point remains behind code-server
authentication:

```text
https://code.zh3nyu.com/proxy/18082/
https://spx.zh3nyu.com/
https://spx.zh3nyu.com/friday
```

The short hostname is a redirect-only loopback service; it exposes no JSON and
lands on the code-server-authenticated URL. The frontend refreshes every five
seconds in Live mode and shows freshness, coverage, input source, selected
expiry, weighting, and metric beside the chart. Friday Replay performs one
immutable fetch and never changes the live publisher or execution state.
