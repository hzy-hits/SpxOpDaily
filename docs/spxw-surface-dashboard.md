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

`automatic_ordering` is always `false`. Exact spread execution still requires
fresh, pinned IBKR leg quotes and the independent execution gates; this surface
cannot bypass them.

## Runtime layout

The projection worker writes only the dashboard payload to the dedicated
publish directory:

```text
/srv/data/spx-spark/data/published/spxw-surface/snapshot.json
```

The nginx sidecar mounts that directory read-only and exposes only the exact
snapshot endpoint. It shares the code-server network namespace, so the intended
browser entry point remains behind code-server authentication:

```text
https://code.zh3nyu.com/proxy/18082/
```

The frontend refreshes every five seconds and shows freshness, coverage, input
source, selected expiry, weighting, and metric beside the chart.
