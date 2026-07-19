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

Historical replay is a separate contract and browser mode. A frame has
`kind=spxw_surface_dashboard_replay`, `mode=replay`, `frozen=true`, and no live
`valid_until`. Replay never participates in the five-second live polling loop.
The UI repeats `HISTORICAL REPLAY`, the exact ET/UTC cutoff, `Frozen`, `Not live`,
and `Bounded PIT` in text so it cannot be confused with a current lease.

`/replay` is a session player rather than one static Friday image. Its primary
chart uses actual session time on the horizontal axis and absolute SPX price on
the vertical axis, with the observed SPX path overlaid on positive/negative
Gamma-proxy bands. The older spot-by-forward-time surface remains available as
a collapsed scenario diagnostic; it is not presented as the historical SPX
chart.

The browser renders the playhead at approximately 30 visual frames per second,
but it does not manufacture 30-fps market observations. SPX readouts use the
latest observation known at the playhead (typically one to two seconds apart),
while Gamma uses the latest valid five-minute keyframe. Missing or expired
Gamma intervals remain visibly blank instead of being interpolated. One compact
trend artifact is loaded before playback, so animation does not fetch a new
surface frame on every visual frame.

The replay Y-axis uses a fixed window derived from the first known SPX
observation rather than the full-session high/low, so future prices do not set
the scale. A move beyond that window is clipped in this version. Gamma color
intensity is normalized independently inside each keyframe: sign and zones are
comparable over time, but color depth is not an absolute cross-time magnitude.

The player discovers trading dates from the normalized Schwab Parquet lake,
indexes five-minute session buckets, and chooses the last complete observed
chain cutoff in each bucket. Cutoffs therefore carry their real seconds and are
not interpolated or forced onto round five-minute timestamps. The player
provides a date selector, timeline, previous/next controls, and 1x/2x/4x
playback while preserving the selected front/next expiry role.

Every frame reads the preceding 15 seconds and requires all available
`received_at`, `source_at`, `quote_time`, `trade_time`, and `last_update_at`
clocks to be at or before the requested cutoff. Future-clock candidates are
excluded, equal-clock variants are resolved by surface-input completeness, one
complete source row is selected per instrument, and fields are never stitched.
The Parquet hashes are checked before and after the read. Each policy-v3 frame
also carries raw-lineage hashes, the effective projection policy digest, and an
artifact digest that the browser recomputes with Web Crypto before rendering.

This is an explicitly **bounded**, not mathematically proven, point-in-time
replay. Historical Schwab `received_at` records the collection-cycle start; the
lake does not have a per-request `response_finished_at`/`available_at`. The five
known clocks bound future data and the payload always requires zero selected
lookahead rows, but a row whose actual HTTP response completed after the cutoff
cannot be ruled out from legacy data. Policy v3 therefore publishes
`point_in_time_confidence=bounded_not_proven`,
`availability_clock_available=false`, and the known limitations. The browser
fails closed if those fields are missing or softened. Lake v1 also lacks a
separate option `structure_time`, so `field_stitching=false` does not prove that
price, IV, and OI share one exchange clock.

The archived 2026-07-17 14:35 ET v2 artifact remains available for audit, but
the session player does not pin or reuse it. Dynamic frames use an independent
`policy=v3` cache keyed by lookback, projection-policy digest, and the current
session-source fingerprint. The service revalidates the artifact, projection
policy, and referenced Parquet hashes before every response. Browser responses
are revalidated rather than treated as immutable URLs because the lake can be
rewritten after compaction.

`automatic_ordering` is always `false`. Exact spread execution still requires
fresh, pinned IBKR leg quotes and the independent execution gates; this surface
cannot bypass them.

## Runtime layout

The projection worker writes only the dashboard payload to the dedicated
publish directory:

```text
/srv/data/spx-spark/data/published/spxw-surface/snapshot.json
/srv/data/spx-spark/data/published/spxw-surface/replays/2026-07-17T183500Z.json
/srv/data/spx-spark/data/published/spxw-surface/replay-catalog/session=YYYY-MM-DD/timeline-5m.json
/srv/data/spx-spark/data/published/spxw-surface/replay-cache/policy=v3/lookback=*/projection=*/source=*/*.json
/srv/data/spx-spark/data/published/spxw-surface/trend-cache/policy=v1/frame=5m/lookback=15s/projection=*/source=*/timeline=*/role=*/weighting=*/metric=*/*.json
/srv/data/spx-spark/data/published/spxw-surface/runtime/replay-api.sock
```

The low-priority host replay service reads Parquet, serializes generation with a
global advisory lock, and listens only on a Unix socket. The lock inode is
persistent; process exit releases `flock`, so a killed worker cannot leave a
permanent stale lock. Nginx mounts the runtime
directory read-only, proxies only `/api/v1/replay/`, and exposes no new TCP
listener. It also serves the exact live snapshot and archived v2 frame. The
weekday post-close timer warms the latest timeline manifest and the default
front-expiry, OI-weighted signed-Gamma trend artifact. Alternate selectors and
scenario-diagnostic frames remain on-demand. Sessions are hidden until a
two-hour post-close grace period has elapsed. That delay reduces compaction
races but is not a compactor-completion marker, so the API explicitly publishes
`data_finalization_proven=false` and never calls the source finalized. The
sidecar shares the code-server network namespace, so the browser entry remains
behind code-server authentication:

```text
https://code.zh3nyu.com/proxy/18082/live
https://code.zh3nyu.com/proxy/18082/replay
https://spx.zh3nyu.com/
https://spx.zh3nyu.com/replay
https://spx.zh3nyu.com/friday
```

The short hostname is a redirect-only loopback service; it exposes no JSON and
lands on the code-server-authenticated URL. `/friday` is a compatibility alias
for the 2026-07-17 session. The frontend refreshes every five seconds only in
Live mode. Replay indexes a session once, generates missing frames on demand,
and never changes the live publisher, strategy state, or execution state.
