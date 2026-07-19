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

`/replay` is a fixed-session trading cockpit rather than one static Friday
image. Gamma, the strike profile, and Charm are visible at the same time. The
two surfaces use absolute market time on X and absolute SPX price on Y; the
middle profile uses that identical SPX Y range. Event-sampled SPX OHLC candles
are overlaid on both surfaces. A shared price crosshair, current-SPX line, and
playhead keep the three panels aligned. The older spot-by-forward-time view is
kept only as a secondary diagnostic.

The browser animates the playhead at approximately 30 visual frames per second,
but it does not manufacture 30-fps observations. The timeline response contains
only frame clocks and hashes. When the playhead enters a validated frame, the
browser requests one cutoff-bound Session Surface at the latest frame at or
before the playhead. It never downloads the old full-session trend artifact or
future SPX values. Requests are single-flight and coalesced if a cold build is
slower than playback; the Canvas overlay continues at 30 fps while the static
surface remains cached.

Each response has a fixed 09:30--16:00 ET canvas by default. Completed columns
use only the causal frame valid at that bucket end and never get rebuilt from a
later chain. Future columns use only the current cutoff's chain with fixed IV,
OI/volume, and time decay, and are visibly labeled as model projections. Near
expiry or missing inputs remain null and hatched; they are never converted to
zero or interpolated. Event-sampled candles are cutoff-bound and stop at the
response `as_of`.

The price grid is anchored to the first causal SPX observation rather than the
current spot or a future high/low, so adjacent replay responses retain the same
SPX coordinates. The default grid is five points across +/-100 SPX points;
allowed alternatives are 2.5, 5, or 10 points. Time buckets may be 5, 10, or 15
minutes. The UI defaults to 5 x 5.

Signed color always maps zero to neutral, negative values to coral/red, and
positive values to blue. The symmetric domain is the absolute 98th percentile
available at that cutoff; raw unclipped values remain in tooltips. During
forward playback the scale only expands. Seeking backward resets it before
rendering, so a later cutoff cannot influence an earlier view. Zero ridge,
positive peak, and negative trough markers are drawn on the Gamma surface.

The player discovers trading dates from the normalized Schwab Parquet lake and
indexes validated event-driven chain cutoffs with their real seconds. It
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

The Session Surface contract is `schema_version=1`,
`kind=spxw_session_surface`, and `policy_version=spxw_session_surface.v1`. It
contains one shared time/price grid, nullable Gamma/Charm/Vanna/gross-Gamma
matrices, historical/projection/missing column semantics, cutoff-bound candles,
Gamma ridge/extrema, a Current-vs-First-Validated strike profile, missing
ranges, capabilities, and provenance. The browser verifies the artifact
SHA-256 and fails closed on a weakened PIT or capability contract.

The capability boundary is explicit:

- `proxy_position_available=true`;
- `participant_position_available=false`;
- `open_close_available=false`;
- `signed_flow_available=false`.

Consequently the middle panel says Current OI Exposure Proxy and First
Validated, never MM Position, Dealer Inventory, or Start of Day. Exact start of
day OI is absent from the current lake.

The archived 2026-07-17 14:35 ET v2 artifact remains available for audit, but
the session player does not pin or reuse it. Dynamic frames and Session Surfaces
use independent source/timeline/policy-keyed caches. The service revalidates
artifacts and referenced Parquet hashes before every response. Browser
responses are revalidated rather than treated as immutable URLs because
compaction may rewrite the lake.

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
/srv/data/spx-spark/data/published/spxw-surface/session-surface-cache/policy=v1/contract=2/frame=5m/bucket=*/step=*/lookback=15s/projection=*/source=*/timeline=*/role=*/weighting=*/*.json
/srv/data/spx-spark/data/published/spxw-surface/runtime/replay-api.sock
```

The low-priority host replay service reads Parquet, serializes generation with a
global advisory lock, and listens only on a Unix socket. The lock inode is
persistent; process exit releases `flock`, so a killed worker cannot leave a
permanent stale lock. Nginx mounts the runtime
directory read-only, proxies only `/api/v1/replay/`, and exposes no new TCP
listener. It also serves the exact live snapshot and archived v2 frame. The
weekday post-close timer warms the latest timeline manifest and every default
front-expiry, OI-weighted, 5-minute x 5-point Session Surface. Alternate roles,
weightings, and grids remain on-demand. Sessions are hidden until a
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

The live five-second publisher still emits the original moving scenario grid;
it cannot be losslessly inserted into a session-fixed price matrix. The cockpit
therefore fails closed with an explicit live placeholder until a durable live
session accumulator publishes this same contract. It does not interpolate the
moving live grid or pretend browser-local history is complete.

## Deployment and rollback

The frontend is bind-mounted from `site/spxw-surface/public`; Nginx
configuration changes require a container restart. Backend contract changes
require a restart of `spx-spark-surface-replay.service`.

```text
systemctl --user restart spx-spark-surface-replay.service
docker compose -f site/spxw-surface/compose.yaml restart spxw-surface
curl --unix-socket /srv/data/spx-spark/data/published/spxw-surface/runtime/replay-api.sock http://localhost/healthz
```

Rollback uses the previous Git commit: restore its tracked files, restart the
replay service and Nginx container, then verify `/healthz`. Session-surface
cache files are policy/source keyed and may remain on disk; the previous
frontend and service do not read them.
