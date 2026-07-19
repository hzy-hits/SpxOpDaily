# SPXW Live Session Surface

## Scope

The Live cockpit uses the same fixed `09:30–16:00 ET` Session Canvas as
Replay. It is a read-only market-structure view and does not change strategy,
notification, proposal, or order boundaries.

The source remains an exposure **proxy**:

- signed Gamma, Charm, Vanna, and the strike profile use call-positive / put-negative
  OI or volume weighting;
- participant position, open/close, signed flow, dealer sign, and actual market-maker
  inventory are unavailable;
- K lines are event-sampled SPX observations, not official consolidated OHLC.

Those limitations are part of the API contract and remain visible in the UI.

## Data path

```text
LatestState
  -> spx-spark-surface-dashboard (atomic, leased snapshot + self hash)
  -> spx-spark-surface-live (durable five-minute accumulator)
  -> runtime/live/live-api.sock (GET only)
  -> nginx /api/v1/live/session-surface
  -> Gamma / Strike Proxy / Charm Session Canvas
```

The accumulator is intentionally independent from the strategy and IBKR owner.
It reads only the isolated dashboard snapshot and exposes only a Unix socket.
It cannot submit an order or write strategy state.

## Clock and freeze contract

- `source_at` is the market-data clock carried by an individual selected input.
- `source_as_of` is the publisher selection/pricing cutoff. Kernels use this
  clock, never the later service acceptance clock.
- `accepted_at` is when the live accumulator validated and accepted the complete
  self-hashed dashboard artifact.
- root `as_of` is the accumulator's observation cutoff for the response.
- `server_time` is returned so the browser can enforce the lease without trusting
  its local wall clock.
- `valid_until` is exclusive. At or after that instant, current spot, current
  strike proxy, and all projection columns are cleared.

The enforced ordering is `input/source clocks <= source_as_of <= accepted_at <=
root as_of <= created_at <= server_time`, with `accepted_at < valid_until` for
dynamic values.

The browser anchors that signed server clock to `performance.now()`. It removes
the measured backend interval (`server_time - as_of`) from the full request
elapsed time and conservatively charges the remainder as transport,
serialization, and client work. A response that may have arrived at or after
the exclusive lease is therefore masked immediately instead of receiving a new
client-side grace period.

At each five-minute boundary the accumulator freezes the last candidate accepted
no later than the boundary and still valid at the boundary. It freezes the old
candidate before accepting an artifact observed after that boundary. Frozen files
are immutable and verified by canonical SHA-256. A restart reloads them; it does
not recompute them from newer data.

Starting mid-session never backfills earlier buckets. Those buckets become
explicit `missing` columns with null matrices. A disconnect preserves already
frozen history but cannot preserve a stale current surface or projection.

## Stable coordinates

The first validated direct `index:SPX` observation fixes the session price anchor.
The default grid is +/-100 SPX points at five-point spacing. Time buckets cover
the full market session at five-minute spacing. The grid does not recenter during
the session; an out-of-grid spot is reported as a quality condition instead.

Only direct SPX can establish the coordinate or candles. Chain-implied SPX, ES,
MES, and SPY are not accepted as substitutes for the SPX strike coordinate.

## Live API

```text
GET /api/v1/live/healthz
GET /api/v1/live/session-surface
    ?role=front|next
    &weighting=oi_weighted|volume_weighted
    &bucket_minutes=5
    &price_step=5
```

Live and Replay return the shared `spxw_session_surface.v1` matrix contract. Live
adds its accepted/lease clocks, availability flags, frozen-through clock, and
live provenance. Missing data is null plus a Missing reason, never a fabricated
zero.

## State and recovery

Production state is retained under:

```text
/srv/data/spx-spark/data/published/spxw-surface/live/session=YYYY-MM-DD/
/srv/data/spx-spark/data/published/spxw-surface/runtime/live/live-api.sock
```

The session manifest, five-minute boundary records, and runtime candidate are
written atomically with owner-only state permissions. The runtime candidate is
persisted so a crash across a boundary cannot cause a post-boundary frame to
rewrite the prior bucket.

Do not delete the live state as a normal restart or rollback step. It is the
evidence that frozen history was not reconstructed with later information.

## Deploy

```bash
cd /home/ubuntu/spx-spark
uv sync
systemctl --user restart spx-spark-surface-dashboard.service
scripts/install-spxw-surface-live-service.sh --now
docker compose -f site/spxw-surface/compose.yaml up -d
curl --unix-socket \
  /srv/data/spx-spark/data/published/spxw-surface/runtime/live/live-api.sock \
  http://localhost/healthz
```

The installer creates and validates the private state/runtime directories before
systemd applies `ProtectSystem=strict`. The service uses `AF_UNIX` only. This host
does not provide a working private network namespace for user services, so the
deployment does not claim `PrivateNetwork` isolation.

## Rollback

1. Restore the previous Git revision and Python environment.
2. Restore the previous nginx configuration and recreate only the nginx sidecar.
3. Stop and disable `spx-spark-surface-live.service` if that revision has no Live
   client.
4. Keep `published/spxw-surface/live/` intact for audit and a later forward-only
   resume.

Stopping Live does not require stopping the dashboard publisher, Replay service,
market-data collectors, or any strategy process.

## Acceptance boundary

On a weekend or exchange holiday the production health endpoint may report a
healthy `waiting/closed` service while the surface endpoint remains unavailable.
That is expected fail-closed behavior, not a synthetic session. A real RTH session
must still be observed after deployment before operational acceptance can claim
that production columns have accumulated end to end.

## Pre-deploy performance evidence

Using the 2026-07-17 production-sized contract sets with the default five-point
grid, the projection kernel probe measured about 2.865 seconds for the front
expiry (159 contracts x 67 future buckets) and 1.503 seconds for the next expiry
(80 x 67). The browser requests one selector at a time and the API timeout is 15
seconds. These are engineering probes, not a substitute for the first real RTH
publisher-to-browser p95/p99 measurement after deployment.
