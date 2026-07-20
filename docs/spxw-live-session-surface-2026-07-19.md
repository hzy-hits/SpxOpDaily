# SPXW Live Session Surface

## Scope

The Live cockpit uses the same full-session Canvas as Replay: `20:15 ET` on
the preceding calendar day through the trading day's RTH close. The Canvas is
split into GTH `[20:15, 09:25)`, a scheduled closed gap `[09:25, 09:30)`, and
RTH `[09:30, close]`; early-close dates use the actual exchange-calendar close.
It is a read-only market-structure view and does not change strategy,
notification, proposal, or order boundaries.

The source remains an exposure **proxy**:

- signed Gamma, Charm, Vanna, and the strike profile use call-positive / put-negative
  OI or volume weighting;
- participant position, open/close, signed flow, dealer sign, and actual market-maker
  inventory are unavailable;
- K lines are event-sampled SPX observations, not official consolidated OHLC.

Those limitations are part of the API contract and remain visible in the UI.

## Provider and reference contract

- GTH surface: the fresh IBKR SPXW chain selected by the publisher.
- GTH SPX coordinate: call/put-parity `chain_implied` SPX derived from fresh,
  coeval front-expiry pairs in that same observed chain. It is rendered dashed,
  carries the observed chain provider, and is always `degraded` because complete
  GTH contract-universe coverage is not proven.
- Closed gap: surface, spot, reference, strike state, and candles are Missing;
  no GTH lease is projected through the gap or relabeled as RTH.
- RTH surface: the publisher's validated SPXW pricing chain (normally Schwab,
  with policy-controlled failover).
- RTH SPX coordinate: a fresh direct `index:SPX` quote; no chain-implied value is
  presented as direct SPX.

Live GTH intentionally differs from Replay GTH. Replay uses a frozen
`es_basis_inferred_spx` reference with Schwab basis evidence; Live uses the
current chain-implied reference and has no ES-basis payload.

## Data path

```text
LatestState
  -> spx-spark-surface-dashboard (atomic, leased snapshot + self hash)
  -> spx-spark-surface-live (durable one-minute Live accumulator)
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

At each one-minute boundary the accumulator freezes the last candidate accepted
no later than the boundary and still valid at the boundary. It freezes the old
candidate before accepting an artifact observed after that boundary. Frozen files
are immutable and verified by canonical SHA-256. A restart reloads them; it does
not recompute them from newer data.

A boundary accepts only a frame from its own segment. In particular, a GTH
candidate whose TTL extends past 09:25 cannot populate the closed gap or become
the current RTH frame. GTH historical columns remain visibly `degraded`.

Starting mid-session never backfills earlier buckets. Those buckets become
explicit `missing` columns with null matrices. A disconnect preserves already
frozen history but cannot preserve a stale current surface or projection.

## Stable coordinates

The first validated segment-appropriate SPX reference fixes the session price
anchor: chain-implied SPX in GTH or direct `index:SPX` if the accumulator first
starts in RTH. The default grid is +/-100 SPX points at five-point spacing. Time
buckets cover the full GTH→gap→RTH session at one-minute spacing. The grid does
not recenter; an out-of-grid spot is reported as a quality condition.

GTH candles are event samples of the chain-implied reference and render dashed.
RTH candles are event samples of direct SPX and render solid. Neither is official
consolidated OHLC.

## Live API

```text
GET /api/v1/live/healthz
GET /api/v1/live/session-surface
    ?role=front|next
    &weighting=oi_weighted|volume_weighted
    &bucket_minutes=1
    &price_step=5
```

Live returns `schema_version=2` with
`policy_version=spxw_session_surface.live.v2`. Replay also uses schema 2, but its
independent policy is `spxw_session_surface.v5`; the browser validates the policy
by mode. Live adds accepted/lease clocks, availability flags, frozen-through
clock, dynamic provider declarations, and live provenance. Missing data is null
plus a Missing reason, never a fabricated zero.

For Live, each segment's `surface_provider` is the latest accepted provider for
that segment. A frozen historical column retains the provider of its signed
source frame. Therefore an in-session failover (for example IBKR to Schwab) may
leave historical RTH columns whose `surface_provider` differs from the current
RTH segment declaration; the browser still enforces the source session and
reference method while preserving that immutable provider provenance.

## State and recovery

Production state is retained under:

```text
/srv/data/spx-spark/data/published/spxw-surface/live/policy=live-v2/bucket=1m/session=YYYY-MM-DD/
/srv/data/spx-spark/data/published/spxw-surface/runtime/live/live-api.sock
```

The session manifest, one-minute boundary records, and runtime candidate are
written atomically with owner-only state permissions. The runtime candidate is
persisted so a crash across a boundary cannot cause a post-boundary frame to
rewrite the prior bucket.

State is policy-and-cadence-namespaced. The former five-minute v2 state remains
under `published/spxw-surface/live/policy=live-v2/session=YYYY-MM-DD/`, and v1
remains under `published/spxw-surface/live/session=YYYY-MM-DD/`. The one-minute
service never rewrites either namespace. Do not delete these as a normal restart
or rollback step; immutable records prove frozen history was not reconstructed
with later data.

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

The v2 policy namespace is a required deployment boundary. Running the v2 binary
against a v1 manifest fails closed with `live_persisted_contract_drift`; do not
work around that by deleting the old session directory.

The one-minute cadence also starts in its own empty namespace. It does not
reinterpret five-minute boundaries as one-minute evidence or backfill them from
later snapshots. Until the first fresh segment-appropriate snapshot arrives, the
surface endpoint returns 503 while `/healthz` remains available. The browser's
follow-now viewport begins at `accumulator_started_at` after initialization;
manual panning can still inspect earlier Missing time.

## Rollback

1. Restore the previous Git revision and Python environment.
2. Restore the previous nginx configuration and recreate only the nginx sidecar.
3. Stop and disable `spx-spark-surface-live.service` if that revision has no Live
   client.
4. Keep v1, five-minute v2, and
   `published/spxw-surface/live/policy=live-v2/bucket=1m/session=*` intact. The
   restored binary resumes its matching namespace after rollback.

Stopping Live does not require stopping the dashboard publisher, Replay service,
market-data collectors, or any strategy process.

## Acceptance boundary

On a weekend or exchange holiday the production health endpoint may report a
healthy `waiting/closed` service while the surface endpoint remains unavailable.
That is expected fail-closed behavior, not a synthetic session. A real GTH or RTH
snapshot must be observed after deployment before acceptance can claim a dynamic
surface; full end-to-end acceptance additionally checks the 09:25 closed gap and
the first RTH snapshot.

## Pre-deploy performance evidence

The Live contract now has 1,185 one-minute columns and 41 price rows on a regular
trading day; Replay remains five-minute. The browser requests one selector at a time and the API
timeout remains 15 seconds. Unit/contract coverage includes GTH acceptance,
evening-to-next-trading-date routing, gap nulling, GTH→RTH transition, weekend
retention, browser normalization of an actual signed live-v2 payload, and
cross-segment lease rejection. These checks do not replace production
publisher-to-browser p95/p99 measurement.
