"use strict";

const assert = require("node:assert/strict");
const fs = require("node:fs");
const vm = require("node:vm");

const appPath = process.argv[2];
if (!appPath) throw new Error("missing app.js path");

function stubElement() {
  return {
    textContent: "",
    className: "",
    hidden: false,
    disabled: false,
    value: "",
    open: false,
    width: 800,
    height: 540,
    clientWidth: 800,
    clientHeight: 540,
    style: {},
    dataset: {},
    classList: { add() {}, remove() {}, toggle() {} },
    addEventListener() {},
    setAttribute() {},
    replaceChildren() {},
    append() {},
    getContext() { return { clearRect() {}, setTransform() {} }; },
  };
}

globalThis.__SPX_SPARK_DISABLE_AUTO_START__ = true;
globalThis.__SPX_SPARK_TEST_HOOK__ = {};
globalThis.document = {
  body: stubElement(),
  hidden: false,
  querySelector: stubElement,
  createElement: stubElement,
  addEventListener() {},
};
globalThis.window = {
  location: { pathname: "/live", search: "", href: "http://localhost/live" },
  history: { pushState() {}, replaceState() {} },
  devicePixelRatio: 1,
  setTimeout() { return 0; },
  clearTimeout() {},
  requestAnimationFrame() { return 0; },
  cancelAnimationFrame() {},
  addEventListener() {},
  matchMedia() { return { matches: false }; },
};

vm.runInThisContext(fs.readFileSync(appPath, "utf8"), { filename: appPath });
const hooks = globalThis.__SPX_SPARK_TEST_HOOK__;

for (const name of [
  "normalizeSessionSurface",
  "cockpitDisplayTimeMs",
  "cockpitTimeWindow",
  "conservativeLiveServerNow",
  "historicalOnlyLiveSurface",
  "liveFrozenPrefixSignature",
  "liveServerTimeFromHeaders",
  "liveServerTimeMatchesArtifact",
  "liveSurfaceDisplayState",
  "liveSurfaceIdentity",
  "liveSurfaceTransitionIssue",
  "liveViewportStartAfterPan",
  "unavailableLiveMessage",
  "unavailableLiveReason",
]) {
  assert.equal(typeof hooks[name], "function", `missing test hook ${name}`);
}

const iso = (value) => new Date(value).toISOString();

function liveFixture() {
  const startMs = Date.parse("2026-07-17T13:30:00Z");
  const endMs = Date.parse("2026-07-17T14:00:00Z");
  const asOfMs = Date.parse("2026-07-17T13:42:00Z");
  const sourceAsOfMs = Date.parse("2026-07-17T13:41:58Z");
  const acceptedAtMs = Date.parse("2026-07-17T13:41:59Z");
  const createdAtMs = Date.parse("2026-07-17T13:42:00Z");
  const serverTimeMs = Date.parse("2026-07-17T13:42:01Z");
  const validUntilMs = Date.parse("2026-07-17T13:42:09Z");
  const frozenThroughMs = Date.parse("2026-07-17T13:40:00Z");
  const bucketMs = 5 * 60_000;
  const timeBuckets = [];
  for (let cursor = startMs; cursor < endMs; cursor += bucketMs) {
    timeBuckets.push({ start_at: iso(cursor), end_at: iso(cursor + bucketMs) });
  }
  const priceGrid = Array.from({ length: 21 }, (_, index) => 7400 + index * 10);
  const surfaceColumns = timeBuckets.map((bucket, index) => {
    const bucketEndMs = Date.parse(bucket.end_at);
    if (bucketEndMs <= frozenThroughMs) {
      return {
        kind: "historical",
        quality: "ready",
        source_at: iso(bucketEndMs - 10_000),
        known_at: iso(bucketEndMs - 9_000),
        accepted_at: iso(bucketEndMs - 8_000),
        source_frame_sha256: String(index + 1).padStart(64, "0"),
        valid_until: iso(bucketEndMs + bucketMs),
        reason: null,
      };
    }
    return {
      kind: "projection",
      quality: "ready",
      source_at: iso(sourceAsOfMs),
      known_at: iso(acceptedAtMs),
      accepted_at: iso(acceptedAtMs),
      source_frame_sha256: "a".repeat(64),
      valid_until: iso(validUntilMs),
      reason: null,
    };
  });
  const gamma = timeBuckets.map((_, timeIndex) =>
    priceGrid.map((__, priceIndex) => priceIndex - 10 + timeIndex));
  const charm = gamma.map((row, timeIndex) => row.map((value) => value * 0.2 - timeIndex));
  return {
    schema_version: 1,
    kind: "spxw_session_surface",
    policy_version: "spxw_session_surface.v1",
    mode: "live",
    status: "ready",
    live_status: "ready",
    automatic_ordering: false,
    session_date: "2026-07-17",
    session_start: iso(startMs),
    session_end: iso(endMs),
    source_as_of: iso(sourceAsOfMs),
    accepted_at: iso(acceptedAtMs),
    as_of: iso(asOfMs),
    created_at: iso(createdAtMs),
    server_time: iso(serverTimeMs),
    valid_until: iso(validUntilMs),
    history_frozen_through: iso(frozenThroughMs),
    accumulator_started_at: iso(startMs),
    expiry: "20260717",
    role: "front",
    weighting: "oi_weighted",
    coordinate: "SPX",
    provider: "mixed",
    trading_class: "SPXW",
    bucket_minutes: 5,
    price_step: 10,
    price_grid: priceGrid,
    time_buckets: timeBuckets,
    surface_columns: surfaceColumns,
    gamma_surface: gamma,
    gross_gamma_surface: gamma.map((row) => row.map(Math.abs)),
    charm_surface: charm,
    vanna_surface: charm.map((row) => row.map((value) => value * 0.4)),
    zero_ridges: timeBuckets.map(() => 7500),
    gamma_positive_peaks: timeBuckets.map((_, index) => ({ price: 7540, value: 20 + index })),
    gamma_negative_troughs: timeBuckets.map((_, index) => ({ price: 7460, value: -20 - index })),
    candles: [
      {
        start_at: "2026-07-17T13:30:00Z",
        end_at: "2026-07-17T13:35:00Z",
        open: 7498, high: 7504, low: 7496, close: 7501,
        sample_count: 8, complete: true,
        source_at: "2026-07-17T13:34:58Z",
        known_at: "2026-07-17T13:34:59Z",
      },
      {
        start_at: "2026-07-17T13:35:00Z",
        end_at: "2026-07-17T13:40:00Z",
        open: 7501, high: 7505, low: 7499, close: 7502,
        sample_count: 9, complete: true,
        source_at: "2026-07-17T13:39:58Z",
        known_at: "2026-07-17T13:39:59Z",
      },
      {
        start_at: "2026-07-17T13:40:00Z",
        end_at: "2026-07-17T13:45:00Z",
        open: 7502, high: 7506, low: 7500, close: 7504,
        sample_count: 4, complete: false,
        source_at: iso(sourceAsOfMs),
        known_at: iso(acceptedAtMs),
      },
    ],
    strike_profile: [{
      strike: 7500,
      current_proxy: 12.5,
      first_validated_proxy: 8.25,
      current_open_interest: 120,
      first_validated_open_interest: 100,
      quality: "ready",
    }],
    spot: 7504,
    spot_source_at: iso(sourceAsOfMs),
    spot_known_at: iso(acceptedAtMs),
    metric_units: {
      signed_gamma: "proxy_delta_dollars_per_1pct_underlier_move",
      gross_gamma: "gross_delta_dollars_per_1pct_underlier_move",
      charm: "proxy_1pct_notional_delta_change_per_calendar_minute",
      vanna: "proxy_1pct_notional_delta_change_per_1_vol_point",
    },
    capabilities: {
      proxy_position_available: true,
      participant_position_available: false,
      open_close_available: false,
      signed_flow_available: false,
      dealer_position_sign_available: false,
      strict_point_in_time_available: true,
      known_clock_no_lookahead: true,
      projection_is_model_scenario: true,
      historical_surface_is_model_proxy: true,
    },
    availability: {
      projection_available: true,
      current_strike_profile_available: true,
      current_spot_available: true,
      historical_surface_available: true,
    },
    provenance: {
      lookahead_rows_selected: 0,
      point_in_time_confidence: "observed_live",
      availability_clock_available: true,
      availability_clock: "accepted_at",
      per_leg_availability_clock_available: false,
      frozen_history_prefix_sha256: "f".repeat(64),
      source_as_of: iso(sourceAsOfMs),
    },
    missing_ranges: [],
  };
}

async function sign(payload) {
  delete payload.artifact_sha256;
  payload.artifact_sha256 = await hooks.canonicalReplaySha256(payload);
  return payload;
}

function expected() {
  return {
    mode: "live",
    role: "front",
    weighting: "oi_weighted",
    bucketMinutes: 5,
    priceStep: 10,
  };
}

function expirePayload(payload) {
  payload.status = "degraded";
  payload.live_status = "lease_expired";
  payload.availability.projection_available = false;
  payload.availability.current_strike_profile_available = false;
  payload.availability.current_spot_available = false;
  payload.spot = null;
  payload.spot_source_at = null;
  payload.spot_known_at = null;
  payload.strike_profile.forEach((row) => {
    row.current_proxy = null;
    row.current_open_interest = null;
  });
  const frozenMs = Date.parse(payload.history_frozen_through);
  payload.time_buckets.forEach((bucket, index) => {
    if (Date.parse(bucket.end_at) <= frozenMs) return;
    payload.surface_columns[index] = {
      kind: "missing", quality: "unavailable", source_at: null,
      known_at: null, accepted_at: null, source_frame_sha256: null,
      valid_until: null, reason: "live_lease_expired",
    };
    for (const name of [
      "gamma_surface", "gross_gamma_surface", "charm_surface", "vanna_surface",
    ]) payload[name][index] = payload[name][index].map(() => null);
    payload.zero_ridges[index] = null;
    payload.gamma_positive_peaks[index] = null;
    payload.gamma_negative_troughs[index] = null;
  });
  return payload;
}

(async () => {
  const rollingSurface = {
    mode: "live",
    sessionStartMs: Date.parse("2026-07-17T13:30:00Z"),
    sessionEndMs: Date.parse("2026-07-17T20:00:00Z"),
    asOfMs: Date.parse("2026-07-17T16:00:00Z"),
  };
  assert.deepEqual(
    hooks.cockpitTimeWindow(rollingSurface, {
      serverNowMs: Date.parse("2026-07-17T16:00:00Z"),
    }),
    {
      startMs: Date.parse("2026-07-17T14:30:00Z"),
      endMs: Date.parse("2026-07-17T16:30:00Z"),
      followsNow: true,
    },
  );
  assert.deepEqual(
    hooks.cockpitTimeWindow(rollingSurface, {
      serverNowMs: Date.parse("2026-07-17T13:35:00Z"),
    }),
    {
      startMs: Date.parse("2026-07-17T13:30:00Z"),
      endMs: Date.parse("2026-07-17T15:30:00Z"),
      followsNow: true,
    },
    "the full two-hour window stays inside the session near its open",
  );
  assert.deepEqual(
    hooks.cockpitTimeWindow({
      ...rollingSurface,
      accumulatorStartedAtMs: Date.parse("2026-07-17T15:50:00Z"),
    }, {
      serverNowMs: Date.parse("2026-07-17T16:00:00Z"),
    }),
    {
      startMs: Date.parse("2026-07-17T15:50:00Z"),
      endMs: Date.parse("2026-07-17T16:30:00Z"),
      followsNow: true,
    },
    "a newly started accumulator shows only collected history plus the live horizon",
  );
  assert.deepEqual(
    hooks.cockpitTimeWindow(rollingSurface, {
      manualStartMs: Date.parse("2026-07-17T19:30:00Z"),
    }),
    {
      startMs: Date.parse("2026-07-17T18:00:00Z"),
      endMs: Date.parse("2026-07-17T20:00:00Z"),
      followsNow: false,
    },
    "manual browsing is clamped to the session close",
  );
  assert.equal(
    hooks.liveViewportStartAfterPan(
      rollingSurface,
      Date.parse("2026-07-17T14:30:00Z"),
      -400,
      800,
    ),
    Date.parse("2026-07-17T15:30:00Z"),
    "dragging left browses one hour later in a two-hour window",
  );

  const normalized = await hooks.normalizeSessionSurface(await sign(liveFixture()), expected());
  assert.equal(normalized.mode, "live");
  assert.equal(normalized.provider, "mixed");
  assert.equal(normalized.provenance.point_in_time_confidence, "observed_live");
  assert.equal(hooks.cockpitDisplayTimeMs(normalized), normalized.asOfMs);
  assert.equal(
    hooks.liveSurfaceDisplayState(normalized, Date.parse("2026-07-17T13:42:08.999Z")),
    "fresh",
  );
  assert.equal(
    hooks.liveSurfaceDisplayState(normalized, Date.parse("2026-07-17T13:42:09Z")),
    "expired",
    "valid_until is exclusive",
  );

  const masked = hooks.historicalOnlyLiveSurface(normalized);
  assert.equal(masked.spot, null);
  assert.equal(masked.strikeProfile[0].currentProxy, null);
  assert.equal(masked.availability.projection_available, false);
  assert.equal(hooks.cockpitDisplayTimeMs(masked), masked.historyFrozenThroughMs);
  assert(masked.gamma.slice(2).every((row) => row.every((value) => value === null)));
  assert.equal(hooks.liveSurfaceDisplayState(masked, normalized.validUntilMs + 1), "historical_only");

  const terminal = await hooks.normalizeSessionSurface(
    await sign(expirePayload(liveFixture())),
    expected(),
  );
  assert.equal(hooks.liveSurfaceDisplayState(terminal, terminal.validUntilMs + 1), "historical_only");
  assert(terminal.gamma.slice(2).every((row) => row.every((value) => value === null)));

  const frozenMissingRaw = liveFixture();
  frozenMissingRaw.surface_columns[0] = {
    kind: "missing", quality: "unavailable", source_at: null, known_at: null,
    accepted_at: null, source_frame_sha256: null, valid_until: null, reason: "opening_gap",
  };
  for (const name of [
    "gamma_surface", "gross_gamma_surface", "charm_surface", "vanna_surface",
  ]) frozenMissingRaw[name][0] = frozenMissingRaw[name][0].map(() => null);
  frozenMissingRaw.zero_ridges[0] = null;
  frozenMissingRaw.gamma_positive_peaks[0] = null;
  frozenMissingRaw.gamma_negative_troughs[0] = null;
  const frozenMissing = await hooks.normalizeSessionSurface(
    await sign(frozenMissingRaw), expected(),
  );
  const maskedMissing = hooks.historicalOnlyLiveSurface(frozenMissing);
  assert.equal(maskedMissing.surfaceColumns[0].reason, "opening_gap");
  assert.equal(
    hooks.liveFrozenPrefixSignature(frozenMissing),
    hooks.liveFrozenPrefixSignature(maskedMissing),
  );

  const clockRegression = {
    ...normalized,
    acceptedAtMs: normalized.acceptedAtMs - 1,
  };
  assert.equal(hooks.liveSurfaceTransitionIssue(normalized, clockRegression), "live_surface_clock_regressed");
  const gridDrift = {
    ...normalized,
    priceGrid: normalized.priceGrid.map((value) => value + 5),
  };
  assert.equal(hooks.liveSurfaceTransitionIssue(normalized, gridDrift), "live_surface_grid_or_selector_drift");
  const prefixMutation = {
    ...normalized,
    gamma: normalized.gamma.map((row) => [...row]),
  };
  prefixMutation.gamma[0][0] += 1;
  assert.equal(hooks.liveSurfaceTransitionIssue(normalized, prefixMutation), "live_surface_frozen_prefix_changed");

  const invalidSourceClock = liveFixture();
  invalidSourceClock.source_as_of = "2026-07-17T13:42:00Z";
  invalidSourceClock.provenance.source_as_of = invalidSourceClock.source_as_of;
  await assert.rejects(
    hooks.normalizeSessionSurface(await sign(invalidSourceClock), expected()),
    /invalid_live_session_surface_clock_contract/,
  );

  const missingLeaseClock = liveFixture();
  delete missingLeaseClock.valid_until;
  await assert.rejects(
    hooks.normalizeSessionSurface(await sign(missingLeaseClock), expected()),
    /invalid_live_session_surface_root_contract/,
  );

  const dishonestSpot = liveFixture();
  dishonestSpot.availability.current_spot_available = false;
  await assert.rejects(
    hooks.normalizeSessionSurface(await sign(dishonestSpot), expected()),
    /invalid_live_session_surface_spot_availability/,
  );

  const liveAsReplay = liveFixture();
  await assert.rejects(
    hooks.normalizeSessionSurface(await sign(liveAsReplay), { ...expected(), mode: "replay" }),
    /invalid_session_surface_identity_contract/,
  );

  const exactHeaders = { get(name) {
    return name === "X-SPXW-Server-Time" ? "2026-07-17T13:42:01.250Z" : null;
  } };
  assert.equal(
    hooks.liveServerTimeFromHeaders(exactHeaders, 10, 5010),
    Date.parse("2026-07-17T13:42:01.250Z"),
    "kernel request elapsed must not be added to exact server time",
  );
  assert.equal(
    hooks.liveServerTimeMatchesArtifact(
      exactHeaders,
      Date.parse("2026-07-17T13:42:01.250Z"),
    ),
    true,
    "exact response clock must match the signed artifact clock",
  );
  assert.equal(
    hooks.liveServerTimeMatchesArtifact(
      exactHeaders,
      Date.parse("2026-07-17T13:42:01.251Z"),
    ),
    false,
    "an exact response clock mismatch must fail closed",
  );
  const dateHeaders = { get(name) {
    return name === "Date" ? "Fri, 17 Jul 2026 13:42:01 GMT" : null;
  } };
  assert.equal(
    hooks.liveServerTimeFromHeaders(dateHeaders, 10, 5010),
    Date.parse("2026-07-17T13:42:02Z"),
    "whole-second Date fallback is conservative by at most one second",
  );
  assert.equal(
    hooks.liveServerTimeMatchesArtifact(
      dateHeaders,
      Date.parse("2026-07-17T13:42:01.750Z"),
    ),
    true,
    "coarse HTTP Date fallback tolerates only its bounded precision loss",
  );
  assert.equal(
    hooks.liveServerTimeMatchesArtifact(
      dateHeaders,
      Date.parse("2026-07-17T13:42:04Z"),
    ),
    false,
    "coarse HTTP Date fallback cannot hide a material clock mismatch",
  );
  assert.equal(
    hooks.conservativeLiveServerNow(
      normalized,
      normalized.serverTimeMs,
      1_500,
    ),
    normalized.serverTimeMs + 500,
    "client removes measured backend work but conservatively charges remaining transport time",
  );
  const arrivedAfterLease = hooks.conservativeLiveServerNow(
    normalized,
    normalized.serverTimeMs,
    9_500,
  );
  assert.equal(
    hooks.liveSurfaceDisplayState(normalized, arrivedAfterLease),
    "expired",
    "a response that can only have arrived after the exclusive lease is masked immediately",
  );
  assert.equal(hooks.unavailableLiveReason("live_session_not_rth"), "market_closed");
  assert.match(
    hooks.unavailableLiveMessage("live_session_not_rth"),
    /first validated GTH or RTH snapshot/,
  );
})().catch((error) => {
  process.nextTick(() => { throw error; });
});
