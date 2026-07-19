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
    getContext() {
      return { clearRect() {}, setTransform() {} };
    },
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
  location: { pathname: "/replay", search: "", href: "http://localhost/replay" },
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
  "robustDomain",
  "expandOnlyDomain",
  "sessionTimeToX",
  "sessionXToTime",
  "sessionPriceToY",
  "sessionYToPrice",
  "missingRangeAppliesToPanel",
  "sessionSurfaceFrameIndexFor",
  "sessionSurfaceRequestDecision",
  "shouldResetCockpitDomains",
  "normalizeSessionMetricUnits",
  "cockpitCandleDisplayTime",
  "cockpitCandleAtTime",
  "clampSessionSurfacePlayback",
  "sessionGridPriceDomain",
]) {
  assert.equal(typeof hooks[name], "function", `missing test hook ${name}`);
}

function iso(ms) {
  return new Date(ms).toISOString();
}

function fixture({ bucketMinutes = 5, priceStep = 10 } = {}) {
  const startMs = Date.parse("2026-07-17T13:30:00Z");
  const endMs = Date.parse("2026-07-17T14:00:00Z");
  const asOfMs = Date.parse("2026-07-17T13:40:00Z");
  const bucketMs = bucketMinutes * 60_000;
  const timeBuckets = [];
  for (let cursor = startMs; cursor < endMs; cursor += bucketMs) {
    timeBuckets.push({ start_at: iso(cursor), end_at: iso(Math.min(cursor + bucketMs, endMs)) });
  }
  const priceCount = Math.round(200 / priceStep) + 1;
  const priceGrid = Array.from({ length: priceCount }, (_, index) => 7400 + index * priceStep);
  const surfaceColumns = timeBuckets.map((bucket) => {
    const historical = Date.parse(bucket.end_at) <= asOfMs;
    return {
      kind: historical ? "historical" : "projection",
      quality: "ready",
      source_at: historical ? bucket.end_at : iso(asOfMs),
      valid_until: iso(asOfMs + 5 * 60_000),
      reason: null,
    };
  });
  const gammaSurface = timeBuckets.map((_, timeIndex) =>
    priceGrid.map((__, priceIndex) => priceIndex - Math.floor(priceCount / 2) + timeIndex));
  const charmSurface = timeBuckets.map((_, timeIndex) =>
    priceGrid.map((__, priceIndex) => (priceIndex - Math.floor(priceCount / 2)) * 0.25 - timeIndex));
  const grossGammaSurface = gammaSurface.map((row) => row.map(Math.abs));
  const vannaSurface = charmSurface.map((row) => row.map((value) => value * 0.4));
  return {
    schema_version: 1,
    kind: "spxw_session_surface",
    policy_version: "spxw_session_surface.v1",
    mode: "replay",
    session_date: "2026-07-17",
    session_start: iso(startMs),
    session_end: iso(endMs),
    as_of: iso(asOfMs),
    expiry: "20260717",
    role: "front",
    weighting: "oi_weighted",
    coordinate: "SPX",
    provider: "schwab",
    trading_class: "SPXW",
    bucket_minutes: bucketMinutes,
    price_step: priceStep,
    price_grid: priceGrid,
    time_buckets: timeBuckets,
    surface_columns: surfaceColumns,
    gamma_surface: gammaSurface,
    gross_gamma_surface: grossGammaSurface,
    charm_surface: charmSurface,
    vanna_surface: vannaSurface,
    zero_ridges: timeBuckets.map(() => 7500),
    gamma_positive_peaks: timeBuckets.map((_, index) => ({ price: 7540, value: 20 + index })),
    gamma_negative_troughs: timeBuckets.map((_, index) => ({ price: 7460, value: -20 - index })),
    candles: [{
      start_at: "2026-07-17T13:35:00Z",
      end_at: "2026-07-17T13:40:00Z",
      open: 7498,
      high: 7504,
      low: 7496,
      close: 7502,
      sample_count: 8,
      complete: true,
      source_at: "2026-07-17T13:39:58Z",
      known_at: "2026-07-17T13:39:59Z",
    }],
    strike_profile: [{
      strike: 7500,
      current_proxy: 12.5,
      first_validated_proxy: 8.25,
      current_open_interest: 120,
      first_validated_open_interest: 100,
      quality: "ready",
    }],
    spot: 7502,
    spot_source_at: "2026-07-17T13:39:58Z",
    spot_known_at: "2026-07-17T13:39:59Z",
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
      strict_point_in_time_available: false,
      known_clock_no_lookahead: true,
      projection_is_model_scenario: true,
      historical_surface_is_model_proxy: true,
    },
    provenance: {
      lookahead_rows_selected: 0,
      point_in_time_confidence: "bounded_not_proven",
      availability_clock_available: false,
      availability_clock: "unavailable",
    },
    missing_ranges: [{
      start_at: "2026-07-17T13:50:00Z",
      end_at: "2026-07-17T13:55:00Z",
      reason: "candle_gap",
      component: "spx_candles",
    }],
  };
}

async function sign(payload) {
  delete payload.artifact_sha256;
  payload.artifact_sha256 = await hooks.canonicalReplaySha256(payload);
  return payload;
}

(async () => {
  const expected = {
    at: new Date("2026-07-17T13:40:00Z"),
    sessionDate: "2026-07-17",
    role: "front",
    weighting: "oi_weighted",
    bucketMinutes: 5,
    priceStep: 10,
  };
  const normalized = await hooks.normalizeSessionSurface(await sign(fixture()), expected);
  assert.equal(normalized.priceGrid.length, 21);
  assert.equal(normalized.gammaPositivePeaks[0].value, 20);
  assert.equal(normalized.gammaNegativeTroughs[0].price, 7460);
  assert.deepEqual(normalized.missingRanges[0].components, ["spx_candles"]);
  assert.equal(
    normalized.metricUnits.charm,
    "proxy_1pct_notional_delta_change_per_calendar_minute",
  );
  assert.deepEqual(hooks.sessionGridPriceDomain(normalized), { min: 7390, max: 7610 });

  const invalidUnits = fixture();
  invalidUnits.metric_units.charm = "mystery_unit";
  await assert.rejects(
    hooks.normalizeSessionSurface(await sign(invalidUnits), expected),
    /invalid_session_surface_metric_units/,
  );

  const tampered = await sign(fixture());
  tampered.spot = 7600;
  await assert.rejects(hooks.normalizeSessionSurface(tampered, expected), /artifact_hash_mismatch/);

  const futureCandle = fixture();
  futureCandle.candles[0].known_at = "2026-07-17T13:40:01Z";
  await assert.rejects(
    hooks.normalizeSessionSurface(await sign(futureCandle), expected),
    /invalid_session_surface_candle_contract/,
  );

  const pastProjection = fixture();
  pastProjection.surface_columns[0].kind = "projection";
  await assert.rejects(
    hooks.normalizeSessionSurface(await sign(pastProjection), expected),
    /session_surface_projection_before_cutoff/,
  );

  const lateHistoricalSource = fixture();
  lateHistoricalSource.surface_columns[0].source_at = "2026-07-17T13:39:00Z";
  await assert.rejects(
    hooks.normalizeSessionSurface(await sign(lateHistoricalSource), expected),
    /session_surface_historical_source_after_bucket/,
  );

  const expiredHistoricalTtl = fixture();
  expiredHistoricalTtl.surface_columns[0].valid_until = expiredHistoricalTtl.time_buckets[0].end_at;
  await assert.rejects(
    hooks.normalizeSessionSurface(await sign(expiredHistoricalTtl), expected),
    /session_surface_historical_ttl_expired_at_bucket/,
  );

  const expiredProjectionTtl = fixture();
  expiredProjectionTtl.surface_columns[2].valid_until = expiredProjectionTtl.as_of;
  await assert.rejects(
    hooks.normalizeSessionSurface(await sign(expiredProjectionTtl), expected),
    /session_surface_projection_ttl_expired_at_cutoff/,
  );

  const misalignedCandleSource = fixture();
  misalignedCandleSource.candles[0].source_at = "2026-07-17T13:34:59Z";
  await assert.rejects(
    hooks.normalizeSessionSurface(await sign(misalignedCandleSource), expected),
    /invalid_session_surface_candle_contract/,
  );

  const populatedMissing = fixture();
  populatedMissing.surface_columns[0].kind = "missing";
  await assert.rejects(
    hooks.normalizeSessionSurface(await sign(populatedMissing), expected),
    /session_surface_missing_column_has_values/,
  );

  const unsafeCapabilities = fixture();
  unsafeCapabilities.capabilities.open_close_available = true;
  await assert.rejects(
    hooks.normalizeSessionSurface(await sign(unsafeCapabilities), expected),
    /invalid_session_surface_capabilities_contract/,
  );

  const tenMinute = fixture({ bucketMinutes: 10, priceStep: 2.5 });
  const normalizedTenMinute = await hooks.normalizeSessionSurface(await sign(tenMinute), {
    ...expected,
    bucketMinutes: 10,
    priceStep: 2.5,
  });
  assert.equal(normalizedTenMinute.timeBuckets.length, 3);
  assert.equal(normalizedTenMinute.priceGrid.length, 81);

  const robust = hooks.robustDomain([...Array.from({ length: 100 }, (_, index) => index + 1), 10_000]);
  assert.equal(robust.maxAbs, 99);
  assert.notEqual(robust.maxAbs, 10_000);

  const layout = { plotLeft: 20, plotWidth: 400, plotTop: 10, plotHeight: 300 };
  const time = Date.parse("2026-07-17T13:45:00Z");
  const x = hooks.sessionTimeToX(layout, Date.parse("2026-07-17T13:30:00Z"), Date.parse("2026-07-17T14:00:00Z"), time);
  assert.equal(hooks.sessionXToTime(layout, Date.parse("2026-07-17T13:30:00Z"), Date.parse("2026-07-17T14:00:00Z"), x), time);
  const y = hooks.sessionPriceToY(layout, { min: 7400, max: 7600 }, 7510);
  assert.equal(hooks.sessionYToPrice(layout, { min: 7400, max: 7600 }, y), 7510);

  assert.equal(hooks.missingRangeAppliesToPanel({ components: ["spx_candles"] }, "gamma"), false);
  assert.equal(hooks.missingRangeAppliesToPanel({ components: ["charm_surface"] }, "gamma"), false);
  assert.equal(hooks.missingRangeAppliesToPanel({ components: ["charm_surface"] }, "charm"), true);
  assert.equal(hooks.missingRangeAppliesToPanel({ components: ["surface"] }, "gamma"), true);

  assert.equal(hooks.sessionSurfaceRequestDecision({
    inFlightKey: "old",
    targetKey: "latest",
    renderedKey: "",
  }), "queue");
  assert.equal(hooks.sessionSurfaceRequestDecision({
    inFlightKey: "old",
    targetKey: "latest",
    renderedKey: "",
    interrupt: true,
  }), "interrupt");
  const frameClocks = [{ atMs: 1000 }, { atMs: 2000 }];
  assert.equal(hooks.sessionSurfaceFrameIndexFor(frameClocks, 999), -1);
  assert.equal(hooks.sessionSurfaceFrameIndexFor(frameClocks, 1000), 0);
  assert.equal(hooks.sessionSurfaceFrameIndexFor(frameClocks, 1999), 0);
  assert.equal(hooks.sessionSurfaceFrameIndexFor(frameClocks, 2000), 1);
  assert.equal(hooks.shouldResetCockpitDomains(Date.parse("2026-07-17T15:30:00Z"), Date.parse("2026-07-17T10:27:00Z")), true);
  assert.equal(hooks.shouldResetCockpitDomains(Date.parse("2026-07-17T10:27:00Z"), Date.parse("2026-07-17T15:30:00Z")), false);

  const partial = { startMs: 100, endMs: 200, complete: false };
  assert.equal(hooks.cockpitCandleDisplayTime(partial, 120), 120);
  assert.equal(hooks.cockpitCandleAtTime([partial], 120, 120), partial);
  assert.equal(hooks.cockpitCandleAtTime([partial], 121, 120), null);
  assert.equal(hooks.cockpitCandleAtTime([partial], 150, 120), null);

  const playbackFrames = [{ atMs: 100 }, { atMs: 200 }, { atMs: 300 }];
  assert.equal(hooks.clampSessionSurfacePlayback(playbackFrames, 150, 0), 150);
  assert.equal(hooks.clampSessionSurfacePlayback(playbackFrames, 250, 0), 200);
  assert.equal(hooks.clampSessionSurfacePlayback(playbackFrames, 250, 1), 250);
  assert.equal(hooks.clampSessionSurfacePlayback(playbackFrames, 350, 2), 350);
})().catch((error) => {
  process.nextTick(() => {
    throw error;
  });
});
