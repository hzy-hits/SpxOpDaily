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
    classList: { add() {}, remove() {}, toggle() {} },
    addEventListener() {},
    setAttribute() {},
    replaceChildren() {},
    append() {},
    getContext() {
      return {
        clearRect() {},
        setTransform() {},
      };
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
assert.equal(typeof hooks.normalizeReplayTrend, "function");
assert.equal(typeof hooks.canonicalReplaySha256, "function");

(async () => {
const open = "2026-07-17T13:30:00Z";
const close = "2026-07-17T20:00:00Z";
const firstAt = "2026-07-17T13:35:00Z";
const secondAt = "2026-07-17T13:40:00Z";
const openMs = Date.parse(open);
const baseSourceAtMs = openMs + 1_000;
const projectionPolicy = { spot_step: 5, time_slices: [0, 5] };
const projectionPolicySha256 = await hooks.canonicalReplaySha256(projectionPolicy);
const timelineSha256 = "b".repeat(64);
const sourceFingerprint = "c".repeat(64);

function fixture() {
  return {
    schema_version: 1,
    kind: "spxw_intraday_gamma_replay",
    mode: "replay",
    policy_version: "spxw_surface_replay_trend.v1",
    frame_policy_version: "spxw_surface_replay.v3",
    timeline_policy_version: "spxw_surface_replay_timeline.event_driven.v2",
    session_date: "2026-07-17",
    provider: "schwab",
    coordinate: "SPX",
    trading_class: "SPXW",
    role: "front",
    weighting: "oi_weighted",
    metric: "signed_gamma",
    projection_policy: projectionPolicy,
    projection_policy_sha256: projectionPolicySha256,
    source_fingerprint: sourceFingerprint,
    timeline_sha256: timelineSha256,
    open_at: open,
    close_at: close,
    frame_interval_minutes: 5,
    lookback_seconds: 15,
    session_close_grace_elapsed: true,
    session_close_grace_elapsed_at: "2026-07-17T22:00:00Z",
    session_close_grace_policy: "session_close_plus_2h_grace",
    session_close_grace_seconds: 7200,
    availability_proven: false,
    availability_clock: "unavailable",
    point_in_time_confidence: "bounded_not_proven",
    data_finalization_proven: false,
    source: {
      dataset: "lake/quotes/schema=v1",
      source_files: ["lake/quotes/schema=v1/date=2026-07-17/provider=schwab/hour=13/quotes.parquet"],
      parquet_file_sha256: {
        "lake/quotes/schema=v1/date=2026-07-17/provider=schwab/hour=13/quotes.parquet": "a".repeat(64),
      },
      source_files_verified_unchanged_during_build: true,
      source_fingerprint: sourceFingerprint,
      cutoff_fields: ["received_at", "source_at", "quote_time", "trade_time", "last_update_at"],
      availability_clock_available: false,
      availability_clock: "unavailable",
      point_in_time_confidence: "bounded_not_proven",
      known_limitations: ["response_finished_at_unavailable", "received_at_is_cycle_started_at"],
      spx: {
        point_count: 2,
        raw_row_count: 2,
        duplicate_source_at_group_count: 0,
        base_source_at_ms: baseSourceAtMs,
        source_offset_ms: [0, 1_000],
        known_at_offset_ms: [100, 1_100],
        price: [7440.25, 7441.5],
        price_field: "mark",
        market_clock: "source_at",
        source_at_resolution: "milliseconds",
        known_at_rule: "max_recorded_clocks",
        known_at_is_availability_clock: false,
        dedupe_rule: "latest_known_at_then_received_at_then_source_file_position_per_source_at",
      },
    },
    surface: {
      cadence: "catalog_timeline_keyframes",
      frame_count: 2,
      shared_relative_spot_offsets: [-5, 0, 5],
      metric_unit: "proxy units",
      validity_rule: "min(next_keyframe_at, at_plus_frame_interval, expiry_close, session_close); unavailable_at_at",
      interpolation: "none",
      higher_frequency_candidate_upgrade: false,
      keyframes: [
        {
          at: firstAt,
          at_offset_ms: Date.parse(firstAt) - openMs,
          valid_until: secondAt,
          valid_until_offset_ms: Date.parse(secondAt) - openMs,
          expiry: "20260717",
          reference_spot: 7440,
          values: [-2, 0, 3],
          zero_ridge_spot: 7440,
          quality: "ready",
          warnings: [],
          frame_artifact_sha256: "d".repeat(64),
        },
        {
          at: secondAt,
          at_offset_ms: Date.parse(secondAt) - openMs,
          valid_until: secondAt,
          valid_until_offset_ms: Date.parse(secondAt) - openMs,
          expiry: "20260717",
          reference_spot: 7441,
          values: [null, null, null],
          zero_ridge_spot: null,
          quality: "unavailable",
          warnings: ["surface_unavailable"],
          frame_artifact_sha256: "e".repeat(64),
        },
      ],
      gaps: [
        {
          start_at: open,
          end_at: firstAt,
          start_offset_ms: 0,
          end_offset_ms: Date.parse(firstAt) - openMs,
          reason: "surface_keyframe_unavailable_before_first",
        },
        {
          start_at: secondAt,
          end_at: close,
          start_offset_ms: Date.parse(secondAt) - openMs,
          end_offset_ms: Date.parse(close) - openMs,
          reason: "surface_keyframe_unavailable",
        },
      ],
    },
  };
}

async function sign(payload) {
  delete payload.artifact_sha256;
  payload.artifact_sha256 = await hooks.canonicalReplaySha256(payload);
  return payload;
}

const expected = {
  sessionDate: "2026-07-17",
  role: "front",
  weighting: "oi_weighted",
  metric: "signed_gamma",
  projectionPolicySha256,
  timelineSha256,
  sourceFingerprint,
  frames: [
    { id: "2026-07-17T133500Z", at: new Date(firstAt) },
    { id: "2026-07-17T134000Z", at: new Date(secondAt) },
  ],
};

const normalized = await hooks.normalizeReplayTrend(await sign(fixture()), expected);
assert.equal(normalized.spx.prices.length, 2);
assert.equal(normalized.gamma.keyframes[0].status, "ready");
assert.equal(normalized.gamma.keyframes[1].status, "unavailable");
assert.equal(normalized.gamma.keyframes[1].validUntilMs, normalized.gamma.keyframes[1].atMs);
assert.equal(normalized.gamma.gaps[0].raw.start_at, open);

const lookaheadKnownAt = fixture();
lookaheadKnownAt.source.spx.known_at_offset_ms[1] = 999;
await assert.rejects(
  hooks.normalizeReplayTrend(await sign(lookaheadKnownAt), expected),
  /invalid_replay_trend_spx_point/,
);

const invalidUnavailableValidity = fixture();
invalidUnavailableValidity.surface.keyframes[1].valid_until = "2026-07-17T13:41:00Z";
invalidUnavailableValidity.surface.keyframes[1].valid_until_offset_ms =
  Date.parse("2026-07-17T13:41:00Z") - openMs;
await assert.rejects(
  hooks.normalizeReplayTrend(await sign(invalidUnavailableValidity), expected),
  /invalid_replay_trend_keyframe_contract/,
);
})().catch((error) => {
  process.nextTick(() => {
    throw error;
  });
});
