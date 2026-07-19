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
  "canonicalReplaySha256",
  "normalizeReplaySessions",
  "normalizeReplayTimeline",
  "verifyReplayDigests",
  "legacyReplayFrameIndexFor",
]) {
  assert.equal(typeof hooks[name], "function", `missing test hook ${name}`);
}

const POLICY_SHA = "a".repeat(64);
const ARTIFACT_SHA = "b".repeat(64);

function isoZ(ms) {
  return new Date(ms).toISOString().replace(".000Z", "Z");
}

function isoOffset(ms) {
  return new Date(ms).toISOString().replace(".000Z", "+00:00");
}

function replayId(ms) {
  return isoZ(ms).replaceAll(":", "");
}

async function timelineFixture({ extended = true } = {}) {
  const legacyOpenMs = Date.parse("2026-07-17T13:30:00Z");
  const closeMs = Date.parse("2026-07-17T14:00:00Z");
  const stepMs = 5 * 60_000;
  const frames = [];
  for (let atMs = legacyOpenMs; atMs < closeMs; atMs += stepMs) {
    frames.push({
      id: replayId(atMs),
      at: isoZ(atMs),
      requested_as_of: isoZ(atMs),
      label_et: isoZ(atMs),
      status: "ready",
      projection_policy_sha256: POLICY_SHA,
      cached: true,
    });
  }
  const payload = {
    schema_version: 1,
    kind: "spxw_surface_replay_catalog",
    session_date: "2026-07-17",
    provider: "schwab",
    coordinate: "SPX",
    trading_class: "SPXW",
    frame_interval_minutes: 5,
    step_minutes: 5,
    timeline_policy_version: "spxw_surface_replay_timeline.event_driven.v2",
    availability_proven: false,
    availability_clock: "unavailable",
    point_in_time_confidence: "bounded_not_proven",
    frame_validation: "known_clock_validation_on_frame_request",
    only_close_grace_elapsed_sessions: true,
    session_close_grace_elapsed: true,
    session_close_grace_policy: "session_close_plus_2h_grace",
    session_close_grace_seconds: 7200,
    session_close_grace_elapsed_at: "2026-07-17T16:00:00Z",
    data_finalization_proven: false,
    projection_policy_sha256: POLICY_SHA,
    source_fingerprint: "c".repeat(64),
    open_at: isoZ(legacyOpenMs),
    close_at: isoZ(closeMs),
    frame_count: frames.length,
    frames,
    timeline_sha256: await hooks.canonicalReplaySha256(frames.map((frame) => frame.id)),
  };
  if (!extended) return payload;

  const surfaceOpenMs = Date.parse("2026-07-17T13:15:00Z");
  const surfaceFrames = [];
  for (let atMs = surfaceOpenMs + stepMs; atMs <= closeMs; atMs += stepMs) {
    const startMs = atMs - stepMs;
    const sessionKind = startMs < Date.parse("2026-07-17T13:25:00Z")
      ? "gth"
      : startMs < legacyOpenMs ? "closed_gap" : "rth";
    surfaceFrames.push({
      id: replayId(atMs),
      at: isoOffset(atMs),
      requested_as_of: isoOffset(atMs),
      label_et: isoZ(atMs),
      session_kind: sessionKind,
      status: sessionKind === "closed_gap" ? "scheduled_missing" : "unvalidated_playhead",
      projection_policy_sha256: POLICY_SHA,
    });
  }
  Object.assign(payload, {
    surface_open_at: isoOffset(surfaceOpenMs),
    surface_close_at: isoOffset(closeMs),
    surface_provider: "mixed",
    surface_frame_interval_minutes: 5,
    surface_frame_count: surfaceFrames.length,
    session_segments: [
      {
        kind: "gth",
        start_at: isoOffset(surfaceOpenMs),
        end_at: "2026-07-17T13:25:00+00:00",
        surface_provider: "ibkr",
        reference_method: "es_basis_inferred_spx",
        reference_provider: "schwab",
      },
      {
        kind: "closed_gap",
        start_at: "2026-07-17T13:25:00+00:00",
        end_at: "2026-07-17T13:30:00+00:00",
        surface_provider: null,
        reference_method: null,
        reference_provider: null,
      },
      {
        kind: "rth",
        start_at: "2026-07-17T13:30:00+00:00",
        end_at: isoOffset(closeMs),
        surface_provider: "schwab",
        reference_method: "direct_index_spx",
        reference_provider: "schwab",
      },
    ],
    surface_frames: surfaceFrames,
    surface_timeline_sha256: await hooks.canonicalReplaySha256(
      surfaceFrames.map((frame) => ({
        at: frame.at,
        session_kind: frame.session_kind,
        status: frame.status,
      })),
    ),
  });
  return payload;
}

(async () => {
  const extended = await timelineFixture();
  const normalized = await hooks.normalizeReplayTimeline(
    extended,
    "2026-07-17",
    POLICY_SHA,
  );
  assert.equal(normalized.surfaceTimelineExtended, true);
  assert.equal(normalized.frames.length, 6);
  assert.equal(normalized.surfaceFrames.length, 9);
  assert.equal(normalized.surfaceFrames[0].sessionKind, "gth");
  assert.equal(normalized.surfaceFrames[2].sessionKind, "closed_gap");
  assert.equal(normalized.surfaceFrames.at(-1).sessionKind, "rth");
  assert.equal(normalized.surfaceOpenAt.toISOString(), "2026-07-17T13:15:00.000Z");
  assert.equal(normalized.surfaceCloseAt.toISOString(), "2026-07-17T14:00:00.000Z");

  const legacy = await timelineFixture({ extended: false });
  const normalizedLegacy = await hooks.normalizeReplayTimeline(
    legacy,
    "2026-07-17",
    POLICY_SHA,
  );
  assert.equal(normalizedLegacy.surfaceTimelineExtended, false);
  assert.equal(normalizedLegacy.surfaceFrames.length, normalizedLegacy.frames.length);
  assert.equal(normalizedLegacy.surfaceTimelineSha256, normalizedLegacy.timelineSha256);

  const incomplete = await timelineFixture();
  delete incomplete.surface_frame_count;
  await assert.rejects(
    hooks.normalizeReplayTimeline(incomplete, "2026-07-17", POLICY_SHA),
    /incomplete_replay_surface_timeline_contract/,
  );

  const tamperedStatus = await timelineFixture();
  tamperedStatus.surface_frames[0].status = "ready";
  await assert.rejects(
    hooks.normalizeReplayTimeline(tamperedStatus, "2026-07-17", POLICY_SHA),
    /invalid_replay_surface_timeline_frame_contract/,
  );

  const artifactClaim = await timelineFixture();
  artifactClaim.surface_frames[0].artifact_sha256 = ARTIFACT_SHA;
  await assert.rejects(
    hooks.normalizeReplayTimeline(artifactClaim, "2026-07-17", POLICY_SHA),
    /invalid_replay_surface_timeline_frame_contract/,
  );

  const legacyFrames = normalized.frames;
  assert.equal(legacyFrames[0].artifactSha256, null);
  assert.equal(
    hooks.legacyReplayFrameIndexFor(legacyFrames, Date.parse("2026-07-17T13:25:00Z")),
    -1,
  );
  assert.equal(
    hooks.legacyReplayFrameIndexFor(legacyFrames, Date.parse("2026-07-17T13:37:00Z")),
    1,
  );
  const nearestUncached = legacyFrames.map((frame) => ({ ...frame }));
  nearestUncached[1].cached = false;
  nearestUncached[1].artifactSha256 = ARTIFACT_SHA;
  assert.equal(
    hooks.legacyReplayFrameIndexFor(
      nearestUncached,
      Date.parse("2026-07-17T13:37:00Z"),
    ),
    0,
  );
  const futureCached = legacyFrames.map((frame) => ({ ...frame, cached: false }));
  futureCached[2].cached = true;
  assert.equal(
    hooks.legacyReplayFrameIndexFor(futureCached, Date.parse("2026-07-17T13:37:00Z")),
    -1,
  );

  const projectionPolicy = { spot_step: 5, time_slices: [0, 5] };
  const projectionPolicySha256 = await hooks.canonicalReplaySha256(projectionPolicy);
  const independentlySignedFrame = {
    projection_policy: projectionPolicy,
    projection_policy_sha256: projectionPolicySha256,
    payload: { replay_id: "frame-without-timeline-artifact-hash" },
  };
  independentlySignedFrame.artifact_sha256 = await hooks.canonicalReplaySha256(
    independentlySignedFrame,
  );
  await hooks.verifyReplayDigests(independentlySignedFrame, {
    projectionPolicySha256,
  });
  const tamperedFrame = structuredClone(independentlySignedFrame);
  tamperedFrame.payload.replay_id = "tampered";
  await assert.rejects(
    hooks.verifyReplayDigests(tamperedFrame, { projectionPolicySha256 }),
    /replay_artifact_hash_mismatch/,
  );

  const catalog = {
    schema_version: 1,
    kind: "spxw_surface_replay_catalog",
    provider: "schwab",
    coordinate: "SPX",
    trading_class: "SPXW",
    frame_interval_minutes: 5,
    timeline_policy_version: "spxw_surface_replay_timeline.event_driven.v2",
    availability_proven: false,
    availability_clock: "unavailable",
    point_in_time_confidence: "bounded_not_proven",
    frame_validation: "known_clock_validation_on_frame_request",
    only_close_grace_elapsed_sessions: true,
    session_close_grace_policy: "session_close_plus_2h_grace",
    session_close_grace_seconds: 7200,
    data_finalization_proven: false,
    projection_policy_sha256: POLICY_SHA,
    sessions: [{
      date: "2026-07-17",
      label: "2026-07-17",
      frame_count: 6,
      surface_frame_count: 9,
      surface_timeline_status: "extended",
      status: "ready",
      close_at: "2026-07-17T14:00:00Z",
      session_close_grace_elapsed: true,
      session_close_grace_elapsed_at: "2026-07-17T16:00:00Z",
      data_finalization_proven: false,
      frame_interval_minutes: 5,
      projection_policy_sha256: POLICY_SHA,
    }],
  };
  const normalizedCatalog = hooks.normalizeReplaySessions(catalog);
  assert.equal(normalizedCatalog.sessions[0].frameCount, 6);
  assert.equal(normalizedCatalog.sessions[0].surfaceFrameCount, 9);
  assert.equal(normalizedCatalog.sessions[0].surfaceTimelineStatus, "extended");

  const v1Timeline = await timelineFixture();
  v1Timeline.timeline_policy_version = "spxw_surface_replay_timeline.event_driven.v1";
  await assert.rejects(
    hooks.normalizeReplayTimeline(v1Timeline, "2026-07-17", POLICY_SHA),
    /invalid_replay_timeline/,
  );
  const v1Catalog = structuredClone(catalog);
  v1Catalog.timeline_policy_version = "spxw_surface_replay_timeline.event_driven.v1";
  assert.throws(
    () => hooks.normalizeReplaySessions(v1Catalog),
    /invalid_replay_catalog_contract/,
  );
})().catch((error) => {
  process.nextTick(() => {
    throw error;
  });
});
