"use strict";

const assert = require("node:assert/strict");
const fs = require("node:fs");
const vm = require("node:vm");

const appPath = process.argv[2];
const payloadPath = process.argv[3];
if (!appPath || !payloadPath) throw new Error("missing app.js or payload path");

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
const payload = JSON.parse(fs.readFileSync(payloadPath, "utf8"));

function expectedSurface() {
  return {
    mode: "live",
    sessionDate: payload.session_date,
    role: payload.role,
    weighting: payload.weighting,
    bucketMinutes: payload.bucket_minutes,
    priceStep: payload.price_step,
  };
}

async function resign(value) {
  delete value.artifact_sha256;
  value.artifact_sha256 = await hooks.canonicalReplaySha256(value);
  return value;
}

(async () => {
  const surface = await hooks.normalizeSessionSurface(payload, expectedSurface());

  assert.equal(surface.schemaVersion, 2);
  assert.equal(surface.policyVersion, "spxw_session_surface.live.v2");
  assert.deepEqual(surface.sessionSegments.map((segment) => segment.kind), [
    "gth",
    "closed_gap",
    "rth",
  ]);
  assert.equal(surface.providers.gthSurface, payload.providers.gth_surface);
  assert.equal(surface.providers.gthReference, payload.providers.gth_reference);
  assert.equal(surface.providers.rthSurface, payload.providers.rth_surface);
  assert.equal(surface.providers.rthReference, payload.providers.rth_reference);
  const activeSegment = surface.sessionSegments.find((segment) =>
    segment.startMs <= surface.asOfMs && surface.asOfMs < segment.endMs);
  assert(activeSegment);
  if (surface.availability.current_spot_available) {
    assert.equal(surface.reference.method, activeSegment.referenceMethod);
    assert.equal(surface.reference.provider, activeSegment.referenceProvider);
    assert.equal(surface.reference.inferred, surface.reference.method !== "direct_index_spx");
    assert(Number.isFinite(surface.reference.price));
  } else {
    assert.equal(surface.reference.method, null);
    assert.equal(surface.reference.price, null);
    assert.equal(surface.reference.missingReason, "fresh_coordinate_reference_unavailable");
    assert(["degraded", "lease_expired", "unavailable"].includes(surface.liveStatus));
  }
  assert.equal(surface.capabilities.gth_available, payload.capabilities.gth_available);

  surface.surfaceColumns.forEach((column, index) => {
    if (column.sessionKind !== "closed_gap") return;
    assert.equal(column.kind, "missing");
    assert.equal(column.reason, "scheduled_closed_gap");
    for (const matrix of [surface.gamma, surface.grossGamma, surface.charm, surface.vanna]) {
      assert(matrix[index].every((value) => value === null));
    }
  });

  const historicalIndex = payload.surface_columns.findIndex((column) =>
    column.kind === "historical");
  if (historicalIndex >= 0) {
    const invalidHistoricalProvider = structuredClone(payload);
    invalidHistoricalProvider.surface_columns[historicalIndex].surface_provider = null;
    await assert.rejects(
      hooks.normalizeSessionSurface(
        await resign(invalidHistoricalProvider),
        expectedSurface(),
      ),
      /invalid_session_surface_column_segment_contract/,
    );
  }
})().catch((error) => {
  process.nextTick(() => { throw error; });
});
