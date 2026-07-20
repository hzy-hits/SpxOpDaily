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

(async () => {
  const surface = await hooks.normalizeSessionSurface(payload, {
    mode: "live",
    sessionDate: payload.session_date,
    role: payload.role,
    weighting: payload.weighting,
    bucketMinutes: payload.bucket_minutes,
    priceStep: payload.price_step,
  });

  assert.equal(surface.schemaVersion, 2);
  assert.equal(surface.policyVersion, "spxw_session_surface.live.v2");
  assert.deepEqual(surface.sessionSegments.map((segment) => segment.kind), [
    "gth",
    "closed_gap",
    "rth",
  ]);
  assert.equal(surface.providers.gthSurface, "ibkr");
  assert.equal(surface.providers.gthReference, "ibkr");
  assert.equal(surface.reference.method, "chain_implied");
  assert.equal(surface.reference.inferred, true);
  assert.equal(surface.capabilities.gth_available, true);

  surface.surfaceColumns.forEach((column, index) => {
    if (column.sessionKind !== "closed_gap") return;
    assert.equal(column.kind, "missing");
    assert.equal(column.reason, "scheduled_closed_gap");
    for (const matrix of [surface.gamma, surface.grossGamma, surface.charm, surface.vanna]) {
      assert(matrix[index].every((value) => value === null));
    }
  });
})().catch((error) => {
  process.nextTick(() => { throw error; });
});
