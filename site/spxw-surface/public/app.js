"use strict";

const SNAPSHOT_URL = "api/v1/snapshot";
const LIVE_SESSION_SURFACE_URL = "api/v1/live/session-surface";
const REPLAY_SESSIONS_URL = "api/v1/replay/sessions";
const SESSION_SURFACE_SCHEMA_VERSIONS = new Set([1, 2]);
const SESSION_SURFACE_KIND = "spxw_session_surface";
const SESSION_SURFACE_POLICY_VERSIONS = new Map([
  ["replay:1", "spxw_session_surface.v1"],
  ["replay:2", "spxw_session_surface.v5"],
  ["live:1", "spxw_session_surface.v1"],
  ["live:2", "spxw_session_surface.live.v2"],
]);
const LIVE_SEGMENT_REFERENCE_METHODS = {
  gth: "chain_implied",
  rth: "direct_index_spx",
};
const SESSION_SEGMENT_CONTRACT = {
  gth: {
    surfaceProvider: "ibkr",
    referenceProvider: "schwab",
    referenceMethod: "es_basis_inferred_spx",
  },
  closed_gap: { surfaceProvider: null, referenceProvider: null, referenceMethod: null },
  rth: {
    surfaceProvider: "schwab",
    referenceProvider: "schwab",
    referenceMethod: "direct_index_spx",
  },
};
const SESSION_SURFACE_BUCKET_MINUTES = 5;
const SESSION_SURFACE_PRICE_STEP = 5;
const SESSION_SURFACE_PRICE_EXTENT_POINTS = 100;
const SESSION_SURFACE_BUCKET_MINUTES_ALLOWED = new Set([5, 10, 15]);
const SESSION_SURFACE_PRICE_STEPS_ALLOWED = new Set([2.5, 5, 10]);
const REPLAY_TREND_KIND = "spxw_intraday_gamma_replay";
const REPLAY_TREND_POLICY_VERSION = "spxw_surface_replay_trend.v1";
const REPLAY_POLICY_VERSION = "spxw_surface_replay.v3";
const REPLAY_CATALOG_KIND = "spxw_surface_replay_catalog";
const REPLAY_TIMELINE_POLICY_VERSION = "spxw_surface_replay_timeline.event_driven.v2";
const REPLAY_FRAME_VALIDATION = "known_clock_validation_on_frame_request";
const REPLAY_CLOSE_GRACE_POLICY = "session_close_plus_2h_grace";
const REPLAY_CLOSE_GRACE_SECONDS = 2 * 60 * 60;
const REPLAY_TREND_VALIDITY_RULE =
  "min(next_keyframe_at, at_plus_frame_interval, expiry_close, session_close); unavailable_at_at";
const REPLAY_TIMELINE_STEP_MINUTES = 5;
const REPLAY_VISUAL_FPS = 30;
const REPLAY_VISUAL_FRAME_MS = 1_000 / REPLAY_VISUAL_FPS;
const REPLAY_MARKET_TIME_RATE = 150;
const SESSION_SURFACE_CACHE_LIMIT = 24;
const LIVE_VIEW_HISTORY_MS = 90 * 60_000;
const LIVE_VIEW_HORIZON_MS = 30 * 60_000;
const LIVE_VIEW_SPAN_MS = LIVE_VIEW_HISTORY_MS + LIVE_VIEW_HORIZON_MS;
const MARKET_TIME_ZONE = "America/New_York";
const POLL_INTERVAL_MS = 5_000;
const REQUEST_TIMEOUT_MS = 4_500;
const LIVE_SESSION_REQUEST_TIMEOUT_MS = 15_000;
const REPLAY_REQUEST_TIMEOUT_MS = 60_000;
const SESSION_SURFACE_RETRY_DELAYS_MS = [1_000, 3_000, 10_000];
const LIVE_STATUS_VALUES = new Set([
  "initializing",
  "ready",
  "degraded",
  "lease_expired",
  "closed",
  "unavailable",
]);
const SVG_NS = "http://www.w3.org/2000/svg";

const METRICS = {
  signed_gamma: {
    label: "Signed gamma",
    aliases: ["signed_gamma", "net_gex"],
    signed: true,
  },
  gross_gamma: {
    label: "Gross gamma",
    aliases: ["gross_gamma", "abs_gex"],
    signed: false,
  },
  charm: {
    label: "Charm",
    aliases: ["charm", "cex_proxy"],
    signed: true,
  },
  vanna: {
    label: "Vanna",
    aliases: ["vanna", "vex_proxy"],
    signed: true,
  },
};

const WEIGHTINGS = {
  oi_weighted: "OI · Signed proxy",
  volume_weighted: "Volume · Activity proxy",
};

const WEIGHTING_DESCRIPTIONS = {
  oi_weighted: "OI 结构代理；0 有效，缺失剔除",
  volume_weighted: "累计成交量活动代理（非买卖方向）；0 有效，缺失剔除",
};

const UNIT_LABELS = {
  proxy_delta_dollars_per_1pct_underlier_move: "proxy Δ$ / 1% SPX move",
  gross_delta_dollars_per_1pct_underlier_move: "gross Δ$ / 1% SPX move",
  proxy_1pct_notional_delta_change_per_calendar_minute: "proxy 1% notional Δ / calendar minute",
  proxy_1pct_notional_delta_change_per_1_vol_point: "proxy 1% notional Δ / 1 vol point",
};

const STATUS_LABELS = {
  ready: "Ready",
  degraded: "Degraded",
  unavailable: "Unavailable",
  unknown: "Unknown",
};

const REASON_LABELS = {
  unpaired_strike: "部分 strike 缺少 Call/Put 配对（unpaired_strike）",
  underlier_unavailable: "标的行情不可用（underlier_unavailable）",
  bounded_point_in_time_not_proven: "有界 PIT：缺少真实 availability clock，不能严格证明无前视",
  schwab_availability_clock_unavailable: "Schwab availability clock 不可用",
  availability_clock_unavailable: "availability clock 不可用",
  response_finished_at_unavailable: "Schwab response_finished_at 不可用",
  received_at_is_cycle_started_at: "received_at 实为采集 cycle_started_at，并非响应完成时间",
};

const COLORS = {
  ink: "#17202a",
  muted: "#687482",
  faint: "#939da8",
  border: "#dce1e6",
  neutral: [242, 241, 237],
  positive: [47, 111, 173],
  positiveDark: "#174f84",
  negative: [217, 100, 89],
  negativeDark: "#a93d35",
  missing: "#f8f9fa",
};

const dom = {
  pageLede: document.querySelector("#page-lede"),
  replayBanner: document.querySelector("#replay-banner"),
  replayBannerLabel: document.querySelector("#replay-banner-label"),
  replayBannerAsOf: document.querySelector("#replay-banner-as-of"),
  replayConsole: document.querySelector("#replay-console"),
  replaySessionFilter: document.querySelector("#replay-session-filter"),
  replaySessionMeta: document.querySelector("#replay-session-meta"),
  replayPrevious: document.querySelector("#replay-previous"),
  replayPlay: document.querySelector("#replay-play"),
  replayNext: document.querySelector("#replay-next"),
  replaySpeed: document.querySelector("#replay-speed"),
  replayTimeline: document.querySelector("#replay-timeline"),
  replayFrameTime: document.querySelector("#replay-frame-time"),
  replayFramePosition: document.querySelector("#replay-frame-position"),
  replayTimelineStart: document.querySelector("#replay-timeline-start"),
  replayTimelineEnd: document.querySelector("#replay-timeline-end"),
  sessionCockpit: document.querySelector("#session-cockpit"),
  cockpitTimeline: document.querySelector("#cockpit-timeline"),
  liveViewportControls: document.querySelector("#live-viewport-controls"),
  liveViewportLabel: document.querySelector("#live-viewport-label"),
  liveViewportReset: document.querySelector("#live-viewport-reset"),
  cockpitLoading: document.querySelector("#cockpit-loading"),
  cockpitTooltip: document.querySelector("#cockpit-tooltip"),
  cockpitGammaStage: document.querySelector("#cockpit-gamma-stage"),
  cockpitGammaBase: document.querySelector("#cockpit-gamma-base"),
  cockpitGammaOverlay: document.querySelector("#cockpit-gamma-overlay"),
  cockpitGammaEmpty: document.querySelector("#cockpit-gamma-empty"),
  cockpitGammaValue: document.querySelector("#cockpit-gamma-value"),
  cockpitGammaThreshold: document.querySelector("#cockpit-gamma-threshold"),
  cockpitGammaDomain: document.querySelector("#cockpit-gamma-domain"),
  cockpitStrikeStage: document.querySelector("#cockpit-strike-stage"),
  cockpitStrikeBase: document.querySelector("#cockpit-strike-base"),
  cockpitStrikeOverlay: document.querySelector("#cockpit-strike-overlay"),
  cockpitStrikeEmpty: document.querySelector("#cockpit-strike-empty"),
  cockpitStrikeValue: document.querySelector("#cockpit-strike-value"),
  cockpitStrikeTitle: document.querySelector("#cockpit-strike-title"),
  cockpitStrikeReadoutLabel: document.querySelector("#cockpit-strike-readout-label"),
  cockpitStrikeModeOi: document.querySelector("#cockpit-strike-mode-oi"),
  cockpitStrikeModeGamma: document.querySelector("#cockpit-strike-mode-gamma"),
  cockpitStrikeCurrentLegend: document.querySelector("#cockpit-strike-current-legend"),
  cockpitStrikeBaselineLegend: document.querySelector("#cockpit-strike-baseline-legend"),
  cockpitStrikeColorLegend: document.querySelector("#cockpit-strike-color-legend"),
  cockpitStrikeDomain: document.querySelector("#cockpit-strike-domain"),
  cockpitCharmStage: document.querySelector("#cockpit-charm-stage"),
  cockpitCharmBase: document.querySelector("#cockpit-charm-base"),
  cockpitCharmOverlay: document.querySelector("#cockpit-charm-overlay"),
  cockpitCharmEmpty: document.querySelector("#cockpit-charm-empty"),
  cockpitCharmValue: document.querySelector("#cockpit-charm-value"),
  cockpitCharmThreshold: document.querySelector("#cockpit-charm-threshold"),
  cockpitCharmDomain: document.querySelector("#cockpit-charm-domain"),
  cockpitAuditToggle: document.querySelector("#cockpit-audit-toggle"),
  cockpitAuditClose: document.querySelector("#cockpit-audit-close"),
  cockpitAuditDrawer: document.querySelector("#cockpit-audit-drawer"),
  cockpitAuditScrim: document.querySelector("#cockpit-audit-scrim"),
  cockpitAuditAsOf: document.querySelector("#cockpit-audit-as-of"),
  cockpitAuditContract: document.querySelector("#cockpit-audit-contract"),
  cockpitAuditStats: document.querySelector("#cockpit-audit-stats"),
  cockpitAuditMissing: document.querySelector("#cockpit-audit-missing"),
  cockpitAuditCapabilities: document.querySelector("#cockpit-audit-capabilities"),
  cockpitAuditReference: document.querySelector("#cockpit-audit-reference"),
  cockpitAuditStrike: document.querySelector("#cockpit-audit-strike"),
  cockpitAuditProvenance: document.querySelector("#cockpit-audit-provenance"),
  cockpitAuditFrozen: document.querySelector("#cockpit-audit-frozen"),
  cockpitAuditPit: document.querySelector("#cockpit-audit-pit"),
  cockpitAuditModel: document.querySelector("#cockpit-audit-model"),
  legacyDiagnosticEntry: document.querySelector("#legacy-diagnostic-entry"),
  legacyDiagnosticStatus: document.querySelector("#legacy-diagnostic-status"),
  legacyDiagnosticOpen: document.querySelector("#legacy-diagnostic-open"),
  providerChip: document.querySelector("#provider-chip"),
  referenceChip: document.querySelector("#reference-chip"),
  referenceClock: document.querySelector("#reference-clock"),
  cockpitGammaReferenceIcon: document.querySelector("#cockpit-gamma-reference-icon"),
  cockpitGammaReferenceLegend: document.querySelector("#cockpit-gamma-reference-legend"),
  cockpitCharmReferenceIcon: document.querySelector("#cockpit-charm-reference-icon"),
  cockpitCharmReferenceLegend: document.querySelector("#cockpit-charm-reference-legend"),
  trendPanel: document.querySelector("#trend-panel"),
  trendTitle: document.querySelector("#trend-title"),
  trendSubtitle: document.querySelector("#trend-subtitle"),
  trendSpot: document.querySelector("#trend-spot"),
  trendRegime: document.querySelector("#trend-regime"),
  trendQuality: document.querySelector("#trend-quality"),
  trendStage: document.querySelector("#trend-stage"),
  trendBase: document.querySelector("#trend-base"),
  trendOverlay: document.querySelector("#trend-overlay"),
  trendTooltip: document.querySelector("#trend-tooltip"),
  trendEmpty: document.querySelector("#trend-empty"),
  trendCadence: document.querySelector("#trend-cadence"),
  trendAccessibleSummary: document.querySelector("#trend-accessible-summary"),
  scenarioDiagnostic: document.querySelector("#scenario-diagnostic"),
  statusPill: document.querySelector("#status-pill"),
  refreshState: document.querySelector("#refresh-state"),
  summaryStatus: document.querySelector("#summary-status"),
  summaryReasons: document.querySelector("#summary-reasons"),
  summaryFreshness: document.querySelector("#summary-freshness"),
  summaryAsOf: document.querySelector("#summary-as-of"),
  summaryCoverage: document.querySelector("#summary-coverage"),
  summaryContracts: document.querySelector("#summary-contracts"),
  summaryExpiries: document.querySelector("#summary-expiries"),
  summaryUnderlier: document.querySelector("#summary-underlier"),
  notice: document.querySelector("#data-notice"),
  modeFilter: document.querySelector("#mode-filter"),
  expiryFilter: document.querySelector("#expiry-filter"),
  weightingFilter: document.querySelector("#weighting-filter"),
  metricFilter: document.querySelector("#metric-filter"),
  surfaceTitle: document.querySelector("#surface-title"),
  surfaceSubtitle: document.querySelector("#surface-subtitle"),
  surfaceQuality: document.querySelector("#surface-quality"),
  heatmapStage: document.querySelector("#heatmap-stage"),
  heatmap: document.querySelector("#heatmap"),
  heatmapTooltip: document.querySelector("#heatmap-tooltip"),
  heatmapEmpty: document.querySelector("#heatmap-empty"),
  legendNegative: document.querySelector("#legend-negative"),
  legendNeutral: document.querySelector("#legend-neutral"),
  legendPositive: document.querySelector("#legend-positive"),
  legendRidge: document.querySelector("#legend-ridge"),
  legendPeak: document.querySelector("#legend-peak"),
  legendTrough: document.querySelector("#legend-trough"),
  legendDomain: document.querySelector("#legend-domain"),
  accessibleSummary: document.querySelector("#chart-accessible-summary"),
  ladderChart: document.querySelector("#ladder-chart"),
  ladderEmpty: document.querySelector("#ladder-empty"),
  ladderSubtitle: document.querySelector("#ladder-subtitle"),
  peakList: document.querySelector("#peak-list"),
  troughList: document.querySelector("#trough-list"),
  peakHeading: document.querySelector("#peak-heading"),
  troughHeading: document.querySelector("#trough-heading"),
  extremaSubtitle: document.querySelector("#extrema-subtitle"),
  sourceFile: document.querySelector("#source-file"),
  sourceMode: document.querySelector("#source-mode"),
  schemaVersion: document.querySelector("#schema-version"),
  signConvention: document.querySelector("#sign-convention"),
};

const app = {
  mode: initialModeFromQuery(),
  snapshot: null,
  expiry: "",
  expiryRole: "front",
  weighting: "oi_weighted",
  metric: "signed_gamma",
  chartHit: null,
  timer: null,
  playbackRaf: null,
  reducedMotionTimer: null,
  requestController: null,
  requestGeneration: 0,
  liveLeaseTimer: null,
  liveServerAnchorMs: null,
  livePerformanceAnchorMs: null,
  liveRequestStartedAtMs: null,
  livePhase: "off",
  liveDiagnosticController: null,
  liveDiagnosticGeneration: 0,
  liveLastError: "",
  replayCatalogLoading: false,
  projectionPolicySha256: "",
  timelineSha256: "",
  surfaceTimelineSha256: "",
  surfaceTimelineExtended: false,
  sourceFingerprint: "",
  timelineOpenMs: null,
  timelineCloseMs: null,
  sessions: [],
  sessionDate: "",
  frames: [],
  frameIndex: -1,
  legacyFrames: [],
  legacyFrameIndex: -1,
  frameLoading: false,
  trend: null,
  trendLoading: false,
  trendLayout: null,
  trendPriceLayer: null,
  trendHit: null,
  playheadMs: null,
  playheadAnchorMs: null,
  wallAnchorMs: null,
  lastPaintMs: 0,
  activeSpotIndex: -1,
  activeGammaIndex: -1,
  timelineStepMinutes: REPLAY_TIMELINE_STEP_MINUTES,
  playing: false,
  speed: 1,
  sessionSurface: null,
  sessionSurfaceLoading: false,
  sessionSurfaceController: null,
  sessionSurfaceGeneration: 0,
  sessionSurfaceRequestKey: "",
  sessionSurfaceRenderedKey: "",
  sessionSurfaceKeyframeIndex: -1,
  sessionSurfacePending: false,
  sessionSurfaceRetryTimer: null,
  sessionSurfaceRetryKey: "",
  sessionSurfaceRetryCount: 0,
  sessionSurfaceLastError: "",
  sessionSurfaceCache: new Map(),
  sessionSurfacePrefetchController: null,
  sessionSurfacePrefetchKey: "",
  replaySummarySignature: "",
  replayFrameChrome: { clock: "", position: "", timelineSec: -1, aria: "" },
  cockpitStaticSignature: "",
  cockpitLayouts: {},
  cockpitHover: null,
  liveViewportStartMs: null,
  liveViewportDrag: null,
  cockpitPriceDomain: null,
  cockpitColorDomains: {},
  strikeMode: "oi",
  cockpitPaintRaf: null,
};

function initialModeFromQuery() {
  const view = new URLSearchParams(window.location.search).get("view");
  return /\/(?:replay|friday)\/?$/.test(window.location.pathname) || ["replay", "friday"].includes(view)
    ? "replay"
    : "live";
}

function isObject(value) {
  return value !== null && typeof value === "object" && !Array.isArray(value);
}

function finiteNumber(value) {
  if (typeof value === "number" && Number.isFinite(value)) return value;
  return null;
}

function nonEmptyString(value) {
  return typeof value === "string" && value.trim() ? value.trim() : null;
}

function normalizedStatus(value) {
  const raw = isObject(value) ? value.status : value;
  const text = String(raw || "").toLowerCase();
  if (["ready", "ok", "available", "fresh"].includes(text)) return "ready";
  if (["degraded", "partial", "stale", "warning"].includes(text)) return "degraded";
  if (["unavailable", "blocked", "missing", "error", "insufficient"].includes(text)) {
    return "unavailable";
  }
  return "unknown";
}

function reasonsFrom(value) {
  if (!isObject(value)) return [];
  const candidates = [value.reasons, value.warnings];
  return candidates
    .flatMap((candidate) => (Array.isArray(candidate) ? candidate : []))
    .filter((item) => typeof item === "string" && item.trim())
    .map((item) => item.trim());
}

function reasonLabel(reason) {
  return REASON_LABELS[reason] || reason;
}

function canonicalReplayBytes(value) {
  const encoder = new TextEncoder();
  function encode(item) {
    if (item === null) return "n;";
    if (typeof item === "boolean") return item ? "b1;" : "b0;";
    if (typeof item === "number") {
      if (!Number.isFinite(item)) throw new Error("replay_hash_non_finite_number");
      if (Number.isInteger(item) && !Number.isSafeInteger(item)) {
        throw new Error("replay_hash_unsafe_integer");
      }
      const buffer = new ArrayBuffer(8);
      new DataView(buffer).setFloat64(0, item, false);
      const hex = Array.from(new Uint8Array(buffer), (byte) => byte.toString(16).padStart(2, "0")).join("");
      return `f${hex};`;
    }
    if (typeof item === "string") {
      return `s${encoder.encode(item).length}:${item}`;
    }
    if (Array.isArray(item)) {
      return `a${item.length}[${item.map(encode).join("")}]`;
    }
    if (isObject(item)) {
      const keys = Object.keys(item).sort();
      return `o${keys.length}{${keys.map((key) => `${encode(key)}${encode(item[key])}`).join("")}}`;
    }
    throw new Error("replay_hash_unsupported_value");
  }
  return encoder.encode(encode(value));
}

async function canonicalReplaySha256(value) {
  if (!globalThis.crypto?.subtle) throw new Error("replay_crypto_unavailable");
  const digest = await globalThis.crypto.subtle.digest("SHA-256", canonicalReplayBytes(value));
  return Array.from(new Uint8Array(digest), (byte) => byte.toString(16).padStart(2, "0")).join("");
}

function sha256String(value) {
  return typeof value === "string" && /^[0-9a-f]{64}$/.test(value);
}

async function verifyReplayDigests(raw, expected = null) {
  if (!sha256String(expected?.projectionPolicySha256)) {
    throw new Error("missing_expected_replay_projection_policy_hash");
  }
  if (!isObject(raw.projection_policy)) throw new Error("missing_replay_projection_policy");
  const policyDigest = await canonicalReplaySha256(raw.projection_policy);
  if (!sha256String(raw.projection_policy_sha256) || policyDigest !== raw.projection_policy_sha256) {
    throw new Error("replay_projection_policy_hash_mismatch");
  }
  if (policyDigest !== expected.projectionPolicySha256) {
    throw new Error("replay_timeline_policy_hash_mismatch");
  }
  const artifactBody = { ...raw };
  delete artifactBody.artifact_sha256;
  const artifactDigest = await canonicalReplaySha256(artifactBody);
  if (!sha256String(raw.artifact_sha256) || artifactDigest !== raw.artifact_sha256) {
    throw new Error("replay_artifact_hash_mismatch");
  }
  if (expected?.artifactSha256 && artifactDigest !== expected.artifactSha256) {
    throw new Error("replay_timeline_artifact_hash_mismatch");
  }
}

function parseDate(value) {
  const text = nonEmptyString(value);
  if (!text) return null;
  const parsed = new Date(text);
  return Number.isNaN(parsed.getTime()) ? null : parsed;
}

function normalizeSurface(raw) {
  if (!isObject(raw)) return null;
  const originalGrid = Array.isArray(raw.spot_grid)
    ? raw.spot_grid.map(finiteNumber)
    : Array.isArray(raw.spots)
      ? raw.spots.map(finiteNumber)
      : [];
  if (originalGrid.length < 2 || originalGrid.some((value) => value === null)) return null;

  const spotOrder = originalGrid
    .map((spot, index) => ({ spot, index }))
    .sort((left, right) => left.spot - right.spot);
  const spotGrid = spotOrder.map((item) => item.spot);
  if (spotGrid.some((spot, index) => index > 0 && spot <= spotGrid[index - 1])) return null;
  const sourceSlices = Array.isArray(raw.time_slices) ? raw.time_slices : [];
  const timeSlices = sourceSlices
    .filter(isObject)
    .map((slice, sourceIndex) => ({
      raw: slice,
      sourceIndex,
      minutesForward:
        finiteNumber(slice.minutes_forward) ??
        finiteNumber(Array.isArray(raw.time_offsets_minutes) ? raw.time_offsets_minutes[sourceIndex] : null),
      tauSeconds: finiteNumber(slice.tau_seconds),
      quality: normalizedStatus(slice.quality),
      warnings: [...reasonsFrom(slice.quality), ...reasonsFrom(slice)],
    }))
    .filter((slice) => slice.minutesForward !== null)
    .sort((left, right) => left.minutesForward - right.minutesForward);

  if (!timeSlices.length) return null;
  if (timeSlices.some((slice, index) => index > 0 && slice.minutesForward <= timeSlices[index - 1].minutesForward)) {
    return null;
  }
  return {
    raw,
    schemaVersion: nonEmptyString(raw.schema_version),
    asOf: parseDate(raw.as_of),
    expiry: nonEmptyString(raw.expiry),
    expiryClose: parseDate(raw.expiry_close),
    referenceSpot: finiteNumber(raw.reference_spot),
    spotGrid,
    spotOrder,
    timeSlices,
    quality: normalizedStatus(raw.quality),
    warnings: [...reasonsFrom(raw.quality), ...reasonsFrom(raw)],
    metricUnits: isObject(raw.metric_units) ? raw.metric_units : {},
    weightingSemantics: isObject(raw.weighting_semantics) ? raw.weighting_semantics : {},
    signConvention: nonEmptyString(raw.sign_convention),
    dealerPositionSign: nonEmptyString(raw.dealer_position_sign),
    model: nonEmptyString(raw.model),
  };
}

function normalizeExpiry(raw, index) {
  if (!isObject(raw)) return null;
  const surface = normalizeSurface(raw.surface || raw.exposure_surface || raw);
  const expiry = nonEmptyString(raw.expiry) || surface?.expiry;
  if (!expiry) return null;
  const role = nonEmptyString(raw.role) || (index === 0 ? "front" : index === 1 ? "next" : "other");
  const ladder = Array.isArray(raw.strike_ladder)
    ? raw.strike_ladder.filter(isObject)
    : Array.isArray(raw.ladder)
      ? raw.ladder.filter(isObject)
      : [];
  return {
    raw,
    expiry,
    role,
    surface,
    ladder,
    contractCount: finiteNumber(raw.contract_count) ?? finiteNumber(surface?.raw.contract_count),
    callCount: finiteNumber(raw.call_count),
    putCount: finiteNumber(raw.put_count),
    providers: Array.isArray(raw.providers)
      ? raw.providers.filter((item) => typeof item === "string")
      : [],
    quality: normalizedStatus(raw.quality || surface?.raw.quality),
    warnings: [...reasonsFrom(raw.quality), ...reasonsFrom(raw), ...(surface?.warnings || [])],
  };
}

function normalizeSnapshot(raw) {
  if (!isObject(raw)) throw new Error("snapshot_not_an_object");
  const kind = nonEmptyString(raw.kind);
  if (kind !== "spxw_surface_dashboard") throw new Error("unexpected_snapshot_kind");
  if (raw.schema_version !== 1) throw new Error("unsupported_snapshot_schema");
  if (raw.automatic_ordering !== false) throw new Error("unsafe_automatic_ordering_contract");

  const status = normalizedStatus(raw.status || raw.quality);
  if (status === "unknown") throw new Error("unsupported_snapshot_status");
  const createdAt = parseDate(raw.created_at);
  const asOf = parseDate(raw.as_of);
  const validUntil = parseDate(raw.valid_until);
  if (!createdAt || !asOf || !validUntil || validUntil.getTime() <= asOf.getTime()) {
    throw new Error("invalid_snapshot_clock_contract");
  }
  const expiryRows = Array.isArray(raw.expiries) ? raw.expiries : [];
  const expiries = status === "unavailable"
    ? []
    : expiryRows.map(normalizeExpiry).filter(Boolean);
  return {
    raw,
    mode: "live",
    kind,
    schemaVersion: String(raw.schema_version),
    status,
    createdAt,
    asOf,
    validUntil,
    automaticOrdering: raw.automatic_ordering === true,
    quality: isObject(raw.quality) ? raw.quality : {},
    reasons: [...reasonsFrom(raw.quality), ...reasonsFrom(raw)],
    underlier: isObject(raw.underlier) ? raw.underlier : {},
    session: isObject(raw.session) ? raw.session : {},
    expiries,
  };
}

async function normalizeReplaySnapshot(raw, expected) {
  if (!isObject(raw)) throw new Error("replay_not_an_object");
  if (!isObject(expected) || !(expected.at instanceof Date) || !expected.sessionDate) {
    throw new Error("missing_expected_replay_frame");
  }
  await verifyReplayDigests(raw, expected);
  if (nonEmptyString(raw.kind) !== "spxw_surface_dashboard_replay") {
    throw new Error("unexpected_replay_kind");
  }
  if (raw.schema_version !== 1) throw new Error("unsupported_replay_schema");
  if (raw.mode !== "replay") throw new Error("invalid_replay_mode");
  if (raw.policy_version !== REPLAY_POLICY_VERSION) {
    throw new Error("unexpected_replay_policy_version");
  }
  const replayId = nonEmptyString(raw.replay_id);
  if (!replayId || (expected.id && replayId !== expected.id)) {
    throw new Error("unexpected_replay_id");
  }
  if (raw.frozen !== true) throw new Error("replay_must_be_frozen");
  if (raw.automatic_ordering !== false) throw new Error("unsafe_automatic_ordering_contract");
  if (Object.prototype.hasOwnProperty.call(raw, "valid_until")) {
    throw new Error("replay_must_not_have_valid_until");
  }
  if (Object.prototype.hasOwnProperty.call(raw, "created_at")) {
    throw new Error("replay_must_not_have_live_created_at");
  }
  if (raw.session_date !== expected.sessionDate) throw new Error("unexpected_replay_session_date");

  const status = normalizedStatus(raw.status || raw.quality);
  if (status === "unknown") throw new Error("unsupported_replay_status");
  const requestedAsOf = parseDate(raw.requested_as_of);
  const dataAsOf = parseDate(raw.data_as_of);
  const generatedAt = parseDate(raw.generated_at);
  if (!requestedAsOf || !dataAsOf || !generatedAt) {
    throw new Error("invalid_replay_clock_contract");
  }
  if (requestedAsOf.getTime() !== expected.at.getTime()) {
    throw new Error("unexpected_replay_requested_as_of");
  }
  if (dataAsOf.getTime() > requestedAsOf.getTime()) {
    throw new Error("replay_data_after_requested_as_of");
  }
  if (generatedAt.getTime() < dataAsOf.getTime()) {
    throw new Error("replay_generated_before_data");
  }

  const source = isObject(raw.source) ? raw.source : null;
  if (!source) throw new Error("missing_replay_source_contract");
  const pitFieldNames = [
    "point_in_time_confidence",
    "availability_clock_available",
    "known_limitations",
  ];
  const pitFieldCount = pitFieldNames.filter((field) =>
    Object.prototype.hasOwnProperty.call(source, field)).length;
  if (pitFieldCount !== pitFieldNames.length) {
    throw new Error("incomplete_replay_pit_confidence_contract");
  }
  if (
    source.point_in_time_confidence !== "bounded_not_proven" ||
    source.availability_clock_available !== false ||
    !Array.isArray(source.known_limitations) ||
    source.known_limitations.length < 1 ||
    source.known_limitations.some((item) => !nonEmptyString(item))
  ) {
    throw new Error("unsafe_replay_pit_confidence_contract");
  }
  const knownLimitations = source.known_limitations.map((item) => item.trim());
  const replayPitReasons = [
    "bounded_point_in_time_not_proven",
    ...knownLimitations,
  ];
  const cutoffFields = [
    "received_at",
    "source_at",
    "quote_time",
    "trade_time",
    "last_update_at",
  ];
  if (
    !Array.isArray(source.cutoff_fields) ||
    source.cutoff_fields.length !== cutoffFields.length ||
    source.cutoff_fields.some((field, index) => field !== cutoffFields[index])
  ) {
    throw new Error("unsafe_replay_cutoff_fields");
  }
  if (
    source.cutoff_rule !==
    "received_at_and_available_source_clocks_lte_requested_as_of"
  ) {
    throw new Error("unsafe_replay_cutoff_rule");
  }
  if (source.lookahead_rows_selected !== 0) throw new Error("replay_lookahead_detected");
  if (source.coordinate !== "SPX") throw new Error("unsafe_replay_coordinate");
  if (source.trading_class !== "SPXW") throw new Error("unsafe_replay_trading_class");
  if (source.provider !== "schwab") throw new Error("unexpected_replay_provider");
  if (source.dataset !== "lake/quotes/schema=v1") throw new Error("unexpected_replay_dataset");
  if (source.lookback_seconds !== 15) throw new Error("unexpected_replay_lookback");
  if (
    source.selection_rule !==
    "latest_complete_row_per_instrument_by_available_clocks_then_surface_input_completeness"
  ) {
    throw new Error("unsafe_replay_selection_rule");
  }
  if (source.replay_loader_field_stitching !== false) {
    throw new Error("replay_field_stitching_detected");
  }
  if (source.source_files_verified_unchanged_during_read !== true) {
    throw new Error("unverified_replay_source_files");
  }
  if (
    source.structure_clock_available !== false ||
    source.compacted_at_available !== false ||
    !Array.isArray(source.compacted_at) ||
    source.compacted_at.length !== 0 ||
    !Array.isArray(source.lake_schema_versions) ||
    source.lake_schema_versions.length !== 1 ||
    source.lake_schema_versions[0] !== "v1" ||
    !Array.isArray(source.lake_writer_versions) ||
    source.lake_writer_versions.length !== 1 ||
    source.lake_writer_versions[0] !== "spx-spark-quote-compactor-v1"
  ) {
    throw new Error("unexpected_replay_lake_lineage");
  }
  const maxTransportAge = finiteNumber(source.max_transport_age_seconds);
  if (maxTransportAge === null || maxTransportAge < 0 || maxTransportAge > 15) {
    throw new Error("unsafe_replay_transport_age");
  }
  const maxObservationAge = finiteNumber(source.max_observation_age_seconds);
  const minObservationAge = finiteNumber(source.min_observation_age_seconds);
  if (
    maxObservationAge === null ||
    minObservationAge === null ||
    minObservationAge < 0 ||
    maxObservationAge < minObservationAge ||
    maxObservationAge > 15
  ) {
    throw new Error("unsafe_replay_observation_age");
  }
  const expiryCounts = isObject(source.selected_expiry_counts)
    ? source.selected_expiry_counts
    : {};
  const selectedQuoteCount = finiteNumber(source.selected_quote_count);
  const expiryCountValues = Object.values(expiryCounts).map(finiteNumber);
  if (
    !Number.isInteger(selectedQuoteCount) ||
    selectedQuoteCount <= 0 ||
    expiryCountValues.length < 1 ||
    expiryCountValues.some((value) => !Number.isInteger(value) || value <= 0) ||
    expiryCountValues.reduce((sum, value) => sum + value, 0) > selectedQuoteCount
  ) {
    throw new Error("unexpected_replay_quote_coverage");
  }
  const rawCandidateCount = finiteNumber(source.raw_candidate_count);
  const eligibleCandidateCount = finiteNumber(source.eligible_candidate_count);
  const ambiguousTopCount = finiteNumber(source.ambiguous_top_instrument_count);
  const droppedAmbiguousCount = finiteNumber(source.dropped_ambiguous_instrument_count);
  const auditCounts = [
    source.source_clock_rows_excluded,
    source.duplicate_received_at_group_count,
    source.duplicate_received_at_row_count,
    source.resolved_by_surface_completeness_instrument_count,
    source.identical_top_duplicate_row_count,
    ambiguousTopCount,
    droppedAmbiguousCount,
  ].map(finiteNumber);
  if (
    !Number.isInteger(rawCandidateCount) ||
    !Number.isInteger(eligibleCandidateCount) ||
    rawCandidateCount < eligibleCandidateCount ||
    eligibleCandidateCount < selectedQuoteCount ||
    auditCounts.some((value) => !Number.isInteger(value) || value < 0) ||
    droppedAmbiguousCount !== ambiguousTopCount
  ) {
    throw new Error("unexpected_replay_selection_audit");
  }
  if (
    !Array.isArray(source.source_files) ||
    source.source_files.length < 1 ||
    source.source_files.some((file) => !nonEmptyString(file) || !file.startsWith("lake/quotes/schema=v1/"))
  ) {
    throw new Error("unexpected_replay_source_file");
  }
  const sourceHashes = isObject(source.parquet_file_sha256)
    ? source.parquet_file_sha256
    : {};
  if (Object.keys(sourceHashes).length !== source.source_files.length ||
      source.source_files.some((file) => !sha256String(sourceHashes[file]))) {
    throw new Error("invalid_replay_source_hash");
  }
  const rawSourceHashes = isObject(source.raw_source_file_sha256)
    ? source.raw_source_file_sha256
    : {};
  const rawSourceFiles = Object.keys(rawSourceHashes);
  if (rawSourceFiles.length < 1 ||
      rawSourceFiles.some((file) => !file.startsWith("raw/provider=schwab/") || !sha256String(rawSourceHashes[file]))) {
    throw new Error("invalid_replay_raw_source_hash");
  }

  if (!Array.isArray(raw.expiries)) throw new Error("invalid_replay_expiries");
  const normalizedExpiries = raw.expiries.map(normalizeExpiry);
  if (normalizedExpiries.some((expiry) => !expiry)) throw new Error("invalid_replay_expiry");
  const expiries = status === "unavailable" ? [] : normalizedExpiries;
  const normalizedSource = {
    ...source,
    point_in_time_confidence: "bounded_not_proven",
    availability_clock_available: false,
    known_limitations: knownLimitations,
  };
  return {
    raw,
    mode: "replay",
    kind: raw.kind,
    replayId,
    sessionDate: raw.session_date,
    schemaVersion: String(raw.schema_version),
    status,
    createdAt: generatedAt,
    generatedAt,
    asOf: requestedAsOf,
    requestedAsOf,
    dataAsOf,
    validUntil: null,
    frozen: true,
    automaticOrdering: false,
    source: normalizedSource,
    quality: isObject(raw.quality) ? raw.quality : {},
    reasons: [...replayPitReasons, ...reasonsFrom(raw.quality), ...reasonsFrom(raw)],
    underlier: isObject(raw.underlier) ? raw.underlier : {},
    session: isObject(raw.session) ? raw.session : {},
    expiries,
  };
}

function strictInteger(value, error) {
  if (!Number.isSafeInteger(value)) throw new Error(error);
  return value;
}

function strictTrendClock(value, error) {
  const parsed = parseDate(value);
  if (!parsed || parsed.getUTCMilliseconds() !== 0) throw new Error(error);
  return parsed.getTime();
}

function validateTrendSource(raw, openMs, closeMs, expectedSourceFingerprint) {
  if (!isObject(raw) || !isObject(raw.spx)) throw new Error("invalid_replay_trend_source");
  const cutoffFields = [
    "received_at",
    "source_at",
    "quote_time",
    "trade_time",
    "last_update_at",
  ];
  if (
    raw.dataset !== "lake/quotes/schema=v1" ||
    raw.source_fingerprint !== expectedSourceFingerprint ||
    raw.source_files_verified_unchanged_during_build !== true ||
    raw.availability_clock_available !== false ||
    raw.availability_clock !== "unavailable" ||
    raw.point_in_time_confidence !== "bounded_not_proven" ||
    !Array.isArray(raw.source_files) ||
    raw.source_files.length < 1 ||
    raw.source_files.some((file) => !nonEmptyString(file) || !file.startsWith("lake/quotes/schema=v1/")) ||
    new Set(raw.source_files).size !== raw.source_files.length ||
    !isObject(raw.parquet_file_sha256) ||
    Object.keys(raw.parquet_file_sha256).length !== raw.source_files.length ||
    raw.source_files.some((file) => !sha256String(raw.parquet_file_sha256[file])) ||
    !Array.isArray(raw.cutoff_fields) ||
    raw.cutoff_fields.length !== cutoffFields.length ||
    raw.cutoff_fields.some((field, index) => field !== cutoffFields[index]) ||
    !Array.isArray(raw.known_limitations) ||
    raw.known_limitations.length < 1 ||
    raw.known_limitations.some((item) => !nonEmptyString(item))
  ) {
    throw new Error("invalid_replay_trend_source_contract");
  }
  const spx = raw.spx;
  if (
    spx.price_field !== "mark" ||
    spx.market_clock !== "source_at" ||
    spx.source_at_resolution !== "milliseconds" ||
    spx.known_at_rule !== "max_recorded_clocks" ||
    spx.known_at_is_availability_clock !== false ||
    spx.dedupe_rule !==
      "latest_known_at_then_received_at_then_source_file_position_per_source_at" ||
    !Array.isArray(spx.source_offset_ms) ||
    !Array.isArray(spx.known_at_offset_ms) ||
    !Array.isArray(spx.price)
  ) {
    throw new Error("invalid_replay_trend_spx_contract");
  }
  const baseSourceAtMs = strictInteger(
    spx.base_source_at_ms,
    "invalid_replay_trend_spx_base_clock",
  );
  const count = spx.source_offset_ms.length;
  if (
    count < 2 ||
    spx.known_at_offset_ms.length !== count ||
    spx.price.length !== count ||
    spx.point_count !== count
  ) {
    throw new Error("invalid_replay_trend_spx_shape");
  }
  if (
    !Number.isSafeInteger(spx.raw_row_count) ||
    spx.raw_row_count < count ||
    !Number.isSafeInteger(spx.duplicate_source_at_group_count) ||
    spx.duplicate_source_at_group_count < 0 ||
    spx.duplicate_source_at_group_count > count
  ) {
    throw new Error("invalid_replay_trend_spx_raw_counts");
  }
  const sourceMs = [];
  const knownMs = [];
  const prices = [];
  for (let index = 0; index < count; index += 1) {
    const sourceOffset = strictInteger(
      spx.source_offset_ms[index],
      "invalid_replay_trend_spx_source_offset",
    );
    const knownOffset = strictInteger(
      spx.known_at_offset_ms[index],
      "invalid_replay_trend_spx_known_offset",
    );
    const price = finiteNumber(spx.price[index]);
    const sourceAt = baseSourceAtMs + sourceOffset;
    const knownAt = baseSourceAtMs + knownOffset;
    if (
      sourceOffset < 0 ||
      knownOffset < sourceOffset ||
      sourceAt < openMs ||
      sourceAt > closeMs ||
      knownAt < openMs ||
      knownAt > closeMs ||
      price === null ||
      price <= 0 ||
      (index > 0 && sourceAt <= sourceMs[index - 1]) ||
      (index > 0 && knownAt < knownMs[index - 1])
    ) {
      throw new Error("invalid_replay_trend_spx_point");
    }
    sourceMs.push(sourceAt);
    knownMs.push(knownAt);
    prices.push(price);
  }
  return {
    raw: spx,
    baseSourceAtMs,
    sourceMs,
    knownMs,
    prices,
    maxLineGapMs: 30_000,
  };
}

function normalizeTrendGap(raw, openMs, closeMs) {
  if (!isObject(raw)) throw new Error("invalid_replay_trend_gap");
  const startMs = strictTrendClock(raw.start_at, "invalid_replay_trend_gap_start");
  const endMs = strictTrendClock(raw.end_at, "invalid_replay_trend_gap_end");
  if (
    raw.start_offset_ms !== startMs - openMs ||
    raw.end_offset_ms !== endMs - openMs ||
    startMs < openMs ||
    endMs > closeMs ||
    endMs <= startMs ||
    !nonEmptyString(raw.reason)
  ) {
    throw new Error("invalid_replay_trend_gap_contract");
  }
  return { raw, startMs, endMs, reason: raw.reason };
}

function validateTrendSurface(raw, expected, openMs, closeMs) {
  if (
    !isObject(raw) ||
    raw.cadence !== "catalog_timeline_keyframes" ||
    raw.validity_rule !== REPLAY_TREND_VALIDITY_RULE ||
    raw.interpolation !== "none" ||
    raw.higher_frequency_candidate_upgrade !== false ||
    !nonEmptyString(raw.metric_unit) ||
    !Array.isArray(raw.shared_relative_spot_offsets) ||
    !Array.isArray(raw.keyframes) ||
    !Array.isArray(raw.gaps)
  ) {
    throw new Error("invalid_replay_trend_surface_contract");
  }
  const spotOffsets = raw.shared_relative_spot_offsets.map((value) => {
    const normalized = finiteNumber(value);
    if (normalized === null) throw new Error("invalid_replay_trend_spot_offset");
    return normalized;
  });
  if (
    spotOffsets.length < 2 ||
    spotOffsets.some((value, index) => index > 0 && value <= spotOffsets[index - 1]) ||
    raw.frame_count !== raw.keyframes.length ||
    raw.keyframes.length !== expected.frames.length
  ) {
    throw new Error("invalid_replay_trend_surface_shape");
  }
  const keyframes = raw.keyframes.map((item, index) => {
    if (!isObject(item)) throw new Error("invalid_replay_trend_keyframe");
    const atMs = strictTrendClock(item.at, "invalid_replay_trend_keyframe_at");
    const validUntilMs = strictTrendClock(
      item.valid_until,
      "invalid_replay_trend_keyframe_valid_until",
    );
    const dataAsOfMs = item.data_as_of
      ? strictTrendClock(item.data_as_of, "invalid_replay_trend_keyframe_data_as_of")
      : atMs;
    const referenceSpot = finiteNumber(item.reference_spot);
    const zeroRidgeSpot = item.zero_ridge_spot === null
      ? null
      : finiteNumber(item.zero_ridge_spot);
    const coverageRatio = item.coverage_ratio === undefined
      ? null
      : finiteNumber(item.coverage_ratio);
    const status = normalizedStatus(item.quality);
    const expectedFrame = expected.frames[index];
    if (
      item.at_offset_ms !== atMs - openMs ||
      item.valid_until_offset_ms !== validUntilMs - openMs ||
      atMs !== expectedFrame.at.getTime() ||
      atMs < openMs ||
      atMs >= closeMs ||
      (["ready", "degraded"].includes(status) && validUntilMs <= atMs) ||
      (status === "unavailable" && validUntilMs !== atMs) ||
      validUntilMs > closeMs ||
      dataAsOfMs > atMs ||
      !/^\d{8}$/.test(String(item.expiry || "")) ||
      referenceSpot === null ||
      referenceSpot <= 0 ||
      (zeroRidgeSpot === null && item.zero_ridge_spot !== null) ||
      (coverageRatio !== null && (coverageRatio < 0 || coverageRatio > 1)) ||
      !["ready", "degraded", "unavailable"].includes(status) ||
      !Array.isArray(item.warnings) ||
      item.warnings.some((warning) => !nonEmptyString(warning)) ||
      !sha256String(item.frame_artifact_sha256) ||
      !Array.isArray(item.values) ||
      item.values.length !== spotOffsets.length
    ) {
      throw new Error("invalid_replay_trend_keyframe_contract");
    }
    const values = item.values.map((value) => {
      if (value === null) return null;
      const normalized = finiteNumber(value);
      if (normalized === null) throw new Error("invalid_replay_trend_keyframe_value");
      return normalized;
    });
    if (status !== "unavailable" && !values.some((value) => value !== null)) {
      throw new Error("invalid_replay_trend_keyframe_coverage");
    }
    return {
      raw: item,
      id: expectedFrame.id,
      atMs,
      validUntilMs,
      dataAsOfMs,
      expiry: item.expiry,
      referenceSpot,
      values,
      zeroRidgeSpot,
      coverageRatio,
      status,
      warnings: item.warnings.map((warning) => warning.trim()),
      frameArtifactSha256: item.frame_artifact_sha256,
    };
  });
  for (let index = 0; index < keyframes.length - 1; index += 1) {
    if (keyframes[index].validUntilMs > keyframes[index + 1].atMs) {
      throw new Error("overlapping_replay_trend_keyframes");
    }
  }
  const gaps = raw.gaps.map((gap) => normalizeTrendGap(gap, openMs, closeMs));
  for (let index = 1; index < gaps.length; index += 1) {
    if (gaps[index].startMs < gaps[index - 1].endMs) {
      throw new Error("overlapping_replay_trend_gaps");
    }
  }
  const expectedGaps = [];
  let cursor = openMs;
  for (const keyframe of keyframes) {
    if (cursor < keyframe.atMs) expectedGaps.push([cursor, keyframe.atMs]);
    cursor = Math.max(cursor, keyframe.validUntilMs);
  }
  if (cursor < closeMs) expectedGaps.push([cursor, closeMs]);
  if (
    gaps.length !== expectedGaps.length ||
    gaps.some((gap, index) =>
      gap.startMs !== expectedGaps[index][0] || gap.endMs !== expectedGaps[index][1])
  ) {
    throw new Error("invalid_replay_trend_gap_coverage");
  }
  return {
    raw,
    cadence: raw.cadence,
    metricUnit: raw.metric_unit,
    spotOffsets,
    keyframes,
    gaps,
  };
}

async function normalizeReplayTrend(raw, expected) {
  if (
    !isObject(expected) ||
    !expected.sessionDate ||
    !["front", "next"].includes(expected.role) ||
    !WEIGHTINGS[expected.weighting] ||
    !METRICS[expected.metric] ||
    !sha256String(expected.projectionPolicySha256) ||
    !sha256String(expected.timelineSha256) ||
    !sha256String(expected.sourceFingerprint) ||
    !Array.isArray(expected.frames)
  ) {
    throw new Error("missing_expected_replay_trend_contract");
  }
  await verifyReplayDigests(raw, {
    projectionPolicySha256: expected.projectionPolicySha256,
  });
  if (
    !isObject(raw) ||
    raw.schema_version !== 1 ||
    raw.kind !== REPLAY_TREND_KIND ||
    raw.mode !== "replay" ||
    raw.policy_version !== REPLAY_TREND_POLICY_VERSION ||
    raw.frame_policy_version !== REPLAY_POLICY_VERSION ||
    raw.timeline_policy_version !== REPLAY_TIMELINE_POLICY_VERSION ||
    raw.session_date !== expected.sessionDate ||
    raw.provider !== "schwab" ||
    raw.coordinate !== "SPX" ||
    raw.trading_class !== "SPXW" ||
    raw.role !== expected.role ||
    raw.weighting !== expected.weighting ||
    raw.metric !== expected.metric ||
    raw.projection_policy_sha256 !== expected.projectionPolicySha256 ||
    raw.timeline_sha256 !== expected.timelineSha256 ||
    raw.source_fingerprint !== expected.sourceFingerprint ||
    raw.frame_interval_minutes !== REPLAY_TIMELINE_STEP_MINUTES ||
    raw.lookback_seconds !== 15 ||
    raw.session_close_grace_elapsed !== true ||
    raw.session_close_grace_policy !== REPLAY_CLOSE_GRACE_POLICY ||
    raw.session_close_grace_seconds !== REPLAY_CLOSE_GRACE_SECONDS ||
    raw.availability_proven !== false ||
    raw.availability_clock !== "unavailable" ||
    raw.point_in_time_confidence !== "bounded_not_proven" ||
    raw.data_finalization_proven !== false
  ) {
    throw new Error("invalid_replay_trend_contract");
  }
  const openMs = strictTrendClock(raw.open_at, "invalid_replay_trend_open");
  const closeMs = strictTrendClock(raw.close_at, "invalid_replay_trend_close");
  const closeGraceElapsedAt = parseDate(raw.session_close_grace_elapsed_at);
  if (
    openMs >= closeMs ||
    new Date(openMs).toISOString().slice(0, 10) !== expected.sessionDate ||
    new Date(closeMs).toISOString().slice(0, 10) !== expected.sessionDate ||
    !validateCloseGraceClock(new Date(closeMs), closeGraceElapsedAt)
  ) {
    throw new Error("invalid_replay_trend_session_clock");
  }
  const spx = validateTrendSource(
    raw.source,
    openMs,
    closeMs,
    expected.sourceFingerprint,
  );
  const surface = validateTrendSurface(raw.surface, expected, openMs, closeMs);
  const status = worstQuality(surface.keyframes.map((keyframe) => keyframe.status));
  return {
    raw,
    status,
    sessionDate: raw.session_date,
    openMs,
    closeMs,
    role: raw.role,
    weighting: raw.weighting,
    metric: raw.metric,
    projectionPolicySha256: raw.projection_policy_sha256,
    timelineSha256: raw.timeline_sha256,
    sourceFingerprint: raw.source_fingerprint,
    source: raw.source,
    spx,
    gamma: surface,
  };
}

function finiteOrNull(value, error) {
  if (value === null || value === undefined) return null;
  const parsed = finiteNumber(value);
  if (parsed === null) throw new Error(error);
  return parsed;
}

function robustDomain(values, supplied = null) {
  const magnitudes = values
    .flat(Infinity)
    .filter((value) => Number.isFinite(value))
    .map((value) => Math.abs(value))
    .sort((left, right) => left - right);
  const quantilePosition = magnitudes.length ? (magnitudes.length - 1) * 0.98 : -1;
  const quantileLower = quantilePosition >= 0 ? Math.floor(quantilePosition) : -1;
  const quantileUpper = quantilePosition >= 0 ? Math.ceil(quantilePosition) : -1;
  const quantileFraction = quantilePosition >= 0 ? quantilePosition - quantileLower : 0;
  const robustMaximum = quantileLower >= 0
    ? magnitudes[quantileLower] +
      (magnitudes[quantileUpper] - magnitudes[quantileLower]) * quantileFraction
    : 0;
  let suppliedMaximum = null;
  let suppliedThreshold = null;
  if (Number.isFinite(supplied)) {
    suppliedMaximum = Math.abs(supplied);
  } else if (Array.isArray(supplied)) {
    suppliedMaximum = Math.max(
      ...supplied.filter((value) => Number.isFinite(value)).map((value) => Math.abs(value)),
      0,
    );
  } else if (isObject(supplied)) {
    const bounds = Array.isArray(supplied.domain) ? supplied.domain : [];
    suppliedMaximum = [
      supplied.max_abs,
      supplied.maxAbs,
      supplied.symmetric_max,
      supplied.robust_max,
      ...bounds,
    ].reduce((maximum, value) => {
      const parsed = finiteNumber(value);
      return parsed === null ? maximum : Math.max(maximum, Math.abs(parsed));
    }, 0);
    suppliedThreshold = [
      supplied.threshold,
      supplied.zero_threshold,
      supplied.neutral_threshold,
    ].map(finiteNumber).find((value) => value !== null && value >= 0) ?? null;
  }
  const maxAbs = Math.max(suppliedMaximum || 0, robustMaximum, 1e-12);
  const threshold = Math.min(
    suppliedThreshold ?? maxAbs * 0.025,
    maxAbs,
  );
  return { maxAbs, threshold, sampleCount: magnitudes.length };
}

function expandOnlyDomain(previous, candidate, key) {
  if (!previous || previous.key !== key) return { ...candidate, key };
  return {
    key,
    maxAbs: Math.max(previous.maxAbs, candidate.maxAbs),
    threshold: Math.max(previous.threshold, candidate.threshold),
    sampleCount: candidate.sampleCount,
  };
}

function sessionTimeToX(layout, sessionStartMs, sessionEndMs, timestampMs) {
  const span = Math.max(sessionEndMs - sessionStartMs, 1);
  const ratio = Math.max(0, Math.min((timestampMs - sessionStartMs) / span, 1));
  return layout.plotLeft + ratio * layout.plotWidth;
}

function sessionXToTime(layout, sessionStartMs, sessionEndMs, x) {
  const ratio = Math.max(0, Math.min((x - layout.plotLeft) / layout.plotWidth, 1));
  return sessionStartMs + ratio * (sessionEndMs - sessionStartMs);
}

function cockpitTimeWindow(
  surface,
  { serverNowMs = null, manualStartMs = null } = {},
) {
  if (!surface || surface.mode !== "live") {
    return {
      startMs: surface?.sessionStartMs ?? null,
      endMs: surface?.sessionEndMs ?? null,
      followsNow: false,
    };
  }
  const sessionSpanMs = Math.max(surface.sessionEndMs - surface.sessionStartMs, 1);
  const spanMs = Math.min(LIVE_VIEW_SPAN_MS, sessionSpanMs);
  const latestStartMs = Math.max(surface.sessionEndMs - spanMs, surface.sessionStartMs);
  const followsNow = !Number.isFinite(manualStartMs);
  const anchorMs = Number.isFinite(serverNowMs) ? serverNowMs : surface.asOfMs;
  const requestedStartMs = followsNow
    ? anchorMs - LIVE_VIEW_HISTORY_MS
    : manualStartMs;
  const startMs = Math.max(
    surface.sessionStartMs,
    Math.min(requestedStartMs, latestStartMs),
  );
  return { startMs, endMs: startMs + spanMs, followsNow };
}

function activeCockpitTimeWindow(surface = app.sessionSurface) {
  return cockpitTimeWindow(surface, {
    serverNowMs: surface?.mode === "live" ? liveClockNowMs() : null,
    manualStartMs: surface?.mode === "live" ? app.liveViewportStartMs : null,
  });
}

function cockpitTimeToX(layout, surface, timestampMs) {
  const window = activeCockpitTimeWindow(surface);
  return sessionTimeToX(layout, window.startMs, window.endMs, timestampMs);
}

function cockpitXToTime(layout, surface, x) {
  const window = activeCockpitTimeWindow(surface);
  return sessionXToTime(layout, window.startMs, window.endMs, x);
}

function cockpitVisibleTimeRange(surface, startMs, endMs) {
  const window = activeCockpitTimeWindow(surface);
  const clippedStartMs = Math.max(startMs, window.startMs);
  const clippedEndMs = Math.min(endMs, window.endMs);
  return clippedEndMs > clippedStartMs
    ? { startMs: clippedStartMs, endMs: clippedEndMs }
    : null;
}

function updateLiveViewportChrome(surface = app.sessionSurface) {
  const live = app.mode === "live";
  dom.liveViewportControls.hidden = !live;
  if (!live) return;
  if (!surface) {
    dom.liveViewportLabel.textContent = "跟随当前 · 前 90m / 后 30m";
    dom.liveViewportReset.disabled = true;
    return;
  }
  const window = activeCockpitTimeWindow(surface);
  dom.liveViewportLabel.textContent = `${window.followsNow ? "跟随当前" : "浏览历史"} · ${formatAxisMarketTime(window.startMs)}–${formatAxisMarketTime(window.endMs)}`;
  dom.liveViewportReset.disabled = window.followsNow;
}

function resetLiveViewport({ render = true } = {}) {
  app.liveViewportStartMs = null;
  app.liveViewportDrag = null;
  document.body.classList.remove("live-viewport-dragging");
  app.cockpitStaticSignature = "";
  updateLiveViewportChrome();
  if (render && app.sessionSurface) renderCockpitStatic();
}

function sessionPriceToY(layout, priceDomain, price) {
  const span = Math.max(priceDomain.max - priceDomain.min, 1e-9);
  const ratio = Math.max(0, Math.min((price - priceDomain.min) / span, 1));
  return layout.plotTop + (1 - ratio) * layout.plotHeight;
}

function sessionYToPrice(layout, priceDomain, y) {
  const ratio = Math.max(0, Math.min((y - layout.plotTop) / layout.plotHeight, 1));
  return priceDomain.max - ratio * (priceDomain.max - priceDomain.min);
}

function normalizeSessionMatrix(raw, name, timeCount, priceCount, { optional = false } = {}) {
  if (raw === null || raw === undefined) {
    if (optional) return null;
    throw new Error(`missing_session_surface_${name}`);
  }
  if (!Array.isArray(raw) || raw.length !== timeCount) {
    throw new Error(`invalid_session_surface_${name}_shape`);
  }
  return raw.map((row) => {
    if (!Array.isArray(row) || row.length !== priceCount) {
      throw new Error(`invalid_session_surface_${name}_shape`);
    }
    return row.map((value) => finiteOrNull(value, `invalid_session_surface_${name}_value`));
  });
}

function normalizeSessionTimeBuckets(raw, sessionStartMs, sessionEndMs, bucketMinutes) {
  if (!Array.isArray(raw) || !raw.length) throw new Error("invalid_session_surface_time_buckets");
  const starts = raw.map((item) => {
    const value = isObject(item)
      ? item.start_at ?? item.bucket_start ?? item.at
      : item;
    const parsed = parseDate(value);
    if (!parsed) throw new Error("invalid_session_surface_bucket_start");
    return parsed.getTime();
  });
  if (starts.some((value, index) =>
    value < sessionStartMs || value >= sessionEndMs || (index > 0 && value <= starts[index - 1]))) {
    throw new Error("invalid_session_surface_bucket_order");
  }
  const buckets = raw.map((item, index) => {
    const explicitEnd = isObject(item)
      ? parseDate(item.end_at ?? item.bucket_end)?.getTime()
      : null;
    const derivedEnd = starts[index + 1] ?? Math.min(
      starts[index] + bucketMinutes * 60_000,
      sessionEndMs,
    );
    const endMs = explicitEnd ?? derivedEnd;
    const policyEnd = Math.min(starts[index] + bucketMinutes * 60_000, sessionEndMs);
    const previousPolicyEnd = index > 0
      ? Math.min(starts[index - 1] + bucketMinutes * 60_000, sessionEndMs)
      : null;
    if (!Number.isFinite(endMs) || endMs !== policyEnd || endMs <= starts[index] ||
        endMs > sessionEndMs || (index > 0 && starts[index] !== previousPolicyEnd)) {
      throw new Error("invalid_session_surface_bucket_end");
    }
    return {
      raw: item,
      startMs: starts[index],
      endMs,
      centerMs: starts[index] + (endMs - starts[index]) / 2,
    };
  });
  if (buckets[0].startMs !== sessionStartMs || buckets.at(-1).endMs !== sessionEndMs) {
    throw new Error("invalid_session_surface_bucket_coverage");
  }
  return buckets;
}

function sessionSegmentAtTime(segments, timeMs) {
  if (!Array.isArray(segments) || !segments.length || !Number.isFinite(timeMs)) return null;
  const match = segments.find((segment) =>
    segment.startMs <= timeMs && timeMs < segment.endMs);
  if (match) return match;
  const last = segments.at(-1);
  return timeMs === last.endMs ? last : null;
}

function sessionSegmentForBucket(segments, bucket) {
  return segments.find((segment) =>
    segment.startMs <= bucket.startMs && bucket.endMs <= segment.endMs) || null;
}

function normalizeSessionSegments(raw, sessionStartMs, sessionEndMs, schemaVersion, mode = "replay") {
  if (schemaVersion === 1) {
    if (raw !== undefined && raw !== null) {
      throw new Error("session_surface_v1_has_session_segments");
    }
    return [];
  }
  if (!Array.isArray(raw) || raw.length !== 3) {
    throw new Error("invalid_session_surface_segments");
  }
  const expectedKinds = ["gth", "closed_gap", "rth"];
  const segments = raw.map((item, index) => {
    if (!isObject(item)) throw new Error("invalid_session_surface_segment");
    const kind = nonEmptyString(item.kind);
    const contract = kind ? SESSION_SEGMENT_CONTRACT[kind] : null;
    const start = parseDate(item.start_at);
    const end = parseDate(item.end_at);
    const surfaceProvider = item.surface_provider === null
      ? null
      : nonEmptyString(item.surface_provider);
    const referenceMethod = item.reference_method === null
      ? null
      : nonEmptyString(item.reference_method);
    const suppliedReferenceProvider = Object.prototype.hasOwnProperty.call(
      item,
      "reference_provider",
    )
      ? item.reference_provider === null
        ? null
        : nonEmptyString(item.reference_provider)
      : mode === "live"
        ? null
        : contract?.referenceProvider;
    if (mode === "live") {
      // Live providers fail over, so the artifact self-declares its providers;
      // only the reference method semantics are pinned per segment kind.
      const expectedMethod = LIVE_SEGMENT_REFERENCE_METHODS[kind] ?? null;
      const observed = surfaceProvider !== null;
      if (kind !== expectedKinds[index] || !start || !end ||
          end.getTime() <= start.getTime() ||
          (kind === "closed_gap" &&
            (surfaceProvider !== null || referenceMethod !== null ||
              suppliedReferenceProvider !== null)) ||
          (kind !== "closed_gap" && (
            (observed && (referenceMethod !== expectedMethod || !suppliedReferenceProvider)) ||
            (!observed && (referenceMethod !== null || suppliedReferenceProvider !== null))
          ))) {
        throw new Error("invalid_session_surface_segment_contract");
      }
    } else if (!contract || kind !== expectedKinds[index] || !start || !end ||
        end.getTime() <= start.getTime() ||
        surfaceProvider !== contract.surfaceProvider ||
        referenceMethod !== contract.referenceMethod ||
        suppliedReferenceProvider !== contract.referenceProvider) {
      throw new Error("invalid_session_surface_segment_contract");
    }
    return {
      raw: item,
      kind,
      startMs: start.getTime(),
      endMs: end.getTime(),
      surfaceProvider,
      referenceProvider: suppliedReferenceProvider,
      referenceMethod,
    };
  });
  if (segments[0].startMs !== sessionStartMs || segments.at(-1).endMs !== sessionEndMs ||
      segments.some((segment, index) =>
        index > 0 && segment.startMs !== segments[index - 1].endMs)) {
    throw new Error("invalid_session_surface_segment_coverage");
  }
  return segments;
}

function normalizeSessionProviders(raw, schemaVersion, mode = "replay") {
  if (schemaVersion === 1) {
    if (raw !== undefined && raw !== null) {
      throw new Error("session_surface_v1_has_providers_contract");
    }
    return null;
  }
  if (mode === "live") {
    if (!isObject(raw)) throw new Error("invalid_session_surface_providers_contract");
    const values = {
      gthSurface: raw.gth_surface,
      gthReference: raw.gth_reference,
      rthSurface: raw.rth_surface,
      rthReference: raw.rth_reference,
    };
    for (const value of Object.values(values)) {
      if (value !== null && !nonEmptyString(value)) {
        throw new Error("invalid_session_surface_providers_contract");
      }
    }
    return {
      raw,
      gthSurface: values.gthSurface ?? null,
      gthReference: values.gthReference ?? null,
      rthSurface: values.rthSurface ?? null,
      rthReference: values.rthReference ?? null,
    };
  }
  if (!isObject(raw) || raw.gth_surface !== "ibkr" || raw.gth_reference !== "schwab" ||
      raw.rth_surface !== "schwab" || raw.rth_reference !== "schwab") {
    throw new Error("invalid_session_surface_providers_contract");
  }
  return {
    raw,
    gthSurface: raw.gth_surface,
    gthReference: raw.gth_reference,
    rthSurface: raw.rth_surface,
    rthReference: raw.rth_reference,
  };
}

function normalizeReferenceBasis(raw, asOfMs) {
  if (!isObject(raw)) throw new Error("invalid_session_surface_reference_basis");
  const value = finiteNumber(raw.value);
  const esContract = nonEmptyString(raw.es_contract);
  const sampleCount = raw.sample_count;
  const windowStart = parseDate(raw.window_start_at);
  const windowEnd = parseDate(raw.window_end_at);
  const knownAt = parseDate(raw.known_at);
  const frozenAt = parseDate(raw.frozen_at);
  if (value === null || raw.method !== "frozen_previous_rth_median" ||
      raw.provider !== "schwab" || !esContract ||
      !Number.isSafeInteger(sampleCount) || sampleCount < 1 ||
      !windowStart || !windowEnd || !knownAt || !frozenAt ||
      windowStart.getTime() > windowEnd.getTime() ||
      windowEnd.getTime() > knownAt.getTime() ||
      knownAt.getTime() > frozenAt.getTime() || frozenAt.getTime() > asOfMs) {
    throw new Error("invalid_session_surface_reference_basis");
  }
  return {
    raw,
    value,
    method: raw.method,
    provider: raw.provider,
    esContract,
    sampleCount,
    windowStartMs: windowStart.getTime(),
    windowEndMs: windowEnd.getTime(),
    knownAtMs: knownAt.getTime(),
    frozenAtMs: frozenAt.getTime(),
  };
}

function normalizeSessionReference(raw, asOfMs, activeSegment, mode = "replay") {
  if (!isObject(raw) || raw.coordinate !== "SPX") {
    throw new Error("invalid_session_surface_reference_contract");
  }
  const price = finiteOrNull(raw.price, "invalid_session_surface_reference_price");
  const method = raw.method === null ? null : nonEmptyString(raw.method);
  const provider = raw.provider === null ? null : nonEmptyString(raw.provider);
  const instrumentId = raw.instrument_id === null
    ? null
    : nonEmptyString(raw.instrument_id);
  const sourceAt = raw.source_at === null ? null : parseDate(raw.source_at);
  const knownAt = raw.known_at === null ? null : parseDate(raw.known_at);
  const acceptedAt = raw.accepted_at === null ? null : parseDate(raw.accepted_at);
  const validUntil = raw.valid_until === null ? null : parseDate(raw.valid_until);
  const quality = normalizedStatus(raw.quality);
  const missingReason = raw.missing_reason === null
    ? null
    : nonEmptyString(raw.missing_reason);
  if (price === null) {
    if ([method, provider, instrumentId, sourceAt, knownAt, acceptedAt, validUntil]
        .some((value) => value !== null) || raw.basis !== null || !missingReason ||
        quality !== "unavailable" || !activeSegment) {
      throw new Error("invalid_session_surface_missing_reference");
    }
    return {
      raw,
      coordinate: raw.coordinate,
      price: null,
      method: null,
      provider: null,
      instrumentId: null,
      sourceAtMs: null,
      knownAtMs: null,
      acceptedAtMs: null,
      validUntilMs: null,
      quality,
      missingReason,
      basis: null,
      inferred: false,
    };
  }
  const allowedMethods = mode === "live"
    ? ["chain_implied", "direct_index_spx"]
    : ["es_basis_inferred_spx", "direct_index_spx"];
  if (price <= 0 || !activeSegment || activeSegment.kind === "closed_gap" ||
      !allowedMethods.includes(method) ||
      method !== activeSegment.referenceMethod ||
      provider !== activeSegment.referenceProvider ||
      !instrumentId || !sourceAt || !knownAt ||
      sourceAt.getTime() > knownAt.getTime() ||
      knownAt.getTime() > asOfMs ||
      (acceptedAt && (knownAt.getTime() > acceptedAt.getTime() ||
        acceptedAt.getTime() > asOfMs)) ||
      (validUntil && validUntil.getTime() <= (acceptedAt?.getTime() ?? knownAt.getTime())) ||
      !["ready", "degraded"].includes(quality) || missingReason !== null) {
    throw new Error("invalid_session_surface_reference_contract");
  }
  const needsBasis = method === "es_basis_inferred_spx";
  const basis = needsBasis ? normalizeReferenceBasis(raw.basis, asOfMs) : null;
  if ((!needsBasis && raw.basis !== null) || (needsBasis && !basis)) {
    throw new Error("invalid_session_surface_reference_basis_contract");
  }
  const inferred = method !== "direct_index_spx";
  return {
    raw,
    coordinate: raw.coordinate,
    price,
    method,
    provider,
    instrumentId,
    sourceAtMs: sourceAt.getTime(),
    knownAtMs: knownAt.getTime(),
    acceptedAtMs: acceptedAt?.getTime() ?? null,
    validUntilMs: validUntil?.getTime() ?? null,
    quality,
    missingReason: null,
    basis,
    inferred,
  };
}

function normalizeSurfaceColumn(
  raw,
  bucket,
  asOfMs,
  { mode = "replay", schemaVersion = 1, segment = null, activeSegment = null, segmentsByKind = null } = {},
) {
  if (!isObject(raw)) throw new Error("invalid_session_surface_column");
  const kind = String(raw.kind || "").toLowerCase();
  if (!["historical", "projection", "missing"].includes(kind)) {
    throw new Error("invalid_session_surface_column_kind");
  }
  const sourceAt = raw.source_at === null || raw.source_at === undefined
    ? null
    : parseDate(raw.source_at);
  if ((raw.source_at !== null && raw.source_at !== undefined && !sourceAt) ||
      (sourceAt && sourceAt.getTime() > asOfMs)) {
    throw new Error("session_surface_lookahead_column");
  }
  const knownAt = raw.known_at === null || raw.known_at === undefined
    ? null
    : parseDate(raw.known_at);
  const acceptedAt = raw.accepted_at === null || raw.accepted_at === undefined
    ? null
    : parseDate(raw.accepted_at);
  const sourceFrameSha256 = raw.source_frame_sha256 === null ||
    raw.source_frame_sha256 === undefined
    ? null
    : nonEmptyString(raw.source_frame_sha256);
  if (mode === "live" && kind !== "missing") {
    if (!sourceAt || !knownAt || !acceptedAt ||
        knownAt.getTime() < sourceAt.getTime() ||
        acceptedAt.getTime() < knownAt.getTime() ||
        acceptedAt.getTime() > asOfMs ||
        !sha256String(sourceFrameSha256)) {
      throw new Error("invalid_live_session_surface_column_clock");
    }
  }
  if (kind === "historical" && bucket.endMs > asOfMs) {
    throw new Error("session_surface_historical_after_cutoff");
  }
  if (kind === "historical" && sourceAt && sourceAt.getTime() > bucket.endMs) {
    throw new Error("session_surface_historical_source_after_bucket");
  }
  if (mode === "live" && kind === "historical" && acceptedAt &&
      acceptedAt.getTime() > bucket.endMs) {
    throw new Error("live_session_surface_historical_accepted_after_bucket");
  }
  if (kind === "projection" && bucket.endMs <= asOfMs) {
    throw new Error("session_surface_projection_before_cutoff");
  }
  const validUntil = raw.valid_until === null || raw.valid_until === undefined
    ? null
    : parseDate(raw.valid_until);
  if (raw.valid_until !== null && raw.valid_until !== undefined && !validUntil) {
    throw new Error("invalid_session_surface_column_valid_until");
  }
  if (kind === "historical" && (!validUntil || validUntil.getTime() <= bucket.endMs)) {
    throw new Error("session_surface_historical_ttl_expired_at_bucket");
  }
  if (kind === "projection" && (!validUntil || validUntil.getTime() <= asOfMs)) {
    throw new Error("session_surface_projection_ttl_expired_at_cutoff");
  }
  let sessionKind = null;
  let sourceSessionKind = null;
  let surfaceProvider = null;
  let referenceMethod = null;
  if (schemaVersion === 2) {
    sessionKind = nonEmptyString(raw.session_kind);
    sourceSessionKind = raw.source_session_kind === null ||
      raw.source_session_kind === undefined
      ? null
      : nonEmptyString(raw.source_session_kind);
    surfaceProvider = raw.surface_provider === null
      ? null
      : nonEmptyString(raw.surface_provider);
    referenceMethod = raw.reference_method === null
      ? null
      : nonEmptyString(raw.reference_method);
    const sourceContract = sourceSessionKind
      ? segmentsByKind?.[sourceSessionKind] ?? null
      : null;
    const targetContract = segment?.kind ? segmentsByKind?.[segment.kind] ?? null : null;
    const providerContract = kind === "missing" ? targetContract : sourceContract;
    if (!segment || sessionKind !== segment.kind ||
        (segment.kind === "closed_gap" && kind !== "missing") ||
        (kind === "missing" && sourceSessionKind !== null) ||
        (kind === "historical" && sourceSessionKind !== segment.kind) ||
        (kind === "projection" && (
          !activeSegment || activeSegment.kind === "closed_gap" ||
          sourceSessionKind !== activeSegment.kind
        )) ||
        !providerContract ||
        surfaceProvider !== providerContract.surfaceProvider ||
        referenceMethod !== providerContract.referenceMethod) {
      throw new Error("invalid_session_surface_column_segment_contract");
    }
    if (kind !== "missing" && sourceSessionKind === "gth" &&
        normalizedStatus(raw.quality) !== "degraded") {
      throw new Error("invalid_session_surface_gth_column_quality");
    }
  } else if (["session_kind", "source_session_kind", "surface_provider", "reference_method"]
      .some((field) => Object.prototype.hasOwnProperty.call(raw, field))) {
    throw new Error("session_surface_v1_has_segment_column_contract");
  }
  return {
    raw,
    kind,
    quality: normalizedStatus(raw.quality),
    sourceAtMs: sourceAt?.getTime() ?? null,
    knownAtMs: knownAt?.getTime() ?? null,
    acceptedAtMs: acceptedAt?.getTime() ?? null,
    sourceFrameSha256,
    validUntilMs: validUntil?.getTime() ?? null,
    reason: nonEmptyString(raw.reason) || nonEmptyString(raw.missing_reason),
    sessionKind,
    sourceSessionKind,
    surfaceProvider,
    referenceMethod,
  };
}

function normalizeSessionCandles(
  raw,
  sessionStartMs,
  sessionEndMs,
  asOfMs,
  { schemaVersion = 1, segments = [] } = {},
) {
  if (!Array.isArray(raw)) throw new Error("invalid_session_surface_candles");
  return raw.map((item) => {
    if (!isObject(item)) throw new Error("invalid_session_surface_candle");
    const start = parseDate(item.start_at);
    const end = parseDate(item.end_at);
    const open = finiteNumber(item.open);
    const high = finiteNumber(item.high);
    const low = finiteNumber(item.low);
    const close = finiteNumber(item.close);
    const samples = item.sample_count;
    const sourceAt = item.source_at === undefined || item.source_at === null
      ? null
      : parseDate(item.source_at);
    const knownAt = item.known_at === undefined || item.known_at === null
      ? null
      : parseDate(item.known_at);
    if (
      !start || !end ||
      start.getTime() < sessionStartMs || start.getTime() > asOfMs ||
      end.getTime() <= start.getTime() || end.getTime() > sessionEndMs ||
      [open, high, low, close].some((value) => value === null) ||
      low > Math.min(open, close) || high < Math.max(open, close) || high < low ||
      !Number.isSafeInteger(samples) || samples < 1 ||
      typeof item.complete !== "boolean" ||
      item.complete !== (end.getTime() <= asOfMs) ||
      !sourceAt || sourceAt.getTime() < start.getTime() ||
      sourceAt.getTime() >= end.getTime() || sourceAt.getTime() > asOfMs ||
      !knownAt || knownAt.getTime() < sourceAt.getTime() || knownAt.getTime() > asOfMs
    ) {
      throw new Error("invalid_session_surface_candle_contract");
    }
    const segment = schemaVersion === 2
      ? sessionSegmentAtTime(segments, start.getTime())
      : null;
    let sessionKind = null;
    let referenceMethod = null;
    let referenceProvider = null;
    let referenceInstrumentId = null;
    let acceptedAt = null;
    let validUntil = null;
    let basisValue = null;
    let basisObservedAt = null;
    let renderStyle = "legacy_solid";
    if (schemaVersion === 2) {
      sessionKind = nonEmptyString(item.session_kind);
      referenceMethod = nonEmptyString(item.reference_method);
      referenceProvider = nonEmptyString(item.reference_provider);
      referenceInstrumentId = nonEmptyString(item.reference_instrument_id);
      acceptedAt = parseDate(item.accepted_at);
      validUntil = parseDate(item.valid_until);
      basisValue = finiteOrNull(
        item.basis_value,
        "invalid_session_surface_candle_basis_value",
      );
      basisObservedAt = item.basis_observed_at === null
        ? null
        : parseDate(item.basis_observed_at);
      renderStyle = nonEmptyString(item.render_style);
      const inferred = referenceMethod !== "direct_index_spx";
      const needsBasis = referenceMethod === "es_basis_inferred_spx";
      if (!segment || segment.kind === "closed_gap" || end.getTime() > segment.endMs ||
          sessionKind !== segment.kind || referenceMethod !== segment.referenceMethod ||
          referenceProvider !== segment.referenceProvider ||
          !referenceInstrumentId ||
          (acceptedAt && (acceptedAt.getTime() < knownAt.getTime() ||
            acceptedAt.getTime() > asOfMs)) ||
          (validUntil && validUntil.getTime() <=
            (acceptedAt?.getTime() ?? knownAt.getTime())) ||
          renderStyle !== (inferred ? "inferred_dashed" : "direct_solid") ||
          (needsBasis && (basisValue === null || !basisObservedAt ||
            basisObservedAt.getTime() > sourceAt.getTime())) ||
          (!needsBasis && (basisValue !== null || basisObservedAt !== null))) {
        throw new Error("invalid_session_surface_candle_reference_contract");
      }
    }
    return {
      raw: item,
      startMs: start.getTime(),
      endMs: end.getTime(),
      open,
      high,
      low,
      close,
      sampleCount: samples,
      complete: item.complete,
      sourceAtMs: sourceAt?.getTime() ?? null,
      knownAtMs: knownAt?.getTime() ?? null,
      sessionKind,
      referenceMethod,
      referenceProvider,
      referenceInstrumentId,
      acceptedAtMs: acceptedAt?.getTime() ?? null,
      validUntilMs: validUntil?.getTime() ?? null,
      basisValue,
      basisObservedAtMs: basisObservedAt?.getTime() ?? null,
      renderStyle,
      inferred: renderStyle === "inferred_dashed",
    };
  }).sort((left, right) => left.startMs - right.startMs);
}

function normalizeGammaExtrema(raw, name, count, sign) {
  if (!Array.isArray(raw) || raw.length !== count) {
    throw new Error(`invalid_session_surface_${name}_shape`);
  }
  return raw.map((item) => {
    if (item === null) return null;
    if (!isObject(item)) throw new Error(`invalid_session_surface_${name}`);
    const price = finiteNumber(item.price);
    const value = finiteNumber(item.value);
    if (price === null || price <= 0 || value === null ||
        (sign > 0 && value <= 0) || (sign < 0 && value >= 0)) {
      throw new Error(`invalid_session_surface_${name}`);
    }
    return { raw: item, price, value };
  });
}

function normalizeStrikeProfile(raw) {
  if (!Array.isArray(raw)) throw new Error("invalid_session_surface_strike_profile");
  const rows = raw.map((item) => {
    if (!isObject(item)) throw new Error("invalid_session_surface_strike_row");
    const strike = finiteNumber(item.strike);
    const currentProxy = finiteOrNull(item.current_proxy, "invalid_session_surface_current_proxy");
    const firstValidatedProxy = finiteOrNull(
      item.first_validated_proxy ?? item.baseline_proxy,
      "invalid_session_surface_first_validated_proxy",
    );
    if (strike === null || strike <= 0) throw new Error("invalid_session_surface_strike");
    const currentOpenInterest = finiteOrNull(
      item.current_open_interest,
      "invalid_session_surface_current_open_interest",
    );
    const firstValidatedOpenInterest = finiteOrNull(
      item.first_validated_open_interest ?? item.baseline_open_interest,
      "invalid_session_surface_first_validated_open_interest",
    );
    if (currentOpenInterest < 0 || firstValidatedOpenInterest < 0) {
      throw new Error("negative_session_surface_open_interest");
    }
    return {
      raw: item,
      strike,
      currentProxy,
      firstValidatedProxy,
      currentOpenInterest,
      firstValidatedOpenInterest,
      quality: normalizedStatus(item.quality),
    };
  }).sort((left, right) => left.strike - right.strike);
  if (rows.some((row, index) => index > 0 && row.strike === rows[index - 1].strike)) {
    throw new Error("duplicate_session_surface_strike");
  }
  return rows;
}

function normalizeStrikeProfileMetadata(raw, {
  schemaVersion,
  asOfMs,
  strikeProfile,
  segmentsByKind,
}) {
  if (schemaVersion === 1) {
    if (raw !== undefined && raw !== null && !isObject(raw)) {
      throw new Error("invalid_session_surface_strike_profile_metadata");
    }
    const baselineAt = raw?.baseline_at === null || raw?.baseline_at === undefined
      ? null
      : parseDate(raw.baseline_at);
    const currentAt = raw?.current_at === null || raw?.current_at === undefined
      ? null
      : parseDate(raw.current_at);
    if ((raw?.baseline_at && !baselineAt) || (raw?.current_at && !currentAt) ||
        (baselineAt && baselineAt.getTime() > asOfMs) ||
        (currentAt && currentAt.getTime() > asOfMs)) {
      throw new Error("invalid_session_surface_strike_profile_metadata_clock");
    }
    return {
      raw: raw || null,
      baselineLabel: nonEmptyString(raw?.baseline_label) || "first_validated",
      baselineAt,
      baselineAtMs: baselineAt?.getTime() ?? null,
      baselineSessionKind: null,
      baselineSurfaceProvider: null,
      baselineReferenceMethod: null,
      currentAt,
      currentAtMs: currentAt?.getTime() ?? null,
      currentSessionKind: null,
      currentSurfaceProvider: null,
      currentReferenceMethod: null,
      comparisonSemantics: "snapshot_state_not_position_or_flow",
      baselineUnavailableReason: null,
      exactSodAvailable: false,
      proxyMetric: "signed_gamma",
      contractVerified: false,
    };
  }
  if (!isObject(raw) ||
      raw.baseline_label !== "first_validated_same_segment_provider" ||
      raw.comparison_semantics !== "snapshot_state_not_position_or_flow" ||
      !Object.prototype.hasOwnProperty.call(raw, "baseline_unavailable_reason") ||
      ![null, "gth_contract_universe_completeness_unproven"].includes(
        raw.baseline_unavailable_reason,
      ) ||
      raw.exact_sod_available !== false ||
      raw.missing_join_value !== null ||
      raw.proxy_metric !== "signed_gamma") {
    throw new Error("invalid_session_surface_strike_profile_metadata");
  }
  const normalizeClock = (key) => {
    if (raw[key] === null) return null;
    const at = parseDate(raw[key]);
    if (!at || at.getTime() > asOfMs) {
      throw new Error("invalid_session_surface_strike_profile_metadata_clock");
    }
    return at;
  };
  const baselineAt = normalizeClock("baseline_at");
  const currentAt = normalizeClock("current_at");
  if (baselineAt && currentAt && baselineAt.getTime() > currentAt.getTime()) {
    throw new Error("invalid_session_surface_strike_profile_metadata_clock");
  }
  const normalizeContext = (prefix, at) => {
    const sessionKind = raw[`${prefix}_session_kind`];
    const surfaceProvider = raw[`${prefix}_surface_provider`];
    const referenceMethod = raw[`${prefix}_reference_method`];
    if (!at) {
      if ([sessionKind, surfaceProvider, referenceMethod].some((value) => value !== null)) {
        throw new Error("invalid_session_surface_strike_profile_metadata_context");
      }
      return { sessionKind: null, surfaceProvider: null, referenceMethod: null };
    }
    const contract = segmentsByKind?.[sessionKind];
    if (!contract || sessionKind === "closed_gap" ||
        surfaceProvider !== contract.surfaceProvider ||
        referenceMethod !== contract.referenceMethod) {
      throw new Error("invalid_session_surface_strike_profile_metadata_context");
    }
    return { sessionKind, surfaceProvider, referenceMethod };
  };
  const baseline = normalizeContext("baseline", baselineAt);
  const current = normalizeContext("current", currentAt);
  const baselineUnavailableReason = raw.baseline_unavailable_reason;
  const baselineValues = strikeProfile.some((row) =>
    row.firstValidatedProxy !== null || row.firstValidatedOpenInterest !== null);
  const currentValues = strikeProfile.some((row) =>
    row.currentProxy !== null || row.currentOpenInterest !== null);
  const gthBaselineUnavailable = current.sessionKind === "gth" && !baselineAt;
  if ((baselineValues && !baselineAt) || (currentValues && !currentAt) ||
      (baselineAt && !currentAt) ||
      (current.sessionKind === "gth" && Boolean(baselineAt)) ||
      (gthBaselineUnavailable !==
        (baselineUnavailableReason === "gth_contract_universe_completeness_unproven")) ||
      (!currentAt && baselineUnavailableReason !== null) ||
      (baselineAt && currentAt && (
        baseline.sessionKind !== current.sessionKind ||
        baseline.surfaceProvider !== current.surfaceProvider ||
        baseline.referenceMethod !== current.referenceMethod
      ))) {
    throw new Error("invalid_session_surface_strike_profile_comparison_contract");
  }
  return {
    raw,
    baselineLabel: raw.baseline_label,
    baselineAt,
    baselineAtMs: baselineAt?.getTime() ?? null,
    baselineSessionKind: baseline.sessionKind,
    baselineSurfaceProvider: baseline.surfaceProvider,
    baselineReferenceMethod: baseline.referenceMethod,
    currentAt,
    currentAtMs: currentAt?.getTime() ?? null,
    currentSessionKind: current.sessionKind,
    currentSurfaceProvider: current.surfaceProvider,
    currentReferenceMethod: current.referenceMethod,
    comparisonSemantics: raw.comparison_semantics,
    baselineUnavailableReason,
    exactSodAvailable: false,
    proxyMetric: raw.proxy_metric,
    contractVerified: true,
  };
}

function strikeProfileDomains(strikeProfile) {
  return {
    gamma: robustDomain(
      strikeProfile.flatMap((row) => [row.currentProxy, row.firstValidatedProxy]),
    ),
    openInterest: robustDomain(
      strikeProfile.flatMap((row) => [
        row.currentOpenInterest,
        row.firstValidatedOpenInterest,
      ]),
    ),
  };
}

function normalizeMissingRanges(raw, sessionStartMs, sessionEndMs) {
  if (raw === undefined || raw === null) return [];
  if (!Array.isArray(raw)) throw new Error("invalid_session_surface_missing_ranges");
  return raw.map((item) => {
    if (!isObject(item)) throw new Error("invalid_session_surface_missing_range");
    const start = parseDate(item.start_at);
    const end = parseDate(item.end_at);
    if (!start || !end || start.getTime() < sessionStartMs ||
        end.getTime() > sessionEndMs || end.getTime() <= start.getTime()) {
      throw new Error("invalid_session_surface_missing_range_clock");
    }
    return {
      raw: item,
      startMs: start.getTime(),
      endMs: end.getTime(),
      reason: nonEmptyString(item.reason) || "missing",
      components: Array.isArray(item.components)
        ? item.components.map(nonEmptyString).filter(Boolean)
        : nonEmptyString(item.component)
          ? [nonEmptyString(item.component)]
          : [],
    };
  });
}

function suppliedColorDomain(raw, metric) {
  if (!isObject(raw)) return null;
  return raw[metric] ?? raw[`${metric}_surface`] ?? null;
}

function normalizeSessionMetricUnits(raw) {
  if (!isObject(raw)) throw new Error("invalid_session_surface_metric_units");
  const result = {};
  for (const metric of ["signed_gamma", "gross_gamma", "charm", "vanna"]) {
    const unit = nonEmptyString(raw[metric]);
    if (!unit || !UNIT_LABELS[unit]) {
      throw new Error("invalid_session_surface_metric_units");
    }
    result[metric] = unit;
  }
  return result;
}

function sessionMetricUnitLabel(surface, metric) {
  const unit = surface?.metricUnits?.[metric];
  return UNIT_LABELS[unit] || unit || "unknown unit";
}

async function normalizeSessionSurface(raw, expected = {}) {
  if (!isObject(raw)) throw new Error("session_surface_not_an_object");
  const mode = expected.mode === "live" ? "live" : "replay";
  const schemaVersion = raw.schema_version;
  const policyVersion = SESSION_SURFACE_POLICY_VERSIONS.get(`${mode}:${schemaVersion}`);
  const providerValid = schemaVersion === 2
    ? mode === "replay"
      ? raw.provider === "mixed"
      : ["schwab", "ibkr", "mixed", "unavailable"].includes(raw.provider)
    : mode === "replay"
      ? raw.provider === "schwab"
      : ["schwab", "ibkr", "mixed", "unavailable"].includes(raw.provider);
  if (
    !SESSION_SURFACE_SCHEMA_VERSIONS.has(schemaVersion) ||
    raw.kind !== SESSION_SURFACE_KIND ||
    raw.policy_version !== policyVersion ||
    raw.mode !== mode ||
    !providerValid ||
    raw.trading_class !== "SPXW" ||
    raw.coordinate !== "SPX"
  ) {
    throw new Error("invalid_session_surface_identity_contract");
  }
  const artifactBody = { ...raw };
  delete artifactBody.artifact_sha256;
  const artifactSha256 = await canonicalReplaySha256(artifactBody);
  if (!sha256String(raw.artifact_sha256) || raw.artifact_sha256 !== artifactSha256) {
    throw new Error("session_surface_artifact_hash_mismatch");
  }
  const asOf = parseDate(raw.as_of);
  const sessionStart = parseDate(raw.session_start);
  const sessionEnd = parseDate(raw.session_end);
  if (!asOf || !sessionStart || !sessionEnd ||
      sessionStart.getTime() >= sessionEnd.getTime() ||
      asOf.getTime() < sessionStart.getTime() || asOf.getTime() > sessionEnd.getTime()) {
    throw new Error("invalid_session_surface_clock_contract");
  }
  if (mode === "replay" && expected.at instanceof Date &&
      asOf.getTime() !== expected.at.getTime()) {
    throw new Error("unexpected_session_surface_as_of");
  }
  let status = "ready";
  let liveStatus = null;
  let createdAt = null;
  let acceptedAt = null;
  let sourceAsOf = null;
  let validUntil = null;
  let historyFrozenThrough = null;
  let accumulatorStartedAt = null;
  let serverTime = null;
  let availability = null;
  if (mode === "replay") {
    for (const field of [
      "created_at",
      "server_time",
      "accepted_at",
      "source_as_of",
      "valid_until",
      "live_status",
      "availability",
      "history_frozen_through",
      "accumulator_started_at",
    ]) {
      if (Object.prototype.hasOwnProperty.call(raw, field)) {
        throw new Error("replay_session_surface_has_live_contract");
      }
    }
  } else {
    status = normalizedStatus(raw.status || raw.quality);
    liveStatus = nonEmptyString(raw.live_status);
    createdAt = parseDate(raw.created_at);
    serverTime = parseDate(raw.server_time);
    acceptedAt = raw.accepted_at === null ? null : parseDate(raw.accepted_at);
    sourceAsOf = raw.source_as_of === null ? null : parseDate(raw.source_as_of);
    validUntil = raw.valid_until === null ? null : parseDate(raw.valid_until);
    historyFrozenThrough = raw.history_frozen_through === null
      ? null
      : parseDate(raw.history_frozen_through);
    accumulatorStartedAt = parseDate(raw.accumulator_started_at);
    availability = raw.availability;
    if (!LIVE_STATUS_VALUES.has(liveStatus) ||
        !["ready", "degraded", "unavailable"].includes(status) ||
        raw.automatic_ordering !== false ||
        [
          "source_as_of",
          "accepted_at",
          "valid_until",
          "history_frozen_through",
          "spot",
          "spot_source_at",
          "spot_known_at",
        ].some((field) => !Object.prototype.hasOwnProperty.call(raw, field)) ||
        !createdAt || !serverTime || !accumulatorStartedAt ||
        !isObject(availability) ||
        [
          "projection_available",
          "current_strike_profile_available",
          "current_spot_available",
          "historical_surface_available",
        ].some((field) => typeof availability[field] !== "boolean")) {
      throw new Error("invalid_live_session_surface_root_contract");
    }
    const acceptedMs = acceptedAt?.getTime() ?? null;
    const sourceAsOfMs = sourceAsOf?.getTime() ?? null;
    const validUntilMs = validUntil?.getTime() ?? null;
    const frozenMs = historyFrozenThrough?.getTime() ?? null;
    if (
      accumulatorStartedAt.getTime() > createdAt.getTime() ||
      createdAt.getTime() < asOf.getTime() ||
      serverTime.getTime() < createdAt.getTime() ||
      serverTime.getTime() < asOf.getTime() ||
      (acceptedMs !== null && (
        acceptedMs > asOf.getTime() || acceptedMs > createdAt.getTime()
      )) ||
      (sourceAsOfMs !== null && (
        acceptedMs === null || sourceAsOfMs > acceptedMs
      )) ||
      (validUntilMs !== null && (
        acceptedMs === null || validUntilMs <= acceptedMs
      )) ||
      (frozenMs !== null && (
        frozenMs < sessionStart.getTime() ||
        frozenMs > Math.min(asOf.getTime(), sessionEnd.getTime())
      ))
    ) {
      throw new Error("invalid_live_session_surface_clock_contract");
    }
    const neverAccepted = acceptedAt === null;
    if (neverAccepted !== (validUntil === null) ||
        neverAccepted !== (sourceAsOf === null) ||
        (neverAccepted && historyFrozenThrough !== null) ||
        (neverAccepted && Object.values(availability).some(Boolean))) {
      throw new Error("invalid_live_session_surface_availability_clock");
    }
  }
  const sessionDate = nonEmptyString(raw.session_date);
  if (!sessionDate || !/^\d{4}-\d{2}-\d{2}$/.test(sessionDate) ||
      (expected.sessionDate && sessionDate !== expected.sessionDate)) {
    throw new Error("invalid_session_surface_session_date");
  }
  const role = nonEmptyString(raw.role);
  const weighting = nonEmptyString(raw.weighting);
  const expiry = nonEmptyString(raw.expiry);
  if (!role || !["front", "next"].includes(role) ||
      !weighting || !WEIGHTINGS[weighting] ||
      !expiry || !/^\d{8}$/.test(expiry) ||
      (expected.role && role !== expected.role) ||
      (expected.weighting && weighting !== expected.weighting)) {
    throw new Error("invalid_session_surface_selector_contract");
  }
  const bucketMinutes = finiteNumber(raw.bucket_minutes) ?? SESSION_SURFACE_BUCKET_MINUTES;
  const priceStep = finiteNumber(raw.price_step) ?? SESSION_SURFACE_PRICE_STEP;
  if (
    !SESSION_SURFACE_BUCKET_MINUTES_ALLOWED.has(bucketMinutes) ||
    !SESSION_SURFACE_PRICE_STEPS_ALLOWED.has(priceStep) ||
    (expected.bucketMinutes !== undefined && bucketMinutes !== expected.bucketMinutes) ||
    (expected.priceStep !== undefined && priceStep !== expected.priceStep)
  ) {
    throw new Error("invalid_session_surface_grid_policy");
  }
  if (!Array.isArray(raw.price_grid) || raw.price_grid.length < 2) {
    throw new Error("invalid_session_surface_price_grid");
  }
  const priceGrid = raw.price_grid.map((value) => {
    const parsed = finiteNumber(value);
    if (parsed === null || parsed <= 0) throw new Error("invalid_session_surface_price");
    return parsed;
  });
  if (priceGrid.some((value, index) => index > 0 && value <= priceGrid[index - 1])) {
    throw new Error("invalid_session_surface_price_order");
  }
  const expectedPriceCount = Math.round(2 * SESSION_SURFACE_PRICE_EXTENT_POINTS / priceStep) + 1;
  if (priceGrid.length !== expectedPriceCount || priceGrid.some((value, index) =>
    index > 0 && Math.abs(value - priceGrid[index - 1] - priceStep) > 1e-9)) {
    throw new Error("invalid_session_surface_price_grid_policy");
  }
  const timeBuckets = normalizeSessionTimeBuckets(
    raw.time_buckets,
    sessionStart.getTime(),
    sessionEnd.getTime(),
    bucketMinutes,
  );
  const sessionSegments = normalizeSessionSegments(
    raw.session_segments,
    sessionStart.getTime(),
    sessionEnd.getTime(),
    schemaVersion,
    mode,
  );
  const segmentsByKind = Object.fromEntries(
    sessionSegments.map((segment) => [segment.kind, segment]),
  );
  const providers = normalizeSessionProviders(raw.providers, schemaVersion, mode);
  if (mode === "live" && schemaVersion === 2 && providers && (
      providers.gthSurface !== segmentsByKind.gth?.surfaceProvider ||
      providers.gthReference !== segmentsByKind.gth?.referenceProvider ||
      providers.rthSurface !== segmentsByKind.rth?.surfaceProvider ||
      providers.rthReference !== segmentsByKind.rth?.referenceProvider)) {
    throw new Error("invalid_session_surface_providers_contract");
  }
  const activeSegment = schemaVersion === 2
    ? sessionSegmentAtTime(sessionSegments, asOf.getTime())
    : null;
  if (!Array.isArray(raw.surface_columns) || raw.surface_columns.length !== timeBuckets.length) {
    throw new Error("invalid_session_surface_columns_shape");
  }
  const surfaceColumns = raw.surface_columns.map((item, index) => {
    const segment = schemaVersion === 2
      ? sessionSegmentForBucket(sessionSegments, timeBuckets[index])
      : null;
    if (schemaVersion === 2 && !segment) {
      throw new Error("session_surface_bucket_crosses_segment");
    }
    return normalizeSurfaceColumn(item, timeBuckets[index], asOf.getTime(), {
      mode,
      schemaVersion,
      segment,
      activeSegment,
      segmentsByKind,
    });
  });
  const gamma = normalizeSessionMatrix(
    raw.gamma_surface,
    "gamma",
    timeBuckets.length,
    priceGrid.length,
  );
  const charm = normalizeSessionMatrix(
    raw.charm_surface,
    "charm",
    timeBuckets.length,
    priceGrid.length,
  );
  const vanna = normalizeSessionMatrix(
    raw.vanna_surface,
    "vanna",
    timeBuckets.length,
    priceGrid.length,
  );
  const grossGamma = normalizeSessionMatrix(
    raw.gross_gamma_surface,
    "gross_gamma",
    timeBuckets.length,
    priceGrid.length,
  );
  if (grossGamma?.some((row) => row.some((value) => value !== null && value < 0))) {
    throw new Error("negative_session_surface_gross_gamma");
  }
  if (!Array.isArray(raw.zero_ridges) || raw.zero_ridges.length !== timeBuckets.length) {
    throw new Error("invalid_session_surface_zero_ridges");
  }
  const zeroRidges = raw.zero_ridges.map((value) =>
    finiteOrNull(value, "invalid_session_surface_zero_ridge"));
  const gammaPositivePeaks = normalizeGammaExtrema(
    raw.gamma_positive_peaks,
    "gamma_positive_peaks",
    timeBuckets.length,
    1,
  );
  const gammaNegativeTroughs = normalizeGammaExtrema(
    raw.gamma_negative_troughs,
    "gamma_negative_troughs",
    timeBuckets.length,
    -1,
  );
  surfaceColumns.forEach((column, index) => {
    if (column.kind !== "missing") return;
    for (const matrix of [gamma, grossGamma, charm, vanna]) {
      if (matrix[index].some((value) => value !== null)) {
        throw new Error("session_surface_missing_column_has_values");
      }
    }
    if (zeroRidges[index] !== null || gammaPositivePeaks[index] !== null ||
        gammaNegativeTroughs[index] !== null) {
      throw new Error("session_surface_missing_column_has_extrema");
    }
  });
  const candles = normalizeSessionCandles(
    raw.candles,
    sessionStart.getTime(),
    sessionEnd.getTime(),
    asOf.getTime(),
    { schemaVersion, segments: sessionSegments },
  );
  const strikeProfile = normalizeStrikeProfile(raw.strike_profile);
  const strikeProfileMetadata = normalizeStrikeProfileMetadata(
    raw.strike_profile_metadata,
    { schemaVersion, asOfMs: asOf.getTime(), strikeProfile, segmentsByKind },
  );
  const spot = finiteOrNull(raw.spot, "invalid_session_surface_spot");
  if (spot !== null && spot <= 0) throw new Error("invalid_session_surface_spot");
  const spotSourceAt = raw.spot_source_at === null ? null : parseDate(raw.spot_source_at);
  const spotKnownAt = raw.spot_known_at === null ? null : parseDate(raw.spot_known_at);
  let reference = null;
  if (schemaVersion === 2) {
    reference = normalizeSessionReference(raw.reference, asOf.getTime(), activeSegment, mode);
    if ((reference.price === null) !== (spot === null) ||
        (reference.price !== null && Math.abs(reference.price - spot) > 1e-9)) {
      throw new Error("session_surface_reference_spot_mismatch");
    }
  } else if (Object.prototype.hasOwnProperty.call(raw, "reference")) {
    throw new Error("session_surface_v1_has_reference_contract");
  }
  if (mode === "replay" && schemaVersion === 1 && (!spot || !spotSourceAt || !spotKnownAt)) {
    throw new Error("invalid_session_surface_spot");
  }
  if (mode === "replay" && schemaVersion === 2 && spot !== null &&
      (!spotSourceAt || !spotKnownAt)) {
    throw new Error("invalid_session_surface_spot");
  }
  if ((spotSourceAt && spotSourceAt.getTime() > asOf.getTime()) ||
      (spotKnownAt && spotKnownAt.getTime() > asOf.getTime())) {
    throw new Error("session_surface_lookahead_spot");
  }
  if (mode === "live") {
    const hasSpotContract = spot !== null && spotSourceAt && spotKnownAt;
    if (availability.current_spot_available !== Boolean(hasSpotContract) ||
        (hasSpotContract && sourceAsOf && spotSourceAt.getTime() > sourceAsOf.getTime()) ||
        (hasSpotContract && acceptedAt && spotKnownAt.getTime() !== acceptedAt.getTime())) {
      throw new Error("invalid_live_session_surface_spot_availability");
    }
    const hasCurrentStrike = strikeProfile.some((row) =>
      row.currentProxy !== null || row.currentOpenInterest !== null);
    if (availability.current_strike_profile_available !== hasCurrentStrike ||
        (!availability.current_strike_profile_available && strikeProfile.some((row) =>
          row.currentProxy !== null || row.currentOpenInterest !== null))) {
      throw new Error("invalid_live_session_surface_strike_availability");
    }
    const frozenMs = historyFrozenThrough?.getTime() ?? sessionStart.getTime();
    const dynamicUnavailable = !availability.projection_available;
    if (dynamicUnavailable) {
      surfaceColumns.forEach((column, index) => {
        if (timeBuckets[index].endMs <= frozenMs) return;
        if (column.kind !== "missing" ||
            [gamma, grossGamma, charm, vanna].some((matrix) =>
              matrix[index].some((value) => value !== null)) ||
            zeroRidges[index] !== null || gammaPositivePeaks[index] !== null ||
            gammaNegativeTroughs[index] !== null) {
          throw new Error("live_session_surface_unavailable_projection_has_values");
        }
      });
    }
    const hasHistorical = surfaceColumns.some((column, index) =>
      column.kind === "historical" && timeBuckets[index].endMs <= frozenMs);
    if (availability.historical_surface_available !== hasHistorical) {
      throw new Error("invalid_live_session_surface_historical_availability");
    }
    const terminal = ["lease_expired", "closed", "unavailable", "initializing"].includes(liveStatus);
    if (terminal && (
      availability.projection_available ||
      availability.current_strike_profile_available ||
      availability.current_spot_available
    )) {
      throw new Error("invalid_live_session_surface_terminal_availability");
    }
    if ((availability.projection_available || availability.current_spot_available ||
         availability.current_strike_profile_available) && !validUntil) {
      throw new Error("invalid_live_session_surface_dynamic_lease");
    }
  }
  const capabilities = raw.capabilities;
  if (!isObject(capabilities) ||
      capabilities.proxy_position_available !== true ||
      capabilities.participant_position_available !== false ||
      capabilities.open_close_available !== false ||
      capabilities.signed_flow_available !== false ||
      capabilities.dealer_position_sign_available !== false ||
      capabilities.strict_point_in_time_available !== (mode === "live") ||
      capabilities.known_clock_no_lookahead !== true ||
      capabilities.projection_is_model_scenario !== true ||
      capabilities.historical_surface_is_model_proxy !== true ||
      (schemaVersion === 2 && (
        typeof capabilities.gth_available !== "boolean" ||
        capabilities.gth_complete_chain_available !== false ||
        capabilities.official_spx_ohlc_available !== false
      ))) {
    throw new Error("invalid_session_surface_capabilities_contract");
  }
  const provenance = raw.provenance;
  if (!isObject(provenance) ||
      provenance.lookahead_rows_selected !== 0 ||
      provenance.point_in_time_confidence !==
        (mode === "live" ? "observed_live" : "bounded_not_proven") ||
      provenance.availability_clock_available !== (mode === "live") ||
      provenance.availability_clock !== (mode === "live" ? "accepted_at" : "unavailable") ||
      (mode === "live" && (
        provenance.per_leg_availability_clock_available !== false ||
        !sha256String(provenance.frozen_history_prefix_sha256) ||
        (sourceAsOf && parseDate(provenance.source_as_of)?.getTime() !== sourceAsOf.getTime()) ||
        (!sourceAsOf && provenance.source_as_of !== null)
      ))) {
    throw new Error("invalid_session_surface_provenance_contract");
  }
  const colorDomains = isObject(raw.color_domains) ? raw.color_domains : {};
  const metricUnits = normalizeSessionMetricUnits(raw.metric_units);
  const strikeDomains = strikeProfileDomains(strikeProfile);
  const domains = {
    gamma: robustDomain(gamma, suppliedColorDomain(colorDomains, "gamma")),
    charm: robustDomain(charm, suppliedColorDomain(colorDomains, "charm")),
    vanna: vanna ? robustDomain(vanna, suppliedColorDomain(colorDomains, "vanna")) : null,
    grossGamma: grossGamma
      ? robustDomain(grossGamma, suppliedColorDomain(colorDomains, "gross_gamma"))
      : null,
    strikeGamma: strikeDomains.gamma,
    strikeOpenInterest: strikeDomains.openInterest,
  };
  return {
    raw,
    mode,
    status,
    liveStatus,
    provider: raw.provider,
    providers,
    kind: nonEmptyString(raw.kind),
    schemaVersion,
    policyVersion,
    sessionDate,
    asOf,
    asOfMs: asOf.getTime(),
    sessionStart,
    sessionStartMs: sessionStart.getTime(),
    sessionEnd,
    sessionEndMs: sessionEnd.getTime(),
    expiry,
    role,
    weighting,
    bucketMinutes,
    priceStep,
    priceGrid,
    timeBuckets,
    sessionSegments,
    surfaceColumns,
    gamma,
    charm,
    vanna,
    grossGamma,
    zeroRidges,
    gammaPositivePeaks,
    gammaNegativeTroughs,
    candles,
    strikeProfile,
    strikeProfileMetadata,
    spot,
    spotSourceAtMs: spotSourceAt?.getTime() ?? null,
    spotKnownAtMs: spotKnownAt?.getTime() ?? null,
    reference,
    createdAt,
    createdAtMs: createdAt?.getTime() ?? null,
    acceptedAt,
    acceptedAtMs: acceptedAt?.getTime() ?? null,
    sourceAsOf,
    sourceAsOfMs: sourceAsOf?.getTime() ?? null,
    validUntil,
    validUntilMs: validUntil?.getTime() ?? null,
    historyFrozenThrough,
    historyFrozenThroughMs: historyFrozenThrough?.getTime() ?? null,
    accumulatorStartedAt,
    accumulatorStartedAtMs: accumulatorStartedAt?.getTime() ?? null,
    serverTime,
    serverTimeMs: serverTime?.getTime() ?? null,
    availability,
    metricUnits,
    domains,
    artifactSha256,
    capabilities,
    provenance,
    missingRanges: normalizeMissingRanges(
      raw.missing_ranges,
      sessionStart.getTime(),
      sessionEnd.getTime(),
    ),
  };
}

function liveSurfaceIdentity(surface) {
  if (!surface) return "";
  return [
    surface.schemaVersion,
    surface.policyVersion,
    surface.sessionDate,
    surface.sessionStartMs,
    surface.sessionEndMs,
    surface.expiry,
    surface.role,
    surface.weighting,
    surface.bucketMinutes,
    surface.priceStep,
    surface.priceGrid.join(","),
    surface.timeBuckets.map((bucket) => `${bucket.startMs}:${bucket.endMs}`).join(","),
    surface.sessionSegments.map((segment) =>
      `${segment.kind}:${segment.startMs}:${segment.endMs}:${segment.surfaceProvider}:${segment.referenceProvider}:${segment.referenceMethod}`
    ).join(","),
    surface.providers ? JSON.stringify({
      gthSurface: surface.providers.gthSurface,
      gthReference: surface.providers.gthReference,
      rthSurface: surface.providers.rthSurface,
      rthReference: surface.providers.rthReference,
    }) : "v1",
  ].join("|");
}

function liveFrozenPrefixSignature(surface, throughMs = surface?.historyFrozenThroughMs) {
  if (!surface || !Number.isFinite(throughMs)) return "";
  const indexes = surface.timeBuckets
    .map((bucket, index) => bucket.endMs <= throughMs ? index : -1)
    .filter((index) => index >= 0);
  return JSON.stringify({
    columns: indexes.map((index) => {
      const column = surface.surfaceColumns[index];
      return [
        column.kind,
        column.quality,
        column.sourceAtMs,
        column.knownAtMs,
        column.acceptedAtMs,
        column.sourceFrameSha256,
        column.validUntilMs,
        column.reason,
        column.sessionKind,
        column.surfaceProvider,
        column.referenceMethod,
      ];
    }),
    gamma: indexes.map((index) => surface.gamma[index]),
    grossGamma: indexes.map((index) => surface.grossGamma[index]),
    charm: indexes.map((index) => surface.charm[index]),
    vanna: indexes.map((index) => surface.vanna[index]),
    zeroRidges: indexes.map((index) => surface.zeroRidges[index]),
    positive: indexes.map((index) => surface.gammaPositivePeaks[index]),
    negative: indexes.map((index) => surface.gammaNegativeTroughs[index]),
    candles: surface.candles
      .filter((candle) => candle.endMs <= throughMs)
      .map((candle) => [
        candle.startMs,
        candle.endMs,
        candle.open,
        candle.high,
        candle.low,
        candle.close,
        candle.sampleCount,
        candle.sourceAtMs,
        candle.knownAtMs,
        candle.sessionKind,
        candle.referenceMethod,
        candle.referenceProvider,
        candle.referenceInstrumentId,
        candle.acceptedAtMs,
        candle.validUntilMs,
        candle.basisValue,
        candle.basisObservedAtMs,
        candle.renderStyle,
      ]),
    baseline: surface.strikeProfile
      .filter((row) => row.firstValidatedProxy !== null ||
        row.firstValidatedOpenInterest !== null)
      .map((row) => [
        row.strike,
        row.firstValidatedProxy,
        row.firstValidatedOpenInterest,
      ]),
  });
}

function liveSurfaceTransitionIssue(previous, next) {
  if (!previous || previous.mode !== "live") return null;
  if (!next || next.mode !== "live") return "live_surface_mode_changed";
  if (next.schemaVersion !== previous.schemaVersion) return null;
  if (next.sessionDate < previous.sessionDate) return "live_surface_session_regressed";
  if (next.sessionDate !== previous.sessionDate) return null;
  if (liveSurfaceIdentity(previous) !== liveSurfaceIdentity(next)) {
    return "live_surface_grid_or_selector_drift";
  }
  if (next.asOfMs < previous.asOfMs ||
      next.createdAtMs < previous.createdAtMs ||
      next.serverTimeMs < previous.serverTimeMs ||
      (previous.sourceAsOfMs !== null &&
       (next.sourceAsOfMs === null || next.sourceAsOfMs < previous.sourceAsOfMs)) ||
      (previous.acceptedAtMs !== null &&
       (next.acceptedAtMs === null || next.acceptedAtMs < previous.acceptedAtMs)) ||
      (previous.historyFrozenThroughMs !== null &&
       (next.historyFrozenThroughMs === null ||
        next.historyFrozenThroughMs < previous.historyFrozenThroughMs))) {
    return "live_surface_clock_regressed";
  }
  if (next.accumulatorStartedAtMs !== previous.accumulatorStartedAtMs) {
    return "live_surface_accumulator_identity_changed";
  }
  if (previous.historyFrozenThroughMs !== null &&
      liveFrozenPrefixSignature(previous, previous.historyFrozenThroughMs) !==
      liveFrozenPrefixSignature(next, previous.historyFrozenThroughMs)) {
    return "live_surface_frozen_prefix_changed";
  }
  if (previous.historyFrozenThroughMs === next.historyFrozenThroughMs &&
      previous.provenance.frozen_history_prefix_sha256 !==
      next.provenance.frozen_history_prefix_sha256) {
    return "live_surface_frozen_prefix_hash_changed";
  }
  return null;
}

function liveSurfaceDisplayState(surface, serverNowMs) {
  if (!surface || surface.mode !== "live" || !Number.isFinite(serverNowMs)) {
    return "unavailable";
  }
  const dynamicAvailable = surface.availability?.projection_available === true ||
    surface.availability?.current_strike_profile_available === true ||
    surface.availability?.current_spot_available === true;
  const leaseFresh = Number.isFinite(surface.validUntilMs) && serverNowMs < surface.validUntilMs;
  if (dynamicAvailable && leaseFresh &&
      ["ready", "degraded"].includes(surface.liveStatus) &&
      ["ready", "degraded"].includes(surface.status)) {
    return "fresh";
  }
  if (!dynamicAvailable && surface.availability?.historical_surface_available === true &&
      ["degraded", "lease_expired", "closed", "unavailable"].includes(surface.liveStatus)) {
    return "historical_only";
  }
  if (dynamicAvailable && !leaseFresh) return "expired";
  return "unavailable";
}

function historicalOnlyLiveSurface(surface) {
  if (!surface || surface.mode !== "live" ||
      surface.availability?.historical_surface_available !== true ||
      !Number.isFinite(surface.historyFrozenThroughMs)) {
    return null;
  }
  const frozenColumn = surface.timeBuckets.map((bucket) =>
    bucket.endMs <= surface.historyFrozenThroughMs);
  const maskMatrix = (matrix) => matrix.map((row, index) =>
    frozenColumn[index] ? row : row.map(() => null));
  const surfaceColumns = surface.surfaceColumns.map((column, index) => frozenColumn[index]
    ? column
    : {
        ...column,
        kind: "missing",
        quality: "unavailable",
        sourceAtMs: null,
        knownAtMs: null,
        acceptedAtMs: null,
        sourceFrameSha256: null,
        validUntilMs: null,
        reason: "live_lease_expired_client_mask",
      });
  const gamma = maskMatrix(surface.gamma);
  const grossGamma = maskMatrix(surface.grossGamma);
  const charm = maskMatrix(surface.charm);
  const vanna = maskMatrix(surface.vanna);
  const strikeProfile = surface.strikeProfile.map((row) => ({
    ...row,
    currentProxy: null,
    currentOpenInterest: null,
  }));
  const strikeProfileMetadata = {
    ...surface.strikeProfileMetadata,
    currentAt: null,
    currentAtMs: null,
    currentSessionKind: null,
    currentSurfaceProvider: null,
    currentReferenceMethod: null,
  };
  const strikeDomains = strikeProfileDomains(strikeProfile);
  return {
    ...surface,
    status: "degraded",
    liveStatus: surface.liveStatus === "closed" ? "closed" : "lease_expired",
    surfaceColumns,
    gamma,
    grossGamma,
    charm,
    vanna,
    zeroRidges: surface.zeroRidges.map((value, index) => frozenColumn[index] ? value : null),
    gammaPositivePeaks: surface.gammaPositivePeaks.map((value, index) =>
      frozenColumn[index] ? value : null),
    gammaNegativeTroughs: surface.gammaNegativeTroughs.map((value, index) =>
      frozenColumn[index] ? value : null),
    candles: surface.candles.filter((candle) =>
      candle.endMs <= surface.historyFrozenThroughMs),
    strikeProfile,
    strikeProfileMetadata,
    spot: null,
    spotSourceAtMs: null,
    spotKnownAtMs: null,
    reference: null,
    availability: {
      ...surface.availability,
      projection_available: false,
      current_strike_profile_available: false,
      current_spot_available: false,
      historical_surface_available: true,
    },
    domains: {
      gamma: robustDomain(gamma),
      charm: robustDomain(charm),
      vanna: robustDomain(vanna),
      grossGamma: robustDomain(grossGamma),
      strikeGamma: strikeDomains.gamma,
      strikeOpenInterest: strikeDomains.openInterest,
    },
    clientLeaseMasked: true,
    sourceArtifactSha256: surface.artifactSha256,
  };
}

function liveServerTimeFromHeaders(headers, requestStartedAtMs, receivedAtMs) {
  if (!headers || typeof headers.get !== "function" ||
      !Number.isFinite(requestStartedAtMs) || !Number.isFinite(receivedAtMs) ||
      receivedAtMs < requestStartedAtMs) {
    return null;
  }
  const exact = nonEmptyString(headers.get("X-SPXW-Server-Time"));
  const fallback = nonEmptyString(headers.get("Date"));
  const parsed = parseDate(exact || fallback);
  if (!parsed) return null;
  const wholeSecondSafetyMs = exact ? 0 : 1_000;
  return parsed.getTime() + wholeSecondSafetyMs;
}

function liveServerTimeMatchesArtifact(headers, artifactServerTimeMs) {
  if (!headers || typeof headers.get !== "function" ||
      !Number.isFinite(artifactServerTimeMs)) return false;
  const exact = parseDate(headers.get("X-SPXW-Server-Time"));
  if (exact) return exact.getTime() === artifactServerTimeMs;
  const fallback = parseDate(headers.get("Date"));
  return Boolean(fallback) && Math.abs(fallback.getTime() - artifactServerTimeMs) <= 2_000;
}

function conservativeLiveServerNow(surface, headerServerTimeMs, requestElapsedMs) {
  if (!surface || !Number.isFinite(surface.serverTimeMs) ||
      !Number.isFinite(surface.asOfMs) || !Number.isFinite(headerServerTimeMs) ||
      !Number.isFinite(requestElapsedMs) || requestElapsedMs < 0) {
    return null;
  }
  // During RTH, as_of is sampled at request dispatch and server_time after
  // projection.  Removing that measured backend interval from the full client
  // round trip leaves a conservative transport/serialization upper bound.
  // Adding it prevents a response that arrived after an exclusive lease from
  // being treated as fresh merely because the signed server clock is older.
  const backendElapsedMs = Math.max(surface.serverTimeMs - surface.asOfMs, 0);
  const transportAndClientMs = Math.max(requestElapsedMs - backendElapsedMs, 0);
  return Math.max(surface.serverTimeMs, headerServerTimeMs) + transportAndClientMs;
}

function effectiveSnapshotStatus(snapshot) {
  if (!snapshot) return "unavailable";
  if (!["ready", "degraded"].includes(snapshot.status)) return "unavailable";
  if (snapshot.mode === "replay") {
    if (snapshot.frozen !== true || snapshot.validUntil !== null) return "unavailable";
    if (snapshot.source?.lookahead_rows_selected !== 0) return "unavailable";
    return snapshot.status;
  }
  if (!snapshot.validUntil) return "unavailable";
  if (snapshot.validUntil && Date.now() > snapshot.validUntil.getTime()) return "unavailable";
  return snapshot.status;
}

function selectedExpiry() {
  return app.snapshot?.expiries.find((item) => item.role === app.expiryRole) || null;
}

function metricArray(weighting, metric) {
  if (!isObject(weighting)) return null;
  const metrics = isObject(weighting.metrics) ? weighting.metrics : weighting;
  for (const key of METRICS[metric].aliases) {
    if (Array.isArray(metrics[key])) return metrics[key];
  }
  return null;
}

function metricScalar(container, metric) {
  if (!isObject(container)) return null;
  const metrics = isObject(container.metrics) ? container.metrics : container;
  for (const key of METRICS[metric].aliases) {
    const value = finiteNumber(metrics[key]);
    if (value !== null) return value;
  }
  return null;
}

function weightingFromSlice(slice, weightingKey) {
  const weightings = isObject(slice.raw.weightings) ? slice.raw.weightings : {};
  return isObject(weightings[weightingKey]) ? weightings[weightingKey] : null;
}

function surfaceView(expiry, weightingKey, metricKey) {
  const surface = expiry?.surface;
  if (!surface) return null;
  const rows = surface.timeSlices.map((slice) => {
    const weighting = weightingFromSlice(slice, weightingKey);
    const values = metricArray(weighting, metricKey);
    const reordered = surface.spotOrder.map(({ index }) => finiteNumber(values?.[index]));
    const quality = normalizedStatus(weighting?.quality || slice.raw.quality);
    const metricShapeValid = Array.isArray(values) &&
      values.length === surface.spotGrid.length &&
      values.every((value) => value === null || finiteNumber(value) !== null);
    return {
      minutesForward: slice.minutesForward,
      tauSeconds: slice.tauSeconds,
      values: reordered,
      weighting,
      quality,
      warnings: [...reasonsFrom(weighting?.quality), ...reasonsFrom(weighting), ...slice.warnings],
      zeroRidgeSpot: metricKey === "signed_gamma" ? finiteNumber(weighting?.zero_ridge_spot) : null,
      positivePeak: metricKey === "signed_gamma" && isObject(weighting?.positive_peak)
        ? {
            spot: finiteNumber(weighting.positive_peak.spot),
            value: finiteNumber(weighting.positive_peak.value),
          }
        : null,
      negativeTrough: metricKey === "signed_gamma" && isObject(weighting?.negative_trough)
        ? {
            spot: finiteNumber(weighting.negative_trough.spot),
            value: finiteNumber(weighting.negative_trough.value),
          }
        : null,
      coverage: isObject(weighting?.coverage) ? weighting.coverage : {},
      shapeValid: metricShapeValid || (values === null && quality === "unavailable"),
    };
  });
  const numericCount = rows.reduce(
    (total, row) => total + row.values.filter((value) => value !== null).length,
    0,
  );
  const invalidCount = metricKey === "gross_gamma"
    ? rows.reduce(
        (total, row) => total + row.values.filter((value) => value !== null && value < 0).length,
        0,
      )
    : 0;
  const shapeValid = rows.every((row) => row.shapeValid);
  return {
    surface,
    spots: surface.spotGrid,
    rows,
    numericCount,
    invalidCount,
    shapeValid,
    unit: metricUnit(surface, metricKey),
  };
}

function metricUnit(surface, metricKey) {
  for (const key of METRICS[metricKey].aliases) {
    const value = nonEmptyString(surface.metricUnits[key]);
    if (value) return UNIT_LABELS[value] || "model proxy units";
  }
  return "model units";
}

function qualityRank(status) {
  return { ready: 0, unknown: 1, degraded: 2, unavailable: 3 }[status] ?? 1;
}

function worstQuality(statuses) {
  return statuses.reduce(
    (worst, status) => (qualityRank(status) > qualityRank(worst) ? status : worst),
    "ready",
  );
}

function activeSurfaceQuality(expiry, view) {
  if (!expiry || !view || !view.shapeValid || view.numericCount === 0 || view.invalidCount > 0) {
    return "unavailable";
  }
  // A degraded near-expiry surface can intentionally omit later time slices.
  // Keep the rectangular grid and render those cells as missing instead of
  // hiding the valid observed slices that remain.
  const rowQualities = view.rows.map((row) =>
    row.quality === "unavailable" ? "degraded" : row.quality);
  return worstQuality([
    expiry.quality,
    expiry.surface?.quality || "unknown",
    ...rowQualities,
  ]);
}

function formatExpiry(value) {
  const text = String(value || "");
  if (!/^\d{8}$/.test(text)) return text || "—";
  const date = new Date(`${text.slice(0, 4)}-${text.slice(4, 6)}-${text.slice(6, 8)}T12:00:00Z`);
  return new Intl.DateTimeFormat("zh-CN", { month: "short", day: "numeric" }).format(date);
}

function formatDateTime(date) {
  if (!(date instanceof Date)) return "—";
  return new Intl.DateTimeFormat("zh-CN", {
    month: "2-digit",
    day: "2-digit",
    hour: "2-digit",
    minute: "2-digit",
    second: "2-digit",
    hour12: false,
  }).format(date);
}

function formatIsoUtc(date) {
  if (!(date instanceof Date)) return "—";
  return date.toISOString().replace(".000Z", "Z");
}

function isReplayView() {
  return app.mode === "replay";
}

function replayFrame() {
  return app.frameIndex >= 0 ? app.frames[app.frameIndex] || null : null;
}

function legacyReplayFrame() {
  return app.legacyFrameIndex >= 0
    ? app.legacyFrames[app.legacyFrameIndex] || null
    : null;
}

function legacyReplayFrameIndexFor(frames, playheadMs) {
  if (!Number.isFinite(playheadMs) || !Array.isArray(frames) || !frames.length) return -1;
  let index = binarySearchLastAtOrBefore(
    frames,
    playheadMs,
    (frame) => frame.at.getTime(),
  );
  while (index >= 0 && frames[index].cached !== true) index -= 1;
  return index;
}

function legacyReplayFrameIndexAtOrBefore(playheadMs = app.playheadMs) {
  return legacyReplayFrameIndexFor(app.legacyFrames, playheadMs);
}

function formatMarketTime(date, includeDate = true) {
  if (!(date instanceof Date)) return "—";
  const options = {
    timeZone: MARKET_TIME_ZONE,
    hour: "2-digit",
    minute: "2-digit",
    second: "2-digit",
    hour12: false,
  };
  if (includeDate) {
    options.month = "2-digit";
    options.day = "2-digit";
  }
  return `${new Intl.DateTimeFormat("en-US", options).format(date)} ET`;
}

function formatReplayAsOf(date) {
  if (!(date instanceof Date)) return "waiting for an audited frame";
  return `${formatMarketTime(date)} · ${formatIsoUtc(date)} UTC`;
}

function viewPath(mode) {
  const current = window.location.pathname;
  const base = current.replace(/\/(?:index\.html|live|replay|friday)\/?$/, "/");
  const root = base.replace(/\/$/, "");
  return `${root}/${mode}` || `/${mode}`;
}

function replayPlayheadQueryClock() {
  return Number.isFinite(app.playheadMs)
    ? { at: new Date(app.playheadMs) }
    : replayFrame();
}

function updateModeQuery(frame = replayPlayheadQueryClock(), { push = false } = {}) {
  const url = new URL(window.location.href);
  url.pathname = viewPath(app.mode);
  url.searchParams.delete("view");
  if (isReplayView()) {
    if (app.sessionDate) url.searchParams.set("date", app.sessionDate);
    else url.searchParams.delete("date");
    if (frame?.at) url.searchParams.set("at", formatIsoUtc(frame.at));
    else url.searchParams.delete("at");
  } else {
    url.searchParams.delete("date");
    url.searchParams.delete("at");
  }
  const target = `${url.pathname}${url.search}${url.hash}`;
  const current = `${window.location.pathname}${window.location.search}${window.location.hash}`;
  if (target === current) return;
  window.history[push ? "pushState" : "replaceState"](null, "", target);
}

function updateModeChrome() {
  const replay = isReplayView();
  const frame = replayFrame();
  const verifiedTrend = replay && app.trend && !app.trend.metadataOnly;
  const verifiedSessionSurface = app.sessionSurface;
  const verifiedFrame = replay && app.snapshot?.mode === "replay";
  const replayAt = Number.isFinite(app.playheadMs)
    ? new Date(app.playheadMs)
    : app.snapshot?.mode === "replay" ? app.snapshot.requestedAsOf : frame?.at;
  document.body.classList.toggle("mode-replay", replay);
  document.body.classList.toggle("mode-live", !replay);
  dom.trendPanel.hidden = true;
  dom.sessionCockpit.hidden = false;
  dom.cockpitTimeline.hidden = !replay;
  dom.modeFilter.value = replay ? "replay" : "live";
  dom.replayBanner.hidden = !replay;
  dom.replayConsole.hidden = !replay;
  updateLiveViewportChrome();
  dom.pageLede.textContent = replay
    ? "Gamma、Strike 与 Charm 共用一个 SPX 坐标和交易时钟；参考价格方法、provider 与缺失区间均来自冻结回放合同。"
    : "完整 Session Canvas 由服务端保留；Live 视图自动跟随最近 90 分钟与未来 30 分钟，GTH 推断参考和 RTH 直接参考严格分标。";
  const sourceFiles = verifiedSessionSurface
    ? app.sessionSurface.provenance?.source_files
    : verifiedTrend
    ? app.trend.source?.source_files
    : verifiedFrame ? app.snapshot.source?.source_files : null;
  dom.sourceFile.textContent = (Array.isArray(sourceFiles) ? sourceFiles.join(", ") : null) ||
    (replay
      ? (frame?.id ? `replay frame ${frame.id}` : "replay catalog")
      : "live session accumulator");
  dom.sourceMode.textContent = replay
    ? verifiedSessionSurface
      ? `CUTOFF SESSION SURFACE · Gamma + Strike + Charm · latest validated frame ≤ playhead · Bounded PIT · Not live`
      : verifiedTrend
      ? `COMPACT TREND ARTIFACT VERIFIED · Visual ${REPLAY_VISUAL_FPS} fps · SPX recorded known_at + Gamma valid_until · Bounded PIT · Not live`
      : `SESSION REPLAY CLOCK · timeline metadata only · market values fetched per validated cutoff · Bounded PIT · Not live`
    : verifiedSessionSurface
      ? `LIVE SESSION SURFACE · ${String(app.sessionSurface.liveStatus).toUpperCase()} · accepted_at clock · read-only`
      : "LIVE SESSION SURFACE · waiting for a validated lease";
  if (replay) {
    if (verifiedSessionSurface) {
      dom.replayBannerLabel.textContent = `Replay · ${formatMarketTime(app.sessionSurface.asOf)} · Session surface`;
      dom.replayBannerAsOf.textContent = `As of ${formatReplayAsOf(app.sessionSurface.asOf)} · latest validated frame at or before playhead · no future chain, candle, or position values requested`;
    } else if (verifiedTrend) {
      dom.replayBannerLabel.textContent = replayAt
        ? `Replay · ${formatMarketTime(replayAt)} · Compact trend verified`
        : "Replay · Compact trend verified";
      dom.replayBannerAsOf.textContent = `As of ${formatReplayAsOf(replayAt)} · compact trend artifact verified · SPX recorded known_at + Gamma valid_until · availability clock missing, PIT bounded not proven`;
    } else {
      const dataAsOf = verifiedFrame ? formatIsoUtc(app.snapshot.dataAsOf) : "等待校验";
      const auditState = verifiedFrame
        ? `${app.snapshot.source.lookahead_rows_selected} selected lookahead rows`
        : "frame contract not yet verified";
      dom.replayBannerLabel.textContent = replayAt
        ? `Replay · ${formatMarketTime(replayAt)}`
        : "Replay · select an audited frame";
      dom.replayBannerAsOf.textContent = `As of ${formatReplayAsOf(replayAt)} · data_as_of ${dataAsOf} · received/source clock cutoff · ${auditState} · availability clock missing, PIT bounded not proven`;
    }
  }
}

function formatAge(seconds) {
  if (!Number.isFinite(seconds) || seconds < 0) return "—";
  if (seconds < 60) return `${Math.round(seconds)}s`;
  if (seconds < 3600) return `${Math.floor(seconds / 60)}m ${Math.round(seconds % 60)}s`;
  return `${Math.floor(seconds / 3600)}h ${Math.round((seconds % 3600) / 60)}m`;
}

function compactNumber(value, digits = 2) {
  if (!Number.isFinite(value)) return "—";
  const absolute = Math.abs(value);
  const scales = [
    [1e12, "T"],
    [1e9, "B"],
    [1e6, "M"],
    [1e3, "K"],
  ];
  for (const [scale, suffix] of scales) {
    if (absolute >= scale) return `${(value / scale).toFixed(digits)}${suffix}`;
  }
  if (absolute >= 100) return value.toFixed(0);
  if (absolute >= 1) return value.toFixed(digits);
  if (absolute === 0) return "0";
  return value.toExponential(2);
}

function ratioValue(coverage) {
  const explicit = finiteNumber(coverage.ratio);
  if (explicit !== null) return explicit;
  const usableRatio = finiteNumber(coverage.usable_ratio);
  if (usableRatio !== null) return usableRatio;
  const usable = finiteNumber(coverage.usable_contracts);
  const total = finiteNumber(coverage.total_contracts);
  return usable !== null && total && total > 0 ? usable / total : null;
}

function currentCoverage(view, expiry) {
  const nearest = view?.rows.reduce((best, row) => {
    if (!best) return row;
    return Math.abs(row.minutesForward) < Math.abs(best.minutesForward) ? row : best;
  }, null);
  const expiryCoverage = isObject(expiry?.raw?.coverage) ? expiry.raw.coverage : null;
  const coverage = expiryCoverage || nearest?.coverage || {};
  return {
    ratio: ratioValue(coverage),
    usable: finiteNumber(coverage.usable_contracts),
    total: finiteNumber(coverage.total_contracts) ?? expiry?.contractCount,
  };
}

function setStatusPill(status, label = null) {
  const normalized = STATUS_LABELS[status] ? status : "unknown";
  dom.statusPill.className = `status-pill status-${normalized}`;
  dom.statusPill.textContent = label || STATUS_LABELS[normalized];
}

function setQualityChip(status) {
  const normalized = STATUS_LABELS[status] ? status : "unknown";
  dom.surfaceQuality.className = `quality-chip quality-${normalized}`;
  dom.surfaceQuality.textContent = STATUS_LABELS[normalized];
}

function setNotice(message, isError = false) {
  dom.notice.hidden = !message;
  dom.notice.textContent = message || "";
  dom.notice.classList.toggle("error", isError);
}

function updateFilters() {
  const expiries = app.snapshot?.expiries || [];
  dom.expiryFilter.replaceChildren();
  for (const role of ["front", "next"]) {
    const expiry = expiries.find((item) => item.role === role)?.expiry ||
      (app.sessionSurface?.role === role ? app.sessionSurface.expiry : null) ||
      (app.trend?.role === role ? app.trend.gamma.keyframes[0]?.expiry : null);
    const option = document.createElement("option");
    option.value = role;
    option.textContent = expiry
      ? `${role.toUpperCase()} · ${formatExpiry(expiry)} · ${expiry}`
      : `${role.toUpperCase()} · ${role === "front" ? "0DTE" : "next trading day"}`;
    dom.expiryFilter.append(option);
  }
  app.expiryRole = ["front", "next"].includes(app.expiryRole) ? app.expiryRole : "front";
  app.expiry = app.expiryRole;
  dom.expiryFilter.value = app.expiryRole;
  const enabled = isReplayView()
    ? app.frames.length > 0 &&
      sha256String(app.timelineSha256) &&
      sha256String(app.surfaceTimelineSha256) &&
      sha256String(app.sourceFingerprint) &&
      !app.trendLoading
    : app.mode === "live";
  dom.expiryFilter.disabled = !enabled;
  dom.weightingFilter.disabled = !enabled;
  dom.metricFilter.disabled = !enabled || !dom.scenarioDiagnostic.open;
  dom.weightingFilter.value = app.weighting;
  dom.metricFilter.value = app.metric;
}

function renderSummary() {
  const snapshot = app.snapshot;
  if (!snapshot) return;
  const replay = isReplayView() && snapshot.mode === "replay";
  const status = effectiveSnapshotStatus(snapshot);
  const expired = !replay && snapshot.validUntil && Date.now() > snapshot.validUntil.getTime();
  setStatusPill(status, replay ? `Replay · ${STATUS_LABELS[status]}` : expired ? "Stale" : null);
  dom.summaryStatus.textContent = replay
    ? `${STATUS_LABELS[status]} · Bounded PIT`
    : expired ? "Stale / fail closed" : STATUS_LABELS[status];
  const expiry = selectedExpiry();
  if (app.sessionSurface) renderReferenceChrome(app.sessionSurface);
  else dom.providerChip.textContent = expiry?.providers.includes("schwab") ? "SCHWAB" : "Provider —";
  const activeReasons = [...new Set([...snapshot.reasons, ...(expiry?.warnings || [])])];
  const reasons = expired ? ["snapshot_valid_until_elapsed", ...activeReasons] : activeReasons;
  const displayedReasons = reasons.map(reasonLabel);
  dom.summaryReasons.textContent = displayedReasons.slice(0, 3).join(" · ") || "无质量警告";

  const ageSeconds = snapshot.asOf ? Math.max((Date.now() - snapshot.asOf.getTime()) / 1000, 0) : null;
  dom.summaryFreshness.textContent = replay
    ? "Frozen · Bounded PIT"
    : expired ? `过期 ${formatAge(ageSeconds)}` : formatAge(ageSeconds);
  dom.summaryAsOf.textContent = replay
    ? `as of ${formatReplayAsOf(snapshot.requestedAsOf)}`
    : `as of ${formatDateTime(snapshot.asOf)}`;

  const view = surfaceView(expiry, app.weighting, app.metric);
  const coverage = currentCoverage(view, expiry);
  dom.summaryCoverage.textContent = coverage.ratio === null ? "—" : `${(coverage.ratio * 100).toFixed(1)}%`;
  dom.summaryContracts.textContent = coverage.usable === null
    ? `合约 ${coverage.total ?? "—"}`
    : `可用 ${coverage.usable} / ${coverage.total ?? "—"}`;

  const front = snapshot.expiries.find((item) => item.role === "front");
  const next = snapshot.expiries.find((item) => item.role === "next");
  dom.summaryExpiries.textContent = front
    ? `${formatExpiry(front.expiry)}${next ? ` → ${formatExpiry(next.expiry)}` : ""}`
    : "—";
  const underlierPrice = finiteNumber(snapshot.underlier.price);
  const underlierSource = nonEmptyString(snapshot.underlier.source) || "unknown source";
  dom.summaryUnderlier.textContent = underlierPrice === null
    ? `SPX — · ${underlierSource}`
    : `SPX ${underlierPrice.toFixed(2)} · ${underlierSource}`;

  dom.schemaVersion.textContent = `schema ${snapshot.schemaVersion}`;
  const sign = expiry?.surface?.signConvention === "calls_positive_puts_negative"
    ? "calls + / puts − proxy"
    : "sign convention unavailable";
  const dealer = expiry?.surface?.dealerPositionSign === "unknown"
    ? "dealer side unknown"
    : "dealer side not asserted";
  dom.signConvention.textContent = `${sign}; ${dealer}`;
  dom.refreshState.textContent = replay
    ? `Frozen replay · loaded ${new Date().toLocaleTimeString("zh-CN", { hour12: false })}`
    : `最近检查 ${new Date().toLocaleTimeString("zh-CN", { hour12: false })}`;
  updateModeChrome();

  if (expired) {
    setNotice("快照已超过 valid_until，所有曲面已清空；等待 publisher 产生新数据。", true);
  } else if (status === "unavailable") {
    setNotice(`${replay ? "历史回放" : "生产快照"}不可用${displayedReasons.length ? `：${displayedReasons.slice(0, 4).join(" · ")}` : ""}`, true);
  } else if (status === "degraded") {
    setNotice(`${replay ? "历史回放为降级视图" : "当前为降级视图"}${displayedReasons.length ? `：${displayedReasons.slice(0, 4).join(" · ")}` : ""}`);
  } else if (displayedReasons.length) {
    setNotice(`${replay ? "历史回放质量提示" : "当前数据质量提示"}：${displayedReasons.slice(0, 4).join(" · ")}`);
  } else {
    setNotice("");
  }
}

function render() {
  updateFilters();
  renderSummary();
  renderVisuals();
}

function renderVisuals() {
  const snapshot = app.snapshot;
  const replay = isReplayView() && snapshot?.mode === "replay";
  const expiry = selectedExpiry();
  const metric = METRICS[app.metric];
  const view = surfaceView(expiry, app.weighting, app.metric);
  const snapshotStatus = effectiveSnapshotStatus(snapshot);
  const quality = snapshotStatus === "unavailable" ? "unavailable" : activeSurfaceQuality(expiry, view);
  setQualityChip(quality);

  const role = expiry?.role === "front" ? "Front" : expiry?.role === "next" ? "Next" : "Expiry";
  dom.surfaceTitle.textContent = expiry
    ? `${replay ? "Replay · " : ""}${role} ${expiry.expiry} · ${metric.label}`
    : "Spot × Time surface";
  const semanticsText = WEIGHTING_DESCRIPTIONS[app.weighting] || WEIGHTINGS[app.weighting];
  dom.surfaceSubtitle.textContent = expiry
    ? `${replay ? `SESSION REPLAY · Frozen · Not live · as of ${formatReplayAsOf(snapshot.requestedAsOf)} · ` : ""}${semanticsText} · X: SPX scenario spot · Y: minutes forward · ${view?.unit || "model units"}`
    : replay ? "等待冻结历史回放" : "等待生产快照";

  if (!dom.scenarioDiagnostic.open) return;

  const available = snapshotStatus !== "unavailable" && quality !== "unavailable" && view?.numericCount > 0;
  dom.heatmapEmpty.hidden = available;
  if (!available) {
    clearCanvas();
    clearLadder();
    renderExtrema(null);
    updateLegend(null);
    dom.accessibleSummary.textContent = "当前没有可用的曲面数据。";
    return;
  }

  drawHeatmap(view);
  drawLadder(expiry, view);
  renderExtrema(view);
}

function clearCanvas() {
  const context = dom.heatmap.getContext("2d");
  context.clearRect(0, 0, dom.heatmap.width, dom.heatmap.height);
  app.chartHit = null;
  dom.heatmapTooltip.hidden = true;
}

function resizeCanvas(canvas) {
  const width = Math.max(canvas.clientWidth, 320);
  const height = Math.max(canvas.clientHeight, 360);
  const ratio = Math.min(window.devicePixelRatio || 1, 2);
  const pixelWidth = Math.round(width * ratio);
  const pixelHeight = Math.round(height * ratio);
  if (canvas.width !== pixelWidth || canvas.height !== pixelHeight) {
    canvas.width = pixelWidth;
    canvas.height = pixelHeight;
  }
  const context = canvas.getContext("2d");
  context.setTransform(ratio, 0, 0, ratio, 0, 0);
  return { context, width, height };
}

function updateCockpitStableDomains(surface) {
  const key = `${surface.sessionDate}|${surface.role}|${surface.weighting}`;
  for (const metric of ["gamma", "charm", "strikeGamma", "strikeOpenInterest"]) {
    app.cockpitColorDomains[metric] = expandOnlyDomain(
      app.cockpitColorDomains[metric],
      surface.domains[metric],
      `${key}|${metric}`,
    );
  }
  // Lock the visible Y-axis to the session grid. Candles and spot may be clipped
  // at the edge, but they must never make the replay coordinate system jump.
  const candidate = {
    key,
    ...sessionGridPriceDomain(surface),
  };
  if (!app.cockpitPriceDomain || app.cockpitPriceDomain.key !== key) {
    app.cockpitPriceDomain = candidate;
  }
}

function sessionGridPriceDomain(surface) {
  const padding = Math.max(surface.priceStep, 1);
  return {
    min: surface.priceGrid[0] - padding,
    max: surface.priceGrid.at(-1) + padding,
  };
}

function divergingColor(value, domain, { projection = false } = {}) {
  if (!Number.isFinite(value)) return "rgba(69, 85, 97, 0.58)";
  const absolute = Math.abs(value);
  const maximum = Math.max(domain?.maxAbs || 0, 1e-12);
  const threshold = Math.min(domain?.threshold || 0, maximum);
  const opacityScale = projection ? 0.72 : 1;
  if (absolute <= threshold) return `rgba(220, 226, 228, ${0.72 * opacityScale})`;
  const ratio = Math.sqrt(Math.max(0, Math.min((absolute - threshold) / Math.max(maximum - threshold, 1e-12), 1)));
  if (value < 0) {
    return `rgba(${Math.round(198 + ratio * 30)}, ${Math.round(133 - ratio * 45)}, ${Math.round(125 - ratio * 40)}, ${(0.34 + ratio * 0.58) * opacityScale})`;
  }
  return `rgba(${Math.round(107 - ratio * 72)}, ${Math.round(164 - ratio * 33)}, ${Math.round(190 - ratio * 21)}, ${(0.34 + ratio * 0.58) * opacityScale})`;
}

function cockpitElements(panel) {
  if (panel === "gamma") {
    return {
      stage: dom.cockpitGammaStage,
      base: dom.cockpitGammaBase,
      overlay: dom.cockpitGammaOverlay,
      empty: dom.cockpitGammaEmpty,
    };
  }
  if (panel === "charm") {
    return {
      stage: dom.cockpitCharmStage,
      base: dom.cockpitCharmBase,
      overlay: dom.cockpitCharmOverlay,
      empty: dom.cockpitCharmEmpty,
    };
  }
  return {
    stage: dom.cockpitStrikeStage,
    base: dom.cockpitStrikeBase,
    overlay: dom.cockpitStrikeOverlay,
    empty: dom.cockpitStrikeEmpty,
  };
}

function resizeCockpitPanel(panel) {
  const elements = cockpitElements(panel);
  const width = Math.max(elements.stage?.clientWidth || 0, 240);
  const height = Math.max(elements.stage?.clientHeight || 0, 360);
  const ratio = Math.min(window.devicePixelRatio || 1, 2);
  for (const canvas of [elements.base, elements.overlay]) {
    const pixelWidth = Math.round(width * ratio);
    const pixelHeight = Math.round(height * ratio);
    if (canvas.width !== pixelWidth || canvas.height !== pixelHeight) {
      canvas.width = pixelWidth;
      canvas.height = pixelHeight;
    }
    canvas.getContext("2d").setTransform(ratio, 0, 0, ratio, 0, 0);
  }
  const narrow = width < 430;
  const horizontal = panel === "strike"
    ? { left: narrow ? 37 : 45, right: narrow ? 37 : 45 }
    : { left: narrow ? 8 : 12, right: narrow ? 42 : 50 };
  const layout = {
    panel,
    width,
    height,
    ratio,
    plotLeft: horizontal.left,
    plotRight: width - horizontal.right,
    plotTop: 14,
    plotBottom: height - 29,
  };
  layout.plotWidth = Math.max(layout.plotRight - layout.plotLeft, 1);
  layout.plotHeight = Math.max(layout.plotBottom - layout.plotTop, 1);
  app.cockpitLayouts[panel] = layout;
  return { ...elements, layout };
}

const cockpitHatchPatterns = new Map();

function cockpitHatchPattern(context, color) {
  let pattern = cockpitHatchPatterns.get(color);
  if (pattern) return pattern;
  const tile = document.createElement("canvas");
  tile.width = 7;
  tile.height = 7;
  const tileContext = tile.getContext("2d");
  tileContext.strokeStyle = color;
  tileContext.lineWidth = 0.7;
  tileContext.beginPath();
  tileContext.moveTo(0, 7);
  tileContext.lineTo(7, 0);
  tileContext.stroke();
  pattern = context.createPattern(tile, "repeat");
  cockpitHatchPatterns.set(color, pattern);
  return pattern;
}

function hatchCockpitRect(context, x, y, width, height, color = "rgba(189, 207, 217, 0.24)") {
  if (width <= 0 || height <= 0) return;
  context.save();
  context.fillStyle = cockpitHatchPattern(context, color);
  context.fillRect(x, y, width, height);
  context.restore();
}

function cockpitPriceBounds(surface, index) {
  const price = surface.priceGrid[index];
  const lower = index === 0
    ? price - (surface.priceGrid[1] - price) / 2
    : (surface.priceGrid[index - 1] + price) / 2;
  const upper = index === surface.priceGrid.length - 1
    ? price + (price - surface.priceGrid[index - 1]) / 2
    : (price + surface.priceGrid[index + 1]) / 2;
  return { lower, upper };
}

function drawCockpitTimeAxes(context, layout, surface) {
  const ticks = layout.width < 470 ? 4 : 6;
  const window = activeCockpitTimeWindow(surface);
  context.save();
  context.font = "8px ui-monospace, SFMono-Regular, monospace";
  context.fillStyle = "#91a9b9";
  context.strokeStyle = "rgba(128, 166, 189, 0.17)";
  context.lineWidth = 0.7;
  context.textBaseline = "top";
  for (let index = 0; index < ticks; index += 1) {
    const ratio = index / (ticks - 1);
    const at = window.startMs + ratio * (window.endMs - window.startMs);
    const x = layout.plotLeft + ratio * layout.plotWidth;
    context.beginPath();
    context.moveTo(x + 0.5, layout.plotTop);
    context.lineTo(x + 0.5, layout.plotBottom);
    context.stroke();
    context.textAlign = index === 0 ? "left" : index === ticks - 1 ? "right" : "center";
    context.fillText(formatAxisMarketTime(at), x, layout.plotBottom + 8);
  }
  context.restore();
}

function drawCockpitPriceAxes(context, layout, priceDomain, { side = "right" } = {}) {
  const ticks = layout.height < 500 ? 6 : 9;
  context.save();
  context.font = "8px ui-monospace, SFMono-Regular, monospace";
  context.fillStyle = "#9eb2c0";
  context.strokeStyle = "rgba(128, 166, 189, 0.14)";
  context.lineWidth = 0.7;
  context.textBaseline = "middle";
  for (let index = 0; index < ticks; index += 1) {
    const ratio = index / (ticks - 1);
    const price = priceDomain.max - ratio * (priceDomain.max - priceDomain.min);
    const y = layout.plotTop + ratio * layout.plotHeight;
    context.beginPath();
    context.moveTo(layout.plotLeft, y + 0.5);
    context.lineTo(layout.plotRight, y + 0.5);
    context.stroke();
    context.textAlign = side === "right" ? "left" : "right";
    const labelX = side === "right" ? layout.plotRight + 4 : layout.plotLeft - 4;
    context.fillText(price.toFixed(0), labelX, y);
  }
  context.restore();
}

function drawCockpitMissingRange(context, layout, surface, startMs, endMs) {
  const visible = cockpitVisibleTimeRange(surface, startMs, endMs);
  if (!visible) return;
  const x1 = cockpitTimeToX(layout, surface, visible.startMs);
  const x2 = cockpitTimeToX(layout, surface, visible.endMs);
  context.fillStyle = "rgba(39, 56, 70, 0.72)";
  context.fillRect(x1, layout.plotTop, Math.max(x2 - x1, 0.5), layout.plotHeight);
  hatchCockpitRect(context, x1, layout.plotTop, Math.max(x2 - x1, 0.5), layout.plotHeight);
}

function drawCockpitSessionSegmentBackgrounds(context, layout, surface) {
  if (!surface.sessionSegments.length) return;
  const fills = {
    gth: "rgba(42, 104, 139, 0.13)",
    closed_gap: "rgba(91, 68, 40, 0.64)",
    rth: "rgba(35, 105, 88, 0.07)",
  };
  context.save();
  for (const segment of surface.sessionSegments) {
    const visible = cockpitVisibleTimeRange(surface, segment.startMs, segment.endMs);
    if (!visible) continue;
    const x1 = cockpitTimeToX(layout, surface, visible.startMs);
    const x2 = cockpitTimeToX(layout, surface, visible.endMs);
    const width = Math.max(x2 - x1, 0.75);
    context.fillStyle = fills[segment.kind];
    context.fillRect(x1, layout.plotTop, width, layout.plotHeight);
    if (segment.kind === "closed_gap") {
      hatchCockpitRect(
        context,
        x1,
        layout.plotTop,
        width,
        layout.plotHeight,
        "rgba(238, 190, 103, 0.48)",
      );
    }
  }
  context.restore();
}

function drawCockpitSessionSegmentBoundaries(context, layout, surface) {
  if (!surface.sessionSegments.length) return;
  const window = activeCockpitTimeWindow(surface);
  context.save();
  context.font = "bold 8px ui-monospace, SFMono-Regular, monospace";
  context.textBaseline = "top";
  for (const [index, segment] of surface.sessionSegments.entries()) {
    const visible = cockpitVisibleTimeRange(surface, segment.startMs, segment.endMs);
    if (!visible) continue;
    const boundaryVisible = segment.startMs >= window.startMs && segment.startMs <= window.endMs;
    const x = cockpitTimeToX(layout, surface, Math.max(segment.startMs, window.startMs));
    if (index > 0 && boundaryVisible) {
      context.setLineDash(segment.kind === "rth" ? [] : [3, 3]);
      context.strokeStyle = segment.kind === "rth"
        ? "rgba(98, 213, 190, 0.92)"
        : "rgba(238, 190, 103, 0.9)";
      context.lineWidth = segment.kind === "rth" ? 1.25 : 1;
      context.beginPath();
      context.moveTo(x, layout.plotTop);
      context.lineTo(x, layout.plotBottom);
      context.stroke();
    }
    if (segment.kind !== "closed_gap") {
      context.setLineDash([]);
      context.fillStyle = segment.kind === "gth" ? "#8fc7e1" : "#8fe3ca";
      context.textAlign = "left";
      context.fillText(segment.kind.toUpperCase(), Math.min(x + 4, layout.plotRight - 22), layout.plotTop + 2);
    }
  }
  context.restore();
}

function missingRangeAppliesToPanel(range, panel) {
  if (!range.components.length) return true;
  const aliases = panel === "gamma"
    ? new Set(["gamma", "gamma_surface", "signed_gamma", "surface"])
    : new Set(["charm", "charm_surface", "surface"]);
  return range.components.some((component) => aliases.has(component));
}

function drawCockpitCandles(context, layout, surface) {
  const window = activeCockpitTimeWindow(surface);
  const visible = surface.candles.filter((candle) => {
    const displayAtMs = cockpitCandleDisplayTime(candle, surface.asOfMs);
    return candle.startMs <= surface.asOfMs &&
      displayAtMs >= window.startMs && displayAtMs <= window.endMs;
  });
  if (!visible.length) return;
  const visibleBucketCount = Math.max(
    Math.round((window.endMs - window.startMs) / (surface.bucketMinutes * 60_000)),
    1,
  );
  const nominalWidth = Math.max(
    Math.min(layout.plotWidth / visibleBucketCount * 0.58, 5),
    1.2,
  );
  context.save();
  for (const candle of visible) {
    context.save();
    if (candle.inferred) {
      context.globalAlpha = 0.56;
      context.setLineDash([3, 2]);
    } else {
      context.globalAlpha = 1;
      context.setLineDash([]);
    }
    const displayAtMs = cockpitCandleDisplayTime(candle, surface.asOfMs);
    const x = cockpitTimeToX(layout, surface, displayAtMs);
    const highY = sessionPriceToY(layout, app.cockpitPriceDomain, candle.high);
    const lowY = sessionPriceToY(layout, app.cockpitPriceDomain, candle.low);
    const openY = sessionPriceToY(layout, app.cockpitPriceDomain, candle.open);
    const closeY = sessionPriceToY(layout, app.cockpitPriceDomain, candle.close);
    const rising = candle.close >= candle.open;
    const color = rising ? "#63d0ad" : "#ef7b6e";
    if (!candle.inferred) {
      context.strokeStyle = "rgba(5, 15, 24, 0.78)";
      context.lineWidth = 3;
      context.beginPath();
      context.moveTo(x, highY);
      context.lineTo(x, lowY);
      context.stroke();
    }
    context.strokeStyle = color;
    context.lineWidth = 1;
    if (candle.inferred) context.setLineDash([3, 2]);
    context.beginPath();
    context.moveTo(x, highY);
    context.lineTo(x, lowY);
    context.stroke();
    const top = Math.min(openY, closeY);
    const height = Math.max(Math.abs(closeY - openY), 1);
    context.fillStyle = candle.inferred
      ? rising ? "rgba(34, 116, 91, 0.34)" : "rgba(142, 57, 55, 0.34)"
      : rising ? "rgba(34, 116, 91, 0.86)" : "rgba(142, 57, 55, 0.86)";
    context.strokeStyle = color;
    context.lineWidth = 0.8;
    context.fillRect(x - nominalWidth / 2, top, nominalWidth, height);
    context.strokeRect(x - nominalWidth / 2, top, nominalWidth, height);
    if (!candle.complete) {
      context.setLineDash([2, 2]);
      context.strokeStyle = "#f0cf72";
      context.strokeRect(x - nominalWidth / 2 - 1, top - 1, nominalWidth + 2, height + 2);
      context.setLineDash([]);
    }
    context.restore();
  }
  context.restore();
}

function cockpitCandleDisplayTime(candle, asOfMs) {
  const centerMs = candle.startMs + (candle.endMs - candle.startMs) / 2;
  return Math.min(centerMs, asOfMs);
}

function cockpitCandleAtTime(candles, timeMs, asOfMs) {
  if (!Number.isFinite(timeMs) || timeMs > asOfMs) return null;
  return candles.find((item) => item.startMs <= timeMs && timeMs < item.endMs) || null;
}

function drawGammaExtremaMarkers(context, layout, surface) {
  const window = activeCockpitTimeWindow(surface);
  const definitions = [
    { rows: surface.gammaPositivePeaks, symbol: "+", color: "#8fe3ca" },
    { rows: surface.gammaNegativeTroughs, symbol: "−", color: "#ff9a91" },
  ];
  context.save();
  context.font = "bold 8px ui-monospace, SFMono-Regular, monospace";
  context.textAlign = "center";
  context.textBaseline = "middle";
  for (const definition of definitions) {
    definition.rows.forEach((marker, index) => {
      if (!marker || marker.price < app.cockpitPriceDomain.min ||
          marker.price > app.cockpitPriceDomain.max) return;
      const centerMs = surface.timeBuckets[index].centerMs;
      if (centerMs < window.startMs || centerMs > window.endMs) return;
      const x = cockpitTimeToX(layout, surface, centerMs);
      const y = sessionPriceToY(layout, app.cockpitPriceDomain, marker.price);
      context.beginPath();
      context.arc(x, y, 4.2, 0, Math.PI * 2);
      context.fillStyle = "rgba(5, 20, 32, 0.9)";
      context.fill();
      context.strokeStyle = definition.color;
      context.lineWidth = 1;
      context.stroke();
      context.fillStyle = definition.color;
      context.fillText(definition.symbol, x, y + 0.3);
    });
  }
  context.restore();
}

function drawCockpitSurface(panel, matrix) {
  const surface = app.sessionSurface;
  if (!surface || !app.cockpitPriceDomain) return;
  const { base, empty, layout } = resizeCockpitPanel(panel);
  const context = base.getContext("2d");
  context.clearRect(0, 0, layout.width, layout.height);
  context.fillStyle = "#081d30";
  context.fillRect(0, 0, layout.width, layout.height);
  drawCockpitSessionSegmentBackgrounds(context, layout, surface);
  const domain = app.cockpitColorDomains[panel];
  let numericCount = 0;
  let projectionBoundary = null;
  surface.timeBuckets.forEach((bucket, timeIndex) => {
    const visible = cockpitVisibleTimeRange(surface, bucket.startMs, bucket.endMs);
    if (!visible) return;
    const column = surface.surfaceColumns[timeIndex];
    const x1 = cockpitTimeToX(layout, surface, visible.startMs);
    const x2 = cockpitTimeToX(layout, surface, visible.endMs);
    if (column.kind === "projection" && projectionBoundary === null) projectionBoundary = x1;
    if (column.kind === "missing") {
      drawCockpitMissingRange(context, layout, surface, bucket.startMs, bucket.endMs);
      return;
    }
    for (let priceIndex = 0; priceIndex < surface.priceGrid.length; priceIndex += 1) {
      const value = matrix[timeIndex][priceIndex];
      const bounds = cockpitPriceBounds(surface, priceIndex);
      if (bounds.upper < app.cockpitPriceDomain.min || bounds.lower > app.cockpitPriceDomain.max) continue;
      const y1 = sessionPriceToY(
        layout,
        app.cockpitPriceDomain,
        Math.min(bounds.upper, app.cockpitPriceDomain.max),
      );
      const y2 = sessionPriceToY(
        layout,
        app.cockpitPriceDomain,
        Math.max(bounds.lower, app.cockpitPriceDomain.min),
      );
      const width = Math.max(x2 - x1 + 0.45, 0.75);
      const height = Math.max(y2 - y1 + 0.45, 0.75);
      if (value === null) {
        context.fillStyle = "rgba(8, 29, 48, 0.96)";
        context.fillRect(x1, y1, width, height);
        hatchCockpitRect(context, x1, y1, width, height, "rgba(160, 184, 198, 0.18)");
      } else {
        context.fillStyle = divergingColor(value, domain, { projection: column.kind === "projection" });
        context.fillRect(x1, y1, width, height);
        numericCount += 1;
      }
    }
  });
  for (const range of surface.missingRanges) {
    if (missingRangeAppliesToPanel(range, panel)) {
      drawCockpitMissingRange(context, layout, surface, range.startMs, range.endMs);
    }
  }
  if (panel === "gamma") {
    context.save();
    context.strokeStyle = "rgba(7, 25, 39, 0.92)";
    context.lineWidth = 3.4;
    context.beginPath();
    let drawing = false;
    const window = activeCockpitTimeWindow(surface);
    surface.zeroRidges.forEach((ridge, index) => {
      const centerMs = surface.timeBuckets[index].centerMs;
      if (!Number.isFinite(ridge) || centerMs < window.startMs || centerMs > window.endMs) {
        drawing = false;
        return;
      }
      const x = cockpitTimeToX(layout, surface, centerMs);
      const y = sessionPriceToY(layout, app.cockpitPriceDomain, ridge);
      if (!drawing) context.moveTo(x, y);
      else context.lineTo(x, y);
      drawing = true;
    });
    context.stroke();
    context.strokeStyle = "rgba(222, 239, 246, 0.88)";
    context.lineWidth = 1;
    context.stroke();
    context.restore();
  }
  if (projectionBoundary !== null) {
    context.save();
    context.setLineDash([4, 4]);
    context.strokeStyle = "rgba(241, 211, 113, 0.9)";
    context.lineWidth = 1;
    context.beginPath();
    context.moveTo(projectionBoundary, layout.plotTop);
    context.lineTo(projectionBoundary, layout.plotBottom);
    context.stroke();
    context.setLineDash([]);
    context.fillStyle = "#efd685";
    context.font = "8px ui-monospace, SFMono-Regular, monospace";
    context.textAlign = "left";
    context.fillText("projection →", Math.min(projectionBoundary + 4, layout.plotRight - 62), layout.plotTop + 10);
    context.restore();
  }
  drawCockpitTimeAxes(context, layout, surface);
  drawCockpitPriceAxes(context, layout, app.cockpitPriceDomain, { side: "right" });
  drawCockpitCandles(context, layout, surface);
  if (panel === "gamma") drawGammaExtremaMarkers(context, layout, surface);
  drawCockpitSessionSegmentBoundaries(context, layout, surface);
  empty.hidden = numericCount > 0;
}

function normalizedStrikeMode(value = app.strikeMode) {
  return value === "gamma" ? "gamma" : "oi";
}

function renderCockpitStrikeModeChrome(surface = app.sessionSurface) {
  const mode = normalizedStrikeMode();
  const oiMode = mode === "oi";
  dom.cockpitStrikeModeOi.setAttribute("aria-pressed", String(oiMode));
  dom.cockpitStrikeModeGamma.setAttribute("aria-pressed", String(!oiMode));
  dom.cockpitStrikeModeOi.classList.toggle("active", oiMode);
  dom.cockpitStrikeModeGamma.classList.toggle("active", !oiMode);
  dom.cockpitStrikeTitle.textContent = oiMode ? "Open Interest by Strike" : "Gamma by Strike";
  dom.cockpitStrikeReadoutLabel.textContent = oiMode
    ? "OI = Call + Put open-interest contracts"
    : `Γ Proxy · ${surface ? sessionMetricUnitLabel(surface, "signed_gamma") : "metric unit"}`;
  dom.cockpitStrikeCurrentLegend.textContent = oiMode ? "Current OI" : "Current Γ";
  const baselineUnavailable = surface?.strikeProfileMetadata
    ?.baselineUnavailableReason === "gth_contract_universe_completeness_unproven";
  const baselineMissing = surface && !surface.strikeProfileMetadata?.baselineAt;
  dom.cockpitStrikeBaselineLegend.textContent = baselineMissing
    ? baselineUnavailable
      ? "Baseline unavailable · chain completeness unproven"
      : "Baseline unavailable · no comparable validated snapshot"
    : "Baseline · first validated";
  dom.cockpitStrikeColorLegend.textContent = "Call + / Put −";
  const domain = oiMode
    ? app.cockpitColorDomains.strikeOpenInterest
    : app.cockpitColorDomains.strikeGamma;
  dom.cockpitStrikeDomain.textContent = domain
    ? oiMode
      ? `0–${compactNumber(domain.maxAbs, 1)} contracts`
      : `±${compactNumber(domain.maxAbs, 1)} · ${surface ? sessionMetricUnitLabel(surface, "signed_gamma") : "metric unit"}`
    : "domain —";
}

function strikeProxyColor(value) {
  if (!Number.isFinite(value) || value === 0) return "rgba(145, 163, 174, 0.72)";
  return value < 0
    ? "rgba(220, 103, 95, 0.82)"
    : "rgba(55, 146, 190, 0.84)";
}

function drawCockpitStrike() {
  const surface = app.sessionSurface;
  if (!surface || !app.cockpitPriceDomain) return;
  const { base, empty, layout } = resizeCockpitPanel("strike");
  const context = base.getContext("2d");
  context.clearRect(0, 0, layout.width, layout.height);
  context.fillStyle = "#081d30";
  context.fillRect(0, 0, layout.width, layout.height);
  drawCockpitPriceAxes(context, layout, app.cockpitPriceDomain, { side: "left" });
  const mode = normalizedStrikeMode();
  const oiMode = mode === "oi";
  const domain = oiMode
    ? app.cockpitColorDomains.strikeOpenInterest
    : app.cockpitColorDomains.strikeGamma;
  const maximum = Math.max(domain?.maxAbs || 0, 1e-12);
  const zeroX = oiMode ? layout.plotLeft : layout.plotLeft + layout.plotWidth / 2;
  const availableWidth = oiMode ? layout.plotWidth : layout.plotWidth / 2;
  context.save();
  context.strokeStyle = "rgba(219, 233, 240, 0.62)";
  context.setLineDash([3, 4]);
  context.beginPath();
  context.moveTo(zeroX, layout.plotTop);
  context.lineTo(zeroX, layout.plotBottom);
  context.stroke();
  context.setLineDash([]);
  for (const row of surface.strikeProfile) {
    if (row.strike < app.cockpitPriceDomain.min || row.strike > app.cockpitPriceDomain.max) continue;
    const y = sessionPriceToY(layout, app.cockpitPriceDomain, row.strike);
    const currentValue = oiMode ? row.currentOpenInterest : row.currentProxy;
    const baselineValue = oiMode ? row.firstValidatedOpenInterest : row.firstValidatedProxy;
    const currentRatio = currentValue === null
      ? 0
      : Math.min(Math.abs(currentValue) / maximum, 1);
    const baselineRatio = baselineValue === null
      ? 0
      : Math.min(Math.abs(baselineValue) / maximum, 1);
    if (currentValue !== null) {
      const width = currentRatio * availableWidth;
      const x = !oiMode && currentValue < 0 ? zeroX - width : zeroX;
      context.fillStyle = strikeProxyColor(row.currentProxy);
      context.fillRect(x, y - 1.7, Math.max(width, currentValue === 0 ? 1 : 0), 3.4);
    }
    if (baselineValue !== null) {
      const width = baselineRatio * availableWidth;
      const x = !oiMode && baselineValue < 0 ? zeroX - width : zeroX;
      const endpointX = !oiMode && baselineValue < 0 ? x : x + width;
      context.strokeStyle = "rgba(218, 228, 234, 0.92)";
      context.lineWidth = 1.5;
      context.setLineDash([3, 3]);
      context.beginPath();
      context.moveTo(x, y);
      context.lineTo(x + width, y);
      context.stroke();
      context.setLineDash([]);
      context.fillStyle = "rgba(218, 228, 234, 0.96)";
      context.fillRect(endpointX - 1.25, y - 3, 2.5, 6);
    }
  }
  context.fillStyle = "#91a9b9";
  context.font = "8px ui-monospace, SFMono-Regular, monospace";
  context.textBaseline = "top";
  context.textAlign = "left";
  if (oiMode) {
    context.fillText("0", layout.plotLeft, layout.plotBottom + 8);
    context.textAlign = "center";
    context.fillText("Call + Put OI contracts →", layout.plotLeft + layout.plotWidth / 2, layout.plotBottom + 8);
    context.textAlign = "right";
    context.fillText(compactNumber(domain?.maxAbs || 0, 1), layout.plotRight, layout.plotBottom + 8);
  } else {
    context.fillText(`−${compactNumber(domain?.maxAbs || 0, 1)}`, layout.plotLeft, layout.plotBottom + 8);
    context.textAlign = "center";
    context.fillText("0 · Γ Proxy", zeroX, layout.plotBottom + 8);
    context.textAlign = "right";
    context.fillText(`+${compactNumber(domain?.maxAbs || 0, 1)}`, layout.plotRight, layout.plotBottom + 8);
  }
  context.restore();
  empty.hidden = surface.strikeProfile.some((row) => oiMode
    ? row.currentOpenInterest !== null || row.firstValidatedOpenInterest !== null
    : row.currentProxy !== null || row.firstValidatedProxy !== null);
  renderCockpitStrikeModeChrome(surface);
}

function nearestNumericIndex(values, target, selector = (value) => value) {
  if (!values.length || !Number.isFinite(target)) return -1;
  let best = 0;
  let distance = Math.abs(selector(values[0]) - target);
  for (let index = 1; index < values.length; index += 1) {
    const candidate = Math.abs(selector(values[index]) - target);
    if (candidate < distance) {
      best = index;
      distance = candidate;
    }
  }
  return best;
}

function cockpitCurrentSpot() {
  const surface = app.sessionSurface;
  if (!surface) return null;
  if (surface.mode === "live" && surface.availability?.current_spot_available !== true) {
    return null;
  }
  return surface.spot ?? null;
}

function cockpitDisplayTimeMs(surface = app.sessionSurface) {
  if (!surface) return null;
  if (surface.mode === "live") {
    return surface.clientLeaseMasked && Number.isFinite(surface.historyFrozenThroughMs)
      ? surface.historyFrozenThroughMs
      : surface.asOfMs;
  }
  return Number.isFinite(app.playheadMs) ? app.playheadMs : surface.asOfMs;
}

function referenceMethodLabel(method) {
  if (method === "es_basis_inferred_spx") return "ES−basis inferred SPX";
  if (method === "chain_implied") return "Chain-implied SPX · inferred";
  if (method === "direct_index_spx") return "Direct SPX reference";
  return "Reference unavailable";
}

function referenceClockLabel(timeMs) {
  return Number.isFinite(timeMs)
    ? formatMarketTime(new Date(timeMs), false)
    : "—";
}

function referenceIsoLabel(timeMs) {
  return Number.isFinite(timeMs)
    ? formatIsoUtc(new Date(timeMs))
    : "unavailable";
}

function referencePresentation(surface, frameSessionKind = null) {
  if (!surface) {
    return {
      providerText: "Provider —",
      providerTitle: "No validated surface provider",
      referenceText: "Reference —",
      referenceTitle: "No validated reference",
      clockText: "Clocks —",
      clockTitle: "No validated reference clocks",
      legendText: "Reference path",
      inferred: false,
      missing: true,
    };
  }
  if (surface.schemaVersion === 1) {
    return {
      providerText: String(surface.provider || "—").toUpperCase(),
      providerTitle: `Surface provider ${String(surface.provider || "unknown")} · v1 RTH-only contract`,
      referenceText: "V1 RTH · METHOD UNDECLARED",
      referenceTitle: "Schema v1 has no session_segments/reference contract; no GTH semantics are inferred",
      clockText: `SRC ${referenceClockLabel(surface.spotSourceAtMs)} · KNOWN ${referenceClockLabel(surface.spotKnownAtMs)}`,
      clockTitle: `spot source_at ${surface.spotSourceAtMs ? formatIsoUtc(new Date(surface.spotSourceAtMs)) : "unavailable"}; spot known_at ${surface.spotKnownAtMs ? formatIsoUtc(new Date(surface.spotKnownAtMs)) : "unavailable"}`,
      legendText: "SPX reference samples · v1 RTH",
      inferred: false,
      missing: false,
    };
  }
  const displayTimeMs = cockpitDisplayTimeMs(surface) ?? surface.asOfMs;
  const displaySegment = sessionSegmentAtTime(surface.sessionSegments, displayTimeMs);
  const effectiveSessionKind = ["gth", "closed_gap", "rth"].includes(frameSessionKind)
    ? frameSessionKind
    : displaySegment?.kind;
  const segment = surface.sessionSegments.find((item) => item.kind === effectiveSessionKind) ||
    displaySegment;
  const reference = effectiveSessionKind === "closed_gap" ? null : surface.reference;
  const surfaceProvider = segment?.surfaceProvider?.toUpperCase() || "—";
  const referenceProvider = reference?.provider?.toUpperCase() ||
    segment?.referenceProvider?.toUpperCase() || "—";
  const providerText = effectiveSessionKind === "gth"
    ? `${surfaceProvider} SPXW · PARTIAL-CHAIN PROXY`
    : effectiveSessionKind === "rth"
      ? `${surfaceProvider} SPXW · ${referenceProvider} SPX REF`
      : "CLOSED GAP · NO PROVIDER";
  const providerTitle = segment
    ? `${segment.kind.toUpperCase()} surface provider ${surfaceProvider}; reference provider ${referenceProvider}${segment.kind === "gth" ? "; SPXW chain completeness unproven" : ""}`
    : "No session segment at display time";
  if (!reference || reference.price === null) {
    return {
      providerText,
      providerTitle,
      referenceText: "REFERENCE MISSING",
      referenceTitle: reference?.missingReason || "Reference cleared or unavailable",
      clockText: "CLOCKS MISSING",
      clockTitle: "No valid source/basis clocks",
      legendText: "GTH PARTIAL-CHAIN PROXY · completeness unproven / RTH direct ref",
      inferred: false,
      missing: true,
    };
  }
  const inferred = reference.inferred;
  const referenceText = inferred
    ? "INFERRED · NOT OFFICIAL SPX OHLC"
    : "DIRECT SPX REFERENCE";
  const referenceTitle = `${referenceMethodLabel(reference.method)} · provider ${reference.provider} · instrument ${reference.instrumentId} · price ${reference.price}`;
  const clockText = inferred
    ? `SRC ${referenceClockLabel(reference.sourceAtMs)} · BASIS ${referenceClockLabel(reference.basis?.frozenAtMs)} · ACCEPTED ${Number.isFinite(reference.acceptedAtMs) ? referenceClockLabel(reference.acceptedAtMs) : "unavailable"}`
    : `SRC ${referenceClockLabel(reference.sourceAtMs)} · ACCEPTED ${Number.isFinite(reference.acceptedAtMs) ? referenceClockLabel(reference.acceptedAtMs) : "unavailable"}`;
  const clockTitle = [
    `source_at ${referenceIsoLabel(reference.sourceAtMs)}`,
    `known_at ${referenceIsoLabel(reference.knownAtMs)}`,
    `accepted_at ${referenceIsoLabel(reference.acceptedAtMs)}`,
    `valid_until ${referenceIsoLabel(reference.validUntilMs)}`,
    reference.basis
      ? `basis ${reference.basis.value} (${reference.basis.method}); basis known_at ${formatIsoUtc(new Date(reference.basis.knownAtMs))}; frozen_at ${formatIsoUtc(new Date(reference.basis.frozenAtMs))}`
      : null,
  ].filter(Boolean).join("; ");
  return {
    providerText,
    providerTitle,
    referenceText,
    referenceTitle,
    clockText,
    clockTitle,
    legendText: "GTH PARTIAL-CHAIN PROXY · completeness unproven / RTH direct ref",
    inferred,
    missing: false,
  };
}

function renderReferenceChrome(surface = app.sessionSurface, frameSessionKind = null) {
  const activeFrameKind = frameSessionKind || (
    surface === app.sessionSurface
      ? app.frames[app.sessionSurfaceKeyframeIndex]?.sessionKind
      : null
  );
  const presentation = referencePresentation(surface, activeFrameKind);
  dom.providerChip.textContent = presentation.providerText;
  dom.providerChip.title = presentation.providerTitle;
  dom.referenceChip.textContent = presentation.referenceText;
  dom.referenceChip.title = presentation.referenceTitle;
  dom.referenceChip.className = `reference-chip${presentation.inferred ? " inferred" : ""}${presentation.missing ? " missing" : ""}`;
  dom.referenceClock.textContent = presentation.clockText;
  dom.referenceClock.title = presentation.clockTitle;
  for (const icon of [dom.cockpitGammaReferenceIcon, dom.cockpitCharmReferenceIcon]) {
    icon.className = `legend-candle${presentation.inferred ? " inferred" : ""}`;
  }
  for (const label of [dom.cockpitGammaReferenceLegend, dom.cockpitCharmReferenceLegend]) {
    label.textContent = presentation.legendText;
  }
}

function cockpitValueAt(panel, timeMs, price) {
  const surface = app.sessionSurface;
  if (!surface) return { value: null, timeIndex: -1, priceIndex: -1, column: null };
  const matrix = panel === "gamma" ? surface.gamma : surface.charm;
  const timeIndex = nearestNumericIndex(surface.timeBuckets, timeMs, (bucket) => bucket.centerMs);
  const priceIndex = nearestNumericIndex(surface.priceGrid, price);
  return {
    value: timeIndex >= 0 && priceIndex >= 0 ? matrix[timeIndex][priceIndex] : null,
    timeIndex,
    priceIndex,
    column: timeIndex >= 0 ? surface.surfaceColumns[timeIndex] : null,
  };
}

function drawCockpitOverlay(panel, spot) {
  const surface = app.sessionSurface;
  const layout = app.cockpitLayouts[panel];
  const overlay = cockpitElements(panel).overlay;
  if (!surface || !layout || !overlay || !app.cockpitPriceDomain) return;
  const context = overlay.getContext("2d");
  context.clearRect(0, 0, layout.width, layout.height);
  context.save();
  const spotY = Number.isFinite(spot)
    ? sessionPriceToY(layout, app.cockpitPriceDomain, spot)
    : null;
  if (spotY !== null) {
    context.setLineDash([5, 4]);
    context.strokeStyle = "rgba(244, 210, 103, 0.94)";
    context.lineWidth = 1;
    context.beginPath();
    context.moveTo(layout.plotLeft, spotY);
    context.lineTo(layout.plotRight, spotY);
    context.stroke();
    context.setLineDash([]);
  }
  const displayTimeMs = cockpitDisplayTimeMs(surface);
  const window = activeCockpitTimeWindow(surface);
  if (panel !== "strike" && Number.isFinite(displayTimeMs) &&
      displayTimeMs >= window.startMs && displayTimeMs <= window.endMs) {
    const currentX = cockpitTimeToX(layout, surface, displayTimeMs);
    context.strokeStyle = "rgba(239, 246, 250, 0.88)";
    context.lineWidth = 1;
    context.beginPath();
    context.moveTo(currentX, layout.plotTop);
    context.lineTo(currentX, layout.plotBottom);
    context.stroke();
  }
  if (app.cockpitHover) {
    const hoverY = sessionPriceToY(layout, app.cockpitPriceDomain, app.cockpitHover.price);
    context.setLineDash([2, 3]);
    context.strokeStyle = "rgba(232, 242, 248, 0.76)";
    context.lineWidth = 0.8;
    context.beginPath();
    context.moveTo(layout.plotLeft, hoverY);
    context.lineTo(layout.plotRight, hoverY);
    context.stroke();
    if (panel !== "strike") {
      const hoverX = cockpitTimeToX(layout, surface, app.cockpitHover.timeMs);
      context.beginPath();
      context.moveTo(hoverX, layout.plotTop);
      context.lineTo(hoverX, layout.plotBottom);
      context.stroke();
    }
    context.setLineDash([]);
  }
  context.restore();
}

function renderCockpitReadouts(spot) {
  const surface = app.sessionSurface;
  if (!surface) return;
  const at = cockpitDisplayTimeMs(surface);
  const gamma = cockpitValueAt("gamma", at, spot);
  const charm = cockpitValueAt("charm", at, spot);
  const strikeIndex = nearestNumericIndex(surface.strikeProfile, spot, (row) => row.strike);
  const strike = strikeIndex >= 0 ? surface.strikeProfile[strikeIndex] : null;
  dom.cockpitGammaValue.textContent = gamma.value === null ? "Γ —" : `Γ ${compactNumber(gamma.value, 3)}`;
  dom.cockpitCharmValue.textContent = charm.value === null ? "Charm —" : compactNumber(charm.value, 3);
  const strikeValue = normalizedStrikeMode() === "oi"
    ? strike?.currentOpenInterest
    : strike?.currentProxy;
  dom.cockpitStrikeValue.textContent = strikeValue === null || strikeValue === undefined || !strike
    ? "—"
    : normalizedStrikeMode() === "oi"
      ? `${strike.strike.toFixed(0)} · OI ${compactNumber(strikeValue, 1)}`
      : `${strike.strike.toFixed(0)} · Γ ${compactNumber(strikeValue, 2)}`;
}

function drawCockpitDynamic() {
  if (!app.sessionSurface) return;
  const spot = cockpitCurrentSpot();
  for (const panel of ["gamma", "strike", "charm"]) drawCockpitOverlay(panel, spot);
  renderCockpitReadouts(spot);
}

function renderCockpitStatic() {
  const surface = app.sessionSurface;
  if (!surface) {
    for (const panel of ["gamma", "strike", "charm"]) {
      const elements = cockpitElements(panel);
      for (const canvas of [elements.base, elements.overlay]) {
        const context = canvas.getContext("2d");
        context.clearRect(0, 0, canvas.width, canvas.height);
      }
      elements.empty.hidden = false;
    }
    dom.cockpitGammaValue.textContent = "Γ —";
    dom.cockpitStrikeValue.textContent = "—";
    dom.cockpitCharmValue.textContent = "Charm —";
    dom.cockpitGammaThreshold.textContent = "neutral —";
    dom.cockpitCharmThreshold.textContent = "neutral —";
    dom.cockpitGammaDomain.textContent = "domain —";
    dom.cockpitCharmDomain.textContent = "domain —";
    renderCockpitStrikeModeChrome(null);
    dom.cockpitTooltip.hidden = true;
    dom.cockpitTooltip.replaceChildren();
    if (dom.cockpitTooltip.dataset) delete dom.cockpitTooltip.dataset.panel;
    renderReferenceChrome(null);
    updateLiveViewportChrome(null);
    app.cockpitStaticSignature = "";
    return;
  }
  renderReferenceChrome(surface);
  const viewport = activeCockpitTimeWindow(surface);
  updateLiveViewportChrome(surface);
  updateCockpitStableDomains(surface);
  const staticSignature = [
    app.sessionSurfaceRenderedKey,
    surface.asOfMs ?? "",
    surface.historyFrozenThroughMs ?? "",
    viewport.startMs,
    viewport.endMs,
    app.strikeMode,
    dom.cockpitGammaStage?.clientWidth ?? 0,
    dom.cockpitGammaStage?.clientHeight ?? 0,
    dom.cockpitStrikeStage?.clientWidth ?? 0,
    dom.cockpitStrikeStage?.clientHeight ?? 0,
    dom.cockpitCharmStage?.clientWidth ?? 0,
    dom.cockpitCharmStage?.clientHeight ?? 0,
  ].join("|");
  if (staticSignature !== app.cockpitStaticSignature) {
    app.cockpitStaticSignature = staticSignature;
    drawCockpitSurface("gamma", surface.gamma);
    drawCockpitStrike();
    drawCockpitSurface("charm", surface.charm);
  }
  const gammaDomain = app.cockpitColorDomains.gamma;
  const charmDomain = app.cockpitColorDomains.charm;
  const gammaUnit = sessionMetricUnitLabel(surface, "signed_gamma");
  const charmUnit = sessionMetricUnitLabel(surface, "charm");
  dom.cockpitGammaThreshold.textContent = `neutral ±${compactNumber(gammaDomain.threshold, 2)}`;
  dom.cockpitCharmThreshold.textContent = `neutral ±${compactNumber(charmDomain.threshold, 2)}`;
  dom.cockpitGammaDomain.textContent = `domain ±${compactNumber(gammaDomain.maxAbs, 2)} · ${gammaUnit}`;
  dom.cockpitCharmDomain.textContent = `domain ±${compactNumber(charmDomain.maxAbs, 2)} · ${charmUnit}`;
  drawCockpitDynamic();
  renderCockpitAudit();
}

function strikeProfileContextLabel(metadata, prefix) {
  const at = metadata?.[`${prefix}At`];
  if (!at) return `${prefix}: unavailable`;
  const sessionKind = metadata[`${prefix}SessionKind`];
  const provider = metadata[`${prefix}SurfaceProvider`];
  const referenceMethod = metadata[`${prefix}ReferenceMethod`];
  return `${prefix}: ${sessionKind?.toUpperCase() || "segment undeclared"} / ${provider?.toUpperCase() || "provider undeclared"} / ${referenceMethodLabel(referenceMethod)} @ ${formatReplayAsOf(at)}`;
}

function strikeProfileComparisonLabel(metadata) {
  if (!metadata) return "Strike comparison metadata unavailable";
  const baselineUnavailable = metadata.baselineUnavailableReason ===
    "gth_contract_universe_completeness_unproven";
  const baselineMissing = !metadata.baselineAt;
  return [
    strikeProfileContextLabel(metadata, "current"),
    baselineMissing
      ? baselineUnavailable
        ? "baseline: unavailable · GTH contract-universe completeness unproven"
        : "baseline: unavailable · no comparable validated snapshot"
      : strikeProfileContextLabel(metadata, "baseline"),
    "snapshot state only; not MM/participant position or signed flow",
    metadata.contractVerified
      ? baselineMissing
        ? "cross-snapshot comparison disabled"
        : "same-segment/provider/reference-method contract verified"
      : "legacy metadata; segment/provider unverified",
  ].join(" · ");
}

function cockpitTooltipFor(panel, timeMs, price) {
  const surface = app.sessionSurface;
  if (!surface) return;
  const gamma = cockpitValueAt("gamma", timeMs, price);
  const charm = cockpitValueAt("charm", timeMs, price);
  const strikeIndex = nearestNumericIndex(surface.strikeProfile, price, (row) => row.strike);
  const strike = strikeIndex >= 0 ? surface.strikeProfile[strikeIndex] : null;
  const candle = cockpitCandleAtTime(surface.candles, timeMs, surface.asOfMs);
  const segment = surface.schemaVersion === 2
    ? sessionSegmentAtTime(surface.sessionSegments, timeMs)
    : null;
  const title = document.createElement("strong");
  title.textContent = `${formatMarketTime(new Date(timeMs))} · SPX ${price.toFixed(2)}`;
  const gammaLine = document.createElement("span");
  const peak = gamma.timeIndex >= 0 ? surface.gammaPositivePeaks[gamma.timeIndex] : null;
  const trough = gamma.timeIndex >= 0 ? surface.gammaNegativeTroughs[gamma.timeIndex] : null;
  const extremaText = [
    peak ? `peak +${String(peak.value)} @ ${peak.price}` : null,
    trough ? `trough ${String(trough.value)} @ ${trough.price}` : null,
  ].filter(Boolean).join(" · ");
  gammaLine.textContent = `Gamma raw: ${gamma.value === null ? "missing" : String(gamma.value)} ${sessionMetricUnitLabel(surface, "signed_gamma")} · ${gamma.column?.kind || "missing"}${extremaText ? ` · ${extremaText}` : ""}`;
  const charmLine = document.createElement("span");
  charmLine.textContent = `Charm raw: ${charm.value === null ? "missing" : String(charm.value)} ${sessionMetricUnitLabel(surface, "charm")} · ${charm.column?.quality || "unknown"}`;
  const strikeLine = document.createElement("span");
  const baselineUnavailable = !surface.strikeProfileMetadata?.baselineAt;
  const gthBaselineUnavailable = surface.strikeProfileMetadata
    ?.baselineUnavailableReason === "gth_contract_universe_completeness_unproven";
  const baselineOiText = baselineUnavailable
    ? gthBaselineUnavailable
      ? "baseline unavailable (GTH chain completeness unproven)"
      : "baseline unavailable (no comparable validated snapshot)"
    : `first validated ${strike?.firstValidatedOpenInterest ?? "missing"} contracts`;
  const baselineGammaText = baselineUnavailable
    ? "Γ baseline unavailable"
    : `Γ baseline ${strike?.firstValidatedProxy ?? "missing"}`;
  strikeLine.textContent = strike
    ? `Strike ${strike.strike}: OI current ${strike.currentOpenInterest ?? "missing"} contracts; ${baselineOiText} · color sign Γ proxy ${strike.currentProxy ?? "missing"}; ${baselineGammaText} ${sessionMetricUnitLabel(surface, "signed_gamma")}`
    : "Strike profile: missing";
  const strikeSemanticsLine = document.createElement("span");
  strikeSemanticsLine.textContent = strikeProfileComparisonLabel(surface.strikeProfileMetadata);
  const candleLine = document.createElement("span");
  candleLine.textContent = candle
    ? candle.inferred
      ? `INFERRED reference sample (ES−basis; NOT official SPX OHLC): ${candle.open} / ${candle.high} / ${candle.low} / ${candle.close} · n=${candle.sampleCount}${candle.complete ? "" : " · partial"}`
      : `Direct SPX reference sample: ${candle.open} / ${candle.high} / ${candle.low} / ${candle.close} · n=${candle.sampleCount}${candle.complete ? "" : " · partial"}`
    : "Reference sample: missing";
  const sourceLine = document.createElement("span");
  sourceLine.textContent = candle
    ? `Reference provider ${candle.referenceProvider || surface.provider} · ${candle.referenceInstrumentId || "instrument undeclared"} · source ${referenceIsoLabel(candle.sourceAtMs)} · known ${referenceIsoLabel(candle.knownAtMs)} · accepted ${referenceIsoLabel(candle.acceptedAtMs)} · valid until ${referenceIsoLabel(candle.validUntilMs)}`
    : `Surface source ${referenceIsoLabel(gamma.column?.sourceAtMs)} · known ${referenceIsoLabel(gamma.column?.knownAtMs)} · accepted ${referenceIsoLabel(gamma.column?.acceptedAtMs)} · valid until ${referenceIsoLabel(gamma.column?.validUntilMs)}`;
  const semanticsLine = document.createElement("span");
  if (surface.schemaVersion === 2) {
    const targetKind = segment?.kind?.toUpperCase() || "UNKNOWN SEGMENT";
    const sourceKind = gamma.column?.sourceSessionKind?.toUpperCase() || null;
    const sourceProvider = gamma.column?.surfaceProvider || "none";
    const method = gamma.column?.referenceMethod || segment?.referenceMethod;
    const sourceMethod = method === "es_basis_inferred_spx"
      ? "ES−basis inferred"
      : method === "direct_index_spx" ? "direct SPX" : "reference unavailable";
    semanticsLine.textContent = gamma.column?.kind === "projection" &&
        sourceKind && sourceKind !== targetKind
      ? `${targetKind} scenario · projected from ${sourceKind} ${sourceProvider.toUpperCase()}/${sourceMethod}`
      : `${targetKind} · source ${sourceKind || "unavailable"} · surface provider ${sourceProvider} · ${referenceMethodLabel(method)}`;
  } else {
    semanticsLine.textContent =
      "Schema v1 RTH-only · reference method undeclared · no GTH semantics inferred";
  }
  const basisLine = document.createElement("span");
  basisLine.textContent = candle?.inferred
    ? `ES−SPX basis ${candle.basisValue ?? "missing"} · basis observed ${referenceIsoLabel(candle.basisObservedAtMs)} · dashed / reduced opacity`
    : candle
      ? "Basis: none · direct reference rendered solid"
      : "Basis: unavailable";
  dom.cockpitTooltip.replaceChildren(
    title,
    gammaLine,
    charmLine,
    strikeLine,
    strikeSemanticsLine,
    semanticsLine,
    candleLine,
    sourceLine,
    basisLine,
  );
  dom.cockpitTooltip.dataset.panel = panel;
}

function liveViewportStartAfterPan(surface, startMs, deltaPixels, plotWidth) {
  if (!surface || surface.mode !== "live" || !Number.isFinite(startMs) ||
      !Number.isFinite(deltaPixels) || !Number.isFinite(plotWidth) || plotWidth <= 0) {
    return startMs;
  }
  const spanMs = Math.min(
    LIVE_VIEW_SPAN_MS,
    Math.max(surface.sessionEndMs - surface.sessionStartMs, 1),
  );
  const requestedStartMs = startMs - deltaPixels / plotWidth * spanMs;
  return cockpitTimeWindow(surface, { manualStartMs: requestedStartMs }).startMs;
}

function panLiveViewportBy(deltaMs) {
  const surface = app.sessionSurface;
  if (app.mode !== "live" || surface?.mode !== "live" || !Number.isFinite(deltaMs)) return;
  const current = activeCockpitTimeWindow(surface);
  app.liveViewportStartMs = cockpitTimeWindow(surface, {
    manualStartMs: current.startMs + deltaMs,
  }).startMs;
  app.cockpitStaticSignature = "";
  app.cockpitHover = null;
  dom.cockpitTooltip.hidden = true;
  renderCockpitStatic();
}

function beginLiveViewportPan(panel, event) {
  const surface = app.sessionSurface;
  const layout = app.cockpitLayouts[panel];
  const overlay = cockpitElements(panel).overlay;
  if (app.mode !== "live" || surface?.mode !== "live" || panel === "strike" ||
      !layout || !overlay || (event.pointerType === "mouse" && event.button !== 0)) return false;
  const rect = overlay.getBoundingClientRect();
  const x = event.clientX - rect.left;
  if (x < layout.plotLeft || x > layout.plotRight) return false;
  const viewport = activeCockpitTimeWindow(surface);
  app.liveViewportDrag = {
    panel,
    pointerId: event.pointerId,
    startClientX: event.clientX,
    startMs: viewport.startMs,
    active: false,
  };
  overlay.setPointerCapture?.(event.pointerId);
  document.body.classList.add("live-viewport-dragging");
  clearCockpitHover();
  event.preventDefault();
  return true;
}

function updateLiveViewportPan(panel, event) {
  const drag = app.liveViewportDrag;
  const surface = app.sessionSurface;
  const layout = app.cockpitLayouts[panel];
  if (!drag || drag.panel !== panel || drag.pointerId !== event.pointerId ||
      surface?.mode !== "live" || !layout) return false;
  const deltaPixels = event.clientX - drag.startClientX;
  if (!drag.active && Math.abs(deltaPixels) < 4) {
    event.preventDefault();
    return true;
  }
  drag.active = true;
  app.liveViewportStartMs = liveViewportStartAfterPan(
    surface,
    drag.startMs,
    deltaPixels,
    layout.plotWidth,
  );
  app.cockpitStaticSignature = "";
  renderCockpitStatic();
  event.preventDefault();
  return true;
}

function endLiveViewportPan(panel, event) {
  const drag = app.liveViewportDrag;
  if (!drag || drag.panel !== panel || drag.pointerId !== event.pointerId) return false;
  cockpitElements(panel).overlay?.releasePointerCapture?.(event.pointerId);
  app.liveViewportDrag = null;
  document.body.classList.remove("live-viewport-dragging");
  updateLiveViewportChrome();
  event.preventDefault();
  return true;
}

function cockpitPointerMove(panel, event) {
  const surface = app.sessionSurface;
  const layout = app.cockpitLayouts[panel];
  const overlay = cockpitElements(panel).overlay;
  if (!surface || !layout || !overlay) return;
  const rect = overlay.getBoundingClientRect();
  const x = event.clientX - rect.left;
  const y = event.clientY - rect.top;
  if (x < layout.plotLeft || x > layout.plotRight || y < layout.plotTop || y > layout.plotBottom) {
    dom.cockpitTooltip.hidden = true;
    return;
  }
  const timeMs = panel === "strike"
    ? app.cockpitHover?.timeMs ?? cockpitDisplayTimeMs(surface)
    : cockpitXToTime(layout, surface, x);
  const price = sessionYToPrice(layout, app.cockpitPriceDomain, y);
  app.cockpitHover = { panel, timeMs, price };
  drawCockpitDynamic();
  cockpitTooltipFor(panel, timeMs, price);
  const cockpitRect = dom.sessionCockpit.getBoundingClientRect();
  const localX = event.clientX - cockpitRect.left;
  const localY = event.clientY - cockpitRect.top;
  const tooltipWidth = Math.min(390, Math.max(cockpitRect.width - 16, 190));
  dom.cockpitTooltip.style.width = `${tooltipWidth}px`;
  dom.cockpitTooltip.style.left = `${Math.min(Math.max(localX + 12, 8), Math.max(cockpitRect.width - tooltipWidth - 8, 8))}px`;
  dom.cockpitTooltip.hidden = false;
  const tooltipHeight = Math.max(dom.cockpitTooltip.offsetHeight || 172, 120);
  dom.cockpitTooltip.style.top = `${Math.min(Math.max(localY + 12, 8), Math.max(cockpitRect.height - tooltipHeight - 8, 8))}px`;
}

function clearCockpitHover() {
  app.cockpitHover = null;
  dom.cockpitTooltip.hidden = true;
  drawCockpitDynamic();
}

function summarizeAuditObject(value) {
  if (!isObject(value)) return "—";
  const entries = Object.entries(value)
    .filter(([, item]) => ["string", "number", "boolean"].includes(typeof item))
    .slice(0, 12);
  return entries.length
    ? entries.map(([key, item]) => `${key}=${String(item)}`).join(" · ")
    : "—";
}

function renderCockpitAudit() {
  const surface = app.sessionSurface;
  if (!surface) {
    dom.cockpitAuditAsOf.textContent = "—";
    dom.cockpitAuditContract.textContent = "—";
    dom.cockpitAuditStats.textContent = "—";
    dom.cockpitAuditMissing.textContent = "—";
    dom.cockpitAuditCapabilities.textContent = "—";
    dom.cockpitAuditReference.textContent = "—";
    dom.cockpitAuditStrike.textContent = "—";
    dom.cockpitAuditProvenance.textContent = "—";
    dom.cockpitAuditFrozen.textContent = "—";
    dom.cockpitAuditPit.textContent = "PIT —";
    dom.cockpitAuditModel.textContent = "Model —";
    return;
  }
  const counts = surface.surfaceColumns.reduce(
    (result, column) => ({ ...result, [column.kind]: result[column.kind] + 1 }),
    { historical: 0, projection: 0, missing: 0 },
  );
  const pit = surface.provenance.point_in_time_confidence ||
    surface.raw.point_in_time_confidence || "bounded_not_proven";
  const model = surface.provenance.model || surface.raw.model || "proxy model";
  const leaseText = surface.mode === "live"
    ? ` · accepted ${formatIsoUtc(surface.acceptedAt)} · valid until ${formatIsoUtc(surface.validUntil)}`
    : "";
  dom.cockpitAuditAsOf.textContent = `${formatReplayAsOf(surface.asOf)}${leaseText} · session ${formatMarketTime(surface.sessionStart)} → ${formatMarketTime(surface.sessionEnd)}`;
  dom.cockpitAuditContract.textContent = `${surface.role.toUpperCase()} ${surface.expiry} · ${surface.weighting} · ${surface.bucketMinutes}m × ${surface.priceStep} SPX`;
  dom.cockpitAuditStats.textContent = `${surface.timeBuckets.length} buckets · ${surface.priceGrid.length} SPX rows · ${surface.candles.length} candles · ${surface.strikeProfile.length} strikes · historical ${counts.historical} / projection ${counts.projection} / missing ${counts.missing}`;
  dom.cockpitAuditMissing.textContent = surface.missingRanges.length
    ? surface.missingRanges.map((range) => {
        const scope = range.components.length ? ` [${range.components.join(", ")}]` : "";
        return `${formatMarketTime(new Date(range.startMs), false)}–${formatMarketTime(new Date(range.endMs), false)}${scope} ${range.reason}`;
      }).join(" · ")
    : "No declared missing ranges";
  dom.cockpitAuditCapabilities.textContent = [
    summarizeAuditObject(surface.capabilities),
    surface.availability ? `availability: ${summarizeAuditObject(surface.availability)}` : null,
  ].filter(Boolean).join(" · ");
  const reference = surface.reference;
  dom.cockpitAuditReference.textContent = surface.schemaVersion === 1
    ? "v1 RTH-only contract · reference method/provider split unavailable · no GTH inferred"
    : reference?.price === null || !reference
      ? `missing · ${reference?.missingReason || "reference cleared"}`
      : [
          `${referenceMethodLabel(reference.method)} ${reference.price}`,
          `provider=${reference.provider}`,
          `instrument=${reference.instrumentId}`,
          `source_at=${referenceIsoLabel(reference.sourceAtMs)}`,
          `known_at=${referenceIsoLabel(reference.knownAtMs)}`,
          `accepted_at=${referenceIsoLabel(reference.acceptedAtMs)}`,
          `valid_until=${referenceIsoLabel(reference.validUntilMs)}`,
          reference.basis
            ? `basis=${reference.basis.value} ${reference.basis.method} frozen_at=${referenceIsoLabel(reference.basis.frozenAtMs)}`
            : "basis=none",
        ].join(" · ");
  dom.cockpitAuditStrike.textContent = strikeProfileComparisonLabel(
    surface.strikeProfileMetadata,
  );
  dom.cockpitAuditProvenance.textContent = summarizeAuditObject(surface.provenance);
  dom.cockpitAuditFrozen.textContent = isReplayView()
    ? "Frozen replay"
    : surface.clientLeaseMasked
      ? "Live lease expired · client projection mask"
      : `Live ${surface.liveStatus}`;
  dom.cockpitAuditPit.textContent = `PIT ${String(pit).replaceAll("_", " ")}`;
  dom.cockpitAuditModel.textContent = `Model ${model}`;
}

function binarySearchLastAtOrBefore(values, target, selector = (value) => value) {
  let low = 0;
  let high = values.length - 1;
  let answer = -1;
  while (low <= high) {
    const middle = (low + high) >> 1;
    if (selector(values[middle]) <= target) {
      answer = middle;
      low = middle + 1;
    } else {
      high = middle - 1;
    }
  }
  return answer;
}

function resizeTrendCanvases() {
  const width = Math.max(dom.trendStage.clientWidth, 320);
  const height = Math.max(dom.trendStage.clientHeight, 360);
  const ratio = Math.min(window.devicePixelRatio || 1, 2);
  for (const canvas of [dom.trendBase, dom.trendOverlay]) {
    const pixelWidth = Math.round(width * ratio);
    const pixelHeight = Math.round(height * ratio);
    if (canvas.width !== pixelWidth || canvas.height !== pixelHeight) {
      canvas.width = pixelWidth;
      canvas.height = pixelHeight;
    }
    canvas.getContext("2d").setTransform(ratio, 0, 0, ratio, 0, 0);
  }
  const mobile = width < 620;
  const margins = {
    left: mobile ? 54 : 70,
    right: mobile ? 16 : 24,
    top: 22,
    bottom: mobile ? 45 : 52,
  };
  return {
    width,
    height,
    ratio,
    margins,
    plotWidth: Math.max(width - margins.left - margins.right, 1),
    plotHeight: Math.max(height - margins.top - margins.bottom, 1),
  };
}

function trendX(layout, trend, timestampMs) {
  const ratio = (timestampMs - trend.openMs) / (trend.closeMs - trend.openMs);
  return layout.margins.left + Math.max(0, Math.min(ratio, 1)) * layout.plotWidth;
}

function trendY(layout, value) {
  const ratio = (value - layout.yMin) / (layout.yMax - layout.yMin);
  return layout.margins.top + (1 - Math.max(0, Math.min(ratio, 1))) * layout.plotHeight;
}

function trendPriceExtent(trend) {
  const first = trend.spx.prices[0];
  const halfRange = Math.max(50, first * 0.008);
  return {
    min: Math.floor((first - halfRange) / 5) * 5,
    max: Math.ceil((first + halfRange) / 5) * 5,
  };
}

function hatchCanvasRect(context, x, y, width, height, color, { cross = false, spacing = 7 } = {}) {
  if (width <= 0 || height <= 0) return;
  context.save();
  context.beginPath();
  context.rect(x, y, width, height);
  context.clip();
  context.strokeStyle = color;
  context.lineWidth = 0.75;
  const span = width + height;
  for (let offset = -height; offset <= width; offset += spacing) {
    context.beginPath();
    context.moveTo(x + offset, y + height);
    context.lineTo(x + offset + height, y);
    context.stroke();
  }
  if (cross) {
    for (let offset = 0; offset <= span; offset += spacing) {
      context.beginPath();
      context.moveTo(x + offset, y);
      context.lineTo(x + offset - height, y + height);
      context.stroke();
    }
  }
  context.restore();
}

function drawTrendGap(context, layout, trend, startMs, endMs) {
  const x1 = trendX(layout, trend, startMs);
  const x2 = trendX(layout, trend, endMs);
  if (x2 <= x1) return;
  context.fillStyle = "rgba(241, 243, 245, 0.86)";
  context.fillRect(x1, layout.margins.top, x2 - x1, layout.plotHeight);
  hatchCanvasRect(
    context,
    x1,
    layout.margins.top,
    x2 - x1,
    layout.plotHeight,
    "rgba(104, 116, 130, 0.18)",
    { cross: true, spacing: 9 },
  );
}

function drawTrendGammaColumns(context, layout, trend) {
  let coveredUntil = trend.openMs;
  for (const keyframe of trend.gamma.keyframes) {
    const startMs = Math.max(keyframe.atMs, trend.openMs);
    const endMs = Math.min(keyframe.validUntilMs, trend.closeMs);
    if (startMs > coveredUntil) drawTrendGap(context, layout, trend, coveredUntil, startMs);
    if (endMs <= startMs) continue;
    const x1 = trendX(layout, trend, startMs);
    const x2 = trendX(layout, trend, endMs);
    const width = Math.max(x2 - x1, 0.75);
    if (!["ready", "degraded"].includes(keyframe.status)) {
      drawTrendGap(context, layout, trend, startMs, endMs);
      coveredUntil = Math.max(coveredUntil, endMs);
      continue;
    }
    const spots = trend.gamma.spotOffsets.map((offset) => keyframe.referenceSpot + offset);
    const gammaDomain = Math.max(
      ...keyframe.values
        .filter((value) => value !== null)
        .map((value) => Math.abs(value)),
      0,
    );
    for (let index = 0; index < spots.length; index += 1) {
      const spot = spots[index];
      const lower = index === 0
        ? spot - (spots[1] - spot) / 2
        : (spots[index - 1] + spot) / 2;
      const upper = index === spots.length - 1
        ? spot + (spot - spots[index - 1]) / 2
        : (spot + spots[index + 1]) / 2;
      if (upper <= layout.yMin || lower >= layout.yMax) continue;
      const y1 = trendY(layout, Math.min(upper, layout.yMax));
      const y2 = trendY(layout, Math.max(lower, layout.yMin));
      const height = Math.max(y2 - y1, 0.5);
      const value = keyframe.values[index];
      if (value === null) {
        context.fillStyle = "rgba(241, 243, 245, 0.8)";
        context.fillRect(x1, y1, width, height);
        hatchCanvasRect(context, x1, y1, width, height, "rgba(104, 116, 130, 0.2)", {
          cross: true,
          spacing: 8,
        });
        continue;
      }
      const strength = gammaDomain > 0 ? Math.sqrt(Math.min(Math.abs(value) / gammaDomain, 1)) : 0;
      if (value < 0) {
        context.fillStyle = `rgba(217, 100, 89, ${0.1 + strength * 0.43})`;
        context.fillRect(x1, y1, width, height);
        hatchCanvasRect(context, x1, y1, width, height, `rgba(169, 61, 53, ${0.16 + strength * 0.28})`, {
          spacing: 7,
        });
      } else {
        context.fillStyle = `rgba(47, 111, 173, ${0.08 + strength * 0.4})`;
        context.fillRect(x1, y1, width, height);
      }
    }
    if (keyframe.zeroRidgeSpot !== null) {
      const ridgeY = trendY(layout, keyframe.zeroRidgeSpot);
      context.save();
      context.setLineDash([5, 4]);
      context.strokeStyle = "rgba(23, 32, 42, 0.82)";
      context.lineWidth = 1.25;
      context.beginPath();
      context.moveTo(x1, ridgeY);
      context.lineTo(x2, ridgeY);
      context.stroke();
      context.restore();
    }
    coveredUntil = Math.max(coveredUntil, endMs);
  }
  if (coveredUntil < trend.closeMs) {
    drawTrendGap(context, layout, trend, coveredUntil, trend.closeMs);
  }
}

function formatAxisMarketTime(timestampMs) {
  return formatMarketTime(new Date(timestampMs), false).replace(" ET", "");
}

function drawTrendAxes(context, layout, trend) {
  const { margins, plotWidth, plotHeight, width, height } = layout;
  const xTicks = width < 620 ? 4 : 7;
  const yTicks = width < 620 ? 5 : 7;
  context.save();
  context.strokeStyle = "rgba(104, 116, 130, 0.2)";
  context.fillStyle = COLORS.muted;
  context.lineWidth = 0.75;
  context.font = "10px ui-monospace, SFMono-Regular, monospace";
  context.textBaseline = "middle";
  for (let index = 0; index < yTicks; index += 1) {
    const ratio = index / (yTicks - 1);
    const value = layout.yMax - ratio * (layout.yMax - layout.yMin);
    const y = margins.top + ratio * plotHeight;
    context.beginPath();
    context.moveTo(margins.left, y + 0.5);
    context.lineTo(margins.left + plotWidth, y + 0.5);
    context.stroke();
    context.textAlign = "right";
    context.fillText(formatSpotAxis(value), margins.left - 8, y);
  }
  context.textBaseline = "top";
  for (let index = 0; index < xTicks; index += 1) {
    const ratio = index / (xTicks - 1);
    const timestampMs = trend.openMs + ratio * (trend.closeMs - trend.openMs);
    const x = margins.left + ratio * plotWidth;
    context.beginPath();
    context.moveTo(x + 0.5, margins.top);
    context.lineTo(x + 0.5, margins.top + plotHeight);
    context.stroke();
    context.textAlign = index === 0 ? "left" : index === xTicks - 1 ? "right" : "center";
    context.fillText(formatAxisMarketTime(timestampMs), x, margins.top + plotHeight + 11);
  }
  context.strokeStyle = COLORS.ink;
  context.lineWidth = 1;
  context.beginPath();
  context.moveTo(margins.left, margins.top + plotHeight + 0.5);
  context.lineTo(margins.left + plotWidth, margins.top + plotHeight + 0.5);
  context.moveTo(margins.left - 0.5, margins.top);
  context.lineTo(margins.left - 0.5, margins.top + plotHeight);
  context.stroke();
  context.translate(15, margins.top + plotHeight / 2);
  context.rotate(-Math.PI / 2);
  context.textAlign = "center";
  context.textBaseline = "top";
  context.fillStyle = COLORS.muted;
  context.fillText("SPX", 0, 0);
  context.restore();
}

function makeTrendPriceLayer(layout) {
  const canvas = document.createElement("canvas");
  canvas.width = Math.round(layout.width * layout.ratio);
  canvas.height = Math.round(layout.height * layout.ratio);
  const context = canvas.getContext("2d");
  context.setTransform(layout.ratio, 0, 0, layout.ratio, 0, 0);
  return { canvas, context, revealedIndex: -1 };
}

function drawTrendPriceSegment(context, layout, trend, previousIndex, index) {
  if (previousIndex < 0 || index <= previousIndex) return;
  const previousSource = trend.spx.sourceMs[previousIndex];
  const source = trend.spx.sourceMs[index];
  if (source - previousSource > trend.spx.maxLineGapMs) return;
  const x1 = trendX(layout, trend, previousSource);
  const y1 = trendY(layout, trend.spx.prices[previousIndex]);
  const x2 = trendX(layout, trend, source);
  const y2 = trendY(layout, trend.spx.prices[index]);
  const bottom = layout.margins.top + layout.plotHeight;
  context.fillStyle = "rgba(23, 32, 42, 0.025)";
  context.beginPath();
  context.moveTo(x1, y1);
  context.lineTo(x2, y2);
  context.lineTo(x2, bottom);
  context.lineTo(x1, bottom);
  context.closePath();
  context.fill();
  context.lineCap = "round";
  context.lineJoin = "round";
  context.strokeStyle = "rgba(255, 255, 255, 0.94)";
  context.lineWidth = 5;
  context.beginPath();
  context.moveTo(x1, y1);
  context.lineTo(x2, y2);
  context.stroke();
  context.strokeStyle = COLORS.ink;
  context.lineWidth = 2.2;
  context.stroke();
}

function revealTrendPriceThrough(index) {
  if (!app.trend || !app.trendLayout || !app.trendPriceLayer) return;
  const layer = app.trendPriceLayer;
  if (index < layer.revealedIndex) {
    layer.context.clearRect(0, 0, app.trendLayout.width, app.trendLayout.height);
    layer.revealedIndex = -1;
  }
  for (let cursor = Math.max(layer.revealedIndex + 1, 1); cursor <= index; cursor += 1) {
    drawTrendPriceSegment(layer.context, app.trendLayout, app.trend, cursor - 1, cursor);
  }
  layer.revealedIndex = Math.max(layer.revealedIndex, index);
}

function renderTrendStatic() {
  const trend = app.trend;
  if (!trend) {
    clearTrendVisuals();
    return;
  }
  if (trend.metadataOnly) {
    app.trendLayout = null;
    app.trendPriceLayer = null;
    app.trendHit = null;
    drawMetadataReplayDynamic(app.playheadMs ?? trend.openMs, { announce: true });
    return;
  }
  const layout = resizeTrendCanvases();
  const priceExtent = trendPriceExtent(trend);
  layout.yMin = priceExtent.min;
  layout.yMax = priceExtent.max;
  app.trendLayout = layout;
  const context = dom.trendBase.getContext("2d");
  context.clearRect(0, 0, layout.width, layout.height);
  context.fillStyle = "#ffffff";
  context.fillRect(0, 0, layout.width, layout.height);
  drawTrendGammaColumns(context, layout, trend);
  drawTrendAxes(context, layout, trend);
  app.trendPriceLayer = makeTrendPriceLayer(layout);
  dom.trendEmpty.hidden = true;
  app.trendHit = { layout, trend };
  drawTrendDynamic(app.playheadMs ?? trend.openMs, { announce: true });
}

function clearTrendVisuals() {
  for (const canvas of [dom.trendBase, dom.trendOverlay]) {
    const context = canvas.getContext("2d");
    context.clearRect(0, 0, canvas.width, canvas.height);
  }
  dom.trendEmpty.hidden = false;
  dom.trendTooltip.hidden = true;
  dom.trendSpot.textContent = "SPX —";
  dom.trendRegime.textContent = "Γ proxy —";
  dom.trendQuality.className = "quality-chip quality-unknown";
  dom.trendQuality.textContent = "AVAILABLE —";
  dom.trendAccessibleSummary.textContent = "当前没有通过验证的 SPX 盘中走势数据。";
  app.trendLayout = null;
  app.trendPriceLayer = null;
  app.trendHit = null;
  app.activeSpotIndex = -1;
  app.activeGammaIndex = -1;
}

function gammaValueAtSpot(keyframe, spotOffsets, spot) {
  if (!keyframe || !Number.isFinite(spot)) return null;
  const relative = spot - keyframe.referenceSpot;
  if (relative < spotOffsets[0] || relative > spotOffsets.at(-1)) return null;
  const right = spotOffsets.findIndex((offset) => offset >= relative);
  if (right < 0) return null;
  if (spotOffsets[right] === relative || right === 0) return keyframe.values[right];
  const left = right - 1;
  const leftValue = keyframe.values[left];
  const rightValue = keyframe.values[right];
  if (leftValue === null || rightValue === null) return null;
  const ratio = (relative - spotOffsets[left]) / (spotOffsets[right] - spotOffsets[left]);
  return leftValue + (rightValue - leftValue) * ratio;
}

function activeGammaAt(playheadMs) {
  if (!app.trend) return { index: -1, keyframe: null };
  const index = binarySearchLastAtOrBefore(
    app.trend.gamma.keyframes,
    playheadMs,
    (keyframe) => keyframe.atMs,
  );
  const keyframe = index >= 0 ? app.trend.gamma.keyframes[index] : null;
  return {
    index,
    keyframe: keyframe && playheadMs < keyframe.validUntilMs ? keyframe : null,
  };
}

function setTrendQuality(status, labelOverride = null) {
  const normalized = STATUS_LABELS[status] ? status : "unknown";
  const className = `quality-chip quality-${normalized}`;
  const label = labelOverride || STATUS_LABELS[normalized];
  if (dom.trendQuality.className !== className) dom.trendQuality.className = className;
  if (dom.trendQuality.textContent !== label) dom.trendQuality.textContent = label;
}

function renderTrendReplaySummary(heldSpot, keyframe) {
  if (!app.trend) return;
  const status = keyframe?.status || "unavailable";
  const gammaGap = !keyframe;
  setStatusPill(
    gammaGap ? "degraded" : status,
    gammaGap ? "Replay · SPX ready / Gamma gap" : `Replay · ${STATUS_LABELS[status]}`,
  );
  dom.summaryStatus.textContent = gammaGap
    ? "SPX Ready · Gamma gap · Bounded PIT"
    : `${STATUS_LABELS[status]} · Bounded PIT`;
  dom.summaryReasons.textContent = gammaGap
    ? "Gamma keyframe gap · SPX remains observed · availability clock missing"
    : "availability clock missing · hold-last by recorded known_at · dealer side unknown";
  dom.summaryFreshness.textContent = "Frozen · Bounded PIT";
  dom.summaryAsOf.textContent = Number.isFinite(app.playheadMs)
    ? `as of ${formatReplayAsOf(new Date(app.playheadMs))}`
    : "as of —";
  const coverage = keyframe?.coverageRatio;
  dom.summaryCoverage.textContent = coverage === null || coverage === undefined
    ? "—"
    : `${(coverage * 100).toFixed(1)}%`;
  dom.summaryContracts.textContent = `${app.trend.gamma.spotOffsets.length} scenario spots · ${app.trend.gamma.keyframes.length} keyframes`;
  dom.summaryExpiries.textContent = keyframe
    ? `${app.trend.role.toUpperCase()} · ${formatExpiry(keyframe.expiry)}`
    : `${app.trend.role.toUpperCase()} · —`;
  dom.summaryUnderlier.textContent = heldSpot === null
    ? "SPX — · schwab index"
    : `SPX ${heldSpot.toFixed(2)} · schwab index`;
  dom.schemaVersion.textContent = "trend schema 1";
  dom.signConvention.textContent = "calls + / puts − OI proxy; dealer side unknown";
  dom.refreshState.textContent = `Frozen replay · visual ${REPLAY_VISUAL_FPS} fps`;
  setNotice(
    "回放只显示游标时刻前 recorded known_at 已知的 SPX；Gamma 仅在 keyframe valid_until 内 hold-last。Schwab availability clock 缺失，PIT 为 bounded not proven。",
  );
  updateModeChrome();
}

function drawTrendFutureMask(context, layout, cursorX) {
  const right = layout.margins.left + layout.plotWidth;
  const width = Math.max(right - cursorX, 0);
  if (width <= 0) return;
  context.fillStyle = "#f7f8fa";
  context.fillRect(cursorX, layout.margins.top, width, layout.plotHeight);
  hatchCanvasRect(
    context,
    cursorX,
    layout.margins.top,
    width,
    layout.plotHeight,
    "rgba(104, 116, 130, 0.12)",
    { spacing: 12 },
  );
}

function drawMetadataReplayDynamic(
  playheadMs,
  { announce = false, loadSessionSurface = true } = {},
) {
  const trend = app.trend;
  if (!trend?.metadataOnly) return;
  const clamped = Math.max(trend.openMs, Math.min(playheadMs, trend.closeMs));
  app.playheadMs = clamped;
  // A backwards seek must never leave a later cutoff painted while the older
  // session-surface request is in flight.  Keeping an older surface while
  // moving forward is causal; keeping a newer one while moving backward is not.
  if (app.sessionSurface?.asOfMs > clamped) {
    cancelSessionSurfaceRequest({ clear: true });
  }
  const previousIndex = app.activeGammaIndex;
  app.activeGammaIndex = binarySearchLastAtOrBefore(
    trend.gamma.keyframes,
    clamped,
    (keyframe) => keyframe.atMs,
  );
  const chrome = app.replayFrameChrome;
  const clockLabel = formatMarketTime(new Date(clamped));
  if (chrome.clock !== clockLabel) {
    chrome.clock = clockLabel;
    dom.replayFrameTime.textContent = clockLabel;
  }
  const positionLabel = `Keyframe ${Math.max(app.activeGammaIndex + 1, 0)} / ${trend.gamma.keyframes.length}`;
  if (chrome.position !== positionLabel) {
    chrome.position = positionLabel;
    dom.replayFramePosition.textContent = positionLabel;
  }
  const timelineSec = Math.floor(clamped / 1_000);
  if (chrome.timelineSec !== timelineSec) {
    chrome.timelineSec = timelineSec;
    dom.replayTimeline.value = String(timelineSec);
  }
  const ariaLabel = `${clockLabel}, validated keyframe ${Math.max(app.activeGammaIndex + 1, 0)} of ${trend.gamma.keyframes.length}`;
  if (chrome.aria !== ariaLabel) {
    chrome.aria = ariaLabel;
    dom.replayTimeline.setAttribute("aria-valuetext", ariaLabel);
  }
  const surface = app.sessionSurface;
  const spot = cockpitCurrentSpot();
  const surfacePhase = replaySessionSurfacePresentationPhase({
    lastError: app.sessionSurfaceLastError,
    retryKey: app.sessionSurfaceRetryKey,
    hasSurface: Boolean(surface),
  });
  // Summary chrome only changes with the surface, phase, or failure state;
  // skip the DOM writes on identical playback frames.
  const summarySignature = [
    surfacePhase,
    app.sessionSurfaceRenderedKey,
    app.sessionSurfaceLastError,
    app.frames[app.sessionSurfaceKeyframeIndex]?.sessionKind ?? "",
  ].join("|");
  if (summarySignature !== app.replaySummarySignature) {
    app.replaySummarySignature = summarySignature;
    if (surfacePhase === "retrying") {
      renderSessionSurfaceChrome("unavailable", app.sessionSurfaceLastError, {
        retrying: true,
      });
    } else if (surfacePhase === "ready") {
      const presentation = scheduledMissingSessionSurfacePresentation(
        app.frames[app.sessionSurfaceKeyframeIndex],
      );
      setStatusPill(
        presentation.scheduledMissing ? "degraded" : "ready",
        presentation.scheduledMissing ? presentation.status : "Replay · Session surface",
      );
      dom.summaryStatus.textContent = presentation.scheduledMissing
        ? "Scheduled Missing · Frozen · Bounded PIT"
        : "Ready · Frozen · Bounded PIT";
      dom.summaryReasons.textContent = presentation.scheduledMissing
        ? presentation.reason
        : "Session-surface cutoff · dealer side unknown";
      dom.summaryFreshness.textContent = "Frozen · Bounded PIT";
      dom.summaryAsOf.textContent = `as of ${formatReplayAsOf(surface.asOf)}`;
      dom.summaryCoverage.textContent = sessionSurfaceCoverageLabel(surface);
      dom.summaryContracts.textContent = `${surface.strikeProfile.length} strikes · ${surface.candles.length} candles`;
      dom.summaryExpiries.textContent = `${surface.role.toUpperCase()} · ${surface.expiry}`;
      dom.summaryUnderlier.textContent = `SPX ${Number.isFinite(spot) ? spot.toFixed(2) : "—"}`;
      dom.schemaVersion.textContent = `session surface schema ${surface.schemaVersion ?? "—"}`;
      dom.signConvention.textContent = "calls + / puts − proxy; participant and dealer side unknown";
      dom.refreshState.textContent = presentation.scheduledMissing
        ? "Scheduled closed gap · Missing · no fabricated market values"
        : `Frozen replay · cutoff ${formatMarketTime(surface.asOf, false)}`;
    } else {
      setStatusPill("unknown", "Loading session surface");
      dom.summaryStatus.textContent = "Waiting · Frozen replay";
      dom.summaryReasons.textContent = "No cutoff-bound market values loaded";
      dom.summaryFreshness.textContent = "Frozen · pending validation";
      dom.summaryAsOf.textContent = `playhead ${formatReplayAsOf(new Date(clamped))}`;
      dom.summaryCoverage.textContent = "—";
      dom.summaryContracts.textContent = "—";
      dom.summaryExpiries.textContent = "—";
      dom.summaryUnderlier.textContent = "SPX —";
      dom.schemaVersion.textContent = "session surface schema —";
      dom.signConvention.textContent = "participant and dealer side unknown";
      dom.refreshState.textContent = "Waiting for cutoff-bound session surface";
    }
  }
  drawCockpitDynamic();
  if (loadSessionSurface && app.activeGammaIndex !== previousIndex) {
    maybeLoadSessionSurfaceForPlayhead();
  }
  if (announce) {
    dom.trendAccessibleSummary.textContent = `${formatMarketTime(new Date(clamped))}; session values are fetched only from the latest validated frame at or before this playhead.`;
    updateModeChrome();
  }
}

function drawTrendDynamic(
  playheadMs,
  { announce = false, loadSessionSurface = true } = {},
) {
  const trend = app.trend;
  if (trend?.metadataOnly) {
    drawMetadataReplayDynamic(playheadMs, { announce, loadSessionSurface });
    return;
  }
  const layout = app.trendLayout;
  if (!trend || !layout) return;
  const clamped = Math.max(trend.openMs, Math.min(playheadMs, trend.closeMs));
  app.playheadMs = clamped;
  app.activeSpotIndex = binarySearchLastAtOrBefore(trend.spx.knownMs, clamped);
  const previousGammaIndex = app.activeGammaIndex;
  const gamma = activeGammaAt(clamped);
  app.activeGammaIndex = gamma.index;
  revealTrendPriceThrough(app.activeSpotIndex);

  const context = dom.trendOverlay.getContext("2d");
  context.clearRect(0, 0, layout.width, layout.height);
  if (app.trendPriceLayer) {
    context.drawImage(app.trendPriceLayer.canvas, 0, 0, layout.width, layout.height);
  }
  const cursorX = trendX(layout, trend, clamped);
  drawTrendFutureMask(context, layout, cursorX);
  context.save();
  context.setLineDash([5, 4]);
  context.strokeStyle = "rgba(23, 32, 42, 0.86)";
  context.lineWidth = 1.25;
  context.beginPath();
  context.moveTo(cursorX, layout.margins.top);
  context.lineTo(cursorX, layout.margins.top + layout.plotHeight);
  context.stroke();
  context.restore();

  const spotIndex = app.activeSpotIndex;
  let heldSpot = null;
  if (spotIndex >= 0) {
    heldSpot = trend.spx.prices[spotIndex];
    const sourceX = trendX(layout, trend, trend.spx.sourceMs[spotIndex]);
    const priceY = trendY(layout, heldSpot);
    if (cursorX > sourceX) {
      context.save();
      context.setLineDash([3, 4]);
      context.strokeStyle = "rgba(23, 32, 42, 0.58)";
      context.lineWidth = 1.2;
      context.beginPath();
      context.moveTo(sourceX, priceY);
      context.lineTo(cursorX, priceY);
      context.stroke();
      context.restore();
    }
    context.fillStyle = "#ffffff";
    context.beginPath();
    context.arc(sourceX, priceY, 5.5, 0, Math.PI * 2);
    context.fill();
    context.fillStyle = COLORS.ink;
    context.beginPath();
    context.arc(sourceX, priceY, 3.2, 0, Math.PI * 2);
    context.fill();
  }

  const gammaValue = gammaValueAtSpot(gamma.keyframe, trend.gamma.spotOffsets, heldSpot);
  dom.trendSpot.textContent = heldSpot === null ? "SPX —" : `SPX ${heldSpot.toFixed(2)}`;
  dom.trendRegime.textContent = gammaValue === null
    ? "Γ proxy unavailable"
    : `${gammaValue > 0 ? "+" : gammaValue < 0 ? "−" : "0"}Γ proxy · ${compactNumber(gammaValue, 2)}`;
  const activeStatus = gamma.keyframe?.status || (gamma.index >= 0 ? "unavailable" : "unknown");
  setTrendQuality(activeStatus, gamma.keyframe ? null : "Gamma gap");
  dom.replayFrameTime.textContent = formatMarketTime(new Date(clamped));
  dom.replayFramePosition.textContent = `Keyframe ${gamma.index >= 0 ? gamma.index + 1 : 0} / ${trend.gamma.keyframes.length}`;
  dom.replayTimeline.value = String(Math.floor(clamped / 1_000));
  dom.replayTimeline.setAttribute(
    "aria-valuetext",
    `${formatMarketTime(new Date(clamped))}, validated keyframe ${gamma.index >= 0 ? gamma.index + 1 : 0} of ${trend.gamma.keyframes.length}`,
  );
  if (announce) {
    dom.trendAccessibleSummary.textContent = `${formatMarketTime(new Date(clamped))}，${heldSpot === null ? "SPX 价格不可用" : `SPX ${heldSpot.toFixed(2)}`}，${gammaValue === null ? "Gamma proxy 不可用" : `Gamma proxy ${compactNumber(gammaValue, 2)}`}。图中仅显示回放游标前已知数据；Y 轴以首个已知 SPX 观测的固定窗口设定，不使用未来日内高低点。`;
    renderTrendReplaySummary(heldSpot, gamma.keyframe);
  }
  drawCockpitDynamic();
  if (loadSessionSurface && gamma.index !== previousGammaIndex) {
    maybeLoadSessionSurfaceForPlayhead();
  }
}

function trendTooltipForEvent(event) {
  if (!app.trendHit || !app.trend || !Number.isFinite(app.playheadMs)) {
    dom.trendTooltip.hidden = true;
    return;
  }
  const rect = dom.trendOverlay.getBoundingClientRect();
  const x = event.clientX - rect.left;
  const y = event.clientY - rect.top;
  const { layout, trend } = app.trendHit;
  const plotLeft = layout.margins.left;
  const plotRight = plotLeft + layout.plotWidth;
  const plotTop = layout.margins.top;
  const plotBottom = plotTop + layout.plotHeight;
  if (x < plotLeft || x > plotRight || y < plotTop || y > plotBottom) {
    dom.trendTooltip.hidden = true;
    return;
  }
  const pointedMs = trend.openMs + ((x - plotLeft) / layout.plotWidth) *
    (trend.closeMs - trend.openMs);
  if (pointedMs > app.playheadMs) {
    dom.trendTooltip.hidden = true;
    return;
  }
  const knownIndex = binarySearchLastAtOrBefore(trend.spx.knownMs, pointedMs);
  const sourceIndex = Math.min(
    knownIndex,
    binarySearchLastAtOrBefore(trend.spx.sourceMs, pointedMs),
  );
  const gammaIndex = binarySearchLastAtOrBefore(
    trend.gamma.keyframes,
    pointedMs,
    (keyframe) => keyframe.atMs,
  );
  const candidateGamma = gammaIndex >= 0 ? trend.gamma.keyframes[gammaIndex] : null;
  const keyframe = candidateGamma && pointedMs < candidateGamma.validUntilMs
    ? candidateGamma
    : null;
  const pointedSpot = layout.yMax - ((y - plotTop) / layout.plotHeight) *
    (layout.yMax - layout.yMin);
  const gammaValue = gammaValueAtSpot(keyframe, trend.gamma.spotOffsets, pointedSpot);
  const title = document.createElement("strong");
  title.textContent = formatMarketTime(new Date(pointedMs));
  const price = document.createElement("span");
  price.textContent = sourceIndex >= 0
    ? `SPX ${trend.spx.prices[sourceIndex].toFixed(2)} · observed ${formatMarketTime(new Date(trend.spx.sourceMs[sourceIndex]), false)} · known ${formatMarketTime(new Date(trend.spx.knownMs[sourceIndex]), false)}`
    : "SPX not known at this replay time";
  const gamma = document.createElement("span");
  gamma.textContent = keyframe
    ? `${METRICS[trend.metric].label} @ SPX ${pointedSpot.toFixed(2)}: ${gammaValue === null ? "Unavailable" : `${compactNumber(gammaValue, 3)} ${trend.gamma.metricUnit}`}`
    : "Gamma keyframe gap / unavailable";
  dom.trendTooltip.replaceChildren(title, price, gamma);
  dom.trendTooltip.hidden = false;
  const tooltipWidth = Math.min(330, rect.width - 16);
  const left = Math.min(Math.max(x + 13, 8), Math.max(rect.width - tooltipWidth - 8, 8));
  const top = Math.min(Math.max(y + 13, 8), Math.max(rect.height - 105, 8));
  dom.trendTooltip.style.width = `${tooltipWidth}px`;
  dom.trendTooltip.style.left = `${left}px`;
  dom.trendTooltip.style.top = `${top}px`;
}

function extent(values) {
  const numeric = values.filter((value) => value !== null && Number.isFinite(value));
  if (!numeric.length) return { min: 0, max: 0, domain: 0 };
  const min = Math.min(...numeric);
  const max = Math.max(...numeric);
  return { min, max, domain: Math.max(Math.abs(min), Math.abs(max)) };
}

function mixColor(from, to, amount) {
  const ratio = Math.max(0, Math.min(amount, 1));
  return `rgb(${from.map((value, index) => Math.round(value + (to[index] - value) * ratio)).join(" ")})`;
}

function cellColor(value, domain, signed) {
  if (value === null) return COLORS.missing;
  if (domain <= 0) return mixColor(COLORS.neutral, COLORS.neutral, 0);
  if (!signed) return mixColor(COLORS.neutral, COLORS.positive, Math.max(value, 0) / domain);
  if (value < 0) return mixColor(COLORS.neutral, COLORS.negative, Math.abs(value) / domain);
  return mixColor(COLORS.neutral, COLORS.positive, value / domain);
}

function drawHeatmap(view) {
  const { context, width, height } = resizeCanvas(dom.heatmap);
  context.clearRect(0, 0, width, height);
  const mobile = width < 620;
  const margins = { left: mobile ? 56 : 72, right: 22, top: 24, bottom: 56 };
  const plotWidth = Math.max(width - margins.left - margins.right, 1);
  const plotHeight = Math.max(height - margins.top - margins.bottom, 1);
  const cellWidth = plotWidth / view.spots.length;
  const cellHeight = plotHeight / view.rows.length;
  const allValues = view.rows.flatMap((row) => row.values);
  const bounds = extent(allValues);
  const signed = METRICS[app.metric].signed;
  const domain = signed ? bounds.domain : Math.max(bounds.max, 0);

  context.fillStyle = "#ffffff";
  context.fillRect(0, 0, width, height);
  for (let rowIndex = 0; rowIndex < view.rows.length; rowIndex += 1) {
    const row = view.rows[rowIndex];
    for (let column = 0; column < view.spots.length; column += 1) {
      const value = row.values[column];
      const x = margins.left + column * cellWidth;
      const y = margins.top + rowIndex * cellHeight;
      context.fillStyle = cellColor(value, domain, signed);
      context.fillRect(x, y, Math.ceil(cellWidth) + 0.25, Math.ceil(cellHeight) + 0.25);
      context.strokeStyle = "rgba(23, 32, 42, 0.07)";
      context.lineWidth = 0.5;
      context.strokeRect(x, y, cellWidth, cellHeight);

      if (value === null) {
        context.beginPath();
        context.moveTo(x + 2, y + cellHeight - 2);
        context.lineTo(x + Math.min(cellWidth - 2, cellHeight - 2), y + 2);
        context.strokeStyle = "rgba(104, 116, 130, 0.28)";
        context.stroke();
      } else if (cellWidth >= 22 && cellHeight >= 18) {
        context.fillStyle = "rgba(23, 32, 42, 0.7)";
        context.font = "700 10px ui-monospace, monospace";
        context.textAlign = "center";
        context.textBaseline = "middle";
        const glyph = signed
          ? value > 0 ? "+" : value < 0 ? "−" : "0"
          : value > 0 ? "•" : "0";
        context.fillText(glyph, x + cellWidth / 2, y + cellHeight / 2);
      }
    }
  }

  drawAxes(context, view, margins, plotWidth, plotHeight, width, height);
  if (app.metric === "signed_gamma") {
    drawSignedOverlays(context, view, margins, cellWidth, cellHeight);
  }
  updateLegend({ domain, signed, unit: view.unit });

  app.chartHit = { view, margins, plotWidth, plotHeight, cellWidth, cellHeight, width, height };
  const coverage = currentCoverage(view, selectedExpiry());
  const replayAsOf = app.snapshot?.mode === "replay" ? formatReplayAsOf(app.snapshot.requestedAsOf) : "";
  dom.accessibleSummary.textContent = `${isReplayView() ? `历史回放，Frozen，Not live，as of ${replayAsOf}。` : ""}${METRICS[app.metric].label} 曲面，${view.spots.length} 个 spot 场景，${view.rows.length} 个时间切片，覆盖率 ${coverage.ratio === null ? "未知" : `${(coverage.ratio * 100).toFixed(1)}%`}，色域 ${signed ? `正负对称 ±${compactNumber(domain)}` : `0 到 ${compactNumber(domain)}`}。`;
}

function drawAxes(context, view, margins, plotWidth, plotHeight, width, height) {
  context.strokeStyle = COLORS.ink;
  context.lineWidth = 1;
  context.beginPath();
  context.moveTo(margins.left, margins.top + plotHeight + 0.5);
  context.lineTo(margins.left + plotWidth, margins.top + plotHeight + 0.5);
  context.moveTo(margins.left - 0.5, margins.top);
  context.lineTo(margins.left - 0.5, margins.top + plotHeight);
  context.stroke();

  context.fillStyle = COLORS.muted;
  context.font = "11px ui-monospace, SFMono-Regular, monospace";
  context.textBaseline = "top";
  const targetTickCount = plotWidth < 520 ? 4 : 7;
  const xStep = Math.max(Math.ceil((view.spots.length - 1) / (targetTickCount - 1)), 1);
  const tickIndexes = [];
  for (let index = 0; index < view.spots.length; index += xStep) {
    tickIndexes.push(index);
  }
  if (tickIndexes.at(-1) !== view.spots.length - 1) tickIndexes.push(view.spots.length - 1);
  for (const index of tickIndexes) {
    const x = margins.left + (index + 0.5) * (plotWidth / view.spots.length);
    context.textAlign = "center";
    context.fillText(formatSpotAxis(view.spots[index]), x, margins.top + plotHeight + 8);
  }

  const yStep = Math.max(Math.ceil(view.rows.length / 6), 1);
  context.textBaseline = "middle";
  for (let index = 0; index < view.rows.length; index += yStep) {
    const y = margins.top + (index + 0.5) * (plotHeight / view.rows.length);
    context.textAlign = "right";
    context.fillText(formatMinutes(view.rows[index].minutesForward), margins.left - 9, y);
  }

  context.fillStyle = COLORS.ink;
  context.font = "600 11px ui-sans-serif, system-ui, sans-serif";
  context.textAlign = "center";
  context.textBaseline = "bottom";
  context.fillText("SPX scenario spot", margins.left + plotWidth / 2, height - 5);
  context.save();
  context.translate(14, margins.top + plotHeight / 2);
  context.rotate(-Math.PI / 2);
  context.fillText("Minutes forward", 0, 0);
  context.restore();

  context.strokeStyle = "rgba(23, 32, 42, 0.08)";
  context.setLineDash([2, 5]);
  context.beginPath();
  context.moveTo(margins.left, margins.top + plotHeight / 2);
  context.lineTo(width - margins.right, margins.top + plotHeight / 2);
  context.stroke();
  context.setLineDash([]);
}

function xForSpot(spot, spots, margins, cellWidth) {
  if (!Number.isFinite(spot) || spot < spots[0] || spot > spots.at(-1)) return null;
  let right = spots.findIndex((candidate) => candidate >= spot);
  if (right <= 0) return margins.left + cellWidth / 2;
  if (right === -1) right = spots.length - 1;
  const left = right - 1;
  const span = spots[right] - spots[left];
  const fraction = span > 0 ? (spot - spots[left]) / span : 0;
  return margins.left + (left + 0.5 + fraction) * cellWidth;
}

function drawSignedOverlays(context, view, margins, cellWidth, cellHeight) {
  const ridge = view.rows
    .map((row, index) => ({
      x: xForSpot(row.zeroRidgeSpot, view.spots, margins, cellWidth),
      y: margins.top + (index + 0.5) * cellHeight,
    }))
    .filter((point) => point.x !== null);
  if (ridge.length) {
    context.strokeStyle = "#273542";
    context.lineWidth = 1.7;
    context.setLineDash([6, 5]);
    context.beginPath();
    ridge.forEach((point, index) => {
      if (index === 0) context.moveTo(point.x, point.y);
      else context.lineTo(point.x, point.y);
    });
    context.stroke();
    context.setLineDash([]);
    context.fillStyle = "#ffffff";
    context.strokeStyle = "#273542";
    for (const point of ridge) {
      context.beginPath();
      context.rect(point.x - 3, point.y - 3, 6, 6);
      context.fill();
      context.stroke();
    }
  }

  view.rows.forEach((row, index) => {
    drawExtremaMarker(context, row.positivePeak, "+", COLORS.positiveDark, index, view, margins, cellWidth, cellHeight);
    drawExtremaMarker(context, row.negativeTrough, "−", COLORS.negativeDark, index, view, margins, cellWidth, cellHeight);
  });
}

function drawExtremaMarker(context, point, glyph, color, rowIndex, view, margins, cellWidth, cellHeight) {
  if (!point || point.spot === null || point.value === null) return;
  const x = xForSpot(point.spot, view.spots, margins, cellWidth);
  if (x === null) return;
  const y = margins.top + (rowIndex + 0.5) * cellHeight;
  context.fillStyle = "rgba(255, 255, 255, 0.92)";
  context.strokeStyle = color;
  context.lineWidth = 1.6;
  context.beginPath();
  context.arc(x, y, 7, 0, Math.PI * 2);
  context.fill();
  context.stroke();
  context.fillStyle = color;
  context.font = "800 10px ui-monospace, monospace";
  context.textAlign = "center";
  context.textBaseline = "middle";
  context.fillText(glyph, x, y + 0.5);
}

function updateLegend(domainState) {
  const metric = METRICS[app.metric];
  const signedGamma = app.metric === "signed_gamma";
  dom.legendNegative.hidden = !metric.signed;
  dom.legendRidge.hidden = !signedGamma;
  dom.legendPeak.hidden = !signedGamma;
  dom.legendTrough.hidden = !signedGamma;
  setLegendText(dom.legendNeutral, metric.signed ? "中性" : "0 / 较低");
  setLegendText(dom.legendPositive, metric.signed ? "正暴露" : "较高 gross gamma");
  setLegendGlyph(dom.legendPositive, metric.signed ? "+" : "•");
  if (!domainState) {
    dom.legendDomain.textContent = "色域 —";
  } else if (domainState.signed) {
    dom.legendDomain.textContent = `对称色域 −${compactNumber(domainState.domain)} ↔ +${compactNumber(domainState.domain)} ${domainState.unit}`;
  } else {
    dom.legendDomain.textContent = `顺序色域 0 → ${compactNumber(domainState.domain)} ${domainState.unit}`;
  }
}

function setLegendText(element, value) {
  const textNode = Array.from(element.childNodes).find((node) => node.nodeType === Node.TEXT_NODE);
  if (textNode) textNode.textContent = ` ${value}`;
}

function setLegendGlyph(element, value) {
  const glyph = element.querySelector("b");
  if (glyph) glyph.textContent = value;
}

function formatMinutes(minutes) {
  if (!Number.isFinite(minutes)) return "—";
  if (minutes === 0) return "Now";
  if (Math.abs(minutes) < 60) return `${minutes > 0 ? "+" : ""}${minutes}m`;
  const hours = minutes / 60;
  return `${minutes > 0 ? "+" : ""}${Number.isInteger(hours) ? hours : hours.toFixed(1)}h`;
}

function formatSpotAxis(spot) {
  if (!Number.isFinite(spot)) return "—";
  return Number.isInteger(spot) ? String(spot) : spot.toFixed(1);
}

function tooltipForHit(hit, event) {
  const rect = dom.heatmap.getBoundingClientRect();
  const x = event.clientX - rect.left;
  const y = event.clientY - rect.top;
  const column = Math.floor((x - hit.margins.left) / hit.cellWidth);
  const rowIndex = Math.floor((y - hit.margins.top) / hit.cellHeight);
  if (column < 0 || column >= hit.view.spots.length || rowIndex < 0 || rowIndex >= hit.view.rows.length) {
    dom.heatmapTooltip.hidden = true;
    return;
  }
  const row = hit.view.rows[rowIndex];
  const value = row.values[column];
  const title = document.createElement("strong");
  title.textContent = `SPX ${hit.view.spots[column].toFixed(2)} · ${formatMinutes(row.minutesForward)}`;
  const metric = document.createElement("span");
  metric.textContent = `${METRICS[app.metric].label}: ${value === null ? "Unavailable" : compactNumber(value, 3)} ${hit.view.unit}`;
  const quality = document.createElement("span");
  quality.textContent = `quality: ${STATUS_LABELS[row.quality] || row.quality}`;
  dom.heatmapTooltip.replaceChildren(title, metric, quality);
  dom.heatmapTooltip.hidden = false;
  const tooltipWidth = 190;
  const left = Math.min(Math.max(x + 13, 8), Math.max(rect.width - tooltipWidth - 8, 8));
  const top = Math.min(Math.max(y + 13, 8), Math.max(rect.height - 88, 8));
  dom.heatmapTooltip.style.left = `${left}px`;
  dom.heatmapTooltip.style.top = `${top}px`;
}

function ladderMetricValue(row, weightingKey, metricKey) {
  const weightings = isObject(row.weightings) ? row.weightings : {};
  const weighting = isObject(weightings[weightingKey])
    ? weightings[weightingKey]
    : isObject(row[weightingKey])
      ? row[weightingKey]
      : null;
  return metricScalar(weighting, metricKey);
}

function ladderContext(row) {
  const call = isObject(row.call) ? row.call : {};
  const put = isObject(row.put) ? row.put : {};
  const callOi = finiteNumber(call.open_interest);
  const putOi = finiteNumber(put.open_interest);
  const callVolume = finiteNumber(call.volume);
  const putVolume = finiteNumber(put.volume);
  return `Call OI ${callOi ?? "—"}, Put OI ${putOi ?? "—"}, Call volume ${callVolume ?? "—"}, Put volume ${putVolume ?? "—"}`;
}

function drawLadder(expiry, view) {
  const rows = (expiry?.ladder || [])
    .map((row) => ({
      raw: row,
      strike: finiteNumber(row.strike),
      value: ladderMetricValue(row, app.weighting, app.metric),
    }))
    .filter((row) => row.strike !== null && row.value !== null)
    .sort((left, right) => left.strike - right.strike);
  if (!rows.length) {
    clearLadder();
    return;
  }

  const reference = finiteNumber(expiry.surface?.referenceSpot) ?? finiteNumber(app.snapshot?.underlier.price);
  let selected = rows;
  const limit = 17;
  if (rows.length > limit) {
    const anchor = reference ?? rows[Math.floor(rows.length / 2)].strike;
    selected = rows
      .slice()
      .sort((left, right) => Math.abs(left.strike - anchor) - Math.abs(right.strike - anchor))
      .slice(0, limit)
      .sort((left, right) => left.strike - right.strike);
  }

  dom.ladderEmpty.hidden = true;
  dom.ladderSubtitle.textContent = `${selected.length} 个真实合约 strike${reference === null ? "" : ` · reference SPX ${reference.toFixed(2)}`} · ${view.unit}`;
  const width = Math.max(dom.ladderChart.clientWidth, 420);
  const rowHeight = 21;
  const height = 38 + selected.length * rowHeight;
  const labelWidth = 62;
  const valueWidth = 82;
  const chartLeft = labelWidth + 8;
  const chartRight = width - valueWidth - 8;
  const chartWidth = Math.max(chartRight - chartLeft, 100);
  const signed = METRICS[app.metric].signed;
  const bounds = extent(selected.map((row) => row.value));
  const domain = signed ? bounds.domain : Math.max(bounds.max, 0);
  const zeroX = signed ? chartLeft + chartWidth / 2 : chartLeft;
  dom.ladderChart.setAttribute("viewBox", `0 0 ${width} ${height}`);
  dom.ladderChart.setAttribute("height", String(height));
  dom.ladderChart.replaceChildren();

  appendSvg("line", {
    x1: zeroX,
    y1: 18,
    x2: zeroX,
    y2: height - 8,
    stroke: COLORS.ink,
    "stroke-width": 1,
    "stroke-dasharray": "3 4",
  });

  selected.forEach((row, index) => {
    const y = 24 + index * rowHeight;
    const ratio = domain > 0 ? Math.min(Math.abs(row.value) / domain, 1) : 0;
    const maxBar = signed ? chartWidth / 2 : chartWidth;
    const barWidth = ratio * maxBar;
    const x = signed && row.value < 0 ? zeroX - barWidth : zeroX;
    const fill = row.value < 0 ? "#d96459" : "#2f6fad";
    const rect = appendSvg("rect", {
      x,
      y,
      width: Math.max(barWidth, row.value === 0 ? 1 : 0),
      height: 13,
      rx: 2,
      fill,
      opacity: row.value === 0 ? 0.45 : 0.88,
    });
    const title = document.createElementNS(SVG_NS, "title");
    title.textContent = `${row.strike}: ${compactNumber(row.value, 3)} ${view.unit}; ${ladderContext(row.raw)}`;
    rect.append(title);
    appendSvgText(5, y + 10, row.strike.toFixed(0), "start", "#17202a", "11px");
    appendSvgText(width - 5, y + 10, compactNumber(row.value, 2), "end", row.value < 0 ? COLORS.negativeDark : COLORS.positiveDark, "11px");
  });
}

function appendSvg(tag, attributes) {
  const node = document.createElementNS(SVG_NS, tag);
  for (const [name, value] of Object.entries(attributes)) node.setAttribute(name, String(value));
  dom.ladderChart.append(node);
  return node;
}

function appendSvgText(x, y, value, anchor, fill, size) {
  const text = appendSvg("text", {
    x,
    y,
    fill,
    "font-size": size,
    "font-family": "ui-monospace, SFMono-Regular, Menlo, monospace",
    "text-anchor": anchor,
  });
  text.textContent = value;
}

function clearLadder() {
  dom.ladderChart.replaceChildren();
  dom.ladderEmpty.hidden = false;
  dom.ladderSubtitle.textContent = "快照未提供可用的真实 strike ladder";
}

function renderExtrema(view) {
  if (!view) {
    setExtremaHeadings();
    renderExtremaList(dom.peakList, [], "暂无数据");
    renderExtremaList(dom.troughList, [], "暂无数据");
    return;
  }
  setExtremaHeadings();
  const points = view.rows.flatMap((row) =>
    row.values.map((value, index) => ({
      value,
      spot: view.spots[index],
      minutesForward: row.minutesForward,
    })),
  ).filter((point) => point.value !== null);
  if (app.metric === "gross_gamma") {
    const highest = points.slice().sort((a, b) => b.value - a.value).slice(0, 5);
    const lowest = points.slice().sort((a, b) => a.value - b.value).slice(0, 5);
    renderExtremaList(dom.peakList, highest, "暂无 gross gamma 数据");
    renderExtremaList(dom.troughList, lowest, "暂无 gross gamma 数据");
    return;
  }
  const positives = points.filter((point) => point.value > 0).sort((a, b) => b.value - a.value).slice(0, 5);
  const negatives = points.filter((point) => point.value < 0).sort((a, b) => a.value - b.value).slice(0, 5);
  renderExtremaList(dom.peakList, positives, "没有正值");
  renderExtremaList(
    dom.troughList,
    negatives,
    METRICS[app.metric].signed ? "没有负值" : "Gross gamma 为非负指标",
  );
}

function setExtremaHeadings() {
  const gross = app.metric === "gross_gamma";
  dom.peakHeading.replaceChildren();
  dom.troughHeading.replaceChildren();
  if (gross) {
    dom.peakHeading.textContent = "Highest gross gamma";
    dom.troughHeading.textContent = "Lowest gross gamma";
    dom.extremaSubtitle.textContent = "Gross gamma 为非负质量指标；分别列出最高/最低值，不推断 dealer 方向。";
    return;
  }
  dom.extremaSubtitle.textContent = "按所选指标绝对幅度排序；数值不代表已知 dealer 实仓方向。";
  const positive = document.createElement("span");
  positive.className = "extrema-symbol positive";
  positive.textContent = "+";
  const negative = document.createElement("span");
  negative.className = "extrema-symbol negative";
  negative.textContent = "−";
  dom.peakHeading.append(positive, document.createTextNode(" Positive peaks"));
  dom.troughHeading.append(negative, document.createTextNode(" Negative troughs"));
}

function renderExtremaList(container, points, emptyLabel) {
  container.replaceChildren();
  if (!points.length) {
    const item = document.createElement("li");
    item.className = "empty-row";
    item.textContent = emptyLabel;
    container.append(item);
    return;
  }
  for (const point of points) {
    const item = document.createElement("li");
    const location = document.createElement("span");
    location.textContent = `SPX ${point.spot.toFixed(2)} · ${formatMinutes(point.minutesForward)}`;
    const value = document.createElement("strong");
    value.textContent = compactNumber(point.value, 3);
    item.append(location, value);
    container.append(item);
  }
}

function validateReplayCatalogContract(raw) {
  if (
    !isObject(raw) ||
    raw.schema_version !== 1 ||
    raw.kind !== REPLAY_CATALOG_KIND ||
    raw.provider !== "schwab" ||
    raw.coordinate !== "SPX" ||
    raw.trading_class !== "SPXW" ||
    raw.frame_interval_minutes !== REPLAY_TIMELINE_STEP_MINUTES ||
    raw.timeline_policy_version !== REPLAY_TIMELINE_POLICY_VERSION ||
    raw.availability_proven !== false ||
    raw.availability_clock !== "unavailable" ||
    raw.point_in_time_confidence !== "bounded_not_proven" ||
    raw.frame_validation !== REPLAY_FRAME_VALIDATION ||
    raw.only_close_grace_elapsed_sessions !== true ||
    raw.session_close_grace_policy !== REPLAY_CLOSE_GRACE_POLICY ||
    raw.session_close_grace_seconds !== REPLAY_CLOSE_GRACE_SECONDS ||
    raw.data_finalization_proven !== false ||
    !sha256String(raw.projection_policy_sha256)
  ) {
    throw new Error("invalid_replay_catalog_contract");
  }
  return raw.projection_policy_sha256;
}

function validateCloseGraceClock(closeAt, elapsedAt) {
  return closeAt instanceof Date && elapsedAt instanceof Date &&
    elapsedAt.getTime() - closeAt.getTime() === REPLAY_CLOSE_GRACE_SECONDS * 1_000;
}

function normalizeReplaySessions(raw) {
  const projectionPolicySha256 = validateReplayCatalogContract(raw);
  if (!Array.isArray(raw.sessions)) throw new Error("invalid_replay_sessions_catalog");
  const dates = new Set();
  const sessions = raw.sessions.map((item) => {
    if (!isObject(item)) throw new Error("invalid_replay_session");
    const date = nonEmptyString(item.date) || nonEmptyString(item.session_date);
    if (!date || !/^\d{4}-\d{2}-\d{2}$/.test(date) || dates.has(date)) {
      throw new Error("invalid_replay_session_date");
    }
    dates.add(date);
    const frameCount = finiteNumber(item.frame_count);
    if (frameCount !== null && (!Number.isInteger(frameCount) || frameCount < 0)) {
      throw new Error("invalid_replay_session_frame_count");
    }
    const surfaceFrameCount = Object.prototype.hasOwnProperty.call(item, "surface_frame_count")
      ? finiteNumber(item.surface_frame_count)
      : null;
    const surfaceTimelineStatus = Object.prototype.hasOwnProperty.call(
      item,
      "surface_timeline_status",
    )
      ? nonEmptyString(item.surface_timeline_status)
      : null;
    if ((surfaceFrameCount !== null &&
        (!Number.isSafeInteger(surfaceFrameCount) || surfaceFrameCount < 1)) ||
        (Object.prototype.hasOwnProperty.call(item, "surface_frame_count") &&
          surfaceFrameCount === null) ||
        (Object.prototype.hasOwnProperty.call(item, "surface_timeline_status") &&
          !surfaceTimelineStatus)) {
      throw new Error("invalid_replay_session_surface_timeline_summary");
    }
    const closeAt = parseDate(item.close_at);
    const closeGraceElapsedAt = parseDate(item.session_close_grace_elapsed_at);
    if (
      item.session_close_grace_elapsed !== true ||
      item.data_finalization_proven !== false ||
      item.frame_interval_minutes !== REPLAY_TIMELINE_STEP_MINUTES ||
      item.projection_policy_sha256 !== projectionPolicySha256 ||
      closeAt?.toISOString().slice(0, 10) !== date ||
      !validateCloseGraceClock(closeAt, closeGraceElapsedAt)
    ) {
      throw new Error("invalid_replay_session_contract");
    }
    return {
      raw: item,
      date,
      label: nonEmptyString(item.label) || date,
      frameCount,
      surfaceFrameCount,
      surfaceTimelineStatus,
      status: normalizedStatus(item.status),
    };
  }).sort((left, right) => right.date.localeCompare(left.date));
  return { sessions, projectionPolicySha256 };
}

async function normalizeReplayTimeline(raw, sessionDate, expectedProjectionPolicySha256) {
  if (
    !isObject(raw) ||
    raw.schema_version !== 1 ||
    raw.kind !== REPLAY_CATALOG_KIND ||
    raw.session_date !== sessionDate ||
    raw.provider !== "schwab" ||
    raw.coordinate !== "SPX" ||
    raw.trading_class !== "SPXW" ||
    raw.frame_interval_minutes !== REPLAY_TIMELINE_STEP_MINUTES ||
    raw.step_minutes !== REPLAY_TIMELINE_STEP_MINUTES ||
    raw.timeline_policy_version !== REPLAY_TIMELINE_POLICY_VERSION ||
    raw.availability_proven !== false ||
    raw.availability_clock !== "unavailable" ||
    raw.point_in_time_confidence !== "bounded_not_proven" ||
    raw.frame_validation !== REPLAY_FRAME_VALIDATION ||
    raw.only_close_grace_elapsed_sessions !== true ||
    raw.session_close_grace_elapsed !== true ||
    raw.session_close_grace_policy !== REPLAY_CLOSE_GRACE_POLICY ||
    raw.session_close_grace_seconds !== REPLAY_CLOSE_GRACE_SECONDS ||
    raw.data_finalization_proven !== false ||
    !sha256String(expectedProjectionPolicySha256) ||
    raw.projection_policy_sha256 !== expectedProjectionPolicySha256 ||
    !sha256String(raw.timeline_sha256) ||
    !sha256String(raw.source_fingerprint) ||
    !Array.isArray(raw.frames)
  ) {
    throw new Error("invalid_replay_timeline");
  }
  const openAt = parseDate(raw.open_at);
  const closeAt = parseDate(raw.close_at);
  const closeGraceElapsedAt = parseDate(raw.session_close_grace_elapsed_at);
  if (!openAt || !closeAt || openAt >= closeAt ||
      openAt.toISOString().slice(0, 10) !== sessionDate ||
      closeAt.toISOString().slice(0, 10) !== sessionDate ||
      !validateCloseGraceClock(closeAt, closeGraceElapsedAt)) {
    throw new Error("invalid_replay_timeline_session_clock");
  }
  const seen = new Set();
  let previousAt = null;
  const frames = raw.frames.map((item) => {
    if (!isObject(item)) throw new Error("invalid_replay_timeline_frame");
    const at = parseDate(item.at || item.requested_as_of);
    if (!at || at.getUTCMilliseconds() !== 0 || at < openAt || at >= closeAt ||
        seen.has(at.getTime()) || (previousAt && at <= previousAt)) {
      throw new Error("invalid_replay_timeline_clock");
    }
    seen.add(at.getTime());
    previousAt = at;
    const id = nonEmptyString(item.id) || nonEmptyString(item.replay_id);
    const artifactSha256 = nonEmptyString(item.artifact_sha256);
    const projectionPolicySha256 = nonEmptyString(item.projection_policy_sha256);
    const expectedId = formatIsoUtc(at).replaceAll(":", "");
    if (id !== expectedId) throw new Error("invalid_replay_timeline_id");
    if (artifactSha256 && !sha256String(artifactSha256)) throw new Error("invalid_timeline_artifact_hash");
    if (!sha256String(projectionPolicySha256) ||
        projectionPolicySha256 !== expectedProjectionPolicySha256) {
      throw new Error("invalid_timeline_policy_hash");
    }
    return {
      raw: item,
      at,
      id,
      label: nonEmptyString(item.label) || nonEmptyString(item.label_et) || formatMarketTime(at),
      url: nonEmptyString(item.url) || nonEmptyString(item.frame_url),
      cached: item.cached === true,
      status: normalizedStatus(item.status),
      artifactSha256,
      projectionPolicySha256,
    };
  });
  if (raw.frame_count !== frames.length) throw new Error("invalid_replay_timeline_frame_count");
  if (await canonicalReplaySha256(frames.map((frame) => frame.id)) !== raw.timeline_sha256) {
    throw new Error("invalid_replay_timeline_hash");
  }

  const surfaceFieldNames = [
    "surface_open_at",
    "surface_close_at",
    "surface_provider",
    "surface_frame_interval_minutes",
    "surface_frame_count",
    "surface_timeline_sha256",
    "session_segments",
    "surface_frames",
  ];
  const suppliedSurfaceFieldCount = surfaceFieldNames.filter((field) =>
    Object.prototype.hasOwnProperty.call(raw, field)).length;
  if (suppliedSurfaceFieldCount !== 0 && suppliedSurfaceFieldCount !== surfaceFieldNames.length) {
    throw new Error("incomplete_replay_surface_timeline_contract");
  }

  let surfaceFrames = frames;
  let surfaceOpenAt = openAt;
  let surfaceCloseAt = closeAt;
  let surfaceTimelineSha256 = raw.timeline_sha256;
  let surfaceSegments = [];
  let surfaceTimelineExtended = false;
  if (suppliedSurfaceFieldCount === surfaceFieldNames.length) {
    surfaceOpenAt = parseDate(raw.surface_open_at);
    surfaceCloseAt = parseDate(raw.surface_close_at);
    const surfaceStepMinutes = raw.surface_frame_interval_minutes;
    if (!surfaceOpenAt || !surfaceCloseAt || surfaceOpenAt >= surfaceCloseAt ||
        surfaceCloseAt.getTime() !== closeAt.getTime() ||
        surfaceCloseAt.toISOString().slice(0, 10) !== sessionDate ||
        raw.surface_provider !== "mixed" ||
        surfaceStepMinutes !== REPLAY_TIMELINE_STEP_MINUTES ||
        !Number.isSafeInteger(raw.surface_frame_count) || raw.surface_frame_count < 1 ||
        !sha256String(raw.surface_timeline_sha256) ||
        !Array.isArray(raw.surface_frames)) {
      throw new Error("invalid_replay_surface_timeline_contract");
    }
    surfaceSegments = normalizeSessionSegments(
      raw.session_segments,
      surfaceOpenAt.getTime(),
      surfaceCloseAt.getTime(),
      2,
    );
    if (surfaceSegments.at(-1)?.startMs !== openAt.getTime()) {
      throw new Error("invalid_replay_surface_rth_alignment");
    }
    const surfaceStepMs = surfaceStepMinutes * 60_000;
    const surfaceSeen = new Set();
    let previousSurfaceAt = null;
    surfaceFrames = raw.surface_frames.map((item, index) => {
      if (!isObject(item)) throw new Error("invalid_replay_surface_timeline_frame");
      const at = parseDate(item.at);
      const requestedAsOf = parseDate(item.requested_as_of);
      if (!at || !requestedAsOf || requestedAsOf.getTime() !== at.getTime() ||
          at.getUTCMilliseconds() !== 0 || at <= surfaceOpenAt || at > surfaceCloseAt ||
          surfaceSeen.has(at.getTime()) ||
          (previousSurfaceAt && at.getTime() - previousSurfaceAt.getTime() !== surfaceStepMs) ||
          (!previousSurfaceAt && at.getTime() !== surfaceOpenAt.getTime() + surfaceStepMs)) {
        throw new Error("invalid_replay_surface_timeline_clock");
      }
      surfaceSeen.add(at.getTime());
      previousSurfaceAt = at;
      const id = nonEmptyString(item.id) || nonEmptyString(item.replay_id);
      const expectedId = formatIsoUtc(at).replaceAll(":", "");
      const bucket = { startMs: at.getTime() - surfaceStepMs, endMs: at.getTime() };
      const segment = sessionSegmentForBucket(surfaceSegments, bucket);
      const status = nonEmptyString(item.status);
      const expectedStatus = segment?.kind === "closed_gap"
        ? "scheduled_missing"
        : "unvalidated_playhead";
      if (!segment || id !== expectedId || item.session_kind !== segment.kind ||
          status !== expectedStatus ||
          item.projection_policy_sha256 !== expectedProjectionPolicySha256 ||
          ["artifact_sha256", "cached", "url", "frame_url"].some((field) =>
            Object.prototype.hasOwnProperty.call(item, field))) {
        throw new Error("invalid_replay_surface_timeline_frame_contract");
      }
      return {
        raw: item,
        at,
        id,
        label: nonEmptyString(item.label) || nonEmptyString(item.label_et) ||
          formatMarketTime(at),
        url: "",
        cached: false,
        status: normalizedStatus(status),
        timelineStatus: status,
        artifactSha256: "",
        projectionPolicySha256: expectedProjectionPolicySha256,
        sessionKind: segment.kind,
      };
    });
    if (raw.surface_frame_count !== surfaceFrames.length ||
        surfaceFrames.at(-1)?.at.getTime() !== surfaceCloseAt.getTime()) {
      throw new Error("invalid_replay_surface_timeline_frame_count");
    }
    const surfaceHashBody = raw.surface_frames.map((item) => ({
      at: item.at,
      session_kind: item.session_kind,
      status: item.status,
    }));
    if (await canonicalReplaySha256(surfaceHashBody) !== raw.surface_timeline_sha256) {
      throw new Error("invalid_replay_surface_timeline_hash");
    }
    surfaceTimelineSha256 = raw.surface_timeline_sha256;
    surfaceTimelineExtended = true;
  }
  return {
    frames,
    surfaceFrames,
    stepMinutes: surfaceTimelineExtended
      ? raw.surface_frame_interval_minutes
      : raw.step_minutes,
    projectionPolicySha256: expectedProjectionPolicySha256,
    timelineSha256: raw.timeline_sha256,
    surfaceTimelineSha256,
    sourceFingerprint: raw.source_fingerprint,
    openAt,
    closeAt,
    surfaceOpenAt,
    surfaceCloseAt,
    surfaceSegments,
    surfaceTimelineExtended,
  };
}

function buildMetadataReplayClock(timeline) {
  const keyframes = timeline.surfaceFrames.map((frame, index) => ({
    id: frame.id,
    atMs: frame.at.getTime(),
    validUntilMs: timeline.surfaceFrames[index + 1]?.at.getTime() ??
      timeline.surfaceCloseAt.getTime(),
    status: frame.status,
    expiry: null,
    values: [],
    referenceSpot: null,
    frameArtifactSha256: frame.artifactSha256,
  }));
  return {
    metadataOnly: true,
    surfaceTimelineExtended: timeline.surfaceTimelineExtended,
    surfaceSegments: timeline.surfaceSegments,
    status: "unknown",
    sessionDate: app.sessionDate,
    openMs: timeline.surfaceOpenAt.getTime(),
    closeMs: timeline.surfaceCloseAt.getTime(),
    role: app.expiryRole,
    weighting: app.weighting,
    metric: "signed_gamma",
    spx: {
      knownMs: keyframes.length ? [keyframes[0].atMs] : [],
      sourceMs: [],
      prices: [],
    },
    gamma: {
      keyframes,
      spotOffsets: [],
      gaps: [],
      metricUnit: "proxy units",
    },
  };
}

function renderReplaySessionOptions() {
  dom.replaySessionFilter.replaceChildren();
  if (!app.sessions.length) {
    const option = document.createElement("option");
    option.value = "";
    option.textContent = "无可回放交易日";
    dom.replaySessionFilter.append(option);
    dom.replaySessionFilter.disabled = true;
    return;
  }
  for (const session of app.sessions) {
    const option = document.createElement("option");
    option.value = session.date;
    const count = session.surfaceFrameCount !== null
      ? ` · ${session.surfaceFrameCount} surface cutoffs`
      : session.frameCount === null ? "" : ` · ${session.frameCount} legacy frames`;
    option.textContent = `${session.label}${count}`;
    dom.replaySessionFilter.append(option);
  }
  dom.replaySessionFilter.value = app.sessionDate;
  dom.replaySessionFilter.disabled = app.replayCatalogLoading;
}

function replayPlaybackStartMs(trend) {
  if (!trend) return 0;
  if (trend.metadataOnly) return trend.gamma.keyframes[0]?.atMs ?? trend.openMs;
  return Math.max(trend.openMs, trend.spx.knownMs[0]);
}

function updateScenarioDiagnosticAvailability() {
  const replay = isReplayView();
  const legacyIndex = replay ? legacyReplayFrameIndexAtOrBefore() : -1;
  const available = !replay || legacyIndex >= 0;
  const entry = legacyDiagnosticEntryState({ replay, available, playing: app.playing });
  dom.legacyDiagnosticEntry.hidden = entry.hidden;
  dom.legacyDiagnosticOpen.disabled = entry.disabled;
  dom.legacyDiagnosticStatus.textContent = entry.status;
  dom.scenarioDiagnostic.classList.toggle("unavailable", !available);
  dom.scenarioDiagnostic.setAttribute("aria-disabled", String(!available));
  if (replay && !available) {
    dom.scenarioDiagnostic.open = false;
    dom.surfaceTitle.textContent = "Legacy scenario diagnostic unavailable at playhead";
    dom.surfaceSubtitle.textContent = app.legacyFrames.length
      ? "No legacy RTH artifact exists at or before this cutoff; future artifacts are never loaded."
      : "This timeline contains no validated legacy frame artifact.";
  }
  return available;
}

function legacyDiagnosticEntryState({ replay = false, available = false, playing = false } = {}) {
  return {
    hidden: !replay,
    disabled: !replay || !available || playing,
    status: !replay
      ? "Replay only"
      : playing
        ? "Pause replay to open the cached diagnostic"
        : available
          ? "Cached RTH artifact at or before the playhead · no future selection"
          : "Unavailable at this playhead · future artifacts are never selected",
  };
}

function updateReplayControls() {
  const trend = app.trend;
  const gammaCount = trend?.gamma.keyframes.length || 0;
  const currentSession = app.sessions.find((item) => item.date === app.sessionDate);
  const playbackStartMs = replayPlaybackStartMs(trend);
  const minSeconds = trend ? Math.ceil(playbackStartMs / 1_000) : 0;
  const maxSeconds = trend ? Math.floor(trend.closeMs / 1_000) : 0;
  const currentSeconds = trend && Number.isFinite(app.playheadMs)
    ? Math.max(minSeconds, Math.min(Math.floor(app.playheadMs / 1_000), maxSeconds))
    : minSeconds;
  dom.replayTimeline.min = String(minSeconds);
  dom.replayTimeline.max = String(maxSeconds);
  dom.replayTimeline.value = String(currentSeconds);
  const navigationLocked = app.replayCatalogLoading || app.trendLoading;
  dom.replaySessionFilter.disabled = navigationLocked || app.sessions.length === 0;
  dom.replayTimeline.disabled = navigationLocked || !trend;
  const previousIndex = trend
    ? binarySearchLastAtOrBefore(
        trend.gamma.keyframes,
        (app.playheadMs ?? trend.openMs) - 1,
        (keyframe) => keyframe.atMs,
      )
    : -1;
  const currentGammaIndex = trend
    ? binarySearchLastAtOrBefore(
        trend.gamma.keyframes,
        app.playheadMs ?? trend.openMs,
        (keyframe) => keyframe.atMs,
      )
    : -1;
  dom.replayPrevious.disabled = navigationLocked || previousIndex < 0;
  dom.replayNext.disabled = navigationLocked || !trend || currentGammaIndex >= gammaCount - 1;
  const recoverableSurfaceFailure = Boolean(
    app.sessionSurfaceLastError && app.sessionSurfaceRetryKey,
  );
  const surfaceBlocksPlayback = sessionSurfaceBlocksPlayback({
    metadataOnly: trend?.metadataOnly === true,
    hasSurface: Boolean(app.sessionSurface),
    loading: app.sessionSurfaceLoading,
    recoverableFailure: recoverableSurfaceFailure,
  });
  dom.replayPlay.disabled = navigationLocked || app.frameLoading || !trend || gammaCount < 2 ||
    (!app.playing && surfaceBlocksPlayback);
  dom.replaySpeed.disabled = navigationLocked || !trend || gammaCount < 2;
  dom.replaySpeed.value = String(app.speed);
  dom.replayPlay.textContent = app.playing ? "❚❚ 暂停" : "▶ 播放";
  dom.replayPlay.setAttribute("aria-label", app.playing ? "暂停回放" : "播放回放");
  if (trend) {
    dom.replayFrameTime.textContent = formatMarketTime(new Date(app.playheadMs ?? trend.openMs));
    dom.replayFramePosition.textContent = `Keyframe ${Math.max(currentGammaIndex + 1, 0)} / ${gammaCount}`;
    dom.replayTimelineStart.textContent = formatMarketTime(new Date(trend.openMs), false);
    dom.replayTimelineEnd.textContent = formatMarketTime(new Date(trend.closeMs), false);
  } else {
    dom.replayFrameTime.textContent = "—";
    dom.replayFramePosition.textContent = "Keyframe 0 / 0";
    dom.replayTimelineStart.textContent = "—";
    dom.replayTimelineEnd.textContent = "—";
  }
  const sessionLabel = currentSession?.label || app.sessionDate || "—";
  dom.replaySessionMeta.textContent = navigationLocked
    ? `${sessionLabel} · 正在校验回放目录与时间轴`
    : trend
    ? trend.metadataOnly
      ? `${sessionLabel} · ${gammaCount} surface cutoff clocks${trend.surfaceTimelineExtended ? " · GTH + closed gap + RTH" : " · legacy RTH fallback"} · ${app.legacyFrames.length} legacy diagnostic artifacts · Visual ${REPLAY_VISUAL_FPS} fps`
      : `${sessionLabel} · ${trend.spx.prices.length} 个 SPX observations · ${gammaCount} 个 Gamma keyframes · Visual ${REPLAY_VISUAL_FPS} fps · hold-last · availability clock 缺失`
    : `${sessionLabel} · 没有可用的盘中走势`;
  updateScenarioDiagnosticAvailability();
  updateModeChrome();
}

function cancelPlaybackAnimation() {
  if (app.playbackRaf !== null) window.cancelAnimationFrame(app.playbackRaf);
  window.clearTimeout(app.reducedMotionTimer);
  app.playbackRaf = null;
  app.reducedMotionTimer = null;
}

function prefersReducedMotion() {
  return window.matchMedia?.("(prefers-reduced-motion: reduce)").matches === true;
}

function stopPlayback({ syncFrame = false, announce = true } = {}) {
  cancelPlaybackAnimation();
  app.playing = false;
  app.playheadAnchorMs = null;
  app.wallAnchorMs = null;
  if (app.trend && Number.isFinite(app.playheadMs)) {
    drawTrendDynamic(app.playheadMs, { announce });
  }
  updateReplayControls();
  if (syncFrame && app.trend?.metadataOnly) {
    app.frameIndex = sessionSurfaceFrameIndex();
    updateModeQuery();
    maybeLoadSessionSurfaceForPlayhead({ force: true });
  } else if (syncFrame) {
    syncScenarioFrameToPlayhead();
  }
}

function schedulePlayback() {
  if (!app.playing || !app.trend || app.playbackRaf !== null || app.reducedMotionTimer !== null) return;
  if (prefersReducedMotion()) {
    app.reducedMotionTimer = window.setTimeout(() => {
      app.reducedMotionTimer = null;
      if (!app.playing || !app.trend) return;
      const next = adjacentGammaPlayhead(1);
      if (next === null) {
        app.playheadMs = app.trend.closeMs;
        drawTrendDynamic(app.playheadMs, { announce: true });
        stopPlayback({ syncFrame: true, announce: true });
        return;
      }
      app.playheadMs = next;
      drawTrendDynamic(app.playheadMs);
      schedulePlayback();
    }, 2_000 / app.speed);
    return;
  }
  app.playbackRaf = window.requestAnimationFrame(playbackTick);
}

function playbackTick(now) {
  app.playbackRaf = null;
  if (!app.playing || !app.trend) return;
  if (app.wallAnchorMs === null || app.playheadAnchorMs === null) {
    app.wallAnchorMs = now;
    app.playheadAnchorMs = app.playheadMs ?? app.trend.openMs;
  }
  const elapsedWallMs = Math.max(now - app.wallAnchorMs, 0);
  let playheadMs = Math.min(
    app.playheadAnchorMs + elapsedWallMs * REPLAY_MARKET_TIME_RATE * app.speed,
    app.trend.closeMs,
  );
  if (app.trend.metadataOnly) {
    playheadMs = clampSessionSurfacePlayback(
      app.trend.gamma.keyframes,
      playheadMs,
      app.sessionSurfaceKeyframeIndex,
    );
  }
  // Allow one small rAF scheduling quantum of tolerance so 60 Hz displays
  // consistently paint every second callback instead of occasionally every third.
  if (now - app.lastPaintMs >= REPLAY_VISUAL_FRAME_MS * 0.9 || playheadMs >= app.trend.closeMs) {
    app.lastPaintMs = now;
    drawTrendDynamic(playheadMs);
  }
  if (playheadMs >= app.trend.closeMs) {
    stopPlayback({ syncFrame: true, announce: true });
    return;
  }
  schedulePlayback();
}

function startPlayback() {
  if (!app.trend || app.trendLoading || app.frameLoading) return;
  const recoverableSurfaceFailure = Boolean(
    app.sessionSurfaceLastError && app.sessionSurfaceRetryKey,
  );
  if (sessionSurfaceBlocksPlayback({
    metadataOnly: app.trend.metadataOnly === true,
    hasSurface: Boolean(app.sessionSurface),
    loading: app.sessionSurfaceLoading,
    recoverableFailure: recoverableSurfaceFailure,
  })) return;
  cancelPlaybackAnimation();
  dom.scenarioDiagnostic.open = false;
  const playbackStartMs = replayPlaybackStartMs(app.trend);
  if (!Number.isFinite(app.playheadMs) || app.playheadMs >= app.trend.closeMs) {
    app.playheadMs = playbackStartMs;
    drawTrendDynamic(app.playheadMs, { announce: true });
  }
  app.playing = true;
  app.wallAnchorMs = performance.now();
  app.playheadAnchorMs = app.playheadMs;
  app.lastPaintMs = 0;
  updateReplayControls();
  schedulePlayback();
}

function clampSessionSurfacePlayback(keyframes, targetMs, renderedIndex) {
  if (!Array.isArray(keyframes) || !keyframes.length || !Number.isFinite(targetMs) ||
      !Number.isSafeInteger(renderedIndex) || renderedIndex < 0) {
    return targetMs;
  }
  const next = keyframes[renderedIndex + 1];
  return next && Number.isFinite(next.atMs) ? Math.min(targetMs, next.atMs) : targetMs;
}

function seekReplay(playheadMs, { syncFrame = false, announce = true } = {}) {
  if (!app.trend || !Number.isFinite(playheadMs)) return;
  stopPlayback({ syncFrame: false, announce: false });
  const playbackStartMs = replayPlaybackStartMs(app.trend);
  app.playheadMs = Math.max(playbackStartMs, Math.min(playheadMs, app.trend.closeMs));
  drawTrendDynamic(app.playheadMs, { announce, loadSessionSurface: false });
  updateReplayControls();
  if (syncFrame) {
    app.frameIndex = sessionSurfaceFrameIndex();
    updateModeQuery();
    syncScenarioFrameToPlayhead();
    maybeLoadSessionSurfaceForPlayhead({ force: true, interrupt: true });
  }
}

function adjacentGammaPlayhead(direction) {
  if (!app.trend || !Number.isFinite(app.playheadMs)) return null;
  if (direction < 0) {
    const index = binarySearchLastAtOrBefore(
      app.trend.gamma.keyframes,
      app.playheadMs - 1,
      (keyframe) => keyframe.atMs,
    );
    return index >= 0 ? app.trend.gamma.keyframes[index].atMs : null;
  }
  const current = binarySearchLastAtOrBefore(
    app.trend.gamma.keyframes,
    app.playheadMs,
    (keyframe) => keyframe.atMs,
  );
  const next = app.trend.gamma.keyframes[current + 1];
  return next?.atMs ?? null;
}

function cancelSnapshotWork() {
  window.clearTimeout(app.timer);
  window.clearTimeout(app.liveLeaseTimer);
  cancelPlaybackAnimation();
  app.timer = null;
  app.liveLeaseTimer = null;
  app.liveServerAnchorMs = null;
  app.livePerformanceAnchorMs = null;
  app.liveRequestStartedAtMs = null;
  app.livePhase = "off";
  app.liveLastError = "";
  app.playing = false;
  app.frameLoading = false;
  app.trendLoading = false;
  app.replayCatalogLoading = false;
  app.requestGeneration += 1;
  if (app.requestController) app.requestController.abort();
  app.requestController = null;
  app.liveDiagnosticGeneration += 1;
  if (app.liveDiagnosticController) app.liveDiagnosticController.abort();
  app.liveDiagnosticController = null;
  cancelSessionSurfaceRequest({ clear: true });
}

function liveSessionSurfaceRequestUrl() {
  const params = new URLSearchParams({
    role: app.expiryRole,
    weighting: app.weighting,
    bucket_minutes: String(SESSION_SURFACE_BUCKET_MINUTES),
    price_step: String(SESSION_SURFACE_PRICE_STEP),
  });
  return `${LIVE_SESSION_SURFACE_URL}?${params}`;
}

function liveClockNowMs() {
  if (Number.isFinite(app.liveServerAnchorMs) &&
      Number.isFinite(app.livePerformanceAnchorMs)) {
    return app.liveServerAnchorMs + Math.max(performance.now() - app.livePerformanceAnchorMs, 0);
  }
  return null;
}

function setLiveClockAnchor(serverNowMs) {
  if (!Number.isFinite(serverNowMs)) throw new Error("live_surface_server_time_missing");
  app.liveServerAnchorMs = serverNowMs;
  app.livePerformanceAnchorMs = performance.now();
}

function renderLiveSessionSurfaceChrome(phase = app.livePhase, reason = app.liveLastError) {
  if (app.mode !== "live") return;
  const surface = app.sessionSurface;
  const historicalOnly = phase === "historical_only";
  const marketClosed = unavailableLiveReason(reason) === "market_closed";
  const fresh = phase === "fresh" || phase === "refreshing" || phase === "degraded_retained";
  const unavailable = !surface || phase === "unavailable";
  const status = unavailable ? "unavailable" : historicalOnly ? "degraded" : surface.status;
  const label = unavailable
    ? marketClosed ? "Live · Market closed" : "Live · Unavailable"
    : historicalOnly
      ? "Live · Historical only"
      : phase === "degraded_retained"
        ? "Live · Refresh degraded"
        : "Live · Session surface";
  setStatusPill(status, label);
  renderReferenceChrome(surface);
  const serverNowMs = liveClockNowMs();
  const ageSeconds = surface && Number.isFinite(serverNowMs)
    ? Math.max((serverNowMs - surface.asOfMs) / 1_000, 0)
    : null;
  const leaseSeconds = surface && Number.isFinite(serverNowMs) &&
      Number.isFinite(surface.validUntilMs)
    ? Math.max((surface.validUntilMs - serverNowMs) / 1_000, 0)
    : null;
  dom.refreshState.textContent = unavailable
    ? marketClosed ? "Market closed · waiting for next GTH/RTH session" : "Live surface unavailable · retrying"
    : historicalOnly
      ? `Historical frozen through ${formatMarketTime(surface.historyFrozenThrough, false)}`
      : `${formatAge(ageSeconds)} old · lease ${formatAge(leaseSeconds)}`;
  dom.summaryStatus.textContent = unavailable
    ? "Unavailable · fail closed"
    : historicalOnly
      ? "Degraded · historical only"
      : `${STATUS_LABELS[status]} · observed live`;
  dom.summaryReasons.textContent = reason || (historicalOnly
    ? "Projection, current strike and current spot are unavailable"
    : "accepted_at availability clock · dealer side unknown");
  dom.summaryFreshness.textContent = historicalOnly
    ? "Lease expired · dynamic values cleared"
    : fresh ? `${formatAge(ageSeconds)} · lease ${formatAge(leaseSeconds)}` : "—";
  dom.summaryAsOf.textContent = surface ? `as of ${formatReplayAsOf(surface.asOf)}` : "as of —";
  if (surface) {
    const totalCells = surface.timeBuckets.length * surface.priceGrid.length * 2;
    const availableCells = [surface.gamma, surface.charm].flat(2).filter(Number.isFinite).length;
    dom.summaryCoverage.textContent = totalCells
      ? `${Math.min(availableCells / totalCells * 100, 100).toFixed(1)}%`
      : "—";
    dom.summaryContracts.textContent = `${surface.strikeProfile.length} strikes · ${surface.candles.length} candles`;
    dom.summaryExpiries.textContent = `${surface.role.toUpperCase()} · ${surface.expiry}`;
    const lastCandle = surface.candles.at(-1);
    dom.summaryUnderlier.textContent = Number.isFinite(cockpitCurrentSpot())
      ? `SPX ${cockpitCurrentSpot().toFixed(2)}`
      : lastCandle ? `Current — · last OHLC ${lastCandle.close.toFixed(2)}` : "SPX current —";
    dom.schemaVersion.textContent = `session surface schema ${surface.schemaVersion}`;
  } else {
    dom.summaryCoverage.textContent = "—";
    dom.summaryContracts.textContent = "—";
    dom.summaryExpiries.textContent = `${app.expiryRole.toUpperCase()} · —`;
    dom.summaryUnderlier.textContent = "SPX current —";
    dom.schemaVersion.textContent = "session surface schema —";
  }
  dom.signConvention.textContent = "calls + / puts − proxy; participant and dealer side unknown";
  dom.cockpitLoading.hidden = !(phase === "connecting" && !surface);
  updateModeChrome();
}

function unavailableLiveReason(reason) {
  return reason === "live_session_not_rth" ? "market_closed" : "unavailable";
}

function unavailableLiveMessage(reason) {
  return unavailableLiveReason(reason) === "market_closed"
    ? "Market is closed. Live Session Canvas starts from the first validated GTH or RTH snapshot; Replay remains available for prior sessions."
    : `Live Session Surface unavailable: ${reason}`;
}

function expireLiveSessionSurface(expectedArtifactSha256 = null) {
  if (app.mode !== "live" || !app.sessionSurface) return;
  if (expectedArtifactSha256 &&
      app.sessionSurface.artifactSha256 !== expectedArtifactSha256) return;
  window.clearTimeout(app.liveLeaseTimer);
  app.liveLeaseTimer = null;
  const historical = historicalOnlyLiveSurface(app.sessionSurface);
  app.sessionSurface = historical;
  app.livePhase = historical ? "historical_only" : "unavailable";
  app.liveLastError = "live_surface_lease_expired";
  app.cockpitHover = null;
  renderCockpitStatic();
  renderCockpitAudit();
  renderLiveSessionSurfaceChrome();
  setNotice(
    historical
      ? "Live lease 已到期；projection、Current strike 和 Current spot 已清除，仅保留冻结历史。"
      : "Live lease 已到期且没有可安全显示的冻结历史。",
    true,
  );
}

function scheduleLiveLeaseExpiry(surface, serverNowMs) {
  window.clearTimeout(app.liveLeaseTimer);
  app.liveLeaseTimer = null;
  if (!surface || surface.mode !== "live" || !Number.isFinite(surface.validUntilMs)) return;
  const hasDynamic = surface.availability?.projection_available === true ||
    surface.availability?.current_strike_profile_available === true ||
    surface.availability?.current_spot_available === true;
  if (!hasDynamic) return;
  const delay = surface.validUntilMs - serverNowMs;
  if (delay <= 0) {
    expireLiveSessionSurface(surface.artifactSha256);
    return;
  }
  const artifactSha256 = surface.artifactSha256;
  app.liveLeaseTimer = window.setTimeout(() => {
    app.liveLeaseTimer = null;
    expireLiveSessionSurface(artifactSha256);
  }, delay);
}

function applyLiveSessionSurface(surface, serverNowMs) {
  const previous = app.sessionSurface;
  const transitionIssue = liveSurfaceTransitionIssue(previous, surface);
  if (transitionIssue) throw new Error(transitionIssue);
  const newSession = previous?.mode === "live" && previous.sessionDate !== surface.sessionDate;
  if (newSession) {
    app.cockpitPriceDomain = null;
    app.cockpitColorDomains = {};
    resetLiveViewport({ render: false });
  }
  let display = surface;
  const displayState = liveSurfaceDisplayState(surface, serverNowMs);
  if (displayState === "expired") display = historicalOnlyLiveSurface(surface);
  if (displayState === "unavailable" || (displayState === "expired" && !display)) {
    display = null;
  }
  app.sessionSurface = display;
  app.sessionDate = surface.sessionDate;
  app.livePhase = display
    ? displayState === "fresh" ? "fresh" : "historical_only"
    : "unavailable";
  app.liveLastError = "";
  app.cockpitHover = null;
  renderCockpitStatic();
  renderCockpitAudit();
  updateFilters();
  renderLiveSessionSurfaceChrome();
  if (displayState === "fresh") {
    scheduleLiveLeaseExpiry(surface, serverNowMs);
    setNotice("");
  } else if (display) {
    setNotice("Live dynamic lease 不可用；仅显示服务端确认的冻结历史，未来区域保持 Missing。", true);
  } else {
    setNotice("Live Session Surface 当前不可用；没有显示旧 projection 或旧 current 值。", true);
  }
}

function renderSessionSurfaceChrome(status, reason = "", { retrying = false } = {}) {
  if (!isReplayView()) return;
  const unavailable = status === "unavailable";
  setStatusPill(
    unavailable ? "unavailable" : "unknown",
    unavailable
      ? retrying ? "Replay · Unavailable · Retrying" : "Replay · Session surface unavailable"
      : "Replay · Loading session surface",
  );
  renderReferenceChrome(app.sessionSurface);
  dom.refreshState.textContent = unavailable
    ? retrying
      ? "Cutoff-bound surface unavailable · retrying"
      : "Cutoff-bound surface unavailable"
    : "Loading cutoff-bound session surface";
  dom.summaryStatus.textContent = unavailable
    ? retrying ? "Unavailable · Retrying · Bounded PIT" : "Unavailable · Bounded PIT"
    : "Loading · Bounded PIT";
  dom.summaryReasons.textContent = reason || "Waiting for a validated cutoff-bound surface";
  dom.summaryFreshness.textContent = "Frozen · Bounded PIT";
  dom.summaryAsOf.textContent = Number.isFinite(app.playheadMs)
    ? `playhead ${formatReplayAsOf(new Date(app.playheadMs))}`
    : "as of —";
  dom.summaryCoverage.textContent = "—";
  dom.summaryContracts.textContent = "—";
  dom.summaryExpiries.textContent = `${app.expiryRole.toUpperCase()} · —`;
  dom.summaryUnderlier.textContent = "SPX —";
}

function clearSessionSurfaceRetry() {
  window.clearTimeout(app.sessionSurfaceRetryTimer);
  app.sessionSurfaceRetryTimer = null;
  app.sessionSurfaceRetryKey = "";
  app.sessionSurfaceRetryCount = 0;
  app.sessionSurfaceLastError = "";
}

function cancelSessionSurfaceRequest({ clear = false } = {}) {
  app.sessionSurfaceGeneration += 1;
  if (app.sessionSurfaceController) app.sessionSurfaceController.abort();
  cancelSessionSurfacePrefetch();
  app.sessionSurfaceController = null;
  app.sessionSurfaceLoading = false;
  app.sessionSurfaceRequestKey = "";
  app.sessionSurfacePending = false;
  clearSessionSurfaceRetry();
  dom.cockpitLoading.hidden = true;
  if (clear) {
    app.sessionSurface = null;
    app.sessionSurfaceRenderedKey = "";
    app.sessionSurfaceKeyframeIndex = -1;
    app.cockpitHover = null;
    app.cockpitPriceDomain = null;
    app.cockpitColorDomains = {};
    app.replaySummarySignature = "";
    renderCockpitStatic();
    renderCockpitAudit();
    renderSessionSurfaceChrome("loading");
  }
}

function sessionSurfaceFrameIndexFor(keyframes, playheadMs) {
  if (!Array.isArray(keyframes) || !keyframes.length || !Number.isFinite(playheadMs)) return -1;
  return binarySearchLastAtOrBefore(keyframes, playheadMs, (keyframe) => keyframe.atMs);
}

function sessionSurfaceFrameIndex(playheadMs = app.playheadMs) {
  return sessionSurfaceFrameIndexFor(app.trend?.gamma.keyframes, playheadMs);
}

function sessionSurfaceRequestUrl(frame) {
  const params = new URLSearchParams({
    at: formatIsoUtc(frame.at),
    role: app.expiryRole,
    weighting: app.weighting,
    bucket_minutes: String(SESSION_SURFACE_BUCKET_MINUTES),
    price_step: String(SESSION_SURFACE_PRICE_STEP),
  });
  return `${REPLAY_SESSIONS_URL}/${encodeURIComponent(app.sessionDate)}/session-surface?${params}`;
}

function sessionSurfaceRequestDecision({
  inFlightKey,
  targetKey,
  renderedKey,
  force = false,
  interrupt = false,
}) {
  if (targetKey === inFlightKey) return "skip";
  if (inFlightKey) return interrupt ? "interrupt" : "queue";
  if (!force && targetKey === renderedKey) return "skip";
  return "start";
}

function shouldResetCockpitDomains(previousAsOfMs, nextAsOfMs) {
  return Number.isFinite(previousAsOfMs) && Number.isFinite(nextAsOfMs) &&
    nextAsOfMs < previousAsOfMs;
}

function replaySessionSurfacePresentationPhase({
  lastError = "",
  retryKey = "",
  hasSurface = false,
} = {}) {
  if (lastError && retryKey) return "retrying";
  return hasSurface ? "ready" : "loading";
}

function shouldClearSessionSurfaceAfterFailure(targetKey, renderedKey) {
  return Boolean(targetKey) && targetKey !== renderedKey;
}

function sessionSurfaceBlocksPlayback({
  metadataOnly = false,
  hasSurface = false,
  loading = false,
  recoverableFailure = false,
} = {}) {
  return metadataOnly && (!hasSurface || loading) && !recoverableFailure;
}

function sessionSurfaceFailureDisposition(
  error,
  { requestCurrent = true, aborted = false, timedOut = false } = {},
) {
  if (!requestCurrent || (aborted && !timedOut)) {
    return { cancelled: true, retry: false, reason: "" };
  }
  const reason = timedOut
    ? `session_surface_timeout_${Math.round(REPLAY_REQUEST_TIMEOUT_MS / 1_000)}s`
    : error instanceof Error
      ? error.message
      : "session_surface_fetch_failed";
  return { cancelled: false, retry: true, reason };
}

function scheduledMissingSessionSurfacePresentation(frame) {
  const scheduledMissing = frame?.sessionKind === "closed_gap";
  return scheduledMissing
    ? {
        scheduledMissing: true,
        status: "Replay · Scheduled Missing",
        reason: "Scheduled closed gap · market values are Missing, never zero-filled",
        notice: "Scheduled Missing: the closed market gap contains no fabricated surface, reference, candle, or position values.",
      }
    : { scheduledMissing: false, status: "", reason: "", notice: "" };
}

function scheduleSessionSurfaceRetry(key) {
  if (app.mode !== "replay" || app.sessionSurfaceRetryKey !== key) return;
  const delay = SESSION_SURFACE_RETRY_DELAYS_MS[
    Math.min(app.sessionSurfaceRetryCount, SESSION_SURFACE_RETRY_DELAYS_MS.length - 1)
  ];
  app.sessionSurfaceRetryCount += 1;
  window.clearTimeout(app.sessionSurfaceRetryTimer);
  app.sessionSurfaceRetryTimer = window.setTimeout(() => {
    app.sessionSurfaceRetryTimer = null;
    const index = sessionSurfaceFrameIndex();
    const frame = index >= 0 ? app.frames[index] : null;
    const currentKey = frame
      ? `${app.sessionDate}|${frame.id}|${app.expiryRole}|${app.weighting}`
      : "";
    if (currentKey === key && !app.sessionSurfaceController) {
      void loadSessionSurfaceAtPlayhead({ force: true });
    }
  }, delay);
}

function sessionSurfaceCoverageLabel(surface) {
  const totalCells = surface.timeBuckets.length * surface.priceGrid.length;
  if (!totalCells) return "—";
  const availableCells = [surface.gamma, surface.charm]
    .flat(2)
    .filter(Number.isFinite).length;
  return `${Math.min((availableCells / (totalCells * 2)) * 100, 100).toFixed(1)}%`;
}

function sessionSurfaceCacheGet(key) {
  const entry = app.sessionSurfaceCache.get(key);
  if (!entry) return null;
  app.sessionSurfaceCache.delete(key);
  app.sessionSurfaceCache.set(key, entry);
  return entry;
}

function sessionSurfaceCachePut(key, entry) {
  if (!key) return;
  app.sessionSurfaceCache.delete(key);
  app.sessionSurfaceCache.set(key, entry);
  while (app.sessionSurfaceCache.size > SESSION_SURFACE_CACHE_LIMIT) {
    app.sessionSurfaceCache.delete(app.sessionSurfaceCache.keys().next().value);
  }
}

function cancelSessionSurfacePrefetch() {
  if (app.sessionSurfacePrefetchController) app.sessionSurfacePrefetchController.abort();
  app.sessionSurfacePrefetchController = null;
  app.sessionSurfacePrefetchKey = "";
}

function applySessionSurface(surface, frame, frameIndex, key) {
  const previousSurface = app.sessionSurface;
  if (shouldResetCockpitDomains(previousSurface?.asOfMs, surface.asOfMs)) {
    app.cockpitPriceDomain = null;
    app.cockpitColorDomains = {};
  }
  app.sessionSurface = surface;
  app.sessionSurfaceRenderedKey = key;
  app.sessionSurfaceKeyframeIndex = frameIndex;
  app.sessionSurfaceLoading = false;
  app.replaySummarySignature = "";
  dom.cockpitLoading.hidden = true;
  clearSessionSurfaceRetry();
  const presentation = scheduledMissingSessionSurfacePresentation(frame);
  setNotice(presentation.notice, presentation.scheduledMissing);
  renderCockpitStatic();
  drawMetadataReplayDynamic(app.playheadMs ?? surface.asOfMs, { announce: true });
  prefetchNextSessionSurface(frameIndex);
}

function prefetchNextSessionSurface(renderedIndex) {
  cancelSessionSurfacePrefetch();
  if (app.mode !== "replay" || !app.trend?.metadataOnly) return;
  if (!Number.isSafeInteger(renderedIndex) || renderedIndex < 0) return;
  const frame = app.frames[renderedIndex + 1];
  if (!frame) return;
  const sessionDate = app.sessionDate;
  const role = app.expiryRole;
  const weighting = app.weighting;
  const key = `${sessionDate}|${frame.id}|${role}|${weighting}`;
  if (app.sessionSurfaceCache.has(key)) return;
  if (key === app.sessionSurfaceRequestKey) return;
  const controller = new AbortController();
  const generation = app.sessionSurfaceGeneration;
  app.sessionSurfacePrefetchController = controller;
  app.sessionSurfacePrefetchKey = key;
  (async () => {
    try {
      const response = await fetch(sessionSurfaceRequestUrl(frame), {
        cache: "no-cache",
        headers: { Accept: "application/json" },
        signal: controller.signal,
      });
      if (!response.ok) return;
      const payload = await response.json();
      const surface = await normalizeSessionSurface(payload, {
        at: frame.at,
        sessionDate,
        role,
        weighting,
        bucketMinutes: SESSION_SURFACE_BUCKET_MINUTES,
        priceStep: SESSION_SURFACE_PRICE_STEP,
      });
      if (controller.signal.aborted || generation !== app.sessionSurfaceGeneration) return;
      if (app.mode !== "replay" || app.sessionDate !== sessionDate ||
          app.expiryRole !== role || app.weighting !== weighting) return;
      sessionSurfaceCachePut(key, { surface, frame });
    } catch {
      // Prefetch is best-effort; the foreground path owns retries and notices.
    } finally {
      if (app.sessionSurfacePrefetchController === controller) {
        app.sessionSurfacePrefetchController = null;
        app.sessionSurfacePrefetchKey = "";
      }
    }
  })();
}

async function loadSessionSurfaceAtPlayhead({ force = false, interrupt = false } = {}) {
  if (app.mode !== "replay" || !app.trend || !app.sessionDate || !app.frames.length) return;
  const frameIndex = sessionSurfaceFrameIndex();
  const frame = app.frames[frameIndex];
  if (!frame) return;
  const role = app.expiryRole;
  const weighting = app.weighting;
  const key = `${app.sessionDate}|${frame.id}|${role}|${weighting}`;
  const decision = sessionSurfaceRequestDecision({
    inFlightKey: app.sessionSurfaceController ? app.sessionSurfaceRequestKey : "",
    targetKey: key,
    renderedKey: app.sessionSurfaceRenderedKey,
    force,
    interrupt,
  });
  if (decision === "skip") return;
  if (decision === "queue") {
    // Keep one request in flight. Playback is boundary-clamped, while explicit
    // seeks and selector changes coalesce to their latest requested cutoff.
    app.sessionSurfacePending = true;
    return;
  }
  if (decision === "interrupt") cancelSessionSurfaceRequest({ clear: false });
  window.clearTimeout(app.sessionSurfaceRetryTimer);
  app.sessionSurfaceRetryTimer = null;
  const retrying = app.sessionSurfaceRetryKey === key && Boolean(app.sessionSurfaceLastError);
  if (app.sessionSurfaceRetryKey !== key) {
    app.sessionSurfaceRetryKey = key;
    app.sessionSurfaceRetryCount = 0;
    app.sessionSurfaceLastError = "";
  }
  const cachedSurface = sessionSurfaceCacheGet(key);
  if (cachedSurface) {
    setNotice("");
    applySessionSurface(cachedSurface.surface, cachedSurface.frame, frameIndex, key);
    return;
  }
  const controller = new AbortController();
  const generation = ++app.sessionSurfaceGeneration;
  let timedOut = false;
  const abortTimer = window.setTimeout(() => {
    timedOut = true;
    controller.abort();
  }, REPLAY_REQUEST_TIMEOUT_MS);
  app.sessionSurfaceController = controller;
  app.sessionSurfaceLoading = true;
  app.sessionSurfaceRequestKey = key;
  dom.cockpitLoading.hidden = retrying;
  if (retrying) {
    setNotice(
      `Session cockpit unavailable at this keyframe: ${app.sessionSurfaceLastError}. Retrying now.`,
      true,
    );
    renderSessionSurfaceChrome("unavailable", app.sessionSurfaceLastError, { retrying: true });
  } else {
    setNotice("");
    renderSessionSurfaceChrome("loading");
  }
  try {
    const response = await fetch(sessionSurfaceRequestUrl(frame), {
      cache: "no-cache",
      headers: { Accept: "application/json" },
      signal: controller.signal,
    });
    if (!response.ok) throw new Error(`session_surface_http_${response.status}`);
    const payload = await response.json();
    const surface = await normalizeSessionSurface(payload, {
      at: frame.at,
      sessionDate: app.sessionDate,
      role,
      weighting,
      bucketMinutes: SESSION_SURFACE_BUCKET_MINUTES,
      priceStep: SESSION_SURFACE_PRICE_STEP,
    });
    if (
      generation !== app.sessionSurfaceGeneration ||
      app.mode !== "replay" ||
      (surface.sessionDate && app.sessionDate !== surface.sessionDate) ||
      app.expiryRole !== role ||
      app.weighting !== weighting ||
      app.sessionSurfaceRequestKey !== key
    ) return;
    sessionSurfaceCachePut(key, { surface, frame });
    applySessionSurface(surface, frame, frameIndex, key);
  } catch (error) {
    const failure = sessionSurfaceFailureDisposition(error, {
      requestCurrent: generation === app.sessionSurfaceGeneration && app.mode === "replay",
      aborted: controller.signal.aborted,
      timedOut,
    });
    if (failure.cancelled) return;
    app.sessionSurfaceLoading = false;
    dom.cockpitLoading.hidden = true;
    app.sessionSurfaceLastError = failure.reason;
    const clearFailedTarget = shouldClearSessionSurfaceAfterFailure(
      key,
      app.sessionSurfaceRenderedKey,
    );
    if (clearFailedTarget) {
      app.sessionSurface = null;
      app.sessionSurfaceRenderedKey = "";
      app.sessionSurfaceKeyframeIndex = -1;
      app.cockpitHover = null;
      renderCockpitStatic();
      renderCockpitAudit();
    }
    scheduleSessionSurfaceRetry(key);
    setNotice(
      `Session cockpit unavailable at this keyframe: ${failure.reason}. Retrying automatically.`,
      true,
    );
    renderSessionSurfaceChrome("unavailable", failure.reason, { retrying: true });
    if (!app.sessionSurface && !clearFailedTarget) renderCockpitStatic();
  } finally {
    window.clearTimeout(abortTimer);
    const ownsRequest = app.sessionSurfaceController === controller;
    if (ownsRequest) app.sessionSurfaceController = null;
    if (app.sessionSurfaceRequestKey === key && key !== app.sessionSurfaceRenderedKey) {
      app.sessionSurfaceRequestKey = "";
    }
    if (ownsRequest && generation === app.sessionSurfaceGeneration) {
      const drainPending = app.sessionSurfacePending;
      app.sessionSurfacePending = false;
      if (!drainPending) {
        app.sessionSurfaceLoading = false;
        dom.cockpitLoading.hidden = true;
      } else {
        Promise.resolve().then(() => maybeLoadSessionSurfaceForPlayhead());
      }
      // A seek or pause can finish while the play control is disabled for the
      // cutoff-bound surface request.  Refresh the transport after releasing
      // that request so the user can immediately resume playback.
      updateReplayControls();
    }
  }
}

function maybeLoadSessionSurfaceForPlayhead({ force = false, interrupt = false } = {}) {
  const index = sessionSurfaceFrameIndex();
  if (index < 0) return;
  if (!force && index === app.sessionSurfaceKeyframeIndex) return;
  void loadSessionSurfaceAtPlayhead({ force, interrupt });
}

function resetReplayNavigationState() {
  app.snapshot = null;
  app.sessions = [];
  app.sessionDate = "";
  app.frames = [];
  app.frameIndex = -1;
  app.legacyFrames = [];
  app.legacyFrameIndex = -1;
  app.timelineSha256 = "";
  app.surfaceTimelineSha256 = "";
  app.surfaceTimelineExtended = false;
  app.sourceFingerprint = "";
  app.projectionPolicySha256 = "";
  app.timelineOpenMs = null;
  app.timelineCloseMs = null;
  app.trend = null;
  app.trendLoading = false;
  app.playheadMs = null;
  app.playheadAnchorMs = null;
  app.wallAnchorMs = null;
  app.lastPaintMs = 0;
  app.timelineStepMinutes = REPLAY_TIMELINE_STEP_MINUTES;
  app.liveViewportStartMs = null;
  app.liveViewportDrag = null;
  document.body.classList.remove("live-viewport-dragging");
  app.sessionSurfaceCache.clear();
  app.replayFrameChrome = { clock: "", position: "", timelineSec: -1, aria: "" };
  clearTrendVisuals();
  cancelSessionSurfaceRequest({ clear: true });
}

function beginSnapshotRequest(timeoutMs = REQUEST_TIMEOUT_MS) {
  if (app.requestController) app.requestController.abort();
  const controller = new AbortController();
  const generation = ++app.requestGeneration;
  const abortTimer = window.setTimeout(() => controller.abort(), timeoutMs);
  app.requestController = controller;
  return { controller, generation, abortTimer };
}

function requestIsCurrent(generation, mode) {
  return app.requestGeneration === generation && app.mode === mode;
}

function clearDashboardState() {
  app.snapshot = null;
  app.expiry = "";
  updateFilters();
  setQualityChip("unavailable");
  dom.heatmapEmpty.hidden = false;
  clearCanvas();
  clearLadder();
  renderExtrema(null);
  updateLegend(null);
  dom.schemaVersion.textContent = "schema —";
  dom.signConvention.textContent = "dealer position sign unknown";
}

function renderLoadingState() {
  clearDashboardState();
  const replay = isReplayView();
  const frame = replayFrame();
  setStatusPill("unknown", replay ? "Loading replay" : "正在连接");
  dom.refreshState.textContent = replay
    ? frame ? `正在读取并校验 ${frame.label}` : "正在读取回放目录"
    : "等待首个 Live Session Surface";
  dom.summaryStatus.textContent = "—";
  dom.summaryReasons.textContent = replay
    ? "校验 replay / cutoff 契约；PIT 仅有界，availability clock 缺失"
    : "校验 accepted_at、lease、固定网格与冻结历史前缀";
  dom.summaryFreshness.textContent = replay ? "Frozen · Bounded PIT" : "—";
  dom.summaryAsOf.textContent = replay && frame ? `as of ${formatReplayAsOf(frame.at)}` : "as of —";
  dom.summaryCoverage.textContent = "—";
  dom.summaryContracts.textContent = "可用合约 —";
  dom.summaryExpiries.textContent = "—";
  dom.summaryUnderlier.textContent = "SPX —";
  dom.surfaceTitle.textContent = "Spot × Time surface";
  dom.surfaceSubtitle.textContent = replay ? "等待冻结历史回放" : "Legacy diagnostic folded";
  setNotice(replay ? "正在读取历史回放；不会显示实时快照或缓存值。" : "");
  updateModeChrome();
}

function renderFetchFailure(error, mode) {
  clearDashboardState();
  const replay = mode === "replay";
  const frame = replayFrame();
  const reason = error instanceof Error ? error.message : `${mode}_snapshot_fetch_failed`;
  setStatusPill("unavailable", replay ? "Replay unavailable" : null);
  dom.refreshState.textContent = replay ? "历史回放读取失败；未自动重试" : "读取失败；5 秒后重试";
  dom.summaryStatus.textContent = "Unavailable";
  dom.summaryReasons.textContent = reason;
  dom.summaryFreshness.textContent = replay ? "Frozen · Bounded PIT" : "—";
  dom.summaryAsOf.textContent = replay && frame ? `as of ${formatReplayAsOf(frame.at)}` : "as of —";
  dom.summaryCoverage.textContent = "—";
  dom.summaryContracts.textContent = "可用合约 —";
  dom.summaryExpiries.textContent = "—";
  dom.summaryUnderlier.textContent = "SPX —";
  dom.surfaceTitle.textContent = replay ? "Replay unavailable" : "Spot × Time surface";
  dom.surfaceSubtitle.textContent = replay
    ? `SESSION REPLAY · Frozen · Not live${frame ? ` · as of ${formatReplayAsOf(frame.at)}` : ""}`
    : "等待生产快照";
  setNotice(
    replay
      ? "无法读取或校验历史回放；图表已清空，不会回退到生产快照、fixture 或缓存值。"
      : "无法读取生产快照；图表已清空，页面不会显示 fixture 或上一次缓存值。",
    true,
  );
  updateModeChrome();
}

async function refreshLiveSessionSurface() {
  if (app.mode !== "live") return;
  window.clearTimeout(app.timer);
  app.timer = null;
  const role = app.expiryRole;
  const weighting = app.weighting;
  const requestStartedAtMs = performance.now();
  app.liveRequestStartedAtMs = requestStartedAtMs;
  const hadSurface = Boolean(app.sessionSurface);
  app.livePhase = hadSurface ? "refreshing" : "connecting";
  renderLiveSessionSurfaceChrome();
  const { controller, generation, abortTimer } = beginSnapshotRequest(
    LIVE_SESSION_REQUEST_TIMEOUT_MS,
  );
  try {
    const response = await fetch(liveSessionSurfaceRequestUrl(), {
      cache: "no-cache",
      headers: { Accept: "application/json" },
      signal: controller.signal,
    });
    const responseReceivedAtMs = performance.now();
    const serverNowMs = liveServerTimeFromHeaders(
      response.headers,
      requestStartedAtMs,
      responseReceivedAtMs,
    );
    if (!Number.isFinite(serverNowMs)) throw new Error("live_surface_server_time_missing");
    if (!requestIsCurrent(generation, "live") ||
        app.expiryRole !== role || app.weighting !== weighting) return;
    if (response.status === 304) {
      const state = liveSurfaceDisplayState(app.sessionSurface, serverNowMs);
      if (state === "expired") expireLiveSessionSurface();
      else {
        app.livePhase = state === "fresh"
          ? "fresh"
          : state === "historical_only" ? "historical_only" : "unavailable";
        renderLiveSessionSurfaceChrome(app.livePhase);
      }
      return;
    }
    if (!response.ok) {
      let responseError = null;
      try {
        const errorPayload = await response.json();
        responseError = isObject(errorPayload) ? nonEmptyString(errorPayload.error) : null;
      } catch (_error) {
        responseError = null;
      }
      throw new Error(responseError || `live_session_surface_http_${response.status}`);
    }
    const payload = await response.json();
    const surface = await normalizeSessionSurface(payload, {
      mode: "live",
      role,
      weighting,
      bucketMinutes: SESSION_SURFACE_BUCKET_MINUTES,
      priceStep: SESSION_SURFACE_PRICE_STEP,
    });
    if (!liveServerTimeMatchesArtifact(response.headers, surface.serverTimeMs)) {
      throw new Error("live_surface_server_time_header_mismatch");
    }
    if (!requestIsCurrent(generation, "live") ||
        app.expiryRole !== role || app.weighting !== weighting) return;
    const trustedServerNowMs = conservativeLiveServerNow(
      surface,
      serverNowMs,
      Math.max(performance.now() - requestStartedAtMs, 0),
    );
    if (!Number.isFinite(trustedServerNowMs)) {
      throw new Error("live_surface_conservative_clock_unavailable");
    }
    setLiveClockAnchor(trustedServerNowMs);
    applyLiveSessionSurface(surface, trustedServerNowMs);
    if (dom.scenarioDiagnostic.open) void refreshSnapshot();
  } catch (error) {
    if (!requestIsCurrent(generation, "live")) return;
    const reason = error instanceof Error ? error.message : "live_session_surface_fetch_failed";
    app.liveLastError = reason;
    const serverNowMs = liveClockNowMs();
    const state = liveSurfaceDisplayState(app.sessionSurface, serverNowMs);
    if (state === "expired") {
      expireLiveSessionSurface();
    } else if (state === "fresh") {
      app.livePhase = "degraded_retained";
      renderLiveSessionSurfaceChrome();
      setNotice(`Live refresh failed; current artifact remains valid only until its lease: ${reason}`, true);
    } else if (state === "historical_only") {
      app.livePhase = "historical_only";
      renderLiveSessionSurfaceChrome();
      setNotice(`Live refresh failed; only frozen historical data remains: ${reason}`, true);
    } else {
      app.sessionSurface = null;
      app.livePhase = "unavailable";
      renderCockpitStatic();
      renderCockpitAudit();
      renderLiveSessionSurfaceChrome();
      setNotice(unavailableLiveMessage(reason), true);
    }
  } finally {
    window.clearTimeout(abortTimer);
    if (app.requestController === controller) app.requestController = null;
    app.liveRequestStartedAtMs = null;
    if (requestIsCurrent(generation, "live")) {
      const elapsed = Math.max(performance.now() - requestStartedAtMs, 0);
      app.timer = window.setTimeout(
        refreshLiveSessionSurface,
        Math.max(POLL_INTERVAL_MS - elapsed, 0),
      );
    }
  }
}

async function refreshSnapshot() {
  if (app.mode !== "live" || !dom.scenarioDiagnostic.open) return;
  const generation = ++app.liveDiagnosticGeneration;
  if (app.liveDiagnosticController) app.liveDiagnosticController.abort();
  const controller = new AbortController();
  const abortTimer = window.setTimeout(() => controller.abort(), REQUEST_TIMEOUT_MS);
  app.liveDiagnosticController = controller;
  try {
    const response = await fetch(SNAPSHOT_URL, {
      cache: "no-store",
      headers: { Accept: "application/json" },
      signal: controller.signal,
    });
    if (!response.ok) throw new Error(`snapshot_http_${response.status}`);
    const snapshot = normalizeSnapshot(await response.json());
    if (generation !== app.liveDiagnosticGeneration || app.mode !== "live" ||
        !dom.scenarioDiagnostic.open) return;
    app.snapshot = snapshot;
    renderVisuals();
  } catch (error) {
    if (generation !== app.liveDiagnosticGeneration || controller.signal.aborted) return;
    app.snapshot = null;
    renderVisuals();
    dom.surfaceTitle.textContent = "Legacy scenario diagnostic unavailable";
    dom.surfaceSubtitle.textContent = error instanceof Error ? error.message : "snapshot_failed";
  } finally {
    window.clearTimeout(abortTimer);
    if (app.liveDiagnosticController === controller) app.liveDiagnosticController = null;
  }
}

async function loadReplayCatalog() {
  if (app.mode !== "replay") return;
  app.replayCatalogLoading = true;
  updateReplayControls();
  const { controller, generation, abortTimer } = beginSnapshotRequest(REPLAY_REQUEST_TIMEOUT_MS);
  let sessionDate = "";
  try {
    const response = await fetch(REPLAY_SESSIONS_URL, {
      cache: "no-store",
      headers: { Accept: "application/json" },
      signal: controller.signal,
    });
    if (!response.ok) throw new Error(`replay_sessions_http_${response.status}`);
    const payload = await response.json();
    const catalog = normalizeReplaySessions(payload);
    if (!requestIsCurrent(generation, "replay")) return;
    if (!catalog.sessions.length) throw new Error("replay_sessions_empty");
    const requestedDate = new URLSearchParams(window.location.search).get("date");
    if (requestedDate && !catalog.sessions.some((item) => item.date === requestedDate)) {
      throw new Error("requested_replay_session_unavailable");
    }
    app.sessions = catalog.sessions;
    app.projectionPolicySha256 = catalog.projectionPolicySha256;
    sessionDate = requestedDate || catalog.sessions[0].date;
    app.sessionDate = sessionDate;
    renderReplaySessionOptions();
    updateReplayControls();
  } catch (error) {
    if (!requestIsCurrent(generation, "replay")) return;
    app.replayCatalogLoading = false;
    resetReplayNavigationState();
    renderFetchFailure(error, "replay");
    renderReplaySessionOptions();
    updateReplayControls();
  } finally {
    window.clearTimeout(abortTimer);
    if (app.requestController === controller) app.requestController = null;
  }
  if (sessionDate && app.mode === "replay") loadReplayTimeline(sessionDate);
}

async function loadReplayTimeline(sessionDate, { preserveRequestedAt = true } = {}) {
  if (app.mode !== "replay" || app.sessionDate !== sessionDate) return;
  stopPlayback();
  cancelSessionSurfaceRequest({ clear: true });
  app.frames = [];
  app.frameIndex = -1;
  app.legacyFrames = [];
  app.legacyFrameIndex = -1;
  app.timelineSha256 = "";
  app.surfaceTimelineSha256 = "";
  app.surfaceTimelineExtended = false;
  app.sourceFingerprint = "";
  app.snapshot = null;
  app.trend = null;
  app.activeGammaIndex = -1;
  renderReplaySessionOptions();
  updateReplayControls();
  const requestedAtText = preserveRequestedAt
    ? new URLSearchParams(window.location.search).get("at")
    : null;
  if (!preserveRequestedAt) updateModeQuery(null);
  const url = `${REPLAY_SESSIONS_URL}/${encodeURIComponent(sessionDate)}/timeline?step_minutes=${REPLAY_TIMELINE_STEP_MINUTES}`;
  const { controller, generation, abortTimer } = beginSnapshotRequest(REPLAY_REQUEST_TIMEOUT_MS);
  try {
    const response = await fetch(url, {
      cache: "no-store",
      headers: { Accept: "application/json" },
      signal: controller.signal,
    });
    if (!response.ok) throw new Error(`replay_timeline_http_${response.status}`);
    const payload = await response.json();
    const timeline = await normalizeReplayTimeline(
      payload,
      sessionDate,
      app.projectionPolicySha256,
    );
    if (!requestIsCurrent(generation, "replay") || app.sessionDate !== sessionDate) return;
    if (!timeline.surfaceFrames.length) throw new Error("replay_surface_timeline_empty");
    app.frames = timeline.surfaceFrames;
    app.legacyFrames = timeline.frames;
    app.timelineStepMinutes = timeline.stepMinutes;
    app.timelineSha256 = timeline.timelineSha256;
    app.surfaceTimelineSha256 = timeline.surfaceTimelineSha256;
    app.surfaceTimelineExtended = timeline.surfaceTimelineExtended;
    app.sourceFingerprint = timeline.sourceFingerprint;
    app.timelineOpenMs = timeline.surfaceOpenAt.getTime();
    app.timelineCloseMs = timeline.surfaceCloseAt.getTime();
    let frameIndex = timeline.surfaceFrames.length - 1;
    let requestedPlayheadMs = timeline.surfaceCloseAt.getTime();
    if (requestedAtText) {
      const requestedAt = parseDate(requestedAtText);
      if (!requestedAt) throw new Error("invalid_requested_replay_at");
      requestedPlayheadMs = Math.max(
        timeline.surfaceFrames[0].at.getTime(),
        Math.min(requestedAt.getTime(), timeline.surfaceCloseAt.getTime()),
      );
      frameIndex = binarySearchLastAtOrBefore(
        timeline.surfaceFrames,
        requestedPlayheadMs,
        (item) => item.at.getTime(),
      );
    }
    app.frameIndex = frameIndex;
    app.playheadMs = requestedPlayheadMs;
    app.trend = buildMetadataReplayClock(timeline);
    app.trendLoading = false;
    app.activeGammaIndex = -1;
    app.replayCatalogLoading = false;
    updateReplayControls();
    updateModeQuery(requestedAtText ? replayPlayheadQueryClock() : null);
    drawMetadataReplayDynamic(app.playheadMs, { announce: true });
  } catch (error) {
    if (!requestIsCurrent(generation, "replay")) return;
    app.replayCatalogLoading = false;
    renderFetchFailure(error, "replay");
    updateReplayControls();
    return;
  } finally {
    window.clearTimeout(abortTimer);
    if (app.requestController === controller) app.requestController = null;
  }
  if (app.mode === "replay" && app.sessionDate === sessionDate) {
    maybeLoadSessionSurfaceForPlayhead({ force: true });
  }
}

function renderScenarioLoadingState(frame) {
  app.snapshot = null;
  clearCanvas();
  clearLadder();
  renderExtrema(null);
  updateLegend(null);
  dom.heatmapEmpty.hidden = false;
  dom.surfaceTitle.textContent = "Scenario diagnostic · loading keyframe";
  dom.surfaceSubtitle.textContent = frame
    ? `正在校验 ${frame.label} 的完整 Spot × Forward-time frame`
    : "正在校验完整诊断帧";
  setQualityChip("unknown");
}

function renderScenarioFetchFailure(error) {
  const reason = error instanceof Error ? error.message : "replay_frame_fetch_failed";
  app.snapshot = null;
  clearCanvas();
  clearLadder();
  renderExtrema(null);
  updateLegend(null);
  dom.heatmapEmpty.hidden = false;
  dom.surfaceTitle.textContent = "Scenario diagnostic unavailable";
  dom.surfaceSubtitle.textContent = reason;
  setQualityChip("unavailable");
  setNotice("主走势图仍使用已验证的紧凑 artifact；最近的完整情景诊断帧读取失败。", true);
}

function syncScenarioFrameToPlayhead() {
  if (
    app.mode !== "replay" ||
    app.playing ||
    app.frameLoading ||
    !dom.scenarioDiagnostic.open ||
    !Number.isFinite(app.playheadMs)
  ) return;
  const frameIndex = legacyReplayFrameIndexAtOrBefore();
  if (frameIndex < 0) {
    dom.scenarioDiagnostic.open = false;
    renderScenarioFetchFailure(new Error("legacy_replay_artifact_unavailable_at_cutoff"));
    updateScenarioDiagnosticAvailability();
    return;
  }
  app.legacyFrameIndex = frameIndex;
  const frame = app.legacyFrames[frameIndex];
  updateReplayControls();
  if (app.snapshot?.mode === "replay" && app.snapshot.replayId === frame.id) return;
  loadReplayFrame();
}

function replayFrameRequestUrl(frame) {
  const params = new URLSearchParams({ at: formatIsoUtc(frame.at) });
  return `${REPLAY_SESSIONS_URL}/${encodeURIComponent(app.sessionDate)}/frame?${params}`;
}

async function loadReplayFrame() {
  if (app.mode !== "replay") return;
  const frame = legacyReplayFrame();
  if (!frame) return;
  const sessionDate = app.sessionDate;
  app.frameLoading = true;
  renderScenarioLoadingState(frame);
  updateReplayControls();
  const { controller, generation, abortTimer } = beginSnapshotRequest(REPLAY_REQUEST_TIMEOUT_MS);
  try {
    const response = await fetch(replayFrameRequestUrl(frame), {
      cache: "no-cache",
      headers: { Accept: "application/json" },
      signal: controller.signal,
    });
    if (!response.ok) throw new Error(`replay_frame_http_${response.status}`);
    const payload = await response.json();
    const snapshot = await normalizeReplaySnapshot(payload, {
      sessionDate,
      at: frame.at,
      id: frame.id,
      artifactSha256: frame.artifactSha256,
      projectionPolicySha256: frame.projectionPolicySha256,
    });
    if (!requestIsCurrent(generation, "replay") || legacyReplayFrame() !== frame ||
        app.sessionDate !== sessionDate) return;
    frame.artifactSha256 = snapshot.raw.artifact_sha256;
    frame.cached = true;
    app.snapshot = snapshot;
    app.frameLoading = false;
    render();
    updateReplayControls();
  } catch (error) {
    if (!requestIsCurrent(generation, "replay")) return;
    app.frameLoading = false;
    renderScenarioFetchFailure(error);
    updateReplayControls();
  } finally {
    window.clearTimeout(abortTimer);
    if (app.requestController === controller) app.requestController = null;
  }
}

function setViewMode(mode, { syncQuery = true } = {}) {
  if (!["live", "replay"].includes(mode)) return;
  cancelSnapshotWork();
  resetReplayNavigationState();
  app.mode = mode;
  app.livePhase = mode === "live" ? "connecting" : "off";
  app.replayCatalogLoading = mode === "replay";
  dom.scenarioDiagnostic.open = false;
  if (syncQuery) updateModeQuery(null, { push: true });
  renderLoadingState();
  updateReplayControls();
  if (mode === "replay") loadReplayCatalog();
  else refreshLiveSessionSurface();
}

function restartLiveSurfaceForSelector() {
  if (app.mode !== "live") return;
  window.clearTimeout(app.timer);
  window.clearTimeout(app.liveLeaseTimer);
  app.timer = null;
  app.liveLeaseTimer = null;
  app.requestGeneration += 1;
  if (app.requestController) app.requestController.abort();
  app.requestController = null;
  app.sessionSurface = null;
  app.sessionDate = "";
  app.livePhase = "connecting";
  app.liveLastError = "";
  app.cockpitPriceDomain = null;
  app.cockpitColorDomains = {};
  app.cockpitHover = null;
  app.liveViewportStartMs = null;
  app.liveViewportDrag = null;
  document.body.classList.remove("live-viewport-dragging");
  renderCockpitStatic();
  renderCockpitAudit();
  updateFilters();
  renderLiveSessionSurfaceChrome();
  void refreshLiveSessionSurface();
  if (dom.scenarioDiagnostic.open) void refreshSnapshot();
}

dom.modeFilter.addEventListener("change", () => {
  setViewMode(dom.modeFilter.value === "replay" ? "replay" : "live");
});
dom.replaySessionFilter.addEventListener("change", () => {
  const date = dom.replaySessionFilter.value;
  if (!app.sessions.some((item) => item.date === date) || date === app.sessionDate) return;
  cancelSnapshotWork();
  app.mode = "replay";
  app.sessionDate = date;
  app.frames = [];
  app.frameIndex = -1;
  app.legacyFrames = [];
  app.legacyFrameIndex = -1;
  app.snapshot = null;
  app.timelineSha256 = "";
  app.surfaceTimelineSha256 = "";
  app.surfaceTimelineExtended = false;
  app.sourceFingerprint = "";
  app.trend = null;
  app.playheadMs = null;
  clearTrendVisuals();
  app.replayCatalogLoading = true;
  updateModeQuery(null, { push: true });
  renderLoadingState();
  loadReplayTimeline(date, { preserveRequestedAt: false });
});
dom.replayPrevious.addEventListener("click", () => {
  const playheadMs = adjacentGammaPlayhead(-1);
  if (playheadMs !== null) seekReplay(playheadMs, { syncFrame: true });
});
dom.replayNext.addEventListener("click", () => {
  const playheadMs = adjacentGammaPlayhead(1);
  if (playheadMs !== null) seekReplay(playheadMs, { syncFrame: true });
});
dom.replayPlay.addEventListener("click", () => {
  if (app.playing) {
    stopPlayback({ syncFrame: true, announce: true });
    return;
  }
  startPlayback();
});
dom.replaySpeed.addEventListener("change", () => {
  const speed = Number(dom.replaySpeed.value);
  const wasPlaying = app.playing;
  if (wasPlaying) cancelPlaybackAnimation();
  app.speed = [1, 2, 4].includes(speed) ? speed : 1;
  if (wasPlaying) {
    app.wallAnchorMs = performance.now();
    app.playheadAnchorMs = app.playheadMs;
    app.lastPaintMs = 0;
  }
  updateReplayControls();
  if (wasPlaying) schedulePlayback();
});
dom.replayTimeline.addEventListener("input", () => {
  seekReplay(Number(dom.replayTimeline.value) * 1_000, {
    syncFrame: false,
    announce: false,
  });
});
dom.replayTimeline.addEventListener("change", () => {
  seekReplay(Number(dom.replayTimeline.value) * 1_000, {
    syncFrame: true,
    announce: true,
  });
});
dom.expiryFilter.addEventListener("change", () => {
  app.expiryRole = ["front", "next"].includes(dom.expiryFilter.value)
    ? dom.expiryFilter.value
    : "front";
  app.expiry = app.expiryRole;
  if (isReplayView()) {
    renderSummary();
    renderVisuals();
    if (app.trend) app.trend.role = app.expiryRole;
    cancelSessionSurfaceRequest({ clear: true });
    maybeLoadSessionSurfaceForPlayhead({ force: true });
  } else {
    restartLiveSurfaceForSelector();
  }
});
dom.weightingFilter.addEventListener("change", () => {
  app.weighting = dom.weightingFilter.value;
  if (isReplayView()) {
    renderSummary();
    renderVisuals();
    if (app.trend) app.trend.weighting = app.weighting;
    cancelSessionSurfaceRequest({ clear: true });
    maybeLoadSessionSurfaceForPlayhead({ force: true });
  } else {
    restartLiveSurfaceForSelector();
  }
});
dom.metricFilter.addEventListener("change", () => {
  app.metric = dom.metricFilter.value;
  if (isReplayView()) renderSummary();
  if (dom.scenarioDiagnostic.open) renderVisuals();
});

function setCockpitStrikeMode(mode) {
  const next = normalizedStrikeMode(mode);
  if (next === app.strikeMode) {
    renderCockpitStrikeModeChrome(app.sessionSurface);
    return;
  }
  app.strikeMode = next;
  renderCockpitStrikeModeChrome(app.sessionSurface);
  if (!app.sessionSurface) return;
  drawCockpitStrike();
  drawCockpitOverlay("strike", cockpitCurrentSpot());
  renderCockpitReadouts(cockpitCurrentSpot());
}

dom.cockpitStrikeModeOi.addEventListener("click", () => setCockpitStrikeMode("oi"));
dom.cockpitStrikeModeGamma.addEventListener("click", () => setCockpitStrikeMode("gamma"));
dom.liveViewportReset.addEventListener("click", () => resetLiveViewport());

function setAuditDrawer(open) {
  const visible = open === true;
  dom.cockpitAuditDrawer.hidden = !visible;
  dom.cockpitAuditScrim.hidden = !visible;
  dom.cockpitAuditToggle.setAttribute("aria-expanded", String(visible));
  document.body.classList.toggle("audit-open", visible);
  if (visible) renderCockpitAudit();
}

function closeLegacyDiagnosticOverlay({ restoreFocus = true } = {}) {
  const wasVisible = document.body.classList.contains?.("legacy-diagnostic-open") === true;
  document.body.classList.remove("legacy-diagnostic-open");
  if (dom.scenarioDiagnostic.open) dom.scenarioDiagnostic.open = false;
  if (restoreFocus && wasVisible) dom.cockpitAuditToggle.focus();
}

function openLegacyDiagnosticOverlay() {
  if (!isReplayView() || app.playing || !updateScenarioDiagnosticAvailability()) return;
  setAuditDrawer(false);
  document.body.classList.add("legacy-diagnostic-open");
  dom.scenarioDiagnostic.open = true;
  dom.scenarioDiagnostic.querySelector("summary")?.focus();
}

dom.cockpitAuditToggle.addEventListener("click", () => {
  setAuditDrawer(dom.cockpitAuditDrawer.hidden);
});
dom.cockpitAuditClose.addEventListener("click", () => setAuditDrawer(false));
dom.cockpitAuditScrim.addEventListener("click", () => setAuditDrawer(false));
dom.legacyDiagnosticOpen.addEventListener("click", openLegacyDiagnosticOverlay);

for (const panel of ["gamma", "strike", "charm"]) {
  const overlay = cockpitElements(panel).overlay;
  overlay.addEventListener("pointerdown", (event) => beginLiveViewportPan(panel, event));
  overlay.addEventListener("pointermove", (event) => {
    if (!updateLiveViewportPan(panel, event)) cockpitPointerMove(panel, event);
  });
  overlay.addEventListener("pointerup", (event) => endLiveViewportPan(panel, event));
  overlay.addEventListener("pointercancel", (event) => endLiveViewportPan(panel, event));
  overlay.addEventListener("pointerleave", () => {
    if (!app.liveViewportDrag) clearCockpitHover();
  });
  if (panel !== "strike") {
    overlay.addEventListener("click", (event) => {
      if (!isReplayView()) return;
      const surface = app.sessionSurface;
      const layout = app.cockpitLayouts[panel];
      if (!surface || !layout) return;
      const rect = overlay.getBoundingClientRect();
      const x = event.clientX - rect.left;
      if (x < layout.plotLeft || x > layout.plotRight) return;
      const at = cockpitXToTime(layout, surface, x);
      seekReplay(at, { syncFrame: true, announce: true });
    });
  }
  overlay.addEventListener("keydown", (event) => {
    if (!isReplayView()) {
      if (panel === "strike") return;
      if (event.key === "Home") {
        event.preventDefault();
        resetLiveViewport();
        return;
      }
      const liveDelta = event.key === "ArrowLeft"
        ? -30 * 60_000
        : event.key === "ArrowRight" ? 30 * 60_000 : 0;
      if (liveDelta) {
        event.preventDefault();
        panLiveViewportBy(liveDelta);
      }
      return;
    }
    if (!app.trend) return;
    if (event.key === " " || event.key === "Spacebar") {
      event.preventDefault();
      if (app.playing) stopPlayback({ syncFrame: true, announce: true });
      else startPlayback();
      return;
    }
    const delta = event.key === "ArrowLeft" ? -60_000 : event.key === "ArrowRight" ? 60_000 : 0;
    if (delta) {
      event.preventDefault();
      seekReplay((app.playheadMs ?? app.trend.openMs) + delta, {
        syncFrame: true,
        announce: true,
      });
    }
  });
}
dom.heatmap.addEventListener("pointermove", (event) => {
  if (app.chartHit) tooltipForHit(app.chartHit, event);
});
dom.heatmap.addEventListener("pointerleave", () => {
  dom.heatmapTooltip.hidden = true;
});
dom.trendOverlay.addEventListener("pointermove", trendTooltipForEvent);
dom.trendOverlay.addEventListener("pointerleave", () => {
  dom.trendTooltip.hidden = true;
});
dom.trendOverlay.addEventListener("click", (event) => {
  if (!app.trendHit || !app.trend) return;
  const rect = dom.trendOverlay.getBoundingClientRect();
  const x = event.clientX - rect.left;
  const { layout, trend } = app.trendHit;
  if (x < layout.margins.left || x > layout.margins.left + layout.plotWidth) return;
  const ratio = (x - layout.margins.left) / layout.plotWidth;
  seekReplay(trend.openMs + ratio * (trend.closeMs - trend.openMs), {
    syncFrame: true,
    announce: true,
  });
});
dom.trendOverlay.addEventListener("keydown", (event) => {
  if (!app.trend) return;
  if (event.key === " " || event.key === "Spacebar") {
    event.preventDefault();
    if (app.playing) stopPlayback({ syncFrame: true, announce: true });
    else startPlayback();
    return;
  }
  const direction = event.key === "ArrowLeft" ? -1 : event.key === "ArrowRight" ? 1 : 0;
  if (direction) {
    event.preventDefault();
    const playheadMs = adjacentGammaPlayhead(direction);
    if (playheadMs !== null) seekReplay(playheadMs, { syncFrame: true });
    return;
  }
  if (event.key === "Home" || event.key === "End") {
    event.preventDefault();
    seekReplay(
      event.key === "Home"
        ? replayPlaybackStartMs(app.trend)
        : app.trend.closeMs,
      {
        syncFrame: true,
      },
    );
  }
});
dom.scenarioDiagnostic.addEventListener("toggle", () => {
  if (!dom.scenarioDiagnostic.open) {
    closeLegacyDiagnosticOverlay();
    app.liveDiagnosticGeneration += 1;
    if (app.liveDiagnosticController) app.liveDiagnosticController.abort();
    app.liveDiagnosticController = null;
    updateFilters();
    return;
  }
  if (app.playing) {
    dom.scenarioDiagnostic.open = false;
    return;
  }
  if (isReplayView() && !updateScenarioDiagnosticAvailability()) return;
  if (isReplayView() && app.trend && !app.playing) {
    syncScenarioFrameToPlayhead();
  } else if (app.mode === "live") {
    updateFilters();
    dom.surfaceTitle.textContent = "Legacy scenario diagnostic · loading";
    dom.surfaceSubtitle.textContent = "Secondary rolling Spot × Forward-time snapshot";
    void refreshSnapshot();
  }
});

window.addEventListener("popstate", () => {
  setViewMode(initialModeFromQuery(), { syncQuery: false });
});

document.addEventListener("keydown", (event) => {
  if (event.key !== "Escape") return;
  if (document.body.classList.contains("legacy-diagnostic-open")) {
    event.preventDefault();
    closeLegacyDiagnosticOverlay();
  } else if (!dom.cockpitAuditDrawer.hidden) {
    setAuditDrawer(false);
  }
});

if ("ResizeObserver" in window) {
  const resizeObserver = new ResizeObserver(() => {
    if (app.snapshot && dom.scenarioDiagnostic.open) window.requestAnimationFrame(renderVisuals);
    if (app.trend) window.requestAnimationFrame(renderTrendStatic);
    if (app.sessionSurface) window.requestAnimationFrame(renderCockpitStatic);
  });
  resizeObserver.observe(dom.heatmapStage);
  resizeObserver.observe(dom.trendStage);
  resizeObserver.observe(dom.cockpitGammaStage);
  resizeObserver.observe(dom.cockpitStrikeStage);
  resizeObserver.observe(dom.cockpitCharmStage);
} else {
  window.addEventListener("resize", () => {
    if (app.snapshot && dom.scenarioDiagnostic.open) window.requestAnimationFrame(renderVisuals);
    if (app.trend) window.requestAnimationFrame(renderTrendStatic);
    if (app.sessionSurface) window.requestAnimationFrame(renderCockpitStatic);
  });
}

document.addEventListener("visibilitychange", () => {
  if (document.hidden && app.playing) {
    stopPlayback({ syncFrame: false, announce: true });
  }
  if (!document.hidden && app.mode === "live") {
    const state = liveSurfaceDisplayState(app.sessionSurface, liveClockNowMs());
    if (state === "expired") expireLiveSessionSurface();
    if (!app.requestController) {
      window.clearTimeout(app.timer);
      app.timer = null;
      void refreshLiveSessionSurface();
    }
  }
});

if (isObject(globalThis.__SPX_SPARK_TEST_HOOK__)) {
  Object.assign(globalThis.__SPX_SPARK_TEST_HOOK__, {
    canonicalReplaySha256,
    cockpitDisplayTimeMs,
    cockpitTimeWindow,
    expandOnlyDomain,
    historicalOnlyLiveSurface,
    conservativeLiveServerNow,
    liveFrozenPrefixSignature,
    liveServerTimeFromHeaders,
    liveServerTimeMatchesArtifact,
    liveSurfaceDisplayState,
    liveSurfaceIdentity,
    liveViewportStartAfterPan,
    liveSurfaceTransitionIssue,
    unavailableLiveMessage,
    unavailableLiveReason,
    missingRangeAppliesToPanel,
    normalizeReplaySessions,
    normalizeReplayTimeline,
    normalizeSessionSurface,
    normalizeSessionMetricUnits,
    normalizeSessionReference,
    normalizeSessionSegments,
    normalizeStrikeProfileMetadata,
    normalizeReplayTrend,
    referencePresentation,
    renderReferenceChrome,
    robustDomain,
    verifyReplayDigests,
    legacyReplayFrameIndexFor,
    legacyDiagnosticEntryState,
    sessionSurfaceFrameIndexFor,
    sessionSurfaceRequestDecision,
    sessionSurfaceFailureDisposition,
    sessionSurfaceCacheGet,
    sessionSurfaceCachePut,
    sessionSurfaceCoverageLabel,
    sessionSurfaceBlocksPlayback,
    replaySessionSurfacePresentationPhase,
    shouldClearSessionSurfaceAfterFailure,
    sessionPriceToY,
    sessionTimeToX,
    sessionXToTime,
    sessionYToPrice,
    shouldResetCockpitDomains,
    scheduledMissingSessionSurfacePresentation,
    renderSessionSurfaceChrome,
    cockpitCandleDisplayTime,
    cockpitCandleAtTime,
    clampSessionSurfacePlayback,
    sessionGridPriceDomain,
    strikeProfileComparisonLabel,
    strikeProfileDomains,
    strikeProxyColor,
  });
}

if (globalThis.__SPX_SPARK_DISABLE_AUTO_START__ !== true) {
  setViewMode(app.mode, { syncQuery: false });
}
