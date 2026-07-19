"use strict";

const SNAPSHOT_URL = "api/v1/snapshot";
const REPLAY_SESSIONS_URL = "api/v1/replay/sessions";
const REPLAY_TREND_KIND = "spxw_intraday_gamma_replay";
const REPLAY_TREND_POLICY_VERSION = "spxw_surface_replay_trend.v1";
const REPLAY_POLICY_VERSION = "spxw_surface_replay.v3";
const REPLAY_CATALOG_KIND = "spxw_surface_replay_catalog";
const REPLAY_TIMELINE_POLICY_VERSION = "spxw_surface_replay_timeline.event_driven.v1";
const REPLAY_FRAME_VALIDATION = "known_clock_validation_on_frame_request";
const REPLAY_CLOSE_GRACE_POLICY = "session_close_plus_2h_grace";
const REPLAY_CLOSE_GRACE_SECONDS = 2 * 60 * 60;
const REPLAY_TREND_VALIDITY_RULE =
  "min(next_keyframe_at, at_plus_frame_interval, expiry_close, session_close); unavailable_at_at";
const REPLAY_TIMELINE_STEP_MINUTES = 5;
const REPLAY_VISUAL_FPS = 30;
const REPLAY_VISUAL_FRAME_MS = 1_000 / REPLAY_VISUAL_FPS;
const REPLAY_MARKET_TIME_RATE = 150;
const MARKET_TIME_ZONE = "America/New_York";
const POLL_INTERVAL_MS = 5_000;
const REQUEST_TIMEOUT_MS = 4_500;
const REPLAY_REQUEST_TIMEOUT_MS = 60_000;
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
  replayCatalogLoading: false,
  projectionPolicySha256: "",
  timelineSha256: "",
  sourceFingerprint: "",
  sessions: [],
  sessionDate: "",
  frames: [],
  frameIndex: -1,
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
  if (isReplayView()) {
    return app.snapshot?.expiries.find((item) => item.role === app.expiryRole) || null;
  }
  return app.snapshot?.expiries.find((item) => item.expiry === app.expiry) || null;
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

function updateModeQuery(frame = replayFrame(), { push = false } = {}) {
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
  const verifiedTrend = replay && app.trend;
  const verifiedFrame = replay && app.snapshot?.mode === "replay";
  const replayAt = Number.isFinite(app.playheadMs)
    ? new Date(app.playheadMs)
    : app.snapshot?.mode === "replay" ? app.snapshot.requestedAsOf : frame?.at;
  document.body.classList.toggle("mode-replay", replay);
  dom.trendPanel.hidden = !replay;
  dom.modeFilter.value = replay ? "replay" : "live";
  dom.replayBanner.hidden = !replay;
  dom.replayConsole.hidden = !replay;
  dom.pageLede.textContent = replay
    ? "按交易日连续回放 SPX 实际走势与 Gamma proxy 区间。SPX 仅在 recorded known_at 到达后显示；Schwab 缺少真实 availability clock，PIT 仍为有界但未严格证明。"
    : "标的价格 × 时间的期权暴露地形。所有值均来自只读生产快照，不提供下单入口。";
  const sourceFiles = verifiedTrend
    ? app.trend.source?.source_files
    : verifiedFrame ? app.snapshot.source?.source_files : null;
  dom.sourceFile.textContent = replay
    ? sourceFiles?.join(", ") || (frame?.id ? `replay frame ${frame.id}` : "replay catalog")
    : "spxw_surface_dashboard.json";
  dom.sourceMode.textContent = replay
    ? verifiedTrend
      ? `COMPACT TREND ARTIFACT VERIFIED · Visual ${REPLAY_VISUAL_FPS} fps · SPX recorded known_at + Gamma valid_until · Bounded PIT · Not live`
      : `SESSION REPLAY · Visual ${REPLAY_VISUAL_FPS} fps · SPX observed / Gamma hold-last · Bounded PIT · Not live`
    : "5 秒只读刷新";
  if (replay) {
    if (verifiedTrend) {
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
  const previous = app.expiry;
  dom.expiryFilter.replaceChildren();
  if (isReplayView()) {
    for (const role of ["front", "next"]) {
      const expiry = expiries.find((item) => item.role === role)?.expiry ||
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
    const enabled = app.frames.length > 0 &&
      sha256String(app.timelineSha256) &&
      sha256String(app.sourceFingerprint) &&
      !app.trendLoading;
    dom.expiryFilter.disabled = !enabled;
    dom.weightingFilter.disabled = !enabled;
    dom.metricFilter.disabled = !enabled;
    dom.weightingFilter.value = app.weighting;
    dom.metricFilter.value = app.metric;
    return;
  }
  if (!expiries.length) {
    const option = document.createElement("option");
    option.value = "";
    option.textContent = "无可用到期日";
    dom.expiryFilter.append(option);
    app.expiry = "";
  } else {
    for (const expiry of expiries) {
      const option = document.createElement("option");
      option.value = expiry.expiry;
      const role = expiry.role === "front" ? "FRONT" : expiry.role === "next" ? "NEXT" : expiry.role.toUpperCase();
      option.textContent = `${role} · ${formatExpiry(expiry.expiry)} · ${expiry.expiry}`;
      dom.expiryFilter.append(option);
    }
    const retained = expiries.some((item) => item.expiry === previous);
    app.expiry = retained
      ? previous
      : (expiries.find((item) => item.role === app.expiryRole) || expiries[0]).expiry;
    app.expiryRole = expiries.find((item) => item.expiry === app.expiry)?.role || app.expiryRole;
    dom.expiryFilter.value = app.expiry;
  }
  const enabled = expiries.length > 0 && ["ready", "degraded"].includes(effectiveSnapshotStatus(app.snapshot));
  dom.expiryFilter.disabled = !enabled;
  dom.weightingFilter.disabled = !enabled;
  dom.metricFilter.disabled = !enabled;
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

function drawTrendDynamic(playheadMs, { announce = false } = {}) {
  const trend = app.trend;
  const layout = app.trendLayout;
  if (!trend || !layout) return;
  const clamped = Math.max(trend.openMs, Math.min(playheadMs, trend.closeMs));
  app.playheadMs = clamped;
  app.activeSpotIndex = binarySearchLastAtOrBefore(trend.spx.knownMs, clamped);
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
  dom.replayFramePosition.textContent = `Gamma ${gamma.index >= 0 ? gamma.index + 1 : 0} / ${trend.gamma.keyframes.length}`;
  dom.replayTimeline.value = String(Math.floor(clamped / 1_000));
  dom.replayTimeline.setAttribute(
    "aria-valuetext",
    `${formatMarketTime(new Date(clamped))}, Gamma keyframe ${gamma.index >= 0 ? gamma.index + 1 : 0} of ${trend.gamma.keyframes.length}`,
  );
  if (announce) {
    dom.trendAccessibleSummary.textContent = `${formatMarketTime(new Date(clamped))}，${heldSpot === null ? "SPX 价格不可用" : `SPX ${heldSpot.toFixed(2)}`}，${gammaValue === null ? "Gamma proxy 不可用" : `Gamma proxy ${compactNumber(gammaValue, 2)}`}。图中仅显示回放游标前已知数据；Y 轴以首个已知 SPX 观测的固定窗口设定，不使用未来日内高低点。`;
    renderTrendReplaySummary(heldSpot, gamma.keyframe);
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
  return {
    frames,
    stepMinutes: raw.step_minutes,
    projectionPolicySha256: expectedProjectionPolicySha256,
    timelineSha256: raw.timeline_sha256,
    sourceFingerprint: raw.source_fingerprint,
    openAt,
    closeAt,
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
    const count = session.frameCount === null ? "" : ` · ${session.frameCount} frames`;
    option.textContent = `${session.label}${count}`;
    dom.replaySessionFilter.append(option);
  }
  dom.replaySessionFilter.value = app.sessionDate;
  dom.replaySessionFilter.disabled = app.replayCatalogLoading;
}

function updateReplayControls() {
  const trend = app.trend;
  const gammaCount = trend?.gamma.keyframes.length || 0;
  const currentSession = app.sessions.find((item) => item.date === app.sessionDate);
  const playbackStartMs = trend
    ? Math.max(trend.openMs, trend.spx.knownMs[0])
    : 0;
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
  dom.replayPlay.disabled = navigationLocked || app.frameLoading || !trend || gammaCount < 2;
  dom.replaySpeed.disabled = navigationLocked || !trend || gammaCount < 2;
  dom.replaySpeed.value = String(app.speed);
  dom.replayPlay.textContent = app.playing ? "❚❚ 暂停" : "▶ 播放";
  dom.replayPlay.setAttribute("aria-label", app.playing ? "暂停回放" : "播放回放");
  if (trend) {
    dom.replayFrameTime.textContent = formatMarketTime(new Date(app.playheadMs ?? trend.openMs));
    dom.replayFramePosition.textContent = `Gamma ${Math.max(currentGammaIndex + 1, 0)} / ${gammaCount}`;
    dom.replayTimelineStart.textContent = formatMarketTime(new Date(trend.openMs), false);
    dom.replayTimelineEnd.textContent = formatMarketTime(new Date(trend.closeMs), false);
  } else {
    dom.replayFrameTime.textContent = "—";
    dom.replayFramePosition.textContent = "Gamma 0 / 0";
    dom.replayTimelineStart.textContent = "—";
    dom.replayTimelineEnd.textContent = "—";
  }
  const sessionLabel = currentSession?.label || app.sessionDate || "—";
  dom.replaySessionMeta.textContent = navigationLocked
    ? `${sessionLabel} · 正在校验回放走势合同`
    : trend
    ? `${sessionLabel} · ${trend.spx.prices.length} 个 SPX observations · ${gammaCount} 个 Gamma keyframes · Visual ${REPLAY_VISUAL_FPS} fps · hold-last · availability clock 缺失`
    : `${sessionLabel} · 没有可用的盘中走势`;
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
  if (syncFrame) syncScenarioFrameToPlayhead();
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
  const playheadMs = Math.min(
    app.playheadAnchorMs + elapsedWallMs * REPLAY_MARKET_TIME_RATE * app.speed,
    app.trend.closeMs,
  );
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
  cancelPlaybackAnimation();
  dom.scenarioDiagnostic.open = false;
  const playbackStartMs = Math.max(app.trend.openMs, app.trend.spx.knownMs[0]);
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

function seekReplay(playheadMs, { syncFrame = false, announce = true } = {}) {
  if (!app.trend || !Number.isFinite(playheadMs)) return;
  stopPlayback({ syncFrame: false, announce: false });
  const playbackStartMs = Math.max(app.trend.openMs, app.trend.spx.knownMs[0]);
  app.playheadMs = Math.max(playbackStartMs, Math.min(playheadMs, app.trend.closeMs));
  drawTrendDynamic(app.playheadMs, { announce });
  updateReplayControls();
  if (syncFrame) syncScenarioFrameToPlayhead();
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
  cancelPlaybackAnimation();
  app.timer = null;
  app.playing = false;
  app.frameLoading = false;
  app.trendLoading = false;
  app.replayCatalogLoading = false;
  app.requestGeneration += 1;
  if (app.requestController) app.requestController.abort();
  app.requestController = null;
}

function resetReplayNavigationState() {
  app.snapshot = null;
  app.sessions = [];
  app.sessionDate = "";
  app.frames = [];
  app.frameIndex = -1;
  app.projectionPolicySha256 = "";
  app.timelineSha256 = "";
  app.sourceFingerprint = "";
  app.trend = null;
  app.trendLoading = false;
  app.playheadMs = null;
  app.playheadAnchorMs = null;
  app.wallAnchorMs = null;
  app.lastPaintMs = 0;
  app.timelineStepMinutes = REPLAY_TIMELINE_STEP_MINUTES;
  clearTrendVisuals();
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
    : "等待首个快照";
  dom.summaryStatus.textContent = "—";
  dom.summaryReasons.textContent = replay ? "校验 replay / cutoff 契约；PIT 仅有界，availability clock 缺失" : "尚未加载";
  dom.summaryFreshness.textContent = replay ? "Frozen · Bounded PIT" : "—";
  dom.summaryAsOf.textContent = replay && frame ? `as of ${formatReplayAsOf(frame.at)}` : "as of —";
  dom.summaryCoverage.textContent = "—";
  dom.summaryContracts.textContent = "可用合约 —";
  dom.summaryExpiries.textContent = "—";
  dom.summaryUnderlier.textContent = "SPX —";
  dom.surfaceTitle.textContent = "Spot × Time surface";
  dom.surfaceSubtitle.textContent = replay ? "等待冻结历史回放" : "等待生产快照";
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

async function refreshSnapshot() {
  if (app.mode !== "live") return;
  window.clearTimeout(app.timer);
  app.timer = null;
  const { controller, generation, abortTimer } = beginSnapshotRequest();
  try {
    const response = await fetch(SNAPSHOT_URL, {
      cache: "no-store",
      headers: { Accept: "application/json" },
      signal: controller.signal,
    });
    if (!response.ok) throw new Error(`snapshot_http_${response.status}`);
    const payload = await response.json();
    const snapshot = normalizeSnapshot(payload);
    if (!requestIsCurrent(generation, "live")) return;
    app.snapshot = snapshot;
    render();
  } catch (error) {
    if (!requestIsCurrent(generation, "live")) return;
    renderFetchFailure(error, "live");
  } finally {
    window.clearTimeout(abortTimer);
    if (app.requestController === controller) app.requestController = null;
    if (requestIsCurrent(generation, "live")) {
      app.timer = window.setTimeout(refreshSnapshot, POLL_INTERVAL_MS);
    }
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
  app.frames = [];
  app.frameIndex = -1;
  app.snapshot = null;
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
    if (!timeline.frames.length) throw new Error("replay_timeline_empty");
    app.frames = timeline.frames;
    app.timelineStepMinutes = timeline.stepMinutes;
    app.timelineSha256 = timeline.timelineSha256;
    app.sourceFingerprint = timeline.sourceFingerprint;
    let frameIndex = timeline.frames.length - 1;
    if (requestedAtText) {
      const requestedAt = parseDate(requestedAtText);
      if (!requestedAt) throw new Error("invalid_requested_replay_at");
      frameIndex = timeline.frames.findIndex((item) => item.at.getTime() === requestedAt.getTime());
      if (frameIndex < 0) throw new Error("requested_replay_frame_unavailable");
    }
    app.frameIndex = frameIndex;
    app.playheadMs = requestedAtText
      ? timeline.frames[frameIndex].at.getTime()
      : timeline.closeAt.getTime();
    app.replayCatalogLoading = false;
    updateReplayControls();
    updateModeQuery(requestedAtText ? replayFrame() : null);
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
    loadReplayTrend();
  }
}

function replayTrendRequestUrl() {
  const role = ["front", "next"].includes(app.expiryRole) ? app.expiryRole : "front";
  const params = new URLSearchParams({
    role,
    weighting: app.weighting,
    metric: app.metric,
  });
  return `${REPLAY_SESSIONS_URL}/${encodeURIComponent(app.sessionDate)}/trend?${params}`;
}

function renderTrendFailure(error) {
  const reason = error instanceof Error ? error.message : "replay_trend_fetch_failed";
  app.trend = null;
  app.trendLoading = false;
  clearTrendVisuals();
  setTrendQuality("unavailable");
  dom.trendTitle.textContent = "SPX intraday · Gamma proxy unavailable";
  dom.trendSubtitle.textContent = reason;
  updateFilters();
  setStatusPill("unavailable", "Replay trend unavailable");
  setNotice("无法读取或校验盘中走势合同；主图已清空，不会回退到未验证数据。", true);
  updateReplayControls();
}

async function loadReplayTrend({ syncScenario = false } = {}) {
  if (app.mode !== "replay" || !app.sessionDate || !app.frames.length) return;
  stopPlayback();
  app.trendLoading = true;
  app.trend = null;
  clearTrendVisuals();
  dom.trendTitle.textContent = "SPX intraday · Gamma proxy zones";
  dom.trendSubtitle.textContent = "正在读取并校验紧凑走势 artifact";
  updateFilters();
  updateReplayControls();
  const sessionDate = app.sessionDate;
  const role = ["front", "next"].includes(app.expiryRole) ? app.expiryRole : "front";
  const weighting = app.weighting;
  const metric = app.metric;
  const { controller, generation, abortTimer } = beginSnapshotRequest(REPLAY_REQUEST_TIMEOUT_MS);
  let shouldSyncScenario = false;
  try {
    const response = await fetch(replayTrendRequestUrl(), {
      cache: "no-cache",
      headers: { Accept: "application/json" },
      signal: controller.signal,
    });
    if (!response.ok) throw new Error(`replay_trend_http_${response.status}`);
    const payload = await response.json();
    const trend = await normalizeReplayTrend(payload, {
      sessionDate,
      role,
      weighting,
      metric,
      projectionPolicySha256: app.projectionPolicySha256,
      timelineSha256: app.timelineSha256,
      sourceFingerprint: app.sourceFingerprint,
      frames: app.frames,
    });
    if (
      !requestIsCurrent(generation, "replay") ||
      app.sessionDate !== sessionDate ||
      app.expiryRole !== role ||
      app.weighting !== weighting ||
      app.metric !== metric
    ) return;
    app.trend = trend;
    app.trendLoading = false;
    const requestedPlayhead = Number.isFinite(app.playheadMs) ? app.playheadMs : trend.openMs;
    app.playheadMs = Math.max(trend.openMs, Math.min(requestedPlayhead, trend.closeMs));
    dom.trendTitle.textContent = `SPX intraday · ${METRICS[metric].label} proxy zones`;
    dom.trendSubtitle.textContent = `${role.toUpperCase()} ${trend.gamma.keyframes[0]?.expiry || "—"} · ${WEIGHTINGS[weighting]} · X: session time · Y: absolute SPX · Gamma sample-and-hold only within valid_until`;
    dom.trendCadence.textContent = `Visual ${REPLAY_VISUAL_FPS} fps · SPX observed ~1–2s · Gamma keyframes ~${app.timelineStepMinutes}m · hold-last · first-observation fixed axis · color intensity normalized per keyframe · not market-data FPS`;
    if (!app.snapshot && !dom.scenarioDiagnostic.open) {
      dom.surfaceTitle.textContent = "Scenario diagnostic · full frame not loaded";
      dom.surfaceSubtitle.textContent = "展开后按当前 Gamma keyframe 异步加载并校验完整 Spot × Forward-time frame";
    }
    renderTrendStatic();
    updateFilters();
    updateReplayControls();
    const exactUrlFrame = app.frames.find((frame) => frame.at.getTime() === app.playheadMs);
    updateModeQuery(exactUrlFrame || null);
    shouldSyncScenario = syncScenario;
  } catch (error) {
    if (!requestIsCurrent(generation, "replay")) return;
    renderTrendFailure(error);
  } finally {
    window.clearTimeout(abortTimer);
    if (app.requestController === controller) app.requestController = null;
  }
  if (shouldSyncScenario && app.mode === "replay" && app.sessionDate === sessionDate) {
    syncScenarioFrameToPlayhead();
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
    !app.trend ||
    !Number.isFinite(app.playheadMs)
  ) return;
  let gammaIndex = binarySearchLastAtOrBefore(
    app.trend.gamma.keyframes,
    app.playheadMs,
    (keyframe) => keyframe.atMs,
  );
  if (gammaIndex < 0) gammaIndex = 0;
  const keyframe = app.trend.gamma.keyframes[gammaIndex];
  const frameIndex = app.frames.findIndex((frame) => frame.id === keyframe.id);
  if (frameIndex < 0) {
    renderScenarioFetchFailure(new Error("replay_trend_frame_not_in_timeline"));
    return;
  }
  app.frameIndex = frameIndex;
  app.frames[frameIndex].artifactSha256 = keyframe.frameArtifactSha256;
  updateModeQuery(app.frames[frameIndex]);
  updateReplayControls();
  if (app.snapshot?.mode === "replay" && app.snapshot.replayId === keyframe.id) return;
  loadReplayFrame();
}

function replayFrameRequestUrl(frame) {
  const params = new URLSearchParams({ at: formatIsoUtc(frame.at) });
  return `${REPLAY_SESSIONS_URL}/${encodeURIComponent(app.sessionDate)}/frame?${params}`;
}

async function loadReplayFrame() {
  if (app.mode !== "replay") return;
  const frame = replayFrame();
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
    if (!requestIsCurrent(generation, "replay") || replayFrame() !== frame ||
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
  app.replayCatalogLoading = mode === "replay";
  dom.scenarioDiagnostic.open = mode === "live";
  if (syncQuery) updateModeQuery(null, { push: true });
  renderLoadingState();
  updateReplayControls();
  if (mode === "replay") loadReplayCatalog();
  else refreshSnapshot();
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
  app.snapshot = null;
  app.timelineSha256 = "";
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
  if (isReplayView()) {
    app.expiryRole = ["front", "next"].includes(dom.expiryFilter.value)
      ? dom.expiryFilter.value
      : "front";
    app.expiry = app.expiryRole;
  } else {
    app.expiry = dom.expiryFilter.value;
    app.expiryRole = selectedExpiry()?.role || app.expiryRole;
  }
  renderSummary();
  renderVisuals();
  if (isReplayView()) loadReplayTrend();
});
dom.weightingFilter.addEventListener("change", () => {
  app.weighting = dom.weightingFilter.value;
  renderSummary();
  renderVisuals();
  if (isReplayView()) loadReplayTrend();
});
dom.metricFilter.addEventListener("change", () => {
  app.metric = dom.metricFilter.value;
  renderSummary();
  renderVisuals();
  if (isReplayView()) loadReplayTrend();
});
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
        ? Math.max(app.trend.openMs, app.trend.spx.knownMs[0])
        : app.trend.closeMs,
      {
        syncFrame: true,
      },
    );
  }
});
dom.scenarioDiagnostic.addEventListener("toggle", () => {
  if (!dom.scenarioDiagnostic.open) return;
  if (app.playing) {
    dom.scenarioDiagnostic.open = false;
    return;
  }
  if (isReplayView() && app.trend && !app.playing) {
    syncScenarioFrameToPlayhead();
  } else if (app.snapshot) {
    window.requestAnimationFrame(renderVisuals);
  }
});

window.addEventListener("popstate", () => {
  setViewMode(initialModeFromQuery(), { syncQuery: false });
});

if ("ResizeObserver" in window) {
  const resizeObserver = new ResizeObserver(() => {
    if (app.snapshot && dom.scenarioDiagnostic.open) window.requestAnimationFrame(renderVisuals);
    if (app.trend) window.requestAnimationFrame(renderTrendStatic);
  });
  resizeObserver.observe(dom.heatmapStage);
  resizeObserver.observe(dom.trendStage);
} else {
  window.addEventListener("resize", () => {
    if (app.snapshot && dom.scenarioDiagnostic.open) window.requestAnimationFrame(renderVisuals);
    if (app.trend) window.requestAnimationFrame(renderTrendStatic);
  });
}

document.addEventListener("visibilitychange", () => {
  if (document.hidden && app.playing) {
    stopPlayback({ syncFrame: false, announce: true });
  }
});

if (isObject(globalThis.__SPX_SPARK_TEST_HOOK__)) {
  Object.assign(globalThis.__SPX_SPARK_TEST_HOOK__, {
    canonicalReplaySha256,
    normalizeReplayTrend,
  });
}

if (globalThis.__SPX_SPARK_DISABLE_AUTO_START__ !== true) {
  setViewMode(app.mode, { syncQuery: false });
}
