"use strict";

const SNAPSHOT_URL = "api/v1/snapshot";
const REPLAY_SESSIONS_URL = "api/v1/replay/sessions";
const REPLAY_POLICY_VERSION = "spxw_surface_replay.v3";
const REPLAY_CATALOG_KIND = "spxw_surface_replay_catalog";
const REPLAY_TIMELINE_POLICY_VERSION = "spxw_surface_replay_timeline.event_driven.v1";
const REPLAY_FRAME_VALIDATION = "known_clock_validation_on_frame_request";
const REPLAY_CLOSE_GRACE_POLICY = "session_close_plus_2h_grace";
const REPLAY_CLOSE_GRACE_SECONDS = 2 * 60 * 60;
const REPLAY_TIMELINE_STEP_MINUTES = 5;
const REPLAY_PLAY_INTERVAL_MS = 2_000;
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
  playbackTimer: null,
  requestController: null,
  requestGeneration: 0,
  replayCatalogLoading: false,
  projectionPolicySha256: "",
  sessions: [],
  sessionDate: "",
  frames: [],
  frameIndex: -1,
  frameLoading: false,
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
  const replayAt = app.snapshot?.mode === "replay" ? app.snapshot.requestedAsOf : frame?.at;
  document.body.classList.toggle("mode-replay", replay);
  dom.modeFilter.value = replay ? "replay" : "live";
  dom.replayBanner.hidden = !replay;
  dom.replayConsole.hidden = !replay;
  dom.pageLede.textContent = replay
    ? "按交易日回放标的价格 × 时间的期权暴露地形。帧使用有界 PIT；Schwab 缺少真实 availability clock，因此不能严格证明无前视。"
    : "标的价格 × 时间的期权暴露地形。所有值均来自只读生产快照，不提供下单入口。";
  const sourceFiles = app.snapshot?.mode === "replay" ? app.snapshot.source?.source_files : null;
  dom.sourceFile.textContent = replay
    ? sourceFiles?.join(", ") || (frame?.id ? `replay frame ${frame.id}` : "replay catalog")
    : "spxw_surface_dashboard.json";
  dom.sourceMode.textContent = replay
    ? `SESSION REPLAY · Frozen · Bounded PIT · frame ${app.frameIndex >= 0 ? app.frameIndex + 1 : 0}/${app.frames.length} · Not live`
    : "5 秒只读刷新";
  if (replay) {
    const verified = app.snapshot?.mode === "replay";
    const dataAsOf = verified ? formatIsoUtc(app.snapshot.dataAsOf) : "等待校验";
    const auditState = verified
      ? `${app.snapshot.source.lookahead_rows_selected} selected lookahead rows`
      : "frame contract not yet verified";
    dom.replayBannerLabel.textContent = replayAt
      ? `Replay · ${formatMarketTime(replayAt)}`
      : "Replay · select an audited frame";
    dom.replayBannerAsOf.textContent = `As of ${formatReplayAsOf(replayAt)} · data_as_of ${dataAsOf} · received/source clock cutoff · ${auditState} · availability clock missing, PIT bounded not proven`;
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

function normalizeReplayTimeline(raw, sessionDate, expectedProjectionPolicySha256) {
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
  return {
    frames,
    stepMinutes: raw.step_minutes,
    projectionPolicySha256: expectedProjectionPolicySha256,
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
  const frame = replayFrame();
  const frameCount = app.frames.length;
  const currentSession = app.sessions.find((item) => item.date === app.sessionDate);
  dom.replayTimeline.min = "0";
  dom.replayTimeline.max = String(Math.max(frameCount - 1, 0));
  dom.replayTimeline.value = String(Math.max(app.frameIndex, 0));
  const navigationLocked = app.replayCatalogLoading;
  dom.replaySessionFilter.disabled = navigationLocked || app.sessions.length === 0;
  dom.replayTimeline.disabled = navigationLocked || frameCount === 0 || app.frameLoading;
  dom.replayPrevious.disabled = navigationLocked || app.frameIndex <= 0 || app.frameLoading;
  dom.replayNext.disabled = navigationLocked || app.frameIndex < 0 || app.frameIndex >= frameCount - 1 || app.frameLoading;
  dom.replayPlay.disabled = navigationLocked || frameCount < 2 || (app.frameLoading && !app.playing);
  dom.replaySpeed.disabled = navigationLocked || frameCount < 2;
  dom.replaySpeed.value = String(app.speed);
  dom.replayPlay.textContent = app.playing ? "❚❚ 暂停" : "▶ 播放";
  dom.replayPlay.setAttribute("aria-label", app.playing ? "暂停回放" : "播放回放");
  dom.replayFrameTime.textContent = frame ? frame.label : "—";
  dom.replayFramePosition.textContent = `${app.frameIndex >= 0 ? app.frameIndex + 1 : 0} / ${frameCount}`;
  dom.replayTimeline.setAttribute(
    "aria-valuetext",
    frame ? `${app.frameIndex + 1} of ${frameCount}, ${frame.label}` : "No replay frame selected",
  );
  dom.replayTimelineStart.textContent = frameCount ? formatMarketTime(app.frames[0].at, false) : "—";
  dom.replayTimelineEnd.textContent = frameCount ? formatMarketTime(app.frames.at(-1).at, false) : "—";
  const sessionLabel = currentSession?.label || app.sessionDate || "—";
  const cachedCount = app.frames.filter((item) => item.cached).length;
  dom.replaySessionMeta.textContent = navigationLocked
    ? `${sessionLabel} · 正在校验回放目录合同`
    : frameCount
    ? `${sessionLabel} · ${frameCount} 个有界 PIT 帧 · ${app.timelineStepMinutes} 分钟索引 · ${cachedCount} 个已缓存 · availability clock 缺失`
    : `${sessionLabel} · 没有可用有界 PIT 帧`;
  updateModeChrome();
}

function stopPlayback() {
  window.clearTimeout(app.playbackTimer);
  app.playbackTimer = null;
  app.playing = false;
  updateReplayControls();
}

function schedulePlayback() {
  window.clearTimeout(app.playbackTimer);
  app.playbackTimer = null;
  if (!app.playing || app.frameLoading) return;
  if (app.frameIndex >= app.frames.length - 1) {
    stopPlayback();
    return;
  }
  app.playbackTimer = window.setTimeout(() => {
    app.playbackTimer = null;
    selectReplayFrame(app.frameIndex + 1, { keepPlaying: true });
  }, REPLAY_PLAY_INTERVAL_MS / app.speed);
}

function cancelSnapshotWork() {
  window.clearTimeout(app.timer);
  window.clearTimeout(app.playbackTimer);
  app.timer = null;
  app.playbackTimer = null;
  app.playing = false;
  app.frameLoading = false;
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
  app.timelineStepMinutes = REPLAY_TIMELINE_STEP_MINUTES;
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
    const timeline = normalizeReplayTimeline(
      payload,
      sessionDate,
      app.projectionPolicySha256,
    );
    if (!requestIsCurrent(generation, "replay") || app.sessionDate !== sessionDate) return;
    if (!timeline.frames.length) throw new Error("replay_timeline_empty");
    app.frames = timeline.frames;
    app.timelineStepMinutes = timeline.stepMinutes;
    let frameIndex = timeline.frames.length - 1;
    if (requestedAtText) {
      const requestedAt = parseDate(requestedAtText);
      if (!requestedAt) throw new Error("invalid_requested_replay_at");
      frameIndex = timeline.frames.findIndex((item) => item.at.getTime() === requestedAt.getTime());
      if (frameIndex < 0) throw new Error("requested_replay_frame_unavailable");
    }
    app.frameIndex = frameIndex;
    app.replayCatalogLoading = false;
    updateReplayControls();
    updateModeQuery();
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
  if (app.mode === "replay" && app.sessionDate === sessionDate) loadReplayFrame();
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
  renderLoadingState();
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
    schedulePlayback();
  } catch (error) {
    if (!requestIsCurrent(generation, "replay")) return;
    app.frameLoading = false;
    app.playing = false;
    renderFetchFailure(error, "replay");
    updateReplayControls();
  } finally {
    window.clearTimeout(abortTimer);
    if (app.requestController === controller) app.requestController = null;
  }
}

function selectReplayFrame(index, { keepPlaying = false } = {}) {
  if (!Number.isInteger(index) || index < 0 || index >= app.frames.length) return;
  if (!keepPlaying) stopPlayback();
  if (index === app.frameIndex && app.snapshot) {
    if (keepPlaying) schedulePlayback();
    return;
  }
  app.frameIndex = index;
  updateModeQuery(replayFrame(), { push: !keepPlaying });
  updateReplayControls();
  loadReplayFrame();
}

function setViewMode(mode, { syncQuery = true } = {}) {
  if (!["live", "replay"].includes(mode)) return;
  cancelSnapshotWork();
  resetReplayNavigationState();
  app.mode = mode;
  app.replayCatalogLoading = mode === "replay";
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
  app.replayCatalogLoading = true;
  updateModeQuery(null, { push: true });
  renderLoadingState();
  loadReplayTimeline(date, { preserveRequestedAt: false });
});
dom.replayPrevious.addEventListener("click", () => {
  selectReplayFrame(app.frameIndex - 1);
});
dom.replayNext.addEventListener("click", () => {
  selectReplayFrame(app.frameIndex + 1);
});
dom.replayPlay.addEventListener("click", () => {
  if (app.playing) {
    stopPlayback();
    return;
  }
  if (app.frameIndex >= app.frames.length - 1) {
    app.frameIndex = 0;
    updateModeQuery(replayFrame(), { push: true });
    updateReplayControls();
    app.playing = true;
    loadReplayFrame();
    return;
  }
  app.playing = true;
  updateReplayControls();
  schedulePlayback();
});
dom.replaySpeed.addEventListener("change", () => {
  const speed = Number(dom.replaySpeed.value);
  app.speed = [1, 2, 4].includes(speed) ? speed : 1;
  updateReplayControls();
  if (app.playing) schedulePlayback();
});
dom.replayTimeline.addEventListener("input", () => {
  const index = Number(dom.replayTimeline.value);
  const frame = app.frames[index];
  if (frame) {
    dom.replayFrameTime.textContent = frame.label;
    dom.replayFramePosition.textContent = `${index + 1} / ${app.frames.length}`;
    dom.replayTimeline.setAttribute(
      "aria-valuetext",
      `${index + 1} of ${app.frames.length}, ${frame.label}`,
    );
  }
});
dom.replayTimeline.addEventListener("change", () => {
  selectReplayFrame(Number(dom.replayTimeline.value));
});
dom.expiryFilter.addEventListener("change", () => {
  app.expiry = dom.expiryFilter.value;
  app.expiryRole = selectedExpiry()?.role || app.expiryRole;
  renderSummary();
  renderVisuals();
});
dom.weightingFilter.addEventListener("change", () => {
  app.weighting = dom.weightingFilter.value;
  renderSummary();
  renderVisuals();
});
dom.metricFilter.addEventListener("change", () => {
  app.metric = dom.metricFilter.value;
  renderSummary();
  renderVisuals();
});
dom.heatmap.addEventListener("pointermove", (event) => {
  if (app.chartHit) tooltipForHit(app.chartHit, event);
});
dom.heatmap.addEventListener("pointerleave", () => {
  dom.heatmapTooltip.hidden = true;
});

window.addEventListener("popstate", () => {
  setViewMode(initialModeFromQuery(), { syncQuery: false });
});

if ("ResizeObserver" in window) {
  const resizeObserver = new ResizeObserver(() => {
    if (app.snapshot) window.requestAnimationFrame(renderVisuals);
  });
  resizeObserver.observe(dom.heatmapStage);
} else {
  window.addEventListener("resize", () => {
    if (app.snapshot) window.requestAnimationFrame(renderVisuals);
  });
}

setViewMode(app.mode, { syncQuery: false });
